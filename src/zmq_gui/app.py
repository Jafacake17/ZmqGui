"""
ZmqGui — standalone NiceGUI dashboard that subscribes to any set of
ZMQ PUB endpoints and renders fills, heartbeats, metrics, and
arbitrage snapshots.

This is a faithful port of ModularTradeApp/console/gui/app.py,
decoupled from that project's imports so it can run as an
independent web server that any ZMQ-publishing app (orchestrators,
scanners, bots) can aim at.

Message envelope it speaks:
    topic = "fill.<scenario>.<strategy>" or "fill.<strategy>"
    topic = "metric.<strategy>"
    topic = "heartbeat.<strategy>"
    topic = "arb.scan" (whole-snapshot from a sidecar)
    topic = "tick.<symbol>" (on the separate feed endpoint)
    topic = "command" (stop / reload / ...)

See README.md for the dataflow picture.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from typing import Optional

import zmq
import yaml
from nicegui import app, run, ui
from nicegui.elements.timer import Timer as _NgTimer
from contextlib import nullcontext as _nullcontext
from pathlib import Path

from .bus import Bus


# ---------------------------------------------------------------------- #
# NiceGUI timer parent-slot-deleted race fix
# ---------------------------------------------------------------------- #
# `nicegui.element.Element.parent_slot` is a property that raises
# RuntimeError when its weakref'd slot has been GC'd (e.g. after a
# client disconnects and the per-session element tree is torn down).
# `Timer._get_context` reads it every tick — so a timer whose slot has
# just been deleted logs a full traceback at ERROR via
# `background_tasks._handle_exceptions`, even though the only action
# needed is "stop ticking".
#
# NiceGUI's own `_should_stop` checks `is_deleted` and `client.id not
# in Client.instances` but not the slot-weakref-dead case, so the
# race still escapes. We narrowly patch `Timer._get_context` to catch
# the RuntimeError and fall back to `nullcontext()` — the next pass
# of the surrounding loop's `_should_stop` check exits cleanly.
#
# Trigger observed this session: the external health-check `curl /`
# probe opens a short-lived NiceGUI session; teardown races with the
# 1–2s `ui.timer` callbacks. Three clusters of 6 tracebacks each on
# 2026-04-22 22:47:24, 2026-04-23 00:48:44, and 2026-04-23 04:49:34,
# always ~1 min after a sweep.
def _safe_timer_get_context(self):
    try:
        ps = self.parent_slot
    except RuntimeError:
        return _nullcontext()
    return ps or _nullcontext()


_NgTimer._get_context = _safe_timer_get_context
from .config import GuiCfg, StaCfg, load as load_config
from .theme import (
    BG_DARK, BG_PANEL, BG_HEADER,
    GREEN, YELLOW, RED, BLUE, TEXT_SECONDARY, TEXT_PRIMARY,
    PNL_LINE_COLOUR, ROW_EVEN, ROW_ODD,
    CHART_HEIGHT, TRADE_LOG_HEIGHT,
)

logger = logging.getLogger(__name__)

# Maximum items to keep in memory for display
MAX_TRADE_LOG = 200
MAX_PNL_POINTS = 500

# Strategy IDs that publish fills via the ZMQ envelope but aren't part
# of the main trader book — they have their own dedicated tabs. Keep
# these OUT of the trader-tab cumulative P&L so the chart reflects
# plugin trading only. Extend when adding non-trader sidecars.
_SIDECAR_SIDS = {"arbitrage", "arbitrage_live"}


# ---------------------------------------------------------------------- #
# Economic calendar state persistence
# ---------------------------------------------------------------------- #

_ECON_CAL_STATE_PATH = Path.home() / ".config" / "zmqgui" / "econ_cal_state.json"
_ECON_CAL_STATE_TTL = 86400  # 24 h — beyond this the persisted OK is ignored


def _load_econ_cal_state() -> tuple[float, dict | None]:
    """Return (ok_ts, ok_msg) from disk, or (0.0, None) if absent/stale/corrupt."""
    try:
        data = json.loads(_ECON_CAL_STATE_PATH.read_text())
        ok_ts = float(data["ok_ts"])
        if time.time() - ok_ts > _ECON_CAL_STATE_TTL:
            return 0.0, None
        return ok_ts, data.get("ok_msg")
    except Exception:
        return 0.0, None


def _save_econ_cal_state(ok_ts: float, ok_msg: dict) -> None:
    """Atomically persist the last calendar OK heartbeat to disk."""
    try:
        _ECON_CAL_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _ECON_CAL_STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps({"ok_ts": ok_ts, "ok_msg": ok_msg}))
        tmp.rename(_ECON_CAL_STATE_PATH)
    except Exception as exc:
        logger.warning("Failed to persist econ_cal_state: %s", exc)


# ---------------------------------------------------------------------- #
# Broker cost profile loading
# ---------------------------------------------------------------------- #

# PR-5 cost-model unification (2026-05-13): _load_broker_profiles and
# _calc_commission_bps were retired. The Broker Effective Costs panel
# now reads `total_bps` and the per-component breakdown directly off
# the orchestrator's heartbeat `broker_spreads[<short>][<sym>]` fields
# (orch-side `_apply_structural_breakdown` populated by PR-4 lands the
# size-invariant structural RT cost via cost_model.cost_per_roundtrip_
# structural_bps). ZmqGui no longer parses yamls or recomputes
# commission — single source of truth lives in
# console/core/cost_model.py. See git e03d527..PR-5 for the full
# unification arc.


# Active deployed-spec instrument set as of 2026-05-13. Used by the
# Broker Effective Costs panel for row enumeration. USD_CHF and
# EUR_JPY intentionally excluded — no promoted spec touches them today.
# When a spec promotes there, append to this list. Sorted display
# order matches the table layout.
_ACTIVE_PAIRS: list[str] = [
    "AUD_USD", "EUR_GBP", "EUR_USD", "GBP_USD",
    "NZD_USD", "USD_CAD", "USD_JPY", "XAU_USD",
]


def _fmt_canonical_mid(instrument: str, mid: float) -> str:
    """Format mid price per pair-class convention.

    - 5dp for FX majors (EUR_USD, GBP_USD, AUD_USD, NZD_USD, USD_CAD, EUR_GBP)
    - 3dp for *_JPY pairs (USD_JPY, EUR_JPY)
    - 2dp for metals (XAU_USD, XAG_USD)
    Returns "—" when no valid mid is available.
    """
    if mid <= 0:
        return "—"
    if instrument.startswith("XAU") or instrument.startswith("XAG"):
        return f"{mid:,.2f}"
    if instrument.endswith("_JPY"):
        return f"{mid:.3f}"
    return f"{mid:.5f}"


def _tick_mid(tick_prices: dict, symbol: str) -> float:
    data = tick_prices.get(symbol) or tick_prices.get(symbol.upper())
    if not data:
        return 0.0
    bid = float(data.get("bid") or 0)
    ask = float(data.get("ask") or 0)
    if bid > 0 and ask > bid:
        return (bid + ask) / 2.0
    return float(bid or ask or 0)


def _to_gbp(pnl_quote: float, symbol: str, tick_prices: dict) -> float:
    """Convert P&L from a pair's quote currency into GBP account currency.

    FX P&L is naturally in the quote currency: (exit − entry) × size gives
    USD for *_USD pairs and JPY for *_JPY pairs. Without conversion the
    dashboard over-states JPY P&L by ~190× (JPY/GBP ≈ 190).

    Conversion factors:
      quote=GBP → 1.0 (already home currency)
      quote=USD → divide by GBP_USD mid
      quote=JPY → divide by GBP_JPY synthetic (= GBP_USD × USD_JPY)

    Falls back to 1.0 (no conversion) when the required rates are absent
    from tick_prices — this preserves old behaviour for symbols not covered
    by the live feed rather than silently zeroing P&L.
    """
    parts = symbol.upper().split("_")
    if len(parts) != 2:
        return pnl_quote
    quote = parts[1]
    if quote == "GBP":
        return pnl_quote
    if quote == "USD":
        gbp_usd = _tick_mid(tick_prices, "GBP_USD")
        if gbp_usd > 0:
            return pnl_quote / gbp_usd
    elif quote == "JPY":
        gbp_usd = _tick_mid(tick_prices, "GBP_USD")
        usd_jpy = _tick_mid(tick_prices, "USD_JPY")
        if gbp_usd > 0 and usd_jpy > 0:
            return pnl_quote / (gbp_usd * usd_jpy)
    return pnl_quote


# ---------------------------------------------------------------------- #
# Condition-chip rendering
# ---------------------------------------------------------------------- #

def _fmt_currency(amount: float, currency: str = "GBP") -> str:
    """Format currency with 2 decimals and thousands separators.

    E.g. 1234.5 -> "£1,234.50"
    """
    if not isinstance(amount, (int, float)):
        return "—"
    symbol = "£" if currency == "GBP" else currency
    return f"{symbol}{amount:,.2f}"


def _fmt_age(seconds: float) -> str:
    """Compact relative-time formatter: "12s", "4m", "2h", "3d"."""
    s = max(0.0, float(seconds))
    if s < 60:
        return f"{s:.0f}s"
    if s < 3600:
        return f"{s / 60:.0f}m"
    if s < 86400:
        return f"{s / 3600:.1f}h"
    return f"{s / 86400:.1f}d"


def _fmt_trace_num(v) -> str:
    """Short numeric formatter for condition chips.

    Same rules as the one in ModularTradeApp's harness — keep them
    visually consistent."""
    if v is None:
        return "?"
    try:
        fv = float(v)
    except (TypeError, ValueError):
        return str(v)[:12]
    if abs(fv) >= 1000:
        return f"{fv:.2f}"
    return f"{fv:.4f}".rstrip("0").rstrip(".")


def _short_cond_label(full: str) -> str:
    """`indicator.hurst` → `hurst`. Drop only the 'indicator.' prefix
    since it's 90% of conditions in practice; keep `time.`/`price.`/
    `gap.` explicit so the condition origin stays readable."""
    if full.startswith("indicator."):
        return full[len("indicator."):]
    return full


def _coerce_float(v) -> float | None:
    """Convert STA's Decimal-as-JSON-string values to float.

    STA serialises Decimal fields as strings ("0.0", "1.45") for
    precision-preserving JSON. ZmqGui is the conversion boundary;
    None passes through so callers can distinguish "no data" from zero.
    """
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _fmt_countdown(seconds) -> str:
    """Format a seconds-until-event as a compact countdown: "2d 3h", "4h 12m", "8m 30s"."""
    if seconds is None:
        return "—"
    s = max(0, int(seconds))
    if s >= 86400:
        return f"{s // 86400}d {(s % 86400) // 3600}h"
    if s >= 3600:
        return f"{s // 3600}h {(s % 3600) // 60}m"
    if s >= 60:
        return f"{s // 60}m {s % 60}s"
    return f"{s}s"


def _fmt_legs(legs: list) -> str:
    """Condense STA leg list into 'SHORT 222.5P / LONG 215P NVDA 2026-05-22'."""
    if not legs:
        return "—"
    parts = []
    underlying = ""
    expiry = ""
    for leg in legs:
        c = leg.get("contract") or {}
        raw_side = (leg.get("side") or "").upper()
        side = "LONG" if raw_side in ("BUY", "LONG") else "SHORT"
        strike = leg.get("qty", "")  # qty as fallback label
        strike = c.get("strike", strike)
        right = (c.get("right") or "")[:1].upper()
        if not underlying:
            underlying = c.get("underlying", "")
            expiry = (c.get("expiry") or "")[:10]
        parts.append(f"{side} {strike}{right}")
    result = " / ".join(parts)
    if underlying:
        result += f"  {underlying} {expiry}"
    return result


def _build_gate_chips(gate_trace: list) -> list:
    """Build chip list from STA gate_trace — same pattern as MTA entry_trace chips."""
    from .theme import GREEN, RED, YELLOW, TEXT_SECONDARY
    chips = []
    for g in (gate_trace or []):
        passed = g.get("passed")
        indicator = g.get("indicator") or g.get("gate_id") or "?"
        op = g.get("op", "")
        threshold = g.get("threshold")
        observed = g.get("observed")
        thr_s = _fmt_trace_num(threshold)
        obs_s = _fmt_trace_num(observed) if observed is not None else "?"
        label = f"{indicator}: {obs_s} {op} {thr_s}"
        blocker = g.get("blocker_reason")
        if blocker and passed is False:
            label += f" ({blocker})"
        color = GREEN if passed is True else RED if passed is False else YELLOW
        chips.append({"label": label, "color": color})
    return chips


_TRADE_TYPE_SKIP = {"credit", "spread", "put", "call"}


def _derive_label(spec_id: str) -> str:
    """Derive a short readable label from spec_id.

    'a1_nvda_bull_put_2026_05_29'        → 'A1 NVDA Bull Put 5/29'
    'a3_retail_earnings_basket_2026_05_22' → 'A3 Retail Earnings 5/22'
    'a3a_tgt_bull_put_credit_spread'      → 'A3a TGT Bull Put'
    """
    parts = (spec_id or "").split("_")
    if not parts:
        return spec_id or "?"

    raw_prefix = parts[0]
    # a1→A1, a3a→A3a, smoke→Smoke
    if len(raw_prefix) >= 2 and raw_prefix[0].isalpha() and raw_prefix[1].isdigit():
        prefix = raw_prefix[0].upper() + raw_prefix[1:]
    else:
        prefix = raw_prefix.capitalize()

    # Find year (4-digit, ≥2020) to delimit name vs date
    year_idx = None
    for i, p in enumerate(parts[1:], 1):
        if len(p) == 4 and p.isdigit() and int(p) >= 2020:
            year_idx = i
            break

    middle = parts[1:year_idx] if year_idx else parts[1:]
    date_str = ""
    if year_idx and len(parts) > year_idx + 2:
        try:
            date_str = f"{int(parts[year_idx + 1])}/{int(parts[year_idx + 2])}"
        except (ValueError, IndexError):
            pass

    # Drop filler words that add noise; keep structural type words
    FILLER = {"absolute", "strikes", "test", "spread", "credit"}
    middle = [p for p in middle if p.lower() not in FILLER]

    formatted = []
    for i, p in enumerate(middle[:4]):
        # Short alpha-only part → likely a ticker → UPPER; else Title
        if i == 0 and p.isalpha() and len(p) <= 5:
            formatted.append(p.upper())
        else:
            formatted.append(p.capitalize())

    result = prefix
    if formatted:
        result += " " + " ".join(formatted)
    if date_str:
        result += " " + date_str
    return result


def _derive_trade_type(spec_id: str) -> tuple[str, str]:
    """Return (label, color) for trade-type badge derived from spec_id keywords."""
    from .theme import GREEN, RED, YELLOW, BLUE, TEXT_SECONDARY
    s = (spec_id or "").lower()
    if "basket" in s:
        return "Basket", BLUE
    if "bull_put" in s or "bull put" in s:
        return "Bull Put", GREEN
    if "bear_call" in s:
        return "Bear Call", RED
    if "iron_condor" in s or "condor" in s:
        return "Iron Condor", YELLOW
    if "calendar" in s:
        return "Calendar", YELLOW
    if "straddle" in s or "strangle" in s:
        return "Vol Play", YELLOW
    return "Options", TEXT_SECONDARY


def _fmt_sched_col(state: str, scheduled_entry: str | None,
                   time_to_stop: int | None) -> str:
    """Format the Scheduled/Countdown cell per state."""
    if state == "AUTHORED":
        if scheduled_entry:
            try:
                from datetime import datetime as _dt
                dt = _dt.fromisoformat(
                    scheduled_entry.replace("Z", "+00:00"))
                return f"fires {dt.month}/{dt.day} {dt.strftime('%H:%M')} UTC"
            except Exception:
                return (scheduled_entry[:16] if scheduled_entry else "—")
        return "—"
    if state == "GATE_PENDING":
        if time_to_stop is not None:
            return f"gate pending  ({_fmt_countdown(time_to_stop)} to stop)"
        return "gate pending"
    if state in ("ACTIVE", "DISPATCHED", "MILESTONE_CHECK", "PARTIAL_CLOSED"):
        if time_to_stop is not None:
            return f"stop in {_fmt_countdown(time_to_stop)}"
        return "—"
    if state == "CLOSED":
        return "closed"
    return "—"


def _build_strategy_rows(strategies: dict,
                          allowed_strats: set | None,
                          strat_to_scenarios: dict,
                          broker_spreads: dict,
                          scenario_spread_gates: dict | None = None) -> list[dict]:
    """Build one row per (strategy, symbol) for the strategy table.

    When a plugin trades multiple instruments it emits per-symbol
    traces (entry_traces_by_symbol / filter_traces_by_symbol /
    filter_blocks_by_symbol). We produce one row per symbol so the
    conditions chips don't flicker every tick between instruments.

    For plugins that haven't emitted per-symbol traces yet (old harness,
    or before first tick), fall back to a single row with whatever
    singular trace is available.
    """
    rows: list[dict] = []
    for sid, info in sorted(strategies.items()):
        if sid == "arbitrage":
            continue
        if allowed_strats is not None and sid not in allowed_strats:
            continue

        status = info.get("status", "unknown")
        hb_age = time.time() - info.get("last_heartbeat", 0)
        if hb_age > 15 and status == "running":
            status = "stale"

        last_metric = info.get("last_metric", "")
        hb_activity = info.get("_heartbeat_activity", "")
        display_text = last_metric if last_metric else hb_activity
        scen_ids = strat_to_scenarios.get(sid, [])

        # Pull per-symbol maps (each may be absent on a cold start).
        etbs = info.get("_entry_traces_by_symbol") or {}
        ftbs = info.get("_filter_traces_by_symbol") or {}
        fbbs = info.get("_filter_blocks_by_symbol") or {}
        # Union of symbols seen anywhere in the per-symbol maps.
        symbols = sorted(set(etbs) | set(ftbs) | set(fbbs))

        if not symbols:
            # Fall back to a single aggregated row so the strategy
            # still appears while warmup/first-tick is pending.
            wc, wt = _warming_count(info)
            rows.append({
                "strategy_symbol": f"{sid}::",
                "strategy": sid,
                "symbol": "—",
                "status": status.upper(),
                "warming": f"{wc}/{wt}" if wc > 0 else "—",
                "warming_active": wc > 0,
                "last_signal": display_text,
                "condition_chips": _build_condition_chips(info),
                "scenario_chips": _build_scenario_chips(
                    scen_ids, "—", scenario_spread_gates),
                "spread_chips": _build_spread_chips("—", broker_spreads),
            })
            continue

        ci_strat = info.get("_current_indicators") or {}
        cibs = info.get("_current_indicators_by_symbol") or {}
        for sym in symbols:
            per = {
                "_entry_trace": etbs.get(sym),
                "_filter_trace": ftbs.get(sym),
                "_filter_block": fbbs.get(sym),
                # Strategy-level spec metadata is shared across all symbols
                "_spec_conditions": info.get("_spec_conditions"),
                "_spec_filters": info.get("_spec_filters"),
                # Per-symbol current_indicators (falls back to strategy-level)
                "_current_indicators": cibs.get(sym, ci_strat),
            }
            wc, wt = _warming_count(per)
            rows.append({
                "strategy_symbol": f"{sid}::{sym}",
                "strategy": sid,
                "symbol": sym,
                "status": status.upper(),
                "warming": f"{wc}/{wt}" if wc > 0 else "—",
                "warming_active": wc > 0,
                "last_signal": display_text,
                "condition_chips": _build_condition_chips(per),
                "scenario_chips": _build_scenario_chips(
                    scen_ids, sym, scenario_spread_gates),
                "spread_chips": _build_spread_chips(sym, broker_spreads),
            })
    return rows


def _build_scenario_chips(scenario_ids: list[str], symbol: str,
                          scenario_spread_gates: dict | None) -> list[dict]:
    """One pill per scenario this (strategy, symbol) routes to, coloured
    by whether that scenario's per-scenario spread gate would PASS on
    the current quote. Scenarios without a gate configured render in
    the neutral grey.

    Shape of each chip: {id, color, title}. `title` is an HTML tooltip
    summarising the gate state. Passed/failed colouring only covers
    the spread gate right now — richer risk-gate status can be layered
    in later without changing the Vue template.
    """
    if not scenario_ids:
        return []
    gates = scenario_spread_gates or {}
    chips = []
    for sid in scenario_ids:
        g = (gates.get(sid) or {}).get(symbol)
        if g is None:
            # No gate defined OR no quote yet — neutral
            color = TEXT_SECONDARY
            title = "no scenario spread gate" if sid not in gates else \
                    f"{sid}: no quote for {symbol}"
        elif g.get("passed"):
            color = GREEN
            title = (f"{sid}: {g.get('data_source')} {g.get('spread_bps')} bps "
                     f"≤ {g.get('threshold_bps')} bps")
        else:
            color = RED
            title = (f"{sid}: {g.get('data_source')} {g.get('spread_bps')} bps "
                     f"> {g.get('threshold_bps')} bps → blocks routing")
        chips.append({"id": sid, "color": color, "title": title})
    return chips


def _build_spread_chips(symbol: str, broker_spreads: dict) -> list[dict]:
    """One chip per broker carrying that symbol's current spread (bps).

    Colours by magnitude: green when < 1 bps, yellow 1-3 bps, red > 3 bps
    — tuned for FX majors. A stale quote (age > 15s) shows in grey to
    signal "no recent data, don't trust this number".
    """
    if symbol == "—" or not broker_spreads:
        return []
    chips = []
    for broker in sorted(broker_spreads.keys()):
        sym_map = broker_spreads.get(broker) or {}
        entry = sym_map.get(symbol)
        if not entry:
            continue
        bps = entry.get("spread_bps")
        age = entry.get("age_s") or 0
        if bps is None:
            color = TEXT_SECONDARY
            display = "—"
        elif age > 15:
            color = TEXT_SECONDARY
            display = f"{bps} (stale)"
        elif bps < 1.0:
            color = GREEN
            display = str(bps)
        elif bps < 3.0:
            color = YELLOW
            display = str(bps)
        else:
            color = RED
            display = str(bps)
        chips.append({"broker": broker, "bps": display, "color": color})
    return chips


def _build_condition_chips(info: dict) -> list[dict]:
    """Produce the list of {label, color} chips for one strategy row.

    Renders every filter AND every entry condition from the latest
    heartbeat trace, each as a separate chip coloured by its state:

      green  — passed
      red    — failed
      yellow — warming up (indicator not ready yet)

    Order: filters first (they run first in the engine), then entry
    conditions. When a filter is blocking, entry conditions may be
    stale — the GUI still shows them, greyed out, so the operator sees
    the full pipeline rather than just the first blocker.

    Returns [] if no data yet.
    """
    chips: list[dict] = []

    # Filters — render spec_filters with state from filter_trace,
    # or fall back to filter_trace / filter_block directly.
    ftrace = info.get("_filter_trace")
    fb = info.get("_filter_block")
    spec_filters = info.get("_spec_filters") or []

    if spec_filters:
        # Use spec_filters as canonical list, match against filter_trace.
        # Heartbeat spec_filters use "type" as the filter name key.
        for spec in spec_filters:
            fname = spec.get("type", spec.get("label", "?"))

            # Look up state in filter_trace
            passed = None
            detail = ""
            if ftrace:
                for entry in ftrace:
                    if entry.get("filter", "") == fname:
                        passed = entry.get("passed")
                        detail = entry.get("detail", "")
                        break
            # If not in filter_trace but there's a filter_block, check if it matches
            elif fb and fb.get("filter", "") == fname:
                passed = False
                detail = fb.get("detail", "")

            color = GREEN if passed is True else RED if passed is False else YELLOW

            if detail:
                chips.append({"label": f"{fname}: {detail}", "color": color})
            else:
                chips.append({"label": fname, "color": color})
    elif ftrace:
        # Fall back to filter_trace
        for entry in ftrace:
            passed = entry.get("passed")
            color = GREEN if passed else RED
            fname = entry.get("filter", "?")
            detail = entry.get("detail", "")
            chips.append({
                "label": f"{fname}: {detail}" if detail else fname,
                "color": color,
            })
    elif fb:
        # Fall back to filter_block (legacy path)
        chips.append({
            "label": f"blocked by {fb.get('filter', '?')}: {fb.get('detail', '')}",
            "color": RED,
        })

    # Entry conditions — render spec_conditions with their current state
    # from entry_trace, or fall back to the entry_trace data directly.
    # Spec conditions are static metadata; entry_trace provides runtime state.
    trace = info.get("_entry_trace") or {}
    conds = trace.get("conditions") or {}
    spec_conds = info.get("_spec_conditions") or []

    # When a filter is blocking and entry eval didn't run, show spec conditions
    # as grey "pending" chips (name: — op threshold) rather than suppressing
    # them entirely. This preserves condition structure visibility for the
    # operator without implying the indicator is warming up or has failed.
    filter_blocked_no_data = bool(not conds and fb and fb.get("filter"))

    # If we have spec_conditions, use them as the canonical list
    # (ensures we show all expected conditions, even if not yet evaluated).
    # Otherwise fall back to whatever's in entry_trace.
    if spec_conds:
        for spec in spec_conds:
            # Heartbeat spec_conditions use type/name/field/op/value keys.
            # Build the entry_trace lookup key, current_indicators lookup key,
            # and display name for each condition type.
            ctype = spec.get("type", "")
            if ctype in ("indicator", "indicator_cross"):
                cname = spec.get("name", "?")
                full_label = f"{ctype}.{cname}"
                display_name = _short_cond_label(full_label)
                # indicator_cross collapses to indicator.* in current_indicators
                ci_key = f"indicator.{cname}"
            elif ctype == "price":
                field = spec.get("field", "mid")
                val = spec.get("value", "?")
                full_label = f"price.{field}:{val}"
                display_name = f"price.{field}"
                ci_key = f"price.{field}"
            elif ctype == "time":
                field = spec.get("field", "?")
                full_label = f"time.{field}"
                display_name = full_label
                ci_key = full_label
            else:
                # Unknown type or legacy dict with explicit "label" key
                full_label = spec.get("label", spec.get("name", ctype or "?"))
                display_name = _short_cond_label(full_label)
                ci_key = full_label

            op = spec.get("op", spec.get("operator", ""))
            threshold = spec.get("value", spec.get("threshold", "?"))

            # When the filter blocked entry eval, use current_indicators (new
            # field from orchestrator) for live observed values. If the field
            # is present for this condition, render amber (blocked but live
            # data). If absent, fall back to grey em-dash (blocked, no data).
            if filter_blocked_no_data:
                ci = info.get("_current_indicators") or {}

                # Resolve RHS: if threshold is a named indicator ref
                # (e.g. "donchian_upper_d1"), look it up in current_indicators
                # so both sides are numeric in the chip label.
                try:
                    float(str(threshold))
                    rhs = threshold
                except (ValueError, TypeError):
                    rhs = ci.get(f"indicator.{threshold}", threshold)

                try:
                    display_thr = _fmt_trace_num(float(str(rhs)))
                except (ValueError, TypeError):
                    display_thr = str(rhs)

                obs_raw = ci.get(ci_key)
                if obs_raw is not None:
                    chips.append({
                        "label": f"{display_name}: {_fmt_trace_num(obs_raw)} {op} {display_thr}",
                        "color": "#e08c3c",  # amber — filter-blocked but live value
                    })
                else:
                    chips.append({
                        "label": f"{display_name}: — {op} {display_thr}",
                        "color": TEXT_SECONDARY,
                    })
                continue

            # Look up current state in entry_trace.
            # Price conditions with numeric thresholds: entry_trace stores the
            # unqualified key "price.mid" (not "price.mid:160.5"), so try
            # display_name as a fallback. Named-ref thresholds ("donchian_upper_h1")
            # produce full_label == "price.mid:donchian_upper_h1" which IS the
            # entry_trace key and hits on the first try — display_name fallback
            # is never reached. For indicator conditions with named refs, MTA
            # qualifies as "indicator.{name}:{ref}" — try the double-qualified form.
            entry = (conds.get(full_label)
                     or (conds.get(display_name) if ctype == "price" else None)
                     or conds.get(f"{full_label}:{threshold}")
                     or {})
            passed = entry.get("passed")
            obs = _fmt_trace_num(entry.get("observed"))
            # Prefer the resolved numeric threshold from entry_trace over
            # the spec's raw value (which may be a formula name like
            # "donchian_upper_h1" — MTA resolves it; we just need to display
            # the resolved number, not the formula string).
            display_thr = _fmt_trace_num(entry.get("threshold", threshold))

            if passed is True:
                color = GREEN
            elif passed is False:
                color = RED
            else:
                color = YELLOW

            if passed is None:
                if fb and fb.get("filter"):
                    # filter_block is active but stale entry_traces_by_symbol
                    # made conds non-empty, bypassing filter_blocked_no_data.
                    # Entry eval didn't run — show filter state, not "warming up".
                    f_name = fb.get("filter", "?")
                    f_detail = fb.get("detail", "")
                    chip_label = (f"{f_name}: {f_detail}" if f_detail else f_name)
                    chips.append({"label": chip_label, "color": TEXT_SECONDARY})
                else:
                    chips.append({"label": f"{display_name}: warming up", "color": color})
            else:
                chips.append({
                    "label": f"{display_name}: {obs} {op} {display_thr}",
                    "color": color,
                })
    elif conds:
        # Fall back to entry_trace conditions (legacy path)
        for full_label, entry in conds.items():
            passed = entry.get("passed")
            if passed is True:
                color = GREEN
            elif passed is False:
                color = RED
            else:
                color = YELLOW
            name = _short_cond_label(full_label)
            op = entry.get("op", "")
            obs = _fmt_trace_num(entry.get("observed"))
            thr = _fmt_trace_num(entry.get("threshold"))
            if passed is None:
                if fb and fb.get("filter"):
                    f_name = fb.get("filter", "?")
                    f_detail = fb.get("detail", "")
                    chip_label = (f"{f_name}: {f_detail}" if f_detail else f_name)
                    chips.append({"label": chip_label, "color": TEXT_SECONDARY})
                else:
                    chips.append({"label": f"{name}: warming up", "color": color})
            else:
                chips.append({
                    "label": f"{name}: {obs} {op} {thr}",
                    "color": color,
                })

    # Broker routing annotation — show most recent broker used for fills
    last_broker = info.get("last_broker_id", "")
    if last_broker:
        # Abbreviated broker names for display (oanda-practice → OANDA, etc.)
        broker_display = (
            "OANDA" if "oanda" in last_broker.lower() else
            "Duka" if "dukascopy" in last_broker.lower() else
            "IG" if "ig-cfd-demo" in last_broker.lower() else
            last_broker[:6]
        )
        chips.append({
            "label": f"Routed → {broker_display}",
            "color": BLUE,
        })

    return chips


def _warming_count(info: dict) -> tuple[int, int]:
    """Count entry conditions where observed=None vs total conditions.

    Returns (warming, total). Returns (0, 0) when conds is empty —
    empty means no entry-eval data yet (filter-blocked, not warming),
    so we don't surface a misleading count.
    """
    trace = info.get("_entry_trace") or {}
    conds = trace.get("conditions") or {}
    if not conds:
        return 0, 0
    warming = sum(1 for v in conds.values() if v.get("observed") is None)
    return warming, len(conds)


# Dedup window for fills — publishers may dual-publish every fill on
# both `fill.<scenario>.<strategy>` and `fill.<strategy>` during the
# scenarios transition. We dedupe by (scenario_id, order_id) so each
# fill is processed exactly once. The deque is bounded so memory stays flat.
MAX_FILL_DEDUP = 4096


class Dashboard:
    """
    Main dashboard. Subscribes to configured ZMQ sources and serves a
    web GUI via NiceGUI.

    Usage:
        cfg = load_config("config.yaml")
        Dashboard(cfg).run()   # blocks — starts the web server
    """

    def __init__(self, cfg: GuiCfg):
        self.cfg = cfg

        # -- State updated by ZMQ thread, read by UI timer --
        self._lock = threading.Lock()

        # Per-strategy tracking
        # strategy_id -> {status, pnl, trades, wins, last_heartbeat, last_metric, ...}
        self._strategies: dict[str, dict] = {}

        # Per-scenario tracking — populated from scenario-tagged fills and
        # heartbeat scenario_exposure payloads. Empty dict means scenarios
        # are not enabled or have not started reporting yet, in which case
        # the strategy table behaves as if only one scenario exists.
        self._scenarios: dict[str, dict] = {}

        # Authoritative map {scenario_id: [strategy_ids]} from the
        # orchestrator's heartbeat. Drives the scenario filter toggle
        # above the strategy table.
        self._scenario_strategies: dict[str, list[str]] = {}
        # Current filter selection ("All" or a scenario id).
        self._scenario_filter: str = "All"
        # Per-broker current spread snapshot for the Spread column.
        # Shape: {broker: {symbol: {bid, ask, spread_bps, age_s}}}
        self._broker_spreads: dict[str, dict] = {}
        # Per-scenario spread-gate snapshot used to colour scenario
        # chips per row. Shape: {scenario_id: {symbol: {passed, ...}}}
        self._scenario_spread_gates: dict[str, dict] = {}

        # Cumulative P&L over time: list of (timestamp, cumulative_pnl)
        self._pnl_history: list[tuple[float, float]] = []
        self._total_pnl: float = 0.0

        # Recent fills — raw stream, newest first. Drives the Trade Log
        # text history.
        self._trades: deque[dict] = deque(maxlen=MAX_TRADE_LOG)
        # Closed trades — pairs of opener+closer fills, with realised
        # P&L computed from the price delta × qty. Newest first.
        # Feeds the Recent Trades TABLE (distinct from the fills log).
        # Dedup by (scenario_id, strategy_id, symbol, entry_ts) so a
        # chaotic fan-out doesn't produce multiple rows for the same
        # logical trade.
        self._closed_trades: deque[dict] = deque(maxlen=MAX_TRADE_LOG)
        self._closed_trade_keys: set = set()

        # Fill deduplication — see MAX_FILL_DEDUP comment above.
        self._seen_fills: deque[tuple[str, str]] = deque(maxlen=MAX_FILL_DEDUP)
        self._seen_fills_set: set[tuple[str, str]] = set()

        # Alert banner text (cleared after display)
        self._alert: str = ""

        # Latest snapshot from each arb scanner publisher, keyed by
        # strategy_id ("arbitrage" = pre-match, "arbitrage_live" = live).
        # The Arbitrage tab's timer renders rows from both, tagged "PM"/"LIVE".
        # Kept as legacy `_arb_scan` for any old-tab code that reads it.
        self._arb_scans: dict[str, dict] = {}
        self._arb_scan: Optional[dict] = None

        # KnowledgeVault repo-hook heartbeats keyed by repo name, so
        # the Vault tab shows one row per repo with its latest commit
        # status. Value shape mirrors the publisher envelope
        # (status, activity, repo, sha, ts) plus `_received` = local
        # wall-clock when this process saw it (used for "Xs ago").
        self._vault_hooks: dict[str, dict] = {}

        # Arbitrage exchange balances: {strategy_id: {exchanges: [...]}}
        # Latest envelope from balance.arbitrage topic (paper + live streams)
        self._balance_arbitrage: Optional[dict] = None

        # Mode label (e.g. "live" / "paper") — updated from heartbeat.
        self._mode: str = "--"

        # Economic calendar health state.
        # _econ_cal_alarm: last non-OK message (stale/degraded/rotted)
        # _econ_cal_ok_ts: timestamp of last .ok heartbeat received
        # _econ_cal_ok_msg: last .ok payload (carries last_successful_fetch_age_sec)
        self._econ_cal_alarm: dict | None = None
        self._econ_cal_alarm_ts: float = 0.0
        _ok_ts, _ok_msg = _load_econ_cal_state()
        self._econ_cal_ok_ts: float = _ok_ts
        self._econ_cal_ok_msg: dict | None = _ok_msg
        if _ok_ts:
            logger.info("Loaded persisted calendar OK state (age %.0fs)", time.time() - _ok_ts)

        # Open trades — keyed by (strategy_id, symbol). An entry fill
        # opens a position; a closing fill (opposite side, same key)
        # removes it. Also synced authoritatively from heartbeat's
        # `open_positions` payload when that's present.
        self._open_trades: dict[tuple[str, str], dict] = {}

        # Latest tick prices from the feed PUB — symbol → {bid, ask}
        self._tick_prices: dict[str, dict] = {}

        # PR-5 cost-model unification (2026-05-13): self._broker_profiles
        # retired. The Broker Effective Costs panel reads total_bps +
        # breakdown directly from orch heartbeat's broker_spreads — the
        # orch-side _apply_structural_breakdown (cost_model.cost_per_
        # roundtrip_structural_bps) is the single source of truth for
        # what an RT costs on each (broker, instrument). ZmqGui doesn't
        # parse yamls anymore.

        # Crypto paper-trader state — per-strategy tracking
        # {spec_id: {ts, chains, halted, last_heartbeat, fill_count}}
        self._crypto_strategies: dict[str, dict] = {}
        # Recent crypto fills — newest first, limited window
        self._crypto_fills: deque[dict] = deque(maxlen=MAX_TRADE_LOG)
        # Per-chain block ticks from feed bus {chain: {number, ts, last_tick}}
        self._crypto_chains: dict[str, dict] = {}

        # MFE/MAE cache — keyed by (strategy_id, symbol, entry_ts).
        # Values: dict with mfe_bps/mae_bps, or None while pending.
        self._mfe_mae_cache: dict = {}
        # Queue of closed-trade dicts waiting for MFE/MAE computation.
        # Background thread drains it via psycopg2 queries on QuestDB.
        self._mfe_mae_queue: list = []

        # Margin + aggregate bps — updated from heartbeat
        self._margin_summary: dict = {}   # latest margin_summary from orch
        self._agg_bps: dict = {}          # total_bps/mean_bps/median_bps from compute_stats

        # STA (StructuredTradeApp) state — updated from tcp://127.0.0.1:5570
        # _sta_latest: most recent heartbeat payload (full dict)
        # _sta_last_ts: wall-clock when last heartbeat was received
        # _sta_lifecycle_history: record_id → ordered list of state-transition
        #   snapshots for the STA Lifecycle audit tab
        self._sta_latest: dict | None = None
        self._sta_last_ts: float = 0.0
        self._sta_lifecycle_history: dict[str, list[dict]] = {}

        # ZMQ thread control
        self._zmq_running = False
        self._zmq_thread = None
        self._feed_thread = None
        self._sta_feed_thread = None
        # Watchdog counter — incremented at the top of every zmq_loop
        # iteration, including empty polls. The watchdog thread samples
        # this every 60s to detect the 2026-04-21-style wedge where the
        # process stays alive but the event loop goes silent.
        # Python int increments are atomic under the GIL, so no lock
        # is needed for a write-one-read-one counter.
        self._zmq_ticks: int = 0
        self._watchdog_thread = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def run(self):
        """Build the web GUI, start ZMQ, and launch the NiceGUI server. Blocks."""
        # Seed the trades deque + open-trades state from QuestDB BEFORE
        # the ZMQ stream starts so the dashboard shows history the
        # instant the page renders. Pre-existing open positions come
        # back via the orchestrator's heartbeat anyway (it replays
        # fills into each RiskManager on startup), but Recent Trades
        # is GUI-local state so it needs its own seed.
        self._seed_from_questdb()
        self._start_zmq()
        self._build_page()

        ui.run(
            title="ZmqGui",
            host="0.0.0.0",   # Bind to all interfaces (not just localhost)
            port=self.cfg.web_port,
            dark=True,
            reload=False,
            show=False,       # Don't auto-open browser (headless VPS friendly)
            # Required by app.storage.user (cookie-identified server storage,
            # used to remember the last-selected tab across page reloads).
            storage_secret="zmqgui-local-dashboard",
        )

    # ------------------------------------------------------------------ #
    # Page build — NiceGUI layout
    # ------------------------------------------------------------------ #

    def _build_page(self):
        """Define the single-page dashboard layout. NiceGUI builds it lazily."""

        dashboard = self
        cfg = self.cfg

        @ui.page("/")
        def index():
            ui.add_head_html(f"""
            <style>
                body {{
                    background-color: {BG_DARK} !important;
                    color: {TEXT_PRIMARY} !important;
                }}
                .nicegui-content {{
                    padding: 16px !important;
                }}
                .q-table__container {{
                    background-color: {BG_PANEL} !important;
                    color: {TEXT_PRIMARY} !important;
                }}
                .q-table thead th {{
                    background-color: {BG_HEADER} !important;
                    color: {TEXT_PRIMARY} !important;
                    font-weight: bold;
                }}
                .q-table tbody tr:nth-child(even) {{
                    background-color: {ROW_EVEN} !important;
                }}
                .q-table tbody tr:nth-child(odd) {{
                    background-color: {ROW_ODD} !important;
                }}
                .q-table tbody td {{
                    color: {TEXT_PRIMARY} !important;
                }}
            </style>
            """)

            # ---- Header bar ----
            with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                f"background-color: {BG_HEADER}; border-radius: 6px;"
            ):
                ui.label("ZMQ GUI").classes("text-xl font-bold").style(
                    f"color: {TEXT_PRIMARY};"
                )
                mode_label = ui.label("Mode: --").style(f"color: {TEXT_SECONDARY};")
                pnl_label = ui.label("Daily P&L: --").style(f"color: {GREEN};")
                strat_label = ui.label("Strategies: 0").style(f"color: {TEXT_SECONDARY};")
                clock_label = ui.label("").style(f"color: {TEXT_SECONDARY};")
                cal_label = ui.label("Calendar: OK").style(f"color: {GREEN}; font-size: 12px;")

            # Alert banner (hidden by default)
            alert_label = ui.label("").style(
                f"color: {RED}; font-weight: bold; padding: 4px 16px;"
            )

            # ---- Tabs ----
            with ui.tabs().classes("w-full").style(
                f"background-color: {BG_HEADER};"
            ) as tabs:
                console_tab = (
                    ui.tab("Console").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("console", True) else None
                )
                ftmo_tab = (
                    ui.tab("FTMO MT5").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("ftmo", True) else None
                )
                arb_tab = (
                    ui.tab("Arbitrage").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("arb", True) else None
                )
                vault_tab = (
                    ui.tab("Vault").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("vault", True) else None
                )
                crypto_tab = (
                    ui.tab("Crypto").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("crypto", True) else None
                )
                vuln_tab = (
                    ui.tab("Vuln").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("vuln", True) else None
                )
                qs_tab = (
                    ui.tab("Quick Screen").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("quick_screen", True) else None
                )
                sta_trades_tab = (
                    ui.tab("STA Trades").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("sta_trades", True) else None
                )
                sta_slates_tab = (
                    ui.tab("STA Slates").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("sta_slates", True) else None
                )
                sta_chain_tab = (
                    ui.tab("STA Chain").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("sta_chain", True) else None
                )
                sta_lifecycle_tab = (
                    ui.tab("STA Lifecycle").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("sta_lifecycle", True) else None
                )
                sta_health_tab = (
                    ui.tab("STA Health").style(f"color: {TEXT_PRIMARY};")
                    if cfg.tabs.get("sta_health", True) else None
                )

            # Restore the last-selected tab across page reloads via per-browser
            # user storage. Falls back to the first enabled tab if the saved
            # name is missing or points to a now-disabled tab.
            enabled_tab_names = [
                name for name, t in (
                    ("Console", console_tab),
                    ("FTMO MT5", ftmo_tab),
                    ("Arbitrage", arb_tab),
                    ("Vault", vault_tab),
                    ("Crypto", crypto_tab),
                    ("Vuln", vuln_tab),
                    ("Quick Screen", qs_tab),
                    ("STA Trades", sta_trades_tab),
                    ("STA Slates", sta_slates_tab),
                    ("STA Chain", sta_chain_tab),
                    ("STA Lifecycle", sta_lifecycle_tab),
                    ("STA Health", sta_health_tab),
                ) if t is not None
            ]
            saved_tab = app.storage.user.get("active_tab")
            initial_tab = (
                saved_tab if saved_tab in enabled_tab_names
                else (enabled_tab_names[0] if enabled_tab_names else None)
            )
            tabs.on_value_change(
                lambda e: app.storage.user.update(active_tab=e.value)
            )

            with ui.tab_panels(tabs, value=initial_tab).classes("w-full").style(
                f"background-color: {BG_DARK};"
            ):

              # ================ CONSOLE TAB ================
              if console_tab is not None:
                with ui.tab_panel(console_tab):

                    # ---- Scenarios summary card ----
                    ui.label("Scenarios").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    scen_columns = [
                        {"name": "scenario", "label": "Scenario", "field": "scenario", "align": "left"},
                        {"name": "broker", "label": "Broker", "field": "broker", "align": "left"},
                        {"name": "live", "label": "Live", "field": "live", "align": "center"},
                        {"name": "status", "label": "Status", "field": "status", "align": "center"},
                        {"name": "pnl", "label": "P&L Today (bps)", "field": "pnl", "align": "right"},
                        {"name": "trades", "label": "Trades", "field": "trades", "align": "center"},
                        {"name": "win_pct", "label": "Win %", "field": "win_pct", "align": "center"},
                        {"name": "n_strategies", "label": "Strategies", "field": "n_strategies", "align": "center"},
                    ]
                    scen_table = ui.table(
                        columns=scen_columns, rows=[], row_key="scenario",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    scen_table.add_slot("body-cell-status", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.status === 'active' ? '""" + GREEN + r"""'
                                     : props.row.status === 'killed' ? '""" + RED + r"""'
                                     : '""" + YELLOW + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.status }}</span>
                        </q-td>
                    """)
                    scen_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)

                    # ---- Broker Effective Costs panel ----
                    # PR-5 cost-model unification (2026-05-13): 8-pair × 3-broker
                    # table. Cell value = `total_bps` from the orch heartbeat's
                    # broker_spreads structural breakdown (cost_model.cost_per_
                    # roundtrip_structural_bps = spread + 2×slippage_bps +
                    # 2×per-unit-commission_bps; size-invariant per D9, NO flat
                    # fees in the cell). Canonical column = mid price formatted
                    # per pair-class convention. Tooltip on each broker cell
                    # surfaces the breakdown (spread / slip / comm / flat).
                    # Cheapest-per-row highlighted in green.
                    ui.label("Broker Effective Costs (RT bps)").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    broker_cost_columns = [
                        {"name": "instrument", "label": "Instrument",
                         "field": "instrument", "align": "left"},
                        {"name": "canonical", "label": "Mid",
                         "field": "canonical", "align": "right"},
                        {"name": "oanda", "label": "OANDA",
                         "field": "oanda", "align": "center"},
                        {"name": "dukascopy", "label": "Dukascopy",
                         "field": "dukascopy", "align": "center"},
                        {"name": "ig", "label": "IG",
                         "field": "ig", "align": "center"},
                    ]
                    broker_cost_rows = [
                        {"instrument": sym, "canonical": "—",
                         "oanda": "—", "dukascopy": "—", "ig": "—"}
                        for sym in _ACTIVE_PAIRS
                    ]
                    broker_cost_table = ui.table(
                        columns=broker_cost_columns, rows=broker_cost_rows,
                        row_key="instrument",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

                    # Canonical column rendered in muted text — it's reference
                    # context, not a decision-driving value.
                    broker_cost_table.add_slot("body-cell-canonical", r"""
                        <q-td :props="props" :style="{
                            color: '""" + TEXT_SECONDARY + r"""'
                        }">{{ props.row.canonical }}</q-td>
                    """)
                    # Broker cells: cheapest-per-row green-tinted bold; tooltip
                    # shows the spread/slip/comm breakdown.
                    broker_cost_table.add_slot("body-cell-oanda", r"""
                        <q-td :props="props" :style="{
                            backgroundColor: props.row.oanda_best ? '""" + GREEN + r"""22' : 'transparent',
                            color: props.row.oanda_best ? '""" + GREEN + r"""' : '""" + TEXT_PRIMARY + r"""',
                            fontWeight: props.row.oanda_best ? 'bold' : 'normal'
                        }">
                            {{ props.row.oanda }}
                            <q-tooltip v-if="props.row.oanda_tip" anchor="top middle" self="bottom middle">{{ props.row.oanda_tip }}</q-tooltip>
                        </q-td>
                    """)
                    broker_cost_table.add_slot("body-cell-dukascopy", r"""
                        <q-td :props="props" :style="{
                            backgroundColor: props.row.duka_best ? '""" + GREEN + r"""22' : 'transparent',
                            color: props.row.duka_best ? '""" + GREEN + r"""' : '""" + TEXT_PRIMARY + r"""',
                            fontWeight: props.row.duka_best ? 'bold' : 'normal'
                        }">
                            {{ props.row.dukascopy }}
                            <q-tooltip v-if="props.row.duka_tip" anchor="top middle" self="bottom middle">{{ props.row.duka_tip }}</q-tooltip>
                        </q-td>
                    """)
                    broker_cost_table.add_slot("body-cell-ig", r"""
                        <q-td :props="props" :style="{
                            backgroundColor: props.row.ig_best ? '""" + GREEN + r"""22' : 'transparent',
                            color: props.row.ig_best ? '""" + GREEN + r"""' : '""" + TEXT_PRIMARY + r"""',
                            fontWeight: props.row.ig_best ? 'bold' : 'normal'
                        }">
                            {{ props.row.ig }}
                            <q-tooltip v-if="props.row.ig_tip" anchor="top middle" self="bottom middle">{{ props.row.ig_tip }}</q-tooltip>
                        </q-td>
                    """)

                    # ---- Account Margin tile ----
                    # Populated from heartbeat margin_summary.accounts[].
                    # Values are currency-agnostic (whatever the account
                    # native unit is — USD by default config).
                    with ui.row().classes("w-full gap-4 mt-2 mb-1 flex-wrap") as _margin_row:
                        pass
                    margin_tile = _margin_row

                    # ---- Scenario filter (pills above the strategy table) ----
                    # Operator picks a scenario and the table filters to
                    # strategies routed to it. "All" is the default +
                    # shows the union. Membership from heartbeat's
                    # scenario_strategies map.
                    self._scenario_filter = "All"
                    with ui.row().classes("mt-4").style("gap: 4px;"):
                        ui.label("Scenario filter:").style(
                            f"color: {TEXT_SECONDARY}; font-size: 12px; margin-right: 8px;"
                        )
                        self._scenario_filter_toggle = ui.toggle(
                            options=["All"], value="All",
                        ).props(
                            f'dense color=primary toggle-color=primary'
                        ).on_value_change(
                            lambda e: setattr(self, "_scenario_filter", e.value)
                        )

                    # ---- Strategy table (one row per strategy, symbol) ----
                    columns = [
                        {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left"},
                        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                        {"name": "status", "label": "Status", "field": "status", "align": "center"},
                        # Warming column: how many entry conditions have
                        # observed=None (indicator not yet warmed). Shows
                        # "N/M" in yellow while warming, "—" when all ready.
                        {"name": "warming", "label": "Warming", "field": "warming", "align": "center"},
                        {"name": "scenarios", "label": "Scenarios", "field": "scenarios", "align": "left"},
                        {"name": "spread", "label": "Spread (bps)", "field": "spread", "align": "right"},
                        # Required margin if a signal fires (from heartbeat).
                        # Values are currency-agnostic (native account unit).
                        {"name": "margin", "label": "Margin", "field": "margin", "align": "right"},
                        {"name": "last_signal", "label": "Last Signal", "field": "last_signal", "align": "left"},
                        # Conditions column: renders every entry condition
                        # + filter state as coloured chips.
                        {"name": "conditions", "label": "Conditions", "field": "conditions", "align": "left"},
                    ]
                    strat_table = ui.table(
                        columns=columns, rows=[], row_key="strategy_symbol",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    strat_table.add_slot("body-cell-status", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.status === 'RUNNING' ? '""" + GREEN + r"""'
                                     : props.row.status === 'PAUSED' || props.row.status === 'STALE' ? '""" + YELLOW + r"""'
                                     : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.status }}</span>
                        </q-td>
                    """)
                    strat_table.add_slot("body-cell-warming", r"""
                        <q-td :props="props" style="text-align: center;">
                            <span :style="{
                                color: props.row.warming_active ? '""" + YELLOW + r"""' : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: props.row.warming_active ? 'bold' : 'normal',
                                fontSize: '12px',
                            }">{{ props.row.warming }}</span>
                        </q-td>
                    """)
                    # Spread cell: renders one value per scenario data
                    # source so operators see OANDA/Dukascopy side-by-side
                    # for the same (strategy, symbol) row. Expected shape:
                    # props.row.spread_chips = [{broker, bps, color}].
                    # Fixed height matches the Conditions cell so rows
                    # line up visually.
                    strat_table.add_slot("body-cell-spread", r"""
                        <q-td :props="props" style="vertical-align: top; padding: 4px 8px;">
                            <div style="display: flex; flex-direction: column; gap: 2px; font-size: 11px; height: 120px; overflow-y: auto;">
                                <span v-for="s in props.row.spread_chips"
                                      :key="s.broker"
                                      :style="{
                                        color: s.color,
                                        whiteSpace: 'nowrap',
                                      }">{{ s.broker }}: {{ s.bps }}</span>
                            </div>
                        </q-td>
                    """)
                    # Scenarios cell: one pill per scenario this row
                    # routes to. Pill colour reflects whether THAT
                    # scenario's per-scenario spread gate would pass on
                    # the current quote (green=pass, red=blocked,
                    # grey=no gate or no quote yet). Hover the pill
                    # for the gate details (data source, bps, threshold).
                    # Fixed height to match adjacent chip cells.
                    strat_table.add_slot("body-cell-scenarios", r"""
                        <q-td :props="props" style="vertical-align: top; padding: 4px 8px;">
                            <div style="display: flex; flex-wrap: wrap; gap: 3px; font-size: 10px; height: 120px; overflow-y: auto;">
                                <span v-for="s in props.row.scenario_chips"
                                      :key="s.id"
                                      :title="s.title"
                                      :style="{
                                        color: s.color,
                                        border: '1px solid ' + s.color,
                                        borderRadius: '3px',
                                        padding: '0px 4px',
                                        whiteSpace: 'nowrap',
                                        height: 'fit-content',
                                      }">{{ s.id }}</span>
                            </div>
                        </q-td>
                    """)
                    # Conditions cell: each condition becomes a small
                    # coloured span. `condition_chips` is an array of
                    # {label, color} items that we render inline. Vue
                    # v-for directly over props.row.condition_chips so
                    # the DOM stays lightweight — one span per condition.
                    # Fixed cell height + inner scroll keeps row height
                    # stable across heartbeats even when one strategy
                    # has 8 chips and another has 3.
                    strat_table.add_slot("body-cell-conditions", r"""
                        <q-td :props="props" style="vertical-align: top; padding: 4px 8px;">
                            <div style="display: flex; flex-wrap: wrap; gap: 4px; font-size: 11px; line-height: 1.4; height: 120px; overflow-y: auto;">
                                <span v-for="chip in props.row.condition_chips"
                                      :key="chip.label"
                                      :style="{
                                        color: chip.color,
                                        border: '1px solid ' + chip.color,
                                        borderRadius: '3px',
                                        padding: '1px 5px',
                                        whiteSpace: 'nowrap',
                                        height: 'fit-content',
                                      }">{{ chip.label }}</span>
                            </div>
                        </q-td>
                    """)

                    # ---- Open Trades table ----
                    ui.label("Open Trades").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    # sortable=True on every column gives us click-to-sort
                    # headers via Quasar's built-in QTable sort. Sort keys
                    # need to be comparable — for numerics that means the
                    # raw value (e.g. pnl_raw, move_pct_raw), for strings
                    # the display value is fine. `sort` props are expressed
                    # via :sort-method slot only when we need custom order;
                    # Quasar default compares `field` lexically, which
                    # works for our strings and ordered numeric formatting
                    # (prices with consistent decimals). For P&L and
                    # move%, we route the sort through the _raw numeric
                    # field to avoid string-ordering "10 < 2" surprises.
                    ot_columns = [
                        {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left", "sortable": True},
                        # Execution venue (e.g. "oanda-practice", "ftmo-mt5").
                        # Distinct from scenario_id/account_id (one broker
                        # can hold multiple accounts). Blank for pre-2026-04-22
                        # history — rendered as "—".
                        {"name": "broker", "label": "Broker", "field": "broker", "align": "left", "sortable": True},
                        {"name": "pair", "label": "Pair", "field": "pair", "align": "left", "sortable": True},
                        {"name": "direction", "label": "Direction", "field": "direction", "align": "center", "sortable": True},
                        # Entry time is sortable numerically via the hidden
                        # entry_time_raw field (epoch seconds) — display
                        # shows HH:MM:SS, hover tooltip shows full date.
                        {"name": "entry_time", "label": "Entry Time", "field": "entry_time", "align": "left", "sortable": True, "sort": "numeric"},
                        {"name": "entry_price", "label": "Entry", "field": "entry_price", "align": "right", "sortable": True},
                        {"name": "current_price", "label": "Current", "field": "current_price", "align": "right", "sortable": True},
                        # Entry:Current as a percent move: ((current − entry) /
                        # entry) × 100. Sign is the direction of the move,
                        # NOT of P&L — a short in profit shows negative %
                        # here (price dropped) but green in the P&L column.
                        {"name": "move_pct", "label": "Move %", "field": "move_pct", "align": "right", "sortable": True, "sort": "numeric"},
                        {"name": "stop_loss", "label": "SL", "field": "stop_loss", "align": "right", "sortable": True},
                        {"name": "take_profit", "label": "TP", "field": "take_profit", "align": "right", "sortable": True},
                        # Unrealised bps — from heartbeat position snapshot.
                        # Primary view of open trade performance (bps).
                        {"name": "unrealised_bps", "label": "bps", "field": "unrealised_bps", "align": "right", "sortable": True, "sort": "numeric"},
                        # Live unrealised P&L — price delta × qty − RT cost
                        # estimate (2× opener commission). Colour-coded
                        # green/red; updates on every tick via the UI timer.
                        {"name": "pnl", "label": "P&L", "field": "pnl", "align": "right", "sortable": True},
                        {"name": "timeout", "label": "Timeout", "field": "timeout", "align": "center", "sortable": True},
                        # TradingView chart link per row — quickest way to
                        # eyeball entry/TP/SL against current action
                        # without cluttering the GUI with inline plots.
                        {"name": "chart", "label": "Chart", "field": "chart", "align": "center"},
                    ]
                    ot_rows = [{
                        "key": "_empty",
                        "strategy": "--",
                        "broker": "--",
                        "pair": "No open trades",
                        "direction": "--",
                        "entry_price": "--",
                        "current_price": "--",
                        "move_pct": "--",
                        "move_pct_raw": 0.0,
                        "stop_loss": "--",
                        "take_profit": "--",
                        "pnl": "--",
                        "pnl_raw": 0.0,
                        "timeout": "--",
                        "pnl_positive": True,
                        "chart_url": "",
                    }]
                    open_trades_table = ui.table(
                        columns=ot_columns, rows=ot_rows, row_key="key",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    open_trades_table.add_slot("body-cell-direction", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.direction === 'BUY' ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.direction }}</span>
                        </q-td>
                    """)
                    open_trades_table.add_slot("body-cell-current_price", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_positive ? '""" + GREEN + r"""' : '""" + RED + r"""'
                            }">{{ props.row.current_price }}</span>
                        </q-td>
                    """)
                    open_trades_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.pnl_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)
                    # Move % cell: colour tracks the P&L sign, not the
                    # raw % sign. A short in profit shows a negative %
                    # in green; a long in profit shows positive % in green.
                    open_trades_table.add_slot("body-cell-move_pct", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_positive ? '""" + GREEN + r"""'
                                     : '""" + RED + r"""',
                            }">{{ props.row.move_pct }}</span>
                        </q-td>
                    """)
                    open_trades_table.add_slot("body-cell-unrealised_bps", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.bps_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.bps_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.unrealised_bps }}</span>
                        </q-td>
                    """)
                    # Chart cell: anchor to TradingView's chart with the
                    # OANDA symbol prefilled. Opens in new tab so the
                    # dashboard stays focused. Exchange prefix `OANDA:`
                    # chosen because TradingView has the OANDA feed
                    # default-available; falls back gracefully.
                    open_trades_table.add_slot("body-cell-chart", r"""
                        <q-td :props="props">
                            <a v-if="props.row.chart_url"
                               :href="props.row.chart_url"
                               target="_blank"
                               style="color: """ + BLUE + r""";
                                      text-decoration: underline;">
                                open ↗
                            </a>
                            <span v-else style="color: """ + TEXT_SECONDARY + r""";">--</span>
                        </q-td>
                    """)

                    # ---- P&L chart (full width, top) ----
                    chart_fig = {
                        "data": [{
                            "x": [], "y": [],
                            "type": "scatter", "mode": "lines",
                            "line": {"color": PNL_LINE_COLOUR, "width": 2},
                            "name": "Cumulative P&L (bps)",
                        }],
                        "layout": {
                            "paper_bgcolor": BG_DARK,
                            "plot_bgcolor": BG_PANEL,
                            "font": {"color": TEXT_PRIMARY},
                            "margin": {"l": 50, "r": 20, "t": 30, "b": 30},
                            "xaxis": {"showticklabels": False, "gridcolor": "#32324a"},
                            "yaxis": {"title": "bps", "gridcolor": "#32324a"},
                            "title": {"text": "Cumulative P&L (bps, trader activity only)", "font": {"size": 14}},
                            "showlegend": False,
                        },
                    }
                    pnl_chart = ui.plotly(chart_fig).classes("w-full mt-4").style(
                        f"height: {CHART_HEIGHT}px;"
                    )

                    # ---- Past Trades table (full width, below chart) ----
                    # Mirror of Open Trades but rows are CLOSED trades
                    # with realised P&L. Populated from live fills that
                    # net against existing positions AND seeded on
                    # startup from QuestDB history.
                    ui.label("Past Trades").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    # SL/TP columns show the original opener-side levels
                    # so the operator can eyeball at a glance whether the
                    # exit price matches the SL/TP or was a strategy-
                    # initiated close. For timeout/strategy exits SL and
                    # TP are still displayed — they're the targets the
                    # trade NEVER hit.
                    rt_columns = [
                        {"name": "strategy", "label": "Strategy", "field": "strategy", "align": "left", "sortable": True},
                        {"name": "broker", "label": "Broker", "field": "broker", "align": "left", "sortable": True},
                        {"name": "pair", "label": "Pair", "field": "pair", "align": "left", "sortable": True},
                        {"name": "direction", "label": "Direction", "field": "direction", "align": "center", "sortable": True},
                        {"name": "entry_time", "label": "Entry Time", "field": "entry_time", "align": "left", "sortable": True, "sort": "numeric"},
                        {"name": "entry_price", "label": "Entry", "field": "entry_price", "align": "right", "sortable": True},
                        {"name": "stop_loss", "label": "SL", "field": "stop_loss", "align": "right", "sortable": True},
                        {"name": "take_profit", "label": "TP", "field": "take_profit", "align": "right", "sortable": True},
                        {"name": "exit_time", "label": "Exit Time", "field": "exit_time", "align": "left", "sortable": True, "sort": "numeric"},
                        {"name": "exit_price", "label": "Exit", "field": "exit_price", "align": "right", "sortable": True},
                        {"name": "geometry", "label": "MFE:TP:Act:SL:MAE (bps)", "field": "geometry", "align": "right", "sortable": False},
                        # Net bps per trade — equal-weighted, currency-agnostic primary view.
                        {"name": "net_bps", "label": "bps", "field": "net_bps", "align": "right", "sortable": True, "sort": "numeric"},
                        {"name": "pnl_pct", "label": "P&L %", "field": "pnl_pct", "align": "right", "sortable": True, "sort": "numeric"},
                        {"name": "pnl", "label": "P&L", "field": "pnl", "align": "right", "sortable": True, "sort": "numeric"},
                    ]
                    recent_trades_table = ui.table(
                        columns=rt_columns, rows=[], row_key="key",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    recent_trades_table.add_slot("body-cell-direction", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.direction === 'BUY' ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.direction }}</span>
                        </q-td>
                    """)
                    recent_trades_table.add_slot("body-cell-net_bps", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.pnl_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.net_bps }}</span>
                        </q-td>
                    """)
                    recent_trades_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.pnl_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)
                    recent_trades_table.add_slot("body-cell-pnl_pct", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.pnl_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                            }">{{ props.row.pnl_pct }}</span>
                        </q-td>
                    """)
                    recent_trades_table.add_slot("body-cell-geometry", r"""
                        <q-td :props="props">
                            <span :style="{
                                fontFamily: 'monospace',
                                fontSize: '12px'
                            }">{{ props.row.geometry }}</span>
                        </q-td>
                    """)
                    recent_trades_table.add_slot("body-cell-exit_price", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.exit_reason === 'tp' ? '""" + GREEN + r"""'
                                     : props.row.exit_reason === 'sl' ? '""" + RED + r"""'
                                     : '""" + TEXT_PRIMARY + r"""'
                            }">{{ props.row.exit_price }}</span>
                        </q-td>
                    """)

                    # Legacy `trade_log` label retired — the table above
                    # replaces it. Keep the variable so the update path
                    # doesn't break; None means "no text label to write".
                    trade_log = None
              else:
                  scen_table = strat_table = open_trades_table = None
                  pnl_chart = trade_log = recent_trades_table = None
                  chart_fig = None

              # ================ FTMO MT5 TAB ================
              if ftmo_tab is not None:
                with ui.tab_panel(ftmo_tab):
                    with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                        f"background-color: {BG_PANEL}; border-radius: 6px;"
                    ):
                        ftmo_status_label = ui.label("MT5 Status: checking...").style(
                            f"color: {YELLOW}; font-weight: bold;"
                        )
                        ui.label(cfg.ftmo.account_summary).style(
                            f"color: {TEXT_SECONDARY};"
                        )
                        ui.label(cfg.ftmo.ea_summary).style(
                            f"color: {TEXT_SECONDARY};"
                        )

                    ui.label("FTMO Trades").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    ftmo_trade_columns = [
                        {"name": "time", "label": "Time", "field": "time", "align": "left"},
                        {"name": "type", "label": "Type", "field": "type", "align": "center"},
                        {"name": "symbol", "label": "Symbol", "field": "symbol", "align": "left"},
                        {"name": "lots", "label": "Lots", "field": "lots", "align": "right"},
                        {"name": "price", "label": "Price", "field": "price", "align": "right"},
                        {"name": "sl", "label": "SL", "field": "sl", "align": "right"},
                        {"name": "tp", "label": "TP", "field": "tp", "align": "right"},
                        {"name": "pnl", "label": "P&L", "field": "pnl", "align": "right"},
                        {"name": "comment", "label": "Comment", "field": "comment", "align": "left"},
                    ]
                    ftmo_trade_table = ui.table(
                        columns=ftmo_trade_columns, rows=[], row_key="time",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    ftmo_trade_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl }}</span>
                        </q-td>
                    """)
                    ftmo_trade_table.add_slot("body-cell-type", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.type === 'SELL' ? '""" + RED + r"""' : '""" + GREEN + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.type }}</span>
                        </q-td>
                    """)

                    with ui.row().classes("w-full gap-4 mt-4"):
                        with ui.card().classes("flex-grow").style(
                            f"height: 400px; background-color: {BG_PANEL}; overflow-y: auto;"
                        ):
                            ui.label("EA Log").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            ftmo_log_label = ui.label("Loading...").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 12px;"
                            )
                        with ui.card().style(
                            f"width: 300px; height: 400px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Challenge Progress").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            ftmo_progress_label = ui.label("").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )

                    def update_ftmo():
                        """Read MT5 logs and update the FTMO tab (every 5s)."""
                        import glob
                        mt5_dir = os.path.expanduser(cfg.ftmo.mt5_dir)

                        # Check if MT5 is running
                        mt5_running = False
                        try:
                            import subprocess
                            result = subprocess.run(
                                ["pgrep", "-f", "terminal64.exe"],
                                capture_output=True, text=True,
                            )
                            mt5_running = result.returncode == 0
                        except Exception:
                            pass

                        if mt5_running:
                            ftmo_status_label.set_text("MT5 Status: RUNNING")
                            ftmo_status_label.style(replace=f"color: {GREEN}; font-weight: bold;")
                        else:
                            ftmo_status_label.set_text("MT5 Status: OFFLINE")
                            ftmo_status_label.style(replace=f"color: {RED}; font-weight: bold;")

                        log_dir = os.path.join(mt5_dir, "logs")
                        log_files = sorted(glob.glob(os.path.join(log_dir, "*.log")))
                        ea_lines = []
                        trade_entries = []

                        if log_files:
                            try:
                                with open(log_files[-1], "rb") as f:
                                    content = f.read().decode("utf-16-le", errors="ignore")
                                for line in content.split("\n"):
                                    line = line.strip()
                                    if not line or len(line) < 5:
                                        continue
                                    if any(k in line for k in [
                                        "Expert", "ENTRY SIGNAL", "SELL", "BUY",
                                        "Order", "Deal", "Trade", "profit",
                                        "error", "GoldDonch", "automat",
                                    ]):
                                        ea_lines.append(line)
                                    if "SELL opened" in line or "BUY opened" in line:
                                        trade_entries.append({
                                            "time": line[:12].strip(),
                                            "type": "SELL" if "SELL" in line else "BUY",
                                            "symbol": "XAUUSD",
                                            "lots": "6.0",
                                            "price": "--",
                                            "sl": "--",
                                            "tp": "--",
                                            "pnl": "--",
                                            "pnl_raw": 0,
                                            "comment": line[40:].strip() if len(line) > 40 else "",
                                        })
                            except Exception as e:
                                ea_lines = [f"Error reading log: {e}"]

                        if ea_lines:
                            ftmo_log_label.set_text("\n".join(ea_lines[-40:]))
                        else:
                            ftmo_log_label.set_text(
                                "No EA activity yet.\n"
                                "(MT5 log file at " + log_dir + ")"
                            )

                        ftmo_trade_table.rows = trade_entries if trade_entries else []
                        ftmo_trade_table.update()

                        ch = cfg.ftmo.challenge
                        ftmo_progress_label.set_text(
                            f"Account: £{ch['account_gbp']:,}\n"
                            f"Target:  £{ch['target_gbp']:,} "
                            f"({ch['target_gbp']/ch['account_gbp']*100:.0f}%)\n"
                            f"Max DD:  £{ch['max_dd_gbp']:,} "
                            f"({ch['max_dd_gbp']/ch['account_gbp']*100:.0f}%)\n"
                            f"Daily:   £{ch['daily_dd_gbp']:,} "
                            f"({ch['daily_dd_gbp']/ch['account_gbp']*100:.0f}%)\n"
                            f"Min days: {ch['min_days']}\n"
                            f"Period:  {ch['period_days']} days\n\n"
                            f"Lot size: {ch['lot_size']}\n"
                            f"Expected: £{ch['expected_pnl']:,} / {ch['period_days']}d\n"
                            f"Max DD:   £{ch['expected_dd']:,}\n\n"
                            f"Trades today: {len(trade_entries)}\n"
                            f"Status: {'Active' if mt5_running else 'OFFLINE'}"
                        )

                    ui.timer(5.0, update_ftmo)

              # ================ ARBITRAGE TAB ================
              if arb_tab is not None:
                with ui.tab_panel(arb_tab):
                    with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                        f"background-color: {BG_PANEL}; border-radius: 6px;"
                    ):
                        arb_status_label = ui.label("Arb Scanner: waiting for first scan…").style(
                            f"color: {YELLOW}; font-weight: bold;"
                        )
                        arb_summary_label = ui.label("").style(f"color: {TEXT_SECONDARY};")
                        arb_pnl_label = ui.label("").style(f"color: {TEXT_SECONDARY};")

                    # ===== Exchange Balances =====
                    ui.label("Exchange Balances").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 14px;"
                    )
                    balance_columns = [
                        {"name": "exchange", "label": "Exchange", "field": "exchange", "align": "left"},
                        {"name": "balance", "label": "Total : (Available)/(Locked)", "field": "balance", "align": "left"},
                    ]
                    balance_rows = [
                        {"exchange": "Betfair", "balance": "— : (—)/(—)", "error": None, "stale": False, "last_fetched": ""},
                        {"exchange": "Matchbook", "balance": "— : (—)/(—)", "error": None, "stale": False, "last_fetched": ""},
                    ]
                    balance_table = ui.table(
                        columns=balance_columns, rows=balance_rows, row_key="exchange",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

                    # Style stale + error rows with grey text and tooltips
                    balance_table.add_slot("body-cell-balance", r"""
                        <q-td :props="props" :style="{
                            color: (props.row.stale || props.row.error) ? '""" + TEXT_SECONDARY + r"""' : '""" + TEXT_PRIMARY + r"""',
                            opacity: (props.row.stale || props.row.error) ? 0.6 : 1.0
                        }">
                            <div :title="props.row.error || (props.row.stale ? 'Stale: ' + props.row.last_fetched : '')">
                                {{ props.row.balance }}
                            </div>
                        </q-td>
                    """)

                    # ===== HERO: Profitable Arbs =====
                    # The point of this whole tab. Sortable by P&L (default)
                    # or by Age so newly-opened arbs jump to the top.
                    ui.label("⭐ Profitable Arbs (taking these would make money now)").classes("mt-2").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 18px;"
                    )
                    arb_opp_columns = [
                        {"name": "fresh",      "label": "",           "field": "fresh",      "align": "center", "sortable": True},
                        # Mode = PM (pre-match, port 5562) or LIVE (in-play, 5563)
                        {"name": "mode",       "label": "Mode",       "field": "mode",       "align": "center", "sortable": True},
                        {"name": "sport",      "label": "Sport",      "field": "sport",      "align": "left",   "sortable": True},
                        {"name": "event",      "label": "Fixture",    "field": "event",      "align": "left",   "sortable": True},
                        {"name": "market",     "label": "Market",     "field": "market",     "align": "left",   "sortable": True},
                        {"name": "selection",  "label": "Selection",  "field": "selection",  "align": "left",   "sortable": True},
                        # "Back" and "Lay" show the LOCKED odds + size from
                        # first detection — what we'd have clipped acting then.
                        {"name": "back",       "label": "Back",       "field": "back",       "align": "right",  "sortable": False},
                        {"name": "lay",        "label": "Lay",        "field": "lay",        "align": "right",  "sortable": False},
                        {"name": "stake",      "label": "Stake",      "field": "stake_num",  "align": "right",  "sortable": True},
                        # "Capital" = stake + lay liability — the actual
                        # money you'd need to deploy across both exchanges
                        # to take this arb. Stake alone hides the lay
                        # liability that funds the other leg, so high-odds
                        # arbs look cheap until you sort by Capital.
                        {"name": "capital",    "label": "Capital £",  "field": "capital_num","align": "right",  "sortable": True},
                        # Net % is already pnl / Capital (ROC). High Net %
                        # + low Capital = ideal. High Net % + huge Capital
                        # = the trap (4% on £130 = £5 ties up most of bank).
                        {"name": "net_margin", "label": "Net %",      "field": "net_num",    "align": "right",  "sortable": True},
                        {"name": "pnl",        "label": "Sim P/L",    "field": "pnl_num",    "align": "right",  "sortable": True},
                        # "Live" shows how the arb has drifted since detection:
                        # +£1.20 = improved, −£3.40 = slippage. Lets the user
                        # see at a glance whether the headline number is still
                        # available or if they missed the window.
                        {"name": "live",       "label": "Drift £",    "field": "drift_num",  "align": "right",  "sortable": True},
                        {"name": "age",        "label": "Age",        "field": "age_num",    "align": "right",  "sortable": True},
                        {"name": "confidence", "label": "Match %",    "field": "confidence", "align": "center", "sortable": True},
                    ]
                    arb_opp_table = ui.table(
                        columns=arb_opp_columns, rows=[],
                        row_key="opp_key",
                        pagination={"rowsPerPage": 25, "sortBy": "pnl", "descending": True},
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

                    # NEW badge — green pill for is_new, dim for older
                    arb_opp_table.add_slot("body-cell-fresh", r"""
                        <q-td :props="props">
                            <span v-if="props.row.is_new" :style="{
                                color: 'white', backgroundColor: '""" + GREEN + r"""',
                                padding: '2px 8px', borderRadius: '10px',
                                fontSize: '11px', fontWeight: 'bold'
                            }">NEW</span>
                            <span v-else :style="{
                                color: '""" + TEXT_SECONDARY + r"""',
                                fontSize: '11px'
                            }">{{ '×' + props.row.consecutive }}</span>
                        </q-td>
                    """)
                    # P/L coloured + signed
                    arb_opp_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ '£' + (props.row.pnl_num >= 0 ? '+' : '') + props.row.pnl_num.toFixed(2) }}</span>
                        </q-td>
                    """)
                    arb_opp_table.add_slot("body-cell-net_margin", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.net_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ (props.row.net_num >= 0 ? '+' : '') + props.row.net_num.toFixed(2) + '%' }}</span>
                        </q-td>
                    """)
                    arb_opp_table.add_slot("body-cell-capital", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.capital_num > 100 ? '""" + RED + r"""' :
                                       props.row.capital_num > 30  ? '""" + YELLOW + r"""' :
                                                                      '""" + TEXT_PRIMARY + r"""',
                                fontWeight: 'bold'
                            }">£{{ props.row.capital_num.toFixed(0) }}</span>
                        </q-td>
                    """)
                    arb_opp_table.add_slot("body-cell-stake", r"""
                        <q-td :props="props">£{{ props.row.stake_num.toFixed(2) }}</q-td>
                    """)
                    arb_opp_table.add_slot("body-cell-age", r"""
                        <q-td :props="props">{{ props.row.age_num < 60 ? props.row.age_num.toFixed(0) + 's' : (props.row.age_num / 60).toFixed(1) + 'm' }}</q-td>
                    """)
                    # Drift coloured: positive = arb improved, negative = slippage
                    arb_opp_table.add_slot("body-cell-live", r"""
                        <q-td :props="props">
                            <span v-if="props.row.drift_num === null || props.row.drift_num === 0" :style="{
                                color: '""" + TEXT_SECONDARY + r"""'
                            }">—</span>
                            <span v-else :style="{
                                color: props.row.drift_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'normal', fontSize: '12px'
                            }">{{ (props.row.drift_num >= 0 ? '+' : '') + props.row.drift_num.toFixed(2) }}</span>
                        </q-td>
                    """)

                    with ui.row().classes("w-full gap-4 mt-4"):
                        with ui.card().classes("flex-grow").style(
                            f"min-height: 220px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Last Scan Stats").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            arb_stats_label = ui.label("waiting…").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )
                        with ui.card().style(
                            f"width: 320px; min-height: 220px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Cost Model").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            arb_costs_label = ui.label("").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )
                        with ui.card().style(
                            f"width: 320px; min-height: 220px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Coverage").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            arb_coverage_label = ui.label("").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )
                        with ui.card().style(
                            f"width: 320px; min-height: 220px; background-color: {BG_PANEL};"
                        ):
                            ui.label("Configuration").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            arb_config_label = ui.label("").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; "
                                f"font-family: monospace; font-size: 13px;"
                            )

                    def update_arb():
                        with dashboard._lock:
                            snap = dashboard._arb_scan
                            scans = dict(dashboard._arb_scans)
                            strategies = {
                                k: dict(v) for k, v in dashboard._strategies.items()
                            }
                            balance_envelope = dashboard._balance_arbitrage

                        # Update exchange balances panel
                        balance_new_rows = [
                            {"exchange": "Betfair", "balance": "— : (—)/(—)", "error": None, "stale": False, "last_fetched": ""},
                            {"exchange": "Matchbook", "balance": "— : (—)/(—)", "error": None, "stale": False, "last_fetched": ""},
                        ]
                        if balance_envelope:
                            exchanges_list = balance_envelope.get("exchanges") or []
                            for exch in exchanges_list:
                                exch_name = exch.get("exchange", "").lower()
                                display_name = "Betfair" if exch_name == "betfair" else "Matchbook" if exch_name == "matchbook" else None

                                if display_name:
                                    error = exch.get("error")
                                    stale = exch.get("stale", False)
                                    last_fetched = exch.get("last_fetched", "")

                                    if error:
                                        balance_str = "— : (—)/(—)"
                                    elif stale or not all(k in exch for k in ["total", "available", "locked"]):
                                        balance_str = "— : (—)/(—)"
                                    else:
                                        total = float(exch.get("total", 0))
                                        available = float(exch.get("available", 0))
                                        locked = float(exch.get("locked", 0))
                                        total_fmt = _fmt_currency(total, exch.get("currency", "GBP"))
                                        avail_fmt = _fmt_currency(available, exch.get("currency", "GBP"))
                                        locked_fmt = _fmt_currency(locked, exch.get("currency", "GBP"))
                                        balance_str = f"{total_fmt} : ({avail_fmt})/({locked_fmt})"

                                    # Find and update the matching row
                                    for row in balance_new_rows:
                                        if row["exchange"] == display_name:
                                            row["balance"] = balance_str
                                            row["error"] = error
                                            row["stale"] = stale
                                            row["last_fetched"] = last_fetched
                                            break

                        balance_table.rows = balance_new_rows
                        balance_table.update()

                        # Detect MB lockout from either publisher's heartbeat —
                        # status="errored" + activity mentions "lockout".
                        # Show prominently because the user can't act on stale
                        # data and needs to know recovery is automatic.
                        lockout_msgs = []
                        for sid in ("arbitrage", "arbitrage_live"):
                            info = strategies.get(sid) or {}
                            act = info.get("activity", "")
                            if (info.get("status") == "errored"
                                    and "lockout" in act.lower()):
                                tag = "PM" if sid == "arbitrage" else "LIVE"
                                lockout_msgs.append(f"{tag}: {act}")

                        try:
                            cfg_path = os.path.expanduser(cfg.arb.config_path)
                            with open(cfg_path) as _f:
                                _cfg = json.load(_f).get("scan", {})
                            # Sports moved out of config.json into per-sport
                            # YAML files in sports/ alongside the repo, so we
                            # discover them by directory listing now.
                            sports_dir = os.path.join(
                                os.path.dirname(cfg_path),
                                _cfg.get("sports_dir", "sports"),
                            )
                            try:
                                sport_files = sorted(
                                    f[:-4] for f in os.listdir(sports_dir)
                                    if f.endswith(".yml") or f.endswith(".yaml")
                                )
                                sports = ", ".join(sport_files) or "(none)"
                            except Exception:
                                sports = "(sports dir not found)"
                            arb_config_label.set_text(
                                f"Sports:        {sports}\n"
                                f"Stake:         £{_cfg.get('stake', '?')}\n"
                                f"Min margin:    {_cfg.get('min_margin', 0) * 100:.1f}%\n"
                                f"Min liquidity: £{_cfg.get('min_liquidity', '?')}\n"
                                f"Price depth:   {_cfg.get('price_depth', '?')} levels\n"
                            )
                        except Exception:
                            arb_config_label.set_text("(config not readable)")

                        # If a publisher is in MB lockout, surface it loudly
                        # at the top — overrides the generic "no opps" status.
                        if lockout_msgs:
                            arb_status_label.set_text(
                                "🚧 Matchbook Lockout — " + " | ".join(lockout_msgs)
                            )
                            arb_status_label.style(
                                replace=f"color: {RED}; font-weight: bold;"
                            )

                        if snap is None:
                            if not lockout_msgs:
                                arb_status_label.set_text(
                                    "Arb Scanner: waiting for first scan…"
                                )
                                arb_status_label.style(
                                    replace=f"color: {YELLOW}; font-weight: bold;"
                                )
                            arb_summary_label.set_text("")
                            arb_pnl_label.set_text("")
                            arb_opp_table.rows = []
                            arb_opp_table.update()
                            arb_stats_label.set_text("waiting…")
                            return

                        # Skip the generic status text when lockout already
                        # took the label — lockout is the more important signal.
                        if not lockout_msgs:
                            age = time.time() - snap.get("scan_completed_at", 0)
                            if age > 300:
                                col = RED
                                status_text = f"Arb Scanner: STALE (last scan {age:.0f}s ago)"
                            elif snap.get("profitable_count", 0) > 0:
                                col = GREEN
                                status_text = (
                                    f"Arb Scanner: ACTIVE — "
                                    f"{snap['profitable_count']} profitable opps"
                                )
                            else:
                                col = YELLOW
                                status_text = "Arb Scanner: ACTIVE — no opps right now"
                            arb_status_label.set_text(status_text)
                            arb_status_label.style(replace=f"color: {col}; font-weight: bold;")
                        else:
                            age = time.time() - snap.get("scan_completed_at", 0)
                        arb_summary_label.set_text(
                            f"{snap.get('matched_pairs', 0)} matched pairs from "
                            f"{snap.get('matchbook_events', 0)} MB / "
                            f"{snap.get('betfair_events', 0)} BF events  ·  "
                            f"scan {snap.get('duration_secs', 0):.1f}s "
                            f"({age:.0f}s ago)"
                        )
                        pnl = snap.get("total_simulated_pnl", 0.0)
                        pnl_col = GREEN if pnl > 0 else (RED if pnl < 0 else TEXT_SECONDARY)
                        arb_pnl_label.set_text(f"Total sim P&L: £{pnl:+.2f}")
                        arb_pnl_label.style(replace=f"color: {pnl_col}; font-weight: bold;")

                        # Merge opportunities from BOTH publishers (PM + LIVE)
                        # so the table shows everything in one place. Each row
                        # carries a Mode tag so users see at a glance which
                        # publisher surfaced it (and therefore which book to
                        # check for fillability).
                        rows = []
                        for sid_pub, snap_pub in scans.items():
                            mode_tag = "LIVE" if sid_pub == "arbitrage_live" else "PM"
                            for opp in snap_pub.get("opportunities", []):
                                if opp.get("net_margin", 0) <= 0:
                                    continue
                                rows.append({
                                    "opp_key": (
                                        f"{mode_tag}|{opp.get('event','')}|{opp.get('market','')}|"
                                        f"{opp.get('selection','')}|"
                                        f"{opp.get('back_exchange','')}|{opp.get('lay_exchange','')}"
                                    ),
                                    "mode": mode_tag,
                                    "is_new": opp.get("is_new", False),
                                    "consecutive": opp.get("consecutive_scans", 1),
                                    "sport": opp.get("sport", "").title(),
                                    "event": opp.get("event", ""),
                                    "market": opp.get("market", ""),
                                    "selection": opp.get("selection", ""),
                                    "back": (
                                        f"{opp.get('back_exchange','')[:2].upper()} "
                                        f"{opp.get('back_odds', 0):.2f} "
                                        f"(£{opp.get('back_size', 0):,.0f})"
                                    ),
                                    "lay": (
                                        f"{opp.get('lay_exchange','')[:2].upper()} "
                                        f"{opp.get('lay_odds', 0):.2f} "
                                        f"(£{opp.get('lay_size', 0):,.0f})"
                                    ),
                                    # Show BACK-LEG stake only (not back+lay_stake).
                                    # Back leg is what you front on the back
                                    # exchange; matches the back_size column
                                    # (available back liquidity). Lay stake
                                    # isn't "your money" — it's what the
                                    # counterparty puts up. What YOU put up
                                    # on the lay side is the LIABILITY, which
                                    # is rolled into the Capital column.
                                    "stake_num": opp.get("simulated_stake_back",
                                                         opp.get("simulated_stake", 0)),
                                    "capital_num": opp.get("capital_required", 0),
                                    "net_num":   round(opp.get("net_margin", 0) * 100, 2),
                                    "pnl_num":   round(opp.get("simulated_pnl", 0), 2),
                                    # Drift = current_pnl − locked_pnl. Positive
                                    # if odds moved in our favour since detection.
                                    "drift_num": round(opp.get("pnl_slippage", 0), 2),
                                    "age_num":   round(opp.get("age_seconds", 0), 0),
                                    "confidence": f"{opp.get('match_confidence', 0):.0f}",
                                })
                        arb_opp_table.rows = rows
                        arb_opp_table.update()

                        n_new = snap.get("new_opportunity_count", 0)
                        arb_stats_label.set_text(
                            f"Last completed:    {time.strftime('%H:%M:%S', time.localtime(snap.get('scan_completed_at', 0)))}\n"
                            f"Scan duration:     {snap.get('duration_secs', 0):.1f}s\n"
                            f"Matchbook events:  {snap.get('matchbook_events', 0)}\n"
                            f"Betfair events:    {snap.get('betfair_events', 0)}\n"
                            f"Matched pairs:     {snap.get('matched_pairs', 0)}\n"
                            f"All opportunities: {snap.get('opportunities_count', 0)}\n"
                            f"Profitable:        {snap.get('profitable_count', 0)}  ({n_new} new)\n"
                            f"Best net margin:   {snap.get('best_margin', 0) * 100:+.2f}%\n"
                            f"Total sim P&L:     £{pnl:+.2f}"
                        )

                        # Cost Model card — proves what's baked into P&L numbers
                        cm = snap.get("cost_model", {}) or {}
                        bf_rates = cm.get("betfair_rates_seen_this_scan", [])
                        bf_range = (
                            f"{min(bf_rates)*100:.1f}-{max(bf_rates)*100:.1f}%"
                            if bf_rates else "(none used)"
                        )
                        arb_costs_label.set_text(
                            f"Matchbook:    {cm.get('matchbook_commission_rate', 0)*100:.2f}% taker\n"
                            f"  → both winning AND losing sides\n"
                            f"Betfair:      5.0% default\n"
                            f"  → on net winnings only\n"
                            f"  → per-market rate via market_base_rate\n"
                            f"  → range this scan: {bf_range}\n"
                            f"\nStake config: £{cm.get('configured_stake_gbp', 0):.0f}\n"
                            f"Min margin:   {cm.get('min_margin_threshold', 0)*100:.1f}%\n"
                            f"Min liq:      £{cm.get('min_liquidity_threshold_gbp', 0)}"
                        )

                        # Coverage card — answers "what did we even look at?"
                        cov = snap.get("coverage", {}) or {}
                        per_sport = cov.get("sports_with_events", [])
                        lines = [
                            f"{s['sport']:<14}{s['matchbook']:>4} MB  {s['betfair']:>4} BF"
                            for s in per_sport
                        ]
                        yield_pct = cov.get("matched_pair_yield_pct", 0)
                        arb_coverage_label.set_text(
                            "\n".join(lines) +
                            f"\n\nMatched: {cov.get('matched_pair_count', 0)}"
                            f"  ({yield_pct}% of MB)"
                        )

                    # ===== DIAGNOSTICS — dig in to what's NOT working =====
                    ui.separator().classes("mt-6 mb-2")
                    ui.label("🔍 Diagnostics — every matched market we considered").style(
                        f"color: {TEXT_SECONDARY}; font-size: 14px; font-style: italic;"
                    )

                    # ---- Matched Pairs table ---------------------------------
                    # One row per matched bet (sport / fixture+selection /
                    # back odds + size on each exchange). Lets you eyeball the
                    # cross-exchange spread on every comparable selection in
                    # the slate without having to dig into the raw payload.
                    ui.label("All Matched Markets (no arb on these — see Back/Lay spread)").classes("mt-2").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 14px;"
                    )
                    # Columns are sortable by default in NiceGUI tables; numeric
                    # fields (delta_pct_num, pnl_num) sort numerically while
                    # display columns render the formatted strings via body
                    # slots so colour coding works without breaking sort.
                    arb_match_columns = [
                        {"name": "mode",      "label": "Mode",      "field": "mode",      "align": "center", "sortable": True},
                        {"name": "sport",     "label": "Sport",     "field": "sport",     "align": "left",   "sortable": True},
                        {"name": "bet",       "label": "Bet",       "field": "bet",       "align": "left",   "sortable": True},
                        {"name": "back",      "label": "Back",      "field": "back",      "align": "right",  "sortable": True},
                        {"name": "lay",       "label": "Lay",       "field": "lay",       "align": "right",  "sortable": True},
                        {"name": "stakes",    "label": "Stake B/L", "field": "stakes",    "align": "right",  "sortable": False},
                        {"name": "delta_pct", "label": "Delta",     "field": "delta_num", "align": "right",  "sortable": True},
                        {"name": "pnl",       "label": "Sim P/L",   "field": "pnl_num",   "align": "right",  "sortable": True},
                        # Staleness = magnitude of one-side-moved-other-didn't.
                        # Higher = arb just opened due to one book lagging.
                        {"name": "stale",     "label": "Stale",     "field": "stale_num", "align": "right",  "sortable": True},
                    ]
                    arb_match_table = ui.table(
                        columns=arb_match_columns, rows=[], row_key="bet_key",
                        pagination={"rowsPerPage": 25, "sortBy": "pnl", "descending": True},
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

                    # Body slots: delta and pnl render with sign + colour but
                    # the underlying field is the raw number so sort behaves.
                    arb_match_table.add_slot("body-cell-delta_pct", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.delta_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ (props.row.delta_num >= 0 ? '+' : '') + props.row.delta_num.toFixed(2) + '%' }}</span>
                        </q-td>
                    """)
                    arb_match_table.add_slot("body-cell-pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ '£' + (props.row.pnl_num >= 0 ? '+' : '') + props.row.pnl_num.toFixed(2) }}</span>
                        </q-td>
                    """)

                    def _exch_short(name: str) -> str:
                        # Two-letter prefix for compact cell display.
                        return (name or "")[:2].upper()

                    def update_matches():
                        with dashboard._lock:
                            scans = dict(dashboard._arb_scans)
                        if not scans:
                            arb_match_table.rows = []
                            arb_match_table.update()
                            return
                        rows = []
                        for sid_pub, snap_pub in scans.items():
                            mode_tag = "LIVE" if sid_pub == "arbitrage_live" else "PM"
                            for m in snap_pub.get("matches", []):
                                sport = (m.get("sport") or "").title()
                                fixture = m.get("fixture", "")
                                market = m.get("headline_market") or ""
                                for sel in m.get("arb_rows", []):
                                    # Each row is one comparable arb position:
                                    # back on one exchange, lay on the other,
                                    # stakes scaled to balance outcomes.
                                    back_cell = (
                                        f"{_exch_short(sel['back_exchange'])} "
                                        f"{sel['back_odds']:.2f} "
                                        f"(£{sel['back_size']:,.0f})"
                                    )
                                    lay_cell = (
                                        f"{_exch_short(sel['lay_exchange'])} "
                                        f"{sel['lay_odds']:.2f} "
                                        f"(£{sel['lay_size']:,.0f})"
                                    )
                                    stale = sel.get("staleness")
                                    rows.append({
                                        "bet_key":   f"{mode_tag}|{fixture}|{market}|{sel['selection']}|{sel['direction']}",
                                        "mode":      mode_tag,
                                        "sport":     sport,
                                        "bet":       f"{fixture} — {sel['selection']}",
                                        "back":      back_cell,
                                        "lay":       lay_cell,
                                        "stakes":    f"£{sel['stake_back']:.0f} / £{sel['stake_lay']:.2f}",
                                        "delta_num": round(sel["gross_margin"] * 100, 2),
                                        "pnl_num":   round(sel["sim_pnl"], 2),
                                        "stale_num": round(stale, 3) if stale is not None else None,
                                    })
                        arb_match_table.rows = rows
                        arb_match_table.update()

                    # ---- Dutch (cross-selection) arbs panel ------------------
                    # Each row = a complete dutching position across all
                    # selections of one market, where backing every outcome
                    # on best-of-MB-vs-BF returns a guaranteed profit.
                    # Distinct from same-selection back/lay; doesn't need
                    # any laying at all.
                    ui.label("Dutch Arbs (cross-selection back-back — separate from same-selection back/lay)").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 14px;"
                    )
                    arb_dutch_columns = [
                        {"name": "mode",       "label": "Mode",       "field": "mode",       "align": "center", "sortable": True},
                        {"name": "sport",      "label": "Sport",      "field": "sport",      "align": "left",  "sortable": True},
                        {"name": "fixture",    "label": "Fixture",    "field": "fixture",    "align": "left",  "sortable": True},
                        {"name": "market",     "label": "Market",     "field": "market",     "align": "left",  "sortable": True},
                        {"name": "legs",       "label": "Legs",       "field": "legs",       "align": "left",  "sortable": False},
                        {"name": "overround",  "label": "Overround",  "field": "overround_num", "align": "right", "sortable": True},
                        {"name": "min_pnl",    "label": "Min P/L (£)", "field": "min_pnl_num", "align": "right", "sortable": True},
                        {"name": "size_ok",    "label": "Fillable",   "field": "size_ok",    "align": "center", "sortable": True},
                    ]
                    arb_dutch_table = ui.table(
                        columns=arb_dutch_columns, rows=[], row_key="dutch_key",
                        pagination={"rowsPerPage": 25, "sortBy": "min_pnl", "descending": True},
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

                    arb_dutch_table.add_slot("body-cell-overround", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.overround_num < 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ (props.row.overround_num >= 0 ? '+' : '') + (props.row.overround_num * 100).toFixed(2) + '%' }}</span>
                        </q-td>
                    """)
                    arb_dutch_table.add_slot("body-cell-min_pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.min_pnl_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ '£' + (props.row.min_pnl_num >= 0 ? '+' : '') + props.row.min_pnl_num.toFixed(2) }}</span>
                        </q-td>
                    """)

                    def update_dutch():
                        with dashboard._lock:
                            scans = dict(dashboard._arb_scans)
                        if not scans:
                            arb_dutch_table.rows = []
                            arb_dutch_table.update()
                            return
                        rows = []
                        for sid_pub, snap_pub in scans.items():
                            mode_tag = "LIVE" if sid_pub == "arbitrage_live" else "PM"
                            for d in snap_pub.get("dutch_arbs", []):
                                # Compact "leg1 (BF 2.10) | leg2 (MB 2.08) | …"
                                legs = " | ".join(
                                    f"{leg['selection']} ({leg['back_exchange'][:2].upper()} "
                                    f"{leg['back_odds']:.2f})"
                                    for leg in (d.get("legs") or [])
                                )
                                rows.append({
                                    "dutch_key":     f"{mode_tag}|{d.get('fixture')}|{d.get('market_type')}|{d.get('market_name')}",
                                    "mode":          mode_tag,
                                    "sport":         (d.get("sport") or "").title(),
                                    "fixture":       d.get("fixture", ""),
                                    "market":        f"{d.get('market_type','')} ({d.get('market_name','')})",
                                    "legs":          legs,
                                    "overround_num": d.get("implied_overround", 0),
                                    "min_pnl_num":   d.get("min_pnl", 0),
                                    "size_ok":       "no" if d.get("size_constrained") else "yes",
                                })
                        arb_dutch_table.rows = rows
                        arb_dutch_table.update()

                    # ---- Trade Ledger ---------------------------------------
                    # Live tail of executor/live_trades.jsonl from the
                    # publisher — every fire decision (auto_fire AND manual
                    # fire CLI). Lets the user see at a glance what would
                    # have transmitted in dry-run mode and why anything
                    # got rejected.
                    ui.label("📒 Trade Ledger (last 50 fires — dry-run + live)").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 14px;"
                    )
                    arb_trade_columns = [
                        {"name": "time",      "label": "Time",       "field": "time",      "align": "center", "sortable": True},
                        {"name": "status",    "label": "Status",     "field": "status",    "align": "center", "sortable": True},
                        {"name": "sport",     "label": "Sport",      "field": "sport",     "align": "left",   "sortable": True},
                        {"name": "fixture",   "label": "Fixture",    "field": "fixture",   "align": "left",   "sortable": True},
                        {"name": "selection", "label": "Selection",  "field": "selection", "align": "left",   "sortable": True},
                        {"name": "back_leg",  "label": "Back",       "field": "back_leg",  "align": "right",  "sortable": False},
                        {"name": "lay_leg",   "label": "Lay",        "field": "lay_leg",   "align": "right",  "sortable": False},
                        {"name": "stake",     "label": "Stake £",    "field": "stake_num", "align": "right",  "sortable": True},
                        {"name": "liab",      "label": "Liab £",     "field": "liab_num",  "align": "right",  "sortable": True},
                        # Capital = stake + liability (back/lay) or
                        # total_stake (dutch). Mirrors the Profitable Arbs
                        # column so users see the £ each fired trade tied up.
                        {"name": "capital",   "label": "Capital £",  "field": "capital_num","align": "right",  "sortable": True},
                        # Net % at the placed stake — same metric as the
                        # Profitable Arbs Net % column. Tells you whether
                        # the auto-sized fire still made sense at this size.
                        {"name": "net_pct",   "label": "Net %",      "field": "net_num",   "align": "right",  "sortable": True},
                        {"name": "exp_pnl",   "label": "Exp P/L",    "field": "exp_num",   "align": "right",  "sortable": True},
                        {"name": "real_pnl",  "label": "Real P/L",   "field": "real_num",  "align": "right",  "sortable": True},
                        {"name": "note",      "label": "Note",       "field": "note",      "align": "left",   "sortable": False},
                    ]
                    arb_trade_table = ui.table(
                        columns=arb_trade_columns, rows=[],
                        row_key="trade_id",
                        pagination={"rowsPerPage": 25, "sortBy": "time", "descending": True},
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

                    # Status pill: colour-coded per outcome.
                    arb_trade_table.add_slot("body-cell-status", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: 'white',
                                backgroundColor:
                                    props.row.status === 'executed'    ? '""" + GREEN + r"""' :
                                    props.row.status === 'settled'     ? '""" + GREEN + r"""' :
                                    props.row.status === 'dry_run'     ? '""" + YELLOW + r"""' :
                                    props.row.status === 'rolled_back' ? '#FFA726' :
                                    props.row.status === 'rejected'    ? '""" + RED + r"""' :
                                                                          '""" + TEXT_SECONDARY + r"""',
                                padding: '2px 8px', borderRadius: '10px',
                                fontSize: '11px', fontWeight: 'bold',
                                textTransform: 'uppercase'
                            }">{{ props.row.status }}</span>
                        </q-td>
                    """)
                    arb_trade_table.add_slot("body-cell-stake", r"""
                        <q-td :props="props">£{{ props.row.stake_num.toFixed(2) }}</q-td>
                    """)
                    arb_trade_table.add_slot("body-cell-liab", r"""
                        <q-td :props="props">£{{ props.row.liab_num.toFixed(2) }}</q-td>
                    """)
                    # Capital colour-coded same as Profitable Arbs:
                    # red >£100, yellow >£30, neutral below — at-a-glance
                    # signal of which fires tied up serious bank.
                    arb_trade_table.add_slot("body-cell-capital", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.capital_num > 100 ? '""" + RED + r"""' :
                                       props.row.capital_num > 30  ? '""" + YELLOW + r"""' :
                                                                      '""" + TEXT_PRIMARY + r"""',
                                fontWeight: 'bold'
                            }">£{{ props.row.capital_num.toFixed(2) }}</span>
                        </q-td>
                    """)
                    arb_trade_table.add_slot("body-cell-net_pct", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.net_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ (props.row.net_num >= 0 ? '+' : '') + props.row.net_num.toFixed(2) + '%' }}</span>
                        </q-td>
                    """)
                    arb_trade_table.add_slot("body-cell-exp_pnl", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.exp_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ '£' + (props.row.exp_num >= 0 ? '+' : '') + props.row.exp_num.toFixed(2) }}</span>
                        </q-td>
                    """)
                    arb_trade_table.add_slot("body-cell-real_pnl", r"""
                        <q-td :props="props">
                            <span v-if="props.row.real_num === null" :style="{
                                color: '""" + TEXT_SECONDARY + r"""'
                            }">—</span>
                            <span v-else :style="{
                                color: props.row.real_num >= 0 ? '""" + GREEN + r"""' : '""" + RED + r"""',
                                fontWeight: 'bold'
                            }">{{ '£' + (props.row.real_num >= 0 ? '+' : '') + props.row.real_num.toFixed(2) }}</span>
                        </q-td>
                    """)

                    def update_trades():
                        # Trades come from EITHER publisher (PM / LIVE).
                        # Both read the same on-disk ledger so we just take
                        # whichever snapshot has them and dedupe by trade_id.
                        with dashboard._lock:
                            scans = dict(dashboard._arb_scans)
                        seen: dict[str, dict] = {}
                        for snap in scans.values():
                            for t in snap.get("trades") or []:
                                tid = t.get("trade_id")
                                if not tid or tid in seen:
                                    continue
                                seen[tid] = t

                        rows = []
                        for t in seen.values():
                            opened = t.get("opened_at") or ""
                            # Display HH:MM:SS only — ISO timestamp is too
                            # wide for the table and the date is "today".
                            time_str = opened[11:19] if len(opened) >= 19 else opened
                            note = t.get("error") or ""
                            if t.get("dry_run") and not note:
                                note = "DRY-RUN — no transmission"
                            # Dutch rows have no single back/lay leg —
                            # back_exchange field is already a summary
                            # like "3 legs · BF/MB/MB", lay_exchange is
                            # "—", and back_odds/lay_odds are 0.
                            if t.get("trade_type") == "dutch":
                                back_leg = t.get("back_exchange") or "dutch"
                                lay_leg  = "—"
                            else:
                                be = (t.get("back_exchange") or "")[:2].upper()
                                le = (t.get("lay_exchange") or "")[:2].upper()
                                back_leg = f"{be} @ {t.get('back_odds', 0):.2f}"
                                lay_leg  = f"{le} @ {t.get('lay_odds', 0):.2f}"
                            rows.append({
                                "trade_id":  t.get("trade_id") or opened,
                                "time":      time_str,
                                "status":    t.get("status") or "?",
                                "sport":     (t.get("sport") or "").title(),
                                "fixture":   (t.get("fixture") or "")[:35],
                                "selection": (t.get("selection") or "")[:25],
                                "back_leg":  back_leg,
                                "lay_leg":   lay_leg,
                                "stake_num": float(t.get("stake") or 0.0),
                                "liab_num":  float(t.get("liability") or 0.0),
                                # capital = stake + liab (back/lay) or stake (dutch)
                                "capital_num": float(t.get("capital") or 0.0),
                                # net_margin stored as fraction; * 100 → %
                                "net_num":   round(float(t.get("expected_net_margin") or 0.0) * 100, 2),
                                "exp_num":   float(t.get("expected_pnl") or 0.0),
                                "real_num":  t.get("realised_pnl"),
                                "note":      note,
                            })
                        # Newest first by time string (HH:MM:SS sorts correctly within a day).
                        rows.sort(key=lambda r: r["time"], reverse=True)
                        arb_trade_table.rows = rows
                        arb_trade_table.update()

                    ui.timer(2.0, update_arb)
                    ui.timer(2.0, update_matches)
                    ui.timer(2.0, update_dutch)
                    ui.timer(2.0, update_trades)

              # ================ VAULT TAB ================
              # KnowledgeVault repo-hook heartbeats. One row per repo,
              # keyed on repo name (latest commit overwrites). Activity
              # is the publisher's "OK: <repo> <sha>" / "FAIL: <reason>"
              # string; status drives the row colour.
              if vault_tab is not None:
                with ui.tab_panel(vault_tab):
                    with ui.row().classes("w-full items-center gap-6 px-4 py-2").style(
                        f"background-color: {BG_PANEL}; border-radius: 6px;"
                    ):
                        vault_status_label = ui.label(
                            "KnowledgeVault: waiting for first repo-hook heartbeat…"
                        ).style(f"color: {YELLOW}; font-weight: bold;")
                        vault_summary_label = ui.label("").style(
                            f"color: {TEXT_SECONDARY};"
                        )

                    vault_columns = [
                        {"name": "repo",     "label": "Repo",     "field": "repo",     "align": "left",  "sortable": True},
                        {"name": "status",   "label": "Status",   "field": "status",   "align": "center","sortable": True},
                        {"name": "sha",      "label": "SHA",      "field": "sha",      "align": "left",  "sortable": False},
                        {"name": "activity", "label": "Activity", "field": "activity", "align": "left",  "sortable": False},
                        {"name": "when",     "label": "When",     "field": "when",     "align": "right", "sortable": True},
                    ]
                    vault_table = ui.table(
                        columns=vault_columns, rows=[], row_key="repo",
                        pagination={"rowsPerPage": 25, "sortBy": "when", "descending": False},
                    ).classes("w-full mt-2").style(f"background-color: {BG_PANEL};")

                    # Status chip — green for running (OK), red for failed,
                    # yellow for anything else (future: "stale" if we start
                    # ageing out rows after, say, 7d).
                    vault_table.add_slot("body-cell-status", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: 'white',
                                backgroundColor: props.row.status === 'running' ? '""" + GREEN + r"""'
                                              : props.row.status === 'failed'  ? '""" + RED   + r"""'
                                              : '""" + YELLOW + r"""',
                                padding: '2px 8px', borderRadius: '10px',
                                fontSize: '11px', fontWeight: 'bold'
                            }">{{ props.row.status.toUpperCase() }}</span>
                        </q-td>
                    """)
                    # Monospace short sha so 7-char hashes line up nicely.
                    vault_table.add_slot("body-cell-sha", r"""
                        <q-td :props="props">
                            <span :style="{
                                fontFamily: 'monospace',
                                color: '""" + TEXT_SECONDARY + r"""'
                            }">{{ props.row.sha }}</span>
                        </q-td>
                    """)

                    def update_vault():
                        with dashboard._lock:
                            hooks = {k: dict(v) for k, v in dashboard._vault_hooks.items()}

                        if not hooks:
                            return  # keep the "waiting…" label

                        now = time.time()
                        rows = []
                        for repo, h in hooks.items():
                            age = now - h.get("_received", now)
                            rows.append({
                                "repo":     repo,
                                "status":   h.get("status", "unknown"),
                                "sha":      h.get("sha", ""),
                                "activity": h.get("activity", ""),
                                # Raw seconds for sorting; display string
                                # computed here so Vue doesn't have to
                                # (age column sorts on the numeric key).
                                "when":     _fmt_age(age),
                                "_age_s":   age,
                            })
                        rows.sort(key=lambda r: r["_age_s"])

                        vault_table.rows = rows
                        vault_table.update()

                        # Header: "N repos tracked | last activity Xs ago"
                        newest = min((r["_age_s"] for r in rows), default=0)
                        vault_status_label.set_text(
                            f"KnowledgeVault: {len(rows)} repo(s) tracked"
                        )
                        vault_status_label.style(
                            replace=f"color: {GREEN}; font-weight: bold;"
                        )
                        vault_summary_label.set_text(
                            f"last hook {_fmt_age(newest)} ago"
                        )

                    ui.timer(2.0, update_vault)

              # ================ CRYPTO TAB ================
              if crypto_tab is not None:
                with ui.tab_panel(crypto_tab):
                    ui.label("Crypto Paper-Trader").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )

                    # Top metrics
                    with ui.row().classes("w-full gap-4 mt-2"):
                        total_fills_card = ui.card().classes("flex-1").style(f"background-color: {BG_PANEL};")
                        with total_fills_card:
                            ui.label("Total Fills (Today)").style(f"color: {TEXT_SECONDARY}; font-size: 12px;")
                            fills_label = ui.label("0").style(f"color: {GREEN}; font-weight: bold; font-size: 20px;")

                        profit_card = ui.card().classes("flex-1").style(f"background-color: {BG_PANEL};")
                        with profit_card:
                            ui.label("Sum Profit ($)").style(f"color: {TEXT_SECONDARY}; font-size: 12px;")
                            profit_label = ui.label("$0.00").style(f"color: {GREEN}; font-weight: bold; font-size: 20px;")

                    # Per-strategy table
                    ui.label("Strategies").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    crypto_strat_columns = [
                        {"name": "spec_id", "label": "Spec ID", "field": "spec_id", "align": "left"},
                        {"name": "chains", "label": "Chains", "field": "chains", "align": "left"},
                        {"name": "freshness", "label": "Heartbeat Age", "field": "freshness", "align": "center"},
                        {"name": "halted", "label": "Halted", "field": "halted", "align": "center"},
                        {"name": "fills", "label": "Fills", "field": "fills", "align": "center"},
                        {"name": "last_fill", "label": "Last Fill", "field": "last_fill", "align": "center"},
                    ]
                    crypto_strat_table = ui.table(
                        columns=crypto_strat_columns, rows=[], row_key="spec_id"
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    crypto_strat_table.add_slot("body-cell-freshness", r"""
                        <q-td :props="props" :style="{
                            color: props.row.freshness_age > 30 ? '""" + RED + r"""' : '""" + TEXT_PRIMARY + r"""'
                        }">{{ props.row.freshness }}</q-td>
                    """)
                    crypto_strat_table.add_slot("body-cell-halted", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.halted ? '""" + RED + r"""' : '""" + GREEN + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.halted ? 'YES' : 'NO' }}</span>
                        </q-td>
                    """)

                    # Per-chain table
                    ui.label("Chains").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    crypto_chain_columns = [
                        {"name": "chain", "label": "Chain", "field": "chain", "align": "left"},
                        {"name": "block", "label": "Last Block", "field": "block", "align": "center"},
                        {"name": "block_age", "label": "Age", "field": "block_age", "align": "center"},
                        {"name": "active_specs", "label": "Active Specs", "field": "active_specs", "align": "left"},
                    ]
                    crypto_chain_table = ui.table(
                        columns=crypto_chain_columns, rows=[], row_key="chain"
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    crypto_chain_table.add_slot("body-cell-block_age", r"""
                        <q-td :props="props" :style="{
                            color: props.row.block_age_sec > 30 ? '""" + YELLOW + r"""' : '""" + TEXT_PRIMARY + r"""'
                        }">{{ props.row.block_age }}</q-td>
                    """)

                    # Recent fills
                    ui.label("Recent Fills").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    crypto_fills_columns = [
                        {"name": "timestamp", "label": "Time", "field": "timestamp", "align": "left"},
                        {"name": "spec_id", "label": "Spec ID", "field": "spec_id", "align": "left"},
                        {"name": "chain", "label": "Chain", "field": "chain", "align": "left"},
                        {"name": "bundle_hash", "label": "Bundle Hash", "field": "bundle_hash", "align": "left"},
                        {"name": "profit", "label": "Sim Profit ($)", "field": "profit", "align": "right"},
                        {"name": "status", "label": "Status", "field": "status", "align": "center"},
                    ]
                    crypto_fills_table = ui.table(
                        columns=crypto_fills_columns, rows=[], row_key="key"
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")
                    crypto_fills_table.add_slot("body-cell-profit", r"""
                        <q-td :props="props" :style="{
                            color: props.row.profit_raw > 0 ? '""" + GREEN + r"""' : '""" + RED + r"""'
                        }">{{ props.row.profit }}</q-td>
                    """)

                    # Update function for Crypto tab
                    def update_crypto():
                        from datetime import datetime as _dt
                        with dashboard._lock:
                            crypto_strategies = dict(dashboard._crypto_strategies)
                            crypto_fills = list(dashboard._crypto_fills)[:50]
                            crypto_chains = dict(dashboard._crypto_chains)

                        # Update metrics
                        total_fills = len(crypto_fills)
                        total_profit = sum(f.get("simulated_profit", 0) for f in crypto_fills)
                        fills_label.set_text(str(total_fills))
                        profit_label.set_text(f"${total_profit:.2f}")

                        # Update strategies table
                        strat_rows = []
                        for spec_id, spec_data in sorted(crypto_strategies.items()):
                            age_s = time.time() - spec_data.get("last_heartbeat", time.time())
                            strat_rows.append({
                                "spec_id": spec_id,
                                "chains": ", ".join(spec_data.get("chains", [])) or "—",
                                "freshness": _fmt_age(age_s),
                                "freshness_age": age_s,
                                "halted": spec_data.get("halted", False),
                                "fills": str(spec_data.get("fill_count", 0)),
                                "last_fill": (_dt.fromtimestamp(spec_data.get("last_fill_ts", 0)).strftime("%H:%M:%S")
                                             if spec_data.get("last_fill_ts") else "—"),
                            })
                        crypto_strat_table.rows = strat_rows or [{"spec_id": "—", "chains": "—", "freshness": "—", "halted": False, "fills": "0", "last_fill": "—"}]
                        crypto_strat_table.update()

                        # Update chains table
                        chain_rows = []
                        for chain, chain_data in sorted(crypto_chains.items()):
                            block_age_s = time.time() - chain_data.get("last_tick", time.time())
                            # Count active specs on this chain
                            active_specs = [s for s, d in crypto_strategies.items()
                                           if chain in d.get("chains", [])]
                            chain_rows.append({
                                "chain": chain,
                                "block": str(chain_data.get("number", "?")),
                                "block_age": _fmt_age(block_age_s),
                                "block_age_sec": block_age_s,
                                "active_specs": ", ".join(active_specs) if active_specs else "—",
                            })
                        crypto_chain_table.rows = chain_rows or [{"chain": "—", "block": "—", "block_age": "—", "active_specs": "—"}]
                        crypto_chain_table.update()

                        # Update fills table
                        fill_rows = []
                        for i, f in enumerate(crypto_fills):
                            fill_rows.append({
                                "key": f"{f.get('spec_id')}_{f.get('timestamp')}_{i}",
                                "timestamp": (_dt.fromtimestamp(f.get("timestamp", 0)).strftime("%H:%M:%S")
                                             if f.get("timestamp") else "—"),
                                "spec_id": f.get("spec_id", "?"),
                                "chain": f.get("chain", "?"),
                                "bundle_hash": f.get("bundle_hash", "?")[:12] + "..." if f.get("bundle_hash") else "—",
                                "profit": f"${f.get('simulated_profit', 0):.2f}",
                                "profit_raw": f.get("simulated_profit", 0),
                                "status": f.get("status", "?"),
                            })
                        crypto_fills_table.rows = fill_rows or [{"key": "_empty", "timestamp": "—", "spec_id": "—", "chain": "—", "bundle_hash": "—", "profit": "—", "status": "—"}]
                        crypto_fills_table.update()

                    ui.timer(1.0, update_crypto)

              # ================ VULN SEARCHER TAB ================
              if vuln_tab is not None:
                with ui.tab_panel(vuln_tab):
                    _VULN_SEV_COLORS = {
                        "critical": RED,
                        "high":     "#e0823c",
                        "medium":   YELLOW,
                        "low":      GREEN,
                        "info":     BLUE,
                        "unknown":  TEXT_SECONDARY,
                    }
                    _VULN_CATS = ["All", "vulnerability", "breach", "fine", "news",
                                  "advisory", "geopolitical", "financial"]
                    _VULN_SEVS = ["All", "critical", "high", "medium", "low",
                                  "info", "unknown"]

                    ui.label("Adverse Intelligence").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )

                    with ui.row().classes("w-full gap-4 items-end mt-2 flex-wrap"):
                        vuln_entity = ui.input(
                            label="Entity / vendor",
                            placeholder="fortinet, microsoft, …",
                        ).style("min-width: 200px;")
                        vuln_cat = ui.select(
                            _VULN_CATS, value="All", label="Category",
                        ).style("min-width: 140px;")
                        vuln_sev = ui.select(
                            _VULN_SEVS, value="All", label="Severity",
                        ).style("min-width: 120px;")
                        vuln_days = ui.number(
                            "Days back", value=30, min=1, max=730, step=1,
                        ).style("width: 100px;")
                        vuln_btn = ui.button("Search").style(
                            f"background-color: {BLUE}; color: white;"
                        )

                    vuln_status = ui.label("").style(
                        f"color: {TEXT_SECONDARY}; font-size: 12px; margin-top: 4px;"
                    )

                    vuln_table = ui.table(
                        columns=[
                            {"name": "published_at", "label": "Published",   "field": "published_at", "align": "left",  "sortable": True},
                            {"name": "source",       "label": "Source",      "field": "source",       "align": "left",  "sortable": True},
                            {"name": "category",     "label": "Category",    "field": "category",     "align": "left",  "sortable": True},
                            {"name": "severity",     "label": "Severity",    "field": "severity",     "align": "center","sortable": True},
                            {"name": "title",        "label": "Title",       "field": "title",        "align": "left",  "sortable": False},
                            {"name": "entities",     "label": "Entities",    "field": "entities",     "align": "left",  "sortable": False},
                            {"name": "url",          "label": "URL",         "field": "url",          "align": "left",  "sortable": False},
                        ],
                        rows=[],
                        row_key="row_key",
                        pagination={"rowsPerPage": 50},
                    ).classes("w-full mt-2")

                    vuln_table.add_slot("body-cell-severity", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.sev_color,
                                fontWeight: 'bold',
                                fontSize: '12px',
                                textTransform: 'uppercase',
                            }">{{ props.row.severity }}</span>
                        </q-td>
                    """)
                    vuln_table.add_slot("body-cell-url", r"""
                        <q-td :props="props">
                            <a :href="props.row.url" target="_blank"
                               :style="{color: '""" + BLUE + r"""', fontSize: '12px'}">
                                {{ props.row.url ? 'link' : '' }}
                            </a>
                        </q-td>
                    """)

                    def run_vuln_search():
                        import psycopg2
                        entity_val = (vuln_entity.value or "").strip()
                        cat_val    = vuln_cat.value or "All"
                        sev_val    = vuln_sev.value or "All"
                        days_val   = int(vuln_days.value or 30)

                        wheres = [f"published_at > dateadd('d', -{days_val}, now())"]
                        params: list = []
                        if entity_val:
                            wheres.append("entities LIKE %s")
                            params.append(f"%{entity_val.lower()}%")
                        if cat_val != "All":
                            wheres.append("category = %s")
                            params.append(cat_val)
                        if sev_val != "All":
                            wheres.append("severity = %s")
                            params.append(sev_val)

                        sql = (
                            "SELECT published_at, source, category, severity, "
                            "title, url, entities "
                            "FROM adverse_intel "
                            f"WHERE {' AND '.join(wheres)} "
                            "ORDER BY published_at DESC LIMIT 200"
                        )
                        try:
                            with psycopg2.connect(
                                host="localhost", port=8812,
                                user="admin", password="quest", dbname="qdb",
                            ) as conn:
                                with conn.cursor() as cur:
                                    cur.execute(sql, params or None)
                                    raw_rows = cur.fetchall()
                        except Exception as exc:
                            vuln_status.set_text(f"Query error: {exc}")
                            return

                        new_rows = []
                        for i, (pub, src, cat_r, sev_r, title, url, ents) in enumerate(raw_rows):
                            pub_str = (
                                pub.strftime("%Y-%m-%d %H:%M")
                                if hasattr(pub, "strftime") else str(pub)[:16]
                            )
                            new_rows.append({
                                "row_key":    str(i),
                                "published_at": pub_str,
                                "source":     src or "",
                                "category":   cat_r or "",
                                "severity":   sev_r or "",
                                "sev_color":  _VULN_SEV_COLORS.get(sev_r or "", TEXT_SECONDARY),
                                "title":      title or "",
                                "url":        url or "",
                                "entities":   (ents or "")[:100],
                            })
                        vuln_status.set_text(f"{len(new_rows)} result(s) — last {days_val}d")
                        vuln_table.rows = new_rows
                        vuln_table.update()

                    vuln_btn.on("click", lambda _: run_vuln_search())
                    ui.timer(0, run_vuln_search, once=True)

              # ================ QUICK SCREEN TAB ================
              if qs_tab is not None:
                with ui.tab_panel(qs_tab):
                    from .tabs.quick_screen import fetch_catalogue, build_quick_screen_tab
                    _qs_container = ui.column().classes("w-full")
                    _qs_loading = ui.label("Loading Quick Screen catalogue…").style(
                        f"color: {TEXT_SECONDARY}; font-style: italic;"
                    )

                    async def _init_qs_tab():
                        cat = await run.io_bound(fetch_catalogue)
                        _qs_loading.set_visibility(False)
                        with _qs_container:
                            if cat is None:
                                ui.label(
                                    "Quick Screen service unavailable at tcp://127.0.0.1:5560"
                                ).style(f"color: {RED}; font-weight: bold;")
                            else:
                                build_quick_screen_tab(cat)

                    ui.timer(0, _init_qs_tab, once=True)

              # ================ STA TRADES TAB ================
              # One row per lifecycle record from the latest STA heartbeat.
              # Mirrors Open Trades in layout; lifecycle-state-aware badges.
              if sta_trades_tab is not None:
                with ui.tab_panel(sta_trades_tab):
                    _STA_STATE_COLORS = {
                        "AUTHORED":        TEXT_SECONDARY,
                        "GATE_PENDING":    YELLOW,
                        "DISPATCHED":      BLUE,
                        "ACTIVE":          GREEN,
                        "MILESTONE_CHECK": YELLOW,
                        "PARTIAL_CLOSED":  YELLOW,
                        "CLOSED":          TEXT_SECONDARY,
                    }
                    ui.label("STA Active Trades").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    sta_trades_columns = [
                        # Short readable label derived client-side from spec_id
                        {"name": "label",        "label": "Trade",              "field": "label",        "align": "left",   "sortable": True},
                        {"name": "trade_type",   "label": "Type",               "field": "trade_type",   "align": "center", "sortable": True},
                        {"name": "state",        "label": "State",              "field": "state",        "align": "center", "sortable": True},
                        # Scheduled/countdown: "fires M/D HH:MM UTC" / "gate pending" / "stop in Xh Xm"
                        {"name": "sched",        "label": "Scheduled / Stop",   "field": "sched",        "align": "left"},
                        # Leg preview: filled strikes for ACTIVE; structural from spec for AUTHORED
                        {"name": "legs",         "label": "Legs",               "field": "legs",         "align": "left"},
                        {"name": "entry_credit", "label": "Entry Credit (USD)", "field": "entry_credit", "align": "right",  "sortable": True},
                        {"name": "mark",         "label": "Mark",               "field": "mark",         "align": "right",  "sortable": True},
                        {"name": "pnl_usd",      "label": "P&L (USD)",          "field": "pnl_usd",      "align": "right",  "sortable": True},
                        {"name": "pnl_bps",      "label": "P&L (bps)",          "field": "pnl_bps",      "align": "right",  "sortable": True, "sort": "numeric"},
                        {"name": "gate_chips",   "label": "Gate Status",        "field": "gate_chips",   "align": "left"},
                    ]
                    sta_trades_table = ui.table(
                        columns=sta_trades_columns, rows=[], row_key="record_id",
                    ).classes("w-full mt-2").style(f"background-color: {BG_PANEL};")
                    sta_trades_table.add_slot("body-cell-trade_type", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.trade_type_color,
                                border: '1px solid ' + props.row.trade_type_color,
                                borderRadius: '4px',
                                padding: '2px 6px',
                                fontSize: '11px',
                            }">{{ props.row.trade_type }}</span>
                        </q-td>
                    """)
                    sta_trades_table.add_slot("body-cell-state", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.state_color,
                                border: '1px solid ' + props.row.state_color,
                                borderRadius: '4px',
                                padding: '2px 7px',
                                fontSize: '11px',
                                fontWeight: 'bold',
                            }">{{ props.row.state }}</span>
                        </q-td>
                    """)
                    sta_trades_table.add_slot("body-cell-pnl_bps", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.pnl_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl_bps }}</span>
                        </q-td>
                    """)
                    sta_trades_table.add_slot("body-cell-pnl_usd", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.pnl_usd_raw > 0 ? '""" + GREEN + r"""'
                                     : props.row.pnl_usd_raw < 0 ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.pnl_usd }}</span>
                        </q-td>
                    """)
                    # Gate chips: coloured spans per gate indicator — same pattern
                    # as MTA's condition chips so operators have one familiar idiom.
                    sta_trades_table.add_slot("body-cell-gate_chips", r"""
                        <q-td :props="props" style="vertical-align: top; padding: 4px 8px;">
                            <div style="display: flex; flex-wrap: wrap; gap: 4px; font-size: 11px; line-height: 1.4; max-height: 80px; overflow-y: auto;">
                                <span v-for="chip in props.row.gate_chips"
                                      :key="chip.label"
                                      :style="{
                                        color: chip.color,
                                        border: '1px solid ' + chip.color,
                                        borderRadius: '3px',
                                        padding: '1px 5px',
                                        whiteSpace: 'nowrap',
                                      }">{{ chip.label }}</span>
                            </div>
                        </q-td>
                    """)

                    _sta_trades_status = ui.label(
                        "Waiting for first STA heartbeat on tcp://127.0.0.1:5570…"
                    ).style(f"color: {YELLOW}; font-style: italic; font-size: 13px; margin-top: 6px;")

                    def update_sta_trades():
                        with dashboard._lock:
                            snap = dashboard._sta_latest
                            last_ts = dashboard._sta_last_ts
                        if snap is None:
                            return
                        age_s = time.time() - last_ts
                        _sta_trades_status.set_text(
                            f"STA heartbeat: {_fmt_age(age_s)} ago  ·  v{snap.get('version', '?')}"
                        )
                        _sta_trades_status.style(
                            replace=f"color: {TEXT_SECONDARY}; font-size: 12px;"
                        )
                        rows = []
                        for rec in (snap.get("lifecycle") or []):
                            rid   = rec.get("record_id") or rec.get("spec_id") or "?"
                            state = rec.get("state", "?")
                            pnl   = _coerce_float(rec.get("current_pnl_bps"))
                            pnl_s = (f"+{pnl:.1f}" if pnl is not None and pnl >= 0
                                     else f"{pnl:.1f}" if pnl is not None else "—")
                            # Decimal-as-string coercion at renderer boundary
                            upnl   = _coerce_float(rec.get("unrealised_pnl"))
                            upnl_s = (f"+{upnl:.2f}" if upnl is not None and upnl >= 0
                                      else f"{upnl:.2f}" if upnl is not None else "—")
                            ecredit   = _coerce_float(rec.get("entry_credit"))
                            ecredit_s = (f"+{ecredit:.2f}" if ecredit is not None and ecredit >= 0
                                         else f"{ecredit:.2f}" if ecredit is not None else "—")
                            mark   = _coerce_float(rec.get("current_mark"))
                            mark_s = f"{mark:.2f}" if mark is not None else "—"
                            sched  = rec.get("scheduled_entry") or ""
                            try:
                                from datetime import datetime as _dt
                                entry_dt = _dt.fromisoformat(sched.replace("Z", "+00:00"))
                                age_entry = _fmt_age(time.time() - entry_dt.timestamp())
                            except Exception:
                                age_entry = "—"
                            # Countdown: whichever is sooner — time_stop or expiry
                            ts_stop = rec.get("time_to_time_stop_seconds")
                            ts_exp  = rec.get("time_to_expiry_seconds")
                            if ts_stop is not None and ts_exp is not None:
                                countdown = _fmt_countdown(min(ts_stop, ts_exp))
                            elif ts_stop is not None:
                                countdown = _fmt_countdown(ts_stop)
                            elif ts_exp is not None:
                                countdown = _fmt_countdown(ts_exp)
                            else:
                                countdown = "—"
                            # Gate chips for GATE_PENDING; empty list otherwise
                            gate_chips = (
                                _build_gate_chips(rec.get("gate_trace") or [])
                                if state == "GATE_PENDING" else []
                            )
                            spec_id = rec.get("spec_id", "?")
                            tt_label, tt_color = _derive_trade_type(spec_id)
                            rows.append({
                                "record_id":        str(rid),
                                "label":            _derive_label(spec_id),
                                "trade_type":       tt_label,
                                "trade_type_color": tt_color,
                                "spec_id":          spec_id,
                                "state":            state,
                                "state_color":      _STA_STATE_COLORS.get(state, TEXT_SECONDARY),
                                "sched":            _fmt_sched_col(
                                                        state, sched,
                                                        rec.get("time_to_time_stop_seconds")),
                                "legs":             _fmt_legs(rec.get("legs") or []),
                                "entry_credit":     ecredit_s,
                                "mark":             mark_s,
                                "pnl_usd":          upnl_s,
                                "pnl_usd_raw":      upnl if upnl is not None else 0.0,
                                "pnl_bps":          pnl_s,
                                "pnl_raw":          pnl if pnl is not None else 0.0,
                                "countdown":        countdown,
                                "gate_chips":       gate_chips,
                            })
                        if not rows:
                            rows = [{"record_id": "—", "label": "no active trades",
                                     "trade_type": "—", "trade_type_color": TEXT_SECONDARY,
                                     "spec_id": "—", "state": "—",
                                     "state_color": TEXT_SECONDARY, "sched": "—",
                                     "legs": "—", "entry_credit": "—", "mark": "—",
                                     "pnl_usd": "—", "pnl_usd_raw": 0.0,
                                     "pnl_bps": "—", "pnl_raw": 0.0,
                                     "countdown": "—", "gate_chips": []}]
                        sta_trades_table.rows = rows
                        sta_trades_table.update()

                    ui.timer(2.0, update_sta_trades)

              # ================ STA SLATES TAB ================
              if sta_slates_tab is not None:
                with ui.tab_panel(sta_slates_tab):
                    ui.label("STA Slate Composition").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    # Slates: slate-level constraints per operator's STA spec.
                    # SC1 = defined-risk count, SC2 = hawkish-hedge slot,
                    # SC3 = short-convex count, Q3 = factor-diversity tally.
                    sta_slates_columns = [
                        {"name": "slate_id",     "label": "Slate",         "field": "slate_id",     "align": "left",  "sortable": True},
                        {"name": "constructs",   "label": "Constructs",    "field": "constructs",   "align": "center","sortable": True},
                        {"name": "sc1_count",    "label": "SC1 (def-risk)","field": "sc1_count",    "align": "center","sortable": True},
                        {"name": "sc2_status",   "label": "SC2 (hedge)",   "field": "sc2_status",   "align": "center","sortable": False},
                        {"name": "sc3_count",    "label": "SC3 (short-cvx)","field": "sc3_count",   "align": "center","sortable": True},
                        {"name": "q3_diversity", "label": "Q3 (diversity)","field": "q3_diversity", "align": "center","sortable": True},
                        {"name": "status",       "label": "Status",        "field": "status",       "align": "center","sortable": True},
                    ]
                    sta_slates_table = ui.table(
                        columns=sta_slates_columns, rows=[], row_key="slate_id",
                    ).classes("w-full mt-2").style(f"background-color: {BG_PANEL};")

                    _sta_slates_status = ui.label(
                        "Waiting for STA slate data…"
                    ).style(f"color: {YELLOW}; font-style: italic; font-size: 13px; margin-top: 6px;")

                    def update_sta_slates():
                        with dashboard._lock:
                            snap = dashboard._sta_latest
                        if snap is None:
                            return
                        slates = snap.get("slates") or []
                        if not slates:
                            _sta_slates_status.set_text(
                                "No slate data in heartbeat — STA may not emit slates yet."
                            )
                            _sta_slates_status.style(
                                replace=f"color: {TEXT_SECONDARY}; font-size: 12px;"
                            )
                            return
                        _sta_slates_status.set_text("")
                        rows = []
                        for s in slates:
                            rows.append({
                                "slate_id":     s.get("slate_id", "?"),
                                "constructs":   str(s.get("construct_count", "—")),
                                "sc1_count":    str(s.get("sc1_defined_risk_count", "—")),
                                "sc2_status":   s.get("sc2_hawkish_hedge_status", "—"),
                                "sc3_count":    str(s.get("sc3_short_convex_count", "—")),
                                "q3_diversity": str(s.get("q3_factor_diversity", "—")),
                                "status":       s.get("status", "—"),
                            })
                        sta_slates_table.rows = rows
                        sta_slates_table.update()

                    ui.timer(2.0, update_sta_slates)

              # ================ STA CHAIN TAB ================
              if sta_chain_tab is not None:
                with ui.tab_panel(sta_chain_tab):
                    ui.label("STA Options Chain").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    # Per-underlying summary. Reads enriched chain_status first
                    # (strikes_loaded, strikes_with_greeks, spot_estimate, etc.),
                    # falls back to legacy chain_stats summary format.
                    chain_summary_columns = [
                        {"name": "underlying",    "label": "Underlying",      "field": "underlying",    "align": "left",  "sortable": True},
                        {"name": "spot",          "label": "Spot",            "field": "spot",          "align": "right", "sortable": True},
                        {"name": "strikes_loaded","label": "Strikes",         "field": "strikes_loaded","align": "center","sortable": True},
                        {"name": "greeks_pct",    "label": "Greeks %",        "field": "greeks_pct",    "align": "right", "sortable": True},
                        {"name": "last_quote",    "label": "Last Quote",      "field": "last_quote",    "align": "left"},
                        {"name": "last_greeks",   "label": "Last Greeks",     "field": "last_greeks",   "align": "left"},
                    ]
                    chain_summary_table = ui.table(
                        columns=chain_summary_columns, rows=[], row_key="underlying",
                    ).classes("w-full mt-2").style(f"background-color: {BG_PANEL};")
                    # Greeks % colour: green >80%, yellow 50-80%, red <50%
                    chain_summary_table.add_slot("body-cell-greeks_pct", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.greeks_pct_raw > 80 ? '""" + GREEN + r"""'
                                     : props.row.greeks_pct_raw > 50 ? '""" + YELLOW + r"""'
                                     : props.row.greeks_pct_raw >= 0  ? '""" + RED + r"""'
                                     : '""" + TEXT_SECONDARY + r"""',
                                fontWeight: 'bold'
                            }">{{ props.row.greeks_pct }}</span>
                        </q-td>
                    """)

                    _sta_chain_status = ui.label(
                        "Waiting for STA chain stats…"
                    ).style(f"color: {YELLOW}; font-style: italic; font-size: 13px; margin-top: 6px;")

                    # Expiry sub-table — populated for the selected underlying
                    ui.label("Expiries").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 14px;"
                    )
                    _sta_chain_ul_select = ui.select(
                        options=[], value=None, label="Underlying",
                    ).style(f"color: {TEXT_PRIMARY}; min-width: 120px;").props("dense options-dense")

                    expiry_table = ui.table(
                        columns=[
                            {"name": "expiry",   "label": "Expiry",    "field": "expiry",   "align": "left",  "sortable": True},
                            {"name": "strikes",  "label": "Strikes",   "field": "strikes",  "align": "center","sortable": True},
                            {"name": "atm_iv",   "label": "ATM IV",    "field": "atm_iv",   "align": "right", "sortable": True},
                        ],
                        rows=[], row_key="expiry",
                        pagination={"rowsPerPage": 30, "sortBy": "expiry"},
                    ).classes("w-full mt-1").style(f"background-color: {BG_PANEL};")
                    expiry_table.add_slot("body-cell-atm_iv", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.atm_iv_raw > 0.5 ? '""" + RED + r"""'
                                     : props.row.atm_iv_raw > 0.3 ? '""" + YELLOW + r"""'
                                     : '""" + TEXT_PRIMARY + r"""'
                            }">{{ props.row.atm_iv }}</span>
                        </q-td>
                    """)

                    _sta_chain_snap_cache: list[dict] = [None]

                    def _rebuild_expiry_table():
                        snap = _sta_chain_snap_cache[0]
                        sel  = _sta_chain_ul_select.value
                        if not snap or not sel:
                            expiry_table.rows = []
                            expiry_table.update()
                            return
                        cs    = snap.get("chain_status") or {}
                        stats = cs.get("per_underlying", {}).get(sel) or {}
                        exps  = stats.get("expiries") or []
                        rows  = []
                        for ex in sorted(exps, key=lambda x: x.get("expiry", "")):
                            iv = ex.get("atm_iv")
                            rows.append({
                                "expiry":     ex.get("expiry", "—"),
                                "strikes":    str(ex.get("strikes", "—")),
                                "atm_iv":     f"{iv:.4f}" if iv is not None else "—",
                                "atm_iv_raw": float(iv) if iv is not None else -1.0,
                            })
                        expiry_table.rows = rows or [{"expiry": "—", "strikes": "—", "atm_iv": "—", "atm_iv_raw": -1.0}]
                        expiry_table.update()

                    _sta_chain_ul_select.on("update:model-value",
                                            lambda e: _rebuild_expiry_table())

                    def update_sta_chain():
                        with dashboard._lock:
                            snap = dashboard._sta_latest
                        if snap is None:
                            return
                        _sta_chain_snap_cache[0] = snap
                        # Prefer enriched chain_status (has per_underlying dict);
                        # fall back to legacy chain_stats summary format.
                        chain_status = snap.get("chain_status") or {}
                        chain_stats  = snap.get("chain_stats") or {}
                        per_underlying = chain_status.get("per_underlying") or {}
                        source = per_underlying if per_underlying else chain_stats
                        rows = []
                        for underlying, stats in sorted(source.items()):
                            # Enriched path (chain_status.per_underlying)
                            if per_underlying:
                                loaded  = stats.get("strikes_loaded")
                                greek   = stats.get("strikes_with_greeks")
                                pct_raw = (greek / loaded * 100.0
                                           if loaded and greek is not None else -1.0)
                                pct_s   = f"{pct_raw:.0f}%" if pct_raw >= 0 else "—"
                                spot    = _coerce_float(stats.get("spot_estimate"))
                                lqt     = stats.get("last_quote_ts") or ""
                                lgt     = stats.get("last_greeks_ts") or ""
                            else:
                                # Legacy chain_stats summary
                                iv      = stats.get("iv_index")
                                loaded  = stats.get("strikes_front")
                                pct_raw = -1.0
                                pct_s   = f"{stats.get('iv_index', '—')}"
                                spot    = None
                                lqt     = (snap.get("dxlink_status") or {}).get("last_quote_ts", "")
                                lgt     = ""
                            rows.append({
                                "underlying":     underlying,
                                "spot":           f"{spot:.4f}" if spot else "—",
                                "strikes_loaded": str(loaded) if loaded is not None else "—",
                                "greeks_pct":     pct_s,
                                "greeks_pct_raw": pct_raw,
                                "last_quote":     lqt[:19] if lqt else "—",
                                "last_greeks":    lgt[:19] if lgt else "—",
                            })
                        if rows:
                            _sta_chain_status.set_text("")
                            # Sync underlying selector
                            underlyings = [r["underlying"] for r in rows]
                            if _sta_chain_ul_select.options != underlyings:
                                _sta_chain_ul_select.options = underlyings
                                _sta_chain_ul_select.update()
                                if not _sta_chain_ul_select.value and underlyings:
                                    _sta_chain_ul_select.value = underlyings[0]
                            _rebuild_expiry_table()
                        else:
                            _sta_chain_status.set_text(
                                "No chain_status/chain_stats in heartbeat — STA may not emit chain data yet."
                            )
                            _sta_chain_status.style(
                                replace=f"color: {TEXT_SECONDARY}; font-size: 12px;"
                            )
                        chain_summary_table.rows = rows
                        chain_summary_table.update()

                    ui.timer(2.0, update_sta_chain)

              # ================ STA LIFECYCLE TAB ================
              if sta_lifecycle_tab is not None:
                with ui.tab_panel(sta_lifecycle_tab):
                    ui.label("STA Trade Lifecycle Audit").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    # Dropdown of known record_ids; table shows state transitions.
                    _sta_lc_selected: list[str] = [None]

                    _sta_lc_select = ui.select(
                        options=[], value=None, label="Select trade",
                    ).style(f"color: {TEXT_PRIMARY}; min-width: 260px;").props("dense options-dense")

                    lifecycle_cols = [
                        {"name": "ts",        "label": "Timestamp",        "field": "ts",        "align": "left"},
                        {"name": "state",     "label": "State",            "field": "state",     "align": "center"},
                        {"name": "spec_id",   "label": "Spec",             "field": "spec_id",   "align": "left"},
                        {"name": "gate_note", "label": "Gate / Reason",    "field": "gate_note", "align": "left"},
                    ]
                    lifecycle_table = ui.table(
                        columns=lifecycle_cols, rows=[], row_key="ts",
                        pagination={"rowsPerPage": 50, "sortBy": "ts", "descending": True},
                    ).classes("w-full mt-2").style(f"background-color: {BG_PANEL};")
                    lifecycle_table.add_slot("body-cell-state", r"""
                        <q-td :props="props">
                            <span :style="{
                                color: props.row.state_color,
                                border: '1px solid ' + props.row.state_color,
                                borderRadius: '4px',
                                padding: '2px 6px',
                                fontSize: '11px',
                                fontWeight: 'bold',
                            }">{{ props.row.state }}</span>
                        </q-td>
                    """)

                    _sta_lc_status = ui.label("").style(
                        f"color: {TEXT_SECONDARY}; font-size: 12px;"
                    )

                    def update_sta_lifecycle():
                        with dashboard._lock:
                            history = dict(dashboard._sta_lifecycle_history)
                        known = sorted(history.keys())
                        # Build option dict: key=record_id str, label="#N — Short Name"
                        opts = {
                            rid: (f"#{rid} — "
                                  + _derive_label(history[rid][0].get("spec_id", rid)))
                            for rid in known
                        }
                        if _sta_lc_select.options != opts:
                            _sta_lc_select.options = opts
                            _sta_lc_select.update()
                            if known and _sta_lc_selected[0] is None:
                                _sta_lc_selected[0] = known[0]
                                _sta_lc_select.value = known[0]
                        raw_sel = _sta_lc_selected[0] or (_sta_lc_select.value if _sta_lc_select.value else None)
                        sel = str(raw_sel) if raw_sel is not None else None
                        if not sel or sel not in history:
                            _sta_lc_status.set_text(
                                "Waiting for STA lifecycle data…" if not known
                                else "Select a trade above."
                            )
                            lifecycle_table.rows = []
                            lifecycle_table.update()
                            return
                        transitions = history[sel]
                        rows = []
                        for t in transitions:
                            state = t.get("state", "?")
                            # Build gate note from stored gate_trace blockers
                            gate_trace = t.get("gate_trace") or []
                            blockers = [g.get("blocker_reason") or ""
                                        for g in gate_trace if not g.get("passed")]
                            gate_note = "; ".join(b for b in blockers if b)
                            if not gate_note:
                                gate_note = t.get("last_reason", "")
                            rows.append({
                                "ts":          t.get("ts", "")[:19],
                                "state":       state,
                                "state_color": {"ACTIVE": GREEN, "CLOSED": TEXT_SECONDARY,
                                                "GATE_PENDING": YELLOW, "DISPATCHED": BLUE,
                                                "MILESTONE_CHECK": YELLOW,
                                                "PARTIAL_CLOSED": YELLOW}.get(state, TEXT_SECONDARY),
                                "spec_id":     t.get("spec_id", ""),
                                "gate_note":   gate_note,
                            })
                        _sta_lc_status.set_text(f"{len(rows)} transition(s) recorded for {sel}")
                        lifecycle_table.rows = rows
                        lifecycle_table.update()

                    _sta_lc_select.on("update:model-value",
                                      lambda e: (_sta_lc_selected.__setitem__(
                                          0, str(e.value) if e.value is not None else None),
                                                 update_sta_lifecycle()))
                    ui.timer(2.0, update_sta_lifecycle)

              # ================ STA HEALTH TAB ================
              if sta_health_tab is not None:
                with ui.tab_panel(sta_health_tab):
                    ui.label("STA Health").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;"
                    )
                    with ui.row().classes("w-full gap-4 mt-2"):
                        # Heartbeat summary card
                        with ui.card().classes("flex-1").style(f"background-color: {BG_PANEL};"):
                            ui.label("Heartbeat").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            _sta_hb_label = ui.label("Waiting…").style(
                                f"color: {YELLOW}; white-space: pre-wrap; font-family: monospace; font-size: 13px;"
                            )
                        # DXLink status card
                        with ui.card().classes("flex-1").style(f"background-color: {BG_PANEL};"):
                            ui.label("DXLink Subscriber").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            _sta_dxlink_label = ui.label("—").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; font-family: monospace; font-size: 13px;"
                            )
                        # Schedule engine card
                        with ui.card().classes("flex-1").style(f"background-color: {BG_PANEL};"):
                            ui.label("Schedule Engine").style(
                                f"color: {TEXT_SECONDARY}; font-weight: bold;"
                            )
                            _sta_engine_label = ui.label("—").style(
                                f"color: {TEXT_PRIMARY}; white-space: pre-wrap; font-family: monospace; font-size: 13px;"
                            )

                    # Errors table
                    ui.label("Recent Errors").classes("mt-4").style(
                        f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 14px;"
                    )
                    sta_errors_table = ui.table(
                        columns=[
                            {"name": "ts",        "label": "Time",      "field": "ts",        "align": "left"},
                            {"name": "message",   "label": "Message",   "field": "message",   "align": "left"},
                            {"name": "record_id", "label": "Record ID", "field": "record_id", "align": "left"},
                        ],
                        rows=[], row_key="ts",
                    ).classes("w-full mt-2").style(f"background-color: {BG_PANEL};")
                    sta_errors_table.add_slot("body-cell-message", r"""
                        <q-td :props="props">
                            <span :style="{color: '""" + RED + r"""'}">{{ props.row.message }}</span>
                        </q-td>
                    """)

                    def update_sta_health():
                        with dashboard._lock:
                            snap = dashboard._sta_latest
                            last_ts = dashboard._sta_last_ts
                        if snap is None:
                            return
                        age_s = time.time() - last_ts
                        age_color = (GREEN if age_s < 30
                                     else YELLOW if age_s < 120 else RED)
                        _sta_hb_label.set_text(
                            f"Age:      {_fmt_age(age_s)} ago\n"
                            f"Version:  {snap.get('version', '?')}\n"
                            f"PID:      {snap.get('schedule_engine_pid', '?')}\n"
                            f"TS:       {(snap.get('ts') or '')[:19]}"
                        )
                        _sta_hb_label.style(
                            replace=f"color: {age_color}; white-space: pre-wrap; font-family: monospace; font-size: 13px;"
                        )
                        dx = snap.get("dxlink_status") or {}
                        # DXLink freshness: prefer chain_status.per_underlying timestamps
                        # (enriched schema), fall back to dxlink_status.last_quote_ts.
                        chain_status = snap.get("chain_status") or {}
                        per_ul = chain_status.get("per_underlying") or {}
                        now_t = time.time()
                        quote_ages = []
                        for ul_data in per_ul.values():
                            lqt = ul_data.get("last_quote_ts")
                            if lqt:
                                try:
                                    from datetime import datetime as _dt
                                    dt = _dt.fromisoformat(lqt.replace("Z", "+00:00"))
                                    quote_ages.append(now_t - dt.timestamp())
                                except Exception:
                                    pass
                        if not quote_ages:
                            raw_lqt = dx.get("last_quote_ts") or ""
                            if raw_lqt:
                                try:
                                    from datetime import datetime as _dt
                                    dt = _dt.fromisoformat(raw_lqt.replace("Z", "+00:00"))
                                    quote_ages.append(now_t - dt.timestamp())
                                except Exception:
                                    pass
                        if quote_ages:
                            best_age = min(quote_ages)
                            dx_fresh_color = (GREEN if best_age < 60
                                              else YELLOW if best_age < 300 else RED)
                            dx_fresh_str = f"{_fmt_age(best_age)} ago"
                        else:
                            dx_fresh_color = TEXT_SECONDARY
                            dx_fresh_str = "—"
                        # symbols_subscribed: prefer dxlink_status field,
                        # fall back to chain_status.table_count (real heartbeat
                        # has no dxlink_status key — count comes from chain_status)
                        syms_count = (dx.get("symbols_subscribed")
                                      or chain_status.get("table_count")
                                      or len(per_ul)
                                      or "—")
                        _sta_dxlink_label.set_text(
                            f"Symbols:    {syms_count}\n"
                            f"Last quote: {dx_fresh_str}"
                        )
                        _sta_dxlink_label.style(
                            replace=f"color: {dx_fresh_color}; white-space: pre-wrap; font-family: monospace; font-size: 13px;"
                        )
                        pid = snap.get("schedule_engine_pid")
                        pid_color = GREEN if pid else RED
                        _sta_engine_label.set_text(
                            f"PID: {pid or 'not running'}"
                        )
                        _sta_engine_label.style(
                            replace=f"color: {pid_color}; white-space: pre-wrap; font-family: monospace; font-size: 13px;"
                        )
                        errs = snap.get("errors_recent") or []
                        err_rows = []
                        for i, e in enumerate(errs[:20]):
                            if isinstance(e, str):
                                err_rows.append({"ts": str(i), "message": e, "record_id": ""})
                            else:
                                ts_raw = e.get("timestamp") or e.get("ts") or str(i)
                                err_rows.append({
                                    "ts":        ts_raw[:19] if isinstance(ts_raw, str) else str(ts_raw),
                                    "message":   e.get("message") or e.get("error") or str(e),
                                    "record_id": e.get("record_id") or e.get("spec_id") or "",
                                })
                        if not err_rows:
                            err_rows = [{"ts": "—", "message": "no errors", "record_id": ""}]
                        sta_errors_table.rows = err_rows
                        sta_errors_table.update()

                    ui.timer(2.0, update_sta_health)

            # ---- Periodic UI update — Console tab refresh ----
            # Skipped when Console tab is disabled.
            if console_tab is not None:
                def update_ui():
                    with dashboard._lock:
                        for info in dashboard._strategies.values():
                            if "_metric_age" in info:
                                info["_metric_age"] += 1

                        strategies = {k: dict(v) for k, v in dashboard._strategies.items()}
                        pnl_history = list(dashboard._pnl_history)
                        total_pnl = dashboard._total_pnl
                        trades = list(dashboard._trades)
                        alert = dashboard._alert
                        mode = dashboard._mode
                        dashboard._alert = ""
                        open_trades = dict(dashboard._open_trades)
                        closed_trades = list(dashboard._closed_trades)
                        tick_prices = dict(dashboard._tick_prices)
                        mfe_mae_cache = dict(dashboard._mfe_mae_cache)
                        margin_summary = dict(dashboard._margin_summary)
                        agg_bps = dict(dashboard._agg_bps)
                        scenarios = {
                            k: {
                                "status": v.get("status"),
                                "broker": v.get("broker"),
                                "live": v.get("live"),
                                "pnl": v.get("pnl", 0.0),
                                "trades": v.get("trades", 0),
                                "wins": v.get("wins", 0),
                                "n_strategies": len(v.get("strategies") or {}),
                            }
                            for k, v in dashboard._scenarios.items()
                        }

                    now = time.strftime("%Y-%m-%d %H:%M:%S")

                    pnl_sign = "+" if total_pnl >= 0 else ""
                    pnl_colour = GREEN if total_pnl >= 0 else RED
                    mode_label.set_text(f"Mode: {mode.upper()}")
                    pnl_label.set_text(f"Daily P&L: {pnl_sign}{total_pnl:.1f} bps")
                    pnl_label.style(replace=f"color: {pnl_colour};")
                    strat_label.set_text(f"Strategies: {len(strategies)}")
                    clock_label.set_text(now)

                    alert_label.set_text(f"ALERT: {alert}" if alert else "")

                    # Sidecar apps (like the arb scanner) publish under
                    # their own strategy_id and have a dedicated tab —
                    # exclude them here so data isn't shown in two places.
                    # Keep the scenario filter toggle options in sync
                    # with the orchestrator's current scenario list.
                    # Idempotent on every render — cheap even at 60fps.
                    scen_opts = ["All"] + sorted(self._scenario_strategies.keys())
                    if self._scenario_filter_toggle.options != scen_opts:
                        self._scenario_filter_toggle.options = scen_opts
                        self._scenario_filter_toggle.update()

                    # (A) Account margin tile — one chip per account
                    accounts = margin_summary.get("accounts", [])
                    margin_tile.clear()
                    if accounts:
                        with margin_tile:
                            for acc in accounts:
                                used = acc.get("used_margin", 0) or 0
                                free = acc.get("free_margin", 0) or 0
                                hdroom = acc.get("headroom_trades") or 0
                                cur = acc.get("account_currency", "")
                                cur_tag = f" {cur}" if cur else ""
                                hdroom_color = (GREEN if hdroom >= 5
                                                else YELLOW if hdroom >= 2
                                                else RED)
                                with ui.row().classes("items-center gap-3 px-3 py-1").style(
                                    f"background-color: {BG_PANEL}; border-radius: 4px;"
                                    f" border: 1px solid #2a2a3a;"
                                ):
                                    ui.label(
                                        f"used: {used:,.0f}{cur_tag}"
                                    ).style(f"color: {TEXT_PRIMARY}; font-size: 12px;")
                                    ui.label(
                                        f"free: {free:,.0f}{cur_tag}"
                                    ).style(f"color: {GREEN}; font-size: 12px;")
                                    ui.label(
                                        f"headroom: {hdroom} trades"
                                    ).style(f"color: {hdroom_color}; font-size: 12px; font-weight: bold;")

                    # (E) Aggregate bps — append to header bar when present
                    if agg_bps:
                        tot = agg_bps.get("total_bps")
                        mean = agg_bps.get("mean_bps")
                        med = agg_bps.get("median_bps")
                        parts = []
                        if tot is not None:
                            parts.append(f"Σ {tot:+.1f}")
                        if mean is not None:
                            parts.append(f"μ {mean:+.1f}")
                        if med is not None:
                            parts.append(f"med {med:+.1f}")
                        if parts:
                            bps_color = GREEN if (mean or 0) >= 0 else RED
                            pnl_label.set_text(
                                f"Daily P&L: {pnl_sign}{total_pnl:.1f} bps  |  tick: {' '.join(parts)}"
                            )
                            pnl_label.style(replace=f"color: {pnl_colour};")

                    # Resolve which strategies should appear given the
                    # active filter. "All" = no filter.
                    filt = self._scenario_filter
                    if filt and filt != "All" and filt in self._scenario_strategies:
                        allowed_strats = set(self._scenario_strategies[filt])
                    else:
                        allowed_strats = None  # no filter

                    # Which scenarios route each strategy? Inverse map
                    # so per-row rendering is O(1).
                    strat_to_scenarios: dict[str, list[str]] = {}
                    for scen, strats_list in self._scenario_strategies.items():
                        for s in strats_list:
                            strat_to_scenarios.setdefault(s, []).append(scen)

                    new_rows = _build_strategy_rows(
                        strategies, allowed_strats, strat_to_scenarios,
                        self._broker_spreads,
                        self._scenario_spread_gates,
                    )
                    # Annotate rows with required_margin_gbp from strategy info
                    for row in new_rows:
                        sid_r = row.get("strategy", "")
                        sinfo_r = strategies.get(sid_r, {})
                        rm = sinfo_r.get("required_margin_gbp")
                        row["margin"] = f"{rm:,.0f}" if rm else "—"
                    strat_table.rows = new_rows
                    strat_table.update()

                    # Scenarios table — hidden in the legacy single-scenario case
                    scen_new_rows = []
                    show_scenarios = (
                        len(scenarios) > 1
                        or (len(scenarios) == 1 and "default" not in scenarios)
                    )
                    if show_scenarios:
                        for scen_id, sinfo in sorted(scenarios.items()):
                            # Sum pnl_bps from all strategies routed to this
                            # scenario. Uses the same strat_to_scenarios map
                            # that drives the strategy table filter.
                            scen_strat_ids = self._scenario_strategies.get(scen_id, [])
                            spnl = sum(
                                strategies.get(s, {}).get("pnl_bps", 0.0)
                                for s in scen_strat_ids
                            )
                            spnl_s = f"+{spnl:.1f}" if spnl >= 0 else f"{spnl:.1f}"
                            s_trades = sinfo.get("trades", 0)
                            s_wins = sinfo.get("wins", 0)
                            s_winpct = (
                                f"{(s_wins / s_trades * 100):.0f}%"
                                if s_trades > 0 else "--"
                            )
                            scen_new_rows.append({
                                "scenario": scen_id,
                                "broker": sinfo.get("broker") or "--",
                                "live": "yes" if sinfo.get("live") else "paper",
                                "status": sinfo.get("status") or "active",
                                "pnl": spnl_s,
                                "pnl_raw": spnl,
                                "trades": str(s_trades),
                                "win_pct": s_winpct,
                                "n_strategies": str(sinfo.get("n_strategies", 0)),
                            })
                    scen_table.rows = scen_new_rows
                    scen_table.update()

                    # Broker effective costs panel — total_bps + breakdown
                    # read directly from orch heartbeat's broker_spreads.
                    # PR-5 cost-model unification (2026-05-13): ZmqGui no
                    # longer recomputes anything — orch-side
                    # _apply_structural_breakdown (cost_model.cost_per_
                    # roundtrip_structural_bps) is the single source of
                    # truth. Cell value is the structural total in
                    # USD-notional bps (size-invariant, NO flat fees per
                    # D9); flat-fee impact at baseline qty=1000 shows up
                    # in the tooltip as a reference annotation.
                    with dashboard._lock:
                        broker_spreads = dict(dashboard._broker_spreads)

                    # broker_id → short bucket key in broker_spreads
                    broker_data_source_map = {
                        "oanda-practice": "oanda",
                        "dukascopy-demo": "dukascopy",
                        "ig-cfd-demo": "ig",
                    }

                    broker_cost_new_rows = []
                    for instrument in _ACTIVE_PAIRS:
                        # Per-broker total_bps + breakdown components.
                        costs_per_broker: dict[str, float | None] = {}
                        breakdown_per_broker: dict[str, dict] = {}
                        # Use the first valid bid/ask we see for the
                        # canonical mid column (mid is broker-neutral; any
                        # of the three feeds is fine).
                        mid_for_canonical = 0.0

                        for broker_id, ds_key in broker_data_source_map.items():
                            cell = broker_spreads.get(ds_key, {}).get(instrument)
                            if not cell:
                                costs_per_broker[broker_id] = None
                                breakdown_per_broker[broker_id] = {}
                                continue
                            total = cell.get("total_bps")
                            if total is None:
                                # Pre-PR-4 orch (no structural breakdown).
                                # Fall back to raw spread_bps so the cell
                                # still shows something rather than "—".
                                total = cell.get("spread_bps")
                            costs_per_broker[broker_id] = (
                                float(total) if total is not None else None)
                            breakdown_per_broker[broker_id] = {
                                "spread": cell.get("spread_bps"),
                                "slip":   cell.get("slippage_bps_rt"),
                                "comm":   cell.get("commission_bps_rt"),
                                "flat":   cell.get("flat_bps_baseline_qty"),
                                "source": cell.get("source"),
                            }
                            if mid_for_canonical <= 0:
                                bid = float(cell.get("bid") or 0)
                                ask = float(cell.get("ask") or 0)
                                if bid > 0 and ask > bid:
                                    mid_for_canonical = (bid + ask) / 2

                        canonical_str = _fmt_canonical_mid(
                            instrument, mid_for_canonical)

                        valid_costs = {k: v for k, v in costs_per_broker.items()
                                       if v is not None and v >= 0}
                        best_broker = (min(valid_costs.items(), key=lambda x: x[1])[0]
                                       if valid_costs else None)

                        def _tip(b: str) -> str:
                            d = breakdown_per_broker.get(b) or {}
                            parts = []
                            for label, key in (("spread", "spread"),
                                                ("slip", "slip"),
                                                ("comm", "comm")):
                                v = d.get(key)
                                if v is not None:
                                    parts.append(f"{label}={float(v):.3f}")
                            flat = d.get("flat")
                            if flat is not None and float(flat) > 0:
                                parts.append(f"+flat@qty=1k: {float(flat):.1f} bps")
                            src = d.get("source")
                            if src:
                                parts.append(f"({src})")
                            return "  ".join(parts) if parts else ""

                        row = {
                            "instrument": instrument,
                            "canonical": canonical_str,
                            "oanda": (f"{costs_per_broker['oanda-practice']:.2f}"
                                      if costs_per_broker.get('oanda-practice') is not None else "—"),
                            "dukascopy": (f"{costs_per_broker['dukascopy-demo']:.2f}"
                                           if costs_per_broker.get('dukascopy-demo') is not None else "—"),
                            "ig": (f"{costs_per_broker['ig-cfd-demo']:.2f}"
                                   if costs_per_broker.get('ig-cfd-demo') is not None else "—"),
                            "oanda_best": best_broker == "oanda-practice",
                            "duka_best":  best_broker == "dukascopy-demo",
                            "ig_best":    best_broker == "ig-cfd-demo",
                            "oanda_tip": _tip("oanda-practice"),
                            "duka_tip":  _tip("dukascopy-demo"),
                            "ig_tip":    _tip("ig-cfd-demo"),
                        }
                        broker_cost_new_rows.append(row)

                    broker_cost_table.rows = broker_cost_new_rows
                    broker_cost_table.update()

                    # Open trades
                    ot_new_rows = []
                    now_ts = time.time()
                    for (sid, sym), ot in sorted(open_trades.items()):
                        entry_px = ot.get("entry_price", 0)
                        sl = ot.get("stop_loss", 0)
                        tp = ot.get("take_profit", 0)
                        timeout_sec = ot.get("timeout_seconds", 0)
                        entry_ts = ot.get("entry_ts", 0)
                        qty = float(ot.get("quantity", 0) or 0)
                        opener_comm = float(ot.get("opener_commission", 0) or 0)
                        direction = ot.get("side", "?").upper()
                        tp_data = tick_prices.get(sym)
                        if tp_data:
                            # Mark to bid when long, ask when short — the
                            # price the position would close at right now.
                            cur_price = (
                                tp_data["bid"] if ot.get("side") == "buy"
                                else tp_data["ask"]
                            )
                        else:
                            cur_price = 0
                        if entry_px > 50:
                            fmt = ".2f"
                        elif entry_px > 10:
                            fmt = ".3f"
                        else:
                            fmt = ".5f"

                        # Live unrealised P&L (cost-inclusive):
                        #   gross = (entry − current) × qty  for sell
                        #           (current − entry) × qty  for buy
                        #   RT cost estimate = 2 × opener_commission
                        # Net = gross − RT cost. Colour by sign so a
                        # break-even position after cost still reads as
                        # negative (you're behind by the cost to exit).
                        if cur_price > 0 and entry_px > 0 and qty > 0:
                            if ot.get("side") == "buy":
                                gross = _to_gbp((cur_price - entry_px) * qty, sym, tick_prices)
                            else:
                                gross = _to_gbp((entry_px - cur_price) * qty, sym, tick_prices)
                            rt_cost = 2.0 * opener_comm
                            pnl_raw = gross - rt_cost
                            pnl_str = (f"+{pnl_raw:.2f}" if pnl_raw >= 0
                                        else f"{pnl_raw:.2f}")
                            pnl_positive = pnl_raw >= 0
                        else:
                            pnl_raw = 0.0
                            pnl_str = "--"
                            # Fallback for "no quote yet" — use the
                            # side-vs-entry heuristic the current_price
                            # cell already uses so colouring stays consistent.
                            if ot.get("side") == "buy":
                                pnl_positive = cur_price >= entry_px
                            else:
                                pnl_positive = (
                                    cur_price <= entry_px if cur_price > 0 else True
                                )

                        # Entry:Current percent move. Signed by price
                        # direction (current > entry → positive), NOT by
                        # P&L direction. The cell's colour is driven by
                        # pnl_positive so a short in profit shows
                        # negative % but green — reads as "price dropped,
                        # short in the money".
                        if cur_price > 0 and entry_px > 0:
                            move_pct_raw = (cur_price - entry_px) / entry_px * 100.0
                            move_pct_str = (f"+{move_pct_raw:.3f}%"
                                            if move_pct_raw >= 0
                                            else f"{move_pct_raw:.3f}%")
                        else:
                            move_pct_raw = 0.0
                            move_pct_str = "--"

                        # TradingView chart link. OANDA: prefix since the
                        # default exchange on TV for these instruments.
                        # Replace the underscore so EUR_USD → EURUSD which
                        # matches TradingView's symbol format.
                        # Chart URL points at our local Plotly route
                        # (see /chart/<trade_id> in this file). trade_id
                        # is a GUID stamped on every fill; using it
                        # (instead of strategy+symbol) lets the chart
                        # disambiguate when two concurrent trades exist
                        # for the same strat/pair, and cleanly extend
                        # to multi-instrument trades in the future.
                        # Fallback to sid/sym path-style for legacy
                        # positions that existed before trade_ids.
                        tid = ot.get("trade_id", "")
                        if tid:
                            chart_url = f"/chart/{tid}"
                        else:
                            chart_url = f"/chart/legacy/{sid}/{sym}"

                        # Entry time with full date + HH:MM:SS — operators
                        # scanning history past the current session need
                        # to see the date to disambiguate (a paused/
                        # timeout-held position from yesterday looks the
                        # same as a fresh one without it).
                        if entry_ts > 0:
                            from datetime import datetime as _dt
                            dt = _dt.fromtimestamp(entry_ts)
                            entry_time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                            entry_time_full = entry_time_str
                        else:
                            entry_time_str = "--"
                            entry_time_full = ""

                        if timeout_sec > 0 and entry_ts > 0:
                            remaining = max(0, timeout_sec - (now_ts - entry_ts))
                            # Switch to hours format when >60 min left
                            # so an 8h timeout reads "7h 42m" not "462m".
                            if remaining >= 3600:
                                hours = int(remaining // 3600)
                                mins = int((remaining % 3600) // 60)
                                timeout_str = f"{hours}h {mins:02d}m"
                            else:
                                mins = int(remaining // 60)
                                secs = int(remaining % 60)
                                timeout_str = f"{mins}m {secs:02d}s"
                        else:
                            timeout_str = "--"
                        ubps = ot.get("unrealised_bps")
                        if ubps is not None:
                            ubps_str = f"{ubps:+.1f}"
                            ubps_raw = float(ubps)
                        else:
                            ubps_str = "--"
                            ubps_raw = 0.0
                        ot_new_rows.append({
                            "key": f"{sid}_{sym}",
                            "strategy": sid,
                            "broker": ot.get("broker_id") or "—",
                            "pair": sym,
                            "direction": direction,
                            "entry_time": entry_time_str,
                            "entry_time_raw": entry_ts,
                            "entry_time_full": entry_time_full,
                            "entry_price": f"{entry_px:{fmt}}",
                            "current_price": (
                                f"{cur_price:{fmt}}" if cur_price > 0 else "--"
                            ),
                            "move_pct": move_pct_str,
                            "move_pct_raw": move_pct_raw,
                            "unrealised_bps": ubps_str,
                            "bps_raw": ubps_raw,
                            "stop_loss": f"{sl:{fmt}}" if sl else "--",
                            "take_profit": f"{tp:{fmt}}" if tp else "--",
                            "pnl": pnl_str,
                            "pnl_raw": pnl_raw,
                            "timeout": timeout_str,
                            "pnl_positive": pnl_positive,
                            "chart_url": chart_url,
                        })
                    if not ot_new_rows:
                        ot_new_rows = [{
                            "key": "_empty",
                            "strategy": "--", "broker": "--",
                            "pair": "No open trades",
                            "direction": "--",
                            "entry_time": "--", "entry_time_raw": 0, "entry_time_full": "",
                            "entry_price": "--", "current_price": "--",
                            "move_pct": "--", "move_pct_raw": 0.0,
                            "unrealised_bps": "--", "bps_raw": 0.0,
                            "stop_loss": "--", "take_profit": "--",
                            "pnl": "--", "pnl_raw": 0.0,
                            "timeout": "--", "pnl_positive": True,
                            "chart_url": "",
                        }]
                    open_trades_table.rows = ot_new_rows
                    open_trades_table.update()

                    # P&L chart
                    if pnl_history:
                        xs = [p[0] for p in pnl_history]
                        ys = [p[1] for p in pnl_history]
                        chart_fig["data"][0]["x"] = xs
                        chart_fig["data"][0]["y"] = ys
                        pnl_chart.update()

                    # Legacy text trade log (retired — replaced by the
                    # Recent Trades table below). Kept the branch in
                    # case an older layout re-enables the label.
                    if trade_log is not None and trades:
                        lines = []
                        for t in trades[:30]:
                            side_marker = "BUY " if t.get("side") == "buy" else "SELL"
                            lines.append(
                                f"{side_marker} {t.get('symbol', '?')} "
                                f"qty={t.get('quantity', 0)} @ {t.get('price', 0):.4f} "
                                f"[{t.get('strategy_id', '?')}]"
                            )
                        trade_log.set_text("\n".join(lines))

                    # Recent Trades table (closed trades, realised P&L).
                    # Pulled from the _closed_trades deque. Format each
                    # field once here so Quasar sorting on numeric
                    # columns has a raw number (pnl_raw, entry_time_raw)
                    # while the display string stays human.
                    if recent_trades_table is not None:
                        rt_rows = []
                        from datetime import datetime as _dt
                        for t in list(closed_trades)[:100]:
                            entry_px = float(t.get("entry_price") or 0)
                            exit_px = float(t.get("exit_price") or 0)
                            if entry_px > 50:
                                fmt = ".2f"
                            elif entry_px > 10:
                                fmt = ".3f"
                            else:
                                fmt = ".5f"
                            e_ts = float(t.get("entry_ts") or 0)
                            x_ts = float(t.get("exit_ts") or 0)
                            # Always show full date + HH:MM:SS so history
                            # stays unambiguous across sessions / days.
                            e_str = (_dt.fromtimestamp(e_ts).strftime("%Y-%m-%d %H:%M:%S")
                                     if e_ts else "--")
                            x_str = (_dt.fromtimestamp(x_ts).strftime("%Y-%m-%d %H:%M:%S")
                                     if x_ts else "--")
                            net = float(t.get("pnl_net") or 0)
                            pnl_pct = float(t.get("pnl_pct") or 0)
                            sl_v = float(t.get("stop_loss") or 0)
                            tp_v = float(t.get("take_profit") or 0)
                            sl_str = f"{sl_v:{fmt}}" if sl_v > 0 else "--"
                            tp_str = f"{tp_v:{fmt}}" if tp_v > 0 else "--"
                            mm_key = (t.get("strategy_id",""), t.get("symbol",""), e_ts)
                            mm = mfe_mae_cache.get(mm_key)
                            side = (t.get("side") or "").upper()
                            direction_sign = +1 if side == "BUY" else -1 if side == "SELL" else 0
                            def _to_bps(level):
                                if not level or not entry_px:
                                    return None
                                return (level - entry_px) / entry_px * 1e4 * direction_sign
                            if mm:
                                mfe_bps = mm["mfe_bps"]
                                mae_bps = mm["mae_bps"]
                            else:
                                mfe_bps = None
                                mae_bps = None
                            tp_bps = _to_bps(tp_v) if tp_v > 0 else None
                            sl_bps = _to_bps(sl_v) if sl_v > 0 else None
                            actual_bps = pnl_pct * 100
                            def _fmt_bps(v):
                                if v is None:
                                    return "--"
                                return f"+{v:.1f}" if v >= 0 else f"{v:.1f}"
                            geometry_str = " : ".join([
                                _fmt_bps(mfe_bps),
                                _fmt_bps(tp_bps),
                                _fmt_bps(actual_bps),
                                _fmt_bps(sl_bps),
                                _fmt_bps(mae_bps),
                            ])
                            _tol = max(entry_px, 1e-9) * 1e-4
                            if tp_v > 0 and abs(exit_px - tp_v) < _tol:
                                exit_reason = "tp"
                            elif sl_v > 0 and abs(exit_px - sl_v) < _tol:
                                exit_reason = "sl"
                            else:
                                exit_reason = "timeout"
                            raw_bps = float(t.get("net_bps") or 0)
                            rt_rows.append({
                                # Unique key per closed trade so Quasar's
                                # row diffing doesn't churn.
                                "key": f"{t.get('strategy_id','?')}_{t.get('symbol','?')}_{e_ts}",
                                "strategy": t.get("strategy_id", "?"),
                                "broker": t.get("broker_id") or "—",
                                "pair": t.get("symbol", ""),
                                "direction": side,
                                "entry_time": e_str,
                                "entry_time_raw": e_ts,
                                "entry_price": f"{entry_px:{fmt}}",
                                "stop_loss": sl_str,
                                "take_profit": tp_str,
                                "exit_time": x_str,
                                "exit_time_raw": x_ts,
                                "exit_price": f"{exit_px:{fmt}}",
                                "exit_reason": exit_reason,
                                "geometry": geometry_str,
                                "net_bps": (f"+{raw_bps:.1f}" if raw_bps >= 0
                                            else f"{raw_bps:.1f}"),
                                "pnl_pct": (f"+{pnl_pct:.3f}%" if pnl_pct >= 0
                                            else f"{pnl_pct:.3f}%"),
                                "pnl": (f"+{net:.2f}" if net >= 0 else f"{net:.2f}"),
                                "pnl_raw": net,
                            })
                        if not rt_rows:
                            rt_rows = [{
                                "key": "_empty",
                                "strategy": "--", "broker": "--",
                                "pair": "No trades yet",
                                "direction": "--",
                                "entry_time": "--", "entry_time_raw": 0,
                                "entry_price": "--",
                                "net_bps": "--",
                                "stop_loss": "--", "take_profit": "--",
                                "exit_time": "--", "exit_time_raw": 0,
                                "exit_price": "--",
                                "exit_reason": None,
                                "geometry": "--",
                                "pnl_pct": "--", "pnl": "--", "pnl_raw": 0.0,
                            }]
                        recent_trades_table.rows = rt_rows
                        recent_trades_table.update()

                ui.timer(1.0, update_ui)

            # ---- Calendar health badge — runs regardless of active tab ----
            # OK requires a fresh .ok heartbeat within 1h (Modular publishes
            # every 30min → 1.5× safety factor). Alarms take precedence when
            # they arrived after the last .ok. No .ok within 1h → yellow
            # "stale heartbeat" even if no alarm is active.
            _ECON_CAL_OK_TTL = 3600

            def update_cal_badge():
                with dashboard._lock:
                    alarm = dashboard._econ_cal_alarm
                    alarm_ts = dashboard._econ_cal_alarm_ts
                    ok_ts = dashboard._econ_cal_ok_ts
                    ok_msg = dashboard._econ_cal_ok_msg

                now = time.time()
                # Alarm is "active" if it arrived more recently than the last .ok
                has_active_alarm = alarm is not None and alarm_ts > ok_ts
                last_ok_age = (now - ok_ts) if ok_ts > 0 else float("inf")

                if has_active_alarm:
                    verdict = alarm.get("verdict", "?")
                    age = _fmt_age(now - alarm_ts)
                    reasons = alarm.get("reasons") or []
                    first_reason = reasons[0][:50] if reasons else ""
                    cal_color = RED if verdict in ("STALE", "ROTTED") else YELLOW
                    cal_label.set_text(
                        f"Calendar: {verdict} ({age}) — {first_reason}"
                        if first_reason else f"Calendar: {verdict} ({age})"
                    )
                    cal_label.style(
                        replace=f"color: {cal_color}; font-weight: bold; font-size: 12px;"
                    )
                elif last_ok_age <= _ECON_CAL_OK_TTL:
                    fetch_age = ok_msg.get("last_successful_fetch_age_sec") if ok_msg else None
                    detail = f" (fetched {_fmt_age(fetch_age)} ago)" if fetch_age is not None else ""
                    cal_label.set_text(f"Calendar: OK{detail}")
                    cal_label.style(replace=f"color: {GREEN}; font-size: 12px;")
                else:
                    age_str = _fmt_age(last_ok_age) if ok_ts > 0 else "never"
                    cal_label.set_text(f"Calendar: stale heartbeat ({age_str})")
                    cal_label.style(
                        replace=f"color: {YELLOW}; font-weight: bold; font-size: 12px;"
                    )

            ui.timer(5.0, update_cal_badge)

        # ------------------------------------------------------------------
        # Chart routes — /chart/<trade_id> and /chart/legacy/<sid>/<sym>
        # ------------------------------------------------------------------
        # Primary route is trade_id (GUID per logical trade) — a single
        # trade_id can reference multiple symbols (future multi-leg trades),
        # so the renderer loops once per symbol and draws one Plotly panel
        # per leg. Per-leg entry/SL/TP come from the OPENER fill in QuestDB
        # matched by (trade_id, symbol) — querying fills rather than the
        # in-memory _open_trades lets CLOSED trades be chart-able too.
        #
        # Legacy route keeps the old strat/sym key around for in-flight
        # positions that opened before trade_ids existed. Can be deleted
        # once all live positions have rolled through a post-upgrade cycle.

        def _render_chart_panel(symbol: str, entry_px: float, sl_px: float,
                                tp_px: float, side: str, entry_ts: float):
            """One Plotly panel. Called once per symbol for multi-leg trades,
            once for single-leg. All values resolved by the caller — this
            function just renders what it's given. Returns silently on
            missing QuestDB data; caller decides whether that's fatal."""
            import psycopg2
            tbl = f"ticks_{symbol.lower()}"
            since_micros = int((time.time() - 24 * 3600) * 1_000_000)
            sql = (
                f"SELECT timestamp, avg((bid+ask)/2) AS mid "
                f"FROM {tbl} "
                f"WHERE timestamp >= cast({since_micros} as timestamp) "
                f"SAMPLE BY 1m ALIGN TO CALENDAR"
            )
            times: list[float] = []
            prices: list[float] = []
            try:
                with psycopg2.connect(
                    host="localhost", port=8812,
                    user="admin", password="quest", dbname="qdb",
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                        for ts, mid in cur.fetchall():
                            if ts is None or mid is None:
                                continue
                            if hasattr(ts, "timestamp"):
                                times.append(ts.timestamp())
                            else:
                                times.append(float(ts) / 1e6)
                            prices.append(float(mid))
            except Exception as e:
                ui.label(f"QuestDB query failed for {symbol}: {e}").style(
                    f"color: {RED};"
                )
                return
            if not prices:
                ui.label(f"No recent ticks for {symbol}").style(
                    f"color: {YELLOW};"
                )
                return

            from datetime import datetime as _dt
            iso_times = [_dt.fromtimestamp(t).isoformat() for t in times]

            fig = {
                "data": [{
                    "type": "scatter", "mode": "lines",
                    "x": iso_times, "y": prices, "name": "mid",
                    "line": {"color": PNL_LINE_COLOUR, "width": 1.5},
                }],
                "layout": {
                    "paper_bgcolor": BG_PANEL, "plot_bgcolor": BG_PANEL,
                    "font": {"color": TEXT_PRIMARY},
                    "margin": {"l": 60, "r": 30, "t": 30, "b": 40},
                    "xaxis": {"gridcolor": "#333", "title": "Time (last 24h)"},
                    "yaxis": {"gridcolor": "#333", "title": f"{symbol} mid price"},
                    "shapes": [], "annotations": [],
                },
            }

            def _hline(y: float, colour: str, label: str):
                fig["layout"]["shapes"].append({
                    "type": "line",
                    "xref": "paper", "x0": 0, "x1": 1,
                    "yref": "y", "y0": y, "y1": y,
                    "line": {"color": colour, "width": 1.5, "dash": "dash"},
                })
                fig["layout"]["annotations"].append({
                    "xref": "paper", "x": 1.0, "xanchor": "right",
                    "yref": "y", "y": y,
                    "text": f"{label} {y:.5f}".rstrip("0").rstrip("."),
                    "showarrow": False,
                    "font": {"color": colour, "size": 11},
                    "bgcolor": BG_PANEL,
                })

            if entry_px > 0:
                _hline(entry_px, "#888888", "entry")
            if sl_px > 0:
                _hline(sl_px, RED, "SL")
            if tp_px > 0:
                _hline(tp_px, GREEN, "TP")

            if entry_ts > 0 and entry_ts >= times[0]:
                entry_iso = _dt.fromtimestamp(entry_ts).isoformat()
                fig["layout"]["shapes"].append({
                    "type": "line",
                    "xref": "x", "x0": entry_iso, "x1": entry_iso,
                    "yref": "paper", "y0": 0, "y1": 1,
                    "line": {"color": "#888888", "width": 1, "dash": "dot"},
                })

            # Per-symbol header strip.
            with ui.row().classes("w-full gap-6 px-4 py-2"):
                ui.label(symbol).classes("text-lg font-bold").style(
                    f"color: {TEXT_PRIMARY};"
                )
                if side:
                    ui.label(f"Side: {side.upper()}").style(
                        f"color: {GREEN if side.lower()=='buy' else RED};"
                    )
                if entry_px > 0:
                    ui.label(f"Entry: {entry_px:.5f}").style(
                        f"color: {TEXT_PRIMARY};"
                    )
                if sl_px > 0:
                    ui.label(f"SL: {sl_px:.5f}").style(f"color: {RED};")
                if tp_px > 0:
                    ui.label(f"TP: {tp_px:.5f}").style(f"color: {GREEN};")
                ui.label(f"Last: {prices[-1]:.5f}").style(f"color: {BLUE};")

            ui.plotly(fig).classes("w-full").style("height: 500px;")

        def _page_header(title: str):
            ui.add_head_html(f"""
            <style>
                body {{ background-color: {BG_DARK} !important;
                        color: {TEXT_PRIMARY} !important; }}
            </style>
            """)
            with ui.row().classes("w-full items-center gap-4 px-4 py-2").style(
                f"background-color: {BG_HEADER}; border-radius: 6px;"
            ):
                ui.label(title).classes("text-xl font-bold").style(
                    f"color: {TEXT_PRIMARY};"
                )
                ui.link("← back", "/").style(f"color: {BLUE};")

        @ui.page("/chart/{trade_id}")
        def chart_by_trade(trade_id: str):
            import psycopg2
            # Look up ALL fills for this trade_id. The opener is the
            # earliest fill; closer (if present) tells us the trade is
            # already closed (we still render it for post-hoc review).
            # Multiple symbols under one trade_id = multi-leg; render one
            # panel per distinct symbol.
            rows: list[tuple] = []
            try:
                with psycopg2.connect(
                    host="localhost", port=8812,
                    user="admin", password="quest", dbname="qdb",
                ) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT CAST(timestamp AS LONG), symbol, side, "
                            "price, stop_loss, take_profit, strategy_id "
                            "FROM fills WHERE trade_id = %s ORDER BY timestamp",
                            (trade_id,),
                        )
                        rows = cur.fetchall()
            except Exception as e:
                _page_header(f"trade {trade_id[:12]}… (query error)")
                ui.label(f"QuestDB query failed: {e}").style(f"color: {RED};")
                return

            if not rows:
                _page_header(f"trade {trade_id[:12]}… (unknown)")
                ui.label(
                    "No fills found for this trade_id — may be a legacy "
                    "open position without a stamped GUID."
                ).style(f"color: {YELLOW};")
                return

            # Group by symbol; for each, pick the earliest fill as opener.
            by_symbol: dict[str, dict] = {}
            strat: str = ""
            for ts_us, sym, sd, px, sl, tp, sid in rows:
                strat = strat or (sid or "")
                entry = by_symbol.setdefault(sym, {
                    "entry_ts": (ts_us or 0) / 1e6,
                    "entry_price": float(px) if px is not None else 0.0,
                    "stop_loss": float(sl) if sl is not None else 0.0,
                    "take_profit": float(tp) if tp is not None else 0.0,
                    "side": sd or "",
                })

            _page_header(
                f"{strat} — trade {trade_id[:12]}… ({len(by_symbol)} leg"
                f"{'s' if len(by_symbol) > 1 else ''})"
            )

            for sym, leg in by_symbol.items():
                _render_chart_panel(
                    sym, leg["entry_price"], leg["stop_loss"],
                    leg["take_profit"], leg["side"], leg["entry_ts"],
                )

        @ui.page("/chart/legacy/{strategy}/{symbol}")
        def chart_legacy(strategy: str, symbol: str):
            # Fallback for open positions that predate trade_ids. Uses
            # the live in-memory _open_trades snapshot directly since no
            # fill row exists with a trade_id to query on.
            pos = dashboard._open_trades.get((strategy, symbol))
            _page_header(f"{strategy} — {symbol} (legacy)")
            entry_px = float(pos.get("entry_price", 0)) if pos else 0.0
            sl_px = float(pos.get("stop_loss", 0)) if pos else 0.0
            tp_px = float(pos.get("take_profit", 0)) if pos else 0.0
            side = (pos.get("side") if pos else "") or ""
            entry_ts = float(pos.get("entry_ts", 0)) if pos else 0.0
            _render_chart_panel(symbol, entry_px, sl_px, tp_px, side, entry_ts)

    # ------------------------------------------------------------------ #
    # ZMQ subscriber threads
    # ------------------------------------------------------------------ #

    def _seed_from_questdb(self, host: str = "localhost",
                             pg_port: int = 8812,
                             days: int = 7,
                             limit: int = 500) -> None:
        """Pre-populate the Recent Trades deque from QuestDB's fills table.

        The orchestrator writes every fill there (see
        ModularTradeApp/console/core/store.py). On startup we pull the
        most recent N fills within the last `days` days, most-recent
        first, so the dashboard has content immediately rather than
        waiting for the next live fill.

        Silently no-ops if QuestDB isn't reachable — the dashboard is
        not required to have history to start; live fills will populate
        as they come.
        """
        try:
            import psycopg2
        except ImportError:
            logger.warning("psycopg2 not installed — skipping trade history seed")
            return

        since_micros = int((time.time() - days * 86400) * 1_000_000)
        # broker_id was added to the fills table 2026-04-22; rows
        # pre-dating that have broker_id = empty string. The renderer
        # shows "—" for those so they visibly flag as legacy.
        sql = (
            "SELECT CAST(timestamp AS LONG), scenario_id, strategy_id, symbol, "
            "side, order_id, quantity, price, commission, stop_loss, "
            "take_profit, timeout_seconds, tag, trade_id, broker_id "
            "FROM fills "
            "WHERE timestamp >= cast(%s as timestamp) "
            "ORDER BY timestamp DESC LIMIT %s"
        )
        try:
            with psycopg2.connect(
                host=host, port=pg_port,
                user="admin", password="quest", dbname="qdb",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (since_micros, limit))
                    rows = cur.fetchall()
        except Exception:
            logger.warning(
                "Trade history seed failed — QuestDB unreachable? "
                "GUI will still populate from live fills.",
                exc_info=True,
            )
            return

        seeded = 0
        with self._lock:
            for r in rows:
                (ts_us, scen, strat, sym, side, oid, qty, px, comm,
                 sl, tp, tmo, tag, tid, broker) = r
                # Newest-first from the query; appendleft preserves
                # that ordering in the deque.
                self._trades.appendleft({
                    "timestamp": (ts_us or 0) / 1e6,
                    "scenario_id": scen,
                    "strategy_id": strat or "unknown",
                    "symbol": sym or "",
                    "side": side or "",
                    "broker_id": broker or "",
                    "price": float(px) if px is not None else 0.0,
                    "quantity": float(qty) if qty is not None else 0.0,
                    "commission": float(comm) if comm is not None else 0.0,
                    "order_id": oid or "",
                    "tag": tag or "",
                    "trade_id": tid or "",
                })
                seeded += 1

            # Pair openers with closers to populate _closed_trades.
            # Walk chronologically (reverse the newest-first rows) so
            # FIFO netting matches live behaviour. Keyed by
            # (scenario_id, strategy_id, symbol) because a fan-out gives
            # each scenario its own position book.
            chrono = list(reversed(rows))

            # Build approximate tick prices from fill data for P&L conversion.
            # Iterating oldest→newest means the last value for each symbol
            # is the most recent fill price — good enough as a conversion proxy.
            seed_prices: dict[str, dict] = {}
            for r_s in chrono:
                s_sym = (r_s[3] or "").upper()
                s_px = float(r_s[7]) if r_s[7] is not None else 0.0
                if s_px > 0 and s_sym:
                    seed_prices[s_sym] = {"bid": s_px, "ask": s_px}

            open_positions: dict = {}  # key → list of open fills
            closed_count = 0
            for r in chrono:
                (ts_us, scen, strat, sym, side, oid, qty, px, comm,
                 sl, tp, tmo, tag, tid, broker) = r
                if not sym or not strat or qty is None:
                    continue
                key = (scen or "legacy", strat, sym)
                book = open_positions.setdefault(key, [])
                # If opposite-direction fill exists, net against it
                remaining = float(qty)
                while remaining > 0 and book:
                    head = book[0]
                    if head["side"] == side:
                        break  # same direction — adds to book, not closes
                    close_qty = min(remaining, head["qty"])
                    if head["side"] == "buy":
                        pnl = _to_gbp((float(px) - head["px"]) * close_qty, sym, seed_prices)
                    else:
                        pnl = _to_gbp((head["px"] - float(px)) * close_qty, sym, seed_prices)
                    opener_comm = head.get("comm", 0.0)
                    rt_cost = 2.0 * (opener_comm * close_qty / max(head["qty_orig"], 1))
                    net_pnl = pnl - rt_cost
                    _dsign = +1 if head["side"] == "buy" else -1 if head["side"] == "sell" else 0
                    pnl_pct = (
                        _dsign * (float(px) - head["px"]) / head["px"] * 100.0
                        if head["px"] > 0 else 0.0
                    )
                    trade_key = (scen, strat, sym, head["ts"])
                    if trade_key not in self._closed_trade_keys:
                        self._closed_trade_keys.add(trade_key)
                        # Broker from opener; fall back to closer if opener empty.
                        # For pre-2026-04-22 data both may be empty, showing "—".
                        final_broker = head.get("broker") or broker or ""
                        self._closed_trades.appendleft({
                            "scenario_id": scen,
                            "strategy_id": strat,
                            "symbol": sym,
                            "broker_id": final_broker,
                            "side": head["side"],
                            "quantity": close_qty,
                            "entry_ts": head["ts"],
                            "entry_price": head["px"],
                            # Opener-side SL/TP preserved on the closed
                            # record so the Past Trades table can show
                            # whether the exit landed at an SL/TP level.
                            "stop_loss": head.get("sl", 0.0),
                            "take_profit": head.get("tp", 0.0),
                            "exit_ts": (ts_us or 0) / 1e6,
                            "exit_price": float(px),
                            "pnl_gross": pnl,
                            "pnl_net": net_pnl,
                            "pnl_pct": pnl_pct,
                        })
                        closed_count += 1
                    head["qty"] -= close_qty
                    remaining -= close_qty
                    if head["qty"] <= 0:
                        book.pop(0)
                # Any remainder is a new open-side fill — add to book
                if remaining > 0:
                    book.append({
                        "side": side,
                        "px": float(px) if px is not None else 0.0,
                        "qty": remaining,
                        "qty_orig": float(qty),
                        "ts": (ts_us or 0) / 1e6,
                        "comm": float(comm) if comm is not None else 0.0,
                        # Carry the opener fill's SL/TP so the closer
                        # (which frequently doesn't have these fields)
                        # can still show the target levels post-hoc.
                        "sl": float(sl) if sl is not None else 0.0,
                        "tp": float(tp) if tp is not None else 0.0,
                        "broker": broker or "",
                    })
            # Populate _open_trades with any remaining open positions from
            # the backfill. This allows live closers to match against
            # historical openers, enabling proper broker_id propagation.
            for (scen, strat, sym), book in open_positions.items():
                for fill in book:
                    key = (strat, sym)
                    if key not in self._open_trades:
                        self._open_trades[key] = {
                            "strategy": strat,
                            "symbol": sym,
                            "side": fill["side"],
                            "broker_id": fill.get("broker", ""),
                            "entry_price": fill["px"],
                            "quantity": fill["qty"],
                            "stop_loss": fill.get("sl", 0),
                            "take_profit": fill.get("tp", 0),
                            "timeout_seconds": 0,
                            "entry_ts": fill["ts"],
                        }

        logger.info(
            "Seeded %d historical fills + %d closed trades from QuestDB",
            seeded, closed_count,
        )
        # Queue all seeded closed trades for background MFE/MAE computation.
        self._mfe_mae_queue.extend(list(self._closed_trades))

    def _start_zmq(self):
        self._zmq_running = True
        self._zmq_thread = threading.Thread(
            target=self._zmq_loop, name="zmqgui-events", daemon=True,
        )
        self._zmq_thread.start()
        if self.cfg.feed_source:
            self._feed_thread = threading.Thread(
                target=self._feed_loop, name="zmqgui-feed", daemon=True,
            )
            self._feed_thread.start()
        # Watchdog — see _watchdog_loop. Started AFTER zmq so the
        # first 60-second sample has real ticks to count.
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, name="zmqgui-watchdog", daemon=True,
        )
        self._watchdog_thread.start()
        self._mfe_mae_thread = threading.Thread(
            target=self._mfe_mae_loop, name="zmqgui-mfemae", daemon=True,
        )
        self._mfe_mae_thread.start()
        self._sta_feed_thread = threading.Thread(
            target=self._sta_feed_loop, name="zmqgui-sta", daemon=True,
        )
        self._sta_feed_thread.start()

    def _watchdog_loop(self, interval_s: float = 60.0,
                        stall_threshold: int = 3) -> None:
        """Detect the 2026-04-21-style event-loop wedge externally-visibly.

        Every `interval_s` seconds, sample `self._zmq_ticks` and compare
        to the last sample. Non-zero delta → INFO ("watchdog: ..."); zero
        delta → WARN. `stall_threshold` consecutive zero-delta windows
        escalates to ERROR so a parent sweep grepping the log can
        distinguish "briefly quiet" from "wedged".

        The INFO line is grep-able from outside the process (the parent
        health-check sweep reads zmqgui.log) so this doubles as an
        external heartbeat — silence for >interval_s with no log line
        also indicates trouble.
        """
        last = self._zmq_ticks
        zero_streak = 0
        while self._zmq_running:
            time.sleep(interval_s)
            now = self._zmq_ticks
            delta = now - last
            last = now
            if delta > 0:
                zero_streak = 0
                logger.info(
                    "watchdog: zmq loop ticked %d times in last %.0fs (%.1f Hz)",
                    delta, interval_s, delta / interval_s,
                )
            else:
                zero_streak += 1
                if zero_streak >= stall_threshold:
                    logger.error(
                        "watchdog: ZMQ LOOP WEDGED — 0 ticks for %d consecutive "
                        "%.0fs windows (~%.0fs silent)",
                        zero_streak, interval_s, zero_streak * interval_s,
                    )
                else:
                    logger.warning(
                        "watchdog: 0 ticks in last %.0fs (%d consecutive; "
                        "will escalate to ERROR after %d)",
                        interval_s, zero_streak, stall_threshold,
                    )

    def _mfe_mae_loop(self) -> None:
        """Background thread: drain _mfe_mae_queue, compute MFE/MAE per trade."""
        import time as _time
        while self._zmq_running:
            with self._lock:
                batch = self._mfe_mae_queue[:]
                self._mfe_mae_queue.clear()
            if not batch:
                _time.sleep(1.0)
                continue
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host="localhost", port=8812,
                    user="admin", password="quest", dbname="qdb",
                )
            except Exception as e:
                logger.debug("MFE/MAE: QuestDB connect failed: %s", e)
                _time.sleep(5.0)
                continue
            try:
                for trade in batch:
                    sid = trade.get("strategy_id", "")
                    sym = trade.get("symbol", "")
                    side = trade.get("side", "buy")
                    entry_px = float(trade.get("entry_price") or 0)
                    entry_ts = float(trade.get("entry_ts") or 0)
                    exit_ts = float(trade.get("exit_ts") or 0)
                    cache_key = (sid, sym, entry_ts)
                    if entry_px <= 0 or entry_ts <= 0 or exit_ts <= 0:
                        with self._lock:
                            self._mfe_mae_cache[cache_key] = None
                        continue
                    tbl = f"ticks_{sym.lower()}"
                    entry_us = int(entry_ts * 1_000_000)
                    exit_us = int(exit_ts * 1_000_000)
                    sql = (
                        f"SELECT min((bid+ask)/2.0), max((bid+ask)/2.0) "
                        f"FROM {tbl} "
                        f"WHERE timestamp >= cast({entry_us} as timestamp) "
                        f"  AND timestamp <= cast({exit_us} as timestamp)"
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute(sql)
                            row = cur.fetchone()
                        if row and row[0] is not None and row[1] is not None:
                            min_mid = float(row[0])
                            max_mid = float(row[1])
                            if side == "buy":
                                mfe = (max_mid - entry_px) / entry_px * 10_000
                                mae = (min_mid - entry_px) / entry_px * 10_000
                            else:
                                mfe = (entry_px - min_mid) / entry_px * 10_000
                                mae = (entry_px - max_mid) / entry_px * 10_000
                            with self._lock:
                                self._mfe_mae_cache[cache_key] = {
                                    "mfe_bps": round(mfe, 1),
                                    "mae_bps": round(mae, 1),
                                }
                        else:
                            with self._lock:
                                self._mfe_mae_cache[cache_key] = None
                    except Exception:
                        with self._lock:
                            self._mfe_mae_cache[cache_key] = None
                    _time.sleep(0.05)  # gentle pacing between per-trade queries
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def _zmq_loop(self):
        """Subscribe to every configured PUB endpoint and route messages.

        One SUB socket connects to N endpoints — ZMQ merges the streams
        into a single recv loop. Empty-string topic filter accepts every
        topic; we dispatch by topic prefix in `_process_message`.
        """
        bus = Bus()
        if not self.cfg.sources:
            logger.warning("No ZMQ sources configured — dashboard will be empty.")
            return
        sub = bus.subscriber(self.cfg.sources[0], [""])
        for endpoint in self.cfg.sources[1:]:
            sub.connect(endpoint)
            logger.info("SUB also connected to %s", endpoint)

        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)

        while self._zmq_running:
            # Watchdog counter — incremented every iteration, including
            # empty polls. "Ticked" means "the loop made a pass"; a
            # true wedge (like 2026-04-21 08:01) would freeze this.
            self._zmq_ticks += 1
            events = dict(poller.poll(timeout=100))
            if sub not in events:
                continue
            try:
                msg = Bus.sub_recv(sub)
            except Exception:
                logger.debug("ZMQ recv error", exc_info=True)
                continue
            self._process_message(msg)

        sub.close()
        bus.close()

    def _feed_loop(self):
        """Separate SUB socket for the tick feed (open trades current prices)."""
        bus = Bus()
        sub = bus.subscriber(self.cfg.feed_source, ["tick."])
        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)

        while self._zmq_running:
            events = dict(poller.poll(timeout=200))
            if sub not in events:
                continue
            try:
                msg = Bus.sub_recv(sub)
            except Exception:
                continue
            topic = msg.get("topic", "")
            if topic.startswith("tick."):
                symbol = topic.split(".", 1)[1]
                with self._lock:
                    self._tick_prices[symbol] = {
                        "bid": msg.get("bid", 0),
                        "ask": msg.get("ask", 0),
                    }

        sub.close()
        bus.close()

    def _sta_feed_loop(self):
        """SUB socket for StructuredTradeApp heartbeats on tcp://127.0.0.1:5570.

        Topic prefix: 'sta.heartbeat.'. Separate port from MTA (:5552) so
        subscribing to one doesn't pull the other. Stays alive and retries
        the connection so the tab shows live data the moment STA starts.
        """
        endpoint = self.cfg.sta.endpoint
        bus = Bus()
        sub = bus.subscriber(endpoint, ["sta.heartbeat."])
        poller = zmq.Poller()
        poller.register(sub, zmq.POLLIN)
        logger.info("STA feed loop started on %s", endpoint)
        while self._zmq_running:
            events = dict(poller.poll(timeout=500))
            if sub not in events:
                continue
            try:
                msg = Bus.sub_recv(sub)
            except Exception:
                logger.debug("STA ZMQ recv error", exc_info=True)
                continue
            self._on_sta_heartbeat(msg)
        sub.close()
        bus.close()

    def _on_sta_heartbeat(self, msg: dict):
        """Store latest STA heartbeat; update lifecycle transition history."""
        with self._lock:
            self._sta_latest = msg
            self._sta_last_ts = time.time()
            for rec in (msg.get("lifecycle") or []):
                # Stringify record_id so history keys are consistent strings
                # regardless of whether STA emits int or str for this field.
                rid_raw = rec.get("record_id")
                rid = str(rid_raw) if rid_raw is not None else (rec.get("spec_id") or "?")
                history = self._sta_lifecycle_history.setdefault(rid, [])
                new_state = rec.get("state", "?")
                last_state = history[-1].get("state") if history else None
                if new_state != last_state:
                    history.append({
                        "ts":         msg.get("ts", ""),
                        "state":      new_state,
                        "record_id":  rid,
                        "spec_id":    rec.get("spec_id", ""),
                        # Preserve gate_trace snapshot for GATE_PENDING audit
                        "gate_trace": rec.get("gate_trace") if new_state == "GATE_PENDING" else None,
                        "last_reason": rec.get("last_reason", ""),
                    })

    def _process_message(self, msg: dict):
        topic = msg.get("topic", "")
        if topic.startswith("fill."):
            self._on_fill(msg)
        elif topic.startswith("metric."):
            self._on_metric(msg)
        elif topic == "heartbeat.vault-repo-hook":
            # Divert BEFORE the generic heartbeat path so it never
            # lands in self._strategies — it's a sidecar with its own
            # Vault tab, not a trading strategy.
            self._on_vault_hook(msg)
        elif topic == "balance.arbitrage":
            # Arbitrage exchange balances (Betfair, Matchbook)
            self._on_balance_arbitrage(msg)
        elif topic.startswith("block."):
            # Crypto feed: block ticks per chain
            self._on_crypto_block(msg, topic)
        elif topic.startswith("heartbeat.flash-arb-") or topic.startswith("heartbeat.triangular-arb-"):
            # Crypto strategy heartbeats (divert before generic heartbeat)
            self._on_crypto_heartbeat(msg)
        elif topic.startswith("heartbeat."):
            self._on_heartbeat(msg)
        elif topic == "command":
            self._on_command(msg)
        elif topic.startswith("arb."):
            self._on_arb_scan(msg)
        elif topic.startswith("alert.econ_calendar."):
            self._on_econ_calendar_alert(msg)

    # -- handlers (ported verbatim from ModularTradeApp GUI) -----------

    def _on_balance_arbitrage(self, msg: dict):
        """Store arbitrage exchange balances (Betfair, Matchbook).

        Two publishers send to topic "balance.arbitrage" — paper (strategy_id
        "arbitrage") and live (strategy_id "arbitrage_live"). Both envelopes
        have identical structure; we keep the most recent one for rendering.
        """
        with self._lock:
            self._balance_arbitrage = msg

    def _on_arb_scan(self, msg: dict):
        """Whole-snapshot replacement from an arb scanner publisher.

        Two publishers send to topic "arb.scan" — pre-match (strategy_id
        "arbitrage") and live (strategy_id "arbitrage_live"). We keep both
        snapshots distinct in self._arb_scans so the Arbitrage tab can
        render rows from each side simultaneously, tagged "PM" / "LIVE".
        """
        with self._lock:
            sid = msg.get("strategy_id") or "arbitrage"
            self._arb_scans[sid] = msg
            # Keep _arb_scan pointing at whichever scan landed most recently
            # so legacy single-snapshot code paths don't break.
            self._arb_scan = msg

    def _on_vault_hook(self, msg: dict):
        """KnowledgeVault repo-hook heartbeat — one per git post-commit.

        Envelope (from scripts/heartbeat.py on the vault side):
            status:  "running" | "failed"
            activity: "OK: <repo> <sha>" | "FAIL: <reason>"
            repo:    repo name (used as our row key)
            sha:     short sha
            ts:      ISO8601 publisher timestamp

        Keying by repo — multiple commits to the same repo just
        overwrite, so the Vault tab always shows latest-state.
        """
        repo = msg.get("repo") or "(unknown)"
        with self._lock:
            self._vault_hooks[repo] = {
                "status":   msg.get("status", "unknown"),
                "activity": msg.get("activity", ""),
                "repo":     repo,
                "sha":      msg.get("sha", ""),
                "ts":       msg.get("ts", ""),
                "_received": time.time(),
            }

    def _on_econ_calendar_alert(self, msg: dict):
        """Economic calendar health message (ok heartbeat or stale/degraded/rotted alarm)."""
        verdict = msg.get("verdict", "")
        with self._lock:
            if verdict == "OK":
                self._econ_cal_ok_ts = time.time()
                self._econ_cal_ok_msg = msg
                _save_econ_cal_state(self._econ_cal_ok_ts, msg)
                logger.info("Calendar health OK heartbeat received (fetch age: %ss)",
                            msg.get("last_successful_fetch_age_sec"))
            else:
                self._econ_cal_alarm = msg
                self._econ_cal_alarm_ts = time.time()
                logger.warning("Calendar health alarm: verdict=%s reasons=%s",
                               verdict, msg.get("reasons", []))

    def _on_fill(self, msg: dict):
        # Check if this is a Crypto fill (has spec_id instead of strategy_id)
        spec_id = msg.get("spec_id")
        if spec_id:
            # Crypto paper-trader fill
            with self._lock:
                self._crypto_fills.appendleft({
                    "timestamp": msg.get("timestamp", time.time()),
                    "spec_id": spec_id,
                    "chain": msg.get("chain", ""),
                    "bundle_hash": msg.get("bundle_hash", ""),
                    "simulated_profit": msg.get("simulated_profit", 0.0),
                    "status": msg.get("status", "paper"),
                })
                # Update spec's fill count
                if spec_id in self._crypto_strategies:
                    self._crypto_strategies[spec_id]["fill_count"] = \
                        self._crypto_strategies[spec_id].get("fill_count", 0) + 1
                    self._crypto_strategies[spec_id]["last_fill_ts"] = msg.get("timestamp", time.time())
            return

        sid = msg.get("strategy_id", "unknown")
        symbol = msg.get("symbol", "")
        side = msg.get("side", "")
        price = msg.get("price", 0)
        quantity = msg.get("quantity", 0)
        scen_id = msg.get("scenario_id") or "default"
        order_id = msg.get("order_id", "")
        # Execution venue — added upstream 2026-04-22. Older fills (and
        # the historical QuestDB backfill) lack the field; the tables
        # render "—" for those rather than failing sorts.
        broker_id = msg.get("broker_id", "")
        dedup_key = (scen_id, order_id)
        key = (sid, symbol)

        with self._lock:
            if order_id and dedup_key in self._seen_fills_set:
                return
            if order_id:
                if len(self._seen_fills) >= self._seen_fills.maxlen:
                    oldest = self._seen_fills[0]
                    self._seen_fills_set.discard(oldest)
                self._seen_fills.append(dedup_key)
                self._seen_fills_set.add(dedup_key)

            self._trades.appendleft(msg)

            scen_info = self._scenarios.setdefault(scen_id, {
                "status": "active", "broker": None, "live": False,
                "pnl": 0.0, "trades": 0, "wins": 0, "strategies": {},
            })
            scen_info["trades"] += 1
            scen_strat = scen_info["strategies"].setdefault(sid, {
                "pnl": 0.0, "trades": 0, "wins": 0,
            })
            scen_strat["trades"] += 1

            info = self._strategies.setdefault(sid, {
                "status": "running", "pnl": 0.0, "trades": 0, "wins": 0,
                "last_metric": "", "last_heartbeat": time.time(),
            })
            info["trades"] += 1
            info["last_heartbeat"] = time.time()
            # Track most recent broker for this strategy (for display in entry-trace)
            if broker_id:
                info["last_broker_id"] = broker_id

            existing = self._open_trades.get(key)
            if existing and existing["side"] != side:
                entry_px = existing.get("entry_price", 0)
                close_qty = min(quantity, existing.get("quantity", 0))
                if existing["side"] == "buy":
                    pnl = _to_gbp((price - entry_px) * close_qty, symbol, self._tick_prices)
                else:
                    pnl = _to_gbp((entry_px - price) * close_qty, symbol, self._tick_prices)
                info["pnl"] += pnl
                scen_info["pnl"] += pnl
                scen_strat["pnl"] += pnl
                if pnl > 0:
                    info["wins"] = info.get("wins", 0) + 1
                    scen_info["wins"] += 1
                    scen_strat["wins"] += 1

                # Record this closed trade for the Recent Trades
                # TABLE. Cost-inclusive P&L = gross − 2 × opener_comm
                # (estimate of the RT cost, matching the Open Trades
                # P&L column's convention).
                entry_ts = existing.get("entry_ts", 0)
                opener_comm = float(existing.get("opener_commission", 0) or 0)
                rt_cost = 2.0 * opener_comm
                net_pnl = pnl - rt_cost
                _dsign = +1 if existing["side"] == "buy" else -1 if existing["side"] == "sell" else 0
                pnl_pct = (
                    _dsign * (price - entry_px) / entry_px * 100.0
                    if entry_px > 0 else 0.0
                )
                dedup_key = (scen_id, sid, symbol, entry_ts)
                if dedup_key not in self._closed_trade_keys:
                    self._closed_trade_keys.add(dedup_key)
                    _ct = {
                        "scenario_id": scen_id,
                        "strategy_id": sid,
                        "symbol": symbol,
                        # Broker from opener; fall back to closer if opener
                        # doesn't have it. Historical pre-2026-04-22 data may
                        # have empty broker_id, showing as "—".
                        "broker_id": existing.get("broker_id") or broker_id,
                        "side": existing["side"],
                        "quantity": close_qty,
                        "entry_ts": entry_ts,
                        "entry_price": entry_px,
                        # Opener-side SL/TP — carried through so the
                        # Past Trades table can show "exit at SL" vs
                        # "strategy-initiated close" at a glance.
                        "stop_loss": existing.get("stop_loss", 0),
                        "take_profit": existing.get("take_profit", 0),
                        "exit_ts": msg.get("timestamp", time.time()),
                        "exit_price": price,
                        "pnl_gross": pnl,
                        "pnl_net": net_pnl,
                        "pnl_pct": pnl_pct,
                        "net_bps": msg.get("net_bps") if msg.get("net_bps") is not None else pnl_pct * 100.0,
                    }
                    self._closed_trades.appendleft(_ct)
                    self._mfe_mae_queue.append(_ct)
                    # Keep the dedup set bounded by forgetting the
                    # oldest keys as the deque evicts them.
                    if len(self._closed_trade_keys) > MAX_TRADE_LOG * 2:
                        self._closed_trade_keys = set(
                            (t.get("scenario_id"), t.get("strategy_id"),
                             t.get("symbol"), t.get("entry_ts"))
                            for t in self._closed_trades
                        )
                del self._open_trades[key]
            elif not existing:
                self._open_trades[key] = {
                    "strategy": sid, "symbol": symbol, "side": side,
                    "broker_id": broker_id,
                    "entry_price": price, "quantity": quantity,
                    "stop_loss": msg.get("stop_loss", 0),
                    "take_profit": msg.get("take_profit", 0),
                    "timeout_seconds": msg.get("timeout_seconds", 0),
                    "entry_ts": msg.get("timestamp", time.time()),
                }

            ts = msg.get("timestamp", time.time())
            self._pnl_history.append((ts, self._total_pnl))
            if len(self._pnl_history) > MAX_PNL_POINTS:
                self._pnl_history = self._pnl_history[-MAX_PNL_POINTS:]

    def _on_metric(self, msg: dict):
        sid = msg.get("strategy_id", "unknown")
        name = msg.get("name", "")
        value = msg.get("value", 0)

        with self._lock:
            info = self._strategies.setdefault(sid, {
                "status": "running", "pnl": 0.0, "trades": 0, "wins": 0,
                "last_metric": "", "last_heartbeat": time.time(),
            })
            info["last_metric"] = f"{name}={value}"
            info["last_heartbeat"] = time.time()
            info["_metric_age"] = 0

            if name == "pnl_bps":
                info["pnl_bps"] = float(value)
                # Trader-tab bps total: equal-weighted sum across plugin
                # strategies. Currency-agnostic — XAU and EUR_USD trades
                # contribute equally at the same bps magnitude.
                # Sidecars (arb scanner) excluded — they have their own tab.
                self._total_pnl = sum(
                    s.get("pnl_bps", 0.0)
                    for sid, s in self._strategies.items()
                    if sid not in _SIDECAR_SIDS
                )

            severity = msg.get("severity", "")
            if severity in ("warn", "error", "critical"):
                self._alert = f"[{sid}] {msg.get('message', name)}"

    def _on_heartbeat(self, msg: dict):
        sid = msg.get("strategy_id", "unknown")
        with self._lock:
            info = self._strategies.setdefault(sid, {
                "status": "unknown", "pnl": 0.0, "trades": 0, "wins": 0,
                "last_metric": "", "last_heartbeat": time.time(),
            })

            status = msg.get("status", "")
            if status:
                info["status"] = status
            info["last_heartbeat"] = time.time()
            # Stash the publisher-side activity string so other tabs
            # (specifically the Arbitrage status label) can show "MB
            # lockout — waiting Xs" without needing to subscribe to
            # the heartbeat themselves.
            activity = msg.get("activity", "")
            if activity:
                info["activity"] = activity

            mode = msg.get("mode", "")
            if mode:
                self._mode = mode

            # Authoritative scenario state from the publisher
            scen_exp = msg.get("scenario_exposure")
            if isinstance(scen_exp, dict):
                for scen_id, scen_data in (scen_exp.get("scenarios") or {}).items():
                    info_scen = self._scenarios.setdefault(scen_id, {
                        "status": "active", "broker": None, "live": False,
                        "pnl": 0.0, "trades": 0, "wins": 0, "strategies": {},
                    })
                    info_scen["status"] = scen_data.get("status", info_scen["status"])
                    info_scen["broker"] = scen_data.get("broker", info_scen["broker"])
                    info_scen["live"] = bool(scen_data.get("live", info_scen["live"]))

            # Margin summary + aggregate bps from compute_stats
            margin = msg.get("margin_summary")
            if margin:
                self._margin_summary = margin
            cs = msg.get("compute_stats") or {}
            for _bk in ("total_bps", "mean_bps", "median_bps"):
                if _bk in cs:
                    self._agg_bps[_bk] = cs[_bk]
                elif _bk in msg:
                    self._agg_bps[_bk] = msg[_bk]

            # Strategy-level required margin prediction
            if "required_margin_gbp" in msg:
                info["required_margin_gbp"] = msg["required_margin_gbp"]

            # Authoritative open_positions snapshot (recovers a late-joining GUI)
            positions = msg.get("open_positions")
            if positions is not None:
                self._open_trades.clear()
                for pos in positions:
                    sid_pos = pos.get("strategy_id", "unknown")
                    sym = pos.get("symbol", "")
                    k = (sid_pos, sym)
                    self._open_trades[k] = {
                        "strategy": sid_pos, "symbol": sym,
                        "side": pos.get("side", ""),
                        "broker_id": pos.get("broker_id", ""),
                        "entry_price": pos.get("entry_price", 0),
                        "quantity": pos.get("quantity", 0),
                        "stop_loss": pos.get("stop_loss", 0),
                        "take_profit": pos.get("take_profit", 0),
                        "timeout_seconds": pos.get("timeout_seconds", 0),
                        "entry_ts": pos.get("entry_ts", 0),
                        "trade_id": pos.get("trade_id", ""),
                        "unrealised_bps": pos.get("unrealised_bps"),
                        "required_margin_gbp": pos.get("required_margin_gbp"),
                    }

                existing_keys = {
                    (t.get("strategy_id", ""), t.get("symbol", ""))
                    for t in self._trades
                }
                for pos in positions:
                    pk = (pos.get("strategy_id", ""), pos.get("symbol", ""))
                    if pk not in existing_keys:
                        self._trades.appendleft({
                            "strategy_id": pos.get("strategy_id", ""),
                            "symbol": pos.get("symbol", ""),
                            "side": pos.get("side", ""),
                            "price": pos.get("entry_price", 0),
                            "quantity": pos.get("quantity", 0),
                            "timestamp": pos.get("entry_ts", 0),
                        })

            # Show activity in the "Last Signal" column when no fresh metric
            activity = msg.get("activity", "")
            if activity:
                idle = msg.get("idle_seconds", 0)
                if idle > 5:
                    activity_display = f"{activity} (idle {idle:.0f}s)"
                else:
                    activity_display = activity
                if not info["last_metric"] or info.get("_metric_age", 999) > 10:
                    info["last_metric"] = activity_display
                info["_heartbeat_activity"] = activity_display

            # Per-condition + filter-trace payload published by the
            # plugin harness (see ModularTradeApp CLAUDE.md Dashboard
            # section for shape). The Conditions column in the strategy
            # table renders these so operators can see every filter and
            # entry condition with pass/fail colour. Stash the latest
            # value — missing = "no update this heartbeat", keep the
            # prior value rather than clear it.
            if "entry_trace" in msg:
                info["_entry_trace"] = msg["entry_trace"]
            if "filter_trace" in msg:
                info["_filter_trace"] = msg["filter_trace"]
            if "filter_block" in msg:
                info["_filter_block"] = msg["filter_block"]
            # Per-symbol trace maps — each multi-instrument plugin
            # publishes one entry per symbol. The row builder emits a
            # separate (strategy, symbol) row per key.
            if "entry_traces_by_symbol" in msg:
                info.setdefault("_entry_traces_by_symbol", {}).update(
                    msg["entry_traces_by_symbol"])
            if "filter_traces_by_symbol" in msg:
                info.setdefault("_filter_traces_by_symbol", {}).update(
                    msg["filter_traces_by_symbol"])
            if "filter_blocks_by_symbol" in msg:
                # Overwrite rather than update — None values are
                # meaningful (current pass), and a stale dict-update
                # would keep a dead symbol forever.
                info["_filter_blocks_by_symbol"] = dict(
                    msg["filter_blocks_by_symbol"] or {})

            # Specification conditions and filters — arrays of condition
            # metadata (label, operator, threshold) and filter metadata
            # (label, threshold). These are static per strategy, stashed
            # for rendering as two distinct sections in the Conditions column.
            if "spec_conditions" in msg:
                info["_spec_conditions"] = msg["spec_conditions"] or []
            if "spec_filters" in msg:
                info["_spec_filters"] = msg["spec_filters"] or []
            # Live indicator values published on filter-block ticks (orchestrator
            # computes indicators even when entry eval is skipped). Used to show
            # observed values on grey/amber pending chips when entry didn't run.
            if "current_indicators" in msg:
                info["_current_indicators"] = msg["current_indicators"] or {}
            if "current_indicators_by_symbol" in msg:
                info["_current_indicators_by_symbol"] = msg["current_indicators_by_symbol"] or {}

            # Scenario membership — orchestrator's authoritative list
            # of {scenario: [strategies]}. Stash globally (not per
            # strategy) because the scenario filter pill lives above
            # the table and needs the full map.
            scen_strats = msg.get("scenario_strategies")
            if isinstance(scen_strats, dict) and scen_strats:
                self._scenario_strategies = scen_strats

            # Per-broker spread snapshot for the spread column.
            broker_spreads = msg.get("broker_spreads")
            if isinstance(broker_spreads, dict) and broker_spreads:
                self._broker_spreads = broker_spreads

            # Per-scenario spread gate snapshot. Shape:
            # {scenario_id: {symbol: {spread_bps, threshold_bps,
            # passed, data_source}}}. Empty for scenarios without
            # spread_max_bps set. GUI colours the scenario chips per
            # (strategy, symbol) row based on this.
            sgates = msg.get("scenario_spread_gates")
            if isinstance(sgates, dict) and sgates:
                self._scenario_spread_gates = sgates

    def _on_crypto_heartbeat(self, msg: dict):
        """Crypto paper-trader heartbeat — minimal liveness signal."""
        spec_id = msg.get("spec_id", "")
        if not spec_id:
            return
        with self._lock:
            self._crypto_strategies[spec_id] = {
                "spec_id": spec_id,
                "ts": msg.get("ts", time.time()),
                "chains": msg.get("chains", []),
                "halted": msg.get("halted", False),
                "last_heartbeat": time.time(),
                "fill_count": self._crypto_strategies.get(spec_id, {}).get("fill_count", 0),
                "last_fill_ts": self._crypto_strategies.get(spec_id, {}).get("last_fill_ts", 0),
            }

    def _on_crypto_block(self, msg: dict, topic: str):
        """Crypto feed: block tick per chain (topic: block.<chain>)."""
        chain = topic.split(".", 1)[1] if "." in topic else "unknown"
        with self._lock:
            self._crypto_chains[chain] = {
                "chain": chain,
                "number": msg.get("number", 0),
                "ts": msg.get("ts", time.time()),
                "last_tick": time.time(),
            }

    def _on_command(self, msg: dict):
        action = msg.get("action", "")
        if action == "stop":
            logger.info("GUI received stop command")


# ---------------------------------------------------------------------- #
# CLI entry point
# ---------------------------------------------------------------------- #

def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="ZmqGui dashboard")
    parser.add_argument("--config", type=str, default=None,
                        help="Path to config.yaml (defaults kick in if omitted)")
    parser.add_argument("--web-port", type=int, default=None,
                        help="Override web_port from config")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.web_port:
        cfg.web_port = args.web_port

    Dashboard(cfg).run()


if __name__ == "__main__":
    main()
