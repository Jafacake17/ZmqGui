"""Quick Screen tab — ad-hoc backtest builder over the Quick Screen RPC service."""
from __future__ import annotations

import uuid
from typing import Any

import zmq
from nicegui import run, ui

from ..theme import (
    BG_HEADER, BG_PANEL,
    GREEN, RED, YELLOW, BLUE,
    TEXT_PRIMARY, TEXT_SECONDARY, TEXT_MUTED,
)

QS_ENDPOINT = "tcp://127.0.0.1:5560"
_TIMEOUT_MS = 10_000
_PRICE_FIELDS = ["mid", "bid", "ask"]
_DOW_LABELS = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
_NTH_OPTS = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th", -1: "Last"}
_MAX_HISTORY = 10


# ── ZMQ transport ─────────────────────────────────────────────────────────────

def _qs_call(op: str, body: dict) -> tuple[bool, dict]:
    """Blocking REQ/REP — always use via run.io_bound."""
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, _TIMEOUT_MS)
    sock.setsockopt(zmq.SNDTIMEO, _TIMEOUT_MS)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(QS_ENDPOINT)
    try:
        sock.send_json({"op": op, "id": str(uuid.uuid4()), "body": body})
        resp = sock.recv_json()
        if resp.get("ok"):
            return True, resp.get("result", {})
        return False, resp.get("error", {"code": "unknown", "message": "no error field",
                                        "suggestions": []})
    except zmq.ZMQError as e:
        return False, {"code": "timeout", "field": None, "message": str(e),
                       "suggestions": ["Check service is running at tcp://127.0.0.1:5560"]}
    finally:
        sock.close()


def fetch_catalogue() -> dict | None:
    ok, result = _qs_call("discover", {})
    return result if ok else None


# ── Catalogue helpers ──────────────────────────────────────────────────────────

def _all_indicators(cat: dict, instrument: str) -> list[str]:
    inds = cat.get("indicators", {}).get(instrument, {})
    seen: set[str] = set()
    out: list[str] = []
    for group in ("main_table", "liquidity_sidecar", "econ_sidecar",
                  "orderbook_sidecar", "bar_zscore_sidecar", "derived_session_open"):
        for x in inds.get(group, []):
            if x not in seen:
                seen.add(x)
                out.append(x)
    return sorted(out)


def _ts_to_date(ts: str) -> str:
    """ISO timestamp → YYYY/MM/DD for NiceGUI date picker."""
    try:
        return ts[:10].replace("-", "/")
    except Exception:
        return ""


def _date_to_iso(d: str) -> str:
    return d.replace("/", "-") if d else ""


# ── Spec assembly ──────────────────────────────────────────────────────────────

def _try_num(v: Any) -> Any:
    if v is None or v == "":
        return 0
    try:
        f = float(v)
        return int(f) if f == int(f) else f
    except (TypeError, ValueError):
        return v


def _serialise_cond(c: dict) -> dict:
    t = c.get("type", "indicator")
    b: dict = {"type": t}
    if t in ("indicator", "indicator_cross"):
        b.update(name=c.get("name", ""), op=c.get("op", ">"),
                 value=_try_num(c.get("value", 0)))
    elif t == "time":
        b.update(field=c.get("field", "hour_utc"), op=c.get("op", ">="),
                 value=_try_num(c.get("value", 0)))
    elif t == "price":
        b.update(field=c.get("field", "mid"), op=c.get("op", ">"),
                 value=_try_num(c.get("value", 0)))
    elif t == "gap":
        b.update(field=c.get("field", "gap_bp"), op=c.get("op", ">="),
                 value=_try_num(c.get("value", 0)))
    elif t == "cross_instrument":
        b["instrument"] = c.get("ci_instrument", "")
        if c.get("ci_mode", "field") == "indicator":
            b["indicator"] = c.get("ci_indicator", "")
        else:
            b["field"] = c.get("ci_field", "mid")
        b.update(op=c.get("op", ">"), value=_try_num(c.get("value", 0)))
    elif t == "divergence":
        b.update(name=c.get("name", ""), bars=int(c.get("bars", 5) or 5),
                 direction=c.get("direction", "bull"))
    elif t == "pct_dist":
        b.update(lhs=c.get("lhs", ""), rhs=c.get("rhs", ""),
                 op=c.get("op", ">"), value=_try_num(c.get("value", 0)))
    elif t == "pct_change":
        b.update(target=c.get("target", ""), bars=int(c.get("bars", 5) or 5),
                 op=c.get("op", ">"), value=_try_num(c.get("value", 0)))
    elif t == "diff":
        b.update(lhs=c.get("lhs", ""), rhs=c.get("rhs", ""),
                 op=c.get("op", ">"), value=_try_num(c.get("value", 0)))
    elif t == "range_position":
        b.update(target=c.get("target", ""), low=c.get("low", ""),
                 high=c.get("high", ""), op=c.get("op", ">"),
                 value=_try_num(c.get("value", 0.5)))
    elif t == "bars_since_cross":
        b.update(indicator=c.get("indicator", ""), op=c.get("op", ">="),
                 value=_try_num(c.get("value", 0)),
                 max_bars=int(c.get("max_bars", 10) or 10))
    elif t == "duration_condition":
        b.update(indicator=c.get("indicator", ""), op=c.get("op", ">"),
                 value=_try_num(c.get("value", 0)),
                 min_bars=int(c.get("min_bars", 5) or 5))
    elif t == "any_of":
        b["conditions"] = [_serialise_cond(ic) for ic in c.get("inner_conds", [])
                           if ic.get("type")]
    return b


def _serialise_filter(f: dict) -> dict:
    t = f.get("type", "")
    b: dict = {"type": t}
    if t == "cooldown":
        b["ticks"] = int(f.get("ticks", 100) or 100)
    elif t == "session":
        b["allowed"] = f.get("allowed", [])
    elif t == "warmup":
        b["ticks"] = int(f.get("ticks", 200) or 200)
    elif t == "spread_max":
        if f.get("spread_mode") == "atr_multiple":
            b["atr_multiple"] = _try_num(f.get("spread_value", 0.5))
        else:
            b["value"] = _try_num(f.get("spread_value", 0.00015))
    elif t == "volatility":
        b.update(indicator=f.get("vol_indicator", "atr_h1_14"),
                 min=_try_num(f.get("vol_min", 0)),
                 max=_try_num(f.get("vol_max", 0)))
    elif t == "day_of_week":
        b["allowed"] = sorted(int(x) for x in f.get("allowed_days", []))
    elif t == "calendar":
        days = []
        for part in str(f.get("month_days_str", "")).split(","):
            try:
                days.append(int(part.strip()))
            except ValueError:
                pass
        b["month_days"] = days
    elif t == "max_trades_per_day":
        b["limit"] = int(f.get("limit", 3) or 3)
    elif t == "nth_weekday_of_month":
        b.update(nth=int(f.get("nth", 1) or 1), weekday=int(f.get("weekday", 0) or 0))
    elif t == "session_window":
        b.update(start=f.get("sw_start", "07:00"), end=f.get("sw_end", "09:00"))
    elif t == "vol_shock_block":
        b.update(short=f.get("vsb_short", "realised_vol_m5"),
                 long=f.get("vsb_long", "realised_vol_h1"),
                 ratio=_try_num(f.get("vsb_ratio", 1.8)))
    return b


_IND_COND_FIELDS: dict[str, tuple[str, ...]] = {
    "indicator":          ("name",),
    "indicator_cross":    ("name",),
    "divergence":         ("name",),
    "bars_since_cross":   ("indicator",),
    "duration_condition": ("indicator",),
    "cross_instrument":   ("indicator",),
}


def _collect_indicator_refs(conds: list[dict]) -> set[str]:
    """Walk serialised conditions and collect indicator name references."""
    refs: set[str] = set()
    for c in (conds or []):
        ctype = c.get("type", "")
        for fld in _IND_COND_FIELDS.get(ctype, ()):
            v = c.get(fld)
            if isinstance(v, str) and v and not v.startswith("$"):
                refs.add(v)
        for inner_key in ("conditions", "any_of"):
            inner = c.get(inner_key)
            if isinstance(inner, list):
                refs |= _collect_indicator_refs(inner)
    return refs


def _build_request_body(state: dict) -> dict:
    entry_conds = [_serialise_cond(c) for c in state["entry_conds"] if c.get("type")]
    filters = [_serialise_filter(f) for f in state["filters"] if f.get("type")]
    kgs = [{"metric": kg["metric"], "threshold": _try_num(kg.get("threshold", 0)),
             "comparison": kg["comparison"]}
           for kg in state["kill_gates"] if kg.get("metric") and kg.get("comparison")]
    # Param overrides: flat dict for body resolution, spec dict for spec_engine.
    params_flat = {p["name"]: _try_num(p["value"]) for p in state["params"] if p.get("name")}
    params_spec = {k: {"default": v, "fixed": True} for k, v in params_flat.items()}
    # Auto-declare any indicator columns referenced in conditions so
    # spec_engine's DISPATCH 11 check sees them as resolvable.
    ind_refs = _collect_indicator_refs(entry_conds)
    indicators_block = [{"name": name} for name in sorted(ind_refs)]
    spec = {
        "id": "zmqgui-quick-screen",
        "thesis": "ad-hoc screen",
        "category": state.get("category", "flow"),
        "counterparty": "",
        "instruments": [state["instrument"]],
        "params": params_spec,
        "indicators": indicators_block,
        "entry": {"side": state["side"], "conditions": entry_conds},
        "exit": {
            "tp": {"type": state["tp_type"], "value": _try_num(state["tp_value"])},
            "sl": {"type": state["sl_type"], "value": _try_num(state["sl_value"])},
            "timeout": {state["timeout_unit"]: int(state["timeout_value"] or 0)},
        },
        "filters": filters,
        "kill_gates": kgs,
    }
    return {
        "instrument": state["instrument"],
        "start": _date_to_iso(state["start"]),
        "end": _date_to_iso(state["end"]),
        "params": params_flat or None,
        "spec": spec,
    }


def _validate(state: dict, unavail_cond: set, unavail_filt: set) -> str:
    if not state.get("instrument"):
        return "Select an instrument"
    if not state.get("start"):
        return "Set start date"
    if not state.get("end"):
        return "Set end date"
    if not state["entry_conds"]:
        return "Add at least one entry condition"
    for c in state["entry_conds"]:
        if c.get("type") in unavail_cond:
            return f"Condition type '{c['type']}' is not available"
    for f in state["filters"]:
        if f.get("type") in unavail_filt:
            return f"Filter type '{f['type']}' is not available"
    if not state.get("tp_value"):
        return "Set TP value"
    if not state.get("sl_value"):
        return "Set SL value"
    if not state.get("timeout_value"):
        return "Set timeout value"
    return ""


# ── UI builder ─────────────────────────────────────────────────────────────────

def build_quick_screen_tab(cat: dict) -> None:
    """Build the full Quick Screen tab UI. Call inside an active NiceGUI context."""

    # ── Catalogue unpacking ──────────────────────────────────────────────────
    instr_info = {i["id"]: i for i in cat["instruments"]}
    instruments = [i["id"] for i in cat["instruments"]]

    cond_types_raw = cat.get("condition_types", [])
    unavail_cond_types: set[str] = {c["name"] for c in cond_types_raw if not c.get("available", True)}
    cond_type_opts: dict[str, str] = {
        c["name"]: c["name"] if c.get("available", True) else f"{c['name']} [unavailable]"
        for c in cond_types_raw
    }

    filter_types_raw = cat.get("filter_types", [])
    unavail_filter_types: dict[str, str] = {
        f["name"]: f.get("unavailable_reason", "unavailable")
        for f in filter_types_raw if not f.get("available", True)
    }
    filter_type_opts: dict[str, str] = {
        f["name"]: f["name"] if f.get("available", True) else f"{f['name']} [unavailable]"
        for f in filter_types_raw
    }

    operators = cat.get("operators", [">", ">=", "<", "<=", "==", "!=", "between", "contains"])
    sides = cat.get("sides", ["buy", "sell", "dynamic"])
    exit_types = cat.get("exit_types", ["pct", "absolute", "atr_multiple"])
    timeout_units = cat.get("exit_timeout_units", ["hours", "seconds", "bars"])
    time_fields = cat.get("time_fields", [])
    session_names = cat.get("session_names", [])
    kg_metrics = cat.get("kill_gate_metrics", [])
    kg_comparisons = cat.get("kill_gate_comparisons", ["above", "below"])
    categories = cat.get("categories", ["flow", "news", "proven-quant"])

    # ── Mutable form state ───────────────────────────────────────────────────
    state: dict = {
        "instrument": instruments[0] if instruments else None,
        "start": _ts_to_date(instr_info[instruments[0]]["min_ts"]) if instruments else "",
        "end": _ts_to_date(instr_info[instruments[0]]["max_ts"]) if instruments else "",
        "side": "buy",
        "category": "flow",
        "entry_conds": [],
        "filters": [],
        "tp_type": "pct", "tp_value": 0.003,
        "sl_type": "pct", "sl_value": 0.002,
        "timeout_unit": "hours", "timeout_value": 8,
        "kill_gates": [],
        "params": [],
        "running": False,
        "result": None,
        "error": None,
        "history": [],
    }

    def get_inds() -> list[str]:
        inst = state["instrument"]
        return _all_indicators(cat, inst) if inst else []

    # ── Refreshable sections ─────────────────────────────────────────────────

    def _cond_fields(c: dict, inds: list[str], is_inner: bool = False) -> None:
        """Render type-dependent fields for one condition row. Inline (no refresh)."""
        t = c.get("type", "indicator")
        s = f"color: {TEXT_PRIMARY};"
        ns = f"color: {TEXT_PRIMARY}; min-width: 140px;"

        if t in ("indicator", "indicator_cross"):
            ui.select(options=inds, value=c.get("name") or (inds[0] if inds else None),
                      label="indicator", on_change=lambda e: c.update(name=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "time":
            ui.select(options=time_fields, value=c.get("field", time_fields[0] if time_fields else ""),
                      label="field", on_change=lambda e: c.update(field=e.value),
                      ).style(s).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">="), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "price":
            ui.select(options=_PRICE_FIELDS, value=c.get("field", "mid"), label="field",
                      on_change=lambda e: c.update(field=e.value),
                      ).style(s).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "gap":
            gap_inds = [x for x in inds if "gap" in x] or inds
            ui.select(options=gap_inds, value=c.get("field", gap_inds[0] if gap_inds else ""),
                      label="field", on_change=lambda e: c.update(field=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">="), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "cross_instrument":
            ui.select(options=instruments, value=c.get("ci_instrument", instruments[0] if instruments else ""),
                      label="instrument", on_change=lambda e: c.update(ci_instrument=e.value),
                      ).style(s).props("dense options-dense")
            ui.select(options=["field", "indicator"], value=c.get("ci_mode", "field"),
                      label="mode",
                      on_change=lambda e, c=c: (
                          c.update(ci_mode=e.value),
                          render_entry_conds.refresh()
                      ),
                      ).style(s).props("dense options-dense")
            if c.get("ci_mode", "field") == "indicator":
                ui.select(options=inds, value=c.get("ci_indicator") or (inds[0] if inds else None),
                          label="indicator", on_change=lambda e: c.update(ci_indicator=e.value),
                          ).style(ns).props("dense options-dense")
            else:
                ui.select(options=_PRICE_FIELDS, value=c.get("ci_field", "mid"),
                          label="field", on_change=lambda e: c.update(ci_field=e.value),
                          ).style(s).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "divergence":
            ui.select(options=inds, value=c.get("name") or (inds[0] if inds else None),
                      label="indicator", on_change=lambda e: c.update(name=e.value),
                      ).style(ns).props("dense options-dense")
            ui.number(label="bars", value=c.get("bars", 5),
                      on_change=lambda e: c.update(bars=int(e.value or 5)),
                      ).style(s).props("dense").classes("w-20")
            ui.select(options=["bull", "bear"], value=c.get("direction", "bull"),
                      label="direction", on_change=lambda e: c.update(direction=e.value),
                      ).style(s).props("dense options-dense")

        elif t in ("pct_dist", "diff"):
            ui.select(options=inds, value=c.get("lhs") or (inds[0] if inds else None),
                      label="lhs", on_change=lambda e: c.update(lhs=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=inds, value=c.get("rhs") or (inds[0] if inds else None),
                      label="rhs", on_change=lambda e: c.update(rhs=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "pct_change":
            ui.select(options=inds, value=c.get("target") or (inds[0] if inds else None),
                      label="target", on_change=lambda e: c.update(target=e.value),
                      ).style(ns).props("dense options-dense")
            ui.number(label="bars", value=c.get("bars", 5),
                      on_change=lambda e: c.update(bars=int(e.value or 5)),
                      ).style(s).props("dense").classes("w-20")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "range_position":
            ui.select(options=inds, value=c.get("target") or (inds[0] if inds else None),
                      label="target", on_change=lambda e: c.update(target=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=inds, value=c.get("low") or (inds[0] if inds else None),
                      label="low", on_change=lambda e: c.update(low=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=inds, value=c.get("high") or (inds[0] if inds else None),
                      label="high", on_change=lambda e: c.update(high=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0.5),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")

        elif t == "bars_since_cross":
            ui.select(options=inds, value=c.get("indicator") or (inds[0] if inds else None),
                      label="indicator", on_change=lambda e: c.update(indicator=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">="), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")
            ui.number(label="max_bars", value=c.get("max_bars", 10),
                      on_change=lambda e: c.update(max_bars=int(e.value or 10)),
                      ).style(s).props("dense").classes("w-24")

        elif t == "duration_condition":
            ui.select(options=inds, value=c.get("indicator") or (inds[0] if inds else None),
                      label="indicator", on_change=lambda e: c.update(indicator=e.value),
                      ).style(ns).props("dense options-dense")
            ui.select(options=operators, value=c.get("op", ">"), label="op",
                      on_change=lambda e: c.update(op=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=c.get("value", 0),
                      on_change=lambda e: c.update(value=e.value),
                      ).style(s).props("dense").classes("w-24")
            ui.number(label="min_bars", value=c.get("min_bars", 5),
                      on_change=lambda e: c.update(min_bars=int(e.value or 5)),
                      ).style(s).props("dense").classes("w-24")

        elif t == "any_of" and not is_inner:
            # Inner conditions (one level deep; no recursive any_of)
            inner = c.setdefault("inner_conds", [])
            with ui.column().classes("w-full pl-6 border-l-2").style(
                    "border-color: #44446a;"):
                ui.label("any_of — inner conditions (OR)").style(
                    f"color: {TEXT_SECONDARY}; font-size: 11px;")
                for j, ic in enumerate(inner):
                    with ui.row().classes("items-center gap-2 flex-wrap"):
                        ui.button("✕", on_click=lambda _, j=j: (
                            inner.pop(j), render_entry_conds.refresh()
                        )).props("dense flat").style(f"color: {RED}; min-width: 28px;")
                        ui.select(
                            options=cond_type_opts, value=ic.get("type", "indicator"),
                            label="type",
                            on_change=lambda e, ic=ic: (
                                ic.update(type=e.value),
                                render_entry_conds.refresh()
                            ),
                        ).style(f"color: {TEXT_PRIMARY}; min-width: 160px;").props("dense options-dense")
                        if ic.get("type") not in unavail_cond_types:
                            _cond_fields(ic, inds, is_inner=True)
                        else:
                            ui.label(f"type unavailable").style(f"color: {RED}; font-size: 11px;")
                ui.button("+ inner condition", on_click=lambda _: (
                    inner.append({"type": "indicator"}),
                    render_entry_conds.refresh()
                )).props("dense flat").style(f"color: {BLUE};")

        elif t in unavail_cond_types:
            ui.label(f"type not available").style(f"color: {RED}; font-size: 11px;")

    def _filter_fields(f: dict) -> None:
        t = f.get("type", "")
        s = f"color: {TEXT_PRIMARY};"
        ns = f"color: {TEXT_PRIMARY}; min-width: 160px;"

        if t in unavail_filter_types:
            reason = unavail_filter_types[t]
            ui.label(f"Not available: {reason}").style(
                f"color: {RED}; font-size: 11px;").props(f'title="{reason}"')
            return

        if t == "cooldown":
            ui.number(label="ticks", value=f.get("ticks", 100),
                      on_change=lambda e: f.update(ticks=int(e.value or 100)),
                      ).style(s).props("dense").classes("w-28")
        elif t == "session":
            ui.select(options=session_names, value=f.get("allowed", []),
                      label="allowed sessions", multiple=True,
                      on_change=lambda e: f.update(allowed=e.value),
                      ).style(ns).props("dense options-dense")
        elif t == "warmup":
            ui.number(label="ticks", value=f.get("ticks", 200),
                      on_change=lambda e: f.update(ticks=int(e.value or 200)),
                      ).style(s).props("dense").classes("w-28")
        elif t == "spread_max":
            ui.select(options=["absolute", "atr_multiple"], value=f.get("spread_mode", "absolute"),
                      label="mode", on_change=lambda e: f.update(spread_mode=e.value),
                      ).style(s).props("dense options-dense")
            ui.number(label="value", value=f.get("spread_value", 0.00015),
                      on_change=lambda e: f.update(spread_value=e.value),
                      ).style(s).props("dense").classes("w-32")
        elif t == "volatility":
            ui.input(label="indicator", value=f.get("vol_indicator", "atr_h1_14"),
                     on_change=lambda e: f.update(vol_indicator=e.value),
                     ).style(ns).props("dense")
            ui.number(label="min", value=f.get("vol_min", 0),
                      on_change=lambda e: f.update(vol_min=e.value),
                      ).style(s).props("dense").classes("w-24")
            ui.number(label="max", value=f.get("vol_max", 0),
                      on_change=lambda e: f.update(vol_max=e.value),
                      ).style(s).props("dense").classes("w-24")
        elif t == "day_of_week":
            dow_opts = {str(k): v for k, v in _DOW_LABELS.items()}
            cur = [str(x) for x in f.get("allowed_days", [])]
            ui.select(options=dow_opts, value=cur, label="allowed days", multiple=True,
                      on_change=lambda e: f.update(allowed_days=[int(x) for x in e.value]),
                      ).style(ns).props("dense options-dense")
        elif t == "calendar":
            ui.input(label="month_days (e.g. -1,-2,15)", value=f.get("month_days_str", ""),
                     on_change=lambda e: f.update(month_days_str=e.value),
                     ).style(ns).props("dense")
        elif t == "max_trades_per_day":
            ui.number(label="limit", value=f.get("limit", 3),
                      on_change=lambda e: f.update(limit=int(e.value or 3)),
                      ).style(s).props("dense").classes("w-24")
        elif t == "nth_weekday_of_month":
            ui.select(options=_NTH_OPTS, value=f.get("nth", 1), label="nth",
                      on_change=lambda e: f.update(nth=int(e.value)),
                      ).style(s).props("dense options-dense")
            ui.select(options=_DOW_LABELS, value=f.get("weekday", 0), label="weekday",
                      on_change=lambda e: f.update(weekday=int(e.value)),
                      ).style(s).props("dense options-dense")
        elif t == "session_window":
            ui.input(label="start (HH:MM UTC)", value=f.get("sw_start", "07:00"),
                     on_change=lambda e: f.update(sw_start=e.value),
                     ).style(s).props("dense").classes("w-32")
            ui.input(label="end (HH:MM UTC)", value=f.get("sw_end", "09:00"),
                     on_change=lambda e: f.update(sw_end=e.value),
                     ).style(s).props("dense").classes("w-32")
        elif t == "vol_shock_block":
            ui.input(label="short indicator", value=f.get("vsb_short", "realised_vol_m5"),
                     on_change=lambda e: f.update(vsb_short=e.value),
                     ).style(ns).props("dense")
            ui.input(label="long indicator", value=f.get("vsb_long", "realised_vol_h1"),
                     on_change=lambda e: f.update(vsb_long=e.value),
                     ).style(ns).props("dense")
            ui.number(label="ratio threshold", value=f.get("vsb_ratio", 1.8),
                      on_change=lambda e: f.update(vsb_ratio=e.value),
                      ).style(s).props("dense").classes("w-28")

    @ui.refreshable
    def render_entry_conds() -> None:
        inds = get_inds()
        conds = state["entry_conds"]
        if not conds:
            ui.label("No entry conditions — add one below.").style(
                f"color: {TEXT_SECONDARY}; font-style: italic; font-size: 13px;")
        for i, c in enumerate(conds):
            with ui.row().classes("items-start gap-2 flex-wrap w-full").style(
                    f"background-color: {BG_PANEL}; border-radius: 4px; padding: 6px;"):
                ui.button("✕", on_click=lambda _, i=i: (
                    state["entry_conds"].pop(i), render_entry_conds.refresh()
                )).props("dense flat").style(f"color: {RED}; min-width: 28px;")
                ui.select(
                    options=cond_type_opts, value=c.get("type", "indicator"), label="type",
                    on_change=lambda e, i=i: (
                        state["entry_conds"][i].update(type=e.value),
                        render_entry_conds.refresh()
                    ),
                ).style(f"color: {TEXT_PRIMARY}; min-width: 170px;").props("dense options-dense")
                _cond_fields(c, inds)

    @ui.refreshable
    def render_filters() -> None:
        items = state["filters"]
        if not items:
            ui.label("No filters — add one below.").style(
                f"color: {TEXT_SECONDARY}; font-style: italic; font-size: 13px;")
        for i, f in enumerate(items):
            with ui.row().classes("items-start gap-2 flex-wrap w-full").style(
                    f"background-color: {BG_PANEL}; border-radius: 4px; padding: 6px;"):
                ui.button("✕", on_click=lambda _, i=i: (
                    state["filters"].pop(i), render_filters.refresh()
                )).props("dense flat").style(f"color: {RED}; min-width: 28px;")
                ui.select(
                    options=filter_type_opts, value=f.get("type", "cooldown"), label="type",
                    on_change=lambda e, i=i: (
                        state["filters"][i].update(type=e.value),
                        render_filters.refresh()
                    ),
                ).style(f"color: {TEXT_PRIMARY}; min-width: 170px;").props("dense options-dense")
                _filter_fields(f)

    @ui.refreshable
    def render_kill_gates() -> None:
        items = state["kill_gates"]
        if not items:
            ui.label("No kill gates.").style(
                f"color: {TEXT_SECONDARY}; font-style: italic; font-size: 13px;")
        for i, kg in enumerate(items):
            with ui.row().classes("items-center gap-2 flex-wrap"):
                ui.button("✕", on_click=lambda _, i=i: (
                    state["kill_gates"].pop(i), render_kill_gates.refresh()
                )).props("dense flat").style(f"color: {RED}; min-width: 28px;")
                ui.select(options=kg_metrics, value=kg.get("metric", kg_metrics[0]),
                          label="metric",
                          on_change=lambda e, i=i: state["kill_gates"][i].update(metric=e.value),
                          ).style(f"color: {TEXT_PRIMARY}; min-width: 160px;").props("dense options-dense")
                ui.number(label="threshold", value=kg.get("threshold", 0),
                          on_change=lambda e, i=i: state["kill_gates"][i].update(threshold=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-28")
                ui.select(options=kg_comparisons, value=kg.get("comparison", "below"),
                          label="comparison",
                          on_change=lambda e, i=i: state["kill_gates"][i].update(comparison=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense options-dense")

    @ui.refreshable
    def render_params() -> None:
        items = state["params"]
        if not items:
            ui.label("No param overrides.").style(
                f"color: {TEXT_SECONDARY}; font-style: italic; font-size: 13px;")
        for i, p in enumerate(items):
            with ui.row().classes("items-center gap-2"):
                ui.button("✕", on_click=lambda _, i=i: (
                    state["params"].pop(i), render_params.refresh()
                )).props("dense flat").style(f"color: {RED}; min-width: 28px;")
                ui.input(label="name ($PARAM)", value=p.get("name", ""),
                         on_change=lambda e, i=i: state["params"][i].update(name=e.value),
                         ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-36")
                ui.input(label="value", value=str(p.get("value", "")),
                         on_change=lambda e, i=i: state["params"][i].update(value=e.value),
                         ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-36")

    @ui.refreshable
    def render_result() -> None:
        res = state.get("result")
        err = state.get("error")
        if res is None and err is None:
            return
        if err:
            with ui.card().classes("w-full").style(
                    f"background-color: {BG_PANEL}; border: 1px solid {RED};"):
                ui.label("Error").style(f"color: {RED}; font-weight: bold; font-size: 14px;")
                code = err.get("code", "")
                field = err.get("field", "")
                msg = err.get("message", "")
                sugg = err.get("suggestions", [])
                ui.label(f"Code: {code}" + (f"  |  Field: {field}" if field else "")).style(
                    f"color: {TEXT_SECONDARY}; font-size: 12px;")
                ui.label(msg).style(f"color: {TEXT_PRIMARY};")
                if sugg:
                    ui.label("Suggestions:").style(f"color: {TEXT_SECONDARY}; font-size: 12px;")
                    for s in sugg:
                        ui.label(f"  • {s}").style(f"color: {TEXT_SECONDARY}; font-size: 12px;")
            return

        verdict = res.get("verdict", "")
        verdict_color = GREEN if verdict == "pass" else (YELLOW if verdict == "borderline" else RED)
        engine = res.get("engine_used", "")
        elapsed = res.get("elapsed_ms", 0)
        n_trades = res.get("n_trades", 0)
        wr = res.get("wr", 0)
        bwr = res.get("breakeven_wr", 0)
        gap = res.get("wr_minus_breakeven", 0)
        exec_wr = res.get("exec_wr_pass", False)
        sharpe = res.get("sharpe_ratio", 0)
        avg_pnl = res.get("avg_trade_pnl", 0)
        kgs = res.get("kill_gates", [])

        with ui.card().classes("w-full").style(
                f"background-color: {BG_PANEL}; border: 1px solid {verdict_color};"):
            with ui.row().classes("items-center gap-4 flex-wrap"):
                ui.label(verdict.upper()).style(
                    f"color: {verdict_color}; font-weight: bold; font-size: 18px;")
                engine_color = BLUE if engine == "sql" else TEXT_SECONDARY
                ui.label(engine).style(
                    f"color: {engine_color}; font-size: 11px; border: 1px solid {engine_color};"
                    " border-radius: 3px; padding: 1px 5px;")
                ui.label(f"{elapsed:.0f} ms").style(f"color: {TEXT_SECONDARY}; font-size: 12px;")

            with ui.grid(columns=4).classes("w-full gap-2 mt-2"):
                for label, val in [
                    ("Trades", str(n_trades)),
                    ("Win Rate", f"{wr:.1%}"),
                    ("Breakeven WR", f"{bwr:.1%}"),
                    ("WR − BEven", f"{gap:+.1%}"),
                    ("Exec WR Pass", "YES" if exec_wr else "NO"),
                    ("Sharpe", f"{sharpe:.2f}"),
                    ("Avg Trade P&L", f"{avg_pnl:.4f}"),
                ]:
                    with ui.column().classes("gap-0").style(
                            f"background-color: {BG_PANEL}; padding: 4px 8px; border-radius: 4px;"):
                        ui.label(label).style(f"color: {TEXT_SECONDARY}; font-size: 11px;")
                        col = (GREEN if (label == "Exec WR Pass" and exec_wr) or
                               (label == "WR − BEven" and gap >= 0) else
                               RED if (label == "Exec WR Pass" and not exec_wr) or
                               (label == "WR − BEven" and gap < 0) else TEXT_PRIMARY)
                        ui.label(val).style(f"color: {col}; font-weight: bold;")

            # Kill gate table
            if kgs:
                ui.label("Kill Gates").classes("mt-3").style(
                    f"color: {TEXT_SECONDARY}; font-size: 12px; font-weight: bold;")
                kg_cols = [
                    {"name": "metric", "label": "Metric", "field": "metric", "align": "left"},
                    {"name": "threshold", "label": "Threshold", "field": "threshold", "align": "right"},
                    {"name": "comparison", "label": "vs", "field": "comparison", "align": "center"},
                    {"name": "observed", "label": "Observed", "field": "observed", "align": "right"},
                    {"name": "passed", "label": "Pass", "field": "passed", "align": "center"},
                ]
                kg_rows = [
                    {"metric": kg["metric"], "threshold": kg["threshold"],
                     "comparison": kg["comparison"],
                     "observed": f"{kg['observed']:.4f}" if isinstance(kg.get("observed"), float) else str(kg.get("observed", "")),
                     "passed": "✓" if kg.get("passed") else "✗",
                     "_passed": kg.get("passed", False)}
                    for kg in kgs
                ]
                kg_table = ui.table(columns=kg_cols, rows=kg_rows, row_key="metric").style(
                    f"background-color: {BG_PANEL};")
                kg_table.add_slot("body-cell-passed", f"""
                    <q-td :props="props">
                        <span :style="{{color: props.row._passed ? '{GREEN}' : '{RED}', fontWeight: 'bold'}}">
                            {{{{ props.row.passed }}}}
                        </span>
                    </q-td>
                """)

            # Full stats collapsible
            stats = res.get("stats", {})
            if stats:
                with ui.expansion("Full stats", icon="analytics").classes("w-full mt-2"):
                    stats_rows = [{"key": k, "value": str(v)} for k, v in sorted(stats.items())]
                    ui.table(
                        columns=[
                            {"name": "key", "label": "Stat", "field": "key", "align": "left"},
                            {"name": "value", "label": "Value", "field": "value", "align": "right"},
                        ],
                        rows=stats_rows, row_key="key",
                    ).classes("w-full").style(f"background-color: {BG_PANEL};")

    @ui.refreshable
    def render_history() -> None:
        hist = state["history"]
        if not hist:
            return
        ui.label("Recent runs").style(
            f"color: {TEXT_SECONDARY}; font-size: 12px; font-weight: bold;")
        h_cols = [
            {"name": "ts", "label": "Time", "field": "ts", "align": "left"},
            {"name": "instrument", "label": "Instrument", "field": "instrument", "align": "left"},
            {"name": "verdict", "label": "Verdict", "field": "verdict", "align": "center"},
            {"name": "n_trades", "label": "Trades", "field": "n_trades", "align": "right"},
            {"name": "elapsed_ms", "label": "ms", "field": "elapsed_ms", "align": "right"},
        ]
        h_rows = []
        for idx, h in enumerate(hist):
            row = {**h, "_idx": idx,
                   "ts": h.get("ts", ""),
                   "verdict": h.get("verdict", ""),
                   "n_trades": h.get("n_trades", ""),
                   "elapsed_ms": f"{h.get('elapsed_ms', 0):.0f}"}
            if h.get("error"):
                row["verdict"] = "error"
            h_rows.append(row)
        h_table = ui.table(columns=h_cols, rows=h_rows, row_key="ts").style(
            f"background-color: {BG_PANEL};")
        h_table.add_slot("body-cell-verdict", f"""
            <q-td :props="props">
                <span :style="{{color: props.row.verdict === 'pass' ? '{GREEN}'
                    : props.row.verdict === 'borderline' ? '{YELLOW}' : '{RED}'}}">
                    {{{{ props.row.verdict }}}}
                </span>
            </q-td>
        """)
        ui.label("Click a row to reload its request into the form.").style(
            f"color: {TEXT_MUTED}; font-size: 11px;")

        def reload_from_history(e) -> None:
            idx = e.args.get("row", {}).get("_idx")
            if idx is None:
                return
            h = state["history"][idx]
            saved = h.get("_state_snapshot")
            if not saved:
                return
            for k in ("instrument", "start", "end", "side", "category",
                       "entry_conds", "filters", "tp_type", "tp_value",
                       "sl_type", "sl_value", "timeout_unit", "timeout_value",
                       "kill_gates", "params"):
                if k in saved:
                    state[k] = saved[k]
            state["result"] = None
            state["error"] = None
            render_entry_conds.refresh()
            render_filters.refresh()
            render_kill_gates.refresh()
            render_params.refresh()
            render_result.refresh()

        h_table.on("rowClick", reload_from_history)

    # ── Run logic ────────────────────────────────────────────────────────────

    async def do_run() -> None:
        err_msg = _validate(state, unavail_cond_types, set(unavail_filter_types))
        if err_msg:
            _val_label.set_text(err_msg)
            state["error"] = {"code": "validation", "field": None, "message": err_msg,
                              "suggestions": []}
            state["result"] = None
            render_result.refresh()
            return
        _val_label.set_text("")

        state["running"] = True
        run_btn.props("loading")
        state["result"] = None
        state["error"] = None
        render_result.refresh()

        body = _build_request_body(state)
        ok, payload = await run.io_bound(_qs_call, "quick_screen", body)

        state["running"] = False
        run_btn.props(remove="loading")

        import copy, time as _time
        ts = _time.strftime("%H:%M:%S")
        hist_entry: dict = {"ts": ts, "instrument": state["instrument"],
                             "_state_snapshot": copy.deepcopy(state)}
        if ok:
            state["result"] = payload
            state["error"] = None
            hist_entry.update(verdict=payload.get("verdict", ""),
                               n_trades=payload.get("n_trades", ""),
                               elapsed_ms=payload.get("elapsed_ms", 0))
        else:
            state["error"] = payload
            state["result"] = None
            hist_entry["error"] = True

        state["history"].insert(0, hist_entry)
        if len(state["history"]) > _MAX_HISTORY:
            state["history"].pop()

        render_result.refresh()
        render_history.refresh()

    # ── Page layout ──────────────────────────────────────────────────────────

    with ui.column().classes("w-full gap-4").style(f"color: {TEXT_PRIMARY};"):

        # ── 1. Header row ────────────────────────────────────────────────────
        ui.label("Quick Screen").style(
            f"color: {TEXT_PRIMARY}; font-weight: bold; font-size: 16px;").classes("mt-2")

        with ui.row().classes("items-end gap-4 flex-wrap"):
            instr_sel = ui.select(
                options=instruments,
                value=state["instrument"],
                label="Instrument",
            ).style(f"color: {TEXT_PRIMARY}; min-width: 130px;").props("dense options-dense")

            start_input = ui.input(
                label="Start (YYYY/MM/DD)", value=state["start"],
                on_change=lambda e: state.update(start=e.value),
            ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-36")

            end_input = ui.input(
                label="End (YYYY/MM/DD)", value=state["end"],
                on_change=lambda e: state.update(end=e.value),
            ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-36")

            ui.select(
                options=sides, value=state["side"], label="Side",
                on_change=lambda e: state.update(side=e.value),
            ).style(f"color: {TEXT_PRIMARY}; min-width: 90px;").props("dense options-dense")

            ui.select(
                options=categories, value=state["category"], label="Category",
                on_change=lambda e: state.update(category=e.value),
            ).style(f"color: {TEXT_PRIMARY}; min-width: 120px;").props("dense options-dense")

        # Instrument change: update state + date inputs + refresh builders.
        def _on_instrument_change(e) -> None:
            inst = e.value
            info = instr_info.get(inst, {})
            new_start = _ts_to_date(info.get("min_ts", ""))
            new_end = _ts_to_date(info.get("max_ts", ""))
            state.update(instrument=inst, start=new_start, end=new_end)
            start_input.value = new_start
            end_input.value = new_end
            render_entry_conds.refresh()
            render_filters.refresh()

        instr_sel.on("update:model-value", _on_instrument_change)

        # Date range hint (updates when instrument changes)
        def _date_range_hint(inst: str) -> str:
            info = instr_info.get(inst, {})
            mn = _ts_to_date(info.get("min_ts", ""))
            mx = _ts_to_date(info.get("max_ts", ""))
            rows = instr_info.get(inst, {}).get("n_rows", 0)
            return f"Available: {mn} → {mx}  ({rows:,} rows)" if mn and mx else ""

        _range_hint = ui.label(_date_range_hint(state["instrument"] or "")).style(
            f"color: {TEXT_MUTED}; font-size: 11px;"
        )

        def _update_hint(e) -> None:
            _range_hint.set_text(_date_range_hint(e.value))

        instr_sel.on("update:model-value", _update_hint)

        # ── 2. Entry conditions ──────────────────────────────────────────────
        with ui.card().classes("w-full").style(f"background-color: {BG_PANEL};"):
            ui.label("Entry Conditions").style(
                f"color: {TEXT_PRIMARY}; font-weight: bold;").classes("mb-2")
            render_entry_conds()
            ui.button("+ Add condition", on_click=lambda: (
                state["entry_conds"].append({"type": "indicator"}),
                render_entry_conds.refresh()
            )).props("flat dense").style(f"color: {BLUE}; margin-top: 6px;")

        # ── 3. Filters ───────────────────────────────────────────────────────
        with ui.card().classes("w-full").style(f"background-color: {BG_PANEL};"):
            ui.label("Filters").style(
                f"color: {TEXT_PRIMARY}; font-weight: bold;").classes("mb-2")
            render_filters()
            ui.button("+ Add filter", on_click=lambda: (
                state["filters"].append({"type": "cooldown"}),
                render_filters.refresh()
            )).props("flat dense").style(f"color: {BLUE}; margin-top: 6px;")

        # ── 4. Exit ──────────────────────────────────────────────────────────
        with ui.card().classes("w-full").style(f"background-color: {BG_PANEL};"):
            ui.label("Exit").style(f"color: {TEXT_PRIMARY}; font-weight: bold;").classes("mb-2")
            with ui.row().classes("items-end gap-4 flex-wrap"):
                ui.label("TP:").style(f"color: {TEXT_SECONDARY}; align-self: center;")
                ui.select(options=exit_types, value=state["tp_type"], label="type",
                          on_change=lambda e: state.update(tp_type=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense options-dense")
                ui.number(label="value", value=state["tp_value"],
                          on_change=lambda e: state.update(tp_value=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-28")

                ui.label("SL:").style(f"color: {TEXT_SECONDARY}; align-self: center;")
                ui.select(options=exit_types, value=state["sl_type"], label="type",
                          on_change=lambda e: state.update(sl_type=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense options-dense")
                ui.number(label="value", value=state["sl_value"],
                          on_change=lambda e: state.update(sl_value=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-28")

                ui.label("Timeout:").style(f"color: {TEXT_SECONDARY}; align-self: center;")
                ui.number(label="value", value=state["timeout_value"],
                          on_change=lambda e: state.update(timeout_value=int(e.value or 0)),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense").classes("w-24")
                ui.select(options=timeout_units, value=state["timeout_unit"], label="unit",
                          on_change=lambda e: state.update(timeout_unit=e.value),
                          ).style(f"color: {TEXT_PRIMARY};").props("dense options-dense")

        # ── 5. Kill gates ────────────────────────────────────────────────────
        with ui.card().classes("w-full").style(f"background-color: {BG_PANEL};"):
            ui.label("Kill Gates (optional)").style(
                f"color: {TEXT_PRIMARY}; font-weight: bold;").classes("mb-2")
            render_kill_gates()
            ui.button("+ Add kill gate", on_click=lambda: (
                state["kill_gates"].append({"metric": kg_metrics[0], "threshold": 0,
                                            "comparison": "below"}),
                render_kill_gates.refresh()
            )).props("flat dense").style(f"color: {BLUE}; margin-top: 6px;")

        # ── 6. Param overrides ───────────────────────────────────────────────
        with ui.card().classes("w-full").style(f"background-color: {BG_PANEL};"):
            ui.label("Param Overrides (optional)").style(
                f"color: {TEXT_PRIMARY}; font-weight: bold;").classes("mb-2")
            render_params()
            ui.button("+ Add param", on_click=lambda: (
                state["params"].append({"name": "", "value": ""}),
                render_params.refresh()
            )).props("flat dense").style(f"color: {BLUE}; margin-top: 6px;")

        # ── 7. Run button ────────────────────────────────────────────────────
        with ui.row().classes("items-center gap-3"):
            run_btn = ui.button("Run", on_click=do_run).style(
                f"background-color: {GREEN}; color: #000; font-weight: bold; min-width: 100px;"
            )
            _val_label = ui.label("").style(
                f"color: {YELLOW}; font-size: 12px; font-style: italic;"
            )

        # ── 8. Result panel ──────────────────────────────────────────────────
        render_result()

        # ── 9. History strip ─────────────────────────────────────────────────
        render_history()
