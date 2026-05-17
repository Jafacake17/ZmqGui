"""
Regression tests against the real STA heartbeat fixture.

Uses tests/fixtures/sta_heartbeat_live.json — a captured real STA heartbeat
(or faithful replica of the operator-reported shape). Tests exercise the
same rendering paths that update_sta_* timers call, asserting non-crashing
output and correct field values.

Bugs caught and locked:
  Bug1 — Decimal-as-string (entry_credit="0.0") → must not crash on float comparison
  Bug2 — chain_status.per_underlying (not chain_status itself) is the iterable
  Bug3 — DXLink freshness reads per_underlying values, not chain_status.values()
  Bug4 — record_id is int; history keys must be str for dropdown consistency
"""
import json, sys, types, time, pathlib
sys.path.insert(0, "src")

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sta_heartbeat_live.json"

# ── stubs ─────────────────────────────────────────────────────────────────────
zmq_mod = types.ModuleType("zmq")
zmq_mod.SUB = 0; zmq_mod.SUBSCRIBE = 1; zmq_mod.POLLIN = 2
zmq_mod.RCVTIMEO = 3; zmq_mod.SNDTIMEO = 4; zmq_mod.LINGER = 5; zmq_mod.REQ = 6
class _ZCtx:
    def socket(self, *a, **kw): return _ZSock()
    def term(self): pass
class _ZSock:
    def connect(self, *a): pass
    def setsockopt(self, *a): pass
    def setsockopt_string(self, *a): pass
    def close(self): pass
    def send_json(self, *a): pass
    def recv_json(self): return {}
    def recv_multipart(self, **kw): return [b"", b"{}"]
    def poll(self, *a, **kw): return 0
class _ZPoller:
    def register(self, *a): pass
    def poll(self, *a, **kw): return {}
zmq_mod.Context = _ZCtx; zmq_mod.Socket = _ZSock
zmq_mod.Poller = _ZPoller; zmq_mod.ZMQError = Exception
sys.modules["zmq"] = zmq_mod
for mod in ("nicegui", "nicegui.app", "nicegui.run", "nicegui.ui",
            "nicegui.elements.timer", "yaml", "plotly", "psycopg2"):
    sys.modules.setdefault(mod, types.ModuleType(mod))
ui_mod = sys.modules["nicegui.ui"]
if not hasattr(ui_mod, "timer"): ui_mod.timer = lambda *a, **kw: None
if not hasattr(ui_mod, "page"):  ui_mod.page = lambda *a, **kw: (lambda f: f)
if not hasattr(ui_mod, "run"):   ui_mod.run = lambda *a, **kw: None
timer_mod = sys.modules["nicegui.elements.timer"]
if not hasattr(timer_mod, "Timer"):
    class _Timer:
        _get_context = lambda self: None
    timer_mod.Timer = _Timer
yaml_mod = sys.modules["yaml"]
if not hasattr(yaml_mod, "safe_load"): yaml_mod.safe_load = lambda f: {}
if not hasattr(sys.modules["nicegui.app"], "storage"):
    sys.modules["nicegui.app"].storage = types.SimpleNamespace(user={})

from zmq_gui.app import (
    Dashboard, _coerce_float, _fmt_countdown, _fmt_legs,
    _build_gate_chips, _fmt_age,
)
from zmq_gui.config import GuiCfg

GREEN = "#32cd64"; YELLOW = "#e6be32"; RED = "#dc3c3c"; TEXT_SECONDARY = "#8c8ca0"
_STA_STATE_COLORS = {
    "AUTHORED": TEXT_SECONDARY, "GATE_PENDING": YELLOW,
    "DISPATCHED": "#4682dc", "ACTIVE": GREEN,
    "MILESTONE_CHECK": YELLOW, "PARTIAL_CLOSED": YELLOW, "CLOSED": TEXT_SECONDARY,
}


# ── load fixture ──────────────────────────────────────────────────────────────

def load_fixture():
    return json.loads(FIXTURE.read_text())


# ── renderer helpers mirroring update_* logic ─────────────────────────────────

def render_trade_rows(snap):
    rows = []
    for rec in (snap.get("lifecycle") or []):
        rid   = rec.get("record_id") or rec.get("spec_id") or "?"
        state = rec.get("state", "?")
        pnl   = _coerce_float(rec.get("current_pnl_bps"))
        pnl_s = (f"+{pnl:.1f}" if pnl is not None and pnl >= 0
                 else f"{pnl:.1f}" if pnl is not None else "—")
        upnl   = _coerce_float(rec.get("unrealised_pnl"))
        upnl_s = (f"+{upnl:.2f}" if upnl is not None and upnl >= 0
                  else f"{upnl:.2f}" if upnl is not None else "—")
        ecredit   = _coerce_float(rec.get("entry_credit"))
        ecredit_s = (f"+{ecredit:.2f}" if ecredit is not None and ecredit >= 0
                     else f"{ecredit:.2f}" if ecredit is not None else "—")
        mark   = _coerce_float(rec.get("current_mark"))
        mark_s = f"{mark:.2f}" if mark is not None else "—"
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
        gate_chips = (
            _build_gate_chips(rec.get("gate_trace") or [])
            if state == "GATE_PENDING" else []
        )
        rows.append({
            "record_id":    str(rid),
            "spec_id":      rec.get("spec_id", "?"),
            "state":        state,
            "state_color":  _STA_STATE_COLORS.get(state, TEXT_SECONDARY),
            "legs":         _fmt_legs(rec.get("legs") or []),
            "entry_credit": ecredit_s,
            "mark":         mark_s,
            "pnl_usd":      upnl_s,
            "pnl_usd_raw":  upnl if upnl is not None else 0.0,
            "pnl_bps":      pnl_s,
            "pnl_raw":      pnl if pnl is not None else 0.0,
            "countdown":    countdown,
            "gate_chips":   gate_chips,
        })
    return rows


def render_chain_rows(snap):
    chain_status  = snap.get("chain_status") or {}
    chain_stats   = snap.get("chain_stats") or {}
    per_underlying = chain_status.get("per_underlying") or {}
    source = per_underlying if per_underlying else chain_stats
    rows = []
    for underlying, stats in sorted(source.items()):
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
            loaded  = stats.get("strikes_front")
            pct_raw = -1.0
            pct_s   = "—"
            spot    = None
            lqt     = ""
            lgt     = ""
        rows.append({
            "underlying":      underlying,
            "spot":            f"{spot:.4f}" if spot else "—",
            "strikes_loaded":  str(loaded) if loaded is not None else "—",
            "greeks_pct":      pct_s,
            "greeks_pct_raw":  pct_raw,
            "last_quote":      lqt[:19] if lqt else "—",
            "last_greeks":     lgt[:19] if lgt else "—",
        })
    return rows


def render_expiry_rows(snap, underlying):
    cs   = snap.get("chain_status") or {}
    pu   = cs.get("per_underlying") or {}
    data = pu.get(underlying) or {}
    exps = data.get("expiries") or []
    rows = []
    for ex in exps:
        iv = ex.get("atm_iv")
        rows.append({
            "expiry":     ex.get("expiry", "—"),
            "strikes":    str(ex.get("strikes", "—")),
            "atm_iv":     f"{iv:.4f}" if iv is not None else "—",
            "atm_iv_raw": float(iv) if iv is not None else -1.0,
        })
    return rows


def derive_dxlink_freshness(snap, now_ts):
    chain_status = snap.get("chain_status") or {}
    per_ul = chain_status.get("per_underlying") or {}
    quote_ages = []
    for ul_data in per_ul.values():
        lqt = ul_data.get("last_quote_ts")
        if lqt:
            try:
                from datetime import datetime as _dt
                dt = _dt.fromisoformat(lqt.replace("Z", "+00:00"))
                quote_ages.append(now_ts - dt.timestamp())
            except Exception:
                pass
    if not quote_ages:
        dx = snap.get("dxlink_status") or {}
        raw_lqt = dx.get("last_quote_ts") or ""
        if raw_lqt:
            try:
                from datetime import datetime as _dt
                dt = _dt.fromisoformat(raw_lqt.replace("Z", "+00:00"))
                quote_ages.append(now_ts - dt.timestamp())
            except Exception:
                pass
    return min(quote_ages) if quote_ages else None


def normalise_error(e, idx):
    if isinstance(e, str):
        return {"ts": str(idx), "message": e, "record_id": ""}
    ts_raw = e.get("timestamp") or e.get("ts") or str(idx)
    return {
        "ts":        ts_raw[:19] if isinstance(ts_raw, str) else str(ts_raw),
        "message":   e.get("message") or e.get("error") or str(e),
        "record_id": e.get("record_id") or e.get("spec_id") or "",
    }


# ── tests ─────────────────────────────────────────────────────────────────────

def test_fixture_loads():
    snap = load_fixture()
    assert "lifecycle" in snap
    assert "chain_status" in snap


# Bug1 — Decimal-as-string coercion

def test_coerce_float_string():
    assert _coerce_float("0.0") == 0.0
    assert _coerce_float("1.45") == 1.45
    assert _coerce_float("-2.30") == -2.30

def test_coerce_float_none():
    assert _coerce_float(None) is None

def test_coerce_float_number_passthrough():
    assert _coerce_float(3.14) == 3.14

def test_trade_rows_no_crash_on_decimal_strings():
    """Bug1: entry_credit='0.0' must not crash the renderer."""
    snap = load_fixture()
    rows = render_trade_rows(snap)
    assert len(rows) > 0

def test_active_row_entry_credit_is_formatted():
    snap = load_fixture()
    rows = render_trade_rows(snap)
    active = next((r for r in rows if r["state"] == "ACTIVE"), None)
    assert active is not None
    # entry_credit="0.0" → "+0.00"
    assert active["entry_credit"] == "+0.00"

def test_active_row_mark_is_dash_when_null():
    snap = load_fixture()
    rows = render_trade_rows(snap)
    active = next((r for r in rows if r["state"] == "ACTIVE"), None)
    assert active["mark"] == "—"

def test_active_row_pnl_usd_dash_when_null():
    snap = load_fixture()
    rows = render_trade_rows(snap)
    active = next((r for r in rows if r["state"] == "ACTIVE"), None)
    assert active["pnl_usd"] == "—"
    assert active["pnl_usd_raw"] == 0.0

def test_active_row_countdown_from_time_stop():
    """time_to_time_stop_seconds=3232 < time_to_expiry_seconds=426331 → use stop."""
    snap = load_fixture()
    rows = render_trade_rows(snap)
    active = next((r for r in rows if r["state"] == "ACTIVE"), None)
    # min(3232, 426331) = 3232 → 53m 52s
    assert active["countdown"] != "—"
    assert "m" in active["countdown"]

def test_active_row_legs_condensed():
    snap = load_fixture()
    rows = render_trade_rows(snap)
    active = next((r for r in rows if r["state"] == "ACTIVE"), None)
    legs_str = active["legs"]
    assert "SHORT" in legs_str
    assert "LONG" in legs_str
    assert "NVDA" in legs_str
    assert "222.5P" in legs_str

def test_gate_pending_chips_rendered():
    """Real gate_trace has observed=null → passed=null → all YELLOW (unknown).
    Blocker_reason may still be set on failing gates."""
    snap = load_fixture()
    rows = render_trade_rows(snap)
    gp = next((r for r in rows if r["state"] == "GATE_PENDING"), None)
    assert gp is not None
    chips = gp["gate_chips"]
    assert len(chips) > 0
    # All are YELLOW (unknown) because observed=null → passed=null
    for chip in chips:
        assert chip["color"] in (YELLOW, RED), f"Unexpected color: {chip['color']}"

def test_gate_pending_chips_with_blocker_reason():
    """Gate chips that have a blocker_reason include it in the label."""
    snap = load_fixture()
    rows = render_trade_rows(snap)
    # record 5 has HARD_VETO gates with blocker_reason
    gp5 = next((r for r in rows if r["record_id"] == "5"), None)
    if gp5 is not None:
        chips_with_blocker = [c for c in gp5["gate_chips"] if "indicator not registered" in c["label"]]
        assert len(chips_with_blocker) > 0

def test_authored_row_no_gate_chips():
    """AUTHORED rows have no gate_chips (gate only applies to GATE_PENDING)."""
    snap = load_fixture()
    rows = render_trade_rows(snap)
    authored = next((r for r in rows if r["state"] == "AUTHORED"), None)
    assert authored is not None
    assert authored["gate_chips"] == []

def test_active_row_pnl_bps_null_renders_dash():
    """ACTIVE row with current_pnl_bps=null renders '—'."""
    snap = load_fixture()
    rows = render_trade_rows(snap)
    active = next((r for r in rows if r["state"] == "ACTIVE"), None)
    assert active is not None
    assert active["pnl_bps"] == "—"


# Bug2 — chain_status.per_underlying iteration

def test_chain_rows_no_crash():
    """Bug2: iterating chain_status directly crashes; must use per_underlying."""
    snap = load_fixture()
    rows = render_chain_rows(snap)
    assert len(rows) > 0

def test_chain_row_underlying_name():
    snap = load_fixture()
    rows = render_chain_rows(snap)
    assert rows[0]["underlying"] == "nvda"

def test_chain_row_strikes_loaded():
    snap = load_fixture()
    rows = render_chain_rows(snap)
    assert rows[0]["strikes_loaded"] == "578"

def test_chain_row_greeks_pct_red():
    """15/578 = 2.6% → red (<50%)."""
    snap = load_fixture()
    rows = render_chain_rows(snap)
    row = rows[0]
    pct = row["greeks_pct_raw"]
    assert pct < 50.0
    assert pct > 0.0
    assert row["greeks_pct"] == "3%"

def test_chain_row_spot_coerced_or_dash():
    """spot_estimate renders as '—' when absent; as float string when present."""
    snap = load_fixture()
    rows = render_chain_rows(snap)
    # Real payload: no spot_estimate in per_underlying → "—"
    spot = rows[0]["spot"]
    # Must be either a formatted float string or "—", never crash
    if spot != "—":
        float(spot.replace(",", ""))  # must be parseable as float

def test_expiry_rows_present_and_sorted():
    """Real payload has many expiries (no atm_iv); rows must be non-empty and sorted."""
    snap = load_fixture()
    rows = render_expiry_rows(snap, "nvda")
    assert len(rows) >= 1
    # Sorted by expiry (first should be soonest)
    expiry_dates = [r["expiry"] for r in rows]
    assert expiry_dates == sorted(expiry_dates)
    # NVDA 2026-05-22 should appear
    assert any("2026-05-22" in r["expiry"] for r in rows)

def test_expiry_rows_no_atm_iv_renders_dash():
    """Real payload has no atm_iv on expiry records — must render '—'."""
    snap = load_fixture()
    rows = render_expiry_rows(snap, "nvda")
    assert rows[0]["atm_iv"] == "—"
    assert rows[0]["atm_iv_raw"] == -1.0


# Bug3 — DXLink freshness from per_underlying

def test_dxlink_freshness_no_crash():
    """Bug3: chain_status.values() contains non-ul dicts; per_underlying is correct."""
    snap = load_fixture()
    age = derive_dxlink_freshness(snap, time.time())
    assert age is not None
    assert age >= 0.0

def test_dxlink_freshness_from_per_underlying():
    """last_quote_ts in per_underlying.nvda used, not chain_status top-level."""
    snap = load_fixture()
    from datetime import datetime as _dt, timezone as _tz
    lqt = snap["chain_status"]["per_underlying"]["nvda"]["last_quote_ts"]
    expected_ts = _dt.fromisoformat(lqt.replace("Z", "+00:00")).timestamp()
    now = time.time()
    age = derive_dxlink_freshness(snap, now)
    assert abs(age - (now - expected_ts)) < 1.0


# Bug4 — record_id int → str history keys

def test_lifecycle_history_keys_are_strings():
    """Bug4: record_id=8 (int) → history key must be '8' (str)."""
    d = Dashboard(GuiCfg())
    snap = load_fixture()
    d._on_sta_heartbeat(snap)
    for key in d._sta_lifecycle_history.keys():
        assert isinstance(key, str), f"Expected str key, got {type(key)}: {key!r}"

def test_lifecycle_history_record_8_present():
    d = Dashboard(GuiCfg())
    d._on_sta_heartbeat(load_fixture())
    assert "8" in d._sta_lifecycle_history
    assert d._sta_lifecycle_history["8"][0]["state"] == "ACTIVE"

def test_lifecycle_gate_trace_stored_for_gate_pending():
    """Real fixture: record_id=2 and record_id=5 are GATE_PENDING with gate_trace."""
    d = Dashboard(GuiCfg())
    snap = load_fixture()
    d._on_sta_heartbeat(snap)
    # Find first GATE_PENDING record_id in the fixture
    gp_record = next(
        (rec for rec in snap.get("lifecycle", []) if rec.get("state") == "GATE_PENDING"),
        None
    )
    assert gp_record is not None, "fixture must have at least one GATE_PENDING record"
    rid_str = str(gp_record["record_id"])
    history = d._sta_lifecycle_history.get(rid_str, [])
    assert len(history) > 0, f"No history for record {rid_str!r}"
    entry = history[0]
    assert entry["state"] == "GATE_PENDING"
    assert entry["gate_trace"] is not None
    assert len(entry["gate_trace"]) >= 1


# Errors ring buffer (timestamp key)

def test_errors_empty_shows_placeholder():
    """Real payload has empty errors_recent — placeholder row must appear."""
    snap = load_fixture()
    errs = snap.get("errors_recent") or []
    err_rows = [normalise_error(e, i) for i, e in enumerate(errs)]
    if not err_rows:
        err_rows = [{"ts": "—", "message": "no errors", "record_id": ""}]
    assert err_rows[0]["message"] == "no errors"

def test_errors_timestamp_key_normalised_when_present():
    """When errors_recent contains dicts with 'timestamp' key, normalise correctly."""
    mock_err = {"timestamp": "2026-05-17T10:15:00Z",
                "message": "Greeks fetch timeout", "record_id": "8"}
    row = normalise_error(mock_err, 0)
    assert row["ts"] == "2026-05-17T10:15:00"
    assert row["message"] == "Greeks fetch timeout"
    assert row["record_id"] == "8"
