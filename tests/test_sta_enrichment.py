"""
Gated contract tests: STA tab enrichments.

Locks:
- _fmt_countdown: seconds → human countdown string
- _fmt_legs: leg list → condensed "SHORT 222.5P / LONG 215P NVDA" string
- _build_gate_chips: gate_trace → chip list (colour, label, blocker_reason)
- chain_status greeks-% colour thresholds (>80 green, 50-80 yellow, <50 red)
- DXLink freshness derivation from chain_status.last_quote_ts
- errors_recent ring-buffer normalisation (timestamp key, record_id key)
- Lifecycle gate_note extraction from gate_trace blockers
"""
import sys, types, time
sys.path.insert(0, "src")

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
    Dashboard, _fmt_countdown, _fmt_legs, _build_gate_chips,
)
from zmq_gui.config import GuiCfg

GREEN = "#32cd64"; YELLOW = "#e6be32"; RED = "#dc3c3c"; TEXT_SECONDARY = "#8c8ca0"


# ── _fmt_countdown ────────────────────────────────────────────────────────────

def test_countdown_none():
    assert _fmt_countdown(None) == "—"

def test_countdown_seconds():
    assert _fmt_countdown(45) == "45s"

def test_countdown_minutes():
    assert _fmt_countdown(150) == "2m 30s"

def test_countdown_hours():
    assert _fmt_countdown(5400) == "1h 30m"

def test_countdown_days():
    assert _fmt_countdown(90000) == "1d 1h"

def test_countdown_zero():
    assert _fmt_countdown(0) == "0s"


# ── _fmt_legs ─────────────────────────────────────────────────────────────────

def test_fmt_legs_empty():
    assert _fmt_legs([]) == "—"

def test_fmt_legs_single_put():
    legs = [{"side": "SELL", "qty": 1,
             "contract": {"underlying": "NVDA", "expiry": "2026-05-22",
                          "strike": 222.5, "right": "P"}}]
    result = _fmt_legs(legs)
    assert "SHORT" in result
    assert "222.5P" in result
    assert "NVDA" in result
    assert "2026-05-22" in result

def test_fmt_legs_spread():
    legs = [
        {"side": "SELL", "qty": 1,
         "contract": {"underlying": "NVDA", "expiry": "2026-05-22", "strike": 222.5, "right": "P"}},
        {"side": "BUY",  "qty": 1,
         "contract": {"underlying": "NVDA", "expiry": "2026-05-22", "strike": 215.0, "right": "P"}},
    ]
    result = _fmt_legs(legs)
    assert "SHORT 222.5P" in result
    assert "LONG 215.0P" in result
    assert "/" in result  # separator between legs


# ── _build_gate_chips ─────────────────────────────────────────────────────────

def test_gate_chips_empty():
    assert _build_gate_chips([]) == []

def test_gate_chips_passing():
    gate = [{"gate_id": "g1_rv_pct_rank", "indicator": "rv_pct_rank",
              "op": ">=", "threshold": 0.75, "observed": 0.84, "passed": True}]
    chips = _build_gate_chips(gate)
    assert len(chips) == 1
    assert chips[0]["color"] == GREEN
    assert "rv_pct_rank" in chips[0]["label"]
    assert "0.84" in chips[0]["label"]
    assert "0.75" in chips[0]["label"]

def test_gate_chips_failing_with_blocker():
    gate = [{"gate_id": "g2_iv_rank", "indicator": "iv_rank",
              "op": ">=", "threshold": 0.5, "observed": 0.3,
              "passed": False, "blocker_reason": "IV too low"}]
    chips = _build_gate_chips(gate)
    assert len(chips) == 1
    assert chips[0]["color"] == RED
    assert "IV too low" in chips[0]["label"]

def test_gate_chips_unknown_passed():
    gate = [{"gate_id": "g3", "indicator": "x", "op": ">", "threshold": 0.0,
              "passed": None}]
    chips = _build_gate_chips(gate)
    assert chips[0]["color"] == YELLOW

def test_gate_chips_multiple():
    gate = [
        {"gate_id": "g1", "indicator": "rv", "op": ">=", "threshold": 0.5, "observed": 0.6, "passed": True},
        {"gate_id": "g2", "indicator": "iv", "op": ">=", "threshold": 0.4, "observed": 0.2, "passed": False,
         "blocker_reason": "IV rank low"},
    ]
    chips = _build_gate_chips(gate)
    assert len(chips) == 2
    assert chips[0]["color"] == GREEN
    assert chips[1]["color"] == RED


# ── chain_status Greeks % colour ─────────────────────────────────────────────

def _greeks_pct_color(pct_raw):
    if pct_raw > 80: return GREEN
    if pct_raw > 50: return YELLOW
    if pct_raw >= 0: return RED
    return TEXT_SECONDARY  # sentinel for N/A

def test_greeks_pct_green():
    assert _greeks_pct_color(85.0) == GREEN

def test_greeks_pct_yellow():
    assert _greeks_pct_color(65.0) == YELLOW

def test_greeks_pct_red():
    assert _greeks_pct_color(30.0) == RED

def test_greeks_pct_not_available():
    assert _greeks_pct_color(-1.0) == TEXT_SECONDARY


# ── DXLink freshness from chain_status ───────────────────────────────────────

def _dxlink_freshness_color(age_s):
    return GREEN if age_s < 60 else YELLOW if age_s < 300 else RED

def test_dxlink_fresh():
    assert _dxlink_freshness_color(5) == GREEN

def test_dxlink_stale_yellow():
    assert _dxlink_freshness_color(120) == YELLOW

def test_dxlink_stale_red():
    assert _dxlink_freshness_color(400) == RED


# ── errors_recent ring buffer normalisation ───────────────────────────────────

def _normalise_error(e, idx):
    if isinstance(e, str):
        return {"ts": str(idx), "message": e, "record_id": ""}
    ts_raw = e.get("timestamp") or e.get("ts") or str(idx)
    return {
        "ts":        ts_raw[:19] if isinstance(ts_raw, str) else str(ts_raw),
        "message":   e.get("message") or e.get("error") or str(e),
        "record_id": e.get("record_id") or e.get("spec_id") or "",
    }

def test_error_timestamp_key():
    """Ring buffer uses 'timestamp' key (new format) — must be normalised."""
    e = {"timestamp": "2026-05-17T10:01:23Z", "message": "feed timeout",
         "record_id": "r1"}
    row = _normalise_error(e, 0)
    assert row["ts"] == "2026-05-17T10:01:23"
    assert row["record_id"] == "r1"
    assert row["message"] == "feed timeout"

def test_error_ts_key_fallback():
    e = {"ts": "2026-05-17T10:02:00Z", "message": "timeout", "spec_id": "bull-put"}
    row = _normalise_error(e, 0)
    assert row["ts"] == "2026-05-17T10:02:00"
    assert row["record_id"] == "bull-put"

def test_error_string_normalised():
    row = _normalise_error("connection reset", 3)
    assert row["ts"] == "3"
    assert row["message"] == "connection reset"
    assert row["record_id"] == ""


# ── lifecycle gate_note extraction ────────────────────────────────────────────

def _extract_gate_note(transition: dict) -> str:
    gate_trace = transition.get("gate_trace") or []
    blockers = [g.get("blocker_reason") or "" for g in gate_trace if not g.get("passed")]
    note = "; ".join(b for b in blockers if b)
    return note or transition.get("last_reason", "")

def test_gate_note_from_trace():
    t = {
        "state": "GATE_PENDING",
        "gate_trace": [
            {"gate_id": "g1", "passed": True, "blocker_reason": ""},
            {"gate_id": "g2", "passed": False, "blocker_reason": "IV rank < 0.5"},
        ]
    }
    note = _extract_gate_note(t)
    assert "IV rank < 0.5" in note

def test_gate_note_falls_back_to_last_reason():
    t = {"state": "GATE_PENDING", "gate_trace": [], "last_reason": "scheduled date not reached"}
    note = _extract_gate_note(t)
    assert note == "scheduled date not reached"

def test_gate_note_empty_when_all_passed():
    t = {
        "state": "GATE_PENDING",
        "gate_trace": [{"gate_id": "g1", "passed": True, "blocker_reason": ""}],
    }
    note = _extract_gate_note(t)
    assert note == ""

def test_lifecycle_history_stores_gate_trace():
    d = Dashboard(GuiCfg())
    hb = {
        "topic": "sta.heartbeat.main",
        "ts": "2026-05-17T10:00:00Z",
        "lifecycle": [{
            "record_id": "r1", "spec_id": "bull-put", "state": "GATE_PENDING",
            "gate_trace": [{"gate_id": "g1", "passed": False, "blocker_reason": "IV too low"}],
        }]
    }
    d._on_sta_heartbeat(hb)
    history = d._sta_lifecycle_history["r1"]
    assert history[0]["gate_trace"] is not None
    assert history[0]["gate_trace"][0]["blocker_reason"] == "IV too low"
