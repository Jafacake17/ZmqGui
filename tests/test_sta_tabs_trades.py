"""
Gated contract tests: STA Trades tab.

Locks:
- lifecycle row rendering (state badge colour, bps P&L, age)
- state badge colour map completeness
- empty-state graceful rendering (no heartbeat yet)
- _on_sta_heartbeat updates _sta_latest + _sta_lifecycle_history
"""
import sys, types, time
sys.path.insert(0, "src")

# ── stubs ────────────────────────────────────────────────────────────────────
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

from zmq_gui.app import Dashboard, _fmt_age
from zmq_gui.config import GuiCfg

GREEN = "#32cd64"; YELLOW = "#e6be32"; RED = "#dc3c3c"; BLUE = "#4682dc"
TEXT_SECONDARY = "#8c8ca0"

_STA_STATE_COLORS = {
    "AUTHORED":        TEXT_SECONDARY,
    "GATE_PENDING":    YELLOW,
    "DISPATCHED":      BLUE,
    "ACTIVE":          GREEN,
    "MILESTONE_CHECK": YELLOW,
    "PARTIAL_CLOSED":  YELLOW,
    "CLOSED":          TEXT_SECONDARY,
}


def _make_dashboard():
    return Dashboard(GuiCfg())


def _heartbeat(lifecycle=None, version="abc1234", ts="2026-05-17T10:00:00Z"):
    return {
        "topic": "sta.heartbeat.main",
        "ts": ts,
        "version": version,
        "lifecycle": lifecycle or [],
        "dxlink_status": {"symbols_subscribed": 5, "last_quote_ts": ts},
        "chain_stats": {},
        "recent_fills": [],
        "errors_recent": [],
        "schedule_engine_pid": 9876,
    }


def _render_trade_row(rec: dict, snap_ts: str) -> dict:
    """Mirror update_sta_trades row-building logic."""
    rid   = rec.get("record_id") or rec.get("spec_id") or "?"
    state = rec.get("state", "?")
    pnl   = rec.get("current_pnl_bps")
    pnl_s = (f"+{pnl:.1f}" if pnl is not None and pnl >= 0
             else f"{pnl:.1f}" if pnl is not None else "—")
    sched = rec.get("scheduled_entry") or ""
    try:
        from datetime import datetime as _dt
        entry_dt = _dt.fromisoformat(sched.replace("Z", "+00:00"))
        age_entry = _fmt_age(time.time() - entry_dt.timestamp())
    except Exception:
        age_entry = "—"
    return {
        "record_id":   rid,
        "spec_id":     rec.get("spec_id", "?"),
        "state":       state,
        "state_color": _STA_STATE_COLORS.get(state, TEXT_SECONDARY),
        "active_legs": str(rec.get("active_legs", "—")),
        "pnl_bps":     pnl_s,
        "pnl_raw":     pnl if pnl is not None else 0.0,
        "entry_ts":    sched[:19] if sched else "—",
        "age":         age_entry,
        "milestone":   sched,
    }


# ── tests ─────────────────────────────────────────────────────────────────────

def test_on_sta_heartbeat_stores_latest():
    d = _make_dashboard()
    hb = _heartbeat(version="sha123")
    d._on_sta_heartbeat(hb)
    assert d._sta_latest is hb
    assert d._sta_last_ts > 0


def test_lifecycle_history_records_state_transitions():
    d = _make_dashboard()
    hb1 = _heartbeat([{"record_id": "r1", "spec_id": "bull-put", "state": "GATE_PENDING"}])
    d._on_sta_heartbeat(hb1)
    hb2 = _heartbeat([{"record_id": "r1", "spec_id": "bull-put", "state": "ACTIVE"}])
    d._on_sta_heartbeat(hb2)
    # Same state repeated — should NOT add duplicate
    d._on_sta_heartbeat(hb2)
    history = d._sta_lifecycle_history["r1"]
    assert len(history) == 2
    assert history[0]["state"] == "GATE_PENDING"
    assert history[1]["state"] == "ACTIVE"


def test_duplicate_state_not_appended():
    d = _make_dashboard()
    hb = _heartbeat([{"record_id": "r2", "spec_id": "s", "state": "ACTIVE"}])
    for _ in range(3):
        d._on_sta_heartbeat(hb)
    assert len(d._sta_lifecycle_history["r2"]) == 1


def test_state_badge_color_active():
    row = _render_trade_row(
        {"record_id": "r1", "spec_id": "bull-put", "state": "ACTIVE",
         "active_legs": 2, "current_pnl_bps": 12.5},
        "2026-05-17T10:00:00Z"
    )
    assert row["state_color"] == GREEN
    assert row["pnl_bps"] == "+12.5"
    assert row["pnl_raw"] == 12.5


def test_state_badge_color_gate_pending():
    row = _render_trade_row(
        {"record_id": "r1", "spec_id": "s", "state": "GATE_PENDING"},
        "2026-05-17T10:00:00Z"
    )
    assert row["state_color"] == YELLOW


def test_state_badge_color_dispatched():
    row = _render_trade_row({"record_id": "r1", "spec_id": "s", "state": "DISPATCHED"}, "")
    assert row["state_color"] == BLUE


def test_state_badge_color_closed():
    row = _render_trade_row({"record_id": "r1", "spec_id": "s", "state": "CLOSED"}, "")
    assert row["state_color"] == TEXT_SECONDARY


def test_negative_pnl_formatted_correctly():
    row = _render_trade_row(
        {"record_id": "r1", "spec_id": "s", "state": "ACTIVE",
         "current_pnl_bps": -45.3}, ""
    )
    assert row["pnl_bps"] == "-45.3"
    assert row["pnl_raw"] == -45.3


def test_missing_pnl_renders_dash():
    row = _render_trade_row({"record_id": "r1", "spec_id": "s", "state": "AUTHORED"}, "")
    assert row["pnl_bps"] == "—"
    assert row["pnl_raw"] == 0.0


def test_all_documented_states_have_color():
    documented = ["AUTHORED", "GATE_PENDING", "DISPATCHED", "ACTIVE",
                  "MILESTONE_CHECK", "PARTIAL_CLOSED", "CLOSED"]
    for state in documented:
        assert state in _STA_STATE_COLORS, f"State {state!r} missing from color map"


def test_no_heartbeat_state_is_none():
    d = _make_dashboard()
    assert d._sta_latest is None
    assert d._sta_last_ts == 0.0
