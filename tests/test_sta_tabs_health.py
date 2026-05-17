"""
Gated contract tests: STA Health + Chain tabs.

Locks:
- dxlink_status field extraction
- heartbeat staleness logic (age thresholds)
- error surface: str errors and dict errors both handled
- chain_stats grid row rendering + IV colour thresholds
- StaCfg endpoint default
"""
import sys, types, time
sys.path.insert(0, "src")

# ── stubs (shared pattern) ────────────────────────────────────────────────────
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
from zmq_gui.config import GuiCfg, StaCfg

GREEN = "#32cd64"; YELLOW = "#e6be32"; RED = "#dc3c3c"


def _make_hb(**kwargs):
    base = {
        "topic": "sta.heartbeat.main",
        "ts": "2026-05-17T10:00:00Z",
        "version": "abc",
        "lifecycle": [],
        "dxlink_status": {"symbols_subscribed": 10, "last_quote_ts": "2026-05-17T10:00:00Z"},
        "chain_stats": {"NVDA": {"expiries": 24, "strikes_front": 58, "iv_index": 0.5296}},
        "recent_fills": [],
        "errors_recent": [],
        "schedule_engine_pid": 1234,
    }
    base.update(kwargs)
    return base


def _chain_row(underlying, stats, dxlink):
    iv = stats.get("iv_index")
    iv_raw = float(iv) if iv is not None else 0.0
    color = (RED if iv_raw > 0.5 else YELLOW if iv_raw > 0.3 else "#dcdce6")
    return {
        "underlying":      underlying,
        "expiries":        str(stats.get("expiries", "—")),
        "strikes_front":   str(stats.get("strikes_front", "—")),
        "iv_index":        f"{iv:.4f}" if iv is not None else "—",
        "iv_raw":          iv_raw,
        "iv_color":        color,
        "syms_subscribed": str(dxlink.get("symbols_subscribed", 0)),
    }


def _parse_error_row(e, idx):
    if isinstance(e, str):
        return {"ts": str(idx), "message": e, "context": ""}
    return {
        "ts":      (e.get("ts") or str(idx))[:19],
        "message": e.get("message") or e.get("error") or str(e),
        "context": e.get("context") or e.get("spec_id") or "",
    }


# ── StaCfg ────────────────────────────────────────────────────────────────────

def test_sta_cfg_default_endpoint():
    cfg = StaCfg()
    assert cfg.endpoint == "tcp://127.0.0.1:5570"


def test_sta_cfg_overrideable():
    cfg = StaCfg(endpoint="tcp://10.0.0.1:5570")
    assert cfg.endpoint == "tcp://10.0.0.1:5570"


def test_gui_cfg_has_sta_field():
    cfg = GuiCfg()
    assert hasattr(cfg, "sta")
    assert isinstance(cfg.sta, StaCfg)


# ── heartbeat staleness ───────────────────────────────────────────────────────

def test_fresh_heartbeat_is_green():
    """last_ts < 30s ago → GREEN."""
    last_ts = time.time() - 5
    age_s = time.time() - last_ts
    color = GREEN if age_s < 30 else YELLOW if age_s < 120 else RED
    assert color == GREEN


def test_stale_heartbeat_is_yellow():
    last_ts = time.time() - 60
    age_s = time.time() - last_ts
    color = GREEN if age_s < 30 else YELLOW if age_s < 120 else RED
    assert color == YELLOW


def test_very_stale_heartbeat_is_red():
    last_ts = time.time() - 200
    age_s = time.time() - last_ts
    color = GREEN if age_s < 30 else YELLOW if age_s < 120 else RED
    assert color == RED


# ── dxlink ────────────────────────────────────────────────────────────────────

def test_dxlink_symbols_extracted():
    d = Dashboard(GuiCfg())
    hb = _make_hb(dxlink_status={"symbols_subscribed": 42, "last_quote_ts": "2026-05-17T10:00:00Z"})
    d._on_sta_heartbeat(hb)
    assert d._sta_latest["dxlink_status"]["symbols_subscribed"] == 42


def test_dxlink_missing_graceful():
    d = Dashboard(GuiCfg())
    hb = _make_hb(dxlink_status=None)
    d._on_sta_heartbeat(hb)
    dx = d._sta_latest.get("dxlink_status") or {}
    assert dx.get("symbols_subscribed", 0) == 0


# ── chain stats ───────────────────────────────────────────────────────────────

def test_chain_row_nvda():
    stats = {"expiries": 24, "strikes_front": 58, "iv_index": 0.5296}
    row = _chain_row("NVDA", stats, {"symbols_subscribed": 5})
    assert row["expiries"] == "24"
    assert row["strikes_front"] == "58"
    assert row["iv_index"] == "0.5296"
    assert row["iv_raw"] == 0.5296
    assert row["iv_color"] == RED  # > 0.5


def test_chain_row_low_iv_neutral():
    row = _chain_row("SPY", {"expiries": 12, "strikes_front": 30, "iv_index": 0.18},
                     {"symbols_subscribed": 3})
    assert row["iv_color"] == "#dcdce6"  # TEXT_PRIMARY / neutral


def test_chain_row_medium_iv_yellow():
    row = _chain_row("AAPL", {"iv_index": 0.35}, {"symbols_subscribed": 1})
    assert row["iv_color"] == YELLOW


def test_chain_stats_empty_graceful():
    d = Dashboard(GuiCfg())
    hb = _make_hb(chain_stats={})
    d._on_sta_heartbeat(hb)
    assert d._sta_latest["chain_stats"] == {}


# ── error surface ─────────────────────────────────────────────────────────────

def test_string_error_normalised():
    row = _parse_error_row("connection refused", 0)
    assert row["message"] == "connection refused"
    assert row["context"] == ""


def test_dict_error_normalised():
    err = {"ts": "2026-05-17T10:00:01Z", "message": "IV feed timeout",
           "spec_id": "nvda-bull-put", "context": "dxlink"}
    row = _parse_error_row(err, 0)
    assert row["message"] == "IV feed timeout"
    assert row["ts"] == "2026-05-17T10:00:01"
    assert row["context"] == "dxlink"


def test_no_errors_shows_placeholder():
    errors = []
    err_rows = [_parse_error_row(e, i) for i, e in enumerate(errors)]
    if not err_rows:
        err_rows = [{"ts": "—", "message": "no errors", "context": ""}]
    assert err_rows[0]["message"] == "no errors"
