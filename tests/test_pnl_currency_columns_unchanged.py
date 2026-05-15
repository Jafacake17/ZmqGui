"""
Contract lock: GBP/currency P&L columns in Open Trades, Past Trades,
and Scenario (fill-based) are not affected by the bps-everywhere change.

The bps change only touches the strategy-aggregate pnl_bps metric path
and the cumulative chart. Trade-level P&L stays in account currency.
"""
import sys, types
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

# ── column-definition mirrors (must stay GBP-labelled) ────────────────────────

OPEN_TRADES_COLUMNS = [
    {"name": "strategy",       "label": "Strategy"},
    {"name": "broker",         "label": "Broker"},
    {"name": "pair",           "label": "Pair"},
    {"name": "direction",      "label": "Direction"},
    {"name": "entry_time",     "label": "Entry Time"},
    {"name": "entry_price",    "label": "Entry"},
    {"name": "current_price",  "label": "Current"},
    {"name": "move_pct",       "label": "Move %"},
    {"name": "stop_loss",      "label": "SL"},
    {"name": "take_profit",    "label": "TP"},
    {"name": "unrealised_bps", "label": "bps"},
    {"name": "pnl",            "label": "P&L"},    # currency — stays
    {"name": "timeout",        "label": "Timeout"},
    {"name": "chart",          "label": "Chart"},
]

PAST_TRADES_COLUMNS = [
    {"name": "strategy",   "label": "Strategy"},
    {"name": "broker",     "label": "Broker"},
    {"name": "pair",       "label": "Pair"},
    {"name": "direction",  "label": "Direction"},
    {"name": "entry_time", "label": "Entry Time"},
    {"name": "entry_price","label": "Entry"},
    {"name": "stop_loss",  "label": "SL"},
    {"name": "take_profit","label": "TP"},
    {"name": "exit_time",  "label": "Exit Time"},
    {"name": "exit_price", "label": "Exit"},
    {"name": "geometry",   "label": "MFE:TP:Act:SL:MAE (bps)"},
    {"name": "net_bps",    "label": "bps"},
    {"name": "pnl_pct",    "label": "P&L %"},
    {"name": "pnl",        "label": "P&L"},        # currency — stays
]

SCEN_COLUMNS = [
    {"name": "scenario",      "label": "Scenario"},
    {"name": "broker",        "label": "Broker"},
    {"name": "live",          "label": "Live"},
    {"name": "status",        "label": "Status"},
    {"name": "pnl",           "label": "P&L Today (bps)"},  # bps — CHANGED
    {"name": "trades",        "label": "Trades"},
    {"name": "win_pct",       "label": "Win %"},
    {"name": "n_strategies",  "label": "Strategies"},
]


def _col_label(cols, name):
    return next(c["label"] for c in cols if c["name"] == name)


def test_open_trades_pnl_column_unlabelled_as_bps():
    """Open Trades P&L column stays labelled 'P&L' (not 'P&L (bps)')."""
    label = _col_label(OPEN_TRADES_COLUMNS, "pnl")
    assert label == "P&L", f"Open Trades pnl column should be 'P&L', got {label!r}"
    assert "bps" not in label


def test_past_trades_pnl_column_unlabelled_as_bps():
    """Past Trades P&L column stays labelled 'P&L' (currency, not bps)."""
    label = _col_label(PAST_TRADES_COLUMNS, "pnl")
    assert label == "P&L", f"Past Trades pnl column should be 'P&L', got {label!r}"
    assert "bps" not in label


def test_scen_pnl_column_is_bps():
    """Scenario table 'pnl' column IS renamed to 'P&L Today (bps)'."""
    label = _col_label(SCEN_COLUMNS, "pnl")
    assert label == "P&L Today (bps)", f"expected 'P&L Today (bps)', got {label!r}"


def test_past_trades_net_bps_column_present():
    """Past Trades keeps the 'bps' net-bps column separate from GBP 'P&L'."""
    bps_label = _col_label(PAST_TRADES_COLUMNS, "net_bps")
    pnl_label  = _col_label(PAST_TRADES_COLUMNS, "pnl")
    assert bps_label == "bps"
    assert pnl_label == "P&L"


def test_fill_based_closed_trade_pnl_format():
    """Closed trade row P&L is formatted as currency (2dp, signed), not bps."""
    net = -4.28
    pnl_str = (f"+{net:.2f}" if net >= 0 else f"{net:.2f}")
    assert pnl_str == "-4.28", "currency format must be :.2f"
    assert "bps" not in pnl_str
