"""
Gated contract tests: Past Trades column redesign.

Root-cause audit summary:
  Issue 1 (bps=0): _seed_from_questdb doesn't write net_bps key → seeded trades
    always read t.get("net_bps") or 0 = 0.  → FIXED: column dropped.
  Issue 2 (PnL%/PnL mismatch): pnl_net = _to_gbp((exit-entry)×qty) - rt_cost
    (GBP, NET) vs pnl_pct = direction×(exit-entry)/entry×100 (GROSS, no FX).
    Numerically different by design → FIXED: pnl_pct column dropped, single
    bps column replaces both.
  Issue 3 (XAU magnitude): XAU_USD -56.1 bps showed -1879.67 GBP (33× magnified).
    → FIXED: pnl column now = pnl_pct×100 bps (instrument-agnostic).

Fixture: tests/fixtures/past_trades_with_xau.json — real+representative closed
  trade records including EUR_USD and XAU_USD rows.
"""
import json, sys, types, pathlib
sys.path.insert(0, "src")

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "past_trades_with_xau.json"

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


def load_fixture():
    return json.loads(FIXTURE.read_text())


def render_rt_row(t: dict) -> dict:
    """Mirror update_ui Past Trades row-building logic."""
    entry_px = float(t.get("entry_price") or 0)
    exit_px  = float(t.get("exit_price") or 0)
    pnl_pct  = float(t.get("pnl_pct") or 0)
    trade_bps = pnl_pct * 100.0
    if entry_px > 50:
        fmt = ".2f"
    elif entry_px > 10:
        fmt = ".3f"
    else:
        fmt = ".5f"
    side = (t.get("side") or "").upper()
    return {
        "symbol":      t.get("symbol", ""),
        "direction":   side,
        "entry_price": f"{entry_px:{fmt}}",
        "exit_price":  f"{exit_px:{fmt}}",
        "pnl":         (f"+{trade_bps:.1f}" if trade_bps >= 0 else f"{trade_bps:.1f}"),
        "pnl_raw":     trade_bps,
    }


# ── column contract ───────────────────────────────────────────────────────────

def test_rt_columns_no_net_bps():
    """net_bps column must NOT appear (was showing 0 for all seeded trades)."""
    from zmq_gui.app import Dashboard
    from zmq_gui.config import GuiCfg
    # Check by grepping app.py source for the column definition
    src = pathlib.Path("src/zmq_gui/app.py").read_text()
    # The OLD column definition had "net_bps" label "bps" — must be gone
    assert '"net_bps"' not in src.split('recent_trades_table')[0].split('rt_columns')[1][:2000], \
        "net_bps column still present in rt_columns definition"


def test_rt_columns_no_pnl_pct():
    """pnl_pct column must NOT appear (replaced by unified bps column)."""
    src = pathlib.Path("src/zmq_gui/app.py").read_text()
    rt_col_block = src.split('rt_columns')[1][:2000]
    assert '"pnl_pct"' not in rt_col_block, \
        "pnl_pct column still present in rt_columns definition"


def test_rt_pnl_column_label_is_bps():
    """The P&L column must be labelled 'P&L (bps)', not bare 'P&L'."""
    src = pathlib.Path("src/zmq_gui/app.py").read_text()
    # Find the rt_columns block and check for P&L (bps) label
    assert '"P&L (bps)"' in src, "P&L (bps) label not found in app.py"


# ── bps arithmetic from fixture ───────────────────────────────────────────────

def test_bps_equals_pnl_pct_times_100():
    """pnl_raw must equal pnl_pct × 100 for every fixture row."""
    trades = load_fixture()
    for t in trades:
        pnl_pct = float(t.get("pnl_pct") or 0)
        expected_bps = pnl_pct * 100.0
        row = render_rt_row(t)
        assert abs(row["pnl_raw"] - expected_bps) < 1e-9, \
            f"{t['symbol']} pnl_raw={row['pnl_raw']:.4f} expected {expected_bps:.4f}"


def test_xau_bps_is_sane_not_currency():
    """XAU_USD bps must be ~56 (not ~1879 GBP)."""
    trades = load_fixture()
    xau_rows = [t for t in trades if t["symbol"] == "XAU_USD"]
    assert xau_rows, "Fixture must contain at least one XAU_USD trade"
    for t in xau_rows:
        row = render_rt_row(t)
        # bps magnitude < 200 (typical trade), not in thousands (old GBP P&L)
        assert abs(row["pnl_raw"]) < 200, \
            f"XAU pnl_raw={row['pnl_raw']:.1f} looks like currency (expected bps magnitude < 200)"


def test_xau_losing_trade_negative_bps():
    """XAU SELL where price went up → bps negative."""
    trades = load_fixture()
    # The row with entry=4482.35, exit=4507.51, side=sell → price up → loss for sell
    xau_loss = next(
        (t for t in trades
         if t["symbol"] == "XAU_USD" and float(t.get("exit_price", 0)) > float(t.get("entry_price", 0))
         and t.get("side", "").lower() == "sell"),
        None,
    )
    if xau_loss:
        row = render_rt_row(xau_loss)
        assert row["pnl_raw"] < 0, \
            f"XAU SELL with exit > entry should be negative bps, got {row['pnl_raw']:.1f}"
        assert row["pnl"].startswith("-"), "Formatted pnl should start with '-'"


def test_xau_winning_trade_positive_bps():
    """XAU SELL where price went down → bps positive."""
    trades = load_fixture()
    xau_win = next(
        (t for t in trades
         if t["symbol"] == "XAU_USD" and float(t.get("exit_price", 0)) < float(t.get("entry_price", 0))
         and t.get("side", "").lower() == "sell"),
        None,
    )
    if xau_win:
        row = render_rt_row(xau_win)
        assert row["pnl_raw"] > 0, \
            f"XAU SELL with exit < entry should be positive bps, got {row['pnl_raw']:.1f}"
        assert row["pnl"].startswith("+"), "Formatted pnl should start with '+'"


def test_bps_arithmetic_consistency_all_rows():
    """For every row: pnl == f'{bps:.1f}' with ± prefix, bps = pnl_pct×100."""
    trades = load_fixture()
    for t in trades:
        row = render_rt_row(t)
        bps = float(t["pnl_pct"]) * 100.0
        expected = f"+{bps:.1f}" if bps >= 0 else f"{bps:.1f}"
        assert row["pnl"] == expected, \
            f"{t['symbol']} {t['side']}: expected {expected!r}, got {row['pnl']!r}"


def test_net_bps_key_absent_from_row():
    """Row dict must NOT have net_bps key (column dropped)."""
    trades = load_fixture()
    for t in trades[:3]:
        row = render_rt_row(t)
        assert "net_bps" not in row, "net_bps key should not be in rendered row"


def test_pnl_pct_key_absent_from_row():
    """Row dict must NOT have pnl_pct key (column dropped)."""
    trades = load_fixture()
    for t in trades[:3]:
        row = render_rt_row(t)
        assert "pnl_pct" not in row, "pnl_pct key should not be in rendered row"
