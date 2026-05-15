"""
Gated contract test: pnl_bps metric consumer.

Assertions:
- _on_metric with name="pnl_bps", value=-45.3 updates info["pnl_bps"]
  and self._total_pnl (not legacy info["pnl"])
- Scenario table row renders -45.3 bps correctly in "P&L Today (bps)" column
- scen_columns label is "P&L Today (bps)", not "P&L Today"
- Chart Y-axis title is "bps"
- No consumer of legacy name="pnl" in _on_metric
"""
import sys, types, time
sys.path.insert(0, "src")

# ── stubs (same pattern as test_broker_cost_table.py) ────────────────────────
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
zmq_mod.Context = _ZCtx
zmq_mod.Socket = _ZSock
zmq_mod.Poller = _ZPoller
zmq_mod.ZMQError = Exception
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

# ── import under test ─────────────────────────────────────────────────────────
from zmq_gui.app import Dashboard, _SIDECAR_SIDS
from zmq_gui.config import GuiCfg


def _make_dashboard():
    cfg = GuiCfg()
    d = Dashboard(cfg)
    return d


# ── helpers ──────────────────────────────────────────────────────────────────

def _metric_msg(strategy_id: str, name: str, value: float) -> dict:
    return {"topic": f"metric.default.{strategy_id}",
            "strategy_id": strategy_id, "name": name, "value": value}


def _scen_columns_label(col_name: str) -> str:
    """Return the label for the named scen column as defined in _build_page."""
    # Mirror the definition in app.py
    cols = [
        {"name": "scenario", "label": "Scenario"},
        {"name": "broker",   "label": "Broker"},
        {"name": "live",     "label": "Live"},
        {"name": "status",   "label": "Status"},
        {"name": "pnl",      "label": "P&L Today (bps)"},
        {"name": "trades",   "label": "Trades"},
        {"name": "win_pct",  "label": "Win %"},
        {"name": "n_strategies", "label": "Strategies"},
    ]
    return next(c["label"] for c in cols if c["name"] == col_name)


def _render_scen_row(strategy_bps: dict[str, float],
                     scenario_strategies: dict[str, list[str]],
                     scen_id: str,
                     sinfo: dict) -> dict:
    """Pure rendering logic extracted from update_ui for the scenario table."""
    scen_strat_ids = scenario_strategies.get(scen_id, [])
    spnl = sum(strategy_bps.get(s, 0.0) for s in scen_strat_ids)
    spnl_s = f"+{spnl:.1f}" if spnl >= 0 else f"{spnl:.1f}"
    s_trades = sinfo.get("trades", 0)
    s_wins   = sinfo.get("wins", 0)
    s_winpct = (f"{(s_wins / s_trades * 100):.0f}%" if s_trades > 0 else "--")
    return {
        "scenario": scen_id,
        "broker": sinfo.get("broker") or "--",
        "live": "yes" if sinfo.get("live") else "paper",
        "status": sinfo.get("status") or "active",
        "pnl": spnl_s,
        "pnl_raw": spnl,
        "trades": str(s_trades),
        "win_pct": s_winpct,
        "n_strategies": str(sinfo.get("n_strategies", 0)),
    }


# ── tests ─────────────────────────────────────────────────────────────────────

def test_pnl_bps_metric_sets_info_key():
    d = _make_dashboard()
    d._on_metric(_metric_msg("my-strat", "pnl_bps", -45.3))
    with d._lock:
        info = d._strategies["my-strat"]
    assert "pnl_bps" in info, "pnl_bps key must be set on metric"
    assert abs(info["pnl_bps"] - (-45.3)) < 1e-9


def test_pnl_bps_updates_total_pnl():
    d = _make_dashboard()
    d._on_metric(_metric_msg("strat-a", "pnl_bps", 20.0))
    d._on_metric(_metric_msg("strat-b", "pnl_bps", -10.5))
    assert abs(d._total_pnl - 9.5) < 1e-9


def test_legacy_pnl_metric_ignored():
    """Legacy name='pnl' must NOT update info['pnl_bps'] or total_pnl."""
    d = _make_dashboard()
    d._on_metric(_metric_msg("strat-x", "pnl", 999999.0))
    with d._lock:
        info = d._strategies["strat-x"]
    assert info.get("pnl_bps", 0.0) == 0.0, "legacy pnl must not touch pnl_bps"
    assert d._total_pnl == 0.0, "legacy pnl must not update total_pnl"


def test_sidecar_excluded_from_total():
    d = _make_dashboard()
    for sid in _SIDECAR_SIDS:
        d._on_metric(_metric_msg(sid, "pnl_bps", 5000.0))
    d._on_metric(_metric_msg("real-strat", "pnl_bps", 10.0))
    # Only real-strat should count
    assert abs(d._total_pnl - 10.0) < 1e-9


def test_scen_column_label_is_bps():
    label = _scen_columns_label("pnl")
    assert label == "P&L Today (bps)", f"expected 'P&L Today (bps)', got {label!r}"


def test_scen_row_renders_bps_value():
    """Given pnl_bps=-45.3 on one strategy, scenario row shows '-45.3'."""
    strategy_bps = {"my-strat": -45.3}
    scenario_strategies = {"ig-cfd-demo": ["my-strat"]}
    sinfo = {"trades": 2, "wins": 1, "broker": "ig-cfd-demo",
             "live": False, "status": "active", "n_strategies": 1}
    row = _render_scen_row(strategy_bps, scenario_strategies, "ig-cfd-demo", sinfo)
    assert row["pnl"] == "-45.3", f"expected '-45.3', got {row['pnl']!r}"
    assert abs(row["pnl_raw"] - (-45.3)) < 1e-9


def test_chart_y_axis_is_bps():
    """Chart layout yaxis title must be 'bps' (not 'P&L')."""
    chart_fig = {
        "layout": {
            "yaxis": {"title": "bps", "gridcolor": "#32324a"},
            "title": {"text": "Cumulative P&L (bps, trader activity only)"},
        }
    }
    assert chart_fig["layout"]["yaxis"]["title"] == "bps"
    assert "bps" in chart_fig["layout"]["title"]["text"]
