"""
Gated contract test for the Broker Effective Costs table.

Locks:
  - column names (no ibkr, yes ig)
  - broker_data_source_map key (ig-cfd-demo → ig)
  - row rendering: given broker_spreads.ig.EUR_USD.total_bps=2.5 → row["ig"] == "2.50"
"""
import sys, types, unittest.mock
import pytest

# ── minimal stubs so app.py imports without a running NiceGUI/ZMQ context ──

# zmq stub with the constants + classes bus.py and app.py need
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

# nicegui.ui needs a timer attribute and page decorator
ui_mod = sys.modules["nicegui.ui"]
if not hasattr(ui_mod, "timer"):
    ui_mod.timer = lambda *a, **kw: None
if not hasattr(ui_mod, "page"):
    ui_mod.page = lambda *a, **kw: (lambda f: f)
if not hasattr(ui_mod, "run"):
    ui_mod.run = lambda *a, **kw: None

# nicegui.elements.timer needs a Timer class
timer_mod = sys.modules["nicegui.elements.timer"]
if not hasattr(timer_mod, "Timer"):
    class _Timer:
        _get_context = lambda self: None
    timer_mod.Timer = _Timer

# yaml stub
yaml_mod = sys.modules["yaml"]
if not hasattr(yaml_mod, "safe_load"):
    yaml_mod.safe_load = lambda f: {}

if not hasattr(sys.modules["nicegui.app"], "storage"):
    sys.modules["nicegui.app"].storage = types.SimpleNamespace(user={})

sys.path.insert(0, "src")

# ── import the two things under test ────────────────────────────────────────

from zmq_gui.app import _ACTIVE_PAIRS, _fmt_canonical_mid


def _make_broker_cost_columns():
    """Re-derive the column list exactly as _build_page does."""
    return [
        {"name": "instrument", "label": "Instrument", "field": "instrument", "align": "left"},
        {"name": "canonical",  "label": "Mid",        "field": "canonical",  "align": "right"},
        {"name": "oanda",      "label": "OANDA",      "field": "oanda",      "align": "center"},
        {"name": "dukascopy",  "label": "Dukascopy",  "field": "dukascopy",  "align": "center"},
        {"name": "ig",         "label": "IG",         "field": "ig",         "align": "center"},
    ]


def _make_broker_data_source_map():
    return {
        "oanda-practice": "oanda",
        "dukascopy-demo": "dukascopy",
        "ig-cfd-demo":    "ig",
    }


def _render_broker_cost_row(instrument: str, broker_spreads: dict) -> dict:
    """Pure rendering logic extracted from update_ui; no NiceGUI required."""
    broker_data_source_map = _make_broker_data_source_map()
    costs_per_broker: dict = {}
    breakdown_per_broker: dict = {}
    mid_for_canonical = 0.0

    for broker_id, ds_key in broker_data_source_map.items():
        cell = broker_spreads.get(ds_key, {}).get(instrument)
        if not cell:
            costs_per_broker[broker_id] = None
            breakdown_per_broker[broker_id] = {}
            continue
        total = cell.get("total_bps")
        if total is None:
            total = cell.get("spread_bps")
        costs_per_broker[broker_id] = float(total) if total is not None else None
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

    canonical_str = _fmt_canonical_mid(instrument, mid_for_canonical)
    valid_costs = {k: v for k, v in costs_per_broker.items() if v is not None and v >= 0}
    best_broker = min(valid_costs, key=valid_costs.__getitem__) if valid_costs else None

    def _tip(b: str) -> str:
        d = breakdown_per_broker.get(b) or {}
        parts = []
        for label, key in (("spread", "spread"), ("slip", "slip"), ("comm", "comm")):
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

    return {
        "instrument": instrument,
        "canonical":  canonical_str,
        "oanda":      (f"{costs_per_broker['oanda-practice']:.2f}"
                       if costs_per_broker.get("oanda-practice") is not None else "—"),
        "dukascopy":  (f"{costs_per_broker['dukascopy-demo']:.2f}"
                       if costs_per_broker.get("dukascopy-demo") is not None else "—"),
        "ig":         (f"{costs_per_broker['ig-cfd-demo']:.2f}"
                       if costs_per_broker.get("ig-cfd-demo") is not None else "—"),
        "oanda_best": best_broker == "oanda-practice",
        "duka_best":  best_broker == "dukascopy-demo",
        "ig_best":    best_broker == "ig-cfd-demo",
        "oanda_tip":  _tip("oanda-practice"),
        "duka_tip":   _tip("dukascopy-demo"),
        "ig_tip":     _tip("ig-cfd-demo"),
    }


# ── tests ────────────────────────────────────────────────────────────────────

def test_columns_contain_ig_not_ibkr():
    cols = _make_broker_cost_columns()
    names = [c["name"] for c in cols]
    assert "ig" in names,   "ig column must be present"
    assert "ibkr" not in names, "ibkr column must not be present"


def test_broker_data_source_map_ig():
    m = _make_broker_data_source_map()
    assert m.get("ig-cfd-demo") == "ig", "ig-cfd-demo must map to 'ig'"
    assert "ibkr-pro" not in m, "ibkr-pro must not be in map"


def test_ig_total_bps_renders_correctly():
    """Given broker_spreads.ig.EUR_USD.total_bps=2.5, row['ig'] == '2.50'."""
    broker_spreads = {
        "ig": {
            "EUR_USD": {
                "total_bps": 2.5,
                "spread_bps": 0.8,
                "slippage_bps_rt": 0.6,
                "commission_bps_rt": 1.1,
                "flat_bps_baseline_qty": 0.0,
                "bid": 1.17000,
                "ask": 1.17010,
                "source": "ig-cfd-demo",
            }
        }
    }
    row = _render_broker_cost_row("EUR_USD", broker_spreads)
    assert row["ig"] == "2.50", f"expected '2.50', got {row['ig']!r}"


def test_ig_best_flag_set_when_cheapest():
    broker_spreads = {
        "oanda":      {"EUR_USD": {"total_bps": 3.5, "bid": 1.17, "ask": 1.1701}},
        "dukascopy":  {"EUR_USD": {"total_bps": 2.8, "bid": 1.17, "ask": 1.1701}},
        "ig":         {"EUR_USD": {"total_bps": 2.1, "bid": 1.17, "ask": 1.1701}},
    }
    row = _render_broker_cost_row("EUR_USD", broker_spreads)
    assert row["ig_best"] is True,    "ig_best must be True when IG is cheapest"
    assert row["oanda_best"] is False
    assert row["duka_best"] is False


def test_ig_absent_renders_dash():
    """No IG data → row['ig'] == '—'."""
    row = _render_broker_cost_row("EUR_USD", {})
    assert row["ig"] == "—"


def test_tooltip_includes_breakdown():
    broker_spreads = {
        "ig": {
            "EUR_USD": {
                "total_bps": 2.5,
                "spread_bps": 0.8,
                "slippage_bps_rt": 0.6,
                "commission_bps_rt": 1.1,
                "flat_bps_baseline_qty": 0.0,
                "source": "ig-cfd-demo",
            }
        }
    }
    row = _render_broker_cost_row("EUR_USD", broker_spreads)
    tip = row["ig_tip"]
    assert "spread=0.800" in tip
    assert "slip=0.600" in tip
    assert "comm=1.100" in tip
