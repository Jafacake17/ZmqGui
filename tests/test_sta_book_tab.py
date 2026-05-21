"""
Gated contract tests: STA Book tab hierarchy + v4 features.

Locks:
- _build_book_hierarchy: slate→trade→child tree building
- _filter_book_node: live/blocked/closed24h/all filters
- _closed_badge_color: CLOSED-blocked=red, CLOSED-successful=grey
- _is_closed_blocked: HARD_VETO/basket aborted/indicator not registered
- _fmt_sched_col: CLOSED with closed_at timestamp, v4 enrichment
- v4 closed24h filter activates when closed_at is non-null
"""
import sys, types
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
    _build_book_hierarchy, _filter_book_node,
    _closed_badge_color, _is_closed_blocked, _fmt_sched_col,
)

RED = "#dc3c3c"; TEXT_SECONDARY = "#8c8ca0"


# ── test fixture builder ──────────────────────────────────────────────────────

def _snap(lifecycle=None, slates=None, schema_version=3):
    return {
        "schema_version": schema_version,
        "lifecycle": lifecycle or [],
        "slates": slates or [],
    }


def _rec(record_id, spec_id, state, parent_id=None, closed_at=None, last_reason="created"):
    return {
        "record_id": record_id,
        "spec_id": spec_id,
        "state": state,
        "parent_id": parent_id,
        "closed_at": closed_at,
        "last_reason": last_reason,
        "time_to_time_stop_seconds": None,
        "scheduled_entry": None,
        "gate_trace": [],
    }


def _slate(name, constituent_record_ids, constraints=None):
    return {
        "slate_id": f"slate-{name}",
        "name": name,
        "status": "AUTHORED",
        "constituent_record_ids": constituent_record_ids,
        "slate_constraints_status": constraints or {},
        "slate_constraints_error": None,
    }


# ── _build_book_hierarchy ─────────────────────────────────────────────────────

def test_hierarchy_slate_groups_built():
    snap = _snap(
        lifecycle=[_rec(1, "a1_nvda", "AUTHORED"), _rec(2, "a3_basket", "GATE_PENDING")],
        slates=[_slate("2wk", [1, 2])],
    )
    h = _build_book_hierarchy(snap)
    assert len(h["slate_groups"]) == 1
    assert len(h["slate_groups"][0]["top_level_records"]) == 2
    assert h["unslated"] == []


def test_hierarchy_unslated_group():
    snap = _snap(
        lifecycle=[_rec(1, "a1_nvda", "AUTHORED"), _rec(8, "smoke", "ACTIVE")],
        slates=[_slate("2wk", [1])],
    )
    h = _build_book_hierarchy(snap)
    assert len(h["unslated"]) == 1
    assert h["unslated"][0]["record"]["record_id"] == 8


def test_hierarchy_basket_children_nested():
    """parent_id relationships reflected in children list."""
    snap = _snap(
        lifecycle=[
            _rec(2, "a3_basket", "GATE_PENDING"),
            _rec(3, "a3a_tgt", "AUTHORED", parent_id=2),
            _rec(4, "a3b_low", "AUTHORED", parent_id=2),
        ],
        slates=[_slate("2wk", [2])],
    )
    h = _build_book_hierarchy(snap)
    grp = h["slate_groups"][0]
    rec2_node = grp["top_level_records"][0]
    assert len(rec2_node["children"]) == 2
    child_ids = [c["record"]["record_id"] for c in rec2_node["children"]]
    assert sorted(child_ids) == [3, 4]


def test_hierarchy_children_sorted_by_record_id():
    snap = _snap(
        lifecycle=[
            _rec(2, "a3", "GATE_PENDING"),
            _rec(6, "a3d", "AUTHORED", parent_id=2),
            _rec(3, "a3a", "AUTHORED", parent_id=2),
        ],
        slates=[_slate("2wk", [2])],
    )
    h = _build_book_hierarchy(snap)
    children = h["slate_groups"][0]["top_level_records"][0]["children"]
    assert [c["record"]["record_id"] for c in children] == [3, 6]


def test_hierarchy_empty_snap():
    h = _build_book_hierarchy(_snap())
    assert h["slate_groups"] == []
    assert h["unslated"] == []


# ── _filter_book_node ─────────────────────────────────────────────────────────

def _node(state, closed_at=None, children=None):
    return {
        "record": {"state": state, "closed_at": closed_at},
        "children": children or [],
    }


def test_filter_live_active():
    assert _filter_book_node(_node("ACTIVE"), "live") is True

def test_filter_live_authored():
    assert _filter_book_node(_node("AUTHORED"), "live") is False

def test_filter_live_gate_pending():
    assert _filter_book_node(_node("GATE_PENDING"), "live") is False

def test_filter_blocked_gate_pending():
    assert _filter_book_node(_node("GATE_PENDING"), "blocked") is True

def test_filter_blocked_active():
    assert _filter_book_node(_node("ACTIVE"), "blocked") is False

def test_filter_all_anything():
    for state in ("AUTHORED", "GATE_PENDING", "ACTIVE", "CLOSED"):
        assert _filter_book_node(_node(state), "all") is True


# ── v4 closed24h filter ───────────────────────────────────────────────────────

def test_filter_closed24h_v4_with_closed_at():
    """v4: CLOSED record with closed_at → passes closed24h filter."""
    assert _filter_book_node(_node("CLOSED", closed_at="2026-05-21T10:00:00Z"), "closed24h") is True

def test_filter_closed24h_v3_no_closed_at():
    """v3: CLOSED record without closed_at → excluded from closed24h filter."""
    assert _filter_book_node(_node("CLOSED", closed_at=None), "closed24h") is False

def test_filter_closed24h_non_closed_state():
    assert _filter_book_node(_node("AUTHORED", closed_at=None), "closed24h") is False
    assert _filter_book_node(_node("ACTIVE", closed_at=None), "closed24h") is False

def test_filter_closed24h_propagates_to_children():
    """Parent AUTHORED + child CLOSED with closed_at → parent visible under closed24h."""
    parent = _node("AUTHORED", children=[_node("CLOSED", closed_at="2026-05-21T10:00:00Z")])
    assert _filter_book_node(parent, "closed24h") is True


# ── CLOSED badge colour (v4 enrichment) ──────────────────────────────────────

def test_closed_blocked_reason_hard_veto():
    rec = {"last_reason": "HARD_VETO: indicator not registered: foo"}
    assert _is_closed_blocked(rec) is True
    assert _closed_badge_color(rec) == RED

def test_closed_blocked_reason_basket_aborted():
    rec = {"last_reason": "basket aborted: parent gate failed"}
    assert _is_closed_blocked(rec) is True
    assert _closed_badge_color(rec) == RED

def test_closed_blocked_reason_indicator_not_registered():
    rec = {"last_reason": "indicator not registered: some_indicator"}
    assert _is_closed_blocked(rec) is True

def test_closed_successful_schedule_reason():
    rec = {"last_reason": "schedule@2026-05-21T10:00:00.123456+00:00"}
    assert _is_closed_blocked(rec) is False
    assert _closed_badge_color(rec) == TEXT_SECONDARY

def test_closed_none_reason():
    rec = {"last_reason": None}
    assert _is_closed_blocked(rec) is False
    assert _closed_badge_color(rec) == TEXT_SECONDARY


# ── _fmt_sched_col with closed_at ─────────────────────────────────────────────

def test_fmt_sched_closed_with_closed_at():
    result = _fmt_sched_col("CLOSED", None, None, "2026-05-21T10:30:00Z")
    assert "closed" in result
    assert "5/21" in result
    assert "10:30" in result

def test_fmt_sched_closed_no_closed_at():
    result = _fmt_sched_col("CLOSED", None, None, None)
    assert result == "closed"

def test_fmt_sched_authored_fires():
    result = _fmt_sched_col("AUTHORED", "2026-05-19T16:00:00-04:00", None)
    assert "fires" in result
    assert "5/19" in result

def test_fmt_sched_gate_pending():
    result = _fmt_sched_col("GATE_PENDING", None, 375426)
    assert "gate pending" in result
    assert "d" in result  # countdown has days

def test_fmt_sched_active_stop():
    result = _fmt_sched_col("ACTIVE", None, 3232)
    assert "stop in" in result
    assert "m" in result or "h" in result


# ── v4 fixture: 0 lifecycle records, schema_version=4 ────────────────────────
import pathlib, json as _json
V4_FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sta_heartbeat_v4_20260521T144948.json"

def load_v4():
    return _json.loads(V4_FIXTURE.read_text())


def test_v4_fixture_schema_version():
    d = load_v4()
    assert d.get("schema_version") == 4


def test_v4_fixture_zero_lifecycle():
    """v4 has 0 lifecycle records after schema bump — empty is correct."""
    d = load_v4()
    assert len(d.get("lifecycle") or []) == 0


def test_v4_fixture_slates_present():
    d = load_v4()
    assert len(d.get("slates") or []) == 1


def test_v4_book_hierarchy_empty_lifecycle():
    """Book hierarchy with 0 lifecycle shows slate group with no records."""
    d = load_v4()
    h = _build_book_hierarchy(d)
    assert len(h["slate_groups"]) == 1
    assert len(h["slate_groups"][0]["top_level_records"]) == 0
    assert len(h["unslated"]) == 0


def test_v4_closed_at_key_in_lifecycle_shape():
    """v4 shape: every lifecycle entry must have closed_at key (null for non-CLOSED).
    Tested synthetically since live v4 has 0 records temporarily."""
    # Synthetic v4 record to verify closed_at handling
    v4_record = {
        "record_id": 1, "spec_id": "a1_nvda", "state": "AUTHORED",
        "parent_id": None, "closed_at": None, "last_reason": "created",
    }
    # closed_at is null for AUTHORED — correctly excluded from closed24h filter
    node = {"record": v4_record, "children": []}
    assert _filter_book_node(node, "closed24h") is False

