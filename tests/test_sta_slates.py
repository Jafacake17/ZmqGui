"""
Gated contract tests for STA Slates tab rendering.

Uses the real v3 fixture (tests/fixtures/sta_heartbeat_live.json) which
has schema_version=3 and slates[] populated.

Locks:
- _build_constraint_chips: SC1/SC2/SC3/Q4 chips from slate_constraints_status
- Slate row rendering: name, status, constituents, constraint chips
- slate_constraints_error fail-loud surface
- Graceful empty-state (no slates key)
"""
import sys, types, json, pathlib
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

from zmq_gui.app import _build_constraint_chips, _derive_label

GREEN = "#32cd64"; RED = "#dc3c3c"; TEXT_SECONDARY = "#8c8ca0"


def load_fixture():
    return json.loads(FIXTURE.read_text())


def render_slate_row(s: dict) -> dict:
    """Mirror update_sta_slates row-building logic."""
    sid = s.get("slate_id", "?")
    status = s.get("status", "—")
    cs_status = s.get("slate_constraints_status") or {}
    resolved = s.get("all_constituents_resolved")
    constituent_labels = ", ".join(
        _derive_label(spec_id)
        for spec_id in (s.get("constituent_spec_ids") or [])
    )
    authored = (f"{s.get('authored_by', '')} on {s.get('authored_at', '')}"
                ).strip().strip("on").strip()
    return {
        "slate_id":         sid,
        "name":             s.get("name", sid),
        "status":           status,
        "authored":         authored,
        "constituents":     constituent_labels or "—",
        "resolved":         "✓ yes" if resolved else ("✗ no" if resolved is False else "—"),
        "resolved_color":   GREEN if resolved else (RED if resolved is False else TEXT_SECONDARY),
        "constraint_chips": _build_constraint_chips(cs_status),
    }


# ── _build_constraint_chips ───────────────────────────────────────────────────

def test_constraint_chips_sc1_pass():
    cs = {"SC1": {"passed": True, "reason": "all 2 constituents are defined-risk"}}
    chips = _build_constraint_chips(cs)
    sc1 = next(c for c in chips if c["label"] == "SC1")
    assert sc1["color"] == GREEN
    assert "defined-risk" in sc1["tooltip"]

def test_constraint_chips_sc2_fail():
    cs = {"SC2": {"passed": False, "reason": "no hawkish hedge construct"}}
    chips = _build_constraint_chips(cs)
    sc2 = next(c for c in chips if c["label"] == "SC2")
    assert sc2["color"] == RED
    assert "hawkish" in sc2["tooltip"]

def test_constraint_chips_all_four_keys():
    cs = {
        "SC1": {"passed": True, "reason": "ok"},
        "SC2": {"passed": False, "reason": "missing hedge"},
        "SC3": {"passed": True, "reason": "ok"},
        "Q3":  {"passed": True, "reason": "ok"},
    }
    chips = _build_constraint_chips(cs)
    labels = [c["label"] for c in chips]
    assert labels == ["SC1", "SC2", "SC3", "Q3"]

def test_constraint_chips_missing_key_grey():
    """Missing constraint → grey 'not evaluated' chip."""
    chips = _build_constraint_chips({})
    for c in chips:
        assert c["color"] == TEXT_SECONDARY
        assert c["tooltip"] == "not evaluated"

def test_constraint_chips_none_input():
    chips = _build_constraint_chips(None)
    assert len(chips) == 4
    assert all(c["color"] == TEXT_SECONDARY for c in chips)


# ── slate row rendering from real fixture ─────────────────────────────────────

def test_fixture_has_slates():
    snap = load_fixture()
    assert snap.get("schema_version") == 3, "Fixture must be schema_version=3"
    assert snap.get("slates"), "Fixture must have at least one slate"

def test_slate_row_name_and_status():
    snap = load_fixture()
    s = snap["slates"][0]
    row = render_slate_row(s)
    assert "2wk" in row["name"] or "high-conf" in row["name"], \
        f"Slate name unexpected: {row['name']!r}"
    assert row["status"] == "AUTHORED"

def test_slate_row_constituents_readable():
    snap = load_fixture()
    s = snap["slates"][0]
    row = render_slate_row(s)
    # Both A1 and A3 must appear in readable form
    assert "A1" in row["constituents"], f"A1 missing from constituents: {row['constituents']!r}"
    assert "A3" in row["constituents"], f"A3 missing from constituents: {row['constituents']!r}"
    assert "NVDA" in row["constituents"], "NVDA ticker missing from constituents"

def test_slate_row_resolved_yes():
    snap = load_fixture()
    s = snap["slates"][0]
    row = render_slate_row(s)
    assert "✓" in row["resolved"], f"resolved should be '✓ yes', got {row['resolved']!r}"
    assert row["resolved_color"] == GREEN

def test_slate_row_constraint_chips_sc1_sc2_sc3_q3():
    snap = load_fixture()
    s = snap["slates"][0]
    row = render_slate_row(s)
    chips = row["constraint_chips"]
    labels = [c["label"] for c in chips]
    assert "SC1" in labels
    assert "SC2" in labels
    assert "SC3" in labels
    assert "Q3" in labels

def test_slate_row_sc1_passes():
    snap = load_fixture()
    row = render_slate_row(snap["slates"][0])
    sc1 = next(c for c in row["constraint_chips"] if c["label"] == "SC1")
    assert sc1["color"] == GREEN

def test_slate_row_sc2_fails():
    snap = load_fixture()
    row = render_slate_row(snap["slates"][0])
    sc2 = next(c for c in row["constraint_chips"] if c["label"] == "SC2")
    assert sc2["color"] == RED
    assert "hawkish" in sc2["tooltip"].lower() or "hedge" in sc2["tooltip"].lower()

def test_slate_no_constraints_error():
    snap = load_fixture()
    s = snap["slates"][0]
    assert s.get("slate_constraints_error") is None, \
        "Real fixture should have no constraints_error"


# ── slate_constraints_error fail-loud surface ─────────────────────────────────

def _slates_error_banner_text(snap: dict) -> str:
    """Derive what the error banner would show."""
    slates = snap.get("slates") or []
    reg_errors = [
        f"{s.get('name','?')}: {s.get('slate_constraints_error')}"
        for s in slates
        if s.get("slate_constraints_error")
    ]
    if reg_errors:
        return "CONSTRAINT REGISTRY ERROR — operator + STA must investigate: " + " | ".join(reg_errors)
    return ""

def test_no_error_banner_when_none():
    snap = load_fixture()
    assert _slates_error_banner_text(snap) == ""

def test_error_banner_shown_when_present():
    """Synthetic slate with slate_constraints_error → red banner appears."""
    snap = {
        "schema_version": 3,
        "slates": [{
            "slate_id": "test-slate",
            "name": "Test Slate",
            "status": "AUTHORED",
            "constituent_spec_ids": [],
            "all_constituents_resolved": False,
            "slate_constraints_status": {},
            "slate_constraints_error": "unknown identifier: custom_constraint_X",
        }]
    }
    banner = _slates_error_banner_text(snap)
    assert "CONSTRAINT REGISTRY ERROR" in banner
    assert "custom_constraint_X" in banner
    assert "Test Slate" in banner

def test_error_banner_multiple_slates():
    """Multiple slates, one with error — only errored one appears."""
    snap = {
        "schema_version": 3,
        "slates": [
            {"slate_id": "s1", "name": "Good", "slate_constraints_error": None},
            {"slate_id": "s2", "name": "Bad", "slate_constraints_error": "bad_id"},
        ]
    }
    banner = _slates_error_banner_text(snap)
    assert "Bad" in banner
    assert "Good" not in banner


# ── graceful empty states ─────────────────────────────────────────────────────

def test_empty_slates_list_no_crash():
    snap = {"schema_version": 3, "slates": []}
    rows = [render_slate_row(s) for s in (snap.get("slates") or [])]
    assert rows == []

def test_schema_v2_no_slates_key():
    snap = {"schema_version": 2}
    rows = [render_slate_row(s) for s in (snap.get("slates") or [])]
    assert rows == []
