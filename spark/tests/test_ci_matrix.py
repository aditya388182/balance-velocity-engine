from __future__ import annotations

import json
import pickle
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from spark.engine.sequencer import step  # noqa: E402
from spark.engine.statecore import empty_state, tuple_to_parts  # noqa: E402

FIXTURE_DIR = REPO_ROOT / "data" / "fixtures" / "ci_sequences"
EXPECTED_SCENARIOS = {
    "in_order", "out_of_order", "duplicate_applied", "duplicate_buffered",
    "gap", "hold_policy", "restart", "overflow",
}


def load_fixtures() -> List[Dict[str, Any]]:
    return [json.loads(p.read_text()) for p in sorted(FIXTURE_DIR.glob("*.json"))]


FIXTURES = load_fixtures()


def run_fixture(fx: Dict[str, Any]):
    """Drive step() through the fixture's phases. Returns (state, outputs)."""
    cfg = fx["config"]
    amount = int(fx.get("amount_per_event", 100))
    state = empty_state()
    outputs: List[tuple] = []

    for phase in fx["phases"]:
        if "restart" in phase:
            # The SAME serialisation path the real operator uses. If this ever
            # diverges from statecore's pickling, this test stops meaning anything.
            state = pickle.loads(pickle.dumps(state))
            continue
        if "timeout" in phase:
            wm = int(phase["timeout"]["watermark_ms"])
            state, out = step(state, [], cfg, timed_out=True, watermark_ms=wm)
            outputs += out
            continue
        seqs = phase["batch"]
        events = [{"seq_no": s, "amount_minor": amount, "event_ts_ms": 1_000_000 + s * 100}
                  for s in seqs]
        state, out = step(state, events, cfg)
        outputs += out

    return state, outputs


def seqs_of(outputs, kind) -> List[int]:
    return sorted({s for k, s, _d in outputs if k == kind})


def details_of(outputs, kind):
    return [(s, d) for k, s, d in outputs if k == kind]


def test_every_expected_scenario_has_a_fixture():
    """A deleted fixture must fail the build, not quietly shrink the matrix."""
    found = {fx["name"] for fx in FIXTURES}
    missing = EXPECTED_SCENARIOS - found
    assert not missing, f"missing committed fixtures: {sorted(missing)}"
    assert len(FIXTURES) == len(found), "duplicate fixture names"


def test_fixture_names_match_their_filenames():
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        fx = json.loads(path.read_text())
        assert fx["name"] == path.stem, f"{path.name} declares name={fx['name']!r}"


def test_every_fixture_documents_itself():
    """The fixtures are the reviewable contract; an undocumented one is not reviewable."""
    for fx in FIXTURES:
        assert fx.get("description", "").strip(), f"{fx['name']} has no description"


@pytest.mark.parametrize("fx", FIXTURES, ids=[f["name"] for f in FIXTURES])
def test_matrix_final_state(fx):
    state, _outputs = run_fixture(fx)
    last, bal, buf, _seen = tuple_to_parts(state)
    exp = fx["expect"]
    assert last == exp["last_applied_seq"], f"{fx['name']}: last_applied_seq"
    assert bal == exp["balance_minor"], f"{fx['name']}: balance_minor"
    assert sorted(buf) == exp["buffer"], f"{fx['name']}: pending buffer"


@pytest.mark.parametrize("fx", FIXTURES, ids=[f["name"] for f in FIXTURES])
def test_matrix_integrity_events(fx):
    _state, outputs = run_fixture(fx)
    exp = fx["expect"]
    for kind in ("DUP_DROPPED", "BUFFER_OVERFLOW", "SEQUENCE_GAP"):
        assert seqs_of(outputs, kind) == exp[kind], f"{fx['name']}: {kind} set"


@pytest.mark.parametrize("fx", [f for f in FIXTURES if "dup_paths" in f["expect"]],
                         ids=[f["name"] for f in FIXTURES if "dup_paths" in f["expect"]])
def test_matrix_dedup_path_is_the_expected_one(fx):
    """applied vs buffered are two distinct branches, not two names for one."""
    _state, outputs = run_fixture(fx)
    got = {str(s): d["path"] for s, d in details_of(outputs, "DUP_DROPPED")}
    assert got == fx["expect"]["dup_paths"], f"{fx['name']}: dedup paths"


@pytest.mark.parametrize("fx", [f for f in FIXTURES if "gap_ranges" in f["expect"]],
                         ids=[f["name"] for f in FIXTURES if "gap_ranges" in f["expect"]])
def test_matrix_gap_ranges_are_coalesced(fx):
    _state, outputs = run_fixture(fx)
    got = sorted([d["lo"], d["hi"], d["count"]] for _s, d in details_of(outputs, "SEQUENCE_GAP"))
    assert got == sorted(fx["expect"]["gap_ranges"]), f"{fx['name']}: gap ranges"


def test_out_of_order_reaches_the_same_state_as_in_order():
    """The cross-fixture invariant: reordering changes nothing about the outcome."""
    by_name = {fx["name"]: fx for fx in FIXTURES}
    a, _ = run_fixture(by_name["in_order"])
    b, _ = run_fixture(by_name["out_of_order"])
    assert tuple_to_parts(a)[:3] == tuple_to_parts(b)[:3]


def test_overflow_evictions_are_min_first():
    by_name = {fx["name"]: fx for fx in FIXTURES}
    _state, outputs = run_fixture(by_name["overflow"])
    order = [s for k, s, _d in outputs if k == "BUFFER_OVERFLOW"]
    assert order == sorted(order), \
        "with in-order arrival the DLQ must receive evictions lowest-first"


def test_the_restart_fixture_is_the_one_the_red_ci_demo_breaks():
    by_name = {fx["name"]: fx for fx in FIXTURES}
    fx = by_name["restart"]
    assert fx["expect"]["balance_minor"] == 400
    assert fx["expect"]["DUP_DROPPED"] == [2, 3]
    naive_double_apply = 400 + 2 * 100
    assert naive_double_apply == 600, \
        "without the drop branch the replay adds seqs 2 and 3 again — a doubled debit"
