from __future__ import annotations

import pickle
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.oracle import coalesce_ranges, compute_oracle  # noqa: E402
from spark.engine.sequencer import step  # noqa: E402
from spark.engine.statecore import (empty_state, pack_buffer, state_to_tuple,  # noqa: E402
                                    tuple_to_parts, unpack_buffer)

CFG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE"}


def ev(seq, amount=100, ts=None):
    return {"seq_no": seq, "amount_minor": amount, "event_ts_ms": ts if ts else 1_000_000 + seq}


def run(state, seqs, cfg=CFG, **kw):
    return step(state, [ev(s) for s in seqs], cfg, **kw)


def test_empty_state_field_order_and_types():
    st = empty_state(now_ms=42)
    assert len(st) == 5, "state tuple is positional into RocksDB; arity is part of the contract"
    last, bal, blob, size, last_seen = st
    assert (last, bal, size, last_seen) == (0, 0, 0, 42)
    assert unpack_buffer(blob) == {}


def test_buffer_keys_are_native_ints_after_round_trip():
    """numpy.int64 keys pickle fine and then compare strangely against last+1."""
    class FakeNpInt(int):
        pass
    blob = pack_buffer({FakeNpInt(7): (FakeNpInt(500), FakeNpInt(9))})
    buf = unpack_buffer(blob)
    (k, (amt, ts)), = buf.items()
    assert type(k) is int and type(amt) is int and type(ts) is int
    assert 6 + 1 in buf, "the drain loop's `last + 1 in buf` lookup must hit"


def test_state_round_trips_through_the_operator_pickle_path():
    st = state_to_tuple(9, -4200, {11: (77, 5), 12: (88, 6)}, 1234)
    revived = pickle.loads(pickle.dumps(st))
    assert tuple_to_parts(revived) == (9, -4200, {11: (77, 5), 12: (88, 6)}, 1234)


def test_in_order_applies_and_sums():
    st, out = run(empty_state(), [1, 2, 3, 4, 5])
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (5, 500, {})
    assert [k for k, _, _ in out] == ["BALANCE"]


def test_signed_amounts_debits_reduce_the_balance():
    st, _ = step(empty_state(), [ev(1, 1000), ev(2, -250), ev(3, -100)], CFG)
    assert tuple_to_parts(st)[1] == 650


def test_within_batch_disorder_is_absorbed_by_the_sort():
    ordered, _ = run(empty_state(), [1, 2, 3])
    jumbled, _ = run(empty_state(), [3, 1, 2])
    assert tuple_to_parts(ordered)[:2] == tuple_to_parts(jumbled)[:2]


def test_state_carries_across_invocations():
    st, _ = run(empty_state(), [1, 2])
    st, _ = run(st, [3, 4])
    assert tuple_to_parts(st)[:2] == (4, 400)


def test_last_seen_ms_tracks_the_max_event_time():
    st, _ = step(empty_state(), [ev(1, ts=500), ev(2, ts=900), ev(3, ts=700)], CFG)
    assert tuple_to_parts(st)[3] == 900, "last_seen_ms is Day 5's TTL input"


def test_cross_batch_disorder_is_deferred_today_and_is_day_2s_job():
    st, out = run(empty_state(), [3])
    detail = out[-1][2]
    assert tuple_to_parts(st)[0] == 0
    assert detail["deferred"] == 1 and detail["deferred_seqs"] == [3]
    st, _ = run(st, [2])
    st, _ = run(st, [1])
    last, bal, _, _ = tuple_to_parts(st)
    assert (last, bal) == (1, 100), "Day 2 must turn this into (3, 300)"


def test_ordered_stream_defers_nothing():
    _, out = run(empty_state(), [1, 2, 3, 4, 5])
    assert out[-1][2]["deferred"] == 0, "this is the Stage-0 assertion"


def test_timeout_invocation_with_no_rows_is_survivable():
    """A timed-out call receives NO ROWS and must work from state alone."""
    st, _ = run(empty_state(), [1, 2])
    st2, out = step(st, [], CFG, timed_out=True, watermark_ms=999)
    assert tuple_to_parts(st2)[:2] == (2, 200)
    assert [k for k, _, _ in out] == ["BALANCE"]


def _log(pairs, account="A1"):
    return [{"publish_order": i, "account_id": account, "seq_no": s,
             "amount_minor": a, "event_type": "CREDIT" if a > 0 else "DEBIT",
             "event_ts_ms": 1000 + s}
            for i, (s, a) in enumerate(pairs)]


def test_oracle_clean_stream():
    o = compute_oracle(_log([(1, 10), (2, 20), (3, 30)]))["A1"]
    assert o["expected_balance_minor"] == 60
    assert o["applied_count"] == 3
    assert o["expected_gap_ranges"] == [] and o["expected_dup_dropped"] == []


def test_oracle_counts_a_duplicate_once():
    o = compute_oracle(_log([(1, 10), (2, 20), (2, 20), (3, 30)]))["A1"]
    assert o["expected_balance_minor"] == 60, "a re-delivery must not be summed twice"
    assert o["expected_dup_dropped"] == [2]


def test_oracle_flag_and_continue_steps_over_a_hole():
    o = compute_oracle(_log([(1, 10), (2, 20), (4, 40)]), "FLAG_AND_CONTINUE")["A1"]
    assert o["expected_gap_ranges"] == [[3, 3, 1]]
    assert o["expected_balance_minor"] == 70
    assert o["expected_last_applied_seq"] == 4


def test_oracle_hold_stops_at_the_hole():
    o = compute_oracle(_log([(1, 10), (2, 20), (4, 40)]), "HOLD")["A1"]
    assert o["expected_balance_minor"] == 30
    assert o["expected_last_applied_seq"] == 2


def test_gap_ranges_coalesce():
    assert coalesce_ranges([3]) == [(3, 3, 1)]
    assert coalesce_ranges([7, 8, 9, 15]) == [(7, 9, 3), (15, 15, 1)]


def test_oracle_predicts_the_overflow_set_when_the_head_is_withheld():
    """Min-first eviction retains the k largest, so the DLQ set is order-independent."""
    o = compute_oracle(_log([(s, 10) for s in range(2, 12)]), "FLAG_AND_CONTINUE",
                       max_buffer_size=4)["A1"]
    assert o["overflow_determinate"] is True
    assert o["expected_overflow_evicted"] == [2, 3, 4, 5, 6, 7]
    assert o["applied_set"] == [8, 9, 10, 11]


def test_oracle_refuses_to_guess_when_overflow_is_arrival_dependent():
    o = compute_oracle(_log([(s, 10) for s in range(1, 12)]), "FLAG_AND_CONTINUE",
                       max_buffer_size=4)["A1"]
    assert o["overflow_determinate"] is False, "an appliable head means the buffer drains"


@pytest.mark.parametrize("n", [1, 5, 50, 500])
def test_engine_matches_oracle_on_ordered_streams(n):
    pairs = [(s, (s * 37) % 500 - 250) for s in range(1, n + 1)]
    st, _ = step(empty_state(), [ev(s, a) for s, a in pairs], CFG)
    last, bal, _, _ = tuple_to_parts(st)
    o = compute_oracle(_log(pairs))["A1"]
    assert bal == o["expected_balance_minor"]
    assert last == o["expected_last_applied_seq"]
