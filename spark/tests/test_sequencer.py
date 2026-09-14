from __future__ import annotations

import pickle
import random
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scripts.oracle import compute_oracle  # noqa: E402
from spark.engine.sequencer import step  # noqa: E402
from spark.engine.statecore import (KIND_BALANCE, KIND_DUP, KIND_OVERFLOW,  # noqa: E402
                                    empty_state, tuple_to_parts)

CFG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE"}
SMALL = {"max_buffer_size": 5, "gap_policy": "FLAG_AND_CONTINUE"}


def ev(seq, amount=100, ts=None):
    return {"seq_no": seq, "amount_minor": amount,
            "event_ts_ms": ts if ts is not None else 1_000_000 + seq}


def feed(state, seqs, cfg=CFG, amounts=None):
    """Apply one batch. Returns (state, outputs)."""
    if amounts is None:
        events = [ev(s) for s in seqs]
    else:
        events = [ev(s, amounts[s]) for s in seqs]
    return step(state, events, cfg)


def kinds(outputs):
    return [k for k, _, _ in outputs]


def of_kind(outputs, kind):
    return [(s, d) for k, s, d in outputs if k == kind]


def test_in_order_balance_is_the_sum_buffer_empty_no_integrity():
    st, out = feed(empty_state(), [1, 2, 3, 4, 5])
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (5, 500, {})
    assert kinds(out) == [KIND_BALANCE]


def test_signed_amounts_debits_reduce_the_balance():
    st, _ = step(empty_state(), [ev(1, 1000), ev(2, -250), ev(3, -100)], CFG)
    assert tuple_to_parts(st)[1] == 650


def test_out_of_order_reaches_the_same_final_state_as_in_order():
    """[3,2,1,5,4] delivered one batch at a time must equal [1,2,3,4,5]."""
    ordered = empty_state()
    for s in [1, 2, 3, 4, 5]:
        ordered, _ = feed(ordered, [s])

    jumbled = empty_state()
    for s in [3, 2, 1, 5, 4]:
        jumbled, _ = feed(jumbled, [s])

    assert tuple_to_parts(ordered)[:3] == tuple_to_parts(jumbled)[:3]
    assert tuple_to_parts(jumbled)[2] == {}, "everything must have drained"


def test_the_drain_loop_is_the_mechanism_not_the_buffering():
    """3 buffers, 2 buffers, 1 applies -> drain applies 2 then 3."""
    st, _ = feed(empty_state(), [3])
    assert tuple_to_parts(st)[0] == 0 and set(tuple_to_parts(st)[2]) == {3}
    st, _ = feed(st, [2])
    assert tuple_to_parts(st)[0] == 0 and set(tuple_to_parts(st)[2]) == {2, 3}
    st, _ = feed(st, [1])
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (3, 300, {}), "one arrival drained two buffered events"


def test_within_batch_disorder_never_touches_the_buffer():
    """Sorting inside the batch is an optimisation; [5,4,3] with 1,2 applied goes straight in."""
    st, _ = feed(empty_state(), [1, 2])
    st, out = feed(st, [5, 4, 3])
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (5, 500, {})
    assert kinds(out) == [KIND_BALANCE], "no integrity events for within-batch reordering"


def test_buffer_size_is_denormalised_correctly():
    st, _ = feed(empty_state(), [4, 5, 6])
    assert st[3] == 3 and len(tuple_to_parts(st)[2]) == 3


def test_duplicate_of_an_applied_seq_is_dropped_and_counted():
    st, _ = feed(empty_state(), [1, 2, 3])
    st, out = feed(st, [2])
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (3, 300, {}), "balance must not move"
    dups = of_kind(out, KIND_DUP)
    assert len(dups) == 1 and dups[0][0] == 2
    assert dups[0][1]["path"] == "applied"


def test_replaying_an_entire_applied_window_changes_nothing():
    st, _ = feed(empty_state(), [1, 2, 3, 4, 5])
    before = tuple_to_parts(st)[:2]
    st, out = feed(st, [1, 2, 3, 4, 5])
    assert tuple_to_parts(st)[:2] == before
    assert len(of_kind(out, KIND_DUP)) == 5, "every replayed event is counted, never silent"


def test_duplicate_of_a_buffered_seq_is_not_double_inserted():
    st, _ = feed(empty_state(), [1, 3])
    assert set(tuple_to_parts(st)[2]) == {3}
    st, out = feed(st, [3])
    buf = tuple_to_parts(st)[2]
    assert set(buf) == {3} and len(buf) == 1
    dups = of_kind(out, KIND_DUP)
    assert len(dups) == 1 and dups[0][1]["path"] == "buffered"


def test_a_buffered_duplicate_does_not_double_apply_on_drain():
    """[1,3,3,2] — the classic. 3 must be applied exactly once."""
    st = empty_state()
    for s in [1, 3, 3, 2]:
        st, _ = feed(st, [s])
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (3, 300, {}), "300 not 400: the second 3 was dropped"


def test_the_two_dedup_paths_are_distinguishable():
    st = empty_state()
    st, _ = feed(st, [1, 2])
    _, out_applied = feed(st, [2])
    st2, _ = feed(st, [7])
    _, out_buffered = feed(st2, [7])
    assert of_kind(out_applied, KIND_DUP)[0][1]["path"] == "applied"
    assert of_kind(out_buffered, KIND_DUP)[0][1]["path"] == "buffered"


def test_state_round_trips_through_the_operator_pickle_path():
    st, _ = feed(empty_state(), [1, 2, 3])
    st, _ = feed(st, [7, 9])
    revived = pickle.loads(pickle.dumps(st))
    assert tuple_to_parts(revived) == tuple_to_parts(st)


def test_restart_then_replay_does_not_double_apply():
    """[1,2,3] -> serialize -> restore -> replay [2,3] -> then [4]."""
    st, _ = feed(empty_state(), [1, 2, 3])
    restored = pickle.loads(pickle.dumps(st))
    restored, out = feed(restored, [2, 3])
    assert tuple_to_parts(restored)[:2] == (3, 300), "a replay must apply nothing twice"
    assert len(of_kind(out, KIND_DUP)) == 2, "and must leave visible evidence it dropped them"
    restored, _ = feed(restored, [4])
    assert tuple_to_parts(restored)[:2] == (4, 400)


def test_restart_preserves_a_pending_buffer():
    st, _ = feed(empty_state(), [1, 5, 6])
    restored = pickle.loads(pickle.dumps(st))
    assert set(tuple_to_parts(restored)[2]) == {5, 6}
    restored, _ = feed(restored, [2, 3, 4])
    assert tuple_to_parts(restored)[:3] == (6, 600, {})


def test_buffer_pins_at_the_cap_and_evicts_min_first():
    st = empty_state()
    evicted = []
    for s in range(2, 13):                    # 2..12, seq 1 withheld
        st, out = feed(st, [s], SMALL)
        evicted += [seq for seq, _ in of_kind(out, KIND_OVERFLOW)]
    buf = tuple_to_parts(st)[2]
    assert len(buf) == 5, "buffer pinned at the cap"
    assert sorted(buf) == [8, 9, 10, 11, 12], "the k largest are retained"
    assert evicted == [2, 3, 4, 5, 6, 7], "min-first, in order"


def test_overflow_then_head_arrives_drains_the_survivors():
    st = empty_state()
    for s in range(2, 13):
        st, _ = feed(st, [s], SMALL)
    st, _ = feed(st, [1], SMALL)
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal) == (1, 100), "8..12 cannot drain: 2..7 were evicted, so 2 is missing"
    assert sorted(buf) == [8, 9, 10, 11, 12]


def test_an_arrival_lower_than_everything_buffered_is_evicted_on_arrival():
    """This is why eviction considers min(buf u {s}), not min(buf)."""
    st = empty_state()
    for s in [10, 11, 12, 13, 14]:
        st, _ = feed(st, [s], SMALL)
    assert sorted(tuple_to_parts(st)[2]) == [10, 11, 12, 13, 14]
    st, out = feed(st, [3], SMALL)
    ovf = of_kind(out, KIND_OVERFLOW)
    assert len(ovf) == 1 and ovf[0][0] == 3
    assert ovf[0][1]["evicted_on_arrival"] is True
    assert sorted(tuple_to_parts(st)[2]) == [10, 11, 12, 13, 14], \
        "the k largest are unchanged — a lower arrival must not displace a higher seq"


@pytest.mark.parametrize("trial", range(40))
def test_k_largest_invariant_holds_for_any_arrival_order(trial):
    """The invariant the oracle's DLQ prediction rests on."""
    rng = random.Random(trial)
    delivered = list(range(2, 30))            # head withheld: nothing can apply or drain
    order = delivered[:]
    rng.shuffle(order)
    st = empty_state()
    evicted = []
    for s in order:
        st, out = feed(st, [s], SMALL)
        evicted += [seq for seq, _ in of_kind(out, KIND_OVERFLOW)]
    buf = tuple_to_parts(st)[2]
    assert sorted(buf) == sorted(delivered)[-5:]
    assert sorted(evicted) == sorted(delivered)[:-5]


def test_engine_overflow_set_matches_the_oracle_prediction():
    delivered = list(range(2, 30))
    st = empty_state()
    evicted = []
    for s in delivered:
        st, out = feed(st, [s], SMALL)
        evicted += [seq for seq, _ in of_kind(out, KIND_OVERFLOW)]
    rows = [{"publish_order": i, "account_id": "A1", "seq_no": s, "amount_minor": 100,
             "event_type": "CREDIT", "event_ts_ms": 1000 + s}
            for i, s in enumerate(delivered)]
    o = compute_oracle(rows, "FLAG_AND_CONTINUE", 5)["A1"]
    assert sorted(evicted) == o["expected_overflow_evicted"]


@pytest.mark.parametrize("trial", range(120))
def test_engine_equals_oracle_on_random_shuffled_and_duplicated_streams(trial):
    rng = random.Random(10_000 + trial)
    n = rng.randint(3, 40)
    amounts = {s: rng.randint(-5000, 5000) for s in range(1, n + 1)}

    delivery = list(range(1, n + 1))
    for _ in range(rng.randint(0, 5)):
        delivery.append(rng.randint(1, n))          # duplicates
    rng.shuffle(delivery)

    st = empty_state()
    dup_seen = set()
    i = 0
    while i < len(delivery):
        size = rng.randint(1, 4)
        batch = delivery[i:i + size]
        i += size
        st, out = step(st, [ev(s, amounts[s]) for s in batch], CFG)
        dup_seen |= {seq for seq, _ in of_kind(out, KIND_DUP)}

    last, bal, buf, _ = tuple_to_parts(st)

    rows = [{"publish_order": j, "account_id": "A1", "seq_no": s,
             "amount_minor": amounts[s], "event_type": "CREDIT",
             "event_ts_ms": 1000 + j}
            for j, s in enumerate(delivery)]
    o = compute_oracle(rows, "FLAG_AND_CONTINUE")["A1"]

    assert buf == {}, "a gap-free stream must fully drain"
    assert bal == o["expected_balance_minor"]
    assert last == o["expected_last_applied_seq"]
    assert dup_seen == set(o["expected_dup_dropped"])


def test_matrix_gap_one_range_and_flag_and_continue_excludes_the_missing_amount():
    """[1,2,4,5] + timeout -> one GAP range (3,3); balance excludes seq 3."""
    from spark.tests.harness import run_stream
    cfg = dict(CFG, watermark_delay_ms=1000, gap_realert_ms=60_000)
    events = [{"seq_no": s, "amount_minor": 100, "event_ts_ms": 10_000 + s * 100}
              for s in [1, 2, 4, 5]]
    st, rec = run_stream(events, cfg, batch_size=1, trailing_idle_batches=10,
                         idle_event_time_step_ms=1000)
    last, bal, buf, _ = tuple_to_parts(st)
    gaps = rec.gaps()
    assert len(gaps) == 1
    _b, seq, d = gaps[0]
    assert seq == 3 and (d["lo"], d["hi"], d["count"]) == (3, 3, 1)
    assert (last, bal, buf) == (5, 400, {}), "4 events applied, seq 3 excluded"


def test_matrix_gap_does_not_fire_before_the_watermark_confirms_it():
    """Without a timeout invocation nothing alerts — the buffer is still hope, not loss."""
    st = empty_state()
    for s in [1, 2, 4, 5]:
        st, out = feed(st, [s])
    last, _bal, buf, _ = tuple_to_parts(st)
    assert last == 2 and sorted(buf) == [4, 5]
    assert "SEQUENCE_GAP" not in kinds(out), "buffered is not the same as lost"


def test_matrix_hold_advances_nothing_and_keeps_buffering():
    from spark.tests.harness import run_stream
    cfg = dict(CFG, gap_policy="HOLD", watermark_delay_ms=1000, gap_realert_ms=60_000)
    events = [{"seq_no": s, "amount_minor": 100, "event_ts_ms": 10_000 + s * 100}
              for s in [1, 2, 4, 5, 6]]
    st, rec = run_stream(events, cfg, batch_size=1, trailing_idle_batches=10,
                         idle_event_time_step_ms=1000)
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal) == (2, 200), "nothing after the hole applies"
    assert sorted(buf) == [4, 5, 6], "successors keep buffering"
    assert len(rec.gaps()) >= 1


def test_matrix_hold_overflows_rather_than_growing_without_bound():
    from spark.tests.harness import run_stream
    cfg = dict(CFG, gap_policy="HOLD", max_buffer_size=4,
               watermark_delay_ms=1000, gap_realert_ms=60_000)
    events = [{"seq_no": s, "amount_minor": 100, "event_ts_ms": 10_000 + s * 100}
              for s in range(1, 20) if s != 3]
    st, rec = run_stream(events, cfg, batch_size=1, trailing_idle_batches=10,
                         idle_event_time_step_ms=1000)
    assert len(tuple_to_parts(st)[2]) == 4, "buffer pinned at the cap"
    assert len(rec.overflows()) > 0, "bounded memory beats unbounded hope"


def test_timeout_invocation_with_no_rows_is_survivable():
    st, _ = feed(empty_state(), [1, 2])
    st2, out = step(st, [], CFG, timed_out=True, watermark_ms=999)
    assert tuple_to_parts(st2)[:2] == (2, 200)
    assert kinds(out) == [KIND_BALANCE]


def test_the_balance_row_always_carries_the_alarm_for_the_operator():
    _st, out = feed(empty_state(), [1, 2])
    assert out[-1][2]["alarm_ts"] is None, "nothing buffered, nothing to wait for"
    _st, out = feed(empty_state(), [1, 5])
    assert out[-1][2]["alarm_ts"] is not None, "a hole is open, so an alarm is armed"
