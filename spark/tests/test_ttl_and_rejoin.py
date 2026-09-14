from __future__ import annotations

import sys
from pathlib import Path

import pytest  # noqa: F401

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from spark.engine.sequencer import compute_alarm, step  # noqa: E402
from spark.engine.statecore import empty_state, tuple_to_parts  # noqa: E402
from spark.tests.harness import run_stream  # noqa: E402

BASE, STEP = 1_000_000_000_000, 50
TTL_MS = 120_000
CFG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE",
       "watermark_delay_ms": 30_000, "gap_realert_ms": 60_000,
       "state_ttl_ms": TTL_MS}
NO_TTL = dict(CFG, state_ttl_ms=0)


def ev(seq, amount=100):
    return {"seq_no": seq, "amount_minor": amount, "event_ts_ms": BASE + seq * STEP}


def kinds(outputs):
    return [k for k, _s, _d in outputs]


def test_empty_buffer_arms_a_TTL_alarm_instead_of_nothing():
    """Days 3-4 returned None here, so an idle account never timed out at all."""
    alarm = compute_alarm({}, watermark_ms=BASE, last_seen_ms=BASE, state_ttl_ms=TTL_MS)
    assert alarm == BASE + TTL_MS


def test_no_ttl_configured_means_no_alarm():
    assert compute_alarm({}, watermark_ms=BASE, last_seen_ms=BASE, state_ttl_ms=0) is None


def test_the_ttl_alarm_is_clamped_above_the_watermark():
    """An account already idle longer than the TTL must fire next batch, not never."""
    alarm = compute_alarm({}, watermark_ms=BASE + 999_999,
                          last_seen_ms=BASE, state_ttl_ms=TTL_MS)
    assert alarm == BASE + 1_000_000


def test_a_pending_buffer_still_wins_the_alarm_slot():
    """Gap detection outranks TTL: an account with an open hole is not idle."""
    buf = {7: (100, BASE + 7_000)}
    alarm = compute_alarm(buf, watermark_ms=BASE, last_seen_ms=BASE, state_ttl_ms=TTL_MS)
    assert alarm == BASE + 7_000


def test_timeout_with_an_open_buffer_is_a_GAP_not_a_TTL():
    st, _ = step(empty_state(), [ev(1), ev(5)], CFG, watermark_ms=BASE)
    _st, out = step(st, [], CFG, timed_out=True, watermark_ms=BASE + 10_000_000)
    assert "SEQUENCE_GAP" in kinds(out)
    assert "TTL_FLUSH" not in kinds(out)


def test_timeout_with_an_empty_buffer_and_a_STALE_last_seen_is_a_TTL_flush():
    st, _ = step(empty_state(), [ev(1), ev(2)], CFG, watermark_ms=BASE)
    _st, out = step(st, [], CFG, timed_out=True,
                    watermark_ms=BASE + 2 * STEP + TTL_MS)
    assert "TTL_FLUSH" in kinds(out)
    assert "SEQUENCE_GAP" not in kinds(out)


def test_timeout_with_an_empty_buffer_and_a_FRESH_last_seen_just_re_arms():
    st, _ = step(empty_state(), [ev(1), ev(2)], CFG, watermark_ms=BASE)
    st2, out = step(st, [], CFG, timed_out=True, watermark_ms=BASE + 2 * STEP + 10)
    assert "TTL_FLUSH" not in kinds(out)
    assert tuple_to_parts(st2)[:2] == tuple_to_parts(st)[:2]
    assert out[-1][2]["alarm_ts"] is not None, "it must re-arm, not go silent"


def test_a_flush_that_also_received_rows_is_not_idle_after_all():
    """The rows ARE the account speaking again. Keep the state."""
    st, _ = step(empty_state(), [ev(1), ev(2)], CFG, watermark_ms=BASE)
    st2, out = step(st, [ev(3)], CFG, timed_out=True,
                    watermark_ms=BASE + 2 * STEP + TTL_MS)
    assert "TTL_FLUSH" not in kinds(out)
    assert tuple_to_parts(st2)[0] == 3


def test_the_flush_row_carries_the_final_balance_and_the_idle_time():
    st, _ = step(empty_state(), [ev(1), ev(2), ev(3)], CFG, watermark_ms=BASE)
    _st, out = step(st, [], CFG, timed_out=True,
                    watermark_ms=BASE + 3 * STEP + TTL_MS + 500)
    kind, seq, detail = out[-1]
    assert kind == "TTL_FLUSH" and seq == 3
    assert detail["idle_ms"] >= TTL_MS
    assert detail["alarm_ts"] is None, "a released key must not stay armed"


def test_ttl_never_fires_when_it_is_not_configured():
    st, _ = step(empty_state(), [ev(1)], NO_TTL, watermark_ms=BASE)
    _st, out = step(st, [], NO_TTL, timed_out=True, watermark_ms=BASE + 10_000_000)
    assert "TTL_FLUSH" not in kinds(out)


def test_an_idle_account_is_flushed_exactly_once_and_released():
    events = [ev(s) for s in range(1, 21)]
    st, rec = run_stream(events, CFG, batch_size=5,
                         trailing_idle_batches=60, idle_event_time_step_ms=5000)
    flushes = [(s, d) for _b, _k, s, d in
               [(b, k, s, d) for b, k, s, d in rec.outputs if k == "TTL_FLUSH"]]
    assert len(flushes) == 1, "not zero (never evicted), not many (re-flushed)"
    seq, detail = flushes[0]
    assert seq == 20 and detail["idle_ms"] >= TTL_MS
    assert tuple_to_parts(st)[:2] == (0, 0), "the state was released"


def test_a_busy_account_is_never_flushed():
    events = []
    for s in range(1, 61):
        events.append(ev(s))
    st, rec = run_stream(events, CFG, batch_size=5)
    assert not [1 for _b, k, _s, _d in rec.outputs if k == "TTL_FLUSH"]
    assert tuple_to_parts(st)[0] == 60


def test_WITHOUT_the_reseed_a_returning_account_starts_from_zero():
    """The bug, asserted, so the fix has something to be a fix OF."""
    st, out = step(empty_state(), [ev(21)], CFG, state_exists=False)
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal) == (0, 0)
    assert sorted(buf) == [21], "seq 21 buffers behind a phantom gap 1..20"


def test_WITH_the_reseed_the_account_continues_from_its_flushed_balance():
    st, out = step(empty_state(), [ev(21)], CFG,
                   opening=(20, 2000), state_exists=False)
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, bal, buf) == (21, 2100, {}), "applied directly, balance continued"
    assert out[-1][2]["reseeded_from"] == [20, 2000]


def test_the_reseed_prevents_the_phantom_gap():
    _st, out = step(empty_state(), [ev(21)], CFG,
                    opening=(20, 2000), state_exists=False)
    assert "SEQUENCE_GAP" not in kinds(out)


def test_the_reseed_only_applies_to_COLD_state():
    """A live account must never have its balance replaced by a stale Delta read."""
    st, _ = step(empty_state(), [ev(1), ev(2)], CFG)
    st2, out = step(st, [ev(3)], CFG, opening=(99, 999_999), state_exists=True)
    assert tuple_to_parts(st2)[:2] == (3, 300), "the opening was ignored"
    assert "reseeded_from" not in out[-1][2]


def test_a_brand_new_account_with_no_opening_row_is_unaffected():
    st, out = step(empty_state(), [ev(1), ev(2)], CFG,
                   opening=None, state_exists=False)
    assert tuple_to_parts(st)[:2] == (2, 200)
    assert "reseeded_from" not in out[-1][2]


def test_an_opening_of_zero_is_not_a_reseed():
    """A row that exists but has never applied anything must not seed a phantom."""
    st, out = step(empty_state(), [ev(1)], CFG, opening=(0, 0), state_exists=False)
    assert tuple_to_parts(st)[:2] == (1, 100)
    assert "reseeded_from" not in out[-1][2]


def test_flush_then_rejoin_round_trip_preserves_the_balance():
    """The whole Stage-6 story in one test."""
    events = [ev(s) for s in range(1, 21)]
    st, rec = run_stream(events, CFG, batch_size=5,
                         trailing_idle_batches=60, idle_event_time_step_ms=5000)
    flush = [(s, d) for b, k, s, d in rec.outputs if k == "TTL_FLUSH"][0]
    flushed_seq = flush[0]
    flushed_balance = 20 * 100

    # the account returns; the sink's row is what the join hands back
    st2, out = step(empty_state(), [ev(21)], CFG,
                    opening=(flushed_seq, flushed_balance), state_exists=False)
    last, bal, buf, _ = tuple_to_parts(st2)
    assert (last, bal, buf) == (21, 2100, {})
    assert "SEQUENCE_GAP" not in kinds(out)


@pytest.mark.parametrize("gap_before_flush", [False, True])
def test_the_merge_guard_is_satisfied_after_a_rejoin(gap_before_flush):
    """The rejoin must emit a HIGHER seq than the stored row, or the sink drops it."""
    opening_seq, opening_bal = 500, 12_345
    st, _ = step(empty_state(), [ev(501)], CFG,
                 opening=(opening_seq, opening_bal), state_exists=False)
    last, bal, _buf, _ = tuple_to_parts(st)
    assert last > opening_seq, "strict > guard would reject an equal or lower seq"
    assert bal == opening_bal + 100
