from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from spark.engine import gap_policy  # noqa: E402
from spark.engine.sequencer import compute_alarm, step  # noqa: E402
from spark.engine.statecore import empty_state, tuple_to_parts  # noqa: E402
from spark.tests.harness import run_stream  # noqa: E402

BASE = 1_000_000_000_000
STEP = 50                       # 50 ms per event == rate 20
WATERMARK_MS = 30_000

FLAG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE",
        "watermark_delay_ms": WATERMARK_MS, "gap_realert_ms": 60_000}
HOLD = dict(FLAG, gap_policy="HOLD")


def ev(seq, amount=100):
    return {"seq_no": seq, "amount_minor": amount, "event_ts_ms": BASE + seq * STEP}


def stream(missing=(), n=60, cfg=FLAG, batch_size=10, idle=40, idle_step=5000,
           watermark_delay_ms=None):
    events = [ev(s) for s in range(1, n + 1) if s not in set(missing)]
    return run_stream(events, cfg, batch_size=batch_size,
                      trailing_idle_batches=idle, idle_event_time_step_ms=idle_step,
                      watermark_delay_ms=watermark_delay_ms)


def test_no_buffer_means_no_alarm():
    assert compute_alarm({}, watermark_ms=500) is None


def test_alarm_is_the_earliest_buffered_successors_event_time():
    buf = {7: (100, 7_000), 9: (100, 9_000), 8: (100, 8_000)}
    assert compute_alarm(buf, watermark_ms=0) == 7_000


def test_alarm_is_clamped_above_the_watermark():
    """setTimeoutTimestamp throws on a value at or below the current watermark."""
    buf = {7: (100, 7_000)}
    assert compute_alarm(buf, watermark_ms=9_000) == 9_001


def test_the_clamp_is_semantically_right_not_just_mechanically_necessary():
    """If the alarm is already behind the watermark the gap is already confirmable,
    so watermark+1 means 'fire on the very next micro-batch', which is correct."""
    buf = {7: (100, 1_000)}
    alarm = compute_alarm(buf, watermark_ms=50_000)
    assert alarm == 50_001 and alarm > 50_000


def test_hold_defers_the_alarm_from_the_watermark_not_the_buffer():
    buf = {7: (100, 7_000)}
    assert compute_alarm(buf, watermark_ms=100_000, defer_ms=60_000) == 160_000


def test_gap_fires_exactly_once_for_one_hole():
    _st, rec = stream(missing=[25])
    assert len(rec.gaps()) == 1, "not zero (lost alarm), not two (double fire)"


def test_gap_carries_the_coalesced_range_and_its_evidence():
    _st, rec = stream(missing=[25])
    _batch, seq, d = rec.gaps()[0]
    assert seq == 25
    assert (d["lo"], d["hi"], d["count"]) == (25, 25, 1)
    assert d["successor_seq"] == 26
    assert d["alarm_event_ts"] == BASE + 26 * STEP
    assert d["watermark_ms"] >= d["alarm_event_ts"]


def test_NOT_BEFORE_no_gap_while_the_watermark_is_behind_the_alarm():
    """The false-positive half of the property: a reorder must not alert."""
    _st, rec = stream(missing=[25])
    batch, _seq, d = rec.gaps()[0]
    for b in rec.batches[:batch]:
        assert b["watermark_ms"] < d["alarm_event_ts"], \
            "a batch before the firing batch had already passed the alarm"


def test_NOT_NEVER_the_gap_does_fire():
    """The silent-corruption half: real loss must not be absorbed."""
    _st, rec = stream(missing=[25])
    assert len(rec.gaps()) >= 1


def test_BOUNDED_it_fires_in_the_first_eligible_batch():
    _st, rec = stream(missing=[25])
    batch, _seq, d = rec.gaps()[0]
    first_eligible = rec.first_batch_with_watermark_at_or_past(d["alarm_event_ts"])
    assert first_eligible is not None
    assert batch == first_eligible, "detection is bounded by watermark + one trigger"


def test_a_50_event_outage_is_one_alert_not_fifty():
    _st, rec = stream(missing=range(101, 151), n=200)
    gaps = rec.gaps()
    assert len(gaps) == 1
    _b, _s, d = gaps[0]
    assert (d["lo"], d["hi"], d["count"]) == (101, 150, 50)


def test_two_separate_holes_produce_two_ranges():
    _st, rec = stream(missing=[10, 40], n=80)
    ranges = sorted((d["lo"], d["hi"]) for _b, _s, d in rec.gaps())
    assert ranges == [(10, 10), (40, 40)]


def test_a_shuffled_but_complete_stream_produces_zero_gaps():
    """Reordering within the watermark is LATE, not LOST."""
    import random
    rng = random.Random(4)
    events = [ev(s) for s in range(1, 121)]
    for i in range(0, len(events), 20):
        chunk = events[i:i + 20]
        rng.shuffle(chunk)
        events[i:i + 20] = chunk
    _st, rec = run_stream(events, FLAG, batch_size=10, trailing_idle_batches=40,
                          idle_event_time_step_ms=5000)
    assert rec.gaps() == [], "a transient reorder must never alert"


def test_too_short_a_watermark_manufactures_false_positives():
    """The parameter earns its value: same input, shorter watermark, real damage.

    Skew here is 300 events x 50 ms = 15 s. A 30 s watermark absorbs it; a 5 s
    watermark declares late events lost, FLAG_AND_CONTINUE steps over them, and
    the balance ends SHORT. That is not merely a spurious alert — it is money.
    """
    import random
    rng = random.Random(11)
    events = [ev(s) for s in range(1, 601)]
    for i in range(0, len(events), 300):
        chunk = events[i:i + 300]
        rng.shuffle(chunk)
        events[i:i + 300] = chunk

    st_ok, rec_ok = run_stream(events, FLAG, batch_size=25, trailing_idle_batches=40,
                               idle_event_time_step_ms=5000, watermark_delay_ms=30_000)
    st_bad, rec_bad = run_stream(events, FLAG, batch_size=25, trailing_idle_batches=40,
                                 idle_event_time_step_ms=5000, watermark_delay_ms=5_000)

    assert rec_ok.gaps() == [], "30s watermark absorbs 15s of skew"
    assert len(rec_bad.gaps()) > 0, "5s watermark cannot"
    assert tuple_to_parts(st_bad)[1] < tuple_to_parts(st_ok)[1], \
        "the false positives cost real balance, not just noise"


def test_flag_and_continue_steps_over_and_drains():
    st, _rec = stream(missing=[25], cfg=FLAG)
    last, bal, buf, _ = tuple_to_parts(st)
    assert (last, buf) == (60, {})
    assert bal == 59 * 100, "the missing amount is excluded; everything else applies"


def test_hold_advances_nothing():
    st, rec = stream(missing=[25], cfg=HOLD)
    last, bal, buf, _ = tuple_to_parts(st)
    assert last == 24, "nothing after the hole applies"
    assert bal == 24 * 100
    assert len(buf) == 35, "successors keep buffering, counting against the cap"
    assert len(rec.gaps()) >= 1


def test_hold_re_asserts_on_a_bounded_cadence_not_every_trigger():
    """A held gap is a live incident, so it should not go stale — but one hole
    must not produce one alert per trigger either."""
    _st, rec = stream(missing=[25], cfg=HOLD, idle=200, idle_step=5000)
    gaps = rec.gaps()
    assert len(gaps) > 1, "a held gap is re-asserted while it stays open"
    fire_batches = [b for b, _s, _d in gaps]
    watermarks = [rec.watermark_at(b) for b in fire_batches]
    spacings = [watermarks[i + 1] - watermarks[i] for i in range(len(watermarks) - 1)]
    assert all(sp >= 60_000 for sp in spacings), \
        f"re-assertions must be at least gap_realert_ms apart, got {spacings}"


def test_hold_eventually_overflows_to_the_dlq_by_design():
    """Bounded memory beats unbounded hope."""
    cfg = dict(HOLD, max_buffer_size=10)
    _st, rec = stream(missing=[5], n=60, cfg=cfg)
    assert len(rec.overflows()) > 0


def test_unknown_policy_fails_loudly():
    with pytest.raises(ValueError, match="unknown gap_policy"):
        gap_policy.advance(1, 100, {3: (100, 300)}, "SOMETHING_ELSE")


def test_gap_range_is_last_plus_one_to_earliest_buffered_minus_one():
    assert gap_policy.gap_range(4, {9: (100, 900), 11: (100, 1100)}) == (5, 8)


def test_on_gap_emits_one_record_with_full_evidence():
    out = gap_policy.on_gap(4, {9: (100, 900)}, watermark_ms=5000)
    assert len(out) == 1
    kind, seq, d = out[0]
    assert kind == "SEQUENCE_GAP" and seq == 5
    assert (d["lo"], d["hi"], d["count"], d["successor_seq"]) == (5, 8, 4, 9)
    assert d["alarm_event_ts"] == 900 and d["watermark_ms"] == 5000


def test_advance_flag_and_continue_uses_the_shared_drain():
    last, bal, advanced = gap_policy.advance(4, 400, {9: (100, 900), 10: (100, 1000)},
                                             "FLAG_AND_CONTINUE")
    assert (last, bal, advanced) == (10, 600, True)


def test_advance_hold_returns_untouched():
    buf = {9: (100, 900)}
    last, bal, advanced = gap_policy.advance(4, 400, buf, "HOLD")
    assert (last, bal, advanced) == (4, 400, False) and buf == {9: (100, 900)}


def test_a_duplicate_arriving_after_the_gap_stepped_over_is_dropped():
    """The late-but-declared-lost event comes back. It must not be applied."""
    cfg = FLAG
    st, _ = step(empty_state(), [ev(1), ev(2), ev(4), ev(5)], cfg, watermark_ms=0)
    st, _ = step(st, [], cfg, timed_out=True, watermark_ms=BASE + 10 * STEP)
    assert tuple_to_parts(st)[0] == 5
    st, out = step(st, [ev(3)], cfg, watermark_ms=BASE + 10 * STEP)
    assert tuple_to_parts(st)[1] == 4 * 100, "seq 3 must not sneak back in"
    assert any(k == "DUP_DROPPED" for k, _s, _d in out)


def test_timeout_on_an_empty_buffer_is_a_no_op_today():
    st, _ = step(empty_state(), [ev(1), ev(2)], FLAG)
    st2, out = step(st, [], FLAG, timed_out=True, watermark_ms=BASE + 99_999)
    assert tuple_to_parts(st2)[:2] == tuple_to_parts(st)[:2]
    assert not any(k == "SEQUENCE_GAP" for k, _s, _d in out)


@pytest.mark.parametrize("missing_seq", [2, 7, 25, 59])
def test_gap_position_does_not_change_the_property(missing_seq):
    st, rec = stream(missing=[missing_seq])
    last, bal, buf, _ = tuple_to_parts(st)
    assert len(rec.gaps()) == 1
    assert (last, buf) == (60, {})
    assert bal == 59 * 100
