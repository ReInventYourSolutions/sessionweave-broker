from __future__ import annotations

import asyncio

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_wait_returns_immediately_when_matching_event_already_buffered() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    # Inject an event into the buffer.
    s._deliver_or_buffer(p.participant_id, "command.received", {"x": 1})

    result = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=5.0
    )

    assert result.timed_out is False
    assert result.messages == [{"name": "command.received", "payload": {"x": 1}, "seq": 1}]


@pytest.mark.asyncio
async def test_wait_wakes_on_event_arrival() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    async def deliver_after_delay() -> None:
        await asyncio.sleep(0.05)
        s._deliver_or_buffer(p.participant_id, "command.received", {"k": "v"})

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=None, timeout_seconds=5.0)
    )
    producer = asyncio.create_task(deliver_after_delay())

    result = await waiter
    await producer

    assert result.timed_out is False
    assert result.messages == [{"name": "command.received", "payload": {"k": "v"}, "seq": 1}]


@pytest.mark.asyncio
async def test_wait_times_out_when_only_non_matching_events_arrive() -> None:
    """Regression: non-matching events were re-routed through the
    subscriber, causing an infinite CPU-spin loop instead of a timeout.
    """
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    async def deliver_non_matching() -> None:
        await asyncio.sleep(0.02)
        s._deliver_or_buffer(p.participant_id, "participant.updated", {"a": 1})

    waiter = asyncio.create_task(
        s.wait_for_event(
            p.participant_id,
            names=["command.received"],
            timeout_seconds=0.2,
        )
    )
    producer = asyncio.create_task(deliver_non_matching())

    # Hard outer timeout: if the bug is present this will hang.
    result = await asyncio.wait_for(waiter, timeout=2.0)
    await producer

    assert result.timed_out is True
    assert result.messages == []
    # The non-matching event must have been re-buffered into _buffers.
    leftover = await s.check_inbox(p.participant_id)
    assert leftover == [{"name": "participant.updated", "payload": {"a": 1}, "seq": 1}]


@pytest.mark.asyncio
async def test_wait_returns_timed_out_when_no_event_arrives() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    result = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=0.05
    )

    assert result.timed_out is True
    assert result.messages == []


@pytest.mark.asyncio
async def test_wait_ignores_non_matching_buffered_event_and_rebuffers_it() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    # Pre-buffer a non-matching event.
    s._deliver_or_buffer(p.participant_id, "participant.updated", {"a": 1})

    result = await s.wait_for_event(
        p.participant_id,
        names=["command.received"],
        timeout_seconds=0.05,
    )

    assert result.timed_out is True
    assert result.messages == []
    # The non-matching event must still be in the buffer.
    leftover = await s.check_inbox(p.participant_id)
    assert leftover == [{"name": "participant.updated", "payload": {"a": 1}, "seq": 1}]


@pytest.mark.asyncio
async def test_wait_returns_only_matching_when_mixed_burst_arrives() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    async def deliver_burst() -> None:
        await asyncio.sleep(0.02)
        s._deliver_or_buffer(p.participant_id, "participant.updated", {"a": 1})
        s._deliver_or_buffer(p.participant_id, "command.received", {"b": 2})

    waiter = asyncio.create_task(
        s.wait_for_event(
            p.participant_id, names=["command.received"], timeout_seconds=5.0
        )
    )
    producer = asyncio.create_task(deliver_burst())

    result = await waiter
    await producer

    assert result.timed_out is False
    assert result.messages == [{"name": "command.received", "payload": {"b": 2}, "seq": 2}]
    # The non-matching event must still be in the buffer.
    leftover = await s.check_inbox(p.participant_id)
    assert leftover == [{"name": "participant.updated", "payload": {"a": 1}, "seq": 1}]


@pytest.mark.asyncio
async def test_wait_rejects_push_mode_participant() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="push")

    with pytest.raises(ToolError) as excinfo:
        await s.wait_for_event(
            p.participant_id, names=None, timeout_seconds=1.0
        )
    assert excinfo.value.code is ErrorCode.INVALID_DELIVERY_MODE


@pytest.mark.asyncio
async def test_wait_rejects_second_concurrent_call_for_same_participant() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    first = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=None, timeout_seconds=0.5)
    )
    # Yield once so the first call has a chance to register.
    await asyncio.sleep(0.01)

    with pytest.raises(ToolError) as excinfo:
        await s.wait_for_event(p.participant_id, names=None, timeout_seconds=0.5)
    assert excinfo.value.code is ErrorCode.WAIT_ALREADY_IN_FLIGHT

    result = await first
    assert result.timed_out is True


@pytest.mark.asyncio
async def test_wait_raises_evicted_when_participant_leaves_mid_wait() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=None, timeout_seconds=2.0)
    )
    await asyncio.sleep(0.02)
    await s.leave(p.participant_id)

    with pytest.raises(ToolError) as excinfo:
        await waiter
    assert excinfo.value.code is ErrorCode.PARTICIPANT_EVICTED


@pytest.mark.asyncio
async def test_wait_cleans_up_subscriber_and_in_flight_on_cancellation() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=None, timeout_seconds=5.0)
    )
    await asyncio.sleep(0.02)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    # The subscriber must be detached and the in-flight flag cleared.
    assert s._subscribers.get(p.participant_id) in (None, [])
    assert p.participant_id not in s._wait_in_flight


@pytest.mark.asyncio
async def test_wait_clamps_timeout_to_relay_config_max() -> None:
    # Build a config with a tiny max so the test runs fast.
    cfg = SessionConfig(max_wait_seconds=1)
    s = Session("s", cfg, ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    start = asyncio.get_running_loop().time()
    result = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=999999.0
    )
    elapsed = asyncio.get_running_loop().time() - start

    assert result.timed_out is True
    assert elapsed < 2.0  # clamped to ~1s, not 999999s


@pytest.mark.asyncio
async def test_wait_rejects_when_global_cap_exceeded() -> None:
    cfg = SessionConfig(max_concurrent_waits_total=1)
    s = Session("s", cfg, ConnectionManager())
    h1, h2 = FakeHandle(), FakeHandle()
    p1 = await s.join(h1, role=None, capabilities=[], delivery_mode="pull")
    p2 = await s.join(h2, role="r", capabilities=[], delivery_mode="pull")
    # Drain any buffered notifications (e.g. participant.joined fired when p2
    # joined) so that first's wait_for_event does not return immediately on
    # a pre-buffered event and is genuinely in-flight.
    await s.check_inbox(p1.participant_id)
    await s.check_inbox(p2.participant_id)

    first = asyncio.create_task(
        s.wait_for_event(p1.participant_id, names=None, timeout_seconds=0.5)
    )
    await asyncio.sleep(0.01)

    with pytest.raises(ToolError) as excinfo:
        await s.wait_for_event(
            p2.participant_id, names=None, timeout_seconds=0.5
        )
    assert excinfo.value.code is ErrorCode.TOO_MANY_WAITS

    await first


@pytest.mark.asyncio
async def test_wait_for_event_increments_metrics() -> None:
    """Verify that wait_for_event drives the Prometheus metrics."""
    from sessionweave.metrics import Metrics

    m = Metrics()
    s = Session("s", SessionConfig(), ConnectionManager(), metrics=m)
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    # Drain join notification so the wait doesn't return immediately.
    await s.check_inbox(p.participant_id)

    # Timeout outcome
    await s.wait_for_event(p.participant_id, names=None, timeout_seconds=0.05)
    rendered = m.render().decode("utf-8")
    assert 'sessionweave_wait_for_event_total{outcome="timeout"}' in rendered

    # Event outcome
    s._deliver_or_buffer(p.participant_id, "command.received", {"x": 1})
    result = await s.wait_for_event(p.participant_id, names=None, timeout_seconds=5.0)
    assert result.timed_out is False
    rendered = m.render().decode("utf-8")
    assert 'sessionweave_wait_for_event_total{outcome="event"}' in rendered


@pytest.mark.asyncio
async def test_wait_returns_session_ended_when_session_ends_mid_wait() -> None:
    """A waiter blocked when the session ends (deleted by its creator, TTL
    eviction) wakes with the terminal ``session.ended`` event as the wait's
    result, whatever its names filter, instead of an eviction error.
    """
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=["command.received"], timeout_seconds=5.0)
    )
    await asyncio.sleep(0.02)
    s.broadcast_session_ended(reason="deleted_by_test")

    result = await waiter
    assert result.timed_out is False
    assert result.messages == [
        {"name": "session.ended", "payload": {"reason": "deleted_by_test"}, "seq": 0}
    ]


@pytest.mark.asyncio
async def test_removal_mid_wait_returns_the_queued_halt_then_removed() -> None:
    """The halt queued before the removal sentinel is delivered as the
    wait's result; the removal is never reported as ``participant_evicted``.
    """
    from sessionweave.access import generate_token, hash_token

    s = Session("s", SessionConfig(), ConnectionManager())
    s.auth._creator_hash = hash_token(generate_token())
    p = await s.join(FakeHandle(), role=None, capabilities=[], delivery_mode="pull")

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=["supervisor.note"], timeout_seconds=5.0)
    )
    await asyncio.sleep(0.02)
    await s.remove_participant(p.participant_id)

    result = await waiter
    assert result.timed_out is False
    assert [(m["name"], m["payload"]["kind"]) for m in result.messages] == [
        ("supervisor.note", "halt")
    ]


@pytest.mark.asyncio
async def test_removal_mid_wait_without_a_match_raises_removed() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    p = await s.join(FakeHandle(), role=None, capabilities=[], delivery_mode="pull")

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=["command.received"], timeout_seconds=5.0)
    )
    await asyncio.sleep(0.02)
    await s.remove_participant(p.participant_id)

    with pytest.raises(ToolError) as excinfo:
        await waiter
    assert excinfo.value.code is ErrorCode.PARTICIPANT_REMOVED


@pytest.mark.asyncio
async def test_wait_releases_in_flight_when_metrics_inc_raises() -> None:
    """Regression: a Prometheus error in wait_for_event_active.inc() must
    NOT leak the participant into _wait_in_flight. Reported from the
    cockpit-side Opus's fan-out repro (2026-05-22): the FIRST
    wait_for_event raised with an empty exception string and every
    subsequent call returned HTTP 409 wait_already_in_flight. Root cause:
    the metrics inc lived between lock-release and the outer try/finally,
    so any raise there bypassed the cleanup forever.

    Simulate the failure by handing the session a metrics stub whose
    wait_for_event_active.inc() blows up. The first call must propagate
    a useful exception AND clean up. The second call must proceed
    normally (no 409).
    """
    from sessionweave.metrics import Metrics

    real = Metrics()
    boom_count = {"n": 0}

    class ExplodingInc:
        def inc(self) -> None:
            boom_count["n"] += 1
            raise RuntimeError("simulated prometheus failure")

        def dec(self) -> None:
            real.wait_for_event_active.dec()

    real.wait_for_event_active = ExplodingInc()  # type: ignore[assignment]
    s = Session("s", SessionConfig(), ConnectionManager(), metrics=real)
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    await s.check_inbox(p.participant_id)

    # Even with metrics inc raising, the wait itself must complete cleanly
    # (the inc failure is logged, not propagated).
    result = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=0.05
    )
    assert result.timed_out is True
    assert boom_count["n"] == 1
    # Cleanup must have run.
    assert p.participant_id not in s._wait_in_flight
    assert s._active_waits == 0

    # A second wait_for_event must NOT be blocked by stale in-flight state.
    result2 = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=0.05
    )
    assert result2.timed_out is True
    assert p.participant_id not in s._wait_in_flight


@pytest.mark.asyncio
async def test_wait_releases_in_flight_when_metrics_finally_raises() -> None:
    """Belt-and-suspenders companion to the inc() test: a raise from the
    metrics block inside the OUTER finally (labels().inc() / observe() /
    dec()) must not skip the lock+discard that clears _wait_in_flight.
    """
    from sessionweave.metrics import Metrics

    real = Metrics()

    class ExplodingLabels:
        def labels(self, **_kwargs: object) -> None:
            raise RuntimeError("simulated label failure")

    real.wait_for_event_total = ExplodingLabels()  # type: ignore[assignment]
    s = Session("s", SessionConfig(), ConnectionManager(), metrics=real)
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    await s.check_inbox(p.participant_id)

    result = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=0.05
    )
    assert result.timed_out is True
    # Cleanup must still have run despite the metrics failure.
    assert p.participant_id not in s._wait_in_flight
    assert s._active_waits == 0


@pytest.mark.asyncio
async def test_wait_effective_timeout_clamped_by_session_ttl() -> None:
    """The wait must never block past the moment the session would idle-expire.
    Configure a 2-second idle TTL and request a 60-second wait; the relay
    must clamp to ~2 seconds (rounding for clock skew) and report it in
    WaitForEventResult.effective_timeout_seconds.
    """
    s = Session(
        "s",
        SessionConfig(idle_ttl_seconds=2, max_wait_seconds=600),
        ConnectionManager(),
    )
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    result = await s.wait_for_event(
        p.participant_id, names=None, timeout_seconds=60.0
    )
    assert result.timed_out is True
    # The wait should NOT have blocked the full 60s; it was clamped by TTL.
    # bump_activity inside join+wait resets the TTL clock to now, so the
    # effective timeout should be ~idle_ttl_seconds (small slop allowed).
    assert 0.5 < result.effective_timeout_seconds <= 2.5
