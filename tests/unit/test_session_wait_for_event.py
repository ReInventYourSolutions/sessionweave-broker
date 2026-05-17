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
    assert result.messages == [{"name": "command.received", "payload": {"x": 1}}]


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
    assert result.messages == [{"name": "command.received", "payload": {"k": "v"}}]


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
    assert leftover == [{"name": "participant.updated", "payload": {"a": 1}}]


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
    assert leftover == [{"name": "participant.updated", "payload": {"a": 1}}]


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
    assert result.messages == [{"name": "command.received", "payload": {"b": 2}}]
    # The non-matching event must still be in the buffer.
    leftover = await s.check_inbox(p.participant_id)
    assert leftover == [{"name": "participant.updated", "payload": {"a": 1}}]


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
async def test_wait_raises_evicted_when_session_ended_mid_wait() -> None:
    """If the session is ended (by supervisor or TTL eviction) while a
    wait_for_event coroutine is in flight, that coroutine must wake with
    PARTICIPANT_EVICTED rather than silently timing out on the full
    budget. Mirrors the leave-mid-wait test but exercises the
    broadcast_session_ended path instead.
    """
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")

    waiter = asyncio.create_task(
        s.wait_for_event(p.participant_id, names=None, timeout_seconds=5.0)
    )
    await asyncio.sleep(0.02)
    # Simulate supervisor-triggered session end.
    s.broadcast_session_ended(reason="deleted_by_test")

    with pytest.raises(ToolError) as excinfo:
        await waiter
    assert excinfo.value.code is ErrorCode.PARTICIPANT_EVICTED
