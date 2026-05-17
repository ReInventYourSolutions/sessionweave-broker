"""Unit tests for T2.7 subscriber dispatch fairness + failure counter.

When ``_deliver_or_buffer`` fans out to multiple subscribers, a failing
callback (e.g. a slow consumer whose queue is full) must NOT prevent
healthy peers from receiving the event. A failure metric records the
incident so operators can see slow consumers in /metrics output.
"""
from __future__ import annotations

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.metrics import Metrics
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_failing_subscriber_does_not_starve_healthy_peer() -> None:
    m = Metrics()
    s = Session("s", SessionConfig(), ConnectionManager(), metrics=m)
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    pid = p.participant_id

    delivered_to_healthy: list[tuple[str, dict[str, object]]] = []

    def healthy(name: str, payload: dict[str, object]) -> None:
        delivered_to_healthy.append((name, payload))

    def broken(name: str, payload: dict[str, object]) -> None:
        raise RuntimeError("queue full")

    # Both attached for the same pid. Order matters: broken first so we
    # prove the iteration continues past a failure.
    s._subscribers.setdefault(pid, []).extend([broken, healthy])

    s._deliver_or_buffer(pid, "command.received", {"x": 1})

    assert delivered_to_healthy == [("command.received", {"x": 1})]
    rendered = m.render().decode("utf-8")
    assert "sessionweave_subscriber_dispatch_failed_total 1.0" in rendered


@pytest.mark.asyncio
async def test_all_subscribers_failing_falls_through_to_buffer() -> None:
    """If every subscriber fails, the event must land in the buffer so
    it isn't silently lost — the participant can still drain it via
    check_inbox or a fresh wait_for_event."""
    s = Session("s", SessionConfig(), ConnectionManager())
    h = FakeHandle()
    p = await s.join(h, role=None, capabilities=[], delivery_mode="pull")
    pid = p.participant_id

    def broken(name: str, payload: dict[str, object]) -> None:
        raise RuntimeError("queue full")

    s._subscribers.setdefault(pid, []).append(broken)

    s._deliver_or_buffer(pid, "result.received", {"y": 2})

    # check_inbox is the buffer drain.
    inbox = await s.check_inbox(pid)
    assert inbox == [{"name": "result.received", "payload": {"y": 2}}]
