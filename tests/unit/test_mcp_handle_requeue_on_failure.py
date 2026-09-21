"""Unit tests for audit T1.2: MCPConnectionHandle requeues events when
_dispatch fails after is_alive() returned True (the delivery-race window).

TDD: test must FAIL before the fix (event silently dropped), and PASS after.
"""
from __future__ import annotations

from typing import Any

import pytest
from mcp.server.connection import Connection

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.mcp_adapter import MCPConnectionHandle
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


async def _build_session_with_participant(
    delivery_mode: str = "push",
) -> tuple[Session, str, ConnectionManager]:
    """Helper: create a session and join one participant, return (sess, pid, cm)."""
    cm = ConnectionManager()
    sess = Session("sess-1", SessionConfig(), cm)
    fake = FakeHandle()
    p = await sess.join(fake, role="agent", capabilities=[], delivery_mode=delivery_mode)
    pid = p.participant_id
    cm.unbind(fake)
    return sess, pid, cm


class _Outbound:
    """Minimal ``Outbound`` whose ``_closed`` flag we can flip.

    That flag is the 2.x stand-in for the 1.x "send raised" signal: the SDK
    sets it when the read loop ends, and ``Connection.notify`` never raises,
    so a dying connection is something the handle must *observe*, not catch.
    """

    def __init__(self) -> None:
        self.notified: list[tuple[str, Any]] = []
        self._closed = False

    async def notify(self, method: str, params: Any, opts: Any = None) -> None:
        self.notified.append((method, params))

    async def send_raw_request(self, *a: Any, **k: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


def _make_failing_mcp_handle(
    requeue_callback: Any = None,
) -> tuple[MCPConnectionHandle, Connection]:
    """A handle whose connection is already torn down on the wire.

    The connection is returned so the caller keeps it alive — the handle
    holds it weakly, and a collected connection would exercise a different
    branch than the one under test.
    """
    outbound = _Outbound()
    conn = Connection.for_loop(outbound, session_id="dead")
    handle = MCPConnectionHandle(conn, requeue_callback=requeue_callback)
    outbound._closed = True
    return handle, conn


# ---------------------------------------------------------------------------
# Regression test: without a callback the event must NOT crash — it's just
# dropped (old behaviour, still correct when no callback is provided).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_failure_does_not_raise() -> None:
    """_dispatch should swallow the transport error and mark the handle dead."""
    handle, _conn = _make_failing_mcp_handle(requeue_callback=None)
    handle.send("test.event", {"x": 1})
    for task in list(handle._pending):
        await task
    assert not handle.is_alive()


# ---------------------------------------------------------------------------
# Core regression: when a requeue_callback is provided, _dispatch must invoke
# it on send_notification failure so the event lands in the participant buffer.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_failure_calls_requeue_callback() -> None:
    """When _dispatch raises, the requeue_callback receives
    ``(name, payload, seq)`` so the event can be re-buffered with
    its original sequence number preserved.
    """
    requeued: list[tuple[str, dict[str, Any], int]] = []

    async def _cb(name: str, payload: dict[str, Any], seq: int) -> None:
        requeued.append((name, payload, seq))

    handle, _conn = _make_failing_mcp_handle(requeue_callback=_cb)
    handle.send("test.event", {"x": 1}, seq=7)
    for task in list(handle._pending):
        await task

    assert requeued == [("test.event", {"x": 1}, 7)]
    assert not handle.is_alive()


# ---------------------------------------------------------------------------
# End-to-end: callback wired to session.requeue_to_buffer -> event in buffer.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_failure_requeues_event_to_session_buffer() -> None:
    """After a failing send, the event must appear in the participant's inbox."""
    sess, pid, cm = await _build_session_with_participant(delivery_mode="push")

    # Wire up the requeue callback the way HandleRegistry.for_session will.
    async def _requeue(name: str, payload: dict[str, Any], seq: int) -> None:
        await sess.requeue_to_buffer(pid, [(name, payload, seq)])

    handle, _conn = _make_failing_mcp_handle(requeue_callback=_requeue)
    cm.bind(handle, sess.session_id, pid)

    try:
        handle.send("test.event", {"x": 99}, seq=42)
        # Drain all pending tasks
        for task in list(handle._pending):
            await task

        inbox = await sess.check_inbox(pid)
        assert inbox == [
            {"name": "test.event", "payload": {"x": 99}, "seq": 42}
        ], f"Expected event in buffer after failed send, got: {inbox!r}"
    finally:
        cm.unbind(handle)


# ---------------------------------------------------------------------------
# Verify that a successful send does NOT requeue (no double delivery).
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_send_does_not_requeue() -> None:
    """A successful _dispatch must not invoke the requeue_callback."""
    requeued: list[Any] = []

    async def _cb(name: str, payload: dict[str, Any], seq: int) -> None:
        requeued.append((name, payload, seq))

    outbound = _Outbound()
    conn = Connection.for_loop(outbound, session_id="live")
    handle = MCPConnectionHandle(conn, requeue_callback=_cb)
    handle.send("ok.event", {"y": 2})
    for task in list(handle._pending):
        await task

    assert requeued == [], "requeue_callback must NOT fire on a successful send"
    assert handle.is_alive()
