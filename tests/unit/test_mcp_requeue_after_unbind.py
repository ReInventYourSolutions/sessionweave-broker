"""An event in flight when its connection dies must still reach the buffer.

``_deliver_or_buffer`` skips the buffer when a handle looks alive, on the
promise that ``_dispatch`` will requeue if the send does not land. The
requeue goes through ``_RequeueBridge``, which resolves the participant by
asking ``ConnectionManager`` who the handle is bound to.

The connection-close callback breaks that promise: it schedules
``Session.handle_disconnect``, which *unbinds* the handle. If that runs
while a dispatch is parked in ``await connection.notify(...)`` — precisely
when a connection is being torn down, which is when dispatches are most
likely to be in flight — the bridge finds no binding, and the event is gone.
It was never buffered, and ``seq`` dedup cannot recover a drop.

This was survivable under 1.x, where only the ≤1 Hz presence sweep unbound.
The 2.x callback fires exactly at teardown, so the window is no longer rare.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest
from mcp.server.connection import Connection

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.mcp_adapter import HandleRegistry
from sessionweave.session import Session


class ParkingOutbound:
    """``notify`` blocks until released, modelling a send in flight."""

    def __init__(self) -> None:
        self.released = asyncio.Event()
        self.notified: list[tuple[str, Any]] = []
        self._closed = False

    async def notify(self, method: str, params: Any, opts: Any = None) -> None:
        await self.released.wait()
        self.notified.append((method, params))

    async def send_raw_request(self, *a: Any, **k: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


class _Registry:
    """Just enough ``SessionRegistry`` surface for the requeue bridge."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get(self, sid: str) -> Session | None:
        return self._session if sid == self._session.session_id else None


@pytest.mark.asyncio
async def test_event_in_flight_survives_the_connection_closing() -> None:
    cm = ConnectionManager()
    sess = Session("s", SessionConfig(), cm)
    handles = HandleRegistry()

    outbound = ParkingOutbound()
    conn = Connection.for_loop(outbound, session_id="sid-1")
    handle = handles.for_connection(
        conn, connections=cm, registry=_Registry(sess)  # type: ignore[arg-type]
    )
    participant = await sess.join(handle, role="r", capabilities=[])

    # An event goes out while the handle still looks alive.
    handle.send("command.received", {"x": 1}, seq=7)
    await asyncio.sleep(0)  # let the dispatch task park inside notify

    # The connection dies underneath it, through the real path: the SDK
    # unwinds the exit stack, which runs the eviction callback, which
    # schedules handle_disconnect — and that unbinds the handle.
    await conn.exit_stack.aclose()
    await asyncio.sleep(0)  # let the scheduled handle_disconnect run
    assert cm.session_for(handle) is None, "precondition: the handle is unbound"

    outbound.released.set()
    for task in list(handle._pending):
        await task

    buffered = await sess.check_inbox(participant.participant_id)
    assert buffered == [
        {"name": "command.received", "payload": {"x": 1}, "seq": 7}
    ], (
        "the in-flight event was dropped: _deliver_or_buffer skipped the "
        "buffer because the handle looked alive, and the requeue could not "
        "find the participant because the disconnect already unbound it"
    )
