"""WebSocket events endpoint for real-time push to non-MCP clients.

HTTP REST clients are pull-only by definition (each call is a new
request), so they normally drain notifications via
``POST /v1/sessions/{sid}/participants/me/inbox/drain``. This module
adds an optional WebSocket upgrade so the same client can receive
live ``command.received`` / ``result.received`` / etc. events without
busy-polling.

Auth shape mirrors the REST surface (sub-project #1): the bearer token
IS the participant_id. Browsers can't easily set an ``Authorization``
header on a WebSocket upgrade, so the token is accepted as the
``token`` query parameter. Once sub-project #3 swaps tokens for JWTs
this same query param will carry the JWT.

Wire format: each frame is a single JSON object
``{"name": "<event>", "payload": {...}}`` matching the REST inbox-drain
schema. The handler also forwards any pending buffered events at
connect-time before streaming subsequent live events, so a client that
just joined doesn't need a separate REST drain to seed itself.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from sessionweave.errors import ErrorCode, ToolError
from sessionweave.tools import ToolDeps

log = logging.getLogger(__name__)

# Bounded queue between the synchronous _deliver_or_buffer callback and
# the async send loop. If the producer outruns the websocket consumer,
# put_nowait raises QueueFull — Session._deliver_or_buffer catches that
# and routes the event to the buffer so it isn't dropped, and the
# client can recover via REST inbox drain after disconnecting.
_QUEUE_MAX = 256

# Custom WS close codes: 4000-4999 is the application-defined range
# per RFC 6455. We use 44xx to mirror the corresponding HTTP status.
_CLOSE_MISSING_TOKEN = 4401
_CLOSE_NOT_IN_SESSION = 4401
_CLOSE_SESSION_NOT_FOUND = 4404


async def events_ws(websocket: WebSocket) -> None:
    sid = websocket.path_params["sid"]
    deps: ToolDeps = websocket.app.state.tool_deps

    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=_CLOSE_MISSING_TOKEN, reason="missing token")
        return

    try:
        session = deps.registry.get_or_raise(sid)
    except ToolError as exc:
        code = (
            _CLOSE_SESSION_NOT_FOUND
            if exc.code is ErrorCode.SESSION_NOT_FOUND
            else _CLOSE_NOT_IN_SESSION
        )
        await websocket.close(code=code, reason=exc.code.value)
        return

    if token not in session._participants:
        await websocket.close(
            code=_CLOSE_NOT_IN_SESSION, reason="not_in_session"
        )
        return

    pid = token
    await websocket.accept()

    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
        maxsize=_QUEUE_MAX
    )

    def callback(name: str, payload: dict[str, Any]) -> None:
        # Invoked from inside Session._deliver_or_buffer while the
        # session lock is held. put_nowait is sync and safe on the same
        # event loop. If the queue is full we raise: Session catches and
        # falls back to the buffer.
        queue.put_nowait((name, payload))

    # Items the callback handed us but the socket failed to deliver
    # before disconnect. detach_subscriber pushes them back onto the
    # per-participant buffer so a REST drain (or a fresh WS attach) can
    # recover them — otherwise _deliver_or_buffer saw the callback
    # succeed and never wrote the event anywhere durable.
    undelivered: list[tuple[str, dict[str, Any]]] = []

    # We must concurrently await ``websocket.receive()`` so a peer-side
    # close is observed even when our send loop is blocked in
    # ``queue.get()`` waiting for events. Without this, a closed socket
    # behind a reverse proxy can stay "alive" from Starlette's
    # perspective until the next ``send_json`` actually fails — by
    # which time we've already handed the event to (an effectively
    # dead) ``send_json`` and counted it delivered.
    async def _watch_close() -> None:
        try:
            while True:
                await websocket.receive()
        except WebSocketDisconnect:
            return

    backlog = await session.attach_subscriber(pid, callback)
    closer: asyncio.Task[None] | None = None
    try:
        for item in backlog:
            name = item["name"]
            payload = item["payload"]
            try:
                await websocket.send_json({"name": name, "payload": payload})
            except WebSocketDisconnect:
                undelivered.append((name, payload))
                raise

        closer = asyncio.create_task(_watch_close())
        while True:
            getter: asyncio.Task[tuple[str, dict[str, Any]]] = asyncio.create_task(
                queue.get()
            )
            done, _pending = await asyncio.wait(
                {getter, closer}, return_when=asyncio.FIRST_COMPLETED
            )
            if closer in done:
                # Peer closed — abandon the in-flight getter and exit. If
                # an event happened to land in the queue between the
                # close being observed and us bailing out, the
                # finally-block drain will catch it.
                getter.cancel()
                break
            name, payload = getter.result()
            if closer.done():
                undelivered.append((name, payload))
                break
            try:
                await websocket.send_json({"name": name, "payload": payload})
            except WebSocketDisconnect:
                undelivered.append((name, payload))
                raise
    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("WebSocket events handler crashed for session=%s pid=%s", sid, pid)
    finally:
        if closer is not None and not closer.done():
            closer.cancel()
            try:
                await closer
            except (asyncio.CancelledError, WebSocketDisconnect):
                pass
        # Drain anything the producer enqueued while we were tearing down.
        while True:
            try:
                pending = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            undelivered.append(pending)
        await session.detach_subscriber(pid, callback, recover=undelivered or None)


def routes() -> list[WebSocketRoute]:
    """Return the list of WebSocket routes to mount on the main app."""
    return [
        WebSocketRoute(
            "/v1/sessions/{sid}/participants/me/events",
            events_ws,
        ),
    ]


__all__ = ["events_ws", "routes"]
