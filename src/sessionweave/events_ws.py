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
``{"name": "<event>", "payload": {...}, "seq": <int>}`` matching the
REST inbox-drain schema. ``seq`` is a per-participant monotonically
increasing sequence number; a client can echo it back as ``{"ack":
N}`` upstream so the relay knows the client has consumed events up to
``N``, and on reconnect pass ``?last_acked_seq=N`` so the relay skips
those events on the buffer replay.

The handler also forwards any pending buffered events at connect-time
before streaming subsequent live events, so a client that just joined
doesn't need a separate REST drain to seed itself.
"""
from __future__ import annotations

import asyncio
import json
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

    # Parse optional ``?last_acked_seq=N`` so a reconnecting client can
    # tell the relay it has already consumed events up to N. The relay
    # will skip those events on the buffer replay. Invalid integers
    # are treated as if the param were absent (full replay).
    raw_acked = websocket.query_params.get("last_acked_seq")
    last_acked_seq: int | None
    if raw_acked is None:
        last_acked_seq = None
    else:
        try:
            last_acked_seq = int(raw_acked)
        except ValueError:
            last_acked_seq = None

    await websocket.accept()

    queue: asyncio.Queue[tuple[str, dict[str, Any], int]] = asyncio.Queue(
        maxsize=_QUEUE_MAX
    )

    def callback(name: str, payload: dict[str, Any], seq: int) -> None:
        # Invoked from inside Session._deliver_or_buffer while the
        # session lock is held. put_nowait is sync and safe on the same
        # event loop. If the queue is full we raise: Session catches and
        # falls back to the buffer.
        queue.put_nowait((name, payload, seq))

    # Items the callback handed us but the socket failed to deliver
    # before disconnect. detach_subscriber pushes them back onto the
    # per-participant buffer so a REST drain (or a fresh WS attach) can
    # recover them — otherwise _deliver_or_buffer saw the callback
    # succeed and never wrote the event anywhere durable.
    undelivered: list[tuple[str, dict[str, Any], int]] = []

    # Events successfully sent over the wire but not yet acknowledged by
    # the client (via an upstream ``{"ack": <seq>}`` frame). Keyed by
    # seq. If the socket dies before an ACK arrives, ``finally`` will
    # push these back onto the per-participant buffer so the next WS
    # attach (or REST drain) can recover them — this is the wire-level
    # guarantee that lets ``?last_acked_seq=N`` deduplicate after a
    # flaky disconnect (issue #43).
    unacked: dict[int, tuple[str, dict[str, Any]]] = {}
    highest_acked_seq: int = last_acked_seq or 0

    # We must concurrently await ``websocket.receive()`` so a peer-side
    # close is observed even when our send loop is blocked in
    # ``queue.get()`` waiting for events. Without this, a closed socket
    # behind a reverse proxy can stay "alive" from Starlette's
    # perspective until the next ``send_json`` actually fails — by
    # which time we've already handed the event to (an effectively
    # dead) ``send_json`` and counted it delivered.
    #
    # We also handle upstream ``{"ack": N}`` frames so a client can
    # advertise the highest seq it has consumed. For now the relay
    # accepts and ignores these (the seq-on-replay path is the
    # critical guarantee); a future revision can trim the buffer
    # eagerly. Non-ACK frames are tolerated to avoid breaking any
    # client that sends ping/pong text frames on the same channel.
    async def _watch_close() -> None:
        nonlocal highest_acked_seq
        try:
            while True:
                msg = await websocket.receive()
                text = msg.get("text") if isinstance(msg, dict) else None
                if not text:
                    continue
                try:
                    body = json.loads(text)
                except (TypeError, ValueError):
                    continue
                if isinstance(body, dict) and isinstance(body.get("ack"), int):
                    n = body["ack"]
                    # Trim unacked of everything up to and including N
                    # and advance the high-water mark. The relay-side
                    # buffer is not touched: events are removed from
                    # ``unacked`` only after the client confirms it has
                    # consumed them in memory.
                    if n > highest_acked_seq:
                        highest_acked_seq = n
                    for seq_key in list(unacked.keys()):
                        if seq_key <= n:
                            unacked.pop(seq_key, None)
        except WebSocketDisconnect:
            return

    backlog = await session.attach_subscriber(
        pid, callback, last_acked_seq=last_acked_seq
    )
    closer: asyncio.Task[None] | None = None
    try:
        for item in backlog:
            name = item["name"]
            payload = item["payload"]
            seq = item.get("seq", 0)
            try:
                await websocket.send_json(
                    {"name": name, "payload": payload, "seq": seq}
                )
            except WebSocketDisconnect:
                undelivered.append((name, payload, seq))
                raise
            else:
                if seq > highest_acked_seq:
                    unacked[seq] = (name, payload)

        closer = asyncio.create_task(_watch_close())
        while True:
            getter: asyncio.Task[tuple[str, dict[str, Any], int]] = asyncio.create_task(
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
            name, payload, seq = getter.result()
            if closer.done():
                undelivered.append((name, payload, seq))
                break
            try:
                await websocket.send_json(
                    {"name": name, "payload": payload, "seq": seq}
                )
            except WebSocketDisconnect:
                undelivered.append((name, payload, seq))
                raise
            else:
                if seq > highest_acked_seq:
                    unacked[seq] = (name, payload)
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
        # Detach FIRST so no new events can land on the queue. detach
        # acquires the session lock, which serialises against any
        # _deliver_or_buffer fan-out — once it returns, the subscriber
        # is gone and any leftover items in the queue are events that
        # landed before the subscriber was removed. THEN drain the
        # queue and push everything to the per-participant buffer in
        # one go. The previous order (drain → detach) had a race where
        # an event arriving between the drain and detach acquiring the
        # lock would be lost.
        await session.detach_subscriber(pid, callback)
        while True:
            try:
                undelivered.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        # Anything we send_json'd but never received an ACK for is, from
        # the client's point of view, potentially unread (its socket may
        # have died mid-frame, or the WS layer drained the TCP buffer
        # past the application). Push those back too so a reconnecting
        # client with ``last_acked_seq=N`` can recover them. Earlier
        # un-ACKed seq goes first so buffer order matches delivery order.
        if unacked:
            for seq in sorted(unacked):
                name, payload = unacked[seq]
                undelivered.append((name, payload, seq))
        if undelivered:
            await session.requeue_to_buffer(pid, undelivered)


async def firehose_ws(websocket: WebSocket) -> None:
    sid = websocket.path_params["sid"]
    token = websocket.query_params.get("token", "")
    if token != sid:
        await websocket.close(code=_CLOSE_MISSING_TOKEN, reason="invalid token")
        return

    registry = websocket.app.state.tool_deps.registry
    firehose = websocket.app.state.firehose
    if registry.get(sid) is None:
        await websocket.close(code=_CLOSE_SESSION_NOT_FOUND, reason="session_not_found")
        return

    await websocket.accept()
    sub = await firehose.subscribe(sid)
    try:
        while True:
            event = await sub.queue.get()
            await websocket.send_text(json.dumps(event))
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        await firehose.unsubscribe(sub)


def routes() -> list[WebSocketRoute]:
    """Return the list of WebSocket routes to mount on the main app."""
    return [
        WebSocketRoute(
            "/v1/sessions/{sid}/participants/me/events",
            events_ws,
        ),
        WebSocketRoute(
            "/v1/sessions/{sid}/firehose",
            firehose_ws,
        ),
    ]


__all__ = ["events_ws", "firehose_ws", "routes"]
