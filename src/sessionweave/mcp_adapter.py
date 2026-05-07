from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from mcp.types import (
    LoggingMessageNotification,
    LoggingMessageNotificationParams,
    ServerNotification,
)

if TYPE_CHECKING:  # pragma: no cover
    from mcp.server.session import ServerSession


logger = logging.getLogger(__name__)

# Wire format for relay events. We tunnel application notifications through
# the standard ``notifications/message`` (LoggingMessageNotification) channel
# so that any spec-conforming MCP client surfaces them — custom-method
# notifications get dropped at the client's ServerNotification validator.
# Recipients extract ``data._name`` and ``data._payload``.
_LOGGER_NAME = "sessionweave"


class MCPConnectionHandle:
    """ConnectionHandle adapter wrapping a per-client MCP ``ServerSession``.

    One adapter exists per MCP client connection. It is the stable identity
    used by ``ConnectionManager`` to bind/unbind a participant.

    ``send`` schedules an MCP JSON-RPC notification fire-and-forget on the
    running event loop. The Session/Registry callers are synchronous and
    typically hold a per-session lock; deferring the actual write avoids
    blocking those critical sections.
    """

    __slots__ = ("_alive", "_pending", "_session")

    def __init__(self, session: ServerSession) -> None:
        self._session = session
        self._alive = True
        self._pending: set[asyncio.Task[None]] = set()

    @property
    def server_session(self) -> ServerSession:
        return self._session

    def send(self, name: str, payload: dict[str, Any]) -> None:
        if not self._alive:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - no running loop
            return
        task = loop.create_task(self._dispatch(name, payload))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    def is_alive(self) -> bool:
        if not self._alive:
            return False
        # The MCP transport closes ``_write_stream`` when the client
        # session ends (HTTP disconnect or session DELETE). Latch the
        # observation so subsequent checks short-circuit.
        ws = getattr(self._session, "_write_stream", None)
        if ws is not None and getattr(ws, "_closed", False):
            self._alive = False
            return False
        return True

    def mark_dead(self) -> None:
        self._alive = False

    async def _dispatch(self, name: str, payload: dict[str, Any]) -> None:
        notif = ServerNotification(
            LoggingMessageNotification(
                params=LoggingMessageNotificationParams(
                    level="info",
                    logger=_LOGGER_NAME,
                    data={"_name": name, "_payload": payload},
                ),
            ),
        )
        try:
            await self._session.send_notification(notif)
        except Exception as exc:  # pragma: no cover - transport failure paths
            logger.debug("dropping notification %s: %s", name, exc)
            self._alive = False


class HandleRegistry:
    """Maps an ``id(ServerSession)`` to its stable ``MCPConnectionHandle``."""

    __slots__ = ("_handles",)

    def __init__(self) -> None:
        self._handles: dict[int, MCPConnectionHandle] = {}

    def for_session(self, session: ServerSession) -> MCPConnectionHandle:
        key = id(session)
        h = self._handles.get(key)
        if h is None:
            h = MCPConnectionHandle(session)
            self._handles[key] = h
        return h

    def discard(self, session: ServerSession) -> MCPConnectionHandle | None:
        return self._handles.pop(id(session), None)
