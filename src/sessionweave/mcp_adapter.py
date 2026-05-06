from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from mcp.server.session import ServerSession


class MCPConnectionHandle:
    """ConnectionHandle adapter wrapping a per-client MCP ``ServerSession``.

    One adapter exists per MCP client connection. It is the stable identity
    used by ``ConnectionManager`` to bind/unbind a participant.

    The ``send`` path here is a no-op for Task 16. Real MCP notification
    fan-out is wired in Task 17.
    """

    __slots__ = ("_alive", "_session")

    def __init__(self, session: ServerSession) -> None:
        self._session = session
        self._alive = True

    @property
    def server_session(self) -> ServerSession:
        return self._session

    def send(self, name: str, payload: dict[str, Any]) -> None:
        # Task 17 will translate (name, payload) into an MCP JSON-RPC
        # notification and dispatch via ``self._session.send_notification``.
        # For now we drop the message; unit tests cover the buffering /
        # delivery decision via fakes, so end-to-end MCP notifications are
        # validated in Task 17 once this path is implemented.
        _ = (name, payload)

    def is_alive(self) -> bool:
        return self._alive

    def mark_dead(self) -> None:
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
