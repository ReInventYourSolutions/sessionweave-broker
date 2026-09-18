from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from mcp.types import (
    LoggingMessageNotification,
    LoggingMessageNotificationParams,
    ServerNotification,
)

if TYPE_CHECKING:  # pragma: no cover
    from mcp.server.session import ServerSession

    from sessionweave.connections import ConnectionManager
    from sessionweave.registry import SessionRegistry


logger = logging.getLogger(__name__)

# Wire format for relay events. We tunnel application notifications through
# the standard ``notifications/message`` (LoggingMessageNotification) channel
# so that any spec-conforming MCP client surfaces them — custom-method
# notifications get dropped at the client's ServerNotification validator.
# Recipients extract ``data._name`` and ``data._payload``.
_LOGGER_NAME = "sessionweave"

# Type alias for the optional failure-recovery callback injected at construction.
# The callback receives (event_name, payload, seq) and is responsible for
# requeuing the event to the appropriate participant buffer with its
# original sequence number preserved (so a reconnecting client's
# ``last_acked_seq`` filter still applies).
_RequeueCB = Callable[[str, dict[str, Any], int], Coroutine[Any, Any, None]]


class MCPConnectionHandle:
    """ConnectionHandle adapter wrapping a per-client MCP ``ServerSession``.

    One adapter exists per MCP client connection. It is the stable identity
    used by ``ConnectionManager`` to bind/unbind a participant.

    ``send`` schedules an MCP JSON-RPC notification fire-and-forget on the
    running event loop. The Session/Registry callers are synchronous and
    typically hold a per-session lock; deferring the actual write avoids
    blocking those critical sections.

    The optional *requeue_callback* closes the delivery-race window: when
    ``_dispatch`` successfully schedules a send but the transport dies before
    the coroutine runs, ``send_notification`` raises and the event would be
    silently dropped (``is_alive()`` returned True at decision time, so the
    buffer path was skipped).  If a *requeue_callback* is provided, ``_dispatch``
    calls it on any send failure so the event lands back in the participant's
    buffer and can be recovered by a later ``inbox/drain`` or ``wait_for_event``.
    """

    __slots__ = ("_alive", "_pending", "_requeue_callback", "_session")

    def __init__(
        self,
        session: ServerSession,
        requeue_callback: _RequeueCB | None = None,
    ) -> None:
        # Held weakly, deliberately. The MCP layer owns the ServerSession
        # for the life of the connection; a strong reference here (plus a
        # registry entry that was never released) kept every connection the
        # relay had ever served reachable for the life of the process. It
        # also let ``HandleRegistry``'s address-keyed map hand a fresh
        # connection a dead one's handle once entries were released.
        self._session: weakref.ref[ServerSession] = weakref.ref(session)
        self._alive = True
        self._pending: set[asyncio.Task[None]] = set()
        self._requeue_callback = requeue_callback

    @property
    def server_session(self) -> ServerSession | None:
        """The live ServerSession, or ``None`` once the connection is gone."""
        return self._session()

    def send(
        self,
        name: str,
        payload: dict[str, Any],
        *,
        seq: int | None = None,
    ) -> None:
        if not self._alive:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - no running loop
            return
        task = loop.create_task(self._dispatch(name, payload, seq))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    def is_alive(self) -> bool:
        if not self._alive:
            return False
        session = self._session()
        if session is None:
            # The MCP layer released the connection and it has been
            # collected. Latch it so the presence sweep can mark the
            # participant offline.
            self._alive = False
            return False
        # The MCP transport closes ``_write_stream`` when the client
        # session ends (HTTP disconnect or session DELETE). Latch the
        # observation so subsequent checks short-circuit.
        ws = getattr(session, "_write_stream", None)
        if ws is not None and getattr(ws, "_closed", False):
            self._alive = False
            return False
        return True

    def mark_dead(self) -> None:
        self._alive = False

    async def _dispatch(
        self, name: str, payload: dict[str, Any], seq: int | None
    ) -> None:
        # Per-event ``_seq`` is tunnelled in the LoggingMessageNotification
        # ``data`` envelope so MCP clients that care about dedup can read
        # it the same way they read ``_name`` and ``_payload``. Pre-seq
        # callers (legacy paths, REST handles) pass ``seq=None``; the
        # envelope field is omitted in that case so older clients don't
        # see a confusing zero.
        data: dict[str, Any] = {"_name": name, "_payload": payload}
        if seq is not None and seq > 0:
            data["_seq"] = seq
        notif = ServerNotification(
            LoggingMessageNotification(
                params=LoggingMessageNotificationParams(
                    level="info",
                    logger=_LOGGER_NAME,
                    data=data,
                ),
            ),
        )
        session = self._session()
        if session is None:
            # Connection collected between scheduling and running this
            # coroutine. Same recovery as a failed send: requeue so the
            # event survives for a later drain.
            self._alive = False
            if self._requeue_callback is not None:
                try:
                    await self._requeue_callback(name, payload, seq or 0)
                except Exception as requeue_exc:  # pragma: no cover
                    logger.debug("requeue failed for %s: %s", name, requeue_exc)
            return
        try:
            await session.send_notification(notif)
        except Exception as exc:
            logger.debug("dropping notification %s: %s", name, exc)
            self._alive = False
            # Requeue the event back to the participant's buffer so it can be
            # recovered via inbox/drain or wait_for_event.  This closes the
            # delivery-race window where is_alive() returned True at dispatch
            # time but the transport closed before send_notification ran.
            if self._requeue_callback is not None:
                try:
                    await self._requeue_callback(name, payload, seq or 0)
                except Exception as requeue_exc:  # pragma: no cover
                    logger.debug(
                        "requeue also failed for %s: %s", name, requeue_exc
                    )


class _RequeueBridge:
    """Thin helper that lets ``HandleRegistry`` wire a requeue callback before
    the ``MCPConnectionHandle`` is exposed to callers.

    The bridge holds a reference to the handle (set immediately after
    construction), the ``ConnectionManager``, and the ``SessionRegistry``.
    When called, it looks up the current (sid, pid) binding from the
    ``ConnectionManager`` — using the late-bound lookup so the callback
    works even if the handle is re-bound between construction and the first
    failed send.
    """

    __slots__ = ("_connections", "_handle", "_registry")

    def __init__(
        self,
        connections: ConnectionManager,
        registry: SessionRegistry,
    ) -> None:
        self._connections = connections
        self._registry = registry
        self._handle: MCPConnectionHandle | None = None

    def set_handle(self, handle: MCPConnectionHandle) -> None:
        self._handle = handle

    async def __call__(
        self, name: str, payload: dict[str, Any], seq: int
    ) -> None:
        if self._handle is None:
            return
        binding = self._connections.session_for(self._handle)
        if binding is None:
            return
        sid, pid = binding
        sess = self._registry.get(sid)
        if sess is None:
            return
        await sess.requeue_to_buffer(pid, [(name, payload, seq)])


class HandleRegistry:
    """Maps each ``ServerSession`` to its stable ``MCPConnectionHandle``.

    Keyed on the session *object*, weakly, so an entry disappears when the
    MCP layer releases the connection. Two reasons it is not an
    ``id(session)`` -> handle dict:

    * nothing ever calls ``discard()``, so an ordinary dict grew without
      bound — one entry per MCP connection ever served, for the life of
      the process;
    * addresses are reused. CPython hands a freshly allocated object the
      address just freed, so once entries *are* released an address-keyed
      map can return a dead connection's handle to a new connection —
      which, if that handle is still bound in ``ConnectionManager``, would
      hand the newcomer a participant identity it never joined as.

    A ``WeakKeyDictionary`` only releases entries if the value does not
    strongly reference the key, which is why ``MCPConnectionHandle`` holds
    its ``ServerSession`` weakly too. The two halves only work together.
    """

    __slots__ = ("_handles",)

    def __init__(self) -> None:
        self._handles: weakref.WeakKeyDictionary[
            ServerSession, MCPConnectionHandle
        ] = weakref.WeakKeyDictionary()

    def __len__(self) -> int:
        return len(self._handles)

    def for_session(
        self,
        session: ServerSession,
        *,
        connections: ConnectionManager | None = None,
        registry: SessionRegistry | None = None,
    ) -> MCPConnectionHandle:
        """Return (or create) the stable handle for *session*.

        When both *connections* and *registry* are supplied the returned handle
        is wired with a requeue callback: if the MCP transport dies after
        ``is_alive()`` returned True but before ``send_notification`` completes,
        the event is pushed back to the participant's buffer instead of being
        silently dropped (audit fix T1.2).
        """
        h = self._handles.get(session)
        if h is None:
            if connections is not None and registry is not None:
                bridge = _RequeueBridge(connections, registry)
                h = MCPConnectionHandle(session, requeue_callback=bridge)
                bridge.set_handle(h)
            else:
                h = MCPConnectionHandle(session)
            self._handles[session] = h
        return h

    def discard(self, session: ServerSession) -> MCPConnectionHandle | None:
        """Drop the entry for *session* explicitly.

        Entries release themselves when the connection is collected, so
        this is only needed to force the issue (tests, or a caller that
        knows a connection is finished before the GC notices).
        """
        return self._handles.pop(session, None)
