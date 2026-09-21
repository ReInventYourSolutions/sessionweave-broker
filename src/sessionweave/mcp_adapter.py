from __future__ import annotations

import asyncio
import logging
import weakref
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from mcp.server.connection import Connection

if TYPE_CHECKING:  # pragma: no cover
    from mcp.server.mcpserver import Context

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

# The JSON-RPC method the relay tunnels its events through. Unchanged from
# the 1.x ``LoggingMessageNotification`` spelling — the wire format is the
# same, only the SDK call that emits it moved.
_NOTIFY_METHOD = "notifications/message"


def connection_of(ctx: Context) -> Connection:
    """The per-connection ``Connection`` behind a tool call's ``Context``.

    This is the relay's identity for an MCP client: ``ctx.session`` is a
    fresh ``ServerSession`` per *request* in mcp 2.x, so it cannot carry a
    participant binding from one tool call to the next, while every one of
    those per-request objects wraps the same ``Connection`` for the life of
    the ``Mcp-Session-Id``.

    The SDK exposes no public accessor, so this reads the private
    ``ServerSession._connection``. That is deliberately confined to this one
    function and pinned by ``tests/unit/test_mcp2_sdk_contract.py``, which
    turns an SDK rename into a red test rather than a runtime failure.

    Raises ``RuntimeError`` if the attribute is gone. There is no fallback
    to the ``Mcp-Session-Id`` header on purpose: quietly keying on something
    else is how events would start vanishing with every test still green.
    """
    session = ctx.session
    connection = getattr(session, "_connection", None)
    if not isinstance(connection, Connection):
        raise RuntimeError(
            "cannot reach the per-connection Connection via "
            "ServerSession._connection — the mcp SDK changed shape. See "
            "tests/unit/test_mcp2_sdk_contract.py and DESIGN-mcp2.md."
        )
    return connection


class MCPConnectionHandle:
    """ConnectionHandle adapter wrapping a per-client MCP ``Connection``.

    One adapter exists per MCP client connection. It is the stable identity
    used by ``ConnectionManager`` to bind/unbind a participant.

    ``send`` schedules an MCP JSON-RPC notification fire-and-forget on the
    running event loop. The Session/Registry callers are synchronous and
    typically hold a per-session lock; deferring the actual write avoids
    blocking those critical sections. The notification goes out on the
    connection's standalone channel, so it reaches a client that has no
    request in flight — which is the whole point of push delivery.

    The ``Connection`` is held weakly. A strong reference would pin it, its
    dispatcher and both memory streams for every connection the relay ever
    served.

    The optional *requeue_callback* closes the delivery-race window: an event
    can be scheduled while the handle looks alive and then find the
    connection torn down by the time the coroutine runs, with
    ``_deliver_or_buffer`` having already skipped the buffer on the strength
    of ``is_alive()``. The callback puts the event back in the participant's
    buffer, to be recovered by a later ``inbox/drain`` or ``wait_for_event``.

    Note that mcp 2.x swallows transport errors at every layer —
    ``Connection.notify`` is documented as never raising — so requeueing
    cannot be driven by catching an exception the way it was under 1.x. It is
    driven by liveness checks either side of the send instead. A residual
    window remains: the connection can die between the final check and the
    dispatcher's own, which costs that one event. It is microseconds wide and
    only on teardown.
    """

    __slots__ = ("_alive", "_connection", "_pending", "_requeue_callback", "_session_id")

    def __init__(
        self,
        connection: Connection,
        requeue_callback: _RequeueCB | None = None,
    ) -> None:
        self._connection: weakref.ref[Connection] = weakref.ref(connection)
        # Kept as a plain string so logs and metrics can still identify the
        # connection after it has been collected.
        self._session_id: str | None = connection.session_id
        self._alive = True
        self._pending: set[asyncio.Task[None]] = set()
        self._requeue_callback = requeue_callback

    @property
    def connection(self) -> Connection | None:
        """The live ``Connection``, or ``None`` once it has been collected."""
        return self._connection()

    @property
    def session_id(self) -> str | None:
        """The ``Mcp-Session-Id``, for logs and metrics."""
        return self._session_id

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
        connection = self._connection()
        if connection is None:
            # Collected. Latch it so the presence sweep marks the
            # participant offline.
            self._alive = False
            return False
        # Defensive peek at the dispatcher's own closed flag. The SDK sets it
        # when the read loop ends, which is *before* the write stream closes
        # and well before our eviction callback runs — so this narrows the
        # window in which an event would be reported as delivered while the
        # wire has already gone. Informational only: if a future SDK drops
        # the attribute, ``getattr`` yields False and eviction still covers
        # us, one canary test in test_mcp2_sdk_contract.py the wiser.
        if getattr(connection.outbound, "_closed", False):
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
        params: dict[str, Any] = {
            "level": "info",
            "logger": _LOGGER_NAME,
            "data": data,
        }
        # Liveness is checked either side of the send rather than by catching
        # from ``notify``, which never raises in mcp 2.x. The pre-check
        # catches a connection that died between scheduling and running this
        # coroutine; the post-check catches one that died during the await,
        # when ``_deliver_or_buffer`` has already skipped the buffer because
        # the handle looked alive.
        connection = self._connection()
        if connection is None or not self.is_alive():
            await self._requeue(name, payload, seq, reason="connection gone")
            return
        try:
            await connection.notify(_NOTIFY_METHOD, params)
        except Exception as exc:  # pragma: no cover - notify swallows its own
            logger.debug("dropping notification %s: %s", name, exc)
            self._alive = False
            await self._requeue(name, payload, seq, reason=str(exc))
            return
        if not self.is_alive():
            # Died mid-flight. Requeue rather than assume delivery; a
            # duplicate is harmless because the buffer and resume paths
            # dedup on ``seq``, whereas a lost event is not recoverable.
            await self._requeue(name, payload, seq, reason="closed mid-send")

    async def _requeue(
        self, name: str, payload: dict[str, Any], seq: int | None, *, reason: str
    ) -> None:
        if self._requeue_callback is None:
            logger.debug("dropping notification %s (%s)", name, reason)
            return
        try:
            await self._requeue_callback(name, payload, seq or 0)
        except Exception as requeue_exc:  # pragma: no cover
            logger.debug("requeue failed for %s: %s", name, requeue_exc)


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
    """Maps each MCP ``Connection`` to its stable ``MCPConnectionHandle``.

    The key is the ``Connection`` object, held weakly. It is not the
    ``ServerSession`` — mcp 2.x builds a fresh one of those per *request*,
    so a binding made during ``session_join`` would already be unreachable
    by the next tool call. It is not the ``Mcp-Session-Id`` string either:
    a string key has no GC signal, and the weak key is what guarantees an
    entry cannot outlive its connection even if explicit eviction never
    runs. It is emphatically not ``id()`` of anything, because CPython
    hands a freshly allocated object the address just freed, and a handle
    still bound in ``ConnectionManager`` would then give a new connection a
    participant identity it never joined as.

    Eviction is belt and braces:

    * ``connection.exit_stack`` gives a deterministic signal the moment the
      SDK tears the connection down — on client DELETE, idle timeout, crash
      or shutdown. Under 1.x no such hook existed and a disconnect was
      noticed only by the periodic presence sweep.
    * the weak key still releases the entry if that callback somehow never
      runs.

    A ``WeakKeyDictionary`` only releases entries when the value does not
    strongly reference the key, so ``MCPConnectionHandle`` holds its
    ``Connection`` weakly and the eviction callback closes over a weakref
    rather than the connection. Either mistake would quietly pin every
    connection the relay ever served.
    """

    __slots__ = ("_handles",)

    def __init__(self) -> None:
        self._handles: weakref.WeakKeyDictionary[
            Connection, MCPConnectionHandle
        ] = weakref.WeakKeyDictionary()

    def __len__(self) -> int:
        return len(self._handles)

    def for_connection(
        self,
        connection: Connection,
        *,
        connections: ConnectionManager | None = None,
        registry: SessionRegistry | None = None,
    ) -> MCPConnectionHandle:
        """Return (or create) the stable handle for *connection*.

        Called on every tool call that needs a handle, so creation and
        eviction-callback registration share one branch — the callback is
        pushed exactly once per connection rather than growing the exit
        stack for the life of the connection.

        When both *connections* and *registry* are supplied the handle is
        wired with a requeue callback, so an event that loses its race with
        connection teardown lands back in the participant's buffer instead
        of being silently dropped (audit fix T1.2).
        """
        h = self._handles.get(connection)
        if h is not None:
            return h
        if connections is not None and registry is not None:
            bridge = _RequeueBridge(connections, registry)
            h = MCPConnectionHandle(connection, requeue_callback=bridge)
            bridge.set_handle(h)
        else:
            h = MCPConnectionHandle(connection)
        self._handles[connection] = h
        connection.exit_stack.callback(
            self._on_connection_closed,
            weakref.ref(connection),
            h,
            connections,
            registry,
        )
        return h

    def _on_connection_closed(
        self,
        connection_ref: weakref.ref[Connection],
        handle: MCPConnectionHandle,
        connections: ConnectionManager | None,
        registry: SessionRegistry | None,
    ) -> None:
        """Release *handle* when the SDK tears its connection down.

        Takes a weakref rather than the connection: this callback lives on
        ``connection.exit_stack``, so holding the connection strongly would
        close the cycle ``connection -> exit_stack -> callback ->
        connection`` and defeat the weak key.

        Runs synchronously inside the SDK's shielded teardown, which is
        time-bounded and swallows exceptions, so it stays O(1) and defers
        the participant bookkeeping to the event loop rather than reaching
        into a session under a lock here.
        """
        handle.mark_dead()
        connection = connection_ref()
        if connection is not None:
            self._handles.pop(connection, None)
        if connections is None or registry is None:
            return
        binding = connections.session_for(handle)
        if binding is None:
            return
        sid, _pid = binding
        session = registry.get(sid)
        if session is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - teardown outside a loop
            return
        # Scheduled, not called inline: ``handle_disconnect`` expects no
        # Session coroutine to be mid-await, and this runs concurrently with
        # tool calls on other connections. Flipping the participant offline
        # here rather than waiting for the presence sweep is the whole
        # benefit of having a real disconnect signal.
        loop.call_soon(session.handle_disconnect, handle)

    def discard(self, connection: Connection) -> MCPConnectionHandle | None:
        """Drop the entry for *connection* explicitly.

        Entries release themselves on connection teardown and on collection,
        so this is only for forcing the issue (tests, or a caller that knows
        a connection is finished before either signal arrives).
        """
        return self._handles.pop(connection, None)
