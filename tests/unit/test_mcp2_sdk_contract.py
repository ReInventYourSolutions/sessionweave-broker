"""Facts about the mcp SDK that the relay's connection model rests on.

The relay needs one stable identity per MCP client connection: something to
key a participant binding on, something to push an out-of-band notification
through, and a signal for when the connection ends. In mcp 2.x that is
``mcp.server.connection.Connection`` — ``ctx.session`` is a fresh
``ServerSession`` per *request* and cannot serve.

The SDK exposes no public accessor for it. ``ServerSession._connection`` is
private, so a rename in a future 2.x release would break the relay at
runtime, on the first tool call, in production. These tests turn that into a
red test here instead. They exercise the SDK alone — no relay code — and are
the reason ``pyproject.toml`` pins ``mcp>=2.2,<3`` rather than ``>=2.0``.

If one of these fails after an SDK bump, do not paper over it: the binding
model needs revisiting, and `scratchpad/DESIGN-mcp2.md` records the reasoning.
"""
from __future__ import annotations

import weakref
from typing import Any

import pytest
from mcp.server.connection import Connection
from mcp.server.session import ServerSession


class FakeOutbound:
    """Minimal ``Outbound``: enough to build a loop-mode ``Connection``."""

    def __init__(self) -> None:
        self.notified: list[tuple[str, Any]] = []
        self._closed = False

    async def notify(self, method: str, params: Any, opts: Any = None) -> None:
        self.notified.append((method, params))

    async def send_raw_request(self, *a: Any, **k: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


def _connection(session_id: str = "sid-1") -> Connection:
    return Connection.for_loop(FakeOutbound(), session_id=session_id)


def test_connection_is_weak_referenceable() -> None:
    """The registry keys on it weakly, so a dead connection's entry
    disappears on its own even if explicit eviction never runs."""
    conn = _connection()
    ref = weakref.ref(conn)
    assert ref() is conn
    del conn
    import gc

    gc.collect()
    assert ref() is None


def test_connection_is_identity_hashable() -> None:
    """WeakKeyDictionary needs hashing by identity, not by value."""
    a, b = _connection("s"), _connection("s")
    assert a != b
    assert len({a, b}) == 2


def test_server_session_exposes_its_connection() -> None:
    """THE load-bearing private attribute. ``connection_of`` in
    mcp_adapter.py is the single place that reads it; if this assertion
    fails, that function must be revisited before anything else."""
    conn = _connection()
    session = ServerSession(None, conn)  # type: ignore[arg-type]

    assert getattr(session, "_connection", None) is conn, (
        "ServerSession._connection is gone or renamed — the relay has no "
        "other way to reach the per-connection identity from a tool call"
    )


@pytest.mark.asyncio
async def test_exit_stack_callback_fires_exactly_once_on_close() -> None:
    """The eviction hook. Under 1.x there was no connection-close signal at
    all and disconnects were noticed only by the periodic presence sweep."""
    conn = _connection()
    calls: list[int] = []
    conn.exit_stack.callback(lambda: calls.append(1))

    await conn.exit_stack.aclose()
    await conn.exit_stack.aclose()

    assert calls == [1], f"expected exactly one eviction callback, got {calls}"


@pytest.mark.asyncio
async def test_notify_never_raises_even_with_no_back_channel() -> None:
    """Informational, and a warning to the implementation: 2.x swallows
    transport errors at every layer, so a failed push does NOT surface as an
    exception. The requeue path must therefore be driven by liveness checks
    around the send, never by catching from ``notify``."""
    # from_envelope is the 2026-07-28 single-exchange path: one POST in, one
    # response out, and the default outbound is the _NoChannelOutbound
    # sentinel — no back-channel at all.
    conn = Connection.from_envelope("2026-07-28", None, None)
    assert conn.has_standalone_channel is False

    await conn.notify("notifications/message", {"level": "info"})  # must not raise


@pytest.mark.asyncio
async def test_notify_reaches_the_outbound_channel_in_loop_mode() -> None:
    """The positive case: a loop-mode connection really does hand the
    notification to its outbound channel, with no request in flight."""
    outbound = FakeOutbound()
    conn = Connection.for_loop(outbound, session_id="sid-1")
    assert conn.has_standalone_channel is True

    await conn.notify("notifications/message", {"level": "info", "data": {"x": 1}})

    assert outbound.notified == [
        ("notifications/message", {"level": "info", "data": {"x": 1}})
    ]
