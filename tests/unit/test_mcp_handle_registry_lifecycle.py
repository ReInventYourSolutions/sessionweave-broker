"""``HandleRegistry`` must not outlive the MCP connections it maps.

The registry hands each MCP connection a stable ``MCPConnectionHandle`` — the
identity ``ConnectionManager`` binds a participant to. Three properties make
that safe, and each has a way of failing quietly:

* **The key must be stable for the life of the connection.** In mcp 2.x
  ``ctx.session`` is a fresh ``ServerSession`` per *request*, so keying on it
  loses the binding between one tool call and the next — a ``session_join``
  followed by ``not_in_session``. The stable object is
  ``mcp.server.connection.Connection``; see `test_mcp2_sdk_contract.py`.

* **Entries must be released when the connection ends**, both by the weak key
  (GC) and by ``connection.exit_stack`` (deterministic). Without the former a
  registry bug pins connections forever; without the latter a participant
  stays "online" until the periodic presence sweep notices.

* **A new connection must never inherit a dead one's handle.** A handle still
  bound in ``ConnectionManager`` would hand the newcomer a participant
  identity it never joined as.
"""
from __future__ import annotations

import gc
import weakref
from typing import Any

import pytest
from mcp.server.connection import Connection

from sessionweave.mcp_adapter import HandleRegistry


class FakeOutbound:
    """Minimal ``Outbound`` — enough to build a loop-mode ``Connection``."""

    def __init__(self) -> None:
        self.notified: list[tuple[str, Any]] = []
        self._closed = False

    async def notify(self, method: str, params: Any, opts: Any = None) -> None:
        self.notified.append((method, params))

    async def send_raw_request(self, *a: Any, **k: Any) -> Any:  # pragma: no cover
        raise NotImplementedError


def _connection(session_id: str = "sid-1") -> Connection:
    return Connection.for_loop(FakeOutbound(), session_id=session_id)


def _collect() -> None:
    """Force a full collection — handle/bridge pairs are cyclic."""
    gc.collect()


def test_the_same_connection_always_gets_the_same_handle() -> None:
    """The property the whole binding depends on. Under mcp 2.x this is
    exactly what keying on ``ServerSession`` lost."""
    reg = HandleRegistry()
    conn = _connection()
    assert reg.for_connection(conn) is reg.for_connection(conn)


def test_distinct_connections_get_distinct_handles() -> None:
    reg = HandleRegistry()
    a, b = _connection("a"), _connection("b")
    assert reg.for_connection(a) is not reg.for_connection(b)


def test_registry_does_not_keep_the_connection_alive() -> None:
    """A strong reference here would pin the Connection, its dispatcher and
    both memory streams, for every connection the relay ever served."""
    reg = HandleRegistry()
    conn = _connection()
    ref = weakref.ref(conn)
    reg.for_connection(conn)

    del conn
    _collect()

    assert ref() is None, "the registry pinned the Connection"


def test_the_eviction_callback_does_not_pin_the_connection() -> None:
    """The subtle one. The callback lives on ``connection.exit_stack``, so a
    callback that closes over the connection strongly creates
    ``connection -> exit_stack -> callback -> connection`` and defeats the
    weak key. It must close over a weakref (or over the handle)."""
    reg = HandleRegistry()
    conn = _connection()
    ref = weakref.ref(conn)
    reg.for_connection(conn)  # pushes the eviction callback

    del conn
    _collect()

    assert ref() is None, (
        "the eviction callback closed over the Connection strongly — the "
        "weak key cannot release the entry"
    )


@pytest.mark.asyncio
async def test_closing_the_connection_evicts_the_entry() -> None:
    """Deterministic eviction, independent of the GC."""
    reg = HandleRegistry()
    conn = _connection()
    reg.for_connection(conn)
    assert len(reg) == 1

    await conn.exit_stack.aclose()

    assert len(reg) == 0, "exit_stack unwind did not evict the registry entry"


@pytest.mark.asyncio
async def test_a_closed_connections_handle_reports_not_alive() -> None:
    """``is_alive`` gates push-vs-buffer routing and the presence sweep. A
    handle whose connection the SDK has torn down is dead, and saying so is
    what lets an event be buffered instead of dropped."""
    reg = HandleRegistry()
    conn = _connection()
    handle = reg.for_connection(conn)
    assert handle.is_alive() is True

    await conn.exit_stack.aclose()

    assert handle.is_alive() is False


def test_the_eviction_callback_is_registered_once_per_connection() -> None:
    """``for_connection`` runs on every tool call; pushing a callback each
    time would grow the exit stack for the life of the connection."""
    reg = HandleRegistry()
    conn = _connection()
    for _ in range(5):
        reg.for_connection(conn)

    # One callback for the handle. Compare against a pristine connection so
    # this does not depend on what else the SDK puts on the stack.
    baseline = len(_connection().exit_stack._exit_callbacks)  # type: ignore[attr-defined]
    assert len(conn.exit_stack._exit_callbacks) == baseline + 1  # type: ignore[attr-defined]


def test_a_new_connection_never_inherits_a_dead_connection_handle() -> None:
    """CPython hands a fresh object the address just freed, so an identity
    map keyed on the address would return the dead connection's handle —
    along with whatever participant binding it still carries."""
    reg = HandleRegistry()
    # Immediate reuse reproduces on every trial in CPython; 40 is a wide
    # margin over the 1 needed, while keeping the collections cheap.
    trials = 40
    inherited = 0
    for _ in range(trials):
        a = _connection()
        addr = id(a)
        handle_a = reg.for_connection(a)
        del a
        _collect()

        b = _connection()
        if id(b) == addr and reg.for_connection(b) is handle_a:
            inherited += 1
        del b
        _collect()

    assert inherited == 0, (
        f"{inherited}/{trials} new connections were handed a dead "
        "connection's handle"
    )


def test_discard_removes_the_entry() -> None:
    """The explicit escape hatch keeps working."""
    reg = HandleRegistry()
    conn = _connection()
    handle = reg.for_connection(conn)

    assert reg.discard(conn) is handle
    assert reg.for_connection(conn) is not handle
