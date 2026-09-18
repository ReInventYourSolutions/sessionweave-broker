"""``HandleRegistry`` must not outlive the MCP connections it maps.

The registry hands each MCP ``ServerSession`` a stable
``MCPConnectionHandle`` — the identity ``ConnectionManager`` binds a
participant to. Two things follow from that, and neither held:

* **It must release the entry when the connection ends.** Nothing ever
  called ``discard()``, and the handle holds a strong reference to its
  ``ServerSession`` (``mcp_adapter.py``: ``self._session = session``), so
  every MCP connection the relay had ever served stayed reachable —
  the whole session object graph, not just a small handle — for the life
  of the process.

* **A new connection must never inherit a dead one's handle.** The map was
  keyed on ``id(session)``. CPython reuses the address of a freed object
  immediately (the test below relies on that; it reproduces on every trial),
  so once entries are released, an address-keyed map can hand a fresh
  connection the handle of a dead one — and if that handle is still bound
  in ``ConnectionManager``, the new connection inherits a participant
  identity it never joined as. Today the leak masks this: sessions are
  never freed, so addresses are never reused. Fixing the leak alone would
  unmask it, which is why both belong in one change.
"""
from __future__ import annotations

import gc
import weakref
from typing import Any

from sessionweave.mcp_adapter import HandleRegistry


class FakeServerSession:
    """Stand-in for ``mcp.server.session.ServerSession``.

    The registry only needs an object identity; ``send_notification`` is
    here so a handle can be exercised against it.
    """

    def __init__(self) -> None:
        self.sent: list[Any] = []

    async def send_notification(self, notif: Any) -> None:
        self.sent.append(notif)


def _new_session() -> Any:
    """A fresh stand-in, typed ``Any`` at the boundary.

    ``HandleRegistry`` is annotated for the real ``ServerSession``; the fake
    satisfies it structurally but not nominally, and widening here keeps the
    ignore out of every call site.
    """
    return FakeServerSession()


def _collect() -> None:
    """Force a full collection — the handle/bridge pair is cyclic."""
    gc.collect()


def test_registry_does_not_keep_the_server_session_alive() -> None:
    reg = HandleRegistry()
    session = _new_session()
    ref = weakref.ref(session)
    reg.for_session(session)

    del session
    _collect()

    assert ref() is None, (
        "the registry pinned the ServerSession — every MCP connection the "
        "relay ever served would stay reachable for the life of the process"
    )


def test_registry_drops_its_entry_when_the_connection_is_collected() -> None:
    reg = HandleRegistry()
    for _ in range(50):
        s = _new_session()
        reg.for_session(s)
        del s
    _collect()

    assert len(reg) == 0, (
        f"registry still holds {len(reg)} entries for connections that are gone"
    )


def test_a_new_connection_never_inherits_a_dead_connection_handle() -> None:
    """CPython hands a fresh object the address just freed, so an
    identity map keyed on the address would return the dead
    connection's handle — along with whatever participant binding it
    still carries."""
    reg = HandleRegistry()
    # Immediate reuse reproduces on every trial in CPython; 40 is a wide
    # margin over the 1 needed, while keeping the collections cheap.
    trials = 40
    inherited = 0
    for _ in range(trials):
        a = _new_session()
        addr = id(a)
        handle_a = reg.for_session(a)
        del a
        _collect()

        b = _new_session()
        if id(b) == addr and reg.for_session(b) is handle_a:
            inherited += 1
        del b
        _collect()

    assert inherited == 0, (
        f"{inherited}/{trials} new connections were handed a dead "
        "connection's handle"
    )


def test_the_same_connection_always_gets_the_same_handle() -> None:
    """The stability the binding depends on — must survive the fix."""
    reg = HandleRegistry()
    session = _new_session()
    assert reg.for_session(session) is reg.for_session(session)


def test_handle_reports_not_alive_once_its_connection_is_gone() -> None:
    """``is_alive`` gates delivery (``Session.detect_disconnects``, the
    push path). A handle whose connection has been collected is dead, and
    saying so lets the presence sweep mark the participant offline."""
    reg = HandleRegistry()
    session = _new_session()
    handle = reg.for_session(session)
    assert handle.is_alive() is True

    del session
    _collect()

    assert handle.is_alive() is False


def test_send_after_the_connection_is_gone_is_a_noop() -> None:
    """Callers hold handles (``Participant.connection_ref``) and send
    synchronously while holding a session lock — a vanished connection
    must not raise into that path."""
    reg = HandleRegistry()
    session = _new_session()
    handle = reg.for_session(session)

    del session
    _collect()

    handle.send("participant.joined", {"x": 1}, seq=3)  # must not raise


def test_discard_removes_the_entry() -> None:
    """The explicit escape hatch keeps working."""
    reg = HandleRegistry()
    session = _new_session()
    handle = reg.for_session(session)

    assert reg.discard(session) is handle
    assert reg.for_session(session) is not handle
