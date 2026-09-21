"""``Session.handle_disconnect`` — the MCP transport's disconnect callback.

This method existed but had no caller: under mcp 1.x the SDK gave the relay
no connection-close signal, so a vanished client was noticed only by the
periodic presence sweep, which goes through ``_mark_offline`` instead. mcp
2.x provides ``connection.exit_stack``, so the callback is now wired and two
latent defects in it matter.

Both are the kind that leave no trace: the participant simply never leaves
the roster, or a gauge drifts. Neither raises.
"""
from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.metrics import Metrics
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


def _online(m: Metrics) -> float:
    return m.participants_active.labels(online="true")._value.get()  # type: ignore[attr-defined,no-any-return,unused-ignore]


def _offline(m: Metrics) -> float:
    return m.participants_active.labels(online="false")._value.get()  # type: ignore[attr-defined,no-any-return,unused-ignore]


@pytest.mark.asyncio
async def test_handle_disconnect_reschedules_the_grace_deadline() -> None:
    """Going offline introduces an offline-grace deadline, and the registry
    only ticks a session when its heap entry comes due. Without telling the
    registry the deadline moved closer, the grace eviction never becomes
    due: the participant shows as offline forever and the other
    participants never receive ``participant.left``.

    ``_mark_offline`` does this; ``handle_disconnect`` must too.
    """
    shortened: list[Session] = []
    cm = ConnectionManager()
    sess = Session(
        "s", SessionConfig(), cm,
        on_deadline_shortened=shortened.append,
    )
    handle = FakeHandle()
    await sess.join(handle, role="r", capabilities=[])
    shortened.clear()  # join itself may reschedule

    sess.handle_disconnect(handle)

    assert shortened, (
        "handle_disconnect did not tell the registry its deadline moved "
        "closer — the offline-grace eviction will never come due"
    )


@pytest.mark.asyncio
async def test_handle_disconnect_does_not_double_count_an_offline_participant() -> None:
    """Called twice, or after the presence sweep already marked the
    participant offline, the gauge must not drift. This is the same class
    of defect as the participants_active drift fixed earlier: nothing
    raises, the number is just wrong from then on.
    """
    m = Metrics(registry=CollectorRegistry())
    cm = ConnectionManager()
    sess = Session("s", SessionConfig(), cm, metrics=m)
    handle = FakeHandle()
    await sess.join(handle, role="r", capabilities=[])
    assert _online(m) == 1.0

    sess.handle_disconnect(handle)
    sess.handle_disconnect(handle)

    assert _online(m) == 0.0
    assert _offline(m) == 1.0, (
        "a second disconnect double-counted the participant as offline"
    )


@pytest.mark.asyncio
async def test_handle_disconnect_marks_offline_and_notifies_others() -> None:
    """The behaviour that makes wiring the callback worthwhile: peers learn
    immediately instead of waiting for the next presence sweep."""
    cm = ConnectionManager()
    sess = Session("s", SessionConfig(), cm)
    gone, watcher = FakeHandle(), FakeHandle()
    p_gone = await sess.join(gone, role="r", capabilities=[])
    await sess.join(watcher, role="s", capabilities=[])
    watcher.sent.clear()

    sess.handle_disconnect(gone)

    names = [n for n, _ in watcher.sent]
    assert "participant.updated" in names
    roster = {p.participant_id: p.online for p in sess.roster_snapshot()}
    assert roster[p_gone.participant_id] is False


@pytest.mark.asyncio
async def test_handle_disconnect_ignores_an_unbound_handle() -> None:
    """Idempotent against a handle already unbound by leave or by the
    sweep — the eviction callback can fire after either."""
    cm = ConnectionManager()
    sess = Session("s", SessionConfig(), cm)
    handle = FakeHandle()
    await sess.join(handle, role="r", capabilities=[])
    await sess.leave(sess.roster_snapshot()[0].participant_id)

    sess.handle_disconnect(handle)  # must not raise


@pytest.mark.asyncio
async def test_sweep_then_disconnect_does_not_double_count() -> None:
    """The production interleaving the two-calls test does not reach.

    The presence sweep and the connection-close callback both act on a
    departed participant, and either can win. The sweep goes through
    ``_mark_offline``; only then does ``handle_disconnect`` run. Nothing
    raises if this drifts — the gauge is simply wrong from then on, which
    is the same silent failure as the participants_active drift.
    """
    m = Metrics(registry=CollectorRegistry())
    cm = ConnectionManager()
    sess = Session("s", SessionConfig(), cm, metrics=m)
    handle = FakeHandle()
    await sess.join(handle, role="r", capabilities=[])

    handle.close()  # the transport dies; is_alive() now False
    sess.detect_disconnects()  # the sweep notices first
    sess.handle_disconnect(handle)  # the callback arrives second

    assert _online(m) == 0.0
    assert _offline(m) == 1.0, "the sweep and the callback both counted it"
