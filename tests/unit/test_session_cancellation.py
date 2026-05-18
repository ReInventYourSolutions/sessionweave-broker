from datetime import UTC, datetime, timedelta

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_leave_during_inflight_cancels_with_sender_gone() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={},
    )
    h_b.sent.clear()
    await s.leave(a.participant_id)
    cancelled = [(n, p) for n, p in h_b.sent if n == "command.cancelled"]
    assert cancelled
    assert cancelled[0][1]["reason"] == "sender_gone"


@pytest.mark.asyncio
async def test_tick_closes_expired_trackers_and_emits_completed_and_cancelled() -> None:
    s = Session("s", SessionConfig(default_command_timeout_seconds=1), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={},
    )
    h_a.sent.clear()
    h_b.sent.clear()
    future = datetime.now(UTC) + timedelta(seconds=2)
    s.tick(now=future)
    assert any(n == "command.completed" and p["reason"] == "ttl_expired" for n, p in h_a.sent)
    assert any(n == "command.cancelled" and p["reason"] == "ttl_expired" for n, p in h_b.sent)


@pytest.mark.asyncio
async def test_tick_silently_evicts_already_completed_tracker_no_second_event() -> None:
    """When a tracker is is_complete() (all_responded earlier) and then
    its TTL elapses, tick() must NOT emit a second command.completed with
    reason=ttl_expired. The sender already saw reason=all_responded; a
    duplicate event with a different reason confuses cockpits and SDKs."""
    s = Session("s", SessionConfig(default_command_timeout_seconds=1),
                ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    send_out = await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={},
    )
    # b responds in time → tracker is_complete, all_responded emitted
    await s.respond(
        responder_id=b.participant_id,
        command_id=send_out.command_id,
        status="ok",
        payload={"ok": True},
    )
    # Verify the all_responded event landed on a's handle
    initial_completed = [(n, p) for n, p in h_a.sent if n == "command.completed"]
    assert any(p["reason"] == "all_responded" for _, p in initial_completed)
    h_a.sent.clear()
    h_b.sent.clear()
    # Now jump past the TTL — tick should silently evict the already-
    # complete tracker; NO second command.completed must fire.
    future = datetime.now(UTC) + timedelta(seconds=2)
    s.tick(now=future)
    second = [(n, p) for n, p in h_a.sent if n == "command.completed"]
    assert second == [], (
        f"expected no second command.completed after silent eviction, got {second}"
    )
    # And no command.cancelled for the (already-empty) pending list
    cancelled = [(n, p) for n, p in h_b.sent if n == "command.cancelled"]
    assert cancelled == []
    # Tracker is gone from _inflight (the eviction did happen)
    assert send_out.command_id not in s._inflight


@pytest.mark.asyncio
async def test_tick_evicts_offline_past_grace() -> None:
    s = Session(
        "s",
        SessionConfig(offline_grace_seconds=1, default_command_timeout_seconds=300),
        ConnectionManager(),
    )
    h_a, h_b = FakeHandle(), FakeHandle()
    _ = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    s.handle_disconnect(h_b)
    h_a.sent.clear()
    future = datetime.now(UTC) + timedelta(seconds=3)
    s.tick(now=future)
    assert any(n == "participant.left" and p["reason"] == "offline_timeout" for n, p in h_a.sent)
    assert b.participant_id not in {p.participant_id for p in s.roster_snapshot()}
