from __future__ import annotations

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


def _config() -> SessionConfig:
    return SessionConfig(
        max_participants=8,
        max_payload_bytes=4096,
        idle_ttl_seconds=3600,
        offline_grace_seconds=60,
        default_command_timeout_seconds=10,
        max_command_timeout_seconds=60,
        max_pending_notifications_per_participant=200,
    )


@pytest.mark.asyncio
async def test_pull_mode_buffers_instead_of_pushing() -> None:
    sess = Session("s", _config(), ConnectionManager())
    h_sender = FakeHandle()
    sender = await sess.join(h_sender, role="s")

    h_recv = FakeHandle()
    recv = await sess.join(
        h_recv, role="r", capabilities=["sec"], delivery_mode="pull"
    )

    # The recipient is online; with delivery_mode=pull, the relay must
    # NOT push notifications via the handle.
    await sess.send(
        sender_id=sender.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[recv.participant_id]),
        payload={"hello": 1},
    )
    assert h_recv.sent == [], "pull-mode recipient must not receive push"

    inbox = await sess.check_inbox(recv.participant_id)
    assert any(m["name"] == "command.received" for m in inbox)
    payload = next(m["payload"] for m in inbox if m["name"] == "command.received")
    assert payload["payload"] == {"hello": 1}


@pytest.mark.asyncio
async def test_check_inbox_drains_buffer() -> None:
    sess = Session("s", _config(), ConnectionManager())
    h_sender = FakeHandle()
    sender = await sess.join(h_sender, role="s")
    h_recv = FakeHandle()
    recv = await sess.join(
        h_recv, role="r", capabilities=["sec"], delivery_mode="pull"
    )

    await sess.send(
        sender_id=sender.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[recv.participant_id]),
        payload={"x": 1},
    )

    first = await sess.check_inbox(recv.participant_id)
    assert first, "first drain must yield the buffered notification"
    second = await sess.check_inbox(recv.participant_id)
    assert second == [], "second drain must be empty"


@pytest.mark.asyncio
async def test_check_inbox_unknown_participant_raises() -> None:
    sess = Session("s", _config(), ConnectionManager())
    with pytest.raises(ToolError) as exc:
        await sess.check_inbox("00000000-0000-0000-0000-000000000000")
    assert exc.value.code is ErrorCode.NOT_IN_SESSION


@pytest.mark.asyncio
async def test_push_default_still_delivers_via_handle() -> None:
    sess = Session("s", _config(), ConnectionManager())
    h_sender = FakeHandle()
    sender = await sess.join(h_sender, role="s")
    h_recv = FakeHandle()
    recv = await sess.join(h_recv, role="r")

    await sess.send(
        sender_id=sender.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[recv.participant_id]),
        payload={"y": 2},
    )
    assert any(name == "command.received" for name, _ in h_recv.sent)
    inbox = await sess.check_inbox(recv.participant_id)
    assert inbox == [], "push-mode recipient should leave the buffer empty"
