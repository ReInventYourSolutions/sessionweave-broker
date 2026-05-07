import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_disconnect_marks_offline_and_notifies_others() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    await s.join(h_a, role=None, capabilities=[])
    await s.join(h_b, role=None, capabilities=[])
    h_b.sent.clear()
    s.handle_disconnect(h_a)
    assert any(
        n == "participant.updated" and p["participant"]["online"] is False
        for n, p in h_b.sent
    )


@pytest.mark.asyncio
async def test_resume_emits_updated_online_true_when_was_offline() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    await s.join(h_b, role=None, capabilities=[])
    s.handle_disconnect(h_a)
    h_b.sent.clear()
    new_handle = FakeHandle()
    await s.resume(new_handle, a.participant_id)
    assert any(
        n == "participant.updated" and p["participant"]["online"] is True
        for n, p in h_b.sent
    )


@pytest.mark.asyncio
async def test_buffered_command_received_flushed_on_resume() -> None:
    s = Session(
        "s",
        SessionConfig(max_pending_notifications_per_participant=10),
        ConnectionManager(),
    )
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role="sender", capabilities=[])
    b = await s.join(h_b, role="reviewer", capabilities=["x"])
    s.handle_disconnect(h_b)  # b goes offline
    await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={"q": 1},
    )
    new_b = FakeHandle()
    await s.resume(new_b, b.participant_id)
    names = [n for n, _ in new_b.sent]
    assert "command.received" in names


@pytest.mark.asyncio
async def test_resume_with_unknown_participant_raises() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    with pytest.raises(ToolError) as exc:
        await s.resume(FakeHandle(), "ghost")
    assert exc.value.code is ErrorCode.PARTICIPANT_NOT_FOUND


@pytest.mark.asyncio
async def test_resume_does_not_emit_when_already_online() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    await s.join(h_b, role=None, capabilities=[])
    h_b.sent.clear()
    new_h = FakeHandle()
    await s.resume(new_h, a.participant_id)  # a was online; rebind
    assert not any(n == "participant.updated" for n, _ in h_b.sent)
