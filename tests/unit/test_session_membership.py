import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.fixture
def sess() -> Session:
    return Session("s", SessionConfig(max_participants=3), ConnectionManager())


@pytest.mark.asyncio
async def test_join_assigns_participant_id_and_sets_online(sess: Session) -> None:
    h = FakeHandle()
    p = await sess.join(h, role="reviewer", capabilities=["sec"])
    assert p.role == "reviewer"
    assert p.online is True
    # No others yet, so no notification was sent to anyone else
    assert h.sent == []


@pytest.mark.asyncio
async def test_join_notifies_existing_participants(sess: Session) -> None:
    h1, h2 = FakeHandle(), FakeHandle()
    await sess.join(h1, role="r1", capabilities=[])
    await sess.join(h2, role="r2", capabilities=[])
    assert any(name == "participant.joined" for name, _ in h1.sent)


@pytest.mark.asyncio
async def test_join_rejects_when_full() -> None:
    sess = Session("s", SessionConfig(max_participants=2), ConnectionManager())
    await sess.join(FakeHandle(), role=None, capabilities=[])
    await sess.join(FakeHandle(), role=None, capabilities=[])
    with pytest.raises(ToolError) as exc:
        await sess.join(FakeHandle(), role=None, capabilities=[])
    assert exc.value.code is ErrorCode.SESSION_FULL


@pytest.mark.asyncio
async def test_leave_removes_and_notifies(sess: Session) -> None:
    h1, h2 = FakeHandle(), FakeHandle()
    p1 = await sess.join(h1, role=None, capabilities=[])
    p2 = await sess.join(h2, role=None, capabilities=[])
    h2.sent.clear()
    await sess.leave(p1.participant_id)
    pids = {p.participant_id for p in sess.roster_snapshot()}
    assert p1.participant_id not in pids
    assert p2.participant_id in pids
    names = [n for n, _ in h2.sent]
    assert "participant.left" in names


@pytest.mark.asyncio
async def test_update_self_emits_updated(sess: Session) -> None:
    h1, h2 = FakeHandle(), FakeHandle()
    p = await sess.join(h1, role="r1", capabilities=["a"])
    await sess.join(h2, role="r2", capabilities=["b"])
    await sess.update_self(p.participant_id, capabilities=["a", "c"])
    names = [n for n, _ in h2.sent]
    assert "participant.updated" in names


@pytest.mark.asyncio
async def test_list_participants_returns_full_roster(sess: Session) -> None:
    await sess.join(FakeHandle(), role=None, capabilities=[])
    await sess.join(FakeHandle(), role=None, capabilities=[])
    roster = sess.roster_snapshot()
    assert len(roster) == 2
