from datetime import UTC, datetime, timedelta

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.session import Session


@pytest.fixture
def session() -> Session:
    return Session(
        session_id="s1",
        config=SessionConfig(idle_ttl_seconds=60),
        connections=ConnectionManager(),
    )


def test_session_starts_with_no_participants(session: Session) -> None:
    assert session.roster_snapshot() == []


def test_session_expires_at_initial(session: Session) -> None:
    now = datetime.now(UTC)
    assert session.expires_at() >= now + timedelta(seconds=59)


def test_bump_extends_expires_at(session: Session) -> None:
    initial = session.expires_at()
    session.bump_activity(at=initial + timedelta(seconds=30))
    assert session.expires_at() == initial + timedelta(seconds=30 + 60)


def test_is_idle_expired() -> None:
    sess = Session(
        session_id="s",
        config=SessionConfig(idle_ttl_seconds=1),
        connections=ConnectionManager(),
    )
    past = datetime.now(UTC) - timedelta(seconds=10)
    sess._last_activity_at = past
    assert sess.is_idle_expired() is True


@pytest.mark.asyncio
async def test_next_deadline_uses_idle_ttl_when_no_activity(
    session: Session,
) -> None:
    assert session.next_deadline() == session.expires_at()


@pytest.mark.asyncio
async def test_next_deadline_picks_inflight_command_when_sooner() -> None:
    from sessionweave.schemas import DispatchMode, Target
    from tests.unit.fakes import FakeHandle

    sess = Session(
        session_id="s",
        config=SessionConfig(idle_ttl_seconds=3600, max_command_timeout_seconds=600),
        connections=ConnectionManager(),
    )
    sender = await sess.join(FakeHandle(), role="sender", capabilities=[])
    receiver = await sess.join(FakeHandle(), role="recv", capabilities=[])
    await sess.send(
        sender_id=sender.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[receiver.participant_id]),
        payload={"q": "hi"},
        timeout_seconds=30,
    )
    # Session idle TTL is 1h, but the inflight command expires in 30s —
    # next_deadline must pick the sooner one.
    assert sess.next_deadline() < sess.expires_at()


@pytest.mark.asyncio
async def test_next_deadline_picks_offline_grace_when_sooner() -> None:
    from tests.unit.fakes import FakeHandle

    sess = Session(
        session_id="s",
        config=SessionConfig(idle_ttl_seconds=3600, offline_grace_seconds=10),
        connections=ConnectionManager(),
    )
    p = await sess.join(FakeHandle(), role="r", capabilities=[])
    sess._mark_offline(p.participant_id)
    # Idle TTL 1h, grace 10s — offline grace wins.
    assert sess.next_deadline() < sess.expires_at()
