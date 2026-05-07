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
