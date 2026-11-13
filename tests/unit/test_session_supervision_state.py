import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_new_participants_are_active() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    p = await s.join(FakeHandle(), role="r", capabilities=[])
    assert p.supervision_state == "active"
    assert s._notes == {}


@pytest.mark.asyncio
async def test_every_roster_entry_has_supervision_state() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    await s.join(FakeHandle(), role="a", capabilities=[])
    await s.join(FakeHandle(), role="b", capabilities=[])
    s._participants[next(iter(s._participants))].supervision_state = "paused"
    states = sorted(e.supervision_state for e in s.roster_snapshot())
    assert states == ["active", "paused"]
    assert all("supervision_state" in e.model_dump() for e in s.roster_snapshot())
