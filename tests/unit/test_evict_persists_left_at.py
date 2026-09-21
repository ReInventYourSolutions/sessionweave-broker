"""Eviction must be durable on a persisted session.

``leave()`` records ``left_at`` through ``soft_delete_participant``, but
``_evict`` — the offline-grace path, and every other forced removal —
did not touch the store at all. On a persisted session that makes the
eviction **temporary**: hydration restores participants whose ``left_at``
is NULL (``persistence.py:379``), so a participant the relay evicted
comes back from disk on the next cold -> hot cycle.

Reproduced end to end against a live relay: an MCP participant whose
connection closed went offline, was evicted at the grace deadline, its
row still read ``left_at=NULL``, and after the session went cold and was
rehydrated it was in the roster again — offline, and due to be evicted a
second time.

Nothing errors when this happens. The roster is simply wrong, and peers
that acted on ``participant.left`` disagree with the relay.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import PersistenceStore, SessionMetadata
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


async def _session_with_participant(store: PersistenceStore):
    sess = Session("s-evict", SessionConfig(), ConnectionManager(),
                   store=store, persistence_mode="ttl")
    await store.save_session_metadata(SessionMetadata(
        session_id="s-evict", config_json="{}", purpose_json=None,
        creator_token_hash=b"x" * 32, persistence_mode="ttl",
        persistence_ttl_seconds=3600, created_at=0, last_activity_at=0,
        relay_epoch_at_create=0,
    ))
    handle = FakeHandle()
    p = await sess.join(handle, role="ghost", capabilities=["chat"])
    return sess, p.participant_id


async def _settle() -> None:
    """_evict is synchronous and schedules its store write as a task."""
    for _ in range(10):
        await asyncio.sleep(0.02)


async def _left_at(store: PersistenceStore, pid: str) -> int | None:
    snap = await store.load_session("s-evict")
    for row in (snap.participants if snap else []):
        if row["participant_id"] == pid:
            return row.get("left_at")
    return None          # absent from the active set = recorded as left


@pytest.mark.asyncio
async def test_eviction_records_left_at(store: PersistenceStore) -> None:
    """The crux: an evicted participant must not be restorable."""
    sess, pid = await _session_with_participant(store)

    sess._evict(pid, reason="offline_timeout")
    await _settle()

    assert await _left_at(store, pid) is None, (
        "the evicted participant is still an active row, so hydration will "
        "bring it back — the eviction does not survive a cold/hot cycle"
    )


@pytest.mark.asyncio
async def test_evicted_participant_is_not_restored_by_hydration(
    store: PersistenceStore,
) -> None:
    """The observable consequence, stated as the behaviour we want."""
    sess, pid = await _session_with_participant(store)
    sess._evict(pid, reason="offline_timeout")
    await _settle()

    snap = await store.load_session("s-evict")
    restored = [r["participant_id"] for r in (snap.participants if snap else [])]
    assert pid not in restored, f"hydration would resurrect {pid[:8]}: {restored}"


@pytest.mark.asyncio
async def test_leave_still_records_left_at(store: PersistenceStore) -> None:
    """Regression guard on the path that already worked."""
    sess, pid = await _session_with_participant(store)
    await sess.leave(pid)
    await _settle()

    snap = await store.load_session("s-evict")
    assert pid not in [r["participant_id"] for r in (snap.participants if snap else [])]
