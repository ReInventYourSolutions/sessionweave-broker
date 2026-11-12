"""Property test: event rows in the DB are strictly monotonic in seq — no gaps,
no duplicates."""
from __future__ import annotations

import time
from pathlib import Path

import pytest
import pytest_asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from sessionweave.access import generate_token as generate_creator_token
from sessionweave.access import hash_token as hash_creator_token
from sessionweave.persistence import PersistenceStore, SessionMetadata


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


async def _seed_session(store: PersistenceStore, sid: str) -> None:
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id=sid, config_json="{}", purpose_json=None,
        creator_token_hash=th, persistence_mode="ttl",
        persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))


@pytest.mark.asyncio
async def test_events_have_monotonic_seq(store: PersistenceStore) -> None:
    """50 sequential events produce strictly increasing seq with no gaps."""
    await _seed_session(store, "m1")
    now = _now_ms()
    for i in range(1, 51):
        await store.persist_event_atomic(
            sid="m1", seq=i, kind="x", payload={"i": i}, ts=now + i,
        )
    page = await store.fetch_events(sid="m1", limit=1000)
    seqs = [e["seq"] for e in page.events]
    assert seqs == list(range(1, 51))


@pytest.mark.asyncio
async def test_events_no_duplicates_on_unique_violation(store: PersistenceStore) -> None:
    """Inserting a duplicate seq raises; the DB still has exactly one row."""
    import aiosqlite
    await _seed_session(store, "dup")
    now = _now_ms()
    await store.persist_event_atomic(
        sid="dup", seq=1, kind="x", payload={"v": "first"}, ts=now,
    )
    with pytest.raises((aiosqlite.IntegrityError, Exception)):
        await store.persist_event_atomic(
            sid="dup", seq=1, kind="x", payload={"v": "second"}, ts=now + 1,
        )
    page = await store.fetch_events(sid="dup", limit=10)
    assert len(page.events) == 1
    assert page.events[0]["seq"] == 1


@pytest.mark.asyncio
async def test_events_mixed_kinds_remain_monotonic(store: PersistenceStore) -> None:
    """Events of different kinds still arrive ordered by seq."""
    await _seed_session(store, "kinds")
    now = _now_ms()
    kinds = ["command.received", "result.received", "supervisor.note",
             "participant.joined", "participant.left"]
    for i, kind in enumerate(kinds, start=1):
        await store.persist_event_atomic(
            sid="kinds", seq=i, kind=kind, payload={"seq": i}, ts=now + i,
        )
    page = await store.fetch_events(sid="kinds", limit=100)
    seqs = [e["seq"] for e in page.events]
    assert seqs == list(range(1, len(kinds) + 1))


@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(n=st.integers(min_value=1, max_value=20))
@pytest.mark.asyncio
async def test_seq_monotonic_hypothesis(tmp_path: Path, n: int) -> None:
    """Hypothesis: for any n, n events yield seqs [1..n] with no gaps."""
    store = PersistenceStore(str(tmp_path / f"mono_{n}.db"))
    await store.init(pool_size=2)
    try:
        sid = "h"
        th = hash_creator_token(generate_creator_token())
        now = _now_ms()
        await store.save_session_metadata(SessionMetadata(
            session_id=sid, config_json="{}", purpose_json=None,
            creator_token_hash=th, persistence_mode="ttl",
            persistence_ttl_seconds=3600,
            created_at=now, last_activity_at=now, relay_epoch_at_create=1,
        ))
        for i in range(1, n + 1):
            await store.persist_event_atomic(
                sid=sid, seq=i, kind="x", payload={"i": i}, ts=now + i,
            )
        page = await store.fetch_events(sid=sid, limit=n + 10)
        seqs = [e["seq"] for e in page.events]
        assert seqs == list(range(1, n + 1))
        # strict monotone: adjacent differences are all 1
        diffs = [seqs[k + 1] - seqs[k] for k in range(len(seqs) - 1)]
        assert all(d == 1 for d in diffs)
    finally:
        await store.close()
