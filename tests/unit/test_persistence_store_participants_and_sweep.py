from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import generate_token as generate_creator_token
from sessionweave.access import hash_token as hash_creator_token
from sessionweave.persistence import (
    PersistenceStore,
    SessionMetadata,
    SessionSnapshot,
)


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


async def _seed(store: PersistenceStore, sid: str, mode: str = "ttl",
                ttl: int | None = 3600, last_activity_at: int | None = None) -> bytes:
    th = hash_creator_token(generate_creator_token())
    now = _now_ms() if last_activity_at is None else last_activity_at
    await store.save_session_metadata(SessionMetadata(
        session_id=sid, config_json="{}", purpose_json=None,
        creator_token_hash=th, persistence_mode=mode,
        persistence_ttl_seconds=ttl,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    return th


@pytest.mark.asyncio
async def test_upsert_and_soft_delete_participant(store: PersistenceStore) -> None:
    await _seed(store, "sid")
    snap = {"role": "agent", "objective": "solve", "capabilities": [], "metadata": {},
            "state": "online", "delivery_mode": "push"}
    await store.upsert_participant("sid", "p1", snap, last_seen_seq=0, joined_at=_now_ms())
    async with store._read_pool[0].execute(
        "SELECT participant_id, left_at FROM participants WHERE session_id='sid'"
    ) as cur:
        rows = list(await cur.fetchall())
    assert rows == [("p1", None)]

    await store.soft_delete_participant("sid", "p1", left_at=_now_ms())
    async with store._read_pool[0].execute(
        "SELECT left_at FROM participants WHERE session_id='sid' AND participant_id='p1'"
    ) as cur:
        _la_row = await cur.fetchone()
    assert _la_row is not None
    (la,) = _la_row
    assert la is not None


@pytest.mark.asyncio
async def test_ack_participant_updates_last_seen_seq(store: PersistenceStore) -> None:
    await _seed(store, "sid")
    await store.upsert_participant("sid", "p1", {"role": "x"}, last_seen_seq=0,
                                    joined_at=_now_ms())
    await store.ack_participant("sid", "p1", last_seen_seq=42)
    async with store._read_pool[0].execute(
        "SELECT last_seen_seq FROM participants WHERE session_id='sid' AND participant_id='p1'"
    ) as cur:
        _s_row = await cur.fetchone()
    assert _s_row is not None
    (s,) = _s_row
    assert s == 42


@pytest.mark.asyncio
async def test_remove_buffered_through(store: PersistenceStore) -> None:
    await _seed(store, "sid")
    await store.upsert_participant("sid", "p1", {"role": "x"}, last_seen_seq=0,
                                    joined_at=_now_ms())
    for seq in range(1, 11):
        await store.persist_event_atomic(
            sid="sid", seq=seq, kind="result.received",
            payload={}, ts=_now_ms(), buffered_pids=("p1",),
        )
    deleted = await store.remove_buffered_through("sid", "p1", max_seq_inclusive=5)
    assert deleted == 5
    async with store._read_pool[0].execute(
        "SELECT seq FROM buffered_notifications"
        " WHERE session_id='sid' AND participant_id='p1' ORDER BY seq"
    ) as cur:
        rows = [r[0] for r in await cur.fetchall()]
    assert rows == [6, 7, 8, 9, 10]


@pytest.mark.asyncio
async def test_load_session_round_trip(store: PersistenceStore) -> None:
    await _seed(store, "sid")
    await store.upsert_participant("sid", "p1",
                                    {"role": "agent", "delivery_mode": "push"},
                                    last_seen_seq=3, joined_at=_now_ms())
    await store.upsert_participant("sid", "p_gone",
                                    {"role": "ex"}, last_seen_seq=0,
                                    joined_at=_now_ms())
    await store.soft_delete_participant("sid", "p_gone", left_at=_now_ms())
    await store.persist_event_atomic(
        sid="sid", seq=1, kind="command.received",
        payload={"command_id": "c1", "text": "go"}, ts=_now_ms(),
        inflight_upsert=("c1", {"sender_pid": "p1", "deadline_at": 9_999_999}),
        buffered_pids=("p1",),
    )
    snap = await store.load_session("sid")
    assert isinstance(snap, SessionSnapshot)
    assert snap.session_id == "sid"
    assert snap.persistence_mode == "ttl"
    assert snap.ended_at is None
    assert {p["participant_id"] for p in snap.participants} == {"p1"}
    assert snap.inflight == {"c1": {"sender_pid": "p1", "deadline_at": 9_999_999}}
    assert snap.buffered == [{"participant_id": "p1", "seq": 1,
                               "kind": "command.received",
                               "payload": {"command_id": "c1", "text": "go"}}]


@pytest.mark.asyncio
async def test_load_session_unknown_returns_none(store: PersistenceStore) -> None:
    assert await store.load_session("does_not_exist") is None


@pytest.mark.asyncio
async def test_find_expired_ttl_mode(store: PersistenceStore) -> None:
    now = _now_ms()
    await _seed(store, "old", mode="ttl", ttl=1, last_activity_at=now - 2000)
    await _seed(store, "fresh", mode="ttl", ttl=3600, last_activity_at=now - 2000)
    expired = await store.find_expired_sessions(now_ms=now, safety_seconds=86400)
    assert "old" in expired
    assert "fresh" not in expired


@pytest.mark.asyncio
async def test_find_expired_until_deleted_safety(store: PersistenceStore) -> None:
    now = _now_ms()
    await _seed(store, "u_old", mode="until_deleted", ttl=None,
                last_activity_at=now - 100 * 86400 * 1000)
    await _seed(store, "u_fresh", mode="until_deleted", ttl=None,
                last_activity_at=now - 3600 * 1000)
    expired = await store.find_expired_sessions(now_ms=now, safety_seconds=90 * 86400)
    assert "u_old" in expired
    assert "u_fresh" not in expired


@pytest.mark.asyncio
async def test_find_stranded_skips_recent(store: PersistenceStore) -> None:
    now = _now_ms()
    await _seed(store, "recent", last_activity_at=now - 3600 * 1000)
    await _seed(store, "ancient", last_activity_at=now - 30 * 86400 * 1000)
    stranded = await store.find_stranded(now_ms=now, idle_threshold_hours=24)
    assert "ancient" in stranded
    assert "recent" not in stranded
