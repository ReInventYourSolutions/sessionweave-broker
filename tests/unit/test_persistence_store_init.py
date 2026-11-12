from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.persistence import PersistenceStore, SessionMetadata
from sessionweave.persistence_token import generate_creator_token, hash_creator_token


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest.mark.asyncio
async def test_init_creates_schema(store: PersistenceStore) -> None:
    async with store._read_pool[0].execute("SELECT count(*) FROM sessions") as cur:
        row = await cur.fetchone()
        assert row == (0,)


@pytest.mark.asyncio
async def test_init_records_migration_version(store: PersistenceStore) -> None:
    async with store._read_pool[0].execute("SELECT max(version) FROM _migrations") as cur:
        _v_row = await cur.fetchone()
    assert _v_row is not None
    (v,) = _v_row
    assert v >= 1


@pytest.mark.asyncio
async def test_init_idempotent(tmp_path: Path) -> None:
    p = str(tmp_path / "test.db")
    s1 = PersistenceStore(p)
    await s1.init(pool_size=1)
    await s1.close()
    s2 = PersistenceStore(p)
    await s2.init(pool_size=1)
    async with s2._read_pool[0].execute("SELECT count(*) FROM _migrations") as cur:
        _n_row = await cur.fetchone()
    assert _n_row is not None
    (n,) = _n_row
    assert n == 2
    await s2.close()


@pytest.mark.asyncio
async def test_init_wal_mode(store: PersistenceStore) -> None:
    async with store._read_pool[0].execute("PRAGMA journal_mode") as cur:
        _mode_row = await cur.fetchone()
    assert _mode_row is not None
    (mode,) = _mode_row
    assert mode == "wal"


@pytest.mark.asyncio
async def test_save_session_metadata_roundtrip(store: PersistenceStore) -> None:
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id="s_1", config_json='{"idle_ttl_seconds": 1800}', purpose_json=None,
        creator_token_hash=th, persistence_mode="ttl", persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=42,
    ))
    assert await store.exists("s_1") is True
    assert await store.exists("s_2") is False


@pytest.mark.asyncio
async def test_save_session_metadata_until_deleted(store: PersistenceStore) -> None:
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id="s_x", config_json="{}", purpose_json='{"text": "hi"}',
        creator_token_hash=th, persistence_mode="until_deleted",
        persistence_ttl_seconds=None,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    assert await store.exists("s_x") is True


@pytest.mark.asyncio
async def test_mark_session_ended(store: PersistenceStore) -> None:
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id="s_e", config_json="{}", purpose_json=None,
        creator_token_hash=hash_creator_token(generate_creator_token()),
        persistence_mode="ttl", persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    await store.mark_session_ended("s_e", reason="host_deleted", ended_at=now + 100)
    async with store._read_pool[0].execute(
        "SELECT ended_at, ended_reason FROM sessions WHERE session_id = ?", ("s_e",),
    ) as cur:
        row = await cur.fetchone()
    assert row == (now + 100, "host_deleted")


@pytest.mark.asyncio
async def test_delete_session(store: PersistenceStore) -> None:
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id="s_d", config_json="{}", purpose_json=None,
        creator_token_hash=hash_creator_token(generate_creator_token()),
        persistence_mode="until_deleted", persistence_ttl_seconds=None,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    assert await store.exists("s_d") is True
    await store.delete_session("s_d")
    assert await store.exists("s_d") is False
