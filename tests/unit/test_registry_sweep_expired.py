from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import PersistenceStore
from sessionweave.persistence_token import generate_creator_token, hash_creator_token
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest_asyncio.fixture
async def registry(store):
    metrics = MagicMock()
    metrics.persistence_failures_total = MagicMock()
    metrics.persistence_failures_total.labels = MagicMock(return_value=MagicMock(inc=MagicMock()))
    metrics.session_purge_total = MagicMock()
    metrics.session_purge_total.labels = MagicMock(return_value=MagicMock(inc=MagicMock()))
    metrics.sessions_active = MagicMock()
    metrics.session_hydrate_total = MagicMock()
    metrics.session_hydrate_seconds = MagicMock()
    metrics.session_cold_total = MagicMock()
    metrics.session_persisted_total = MagicMock()
    metrics.session_persisted_total.labels = MagicMock(return_value=MagicMock(inc=MagicMock()))
    r = SessionRegistry(
        defaults=_defaults(),
        connections=ConnectionManager(),
        metrics=metrics,
        firehose=None,
        store=store,
    )
    return r


@pytest.mark.asyncio
async def test_sweep_purges_ttl_expired(registry, store):
    th = hash_creator_token(generate_creator_token())
    sid, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=th,
    )
    # Force last_activity_at into the past, well beyond the 1-hour TTL
    await store.update_last_activity(sid, _now_ms() - 7200 * 1000)
    purged = await registry.sweep_expired_once(
        now_ms=_now_ms(), safety_seconds=90 * 86400,
    )
    assert purged == 1
    assert await store.exists(sid) is False
    assert sid not in registry._sessions


@pytest.mark.asyncio
async def test_sweep_keeps_fresh_ttl(registry, store):
    th = hash_creator_token(generate_creator_token())
    sid, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=th,
    )
    purged = await registry.sweep_expired_once(
        now_ms=_now_ms(), safety_seconds=90 * 86400,
    )
    assert purged == 0
    assert await store.exists(sid) is True


@pytest.mark.asyncio
async def test_sweep_purges_until_deleted_past_safety(registry, store):
    th = hash_creator_token(generate_creator_token())
    sid, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="until_deleted"),
        creator_token_hash=th,
    )
    # last_activity_at 100 days ago
    await store.update_last_activity(sid, _now_ms() - 100 * 86400 * 1000)
    purged = await registry.sweep_expired_once(
        now_ms=_now_ms(), safety_seconds=90 * 86400,
    )
    assert purged == 1


@pytest.mark.asyncio
async def test_sweep_continues_after_one_failure(registry, store, monkeypatch):
    th = hash_creator_token(generate_creator_token())
    sid_a, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=th,
    )
    sid_b, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=hash_creator_token(generate_creator_token()),
    )
    await store.update_last_activity(sid_a, _now_ms() - 7200 * 1000)
    await store.update_last_activity(sid_b, _now_ms() - 7200 * 1000)

    # Patch delete_session to raise once for sid_a, work for sid_b
    real_delete = store.delete_session
    calls = []
    async def flaky(sid):
        calls.append(sid)
        if sid == sid_a:
            raise RuntimeError("boom")
        await real_delete(sid)
    monkeypatch.setattr(store, "delete_session", flaky)

    purged = await registry.sweep_expired_once(
        now_ms=_now_ms(), safety_seconds=90 * 86400,
    )
    # Either order; at least one purge succeeded; flaky one counted as failure
    assert purged >= 1
    assert sid_a in calls and sid_b in calls
