import asyncio
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.persistence_token import generate_creator_token, hash_creator_token
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


@pytest_asyncio.fixture
async def registry(store):
    return SessionRegistry(
        defaults=_defaults(),
        connections=ConnectionManager(),
        metrics=None,
        firehose=None,
        store=store,
    )


@pytest.mark.asyncio
async def test_create_without_persistence_does_not_save_metadata(
    registry: SessionRegistry, store: PersistenceStore,
) -> None:
    token_hash = hash_creator_token(generate_creator_token())
    sid, _, _, _, persistence_state = await registry.create(
        overrides={}, purpose=None,
        persistence=None, creator_token_hash=token_hash,
    )
    assert persistence_state is None
    # No row in DB
    assert await store.exists(sid) is False


@pytest.mark.asyncio
async def test_create_with_persistence_saves_metadata(
    registry: SessionRegistry, store: PersistenceStore,
) -> None:
    token_hash = hash_creator_token(generate_creator_token())
    sid, _, _, _, persistence_state = await registry.create(
        overrides={}, purpose={"text": "x"},
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=7200),
        creator_token_hash=token_hash,
    )
    assert persistence_state is not None
    assert persistence_state.mode == "ttl"
    assert await store.exists(sid) is True


@pytest.mark.asyncio
async def test_get_or_raise_in_ram(registry: SessionRegistry) -> None:
    token_hash = hash_creator_token(generate_creator_token())
    sid, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=None, creator_token_hash=token_hash,
    )
    sess = await registry.get_or_raise(sid)
    assert sess.session_id == sid


@pytest.mark.asyncio
async def test_get_or_raise_unknown_raises_not_found(
    registry: SessionRegistry,
) -> None:
    with pytest.raises(ToolError) as exc:
        await registry.get_or_raise("does_not_exist")
    assert exc.value.code is ErrorCode.SESSION_NOT_FOUND


@pytest.mark.asyncio
async def test_get_or_raise_hydrates_cold(
    registry: SessionRegistry, store: PersistenceStore,
) -> None:
    token_hash = hash_creator_token(generate_creator_token())
    sid, _, _, _, _ = await registry.create(
        overrides={}, purpose={"text": "hi"},
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=token_hash,
    )
    # Evict from RAM to simulate cold
    registry._sessions.pop(sid, None)
    # First call must hydrate
    sess = await registry.get_or_raise(sid)
    assert sess.session_id == sid
    assert sess.purpose == {"text": "hi"}
    assert sid in registry._sessions  # back in RAM


@pytest.mark.asyncio
async def test_concurrent_hydrate_dedup(
    registry: SessionRegistry, store: PersistenceStore,
) -> None:
    token_hash = hash_creator_token(generate_creator_token())
    sid, _, _, _, _ = await registry.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=token_hash,
    )
    registry._sessions.pop(sid, None)
    # 20 concurrent calls
    results = await asyncio.gather(
        *[registry.get_or_raise(sid) for _ in range(20)]
    )
    # All same instance
    assert all(s is results[0] for s in results)
