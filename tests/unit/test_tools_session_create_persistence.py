from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import hash_token as hash_creator_token
from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.tools import ToolDeps, session_create


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest_asyncio.fixture
async def deps_with_store(store):
    defaults = _defaults()
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )
    return ToolDeps(registry=registry, bounds=defaults.bounds, metrics=None)


@pytest_asyncio.fixture
async def deps_no_store():
    defaults = _defaults()
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(),
        metrics=None, firehose=None, store=None,
    )
    return ToolDeps(registry=registry, bounds=defaults.bounds, metrics=None)


@pytest.mark.asyncio
async def test_create_ephemeral_returns_creator_token(deps_no_store):
    out = await session_create(deps_no_store, {})
    assert out["creator_token"]
    assert 40 <= len(out["creator_token"]) <= 50
    assert out.get("persistence") is None


@pytest.mark.asyncio
async def test_create_persisted_ttl_returns_state(deps_with_store, store):
    out = await session_create(deps_with_store, {
        "persistence": {"mode": "ttl", "ttl_seconds": 7200},
    })
    assert out["creator_token"]
    assert out["persistence"]["mode"] == "ttl"
    assert out["persistence"]["expires_at"] is not None
    assert await store.exists(out["session_id"]) is True
    # The stored hash matches the returned plaintext
    sess = await deps_with_store.registry.get_or_raise(out["session_id"])
    assert sess.auth.creator_token_hash == hash_creator_token(out["creator_token"])


@pytest.mark.asyncio
async def test_create_persisted_until_deleted(deps_with_store):
    out = await session_create(deps_with_store, {
        "persistence": {"mode": "until_deleted"},
    })
    assert out["persistence"]["mode"] == "until_deleted"
    assert out["persistence"]["expires_at"] is None


@pytest.mark.asyncio
async def test_create_ttl_below_min_raises(deps_with_store):
    with pytest.raises(ToolError) as exc:
        await session_create(deps_with_store, {
            "persistence": {"mode": "ttl", "ttl_seconds": 60},  # min is 3600
        })
    assert exc.value.code is ErrorCode.INVALID_CONFIG


@pytest.mark.asyncio
async def test_create_persistence_without_store_raises(deps_no_store):
    with pytest.raises(ToolError) as exc:
        await session_create(deps_no_store, {
            "persistence": {"mode": "ttl", "ttl_seconds": 3600},
        })
    assert exc.value.code is ErrorCode.PERSISTENCE_FAILED


@pytest.mark.asyncio
async def test_persisted_create_hands_back_all_credentials(deps_with_store):
    from sessionweave.access import hash_token

    out = await session_create(
        deps_with_store,
        {"persistence": {"mode": "until_deleted"}},
        public_base_url="https://r.example",
    )
    assert out["invite_link"] == f"swvs://r.example/{out['session_id']}?jt={out['join_token']}"
    assert out["invite_url"] == f"https://r.example/invite/{out['session_id']}?jt={out['join_token']}"
    assert out["supervisor_token_id"] and out["join_token_expires_at"]
    auth = await deps_with_store.registry._store.load_auth(out["session_id"])
    assert auth.match(out["supervisor_token"], 0) is not None
    assert auth.creator_token_hash == hash_token(out["creator_token"])
