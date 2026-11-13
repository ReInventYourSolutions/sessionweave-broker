from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import AuthFailureLimiter
from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.tools import ToolDeps, session_create, session_delete


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest_asyncio.fixture
async def deps(store):
    defaults = _defaults()
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )
    rl = AuthFailureLimiter(max_failures=5, window_seconds=60.0)
    return ToolDeps(
        registry=registry, bounds=defaults.bounds, metrics=None,
        auth_limiter=rl,
    )


@pytest.mark.asyncio
async def test_delete_succeeds_with_correct_token(deps, store):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    out = await session_delete(deps, {
        "session_id": created["session_id"],
        "creator_token": created["creator_token"],
    })
    assert out["deleted_at"]
    assert await store.exists(created["session_id"]) is False


@pytest.mark.asyncio
async def test_delete_with_wrong_token_raises(deps):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    with pytest.raises(ToolError) as exc:
        await session_delete(deps, {
            "session_id": created["session_id"],
            "creator_token": "wrong-token",
        })
    assert exc.value.code is ErrorCode.PERMISSION_DENIED


@pytest.mark.asyncio
async def test_delete_unknown_session_raises_not_found(deps):
    with pytest.raises(ToolError) as exc:
        await session_delete(deps, {
            "session_id": "does_not_exist",
            "creator_token": "anything",
        })
    assert exc.value.code is ErrorCode.SESSION_NOT_FOUND


@pytest.mark.asyncio
async def test_delete_throttles_bad_tokens_but_never_a_valid_one(deps):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    sid = created["session_id"]
    good = created["creator_token"]
    for _ in range(5):
        with pytest.raises(ToolError):
            await session_delete(deps, {"session_id": sid, "creator_token": "wrong"})
    # 6th bad attempt is throttled
    with pytest.raises(ToolError) as exc:
        await session_delete(deps, {"session_id": sid, "creator_token": "wrong"})
    assert exc.value.code is ErrorCode.RATE_LIMITED
    # the valid token still deletes
    out = await session_delete(deps, {"session_id": sid, "creator_token": good})
    assert out["deleted_at"]


@pytest.mark.asyncio
async def test_delete_with_session_id_as_token_denied(deps):
    created = await session_create(deps, {})
    sid = created["session_id"]
    with pytest.raises(ToolError) as exc:
        await session_delete(deps, {"session_id": sid, "creator_token": sid})
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
