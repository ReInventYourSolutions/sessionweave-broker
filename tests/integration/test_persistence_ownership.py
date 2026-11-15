import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import hash_token
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig
from sessionweave.session import Session


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _registry(store: PersistenceStore) -> SessionRegistry:
    return SessionRegistry(
        defaults=load_deployment_defaults({}), connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )


async def _create(reg: SessionRegistry, owner: str | None = None) -> str:
    sid, *_ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=7200),
        creator_token_hash=hash_token("creator"),
        owner_account_id=owner,
    )
    return sid


@pytest.mark.asyncio
async def test_owner_persisted_and_restored(store: PersistenceStore) -> None:
    reg = _registry(store)
    sid = await _create(reg, "acc_x")
    other = await _create(reg, "acc_y")
    hydrated = await _registry(store).get_or_raise(sid)
    assert hydrated.auth.owner_account_id == "acc_x"
    auth = await store.load_auth(sid)
    assert auth is not None and auth.owner_account_id == "acc_x"
    assert await store.find_owned("acc_x") == [sid]
    assert await store.find_owned_all("acc_y") == [other]


@pytest.mark.asyncio
async def test_suspended_flag_restored(store: PersistenceStore) -> None:
    sid = await _create(_registry(store))
    assert (await _registry(store).get_or_raise(sid)).suspended is False
    await store.set_suspended(sid, True)
    assert (await _registry(store).get_or_raise(sid)).suspended is True


@pytest.mark.asyncio
async def test_suspended_not_reaped(store: PersistenceStore) -> None:
    sid = await _create(_registry(store))
    now = int(time.time() * 1000)
    await store.update_last_activity(sid, now - 10 * 24 * 3600 * 1000)
    later = now + 365 * 24 * 3600 * 1000
    assert sid in await store.find_expired_sessions(now_ms=later, safety_seconds=60)
    assert sid in await store.find_stranded(now_ms=later)
    await store.set_suspended(sid, True)
    assert sid not in await store.find_expired_sessions(now_ms=later, safety_seconds=60)
    assert sid not in await store.find_stranded(now_ms=later)


def test_ram_session_not_idle_expired_while_suspended() -> None:
    cfg = load_deployment_defaults({}).defaults
    sess = Session("s1", cfg, ConnectionManager())
    far = datetime.now(UTC) + timedelta(days=365)
    assert sess.is_idle_expired(at=far) is True
    sess.suspended = True
    assert sess.is_idle_expired(at=far) is False
