import time
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import IssuedToken, Principal, hash_token
from sessionweave.access import generate_token as generate_creator_token
from sessionweave.access import hash_token as hash_creator_token
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig
from tests.unit.fakes import FakeHandle


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


def _now() -> int:
    return int(time.time() * 1000)


async def _create(reg: SessionRegistry, creator: str):
    sid, *_ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=7200),
        creator_token_hash=hash_creator_token(creator),
    )
    return sid, await reg.get_or_raise(sid)


@pytest.mark.asyncio
async def test_tokens_survive_restart(store: PersistenceStore) -> None:
    reg = _registry(store)
    creator = generate_creator_token()
    sid, sess = await _create(reg, creator)
    jt, _ = await sess.mint_join_token()
    st, _ = await sess.mint_supervisor_token()
    revoked, rev_issued = await sess.mint_supervisor_token()
    await sess.revoke_supervisor_token(rev_issued.token_id)
    p = await sess.join(FakeHandle(), role="a")
    ptoken = p.issued_token
    assert ptoken is not None

    # Simulate restart: fresh registry, session rebuilt from disk.
    reg2 = _registry(store)
    hydrated = await reg2.get_or_raise(sid)
    assert hydrated is not sess
    now = _now()
    assert hydrated.auth.match(creator, now) == Principal("creator")
    assert hydrated.auth.match(ptoken, now) == Principal("participant", p.participant_id)
    assert hydrated.auth.match(st, now) == Principal("supervisor")
    assert hydrated.auth.match(jt, now) == Principal("joiner")
    assert hydrated.auth.match(revoked, now) is None
    # Revocation is durable and visible on the issued record too.
    sup = {t.token_id: t for t in hydrated.auth.supervisor_tokens()}
    assert sup[rev_issued.token_id].revoked_at_ms is not None


@pytest.mark.asyncio
async def test_load_auth_for_cold_session(store: PersistenceStore) -> None:
    reg = _registry(store)
    creator = generate_creator_token()
    sid, sess = await _create(reg, creator)
    st, _ = await sess.mint_supervisor_token()
    p = await sess.join(FakeHandle(), role="a")
    assert p.issued_token is not None
    auth = await store.load_auth(sid)
    assert auth is not None
    now = _now()
    assert auth.match(creator, now) == Principal("creator")
    assert auth.match(st, now) == Principal("supervisor")
    assert auth.match(p.issued_token, now) == Principal("participant", p.participant_id)
    assert await store.load_auth("missing") is None


@pytest.mark.asyncio
async def test_expired_join_tokens_pruned_on_load(store: PersistenceStore) -> None:
    reg = _registry(store)
    sid, sess = await _create(reg, generate_creator_token())
    await store.save_token(sid, IssuedToken(
        token_id="old", kind="join", token_hash=hash_token("x"),
        created_at_ms=1, expires_at_ms=2,
    ))
    jt, live = await sess.mint_join_token()
    ids = {t.token_id for t in await store.load_tokens(sid)}
    assert ids == {live.token_id}
    assert jt


@pytest.mark.asyncio
async def test_delete_session_removes_tokens(store: PersistenceStore) -> None:
    reg = _registry(store)
    sid, sess = await _create(reg, generate_creator_token())
    await sess.mint_join_token()
    await sess.mint_supervisor_token()
    assert len(await store.load_tokens(sid)) == 2
    await reg.delete(sid, reason="deleted")
    assert await store.load_tokens(sid) == []
