import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave import tools
from sessionweave.access import hash_token
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import DispatchMode, PersistenceConfig, Target
from sessionweave.supervisor import SupervisorNote
from tests.unit.fakes import FakeHandle

OWNER = "acc_owner"


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _registry(store: PersistenceStore | None) -> SessionRegistry:
    return SessionRegistry(
        defaults=load_deployment_defaults({}), connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )


async def _create(
    reg: SessionRegistry, owner: str | None = OWNER, persist: bool = True
) -> str:
    sid, *_ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=7200) if persist else None,
        creator_token_hash=hash_token("creator"),
        owner_account_id=owner,
    )
    return sid


def _order() -> SupervisorNote:
    return SupervisorNote("sn-x", "note", "all", "hi", 1)


def _notes(h: FakeHandle) -> list[dict]:
    return [p for n, p in h.sent if n == "supervisor.note"]


async def _suspended_with_two(store):
    reg = _registry(store)
    sid = await _create(reg)
    sess = await reg.get_or_raise(sid)
    ha, hb = FakeHandle(), FakeHandle()
    a = await sess.join(ha, role="w", capabilities=[])
    b = await sess.join(hb, role="w", capabilities=[])
    ha.sent.clear()
    hb.sent.clear()
    return reg, sid, sess, (a, ha), (b, hb)


@pytest.mark.asyncio
async def test_suspend_halts_and_refuses(store: PersistenceStore) -> None:
    reg, sid, sess, (a, ha), (b, hb) = await _suspended_with_two(store)
    assert await reg.suspend_owned(OWNER) == 1
    for h in (ha, hb):
        notes = _notes(h)
        assert [(n["kind"], n["message"]) for n in notes] == [
            ("halt", "owner account disabled")
        ]
    assert {p.supervision_state for p in sess.roster_snapshot()} == {"paused"}

    for call in (
        lambda: sess.join(FakeHandle(), role="w", capabilities=[]),
        lambda: sess.send(
            sender_id=a.participant_id, dispatch=DispatchMode.ALL,
            target=Target(), payload={"x": 1},
        ),
        lambda: sess.post_order(_order()),
        lambda: sess.mint_join_token(),
    ):
        with pytest.raises(ToolError) as ei:
            await call()
        assert ei.value.code == ErrorCode.SESSION_SUSPENDED
        assert "owner account is disabled" in ei.value.message

    out = await tools.session_describe(
        tools.ToolDeps(reg), {"session_id": sid, "access_token": "creator"}
    )
    assert out["suspended"] is True
    await sess.leave(b.participant_id)


@pytest.mark.asyncio
async def test_resume_continues(store: PersistenceStore) -> None:
    reg, sid, sess, (_a, ha), (_b, hb) = await _suspended_with_two(store)
    await reg.suspend_owned(OWNER)
    ha.sent.clear()
    hb.sent.clear()
    assert await reg.resume_owned(OWNER) == 1
    for h in (ha, hb):
        assert [(n["kind"], n["message"]) for n in _notes(h)] == [
            ("continue", "owner account re-enabled")
        ]
    assert {p.supervision_state for p in sess.roster_snapshot()} == {"active"}
    await sess.join(FakeHandle(), role="w", capabilities=[])
    assert (await store.load_session(sid)).suspended is False  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_suspend_cold_session_and_restart(store: PersistenceStore) -> None:
    sid = await _create(_registry(store))
    reg2 = _registry(store)
    assert await reg2.suspend_owned(OWNER) == 1
    reg3 = _registry(store)
    sess = await reg3.get_or_raise(sid)
    assert sess.suspended is True
    with pytest.raises(ToolError) as ei:
        await sess.join(FakeHandle(), role="w", capabilities=[])
    assert ei.value.code == ErrorCode.SESSION_SUSPENDED


@pytest.mark.asyncio
async def test_delete_owned_removes_everything(store: PersistenceStore) -> None:
    reg = _registry(store)
    hot = await _create(reg)
    sess = await reg.get_or_raise(hot)
    h = FakeHandle()
    await sess.join(h, role="w", capabilities=[])
    await _create(_registry(store))  # cold: only on disk
    ended = await _create(_registry(store))
    await store.mark_session_ended(ended, "host_deleted", int(time.time() * 1000))
    other = await _create(reg, owner="acc_other")
    sids = await store.find_owned_all(OWNER)
    assert len(sids) == 3

    assert await reg.delete_owned(OWNER) == 3
    ends = [p for n, p in h.sent if n == "session.ended"]
    assert [e["reason"] for e in ends] == ["account_deleted"]
    for sid in sids:
        assert not await store.exists(sid)
    assert await store.exists(other)


@pytest.mark.asyncio
async def test_suspended_idle_session_survives_tick() -> None:
    reg = _registry(None)
    sid = await _create(reg, persist=False)
    sess = await reg.get_or_raise(sid)
    await sess.suspend()
    reg._schedule(sess)
    reg.tick(now=datetime.now(UTC) + timedelta(days=30))
    assert sid in reg._sessions


@pytest.mark.asyncio
async def test_resume_restarts_ttl_and_reschedules() -> None:
    reg = _registry(None)
    sid = await _create(reg, persist=False)
    sess = await reg.get_or_raise(sid)
    await sess.suspend()
    reg._schedule(sess)
    await sess.unsuspend()
    # activity bumped: not idle-expired right now
    assert not sess.is_idle_expired()
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=5))
    assert sid in reg._sessions
    # idle past the TTL after resume: the heap must have been re-pushed
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=sess.config.idle_ttl_seconds + 60))
    assert sid not in reg._sessions


@pytest.mark.asyncio
async def test_resume_persists_activity(store: PersistenceStore) -> None:
    reg = _registry(store)
    sid = await _create(reg)
    sess = await reg.get_or_raise(sid)
    await store.update_last_activity(sid, 1000)
    await sess.suspend()
    await sess.unsuspend()
    snap = await store.load_session(sid)
    assert snap is not None and snap.suspended is False
    assert snap.last_activity_at > int(time.time() * 1000) - 60_000


@pytest.mark.asyncio
async def test_wait_on_suspended_session_past_ttl_blocks() -> None:
    reg = _registry(None)
    sid = await _create(reg, persist=False)
    sess = await reg.get_or_raise(sid)
    p = await sess.join(FakeHandle(), role="w", capabilities=[], delivery_mode="pull")
    await sess.suspend()
    # the halt note is waiting in the inbox: drain it so the wait must block
    first = await sess.wait_for_event(p.participant_id, None, 1)
    assert [m["name"] for m in first.messages] == ["supervisor.note"]
    sess._last_activity_at = datetime.now(UTC) - timedelta(days=2)
    t0 = time.monotonic()
    res = await sess.wait_for_event(p.participant_id, None, 1)
    assert time.monotonic() - t0 >= 0.9
    assert res.timed_out and res.effective_timeout_seconds == 1.0
