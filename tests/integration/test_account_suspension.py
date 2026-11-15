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
from sessionweave.session import Session
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


async def _two_participants(store):
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
    reg, sid, sess, (a, ha), (b, hb) = await _two_participants(store)
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
    reg, sid, sess, (_a, ha), (_b, hb) = await _two_participants(store)
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
    ttl = sess.config.idle_ttl_seconds
    # consume the creation-time heap entry while suspended: it is re-pushed
    # at the "no deadline" sentinel
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=ttl + 60))
    assert sid in reg._sessions
    await sess.unsuspend()
    assert not sess.is_idle_expired()
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=5))
    assert sid in reg._sessions
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=ttl + 60))
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


class _FlakyStore:
    """Delegates to a real store; set_suspended can be told to fail once."""

    def __init__(self, inner: PersistenceStore) -> None:
        self._inner = inner
        self.fail_set_suspended = 0
        self.fail_sids: set[str] = set()

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    async def set_suspended(self, sid: str, suspended: bool) -> None:
        if self.fail_set_suspended or sid in self.fail_sids:
            self.fail_set_suspended = max(0, self.fail_set_suspended - 1)
            raise RuntimeError("disk full")
        await self._inner.set_suspended(sid, suspended)


@pytest.mark.asyncio
async def test_failed_suspend_write_leaves_session_unchanged(
    store: PersistenceStore,
) -> None:
    flaky = _FlakyStore(store)
    reg = _registry(flaky)  # type: ignore[arg-type]
    sid = await _create(reg)
    sess = await reg.get_or_raise(sid)
    h = FakeHandle()
    await sess.join(h, role="w", capabilities=[])
    h.sent.clear()
    flaky.fail_set_suspended = 1
    with pytest.raises(RuntimeError):
        await sess.suspend()
    assert sess.suspended is False and _notes(h) == []
    await sess.suspend()
    assert sess.suspended is True
    assert [n["kind"] for n in _notes(h)] == ["halt"]


@pytest.mark.asyncio
async def test_failed_unsuspend_write_leaves_session_suspended(
    store: PersistenceStore,
) -> None:
    flaky = _FlakyStore(store)
    reg = _registry(flaky)  # type: ignore[arg-type]
    sid = await _create(reg)
    sess = await reg.get_or_raise(sid)
    h = FakeHandle()
    await sess.join(h, role="w", capabilities=[])
    await sess.suspend()
    h.sent.clear()
    flaky.fail_set_suspended = 1
    with pytest.raises(RuntimeError):
        await sess.unsuspend()
    assert sess.suspended is True and _notes(h) == []
    await sess.unsuspend()
    assert sess.suspended is False
    assert [n["kind"] for n in _notes(h)] == ["continue"]


async def _db_suspended(store: PersistenceStore) -> bool:
    return (await store.owned_summaries(OWNER))[0][1]


def _fail_once(monkeypatch: pytest.MonkeyPatch) -> None:
    real = Session._apply_order
    calls = {"n": 0}

    async def flaky(self, note):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("order failed")
        return await real(self, note)

    monkeypatch.setattr(Session, "_apply_order", flaky)


@pytest.mark.asyncio
async def test_failed_halt_order_keeps_session_suspended(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    reg = _registry(store)
    sid = await _create(reg)
    sess = await reg.get_or_raise(sid)
    h = FakeHandle()
    await sess.join(h, role="w", capabilities=[])
    h.sent.clear()
    _fail_once(monkeypatch)
    with pytest.raises(RuntimeError):
        await sess.suspend()
    # Fail closed: a disabled account's session stays suspended.
    assert sess.suspended is True
    assert await _db_suspended(store) is True
    with pytest.raises(ToolError) as ei:
        await sess.join(FakeHandle(), role="x", capabilities=[])
    assert ei.value.code is ErrorCode.SESSION_SUSPENDED
    await sess.suspend()  # retry re-sends the owed halt, exactly once
    assert [n["kind"] for n in _notes(h)] == ["halt"]
    await sess.suspend()  # nothing owed now: no-op
    assert [n["kind"] for n in _notes(h)] == ["halt"]


@pytest.mark.asyncio
async def test_failed_continue_order_rolls_back_unsuspension(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    reg = _registry(store)
    sid = await _create(reg)
    sess = await reg.get_or_raise(sid)
    h = FakeHandle()
    await sess.join(h, role="w", capabilities=[])
    await sess.suspend()
    h.sent.clear()
    _fail_once(monkeypatch)
    with pytest.raises(RuntimeError):
        await sess.unsuspend()
    assert sess.suspended is True
    assert await _db_suspended(store) is True
    await sess.unsuspend()
    assert sess.suspended is False and await _db_suspended(store) is False
    assert [n["kind"] for n in _notes(h)] == ["continue"]


@pytest.mark.asyncio
async def test_vanished_session_does_not_abort_batch(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    reg = _registry(store)
    s1, s2, s3 = [await _create(reg) for _ in range(3)]
    real = SessionRegistry.get_or_raise

    async def vanishing(self: SessionRegistry, sid: str):
        if sid == s2:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "gone")
        return await real(self, sid)

    monkeypatch.setattr(SessionRegistry, "get_or_raise", vanishing)
    assert await reg.suspend_owned(OWNER) == 3
    monkeypatch.undo()
    assert (await reg.get_or_raise(s1)).suspended
    assert (await reg.get_or_raise(s3)).suspended


@pytest.mark.asyncio
async def test_failing_session_is_reported_after_the_rest(
    store: PersistenceStore,
) -> None:
    flaky = _FlakyStore(store)
    reg = _registry(flaky)  # type: ignore[arg-type]
    s1, s2, s3 = [await _create(reg) for _ in range(3)]
    flaky.fail_sids = {s2}
    with pytest.raises(ToolError) as ei:
        await reg.suspend_owned(OWNER)
    assert ei.value.code == ErrorCode.PERSISTENCE_FAILED
    assert "1 of 3" in ei.value.message
    assert (await reg.get_or_raise(s1)).suspended
    assert (await reg.get_or_raise(s3)).suspended
    assert not (await reg.get_or_raise(s2)).suspended
