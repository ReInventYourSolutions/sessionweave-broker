import asyncio
import time
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.account_events import EventLog, Origin
from sessionweave.persistence import PersistenceStore


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest.fixture
def log(store: PersistenceStore) -> EventLog:
    return EventLog(store)


async def test_emit_list_ordering_and_next(log: EventLog) -> None:
    ids = [await log.emit("a", "k", {"i": i}) for i in range(3)]
    assert ids == sorted(ids)
    events, nxt, truncated = await log.list_events(0, account_id=None, limit=100)
    assert [e["data"]["i"] for e in events] == [0, 1, 2]
    assert nxt == ids[-1] and truncated is False
    assert set(events[0]) == {"event_id", "account_id", "kind", "ts", "data"}
    events, nxt, _ = await log.list_events(ids[0], account_id=None, limit=1)
    assert [e["event_id"] for e in events] == [ids[1]] and nxt == ids[1]
    events, nxt, _ = await log.list_events(ids[-1], account_id=None, limit=10)
    assert events == [] and nxt == ids[-1]


async def test_scope_by_account(log: EventLog) -> None:
    await log.emit("a", "k", {})
    await log.emit("b", "k", {})
    events, _, _ = await log.list_events(0, account_id="a", limit=10)
    assert [e["account_id"] for e in events] == ["a"]


async def test_truncated_after_prune(log: EventLog) -> None:
    old = await log.emit("a", "k", {})
    now = int(time.time() * 1000)
    # Everything is "old" relative to a clock 40 days ahead; keep a fresh one after.
    assert await log.prune(now + 40 * 86_400_000, 30) == 1
    fresh = await log.emit("a", "k", {})
    events, _, truncated = await log.list_events(0, account_id="a", limit=10)
    assert [e["event_id"] for e in events] == [fresh] and truncated is True
    _, _, truncated = await log.list_events(old, account_id="a", limit=10)
    assert truncated is False
    # Fully emptied table: a stale cursor is still truncated.
    await log.prune(now + 80 * 86_400_000, 30)
    events, _, truncated = await log.list_events(0, account_id="a", limit=10)
    assert events == [] and truncated is True


async def test_wait_for_returns_on_emit(log: EventLog) -> None:
    task = asyncio.create_task(log.wait_for(0, account_id="a", timeout=5))
    await asyncio.sleep(0.05)
    assert not task.done()
    await log.emit("b", "k", {})  # other scope: no release
    await asyncio.sleep(0.05)
    assert not task.done()
    await log.emit("a", "k", {})
    await asyncio.wait_for(task, 1)


async def test_wait_for_returns_immediately_when_events_exist(log: EventLog) -> None:
    await log.emit("a", "k", {})
    await asyncio.wait_for(log.wait_for(0, account_id=None, timeout=5), 1)


async def test_wait_for_timeout(log: EventLog) -> None:
    t0 = time.monotonic()
    await log.wait_for(0, account_id=None, timeout=0.2)
    assert 0.15 <= time.monotonic() - t0 < 1.5


async def test_many_waiters_released_and_none_left(log: EventLog) -> None:
    tasks = [asyncio.create_task(log.wait_for(0, account_id=None, timeout=5)) for _ in range(50)]
    await asyncio.sleep(0.1)
    await log.emit("a", "k", {})
    await asyncio.wait_for(asyncio.gather(*tasks), 2)
    assert len(log._cond._waiters) == 0
    timed = [asyncio.create_task(log.wait_for(99, account_id="z", timeout=0.1)) for _ in range(5)]
    await asyncio.gather(*timed)
    assert len(log._cond._waiters) == 0


async def test_cancelled_waiter_deregisters(log: EventLog) -> None:
    task = asyncio.create_task(log.wait_for(0, account_id=None, timeout=5))
    await asyncio.sleep(0.05)
    assert len(log._cond._waiters) == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(log._cond._waiters) == 0


def test_origin() -> None:
    assert Origin("1.2.3.4", None).client is None
