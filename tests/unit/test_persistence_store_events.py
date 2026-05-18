import json
from pathlib import Path

import aiosqlite
import pytest
import pytest_asyncio

from sessionweave.persistence import PersistenceStore, SessionMetadata
from sessionweave.persistence_token import generate_creator_token, hash_creator_token


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


@pytest_asyncio.fixture
async def store_with_session(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    now = _now_ms()
    await s.save_session_metadata(SessionMetadata(
        session_id="sid", config_json="{}", purpose_json=None,
        creator_token_hash=hash_creator_token(generate_creator_token()),
        persistence_mode="ttl", persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    yield s
    await s.close()


@pytest.mark.asyncio
async def test_persist_event_atomic_simple(store_with_session: PersistenceStore) -> None:
    s = store_with_session
    await s.persist_event_atomic(
        sid="sid", seq=1, kind="command.received",
        payload={"command_id": "c1", "text": "hi"}, ts=_now_ms(),
    )
    page = await s.fetch_events(sid="sid")
    assert len(page.events) == 1
    assert page.events[0]["seq"] == 1
    assert page.events[0]["payload"]["command_id"] == "c1"


@pytest.mark.asyncio
async def test_persist_event_atomic_with_inflight_and_buffered(
    store_with_session: PersistenceStore,
) -> None:
    s = store_with_session
    await s.persist_event_atomic(
        sid="sid", seq=1, kind="command.received",
        payload={"command_id": "c1"}, ts=_now_ms(),
        inflight_upsert=("c1", {"sender_pid": "p_a", "deadline_at": 1234}),
        buffered_pids=("p_b", "p_c"),
    )
    page = await s.fetch_events(sid="sid")
    assert len(page.events) == 1
    async with s._read_pool[0].execute(
        "SELECT command_id, tracker_json FROM inflight_commands WHERE session_id='sid'"
    ) as cur:
        rows = list(await cur.fetchall())
    assert len(rows) == 1
    assert rows[0][0] == "c1"
    assert json.loads(rows[0][1])["sender_pid"] == "p_a"
    async with s._read_pool[0].execute(
        "SELECT participant_id FROM buffered_notifications"
        " WHERE session_id='sid' ORDER BY participant_id"
    ) as cur:
        rows = list(await cur.fetchall())
    assert [r[0] for r in rows] == ["p_b", "p_c"]


@pytest.mark.asyncio
async def test_command_id_uniqueness_enforced(store_with_session: PersistenceStore) -> None:
    s = store_with_session
    await s.persist_event_atomic(
        sid="sid", seq=1, kind="command.received",
        payload={"command_id": "dup"}, ts=_now_ms(),
    )
    with pytest.raises(aiosqlite.IntegrityError):
        await s.persist_event_atomic(
            sid="sid", seq=2, kind="command.received",
            payload={"command_id": "dup"}, ts=_now_ms(),
        )


@pytest.mark.asyncio
async def test_lookup_seq_by_command_id(store_with_session: PersistenceStore) -> None:
    s = store_with_session
    await s.persist_event_atomic(
        sid="sid", seq=7, kind="command.received",
        payload={"command_id": "find_me"}, ts=_now_ms(),
    )
    seq = await s.lookup_seq_by_command_id("sid", "find_me")
    assert seq == 7
    assert await s.lookup_seq_by_command_id("sid", "nope") is None


@pytest.mark.asyncio
async def test_persist_event_atomic_updates_last_activity(
    store_with_session: PersistenceStore,
) -> None:
    s = store_with_session
    await s.persist_event_atomic(
        sid="sid", seq=1, kind="command.received",
        payload={"command_id": "c"}, ts=999_999,
    )
    async with s._read_pool[0].execute(
        "SELECT last_activity_at FROM sessions WHERE session_id='sid'"
    ) as cur:
        _la_row = await cur.fetchone()
    assert _la_row is not None
    (la,) = _la_row
    assert la == 999_999


@pytest.mark.asyncio
async def test_fetch_events_pagination_ascending(
    store_with_session: PersistenceStore,
) -> None:
    s = store_with_session
    for i in range(1, 21):
        await s.persist_event_atomic(
            sid="sid", seq=i, kind="result.received",
            payload={"i": i}, ts=_now_ms(),
        )
    p1 = await s.fetch_events(sid="sid", limit=5, order="asc")
    assert [e["seq"] for e in p1.events] == [1, 2, 3, 4, 5]
    assert p1.next_cursor == 5
    assert p1.has_more is True
    p2 = await s.fetch_events(sid="sid", after_seq=5, limit=5, order="asc")
    assert [e["seq"] for e in p2.events] == [6, 7, 8, 9, 10]


@pytest.mark.asyncio
async def test_fetch_events_descending_with_before(
    store_with_session: PersistenceStore,
) -> None:
    s = store_with_session
    for i in range(1, 11):
        await s.persist_event_atomic(
            sid="sid", seq=i, kind="x", payload={"i": i}, ts=_now_ms(),
        )
    p = await s.fetch_events(sid="sid", limit=3, order="desc")
    assert [e["seq"] for e in p.events] == [10, 9, 8]
    p2 = await s.fetch_events(sid="sid", before_seq=8, limit=3, order="desc")
    assert [e["seq"] for e in p2.events] == [7, 6, 5]


@pytest.mark.asyncio
async def test_fetch_events_kind_filter(
    store_with_session: PersistenceStore,
) -> None:
    s = store_with_session
    await s.persist_event_atomic(sid="sid", seq=1, kind="command.received",
                                 payload={"command_id": "a"}, ts=_now_ms())
    await s.persist_event_atomic(sid="sid", seq=2, kind="result.received",
                                 payload={}, ts=_now_ms())
    await s.persist_event_atomic(sid="sid", seq=3, kind="supervisor.note",
                                 payload={}, ts=_now_ms())
    p = await s.fetch_events(sid="sid", kinds=("command.received", "supervisor.note"))
    assert [e["seq"] for e in p.events] == [1, 3]
