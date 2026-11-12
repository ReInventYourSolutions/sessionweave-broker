from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.tools import ToolDeps, session_create, session_get_events


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
    return ToolDeps(registry=registry, bounds=defaults.bounds, metrics=None)


@pytest.mark.asyncio
async def test_get_events_unknown_returns_not_found(deps):
    with pytest.raises(ToolError) as exc:
        await session_get_events(deps, {"session_id": "does_not_exist"})
    assert exc.value.code is ErrorCode.SESSION_NOT_FOUND


@pytest.mark.asyncio
async def test_get_events_ephemeral_returns_no_history(deps):
    # Create ephemeral (no persistence) — but we need a relay with store
    # disabled for THIS specific create. Workaround: create persisted then
    # manually unset persistence_mode in the hot session.
    created = await session_create(deps, {})
    # Ephemeral; session has _persistence_mode = None
    with pytest.raises(ToolError) as exc:
        await session_get_events(deps, {"session_id": created["session_id"]})
    assert exc.value.code is ErrorCode.NO_HISTORY


@pytest.mark.asyncio
async def test_get_events_hot_session_paginates(deps, store):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    sid = created["session_id"]
    # Manually push 20 events via the store (bypass session.send)
    for seq in range(1, 21):
        await store.persist_event_atomic(
            sid=sid, seq=seq, kind="result.received",
            payload={"i": seq}, ts=int(seq * 1000),
        )
    page1 = await session_get_events(deps, {"session_id": sid, "limit": 10})
    assert [e["seq"] for e in page1["events"]] == list(range(1, 11))
    assert page1["next_cursor"] == 10
    assert page1["has_more"] is True
    assert page1["session_state"] == "hot"
    page2 = await session_get_events(deps, {
        "session_id": sid, "after_seq": 10, "limit": 10,
    })
    assert [e["seq"] for e in page2["events"]] == list(range(11, 21))
    assert page2["has_more"] is False


@pytest.mark.asyncio
async def test_get_events_cold_session_no_hydrate(deps, store):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    sid = created["session_id"]
    for seq in range(1, 6):
        await store.persist_event_atomic(
            sid=sid, seq=seq, kind="x", payload={}, ts=seq * 1000,
        )
    # Evict from RAM to make it cold
    deps.registry._sessions.pop(sid, None)
    page = await session_get_events(deps, {"session_id": sid})
    assert len(page["events"]) == 5
    assert page["session_state"] == "cold"
    # Should NOT have triggered hydrate
    assert sid not in deps.registry._sessions


@pytest.mark.asyncio
async def test_get_events_kind_filter(deps, store):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    sid = created["session_id"]
    await store.persist_event_atomic(sid=sid, seq=1, kind="command.received",
                                      payload={"command_id": "a"}, ts=1)
    await store.persist_event_atomic(sid=sid, seq=2, kind="result.received",
                                      payload={}, ts=2)
    await store.persist_event_atomic(sid=sid, seq=3, kind="supervisor.note",
                                      payload={}, ts=3)
    page = await session_get_events(deps, {
        "session_id": sid, "kind": "command.received,supervisor.note",
    })
    assert [e["seq"] for e in page["events"]] == [1, 3]


@pytest.mark.asyncio
async def test_get_events_descending_order(deps, store):
    created = await session_create(deps, {
        "persistence": {"mode": "ttl", "ttl_seconds": 3600},
    })
    sid = created["session_id"]
    for seq in range(1, 6):
        await store.persist_event_atomic(sid=sid, seq=seq, kind="x",
                                          payload={}, ts=seq)
    page = await session_get_events(deps, {
        "session_id": sid, "order": "desc", "limit": 3,
    })
    assert [e["seq"] for e in page["events"]] == [5, 4, 3]
