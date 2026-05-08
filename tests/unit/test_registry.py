from datetime import UTC, datetime, timedelta

import pytest

from sessionweave.config import (
    ConfigBounds,
    DeploymentDefaults,
    SessionConfig,
)
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.registry import SessionRegistry
from tests.unit.fakes import FakeHandle


def _deps() -> DeploymentDefaults:
    return DeploymentDefaults(SessionConfig(), ConfigBounds.builtin())


@pytest.mark.asyncio
async def test_create_returns_session_id_and_config() -> None:
    reg = SessionRegistry(_deps(), ConnectionManager())
    sid, expires_at, cfg, _purpose = await reg.create({})
    assert sid
    assert expires_at
    assert cfg["max_participants"] == 16


@pytest.mark.asyncio
async def test_create_with_overrides_in_bounds() -> None:
    reg = SessionRegistry(_deps(), ConnectionManager())
    _sid, _, cfg, _purpose = await reg.create({"max_participants": 4})
    assert cfg["max_participants"] == 4


@pytest.mark.asyncio
async def test_create_with_out_of_bounds_raises_invalid_config() -> None:
    reg = SessionRegistry(_deps(), ConnectionManager())
    with pytest.raises(ToolError) as exc:
        await reg.create({"max_participants": 1})
    assert exc.value.code is ErrorCode.INVALID_CONFIG


@pytest.mark.asyncio
async def test_get_or_raise_returns_session_or_session_not_found() -> None:
    reg = SessionRegistry(_deps(), ConnectionManager())
    sid, _, _, _ = await reg.create({})
    assert reg.get_or_raise(sid).session_id == sid
    with pytest.raises(ToolError) as exc:
        reg.get_or_raise("unknown")
    assert exc.value.code is ErrorCode.SESSION_NOT_FOUND


@pytest.mark.asyncio
async def test_tick_evicts_idle_expired_session() -> None:
    deps = DeploymentDefaults(SessionConfig(idle_ttl_seconds=60), ConfigBounds.builtin())
    reg = SessionRegistry(deps, ConnectionManager())
    sid, _, _, _ = await reg.create({})
    future = datetime.now(UTC) + timedelta(seconds=120)
    reg.tick(now=future)
    with pytest.raises(ToolError) as exc:
        reg.get_or_raise(sid)
    assert exc.value.code is ErrorCode.SESSION_NOT_FOUND


@pytest.mark.asyncio
async def test_tick_emits_session_ended_to_online_participants() -> None:
    deps = DeploymentDefaults(SessionConfig(idle_ttl_seconds=1), ConfigBounds.builtin())
    reg = SessionRegistry(deps, ConnectionManager())
    sid, _, _, _ = await reg.create({})
    s = reg.get_or_raise(sid)
    h = FakeHandle()
    await s.join(h, role=None, capabilities=[])
    future = datetime.now(UTC) + timedelta(seconds=10)
    reg.tick(now=future)
    assert any(n == "session.ended" for n, _ in h.sent)


@pytest.mark.asyncio
async def test_tick_only_processes_due_sessions_via_heap() -> None:
    """Heap-driven tick must not pop entries whose deadline is still
    in the future — the heap stays intact and the session is not
    inspected."""
    deps = DeploymentDefaults(
        SessionConfig(idle_ttl_seconds=3600), ConfigBounds.builtin()
    )
    reg = SessionRegistry(deps, ConnectionManager())
    sid, _, _, _ = await reg.create({})
    heap_before = list(reg._heap)
    reg.tick(now=datetime.now(UTC))
    # idle_ttl is 1h: nothing is due so the heap entry must be preserved
    # exactly (no pop + re-push), which means the same tuple still sits at
    # position 0.
    assert reg._heap == heap_before
    assert reg.get_or_raise(sid).session_id == sid


@pytest.mark.asyncio
async def test_tick_skips_stale_heap_entries_for_evicted_sessions() -> None:
    """If a session is removed (e.g. by previous eviction), any leftover
    heap entry is dropped silently rather than raising."""
    deps = DeploymentDefaults(SessionConfig(idle_ttl_seconds=1), ConfigBounds.builtin())
    reg = SessionRegistry(deps, ConnectionManager())
    sid, _, _, _ = await reg.create({})
    # First tick evicts the session.
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=10))
    assert sid not in reg._sessions
    # Manually push a stale entry; second tick should drop it cleanly.
    import heapq

    heapq.heappush(reg._heap, (datetime.now(UTC).timestamp(), sid))
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=20))


@pytest.mark.asyncio
async def test_next_sleep_capped_at_max_when_heap_far_in_future() -> None:
    deps = DeploymentDefaults(
        SessionConfig(idle_ttl_seconds=3600), ConfigBounds.builtin()
    )
    reg = SessionRegistry(deps, ConnectionManager())
    await reg.create({})
    # Heap top is ~1h away; sleep must still be capped (default 1.0s).
    sleep_for = reg._next_sleep_seconds()
    assert 0 < sleep_for <= 1.0


@pytest.mark.asyncio
async def test_next_sleep_zero_when_deadline_already_past() -> None:
    deps = DeploymentDefaults(SessionConfig(idle_ttl_seconds=1), ConfigBounds.builtin())
    reg = SessionRegistry(deps, ConnectionManager())
    await reg.create({})
    # Pretend "now" is well past the heap top — sleep should be 0.
    far_future = datetime.now(UTC) + timedelta(seconds=10)
    assert reg._next_sleep_seconds(now=far_future) == 0.0


@pytest.mark.asyncio
async def test_create_wakes_running_loop() -> None:
    """A new session pushes a fresh heap entry and signals the loop's
    wake-event so a sleeping runner re-evaluates its next deadline."""
    deps = DeploymentDefaults(SessionConfig(idle_ttl_seconds=1), ConfigBounds.builtin())
    reg = SessionRegistry(deps, ConnectionManager())
    await reg.start(interval_seconds=10.0)
    try:
        before_len = len(reg._heap)
        await reg.create({})
        # Newly-pushed entry is visible without waiting for the cap.
        assert len(reg._heap) == before_len + 1
        assert reg._wake is not None
    finally:
        await reg.shutdown()
