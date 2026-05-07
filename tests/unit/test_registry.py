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
