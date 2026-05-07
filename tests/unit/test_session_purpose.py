from __future__ import annotations

import pytest

from sessionweave.config import ConfigBounds, DeploymentDefaults, SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.registry import SessionRegistry


def _defaults() -> DeploymentDefaults:
    return DeploymentDefaults(
        defaults=SessionConfig(
            max_participants=8,
            max_payload_bytes=4096,
            idle_ttl_seconds=3600,
            offline_grace_seconds=60,
            default_command_timeout_seconds=10,
            max_command_timeout_seconds=60,
            max_pending_notifications_per_participant=200,
        ),
        bounds=ConfigBounds.builtin(),
    )


@pytest.mark.asyncio
async def test_create_persists_purpose_and_returns_it() -> None:
    reg = SessionRegistry(_defaults(), ConnectionManager())
    purpose = {"kind": "tic-tac-toe-v1", "your_role": "O"}
    sid, _expires, _cfg, returned = await reg.create({}, purpose=purpose)
    assert returned == purpose
    sess = reg.get_or_raise(sid)
    assert sess.purpose == purpose


@pytest.mark.asyncio
async def test_create_without_purpose_keeps_none() -> None:
    reg = SessionRegistry(_defaults(), ConnectionManager())
    sid, _expires, _cfg, returned = await reg.create({})
    assert returned is None
    assert reg.get_or_raise(sid).purpose is None


@pytest.mark.asyncio
async def test_create_rejects_oversized_purpose() -> None:
    reg = SessionRegistry(_defaults(), ConnectionManager())
    huge = {"blob": "x" * 17_000}  # > 16 KiB JSON
    with pytest.raises(ToolError) as exc:
        await reg.create({}, purpose=huge)
    assert exc.value.code is ErrorCode.INVALID_CONFIG
    assert "purpose" in exc.value.message
