from __future__ import annotations

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


def _config() -> SessionConfig:
    return SessionConfig(
        max_participants=8,
        max_payload_bytes=4096,
        idle_ttl_seconds=3600,
        offline_grace_seconds=60,
        default_command_timeout_seconds=10,
        max_command_timeout_seconds=60,
        max_pending_notifications_per_participant=200,
    )


@pytest.mark.asyncio
async def test_update_purpose_replaces_value_and_notifies_others() -> None:
    sess = Session("s", _config(), ConnectionManager(), purpose={"old": True})

    h_owner = FakeHandle()
    owner = await sess.join(h_owner, role="host")
    h_other = FakeHandle()
    other = await sess.join(h_other, role="guest")
    h_other.sent.clear()

    new_purpose = {"kind": "v2", "your_role": "guest"}
    returned = await sess.update_purpose(owner.participant_id, new_purpose)
    assert returned == new_purpose
    assert sess.purpose == new_purpose

    notifications = [(n, p) for n, p in h_other.sent if n == "session.purpose_updated"]
    assert notifications, "other participant must be notified"
    _, payload = notifications[-1]
    assert payload == {"purpose": new_purpose}

    # The requester does NOT receive their own notification.
    assert all(n != "session.purpose_updated" for n, _ in h_owner.sent)
    # Silence unused-binding lint:
    _ = other


@pytest.mark.asyncio
async def test_update_purpose_can_clear_to_none() -> None:
    sess = Session("s", _config(), ConnectionManager(), purpose={"x": 1})
    h = FakeHandle()
    p = await sess.join(h, role="r")
    returned = await sess.update_purpose(p.participant_id, None)
    assert returned is None
    assert sess.purpose is None


@pytest.mark.asyncio
async def test_update_purpose_rejects_unknown_caller() -> None:
    sess = Session("s", _config(), ConnectionManager())
    with pytest.raises(ToolError) as exc:
        await sess.update_purpose("00000000-0000-0000-0000-000000000000", {"a": 1})
    assert exc.value.code is ErrorCode.NOT_IN_SESSION


@pytest.mark.asyncio
async def test_update_purpose_rejects_oversized() -> None:
    sess = Session("s", _config(), ConnectionManager())
    h = FakeHandle()
    p = await sess.join(h, role="r")
    huge = {"blob": "x" * 17_000}
    with pytest.raises(ToolError) as exc:
        await sess.update_purpose(p.participant_id, huge)
    assert exc.value.code is ErrorCode.INVALID_CONFIG
    assert "purpose" in exc.value.message
