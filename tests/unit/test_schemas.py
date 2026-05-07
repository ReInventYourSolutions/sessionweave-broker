import pytest
from pydantic import ValidationError

from sessionweave.schemas import (
    DispatchMode,
    ParticipantPublic,
    SessionCreateInput,
    SessionJoinInput,
    SessionRespondInput,
    SessionSendInput,
    SessionUpdateSelfInput,
    Target,
)


def test_participant_public_minimal() -> None:
    p = ParticipantPublic(
        participant_id="11111111-1111-1111-1111-111111111111",
        role=None,
        objective=None,
        capabilities=[],
        metadata={},
        joined_at="2026-05-06T10:00:00Z",
        online=True,
    )
    assert p.online is True


def test_session_send_specific_requires_participant_ids() -> None:
    with pytest.raises(ValidationError):
        SessionSendInput(
            dispatch=DispatchMode.SPECIFIC,
            target=Target(),
            payload={"x": 1},
        )


def test_session_send_all_requires_at_least_one_filter() -> None:
    with pytest.raises(ValidationError):
        SessionSendInput(
            dispatch=DispatchMode.ALL,
            target=Target(),
            payload={"x": 1},
        )


def test_session_send_all_accepts_role_only() -> None:
    SessionSendInput(
        dispatch=DispatchMode.ALL,
        target=Target(role="reviewer"),
        payload={"x": 1},
    )


def test_session_join_defaults() -> None:
    j = SessionJoinInput(session_id="s")
    assert j.role is None
    assert j.capabilities == []
    assert j.metadata == {}


def test_session_create_optional_config() -> None:
    SessionCreateInput()  # empty
    SessionCreateInput(config={"max_participants": 4})


def test_session_respond_default_status() -> None:
    r = SessionRespondInput(command_id="c", payload={})
    assert r.status == "ok"


def test_session_update_self_all_optional() -> None:
    SessionUpdateSelfInput()
