import pytest
from pydantic import ValidationError

from sessionweave.schemas import (
    DispatchMode,
    InboxMessage,
    ParticipantPublic,
    SessionCreateInput,
    SessionJoinInput,
    SessionRespondInput,
    SessionSendInput,
    SessionUpdateSelfInput,
    SessionWaitForEventInput,
    SessionWaitForEventOutput,
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
        supervision_state="active",
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


def test_session_wait_for_event_input_defaults() -> None:
    inp = SessionWaitForEventInput.model_validate({"session_id": "s1"})
    assert inp.session_id == "s1"
    assert inp.names is None
    assert inp.timeout_seconds == 60.0


def test_session_wait_for_event_input_with_filter() -> None:
    inp = SessionWaitForEventInput.model_validate(
        {"session_id": "s1", "names": ["command.received"], "timeout_seconds": 30}
    )
    assert inp.names == ["command.received"]
    assert inp.timeout_seconds == 30.0


def test_session_wait_for_event_input_rejects_negative_timeout() -> None:
    with pytest.raises(ValidationError):
        SessionWaitForEventInput.model_validate(
            {"session_id": "s1", "timeout_seconds": -1}
        )


def test_session_wait_for_event_output_shape() -> None:
    out = SessionWaitForEventOutput(
        messages=[InboxMessage(name="x", payload={"y": 1})],
        timed_out=False,
        effective_timeout_seconds=60.0,
    )
    dumped = out.model_dump()
    assert dumped == {
        "messages": [{"name": "x", "payload": {"y": 1}, "seq": None}],
        "timed_out": False,
        "effective_timeout_seconds": 60.0,
    }


def test_session_wait_for_event_output_includes_effective_timeout() -> None:
    out = SessionWaitForEventOutput(
        messages=[InboxMessage(name="x", payload={"y": 1})],
        timed_out=False,
        effective_timeout_seconds=20.0,
    )
    dumped = out.model_dump()
    assert dumped == {
        "messages": [{"name": "x", "payload": {"y": 1}, "seq": None}],
        "timed_out": False,
        "effective_timeout_seconds": 20.0,
    }


def test_session_wait_for_event_output_requires_effective_timeout() -> None:
    # Use model_validate with a partial dict so the missing field is
    # caught at runtime by Pydantic, not at type-check time by mypy.
    with pytest.raises(ValidationError):
        SessionWaitForEventOutput.model_validate({"messages": [], "timed_out": True})
