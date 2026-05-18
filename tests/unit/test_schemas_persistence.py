import pytest
from pydantic import ValidationError

from sessionweave.errors import ErrorCode
from sessionweave.schemas import (
    PersistenceConfig,
    PersistenceStatePublic,
    SessionCreateInput,
    SessionCreateOutput,
    SessionDeleteInput,
    SessionDeleteOutput,
    SessionDescribeOutput,
    SessionGetEventsInput,
    SessionGetEventsOutput,
    SessionJoinOutput,
)


def test_persistence_config_ttl_requires_ttl_seconds() -> None:
    cfg = PersistenceConfig(mode="ttl", ttl_seconds=86400)
    assert cfg.mode == "ttl"
    assert cfg.ttl_seconds == 86400


def test_persistence_config_ttl_without_seconds_raises() -> None:
    with pytest.raises(ValidationError):
        PersistenceConfig(mode="ttl")


def test_persistence_config_until_deleted_rejects_ttl_seconds() -> None:
    cfg = PersistenceConfig(mode="until_deleted")
    assert cfg.ttl_seconds is None
    with pytest.raises(ValidationError):
        PersistenceConfig(mode="until_deleted", ttl_seconds=3600)


def test_persistence_state_public_shape() -> None:
    s = PersistenceStatePublic(mode="ttl", expires_at="2026-06-01T00:00:00+00:00")
    assert s.mode == "ttl"
    assert s.expires_at == "2026-06-01T00:00:00+00:00"
    s2 = PersistenceStatePublic(mode="until_deleted", expires_at=None)
    assert s2.expires_at is None


def test_session_create_input_persistence_optional() -> None:
    inp = SessionCreateInput.model_validate({})
    assert inp.persistence is None
    inp2 = SessionCreateInput.model_validate(
        {"persistence": {"mode": "ttl", "ttl_seconds": 3600}}
    )
    assert inp2.persistence is not None
    assert inp2.persistence.mode == "ttl"


def test_session_create_output_includes_creator_token() -> None:
    out = SessionCreateOutput(
        session_id="s_1",
        expires_at="2026-06-01T00:00:00+00:00",
        config={"idle_ttl_seconds": 1800},
        purpose=None,
        persistence=PersistenceStatePublic(mode="until_deleted", expires_at=None),
        creator_token="abc.def.ghi",
    )
    d = out.model_dump()
    assert d["creator_token"] == "abc.def.ghi"
    assert d["persistence"]["mode"] == "until_deleted"


def test_session_describe_output_persistence_field() -> None:
    out = SessionDescribeOutput.model_validate(
        {
            "session_id": "s_1",
            "expires_at": "2026-06-01T00:00:00+00:00",
            "config": {},
            "purpose": None,
            "roster": [],
            "persistence": None,
        }
    )
    assert out.persistence is None


def test_session_join_output_persistence_field() -> None:
    out = SessionJoinOutput.model_validate(
        {
            "participant_id": "p_1",
            "expires_at": "2026-06-01T00:00:00+00:00",
            "roster": [],
            "config": {},
            "purpose": None,
            "relay_epoch": 0,
            "persistence": {"mode": "ttl", "expires_at": "2026-06-01T00:00:00+00:00"},
        }
    )
    assert out.persistence is not None
    assert out.persistence.mode == "ttl"


def test_session_delete_input_requires_token() -> None:
    SessionDeleteInput.model_validate({"session_id": "s_1", "creator_token": "xyz"})
    with pytest.raises(ValidationError):
        SessionDeleteInput.model_validate({"session_id": "s_1"})


def test_session_delete_output_shape() -> None:
    out = SessionDeleteOutput(deleted_at="2026-05-18T12:00:00+00:00")
    assert out.model_dump() == {"deleted_at": "2026-05-18T12:00:00+00:00"}


def test_session_get_events_input_defaults() -> None:
    inp = SessionGetEventsInput.model_validate({"session_id": "s_1"})
    assert inp.session_id == "s_1"
    assert inp.after_seq is None
    assert inp.before_seq is None
    assert inp.kind is None
    assert inp.participant_id is None
    assert inp.limit == 200
    assert inp.order == "asc"


def test_session_get_events_input_bounds() -> None:
    inp = SessionGetEventsInput.model_validate({"session_id": "s_1", "limit": 1000})
    assert inp.limit == 1000
    with pytest.raises(ValidationError):
        SessionGetEventsInput.model_validate({"session_id": "s_1", "limit": 0})
    with pytest.raises(ValidationError):
        SessionGetEventsInput.model_validate({"session_id": "s_1", "limit": 1001})


def test_session_get_events_input_csv_filters_parsed() -> None:
    inp = SessionGetEventsInput.model_validate(
        {"session_id": "s_1", "kind": "command.received,result.received"}
    )
    assert inp.kind == ["command.received", "result.received"]
    inp2 = SessionGetEventsInput.model_validate({"session_id": "s_1", "participant_id": "p_1,p_2"})
    assert inp2.participant_id == ["p_1", "p_2"]


def test_session_get_events_output_shape() -> None:
    out = SessionGetEventsOutput(
        session_id="s_1",
        events=[{"seq": 1, "kind": "command.received", "ts": "x", "payload": {}}],
        next_cursor=1,
        has_more=False,
        session_state="hot",
    )
    d = out.model_dump()
    assert d["session_state"] == "hot"
    assert d["next_cursor"] == 1


def test_new_error_codes_present() -> None:
    assert ErrorCode.PERMISSION_DENIED.value == "permission_denied"
    assert ErrorCode.NO_HISTORY.value == "no_history"
    assert ErrorCode.PERSISTENCE_FAILED.value == "persistence_failed"
    assert ErrorCode.PERSISTENCE_CORRUPTED.value == "persistence_corrupted"
