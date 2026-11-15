import pytest

from sessionweave.accounts import session_creation_mode
from sessionweave.errors import ErrorCode, ToolError


def test_default_is_open() -> None:
    assert session_creation_mode({}) == "open"
    assert session_creation_mode({"SW_SESSION_CREATION": "open"}) == "open"


def test_account_needs_db() -> None:
    with pytest.raises(ToolError) as ei:
        session_creation_mode({"SW_SESSION_CREATION": "account"})
    assert ei.value.code == ErrorCode.INVALID_CONFIG
    assert "SW_PERSISTENCE_DB_PATH" in ei.value.message


def test_account_needs_admin() -> None:
    with pytest.raises(ToolError) as ei:
        session_creation_mode(
            {"SW_SESSION_CREATION": "account", "SW_PERSISTENCE_DB_PATH": "/x.db"}
        )
    assert ei.value.code == ErrorCode.INVALID_CONFIG
    assert "SW_ADMIN_TOKEN" in ei.value.message


def test_account_ok() -> None:
    env = {
        "SW_SESSION_CREATION": "account",
        "SW_PERSISTENCE_DB_PATH": "/x.db",
        "SW_ADMIN_TOKEN": "t",
    }
    assert session_creation_mode(env) == "account"


def test_bad_value() -> None:
    with pytest.raises(ToolError) as ei:
        session_creation_mode({"SW_SESSION_CREATION": "public"})
    assert ei.value.code == ErrorCode.INVALID_CONFIG
