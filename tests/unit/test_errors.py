from sessionweave.errors import ErrorCode, ToolError


def test_error_code_values_are_unique_strings() -> None:
    values = [c.value for c in ErrorCode]
    assert len(values) == len(set(values))
    assert all(isinstance(v, str) for v in values)


def test_tool_error_carries_code_and_message() -> None:
    err = ToolError(ErrorCode.SESSION_NOT_FOUND, "no such session")
    assert err.code is ErrorCode.SESSION_NOT_FOUND
    assert err.message == "no such session"
    assert str(err) == "session_not_found: no such session"


def test_to_dict_shape() -> None:
    err = ToolError(ErrorCode.INVALID_CONFIG, "bad bound")
    assert err.to_dict() == {"code": "invalid_config", "message": "bad bound"}
