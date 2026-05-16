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


def test_new_error_codes_for_wait_for_event() -> None:
    from sessionweave.errors import ErrorCode

    assert ErrorCode.INVALID_DELIVERY_MODE.value == "invalid_delivery_mode"
    assert ErrorCode.WAIT_ALREADY_IN_FLIGHT.value == "wait_already_in_flight"
    assert ErrorCode.PARTICIPANT_EVICTED.value == "participant_evicted"
    assert ErrorCode.TOO_MANY_WAITS.value == "too_many_waits"


def test_new_error_codes_have_http_status_mapping() -> None:
    from sessionweave.errors import ErrorCode
    from sessionweave.http_api import _STATUS_BY_CODE

    assert _STATUS_BY_CODE[ErrorCode.INVALID_DELIVERY_MODE] == 409
    assert _STATUS_BY_CODE[ErrorCode.WAIT_ALREADY_IN_FLIGHT] == 409
    assert _STATUS_BY_CODE[ErrorCode.PARTICIPANT_EVICTED] == 409
    assert _STATUS_BY_CODE[ErrorCode.TOO_MANY_WAITS] == 429
