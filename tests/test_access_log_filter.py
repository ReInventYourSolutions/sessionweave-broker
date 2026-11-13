import logging

from sessionweave.access_log_filter import AccessLogNoiseFilter


def _rec(name: str, args: tuple[object, ...] | None) -> logging.LogRecord:
    return logging.LogRecord(
        name, logging.INFO, "", 0, '%s - "%s %s HTTP/%s" %d', args, None
    )


def _keep(name: str, args: tuple[object, ...] | None) -> bool:
    return AccessLogNoiseFilter().filter(_rec(name, args))


def test_drops_healthz() -> None:
    assert _keep("uvicorn.access", ("c", "GET", "/healthz", "1.1", 200)) is False


def test_drops_metrics() -> None:
    assert _keep("uvicorn.access", ("c", "GET", "/metrics", "1.1", 200)) is False


def test_drops_healthz_with_query() -> None:
    assert _keep("uvicorn.access", ("c", "GET", "/healthz?probe=1", "1.1", 200)) is False


def test_drops_root_404() -> None:
    assert _keep("uvicorn.access", ("c", "GET", "/", "1.1", 404)) is False


def test_drops_scanner_path_404() -> None:
    assert _keep("uvicorn.access", ("c", "POST", "/wp-login.php", "1.1", 404)) is False


def test_keeps_v1_404() -> None:
    assert _keep("uvicorn.access", ("c", "GET", "/v1/sessions/x", "1.1", 404)) is True


def test_keeps_v1_200() -> None:
    assert _keep("uvicorn.access", ("c", "POST", "/v1/sessions", "1.1", 200)) is True


def test_keeps_mcp() -> None:
    assert _keep("uvicorn.access", ("c", "GET", "/mcp", "1.1", 200)) is True


def test_keeps_websocket_shape() -> None:
    assert _keep("uvicorn.access", ("c", "WebSocket /v1/sessions/x/events", 403)) is True


def test_keeps_non_access_record() -> None:
    assert _keep("uvicorn.error", ("c", "GET", "/healthz", "1.1", 200)) is True


def test_keeps_on_none_args() -> None:
    assert _keep("uvicorn.access", None) is True


def _emitted(args: tuple[object, ...]) -> str:
    rec = _rec("uvicorn.access", args)
    assert AccessLogNoiseFilter().filter(rec) is True
    return rec.getMessage() if len(args) == 5 else str(rec.args)


def test_redacts_jt_value_keeps_rest_of_query() -> None:
    out = _emitted(("c", "GET", "/invite/s1?jt=SECRET123&x=1", "1.1", 200))
    assert "SECRET123" not in out
    assert "jt=[REDACTED]&x=1" in out


def test_redacts_token_value_in_http_path() -> None:
    out = _emitted(("c", "GET", "/v1/sessions/s1/events?a=b&token=SECRET123", "1.1", 200))
    assert "SECRET123" not in out
    assert "a=b&token=[REDACTED]" in out


def _ws_record(args: tuple[object, ...]) -> logging.LogRecord:
    return logging.LogRecord(
        "uvicorn.error", logging.INFO, "", 0, '%s - "WebSocket %s" [accepted]', args, None
    )


def test_redacts_real_websocket_line_on_uvicorn_error() -> None:
    rec = _ws_record(("1.2.3.4:5", "/v1/sessions/s/firehose?token=SECRET123&a=b"))
    assert AccessLogNoiseFilter().filter(rec) is True
    out = rec.getMessage()
    assert "SECRET123" not in out
    assert "token=[REDACTED]&a=b" in out


def test_uvicorn_error_records_are_never_dropped() -> None:
    rec = _ws_record(("c", "/healthz"))
    assert AccessLogNoiseFilter().filter(rec) is True
    rec = logging.LogRecord("uvicorn.error", logging.INFO, "", 0, "/metrics 404", None, None)
    assert AccessLogNoiseFilter().filter(rec) is True


def test_redacts_secret_in_msg() -> None:
    rec = logging.LogRecord(
        "uvicorn.error", logging.INFO, "", 0, "GET /x?jt=SECRET123", None, None
    )
    AccessLogNoiseFilter().filter(rec)
    assert "SECRET123" not in rec.getMessage()


def test_does_not_touch_lookalike_params() -> None:
    out = _emitted(("c", "GET", "/v1/x?mytoken=keep&jt2=keep", "1.1", 200))
    assert "mytoken=keep" in out and "jt2=keep" in out


def test_redaction_never_raises_on_odd_args() -> None:
    assert _keep("uvicorn.access", ("c", None, b"\xff", object(), "x")) is True


def test_install_attaches_to_access_and_error_loggers() -> None:
    from sessionweave.access_log_filter import install_uvicorn_log_filters

    install_uvicorn_log_filters()
    for name in ("uvicorn.access", "uvicorn.error"):
        assert any(
            isinstance(f, AccessLogNoiseFilter) for f in logging.getLogger(name).filters
        )
