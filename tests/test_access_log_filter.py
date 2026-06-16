import logging

from sessionweave.access_log_filter import AccessLogNoiseFilter


def _rec(name: str, args: object) -> logging.LogRecord:
    return logging.LogRecord(
        name, logging.INFO, "", 0, '%s - "%s %s HTTP/%s" %d', args, None
    )


def _keep(name: str, args: object) -> bool:
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
