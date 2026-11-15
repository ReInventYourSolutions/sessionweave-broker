"""Per-IP limit on new MCP transports (initialize without mcp-session-id)."""
from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from sessionweave.limits import McpNewSessionLimitMiddleware, mcp_new_sessions_per_minute


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _app(limit: int, clock: _Clock) -> Starlette:
    async def ok(request: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/mcp", ok, methods=["POST", "GET"])])
    app.add_middleware(McpNewSessionLimitMiddleware, per_minute=limit, clock=clock)
    return app


def test_new_transports_limited_per_ip_with_retry_after() -> None:
    clock = _Clock()
    with TestClient(_app(3, clock)) as c:
        for _ in range(3):
            assert c.post("/mcp").status_code == 200
        r = c.post("/mcp")
        assert r.status_code == 429
        body = r.json()
        assert body["jsonrpc"] == "2.0" and body["id"] is None
        assert body["error"]["code"] == -32000
        assert 1 <= int(r.headers["retry-after"]) <= 60
        clock.now += 61
        assert c.post("/mcp").status_code == 200


def test_requests_with_session_id_and_gets_are_not_limited() -> None:
    clock = _Clock()
    with TestClient(_app(1, clock)) as c:
        assert c.post("/mcp").status_code == 200
        assert c.post("/mcp").status_code == 429
        for _ in range(10):
            assert c.post("/mcp", headers={"mcp-session-id": "abc"}).status_code == 200
            assert c.get("/mcp").status_code == 200


def test_sliding_window_not_fixed() -> None:
    clock = _Clock()
    with TestClient(_app(2, clock)) as c:
        assert c.post("/mcp").status_code == 200
        clock.now += 40
        assert c.post("/mcp").status_code == 200
        assert c.post("/mcp").status_code == 429
        clock.now += 25  # first aged out (65s), second still inside
        assert c.post("/mcp").status_code == 200
        assert c.post("/mcp").status_code == 429


def test_buckets_are_pruned() -> None:
    clock = _Clock()
    mw = McpNewSessionLimitMiddleware(
        _app(5, clock), per_minute=5, clock=clock, max_tracked_ips=50
    )
    for i in range(500):
        mw._allow(f"10.0.{i // 250}.{i % 250}")
        clock.now += 1.0
    assert len(mw._hits) <= 50
    for i in range(200):  # a flood inside one window still stays bounded
        mw._allow(f"11.0.0.{i}")
    assert len(mw._hits) <= 50
    clock.now += 120
    for i in range(50):
        mw._allow(f"12.0.0.{i}")
    mw._allow("9.9.9.9")
    assert "9.9.9.9" in mw._hits and len(mw._hits) <= 51


def test_env_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SW_MCP_NEW_SESSIONS_PER_MINUTE", raising=False)
    assert mcp_new_sessions_per_minute() == 30
    monkeypatch.setenv("SW_MCP_NEW_SESSIONS_PER_MINUTE", "7")
    assert mcp_new_sessions_per_minute() == 7
