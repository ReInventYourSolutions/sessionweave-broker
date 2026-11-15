"""App-level request body cap (SW_MAX_REQUEST_BYTES)."""
from __future__ import annotations

from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute
from starlette.testclient import TestClient
from starlette.websockets import WebSocket

from sessionweave.limits import RequestSizeLimitMiddleware


def _app(cap: int, reads: list[int]) -> Starlette:
    async def echo(request: Request) -> JSONResponse:
        reads.append(1)
        body = await request.body()
        return JSONResponse({"n": len(body)})

    async def ws(sock: WebSocket) -> None:
        await sock.accept()
        data = await sock.receive_text()
        await sock.send_text(str(len(data)))
        await sock.close()

    app = Starlette(routes=[Route("/e", echo, methods=["POST"]), WebSocketRoute("/w", ws)])
    app.add_middleware(RequestSizeLimitMiddleware, max_bytes=cap)
    return app


def test_small_body_passes() -> None:
    reads: list[int] = []
    with TestClient(_app(100, reads)) as c:
        r = c.post("/e", content=b"x" * 100)
    assert r.status_code == 200 and r.json() == {"n": 100}


def test_content_length_over_cap_is_413_without_reading() -> None:
    reads: list[int] = []
    with TestClient(_app(100, reads)) as c:
        r = c.post("/e", content=b"x" * 101)
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "payload_too_large"
    assert reads == []


def test_chunked_body_over_cap_is_413() -> None:
    reads: list[int] = []

    def gen() -> Any:
        for _ in range(10):
            yield b"x" * 30

    with TestClient(_app(100, reads)) as c:
        r = c.post("/e", content=gen())
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "payload_too_large"


def test_chunked_body_under_cap_passes() -> None:
    reads: list[int] = []

    def gen() -> Any:
        yield b"x" * 40
        yield b"x" * 40

    with TestClient(_app(100, reads)) as c:
        r = c.post("/e", content=gen())
    assert r.status_code == 200 and r.json() == {"n": 80}


def test_websocket_is_not_limited() -> None:
    with TestClient(_app(10, [])) as c, c.websocket_connect("/w") as s:
        s.send_text("y" * 500)
        assert s.receive_text() == "500"


def test_env_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    from sessionweave.limits import max_request_bytes

    monkeypatch.delenv("SW_MAX_REQUEST_BYTES", raising=False)
    assert max_request_bytes() == 5_242_880
    monkeypatch.setenv("SW_MAX_REQUEST_BYTES", "1234")
    assert max_request_bytes() == 1234
    monkeypatch.setenv("SW_MAX_REQUEST_BYTES", "junk")
    assert max_request_bytes() == 5_242_880
