"""Request-size and request-rate limits applied at the ASGI layer.

Pure ASGI middlewares (no ``BaseHTTPMiddleware``) so that streaming and
WebSocket traffic is never buffered or wrapped needlessly.
"""
from __future__ import annotations

import json
import logging
import os

from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

DEFAULT_MAX_REQUEST_BYTES = 5_242_880


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning("%s=%r is not an integer; using %s", name, raw, default)
        return default


def max_request_bytes() -> int:
    """``SW_MAX_REQUEST_BYTES``: largest accepted HTTP request body."""
    return _env_int("SW_MAX_REQUEST_BYTES", DEFAULT_MAX_REQUEST_BYTES)


class _BodyTooLargeError(Exception):
    pass


class RequestSizeLimitMiddleware:
    """Answer 413 ``payload_too_large`` to HTTP bodies above ``max_bytes``.

    A declared ``Content-Length`` over the cap is refused without reading the
    body. A body without one (chunked) is counted as it is read and refused
    as soon as it exceeds the cap. WebSocket and lifespan scopes pass through.
    """

    def __init__(self, app: ASGIApp, max_bytes: int | None = None) -> None:
        self.app = app
        self.max_bytes = max_bytes if max_bytes is not None else max_request_bytes()

    def _body(self) -> bytes:
        return json.dumps(
            {
                "error": {
                    "code": "payload_too_large",
                    "message": f"request body exceeds {self.max_bytes} bytes",
                }
            }
        ).encode()

    async def _refuse(self, send: Send) -> None:
        body = self._body()
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"connection", b"close"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    break
                if declared > self.max_bytes:
                    await self._refuse(send)
                    return
                break

        seen = 0
        exceeded = False
        started = False

        async def limited_receive() -> Message:
            nonlocal seen, exceeded
            message = await receive()
            if message["type"] == "http.request":
                seen += len(message.get("body", b""))
                if seen > self.max_bytes:
                    exceeded = True
                    raise _BodyTooLargeError
            return message

        async def guarded_send(message: Message) -> None:
            nonlocal started
            if exceeded:
                # The handler may have swallowed the abort and answered
                # something else; the client still gets the 413.
                if not started:
                    started = True
                    await self._refuse(send)
                return
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, guarded_send)
        except _BodyTooLargeError:
            pass
        if exceeded and not started:
            await self._refuse(send)
