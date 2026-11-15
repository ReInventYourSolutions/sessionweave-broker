"""Request-size and request-rate limits applied at the ASGI layer.

Pure ASGI middlewares (no ``BaseHTTPMiddleware``) so that streaming and
WebSocket traffic is never buffered or wrapped needlessly.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import deque
from collections.abc import Callable

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


def account_max_sessions() -> int:
    """``SW_ACCOUNT_MAX_SESSIONS``: live sessions one account may own (default 100)."""
    return _env_int("SW_ACCOUNT_MAX_SESSIONS", 100)


def account_max_tokens() -> int:
    """``SW_ACCOUNT_MAX_TOKENS``: active tokens one account may hold (default 20)."""
    return _env_int("SW_ACCOUNT_MAX_TOKENS", 20)


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


DEFAULT_MCP_NEW_SESSIONS_PER_MINUTE = 30
_WINDOW_SECONDS = 60.0


def mcp_new_sessions_per_minute() -> int:
    """``SW_MCP_NEW_SESSIONS_PER_MINUTE``: new MCP transports per client IP."""
    return _env_int(
        "SW_MCP_NEW_SESSIONS_PER_MINUTE", DEFAULT_MCP_NEW_SESSIONS_PER_MINUTE
    )


class McpNewSessionLimitMiddleware:
    """Limit, per client IP, POSTs that open a new MCP transport.

    A POST without an ``mcp-session-id`` header is an ``initialize``: it makes
    the SDK allocate a transport. Calls that carry a session id (and GET/DELETE)
    are not counted. The client address is ``scope["client"]``, which uvicorn
    already resolves through the trusted proxy. Sliding window; refused
    requests are not counted. Memory is bounded: buckets whose hits have all
    aged out are dropped, and the table is swept when it grows past
    ``max_tracked_ips`` (the oldest bucket goes if it is still full).
    """

    def __init__(
        self,
        app: ASGIApp,
        per_minute: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        max_tracked_ips: int = 10_000,
    ) -> None:
        self.app = app
        self.per_minute = (
            per_minute if per_minute is not None else mcp_new_sessions_per_minute()
        )
        self._clock = clock
        self._max_tracked = max_tracked_ips
        self._hits: dict[str, deque[float]] = {}
        self._last_sweep = 0.0

    def _sweep(self, now: float) -> None:
        cutoff = now - _WINDOW_SECONDS
        for ip in [ip for ip, q in self._hits.items() if not q or q[-1] <= cutoff]:
            del self._hits[ip]

    def _allow(self, ip: str) -> float:
        """0.0 when allowed (and counted), else seconds until a slot frees."""
        now = self._clock()
        q = self._hits.get(ip)
        if q is None:
            if len(self._hits) >= self._max_tracked:
                if now - self._last_sweep >= 1.0:
                    self._last_sweep = now
                    self._sweep(now)
                if len(self._hits) >= self._max_tracked:
                    # Still full of live buckets (address flood): drop the oldest.
                    del self._hits[next(iter(self._hits))]
            q = self._hits[ip] = deque()
        cutoff = now - _WINDOW_SECONDS
        while q and q[0] <= cutoff:
            q.popleft()
        if len(q) >= self.per_minute:
            return max(q[0] + _WINDOW_SECONDS - now, 0.001)
        q.append(now)
        return 0.0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("method") != "POST":
            await self.app(scope, receive, send)
            return
        if any(name == b"mcp-session-id" for name, _ in scope.get("headers", [])):
            await self.app(scope, receive, send)
            return
        client = scope.get("client")
        retry_after = self._allow(client[0] if client else "")
        if retry_after == 0.0:
            await self.app(scope, receive, send)
            return
        body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32000,
                    "message": "too many new MCP sessions from this address; retry later",
                },
            }
        ).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 429,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"retry-after", str(max(1, math.ceil(retry_after))).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
