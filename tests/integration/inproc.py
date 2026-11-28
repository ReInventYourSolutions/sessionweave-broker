"""The relay in-process: REST and MCP clients over one ASGI app, no subprocess.

Lets a test call the same operation through REST and through an MCP tool
and compare the answers, reset the auth limiter between cases and capture
the relay's logs with ``caplog``.
"""
from __future__ import annotations

import asyncio
import contextlib
import re
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import httpx2
import pytest
from mcp import ClientSession

from sessionweave.server import build_app
from tests.integration.client import Client, sw_client

BASE = "http://127.0.0.1:8765"  # a loopback Host the MCP transport accepts
# "Error executing tool <name>: <code>: <message>"
_CODE = re.compile(r"^(?:Error executing tool [^:]+: )?([a-z][a-z_]+): ")


@dataclass
class InProc:
    app: Any
    http: httpx2.AsyncClient
    mcp: Callable[..., contextlib.AbstractAsyncContextManager[Client]]

    @property
    def deps(self) -> Any:
        return self.app.state.tool_deps

    def reset_auth_limiter(self) -> None:
        self.deps.auth_limiter._buckets.clear()


@contextlib.asynccontextmanager
async def inproc_relay(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str]
) -> AsyncIterator[InProc]:
    for key in ("SW_ADMIN_TOKEN", "SW_SESSION_CREATION", "SW_PERSISTENCE_DB_PATH"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    app = build_app()
    transport = httpx2.ASGITransport(app=app)

    def mcp(headers: dict[str, str] | None = None) -> Any:
        return sw_client(BASE, headers=headers, transport=transport)

    # The lifespan (and the MCP session manager's task group) runs in a task of
    # its own: an async fixture's setup and teardown run in different tasks,
    # and a cancel scope must be left by the task that entered it.
    started, stop = asyncio.Event(), asyncio.Event()

    async def lifespan() -> None:
        async with app.router.lifespan_context(app):
            started.set()
            await stop.wait()

    task = asyncio.create_task(lifespan())
    waiter = asyncio.create_task(started.wait())
    await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
    if task.done():
        waiter.cancel()
        task.result()  # raise the startup failure
    try:
        async with httpx2.AsyncClient(transport=transport, base_url=BASE, timeout=30) as http:
            yield InProc(app, http, mcp)
    finally:
        stop.set()
        await task


async def mcp_outcome(
    client: Client, tool: str, args: dict[str, Any]
) -> tuple[str | None, dict[str, Any]]:
    """``(None, result)`` on success, else ``(error_code, {"text": ...})``."""
    session: ClientSession = client._session
    result = await session.call_tool(name=tool, arguments=args)
    if not result.is_error:
        structured = result.structured_content
        out = structured.get("result", structured) if isinstance(structured, dict) else {}
        return None, out if isinstance(out, dict) else {"result": out}
    text = "".join(getattr(c, "text", "") for c in result.content)
    m = _CODE.search(text)
    return (m.group(1) if m else "unknown"), {"text": text}


def rest_outcome(resp: httpx2.Response) -> tuple[str | None, dict[str, Any]]:
    """``(None, json)`` on 2xx (``{}`` for an empty body), else ``(code, json)``."""
    if resp.is_success:
        return None, (resp.json() if resp.content else {})
    body = resp.json()
    err = body.get("error")
    code = err.get("code") if isinstance(err, dict) else err
    return str(code), body
