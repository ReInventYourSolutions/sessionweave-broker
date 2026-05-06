from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


@contextlib.asynccontextmanager
async def sw_client(base_url: str) -> AsyncIterator[Client]:
    """Open an MCP ClientSession over Streamable HTTP against the relay."""
    url = f"{base_url}/mcp"
    async with streamablehttp_client(url) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield Client(session)


class Client:
    """Thin wrapper over MCP ClientSession returning tool results as dicts."""

    __slots__ = ("_session",)

    def __init__(self, session: ClientSession) -> None:
        self._session = session

    async def call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        result = await self._session.call_tool(name=name, arguments=args)
        if getattr(result, "isError", False):
            payload = _extract_text(result)
            raise RuntimeError(f"tool {name} returned error: {payload}")
        structured = getattr(result, "structuredContent", None)
        if structured is not None:
            if isinstance(structured, dict):
                unwrapped = structured.get("result", structured)
            else:
                unwrapped = structured
            if isinstance(unwrapped, dict):
                return unwrapped
            return {"result": unwrapped}
        if result.content:
            return _extract_text(result)
        return {}


def _extract_text(result: Any) -> dict[str, Any]:
    txt = result.content[0]
    text = getattr(txt, "text", None)
    if text is None:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text}
    return parsed if isinstance(parsed, dict) else {"result": parsed}
