from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import Notification
from pydantic import RootModel


class _PermissiveServerNotification(
    RootModel[Notification[dict[str, Any] | None, str]]
):
    """Accept any JSON-RPC notification regardless of method.

    The MCP ``ClientSession`` defaults to ``ServerNotification`` (a closed
    union of standard methods) and silently drops messages that fail
    validation. The relay sends application-level notifications such as
    ``participant.joined`` whose method strings are not in that union, so
    the test client substitutes this permissive type.
    """


@contextlib.asynccontextmanager
async def sw_client(base_url: str) -> AsyncIterator[Client]:
    """Open an MCP ClientSession over Streamable HTTP against the relay."""
    url = f"{base_url}/mcp"
    async with streamablehttp_client(url) as (read, write, _):
        client_holder: list[Client] = []

        async def message_handler(message: Any) -> None:
            client = client_holder[0] if client_holder else None
            if client is None:
                return
            root = getattr(message, "root", None)
            if root is None or not hasattr(root, "method"):
                return
            params = getattr(root, "params", None)
            params_dump: dict[str, Any]
            if params is None:
                params_dump = {}
            elif hasattr(params, "model_dump"):
                params_dump = params.model_dump(mode="json", by_alias=False)
            elif isinstance(params, dict):
                params_dump = params
            else:
                params_dump = {}
            await client._dispatch_notification(root.method, params_dump)

        async with ClientSession(
            read, write, message_handler=message_handler
        ) as session:
            session._receive_notification_type = _PermissiveServerNotification
            await session.initialize()
            client = Client(session)
            client_holder.append(client)
            yield client


NotificationCallback = Callable[[str, dict[str, Any]], Awaitable[None] | None]


class Client:
    """Thin wrapper over MCP ClientSession returning tool results as dicts."""

    __slots__ = ("_callbacks", "_session")

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._callbacks: list[NotificationCallback] = []

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

    def on_notification(self, callback: NotificationCallback) -> None:
        self._callbacks.append(callback)

    async def _dispatch_notification(
        self, method: str, params: dict[str, Any]
    ) -> None:
        for cb in list(self._callbacks):
            res = cb(method, params)
            if hasattr(res, "__await__"):
                await res  # type: ignore[misc]


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
