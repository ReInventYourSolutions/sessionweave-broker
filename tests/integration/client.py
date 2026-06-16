from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


@contextlib.asynccontextmanager
async def sw_client(base_url: str) -> AsyncIterator[Client]:
    """Open an MCP ClientSession over Streamable HTTP against the relay.

    The relay tunnels its application events through ``notifications/message``
    (LoggingMessageNotification) with ``logger == "sessionweave"`` and
    ``data == {"_name": ..., "_payload": ...}``. We register a
    ``logging_callback`` to extract those into the test client's
    ``on_notification`` hook.
    """
    url = f"{base_url}/mcp"
    # Generous timeout: stress tests (e.g. test_buffer_race fires 200 concurrent
    # dispatches) can back the single-process relay up past the 30s default on a
    # contended CI runner, tripping a ReadTimeout on teardown. This only relaxes
    # how long the test client waits — it does not mask server-side races.
    async with streamablehttp_client(url, timeout=120) as (read, write, _):
        client_holder: list[Client] = []

        async def logging_callback(params: Any) -> None:
            client = client_holder[0] if client_holder else None
            if client is None:
                return
            if getattr(params, "logger", None) != "sessionweave":
                return
            data = getattr(params, "data", None)
            if not isinstance(data, dict):
                return
            # Tests that need to inspect the raw envelope (e.g. to
            # assert ``_seq`` is present on MCP push) hook this side
            # channel; it fires once per LoggingMessageNotification
            # before the (name, payload) unwrap is dispatched.
            await client._dispatch_raw_logging_message(data)
            name = data.get("_name")
            payload = data.get("_payload") or {}
            if not isinstance(name, str):
                return
            if not isinstance(payload, dict):
                payload = {}
            await client._dispatch_notification(name, payload)

        async with ClientSession(
            read, write, logging_callback=logging_callback
        ) as session:
            await session.initialize()
            client = Client(session)
            client_holder.append(client)
            yield client


NotificationCallback = Callable[[str, dict[str, Any]], Awaitable[None] | None]


class Client:
    """Thin wrapper over MCP ClientSession returning tool results as dicts."""

    __slots__ = ("_callbacks", "_raw_callbacks", "_session")

    def __init__(self, session: ClientSession) -> None:
        self._session = session
        self._callbacks: list[NotificationCallback] = []
        self._raw_callbacks: list[Callable[[dict[str, Any]], Any]] = []

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

    def on_raw_logging_message(
        self, callback: Callable[[dict[str, Any]], Any]
    ) -> None:
        """Subscribe to the raw ``LoggingMessageNotification.params.data``
        dict before the ``_name`` / ``_payload`` unwrap. Use this when a
        test needs to inspect envelope fields like ``_seq`` that the
        higher-level ``on_notification`` hook discards.
        """
        self._raw_callbacks.append(callback)

    async def _dispatch_notification(
        self, method: str, params: dict[str, Any]
    ) -> None:
        for cb in list(self._callbacks):
            res = cb(method, params)
            if hasattr(res, "__await__"):
                await res  # type: ignore[misc]

    async def _dispatch_raw_logging_message(
        self, data: dict[str, Any]
    ) -> None:
        for cb in list(self._raw_callbacks):
            res = cb(data)
            if hasattr(res, "__await__"):
                await res


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
