"""Integration tests for the ``session_wait_for_event`` MCP tool.

Spins the relay as a subprocess and drives it through the Streamable-HTTP
MCP transport using the shared ``sw_client`` / ``relay_subprocess`` helpers.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_event_mcp_wakes_on_send() -> None:
    """Client A long-polls; Client B sends a targeted command; A wakes before timeout."""
    async with relay_subprocess() as base:
        async with sw_client(base) as client_a, sw_client(base) as client_b:
            sid = (await client_a.call_tool("session_create", {}))["session_id"]

            # Both join in pull mode.
            join_a = await client_a.call_tool(
                "session_join",
                {"session_id": sid, "role": "waiter", "delivery_mode": "pull"},
            )
            pid_a = join_a["participant_id"]

            join_b = await client_b.call_tool(
                "session_join",
                {"session_id": sid, "role": "sender", "delivery_mode": "pull"},
            )
            pid_b = join_b["participant_id"]
            assert pid_a != pid_b

            # Drain client_a's inbox so the participant.joined notification
            # (emitted when client_b joined) does not satisfy the wait early.
            await client_a.call_tool("session_check_inbox", {"session_id": sid})

            waiter_result: dict[str, Any] = {}

            async def waiter() -> None:
                out = await client_a.call_tool(
                    "session_wait_for_event",
                    {"session_id": sid, "timeout_seconds": 5},
                )
                waiter_result.update(out)

            async def sender() -> None:
                # Allow the waiter coroutine time to start blocking.
                await asyncio.sleep(0.3)
                await client_b.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_a]},
                        "payload": {"hi": True},
                        "timeout_seconds": 10,
                    },
                )

            await asyncio.gather(waiter(), sender())

            assert waiter_result["timed_out"] is False
            assert any(
                m["name"] == "command.received"
                for m in waiter_result["messages"]
            ), f"expected command.received, got: {waiter_result['messages']}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_event_mcp_returns_timed_out() -> None:
    """Returns ``timed_out: true`` with empty messages when no event arrives."""
    async with relay_subprocess() as base:
        async with sw_client(base) as client:
            sid = (await client.call_tool("session_create", {}))["session_id"]
            await client.call_tool(
                "session_join",
                {"session_id": sid, "role": "lone", "delivery_mode": "pull"},
            )

            out = await client.call_tool(
                "session_wait_for_event",
                {"session_id": sid, "timeout_seconds": 0.3},
            )

            assert out["timed_out"] is True
            assert out["messages"] == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_wait_for_event_mcp_rejects_push_mode() -> None:
    """Push-mode participant cannot call session_wait_for_event."""
    async with relay_subprocess() as base:
        async with sw_client(base) as client:
            sid = (await client.call_tool("session_create", {}))["session_id"]
            await client.call_tool(
                "session_join",
                {"session_id": sid, "role": "pusher", "delivery_mode": "push"},
            )

            with pytest.raises(RuntimeError, match="invalid_delivery_mode"):
                await client.call_tool(
                    "session_wait_for_event",
                    {"session_id": sid, "timeout_seconds": 1},
                )
