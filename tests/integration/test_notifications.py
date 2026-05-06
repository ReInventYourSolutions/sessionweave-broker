from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_join_emits_participant_joined_to_others() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool("session_join", {"session_id": sid, "role": "x"})

            received: list[tuple[str, dict[str, Any]]] = []
            a.on_notification(
                lambda name, params: received.append((name, params))
            )

            await b.call_tool(
                "session_join", {"session_id": sid, "role": "y"}
            )

            for _ in range(20):
                if any(name == "participant.joined" for name, _ in received):
                    break
                await asyncio.sleep(0.1)

            names = [n for n, _ in received]
            assert "participant.joined" in names


@pytest.mark.integration
@pytest.mark.asyncio
async def test_send_specific_emits_command_received_to_recipient() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as sender, sw_client(base) as recipient:
            sid = (await sender.call_tool("session_create", {}))["session_id"]
            await sender.call_tool(
                "session_join", {"session_id": sid, "role": "lead"}
            )
            recv_join = await recipient.call_tool(
                "session_join",
                {"session_id": sid, "role": "reviewer", "capabilities": ["sec"]},
            )

            received: list[tuple[str, dict[str, Any]]] = []
            recipient.on_notification(
                lambda name, params: received.append((name, params))
            )

            await sender.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {
                        "participant_ids": [recv_join["participant_id"]]
                    },
                    "payload": {"hello": "world"},
                    "timeout_seconds": 5,
                },
            )

            for _ in range(20):
                if any(n == "command.received" for n, _ in received):
                    break
                await asyncio.sleep(0.1)

            cmds = [(n, p) for n, p in received if n == "command.received"]
            assert cmds, f"expected command.received, got: {received}"
            _, params = cmds[0]
            assert params["payload"] == {"hello": "world"}
            assert params["dispatch_mode"] == "specific"
