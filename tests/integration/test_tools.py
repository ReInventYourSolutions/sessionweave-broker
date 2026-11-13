from __future__ import annotations

import pytest

from tests.integration.client import create_and_invite, sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_create_returns_session_id() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as client:
            result = await client.call_tool("session_create", {})
            assert "session_id" in result
            assert "expires_at" in result
            assert result["config"]["max_participants"] == 16


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_create_with_overrides() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as client:
            result = await client.call_tool(
                "session_create", {"config": {"max_participants": 4}}
            )
            assert result["config"]["max_participants"] == 4


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_join_returns_participant_id_and_roster() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as client_a, sw_client(base) as client_b:
            sid, _creator, jt = await create_and_invite(client_a, {})
            join = await client_b.call_tool(
                "session_join",
                {
                    "session_id": sid, "join_token": jt,
                    "role": "reviewer",
                    "capabilities": ["sec"],
                },
            )
            assert "participant_id" in join
            assert any(
                p["participant_id"] == join["participant_id"]
                for p in join["roster"]
            )
            assert join["config"]["max_participants"] == 16


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_send_specific_returns_command_id() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as sender, sw_client(base) as recipient:
            sid, _creator, jt = await create_and_invite(sender, {})
            sender_join = await sender.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "lead"},
            )
            recv_join = await recipient.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "reviewer", "capabilities": ["sec"]},
            )
            send_result = await sender.call_tool(
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
            assert "command_id" in send_result
            assert send_result["delivered_to"] == [recv_join["participant_id"]]
            # sender should be in the roster, distinct from recipient
            assert sender_join["participant_id"] != recv_join["participant_id"]
