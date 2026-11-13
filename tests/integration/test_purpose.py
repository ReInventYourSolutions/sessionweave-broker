from __future__ import annotations

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_purpose_round_trips_through_create_and_join() -> None:
    purpose = {
        "kind": "tic-tac-toe-v1",
        "your_role": "O",
        "your_capabilities": ["tictactoe-o"],
        "instructions": "X moves first.",
    }
    async with relay_subprocess() as base:
        async with sw_client(base) as creator, sw_client(base) as joiner:
            created = await creator.call_tool(
                "session_create", {"purpose": purpose}
            )
            assert created["purpose"] == purpose
            joined = await joiner.call_tool(
                "session_join",
                {
                    "session_id": created["session_id"],
                    "join_token": created["join_token"],
                    "role": "player",
                    "capabilities": ["tictactoe-o"],
                },
            )
            assert joined["purpose"] == purpose
            # And resume after disconnect echoes it back too:
            ptoken = joined["participant_token"]

        async with sw_client(base) as joiner2:
            resumed = await joiner2.call_tool(
                "session_resume",
                {
                    "session_id": created["session_id"],
                    "participant_token": ptoken,
                },
            )
            assert resumed["purpose"] == purpose


@pytest.mark.integration
@pytest.mark.asyncio
async def test_purpose_can_be_omitted() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as creator:
            created = await creator.call_tool("session_create", {})
            assert created["purpose"] is None
