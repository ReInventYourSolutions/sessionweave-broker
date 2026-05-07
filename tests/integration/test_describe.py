from __future__ import annotations

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_describe_returns_purpose_and_roster_without_joining() -> None:
    purpose = {
        "kind": "tic-tac-toe-v1",
        "your_role": "O",
        "your_capabilities": ["tictactoe-o"],
    }
    async with relay_subprocess() as base:
        async with sw_client(base) as host, sw_client(base) as scout:
            created = await host.call_tool(
                "session_create", {"purpose": purpose}
            )
            sid = created["session_id"]
            await host.call_tool(
                "session_join",
                {
                    "session_id": sid,
                    "role": "player",
                    "capabilities": ["tictactoe-x"],
                },
            )

            described = await scout.call_tool(
                "session_describe", {"session_id": sid}
            )
            assert described["session_id"] == sid
            assert described["purpose"] == purpose
            assert any(p["role"] == "player" for p in described["roster"])
            assert "expires_at" in described
            assert described["config"]["max_participants"] >= 2

            # describe must NOT bind the scout. They can still join freely.
            joined = await scout.call_tool(
                "session_join",
                {
                    "session_id": sid,
                    "role": "player",
                    "capabilities": ["tictactoe-o"],
                },
            )
            assert "participant_id" in joined


@pytest.mark.integration
@pytest.mark.asyncio
async def test_describe_unknown_session_errors() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as scout:
            with pytest.raises(Exception) as exc:
                await scout.call_tool(
                    "session_describe",
                    {"session_id": "00000000-0000-0000-0000-000000000000"},
                )
            assert "session_not_found" in str(exc.value)
