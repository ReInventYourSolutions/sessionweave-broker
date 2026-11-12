from __future__ import annotations

import pytest

from tests.integration.client import create_and_invite, sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_full_rejected() -> None:
    async with relay_subprocess() as base:
        async with (
            sw_client(base) as a,
            sw_client(base) as b,
            sw_client(base) as c,
        ):
            sid, _creator, jt = await create_and_invite(a, {"config": {"max_participants": 2}})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt})
            await b.call_tool("session_join", {"session_id": sid, "join_token": jt})
            with pytest.raises(Exception) as exc:
                await c.call_tool("session_join", {"session_id": sid, "join_token": jt})
            assert "session_full" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_payload_too_large_rejected() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            # Use the minimum allowed payload bound (1 KiB) so the relay
            # accepts the override, then send 4 KiB to trip the limit.
            sid, _creator, jt = await create_and_invite(a, {"config": {"max_payload_bytes": 1024}})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt, "role": "s"})
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "r"}
            )
            with pytest.raises(Exception) as exc:
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pb["participant_id"]]},
                        "payload": {"x": "y" * 4096},
                    },
                )
            assert "payload_too_large" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dispatch_all_without_target_rejected() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a:
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool("session_join", {"session_id": sid, "join_token": jt})
            with pytest.raises(Exception) as exc:
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "all",
                        "target": {},
                        "payload": {},
                    },
                )
            assert "dispatch" in str(exc.value).lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_not_found_rejected() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a:
            with pytest.raises(Exception) as exc:
                await a.call_tool(
                    "session_join",
                    {
                        "session_id": "00000000-0000-0000-0000-000000000000",
                        "join_token": "whatever",
                    },
                )
            assert "session_not_found" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_send_without_join_rejected() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a:
            with pytest.raises(Exception) as exc:
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "all",
                        "target": {"role": "any"},
                        "payload": {},
                    },
                )
            assert "not_in_session" in str(exc.value)
