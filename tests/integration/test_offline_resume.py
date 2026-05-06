from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


async def _wait_for(
    predicate: Any, *, timeout: float = 3.0, message: str = "predicate not met"
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(message)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_specific_to_offline_target_buffers_and_delivers_on_resume() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool("session_join", {"session_id": sid, "role": "s"})
            async with sw_client(base) as b:
                pb = await b.call_tool(
                    "session_join", {"session_id": sid, "role": "r"}
                )
            # b is disconnected. Give the relay a moment to notice.
            await asyncio.sleep(0.5)

            send = await a.call_tool(
                "session_send",
                {
                    "dispatch": "specific",
                    "target": {"participant_ids": [pb["participant_id"]]},
                    "payload": {"q": 1},
                    "timeout_seconds": 60,
                },
            )
            cmd_id = send["command_id"]
            assert pb["participant_id"] in send["delivered_to"]

            async with sw_client(base) as b2:
                received_b: list[tuple[str, dict[str, Any]]] = []
                b2.on_notification(
                    lambda n, p: received_b.append((n, p))
                )
                await b2.call_tool(
                    "session_resume",
                    {
                        "session_id": sid,
                        "participant_id": pb["participant_id"],
                    },
                )
                await _wait_for(
                    lambda: any(
                        n == "command.received" and p["command_id"] == cmd_id
                        for n, p in received_b
                    )
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sender_disconnect_then_resume_sees_buffered_results() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as b:
            sid = (await b.call_tool("session_create", {}))["session_id"]
            pb = await b.call_tool(
                "session_join", {"session_id": sid, "role": "r"}
            )

            async with sw_client(base) as a:
                pa = await a.call_tool(
                    "session_join", {"session_id": sid, "role": "sender"}
                )
                send = await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pb["participant_id"]]},
                        "payload": {"q": 1},
                        "timeout_seconds": 60,
                    },
                )

            await asyncio.sleep(0.5)
            await b.call_tool(
                "session_respond",
                {
                    "command_id": send["command_id"],
                    "status": "ok",
                    "payload": {"r": 2},
                },
            )

            async with sw_client(base) as a2:
                received_a: list[tuple[str, dict[str, Any]]] = []
                a2.on_notification(
                    lambda n, p: received_a.append((n, p))
                )
                await a2.call_tool(
                    "session_resume",
                    {
                        "session_id": sid,
                        "participant_id": pa["participant_id"],
                    },
                )
                await _wait_for(
                    lambda: any(
                        n == "command.completed" for n, _ in received_a
                    )
                )
                assert any(n == "result.received" for n, _ in received_a)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_receiver_past_grace_evicted() -> None:
    async with relay_subprocess(
        env_overrides={"SW_DEFAULT_OFFLINE_GRACE_SECONDS": "1"}
    ) as base:
        async with sw_client(base) as a:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool("session_join", {"session_id": sid, "role": "s"})

            received_a: list[tuple[str, dict[str, Any]]] = []
            a.on_notification(lambda n, p: received_a.append((n, p)))

            async with sw_client(base) as b:
                await b.call_tool(
                    "session_join", {"session_id": sid, "role": "r"}
                )

            # b disconnected; wait past grace + tick (~1s grace + 1s tick)
            await _wait_for(
                lambda: any(
                    n == "participant.left" and p["reason"] == "offline_timeout"
                    for n, p in received_a
                ),
                timeout=6.0,
            )
            assert any(
                n == "participant.left" and p["reason"] == "offline_timeout"
                for n, p in received_a
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_resume_race_second_wins() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as a:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            pa = await a.call_tool(
                "session_join", {"session_id": sid, "role": "x"}
            )
        await asyncio.sleep(0.3)
        async with sw_client(base) as r1, sw_client(base) as r2:
            await r1.call_tool(
                "session_resume",
                {"session_id": sid, "participant_id": pa["participant_id"]},
            )
            await r2.call_tool(
                "session_resume",
                {"session_id": sid, "participant_id": pa["participant_id"]},
            )
            roster = await r2.call_tool("session_list_participants", {})
            assert roster["roster"][0]["participant_id"] == pa["participant_id"]
