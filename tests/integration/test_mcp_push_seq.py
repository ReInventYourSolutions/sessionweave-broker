"""Coverage for issue #48 item 1 — MCP push transport now carries ``seq``.

#43 introduced per-event seq + last_acked_seq for the WS transport,
but the MCP push channel (``Connections.send`` → ``MCPConnectionHandle``)
didn't surface seq on the wire — so MCP-driven clients couldn't dedup
on reconnect. This module verifies the field now rides in the
LoggingMessageNotification envelope at ``data._seq`` so MCP clients
have what they need to drive a ``session_resume?last_acked_seq=N``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


async def _wait_for(
    predicate: Any, *, timeout: float = 5.0, message: str = "predicate not met"
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(message)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_push_envelope_carries_seq() -> None:
    """Live MCP push delivery: every ``notifications/message`` log
    entry the relay emits to a participant carries ``data._seq``
    alongside ``data._name`` and ``data._payload``. The seq is
    strictly monotonic per-participant and starts at 1.

    The sw_client harness surfaces relay notifications as
    ``on_notification(name, payload)`` after the
    ``LoggingMessageNotification`` envelope has been unwrapped, but
    we can dig back into the raw frames via the harness's
    underlying queue. Easier path: read ``ListMcpResources``? No —
    just dispatch a couple of commands and assert the payloads each
    carry a monotonic seq.

    Implementation note: the harness's ``on_notification`` callback
    receives unwrapped (name, payload). To assert ``seq`` is also
    exposed, we use the underlying MCP client's logging-message
    handler to capture the full envelope.
    """
    async with relay_subprocess() as base:
        async with sw_client(base) as a, sw_client(base) as b:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )
            pb = await b.call_tool(
                "session_join",
                {
                    "session_id": sid,
                    "role": "worker",
                    "capabilities": ["respond"],
                },
            )

            # Capture the full LoggingMessageNotification data dicts B
            # receives. The harness already routes ``notifications/message``
            # frames into ``on_notification`` by extracting
            # ``data._name`` / ``data._payload``; we hook one level
            # earlier here.
            raw_envelopes: list[dict[str, Any]] = []
            b.on_raw_logging_message(
                lambda data: raw_envelopes.append(dict(data))
            )

            for i in range(3):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pb["participant_id"]]},
                        "payload": {"i": i},
                        "timeout_seconds": 30,
                    },
                )

            await _wait_for(
                lambda: sum(
                    1
                    for env in raw_envelopes
                    if env.get("_name") == "command.received"
                )
                == 3,
                timeout=5.0,
                message="not all 3 MCP push events arrived",
            )

            command_envelopes = [
                env
                for env in raw_envelopes
                if env.get("_name") == "command.received"
            ]
            seqs = [env.get("_seq") for env in command_envelopes]
            assert all(isinstance(s, int) and s > 0 for s in seqs), (
                f"_seq missing or non-positive on MCP push envelope: {seqs}"
            )
            assert seqs == sorted(seqs), (
                f"seqs not monotonic in delivery order: {seqs}"
            )
            assert len(set(seqs)) == 3, f"duplicate seq observed: {seqs}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_resume_with_last_acked_seq_filters_replay() -> None:
    """MCP client consumes events 1..N over its first connection,
    captures the highest seq from the envelope, disconnects, then
    reconnects via ``session_resume`` with that ``last_acked_seq``.
    The replay must not re-deliver the already-consumed events.

    This is the same recipe as ``test_session_resume_body_field_...``
    in PR #52, but uses the seq the client observed ON THE WIRE
    (not a hardcoded number) — which is the realistic scenario for
    a production MCP client.
    """
    async with relay_subprocess() as base:
        async with sw_client(base) as a:
            sid = (await a.call_tool("session_create", {}))["session_id"]
            await a.call_tool(
                "session_join", {"session_id": sid, "role": "sender"}
            )

            # B joins, consumes some events via push, captures seq,
            # disconnects.
            captured_seq: int = 0
            async with sw_client(base) as b:
                pb = await b.call_tool(
                    "session_join",
                    {
                        "session_id": sid,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )
                envelopes: list[dict[str, Any]] = []
                b.on_raw_logging_message(
                    lambda data: envelopes.append(dict(data))
                )

                for i in range(3):
                    await a.call_tool(
                        "session_send",
                        {
                            "dispatch": "specific",
                            "target": {"participant_ids": [pb["participant_id"]]},
                            "payload": {"i": i},
                            "timeout_seconds": 30,
                        },
                    )

                await _wait_for(
                    lambda: sum(
                        1
                        for env in envelopes
                        if env.get("_name") == "command.received"
                    )
                    == 3,
                    timeout=5.0,
                )
                captured_seq = max(
                    env["_seq"]
                    for env in envelopes
                    if env.get("_name") == "command.received"
                    and env.get("_seq") is not None
                )

            # B is offline now. A dispatches 2 more commands, which
            # get buffered for B.
            for i in range(3, 5):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pb["participant_id"]]},
                        "payload": {"i": i},
                        "timeout_seconds": 60,
                    },
                )

            # B reconnects with last_acked_seq=captured_seq. The
            # buffered events from the previous round were captured;
            # only the 2 new ones should arrive over the resume push.
            async with sw_client(base) as b2:
                got: list[tuple[str, dict[str, Any]]] = []
                b2.on_notification(lambda n, p: got.append((n, p)))
                await b2.call_tool(
                    "session_resume",
                    {
                        "session_id": sid,
                        "participant_id": pb["participant_id"],
                        "last_acked_seq": captured_seq,
                    },
                )
                await _wait_for(
                    lambda: sum(
                        1 for n, _ in got if n == "command.received"
                    )
                    >= 2,
                    timeout=3.0,
                    message="expected the 2 NEW command.received after resume",
                )

            cmd_received = [p for n, p in got if n == "command.received"]
            assert len(cmd_received) == 2, (
                f"expected exactly 2 surviving command.received, got "
                f"{len(cmd_received)}: {got}"
            )
            payloads = sorted(p["payload"]["i"] for p in cmd_received)
            assert payloads == [3, 4], (
                f"expected commands i=3 and i=4 (the new ones), got: {payloads}"
            )
