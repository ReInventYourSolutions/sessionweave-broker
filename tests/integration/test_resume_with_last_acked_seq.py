"""Coverage for issue #48 items 2 + 3 — resume body field + eager buffer trim.

Two behaviours:

1. ``session_resume`` (MCP) accepts an optional ``last_acked_seq``
   field in its request. When set, the relay filters the push-mode
   buffer replay to skip events with ``seq <= last_acked_seq``.

2. The relay maintains an authoritative ``Participant.last_acked_seq``
   watermark, advanced by an upstream ``{"ack": N}`` frame on the WS
   channel (via ``Session.record_ack``). The watermark eagerly trims
   the per-participant buffer and seeds the filter for subsequent
   ``check_inbox`` / WS attach / resume drains so a client doesn't
   need to retransmit the watermark on every reconnect channel.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.client import bearer, create_and_invite, sw_client
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
async def test_check_inbox_respects_authoritative_ack_watermark() -> None:
    """After an upstream WS ``{"ack": N}`` advances the participant's
    server-side ``last_acked_seq``, a subsequent REST ``inbox/drain``
    on the same participant — even WITHOUT passing
    ``?last_acked_seq=N`` — must filter out the already-ACKed
    events.

    Closes the parallel-channel observability gap from #48 item 3:
    one connection ACKs, another connection (REST drain, fresh WS,
    or session_resume push) inherits the watermark.
    """
    async with relay_subprocess() as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            _created = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()
            sid, jt = _created["session_id"], _created["join_token"]

        # B joins via REST so we have a token + pid.
        async with httpx.AsyncClient() as c:
            join = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "worker", "capabilities": ["respond"]},
            )
            join.raise_for_status()
            tok_b = join.json()["participant_token"]
            pid_b = join.json()["participant_id"]

        async with sw_client(base) as a:
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )
            for i in range(5):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_b]},
                        "payload": {"i": i},
                        "timeout_seconds": 30,
                    },
                )

            ws_url = (
                f"{base_ws}/v1/sessions/{sid}/participants/me/events?token={tok_b}"
            )
            async with websockets.connect(ws_url) as ws:
                seqs: list[int] = []
                for _ in range(3):
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    seqs.append(json.loads(raw)["seq"])
                ack = max(seqs)
                await ws.send(json.dumps({"ack": ack}))
                # Settle so record_ack lands before the WS closes.
                await asyncio.sleep(0.2)

            # Now drain via REST without passing ?last_acked_seq. The
            # participant's authoritative watermark must filter the
            # already-ACKed events out of the response.
            async with httpx.AsyncClient() as c:
                r = await c.post(
                    f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
                    headers={"authorization": f"Bearer {tok_b}"},
                )
                r.raise_for_status()
                drained = r.json()

            for msg in drained["messages"]:
                assert msg["seq"] > ack, (
                    f"REST drain returned a message at seq={msg['seq']} "
                    f"<= ack={ack} without the caller passing "
                    "?last_acked_seq; authoritative watermark was not honoured"
                )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_session_resume_body_field_last_acked_seq_filters_push_replay() -> None:
    """``session_resume`` (MCP) accepts ``last_acked_seq`` and the
    push-mode buffer replay filters out events with
    ``seq <= last_acked_seq``.

    Closes the parity gap from #48 item 2: REST WS attach already
    accepted ``?last_acked_seq=N``; ``session_resume`` was the
    inconsistency.
    """
    async with relay_subprocess() as base:
        # B joins via MCP (push-mode by default) and gets a
        # participant_id. We then disconnect by exiting its sw_client
        # context. While offline, A dispatches 5 commands which get
        # buffered for B.
        async with sw_client(base) as a:
            sid, _creator, jt = await create_and_invite(a, {})
            await a.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
            )

            async with sw_client(base) as b:
                pb = await b.call_tool(
                    "session_join",
                    {
                        "session_id": sid, "join_token": jt,
                        "role": "worker",
                        "capabilities": ["respond"],
                    },
                )

            # B is offline (sw_client exited). Buffer fills up.
            for i in range(5):
                await a.call_tool(
                    "session_send",
                    {
                        "dispatch": "specific",
                        "target": {"participant_ids": [pb["participant_id"]]},
                        "payload": {"i": i},
                        "timeout_seconds": 60,
                    },
                )

            # B reconnects via session_resume with last_acked_seq=3 —
            # the relay must filter the push-replay to skip seq<=3.
            # We collect what reaches B via the MCP notification
            # callback. Note: seq order is deterministic because the
            # 5 sends are sequential, so seqs 1..5 are command.received
            # for the 5 commands (B's only events).
            async with sw_client(base) as b2:
                received: list[tuple[str, dict[str, Any]]] = []
                b2.on_notification(lambda n, p: received.append((n, p)))
                await b2.call_tool(
                    "session_resume",
                    {
                        "session_id": sid,
                        "participant_token": pb["participant_token"],
                        "last_acked_seq": 3,
                    },
                )
                # Give the push-replay a moment to deliver.
                await _wait_for(
                    lambda: sum(
                        1 for n, _ in received if n == "command.received"
                    )
                    >= 2,
                    timeout=3.0,
                    message=(
                        "expected 2 surviving command.received after "
                        "filtering seq<=3"
                    ),
                )

            cmd_count = sum(
                1 for n, _ in received if n == "command.received"
            )
            # Exactly the 2 events with seq > 3 (i.e. the 4th and 5th
            # dispatched commands).
            assert cmd_count == 2, (
                f"expected exactly 2 surviving command.received, "
                f"got {cmd_count}: {received}"
            )
            payloads = sorted(
                p["payload"]["i"]
                for n, p in received
                if n == "command.received"
            )
            assert payloads == [3, 4], (
                f"expected the 4th and 5th dispatched commands, got: {payloads}"
            )
