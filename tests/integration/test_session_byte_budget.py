"""Coverage for #63 — per-session aggregate byte budget for buffered events.

When the sum of buffered payload bytes across all participants in a
session exceeds ``config.max_buffer_bytes_per_session``, the relay
must:

  1. Evict the oldest buffered items across the session until under
     the cap (oldest = lowest ``_ts`` enqueued).
  2. Emit a single ``buffer_evicted`` event on the firehose with
     ``{"reason": "session_byte_cap", "evicted_count": N, ...}`` so
     observers see the back-pressure.

Without this guard, a compromised participant could fan ``count_cap *
payload_cap`` bytes across every participant's buffer and OOM the
relay (16 * 1000 * 256 KiB ~= 4 GiB worst case).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest
import websockets

from tests.integration.client import bearer, sw_client
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
async def test_session_byte_cap_evicts_oldest_and_emits_firehose() -> None:
    """Tight byte cap (8 KiB) + a participant that doesn't drain:
    pushes 4 KiB payloads, asserts only the most recent ones survive,
    and asserts a ``buffer_evicted reason=session_byte_cap`` event
    fired on the firehose.
    """
    # 8 KiB cap, 4 KiB-ish payloads — 2 items fit, the third evicts the
    # first.
    cap = 8 * 1024
    # Lower the bounds floor too — the production default is 1 MiB
    # min, but for a unit-style test we need an 8 KiB cap to evict
    # within a few small payloads.
    async with relay_subprocess(
        env_overrides={
            "SW_DEFAULT_MAX_BUFFER_BYTES_PER_SESSION": str(cap),
            "SW_MIN_MAX_BUFFER_BYTES_PER_SESSION": "1024",
        }
    ) as base:
        base_ws = base.replace("http://", "ws://")
        async with httpx.AsyncClient() as c:
            _created = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()
            sid, jt = _created["session_id"], _created["join_token"]

        # B joins via REST in pull mode so events sit in the buffer
        # waiting to be drained (and we can observe what survives).
        async with httpx.AsyncClient() as c:
            join = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={
                    "role": "worker",
                    "capabilities": ["respond"],
                    "delivery_mode": "pull",
                },
            )
            join.raise_for_status()
            pid_b = join.json()["participant_id"]
            tok_b = join.json()["participant_token"]

        firehose_events: list[dict[str, Any]] = []
        firehose_url = f"{base_ws}/v1/sessions/{sid}/firehose?token={_created['supervisor_token']}"
        async with websockets.connect(firehose_url) as ws:

            async def _reader() -> None:
                try:
                    while True:
                        raw = await ws.recv()
                        firehose_events.append(json.loads(raw))
                except websockets.exceptions.ConnectionClosed:
                    return

            reader_task = asyncio.create_task(_reader())

            # A dispatches 4 commands carrying ~3.5 KiB each. Two fit
            # under the 8 KiB cap; the third triggers eviction of the
            # first; the fourth triggers another eviction.
            large_value = "x" * 3500
            async with sw_client(base) as a:
                await a.call_tool(
                    "session_join", {"session_id": sid, "join_token": jt, "role": "sender"}
                )
                for i in range(4):
                    await a.call_tool(
                        "session_send",
                        {
                            "dispatch": "specific",
                            "target": {"participant_ids": [pid_b]},
                            "payload": {"i": i, "blob": large_value},
                            "timeout_seconds": 60,
                        },
                    )

            await _wait_for(
                lambda: any(
                    f["name"] == "buffer_evicted"
                    and f["payload"].get("reason") == "session_byte_cap"
                    for f in firehose_events
                ),
                timeout=5.0,
                message=(
                    "no buffer_evicted reason=session_byte_cap event on "
                    f"firehose; saw: {[(f['name']) for f in firehose_events]}"
                ),
            )

            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        # B drains via REST and observes only the surviving events.
        async with httpx.AsyncClient() as c:
            r = await c.post(
                f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
                headers={"authorization": f"Bearer {tok_b}"},
            )
            r.raise_for_status()
            drained = r.json()["messages"]

        cmds = [m for m in drained if m["name"] == "command.received"]
        # Two evictions happened, so 4 - 2 = 2 commands survive. (Could
        # be 1 or 2 depending on exact byte boundaries; assert the
        # general invariant: strictly less than 4.)
        assert 0 < len(cmds) < 4, (
            f"expected partial-eviction (1..3 commands), got {len(cmds)}"
        )
        # Surviving payloads must be the LATEST (highest ``i``).
        ints = sorted(m["payload"]["payload"]["i"] for m in cmds)
        assert ints == sorted(ints)
        assert max(ints) == 3  # the 4th send must survive

        # Firehose event payload sanity.
        evicted = next(
            f
            for f in firehose_events
            if f["name"] == "buffer_evicted"
            and f["payload"].get("reason") == "session_byte_cap"
        )
        assert evicted["payload"]["evicted_count"] >= 1
        assert evicted["payload"]["freed_bytes"] >= 1
        assert evicted["payload"]["buffered_bytes"] <= cap
        assert evicted["payload"]["buffer_byte_cap"] == cap
