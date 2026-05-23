"""Coverage for #65 — relay_epoch on join/resume/info.

The relay returns ``relay_epoch`` (its startup UNIX-seconds time) on
three surfaces:

  - ``GET /v1/info`` so external monitors poll for relay restarts.
  - ``session_join`` output so a fresh client records the epoch.
  - ``session_resume`` output so a reconnecting client can compare.

All three surfaces must return the same value for a given relay
process. Two different relay subprocesses must return different
values (the test simulates restart by spinning up two relays in
sequence).
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relay_epoch_is_consistent_across_surfaces() -> None:
    """``/v1/info.relay_epoch == session_join.relay_epoch ==
    session_resume.relay_epoch`` for one relay process.
    """
    async with relay_subprocess() as base:
        # 1. Info endpoint.
        async with httpx.AsyncClient() as c:
            info = (await c.get(f"{base}/v1/info")).json()
        info_epoch = info["relay_epoch"]
        assert isinstance(info_epoch, int)
        assert info_epoch > 0

        async with sw_client(base) as a:
            # 2. Join output carries the same epoch.
            sid = (await a.call_tool("session_create", {}))["session_id"]
            join_out = await a.call_tool(
                "session_join", {"session_id": sid, "role": "x"}
            )
            assert join_out.get("relay_epoch") == info_epoch
            pid = join_out["participant_id"]

        # 3. Resume output carries the same epoch (on a fresh transport).
        async with sw_client(base) as a2:
            resume_out = await a2.call_tool(
                "session_resume",
                {"session_id": sid, "participant_id": pid},
            )
            assert resume_out.get("relay_epoch") == info_epoch


@pytest.mark.integration
@pytest.mark.asyncio
async def test_relay_epoch_changes_across_process_restarts() -> None:
    """Two relay subprocess instances must return different
    ``relay_epoch`` values. Simulates the operator-restart scenario
    that motivates the field (audit session ``8916f1cc-...``).
    """
    epochs: list[int] = []
    for i in range(2):
        if i > 0:
            # relay_epoch is int(time.time()) — on fast CI runners the
            # harness teardown + respawn can complete inside one wall
            # second, which collides the two epochs and trips this
            # assertion. Sleep past the second boundary to keep the
            # restart-detection contract testable deterministically.
            await asyncio.sleep(1.1)
        async with relay_subprocess() as base:
            async with httpx.AsyncClient() as c:
                info = (await c.get(f"{base}/v1/info")).json()
            epochs.append(info["relay_epoch"])

    assert epochs[0] != epochs[1], (
        f"both relay subprocesses returned the same relay_epoch "
        f"{epochs[0]} — restart-detection is broken"
    )
