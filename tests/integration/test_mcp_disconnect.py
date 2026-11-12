"""What happens to a participant when its MCP client disconnects.

Under mcp 1.x a departing client was noticed only once the periodic presence
sweep found a handle that failed ``is_alive()``. Under 2.x two independent
mechanisms now notice, and these tests pin the user-visible outcome rather
than either mechanism:

* the eviction callback on ``connection.exit_stack``, which fires the moment
  the SDK tears the connection down and calls ``Session.handle_disconnect``;
* ``is_alive()``'s peek at the dispatcher's ``_closed`` flag, which the
  presence sweep acts on within a tick.

They are deliberately redundant, and each was verified to satisfy the first
test on its own with the other disabled — so a pass here does not attribute
the behaviour to either one. The unit tests do that:
``test_mcp_handle_registry_lifecycle.py`` pins the exit_stack path, and
``test_session_handle_disconnect.py`` pins what the callback then does.

What this file adds is the end-to-end property neither unit test can show:
that a real MCP client going away leaves the roster correct and the gauge
matching it.
"""
from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx
import pytest

from tests.integration.client import create_and_invite, sw_client
from tests.integration.harness import relay_subprocess


async def _wait_for(predicate: Any, timeout: float = 5.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return False


def _gauge(metrics_text: str, online: str) -> float:
    m = re.search(
        rf'^sessionweave_participants_active\{{online="{online}"\}} (\S+)$',
        metrics_text,
        re.M,
    )
    assert m is not None, f"participants_active{{online={online}}} not in /metrics"
    return float(m.group(1))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_client_disconnect_marks_participant_offline_promptly() -> None:
    """The offline grace is 300s, so nothing here is waiting it out: a
    departing client must be visible as offline to its peers in seconds."""
    async with relay_subprocess(
        env_overrides={"SW_DEFAULT_OFFLINE_GRACE_SECONDS": "300"}
    ) as base:
        async with sw_client(base) as watcher:
            sid, _creator, jt = await create_and_invite(watcher, {})
            await watcher.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "w"}
            )

            seen: list[tuple[str, dict[str, Any]]] = []
            watcher.on_notification(lambda n, p: seen.append((n, p)))

            async with sw_client(base) as leaver:
                await leaver.call_tool(
                    "session_join", {"session_id": sid, "join_token": jt, "role": "l"}
                )
            # leaver's context exit sends DELETE /mcp

            went_offline = await _wait_for(
                lambda: any(
                    n == "participant.updated"
                    and p["participant"]["role"] == "l"
                    and p["participant"]["online"] is False
                    for n, p in seen
                ),
                timeout=5.0,
            )

    assert went_offline, (
        "participant was not marked offline within 5s of its client "
        "disconnecting, while the offline grace was 300s — neither the "
        "connection-close callback nor the presence sweep noticed"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_disconnect_does_not_drift_participants_active() -> None:
    """The disconnect callback and the presence sweep can both act on the
    same departed participant. Whichever order they run in, the gauge must
    match reality — a drift here is invisible until an operator reads a
    number that is simply wrong.
    """
    async with relay_subprocess(
        env_overrides={"SW_DEFAULT_OFFLINE_GRACE_SECONDS": "300"}
    ) as base:
        async with sw_client(base) as watcher:
            sid, _creator, jt = await create_and_invite(watcher, {})
            await watcher.call_tool(
                "session_join", {"session_id": sid, "join_token": jt, "role": "w"}
            )

            async with sw_client(base) as leaver:
                await leaver.call_tool(
                    "session_join", {"session_id": sid, "join_token": jt, "role": "l"}
                )

            # Let the disconnect callback land, then give the presence
            # sweep a couple of ticks to act on the same participant.
            await asyncio.sleep(2.5)

            roster = (await watcher.call_tool("session_list_participants", {}))[
                "roster"
            ]
            async with httpx.AsyncClient(timeout=10.0) as c:
                metrics = (await c.get(f"{base}/metrics")).text

    live = sum(1 for p in roster if p["online"])
    gone = sum(1 for p in roster if not p["online"])

    assert live == 1 and gone == 1, roster
    assert _gauge(metrics, "true") == float(live), (
        "participants_active{online=true} drifted from the roster"
    )
    assert _gauge(metrics, "false") == float(gone), (
        "participants_active{online=false} drifted from the roster"
    )
