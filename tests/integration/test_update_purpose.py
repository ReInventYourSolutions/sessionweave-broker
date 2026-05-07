from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_purpose_round_trip_and_notification() -> None:
    initial = {"kind": "demo", "step": 1}
    updated = {"kind": "demo", "step": 2, "your_role": "B"}

    async with relay_subprocess() as base:
        async with sw_client(base) as host, sw_client(base) as guest:
            created = await host.call_tool(
                "session_create", {"purpose": initial}
            )
            sid = created["session_id"]

            await host.call_tool(
                "session_join", {"session_id": sid, "role": "A"}
            )

            received: list[tuple[str, dict[str, Any]]] = []
            guest.on_notification(lambda n, p: received.append((n, p)))
            await guest.call_tool(
                "session_join", {"session_id": sid, "role": "B"}
            )

            # Pre-update describe agrees with the initial value.
            described = await guest.call_tool(
                "session_describe", {"session_id": sid}
            )
            assert described["purpose"] == initial

            # Host updates the purpose mid-session.
            ack = await host.call_tool(
                "session_update_purpose", {"purpose": updated}
            )
            assert ack["purpose"] == updated

            # describe now reflects the new payload.
            described2 = await guest.call_tool(
                "session_describe", {"session_id": sid}
            )
            assert described2["purpose"] == updated

            # Guest must observe the broadcast event.
            for _ in range(20):
                if any(n == "session.purpose_updated" for n, _ in received):
                    break
                await asyncio.sleep(0.1)
            evt = next(
                (p for n, p in received if n == "session.purpose_updated"),
                None,
            )
            assert evt is not None
            assert evt == {"purpose": updated}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_purpose_requires_membership() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as host, sw_client(base) as outsider:
            sid = (
                await host.call_tool(
                    "session_create", {"purpose": {"k": 1}}
                )
            )["session_id"]
            await host.call_tool(
                "session_join", {"session_id": sid, "role": "A"}
            )
            with pytest.raises(Exception) as exc:
                await outsider.call_tool(
                    "session_update_purpose", {"purpose": {"k": 2}}
                )
            assert "not_in_session" in str(exc.value)
