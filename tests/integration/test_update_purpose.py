from __future__ import annotations

import asyncio
from typing import Any

import pytest

from tests.integration.client import create_and_invite, sw_client
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_update_purpose_round_trip_and_notification() -> None:
    initial = {"kind": "demo", "step": 1}
    updated = {"kind": "demo", "step": 2, "your_role": "B"}

    async with relay_subprocess() as base:
        async with sw_client(base) as host, sw_client(base) as guest:
            sid, creator, jt = await create_and_invite(host, purpose=initial)

            await host.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "A"},
            )

            received: list[tuple[str, dict[str, Any]]] = []
            guest.on_notification(lambda n, p: received.append((n, p)))
            await guest.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "B"},
            )

            # Pre-update describe agrees with the initial value.
            described = await guest.call_tool(
                "session_describe", {"session_id": sid, "access_token": jt}
            )
            assert described["purpose"] == initial

            # Host updates the purpose mid-session.
            ack = await host.call_tool(
                "session_update_purpose",
                {"session_id": sid, "creator_token": creator, "purpose": updated},
            )
            assert ack["purpose"] == updated

            # describe now reflects the new payload.
            described2 = await guest.call_tool(
                "session_describe", {"session_id": sid, "access_token": jt}
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
async def test_update_purpose_is_creator_only() -> None:
    async with relay_subprocess() as base:
        async with sw_client(base) as host, sw_client(base) as guest:
            sid, creator, jt = await create_and_invite(host, purpose={"k": 1})
            await guest.call_tool(
                "session_join",
                {"session_id": sid, "join_token": jt, "role": "A"},
            )
            # A participant (or a bare join token) may not change the purpose,
            # and the session id is not a credential.
            for bad in (jt, sid):
                with pytest.raises(Exception) as exc:
                    await guest.call_tool(
                        "session_update_purpose",
                        {"session_id": sid, "creator_token": bad, "purpose": {"k": 2}},
                    )
                assert "permission_denied" in str(exc.value)
            ok = await host.call_tool(
                "session_update_purpose",
                {"session_id": sid, "creator_token": creator, "purpose": {"k": 3}},
            )
            assert ok["purpose"] == {"k": 3}
