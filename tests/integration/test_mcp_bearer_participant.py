"""A launched agent's MCP calls carry its participant token only as the
``Authorization: Bearer`` header (the launcher writes it into the runtime's
MCP config). With no connection binding, the header identifies the caller
for the session named by ``session_id``."""
from __future__ import annotations

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess

ADMIN = "admin-bearer-test-token-0123456789"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_bearer_header_identifies_participant_without_binding() -> None:
    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with sw_client(base) as a:
            created = await a.call_tool("session_create", {})
            sid = created["session_id"]
            joined = await a.call_tool(
                "session_join",
                {"session_id": sid, "join_token": created["join_token"],
                 "role": "w", "delivery_mode": "pull"},
            )
            other = await a.call_tool("session_create", {})
            tok = joined["participant_token"]

            # Fresh connection, Bearer header only: acts as that participant.
            hdr = {"Authorization": f"Bearer {tok}"}
            async with sw_client(base, headers=hdr) as c:
                ok = await c.call_tool("session_list_participants", {"session_id": sid})
                assert joined["participant_id"] in {
                    r["participant_id"] for r in ok["roster"]}
                # The token is valid only for its own session.
                with pytest.raises(RuntimeError, match="not_in_session"):
                    await c.call_tool(
                        "session_list_participants", {"session_id": other["session_id"]})

            # Wrong Bearer: refused.
            bad = {"Authorization": "Bearer not-a-real-token"}
            async with sw_client(base, headers=bad) as c:
                with pytest.raises(RuntimeError, match="not_in_session"):
                    await c.call_tool("session_list_participants", {"session_id": sid})

            # No header, no explicit token: refused as before.
            async with sw_client(base) as c:
                with pytest.raises(RuntimeError, match="not_in_session"):
                    await c.call_tool("session_list_participants", {"session_id": sid})

            # An existing connection binding still wins over a header.
            async with sw_client(base, headers=bad) as d:
                j2 = await d.call_tool(
                    "session_join",
                    {"session_id": sid, "join_token": created["join_token"],
                     "role": "w2", "delivery_mode": "pull"},
                )
                out = await d.call_tool("session_list_participants", {"session_id": sid})
                assert j2["participant_id"] in {r["participant_id"] for r in out["roster"]}
