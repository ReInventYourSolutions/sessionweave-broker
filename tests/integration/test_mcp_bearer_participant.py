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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_non_participant_bearer_is_not_tried_as_participant_token() -> None:
    """An admin or creator credential as Bearer (the documented session_create
    path) is not a participant token: participant tools keep the plain
    not_in_session answer, and no auth failure is counted for the caller."""
    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with sw_client(base, headers={"Authorization": f"Bearer {ADMIN}"}) as a:
            created = await a.call_tool("session_create", {})
            sid = created["session_id"]
            # Far more calls than the per-(sid, ip) invalid-token budget.
            for _ in range(40):
                with pytest.raises(RuntimeError, match="call session_join first"):
                    await a.call_tool("session_list_participants", {"session_id": sid})
        creator = {"Authorization": f"Bearer {created['creator_token']}"}
        async with sw_client(base, headers=creator) as c:
            for _ in range(5):
                with pytest.raises(RuntimeError, match="call session_join first"):
                    await c.call_tool("session_list_participants", {"session_id": sid})
        # The caller's IP is not throttled: a real participant still gets in.
        async with sw_client(base) as j:
            joined = await j.call_tool(
                "session_join",
                {"session_id": sid, "join_token": created["join_token"],
                 "role": "w", "delivery_mode": "pull"})
        hdr = {"Authorization": f"Bearer {joined['participant_token']}"}
        async with sw_client(base, headers=hdr) as p:
            ok = await p.call_tool("session_list_participants", {"session_id": sid})
            assert ok["roster"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_revoked_or_expired_session_credentials_are_not_tried() -> None:
    """A revoked supervisor token or an expired join token of the session is
    still not a participant token: no auth failure is counted for it, so the
    caller keeps the plain not_in_session answer (never rate_limited)."""
    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with sw_client(base, headers={"Authorization": f"Bearer {ADMIN}"}) as a:
            created = await a.call_tool("session_create", {})
            sid = created["session_id"]
            tokens = await a.call_tool("session_list_supervisor_tokens", {"session_id": sid})
            await a.call_tool("session_revoke_supervisor_token", {
                "session_id": sid,
                "token_id": tokens["supervisor_tokens"][0]["token_id"]})
            # Removing a participant expires every join token of the session.
            joined = await a.call_tool(
                "session_join", {"session_id": sid, "join_token": created["join_token"],
                                 "role": "w", "delivery_mode": "pull"})
        async with sw_client(base, headers={"Authorization": f"Bearer {ADMIN}"}) as a:
            await a.call_tool("session_remove_participant",
                              {"session_id": sid, "participant_id": joined["participant_id"]})
        for cred in (created["supervisor_token"], created["join_token"]):
            async with sw_client(base, headers={"Authorization": f"Bearer {cred}"}) as c:
                # More calls than the per-(sid, ip) invalid-token budget.
                for _ in range(8):
                    with pytest.raises(RuntimeError, match="call session_join first"):
                        await c.call_tool("session_list_participants", {"session_id": sid})
