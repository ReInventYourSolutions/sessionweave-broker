from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_brief_recipe_roundtrip() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=10) as c:
            sid = (await c.post("/v1/sessions",
                                json={"purpose": {"kind": "demo"}})).json()["session_id"]

            # Server participant joins (the brief's step 2).
            srv = (await c.post(f"/v1/sessions/{sid}/participants",
                                json={"role": "worker",
                                      "capabilities": ["respond"]})).json()
            srv_tok, srv_pid = srv["participant_token"], srv["participant_id"]

            # Client participant joins and dispatches a command (step 4, client variant).
            cli = (await c.post(f"/v1/sessions/{sid}/participants",
                                json={"role": "client", "capabilities": []})).json()
            cli_tok = cli["participant_token"]
            disp = await c.post(
                f"/v1/sessions/{sid}/commands",
                headers={"authorization": f"Bearer {cli_tok}"},
                json={"dispatch": "specific",
                      "target": {"participant_ids": [srv_pid]},
                      "payload": {"q": "ping"}},
            )
            disp.raise_for_status()

            # Server long-polls (step 3), drains command.received, responds (step 4).
            waited = await c.post(
                f"/v1/sessions/{sid}/participants/me/inbox/wait",
                headers={"authorization": f"Bearer {srv_tok}"},
                json={"timeout_seconds": 5},
            )
            waited.raise_for_status()
            msgs = waited.json()["messages"]
            cmd = next(m for m in msgs if m["name"] == "command.received")
            command_id = cmd["payload"]["command_id"]
            resp = await c.post(
                f"/v1/sessions/{sid}/commands/{command_id}/responses",
                headers={"authorization": f"Bearer {srv_tok}"},
                json={"status": "ok", "payload": {"a": "pong"}},
            )
            resp.raise_for_status()

            # Client long-polls and sees result.received.
            cwaited = await c.post(
                f"/v1/sessions/{sid}/participants/me/inbox/wait",
                headers={"authorization": f"Bearer {cli_tok}"},
                json={"timeout_seconds": 5},
            )
            cwaited.raise_for_status()
            names = [m["name"] for m in cwaited.json()["messages"]]
            assert "result.received" in names

            # Both leave with feedback (step 5).
            for tok in (srv_tok, cli_tok):
                r = await c.request(
                    "DELETE", f"/v1/sessions/{sid}/participants/me",
                    headers={"authorization": f"Bearer {tok}"},
                    json={"feedback": {"summary": "ok", "rating": 5}},
                )
                assert r.status_code // 100 == 2
