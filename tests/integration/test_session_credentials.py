"""Creation hand-back, join/supervisor minting, revocation and /access."""
from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from tests.integration.harness import relay_subprocess

ADMIN = "admin-secret-for-tests"


def _h(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_hands_back_credentials_and_roles() -> None:
    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{base}/v1/sessions", json={})
            assert r.status_code == 201, r.text
            out = r.json()
            sid = out["session_id"]
            for key in (
                "creator_token", "supervisor_token", "supervisor_token_id",
                "join_token", "join_token_expires_at", "invite_url", "invite_link",
            ):
                assert out[key], key
            assert out["invite_url"] == f"{base}/invite/{sid}?jt={out['join_token']}"
            link = urlparse(out["invite_link"])
            assert link.scheme == "swv"
            assert parse_qs(link.query)["jt"] == [out["join_token"]]

            sid_path = f"{base}/v1/sessions/{sid}"
            roles = {
                out["creator_token"]: "creator",
                out["supervisor_token"]: "supervisor",
                out["join_token"]: "joiner",
                ADMIN: "admin",
            }
            for tok, role in roles.items():
                a = await c.get(f"{sid_path}/access", headers=_h(tok))
                assert a.status_code == 200, a.text
                assert a.json() == {"role": role, "participant_id": None}
            bad = await c.get(f"{sid_path}/access", headers=_h("nope"))
            assert bad.status_code == 403
            noauth = await c.get(f"{sid_path}/access")
            assert noauth.status_code in (401, 403)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mint_list_revoke() -> None:
    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            out = (await c.post(f"{base}/v1/sessions", json={})).json()
            sid, creator = out["session_id"], out["creator_token"]
            p = f"{base}/v1/sessions/{sid}"

            r = await c.post(f"{p}/join-tokens", headers=_h(creator))
            assert r.status_code == 201, r.text
            jt = r.json()
            assert jt["join_token"] != out["join_token"]
            assert jt["invite_url"].endswith(f"/invite/{sid}?jt={jt['join_token']}")
            assert jt["invite_link"].startswith("swv://")
            assert jt["expires_at"]
            a = await c.get(f"{p}/access", headers=_h(jt["join_token"]))
            assert a.json()["role"] == "joiner"

            # admin may mint too
            r = await c.post(f"{p}/join-tokens", headers=_h(ADMIN))
            assert r.status_code == 201

            r = await c.post(f"{p}/supervisor-tokens", headers=_h(creator))
            assert r.status_code == 201, r.text
            sup = r.json()
            assert set(sup) == {"token_id", "supervisor_token"}
            a = await c.get(f"{p}/access", headers=_h(sup["supervisor_token"]))
            assert a.json()["role"] == "supervisor"

            # supervisor / joiner cannot mint or list
            for tok in (sup["supervisor_token"], jt["join_token"]):
                assert (await c.post(f"{p}/join-tokens", headers=_h(tok))).status_code == 403
                assert (await c.post(f"{p}/supervisor-tokens", headers=_h(tok))).status_code == 403
                assert (await c.get(f"{p}/supervisor-tokens", headers=_h(tok))).status_code == 403

            lst = await c.get(f"{p}/supervisor-tokens", headers=_h(creator))
            assert lst.status_code == 200
            infos = {i["token_id"]: i for i in lst.json()}
            assert set(infos) == {out["supervisor_token_id"], sup["token_id"]}
            assert all(i["revoked_at"] is None for i in infos.values())
            assert "supervisor_token" not in lst.text

            d = await c.delete(f"{p}/supervisor-tokens/{sup['token_id']}", headers=_h(creator))
            assert d.status_code == 204
            assert d.content == b""
            a = await c.get(f"{p}/access", headers=_h(sup["supervisor_token"]))
            assert a.status_code == 403
            lst = await c.get(f"{p}/supervisor-tokens", headers=_h(creator))
            infos = {i["token_id"]: i for i in lst.json()}
            assert infos[sup["token_id"]]["revoked_at"] is not None
            assert infos[out["supervisor_token_id"]]["revoked_at"] is None

            miss = await c.delete(f"{p}/supervisor-tokens/nonexistent", headers=_h(creator))
            assert miss.status_code == 404
            assert miss.json()["error"]["code"] == "token_not_found"
            sup_del = await c.delete(
                f"{p}/supervisor-tokens/{out['supervisor_token_id']}",
                headers=_h(out["supervisor_token"]),
            )
            assert sup_del.status_code == 403


@pytest.mark.integration
@pytest.mark.asyncio
async def test_public_base_url_used_for_invites() -> None:
    async with relay_subprocess(
        {"SW_PUBLIC_BASE_URL": "https://relay.example.org"}
    ) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            out = (await c.post(f"{base}/v1/sessions", json={})).json()
            assert out["invite_url"].startswith("https://relay.example.org/invite/")
            assert out["invite_link"].startswith("swvs://relay.example.org/")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_tools_mint_list_revoke() -> None:
    from tests.integration.client import sw_client

    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with sw_client(base) as c:
            out = await c.call_tool("session_create", {})
            sid, creator = out["session_id"], out["creator_token"]
            assert out["invite_link"].startswith("swv://127.0.0.1:")
            assert f"jt={out['join_token']}" in out["invite_url"]

            jt = await c.call_tool(
                "session_mint_join_token", {"session_id": sid, "creator_token": creator}
            )
            assert jt["invite_link"].startswith("swv://") and jt["join_token"]
            sup = await c.call_tool(
                "session_mint_supervisor_token",
                {"session_id": sid, "creator_token": ADMIN},
            )
            lst = await c.call_tool(
                "session_list_supervisor_tokens",
                {"session_id": sid, "creator_token": creator},
            )
            ids = {t["token_id"] for t in lst["supervisor_tokens"]}
            assert ids == {out["supervisor_token_id"], sup["token_id"]}
            rev = await c.call_tool(
                "session_revoke_supervisor_token",
                {"session_id": sid, "creator_token": creator, "token_id": sup["token_id"]},
            )
            assert rev == {"revoked": True}
