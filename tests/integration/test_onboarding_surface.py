from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess

PUBLIC = "https://broker.internal:9000"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_landing_page_served() -> None:
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/html")
            body = r.text
            assert "broker.internal:9000" in body          # advertised address
            assert "sessionweave" in body.lower()
            assert "/skill.md" in body                       # links the artefacts
            assert "/connect.mcp.json" in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_skill_md_parametrized_on_public_base() -> None:
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/skill.md")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/markdown")
            body = r.text
            assert "https://broker.internal:9000" in body
            assert "broker.sessionweave.com" not in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_skill_md_download_disposition() -> None:
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/skill.md", params={"download": "1"})
            assert r.status_code == 200
            assert "attachment" in r.headers.get("content-disposition", "")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_skill_md_fallback_downgrades_scheme_for_plain_http() -> None:
    # No SW_PUBLIC_BASE_URL: base resolves to the http loopback, so references
    # to THIS broker must downgrade TLS-only schemes (wss→ws, swvs→swv).
    # Generic scheme-name mentions in prose are left alone.
    async with relay_subprocess() as base:
        netloc = base.split("://", 1)[1]  # 127.0.0.1:PORT
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/skill.md")
            assert r.status_code == 200
            body = r.text
            assert "broker.sessionweave.com" not in body
            assert f"http://{netloc}" in body
            assert f"ws://{netloc}" in body
            # the canonical skill's one wss:// host literal must downgrade
            assert f"wss://{netloc}" not in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_connect_mcp_json() -> None:
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/connect.mcp.json")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("application/json")
            snip = r.json()
            assert snip["mcpServers"]["sessionweave"]["url"] == (
                "https://broker.internal:9000/mcp"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_brief_auto_wires_to_native_routes() -> None:
    # No SW_SKILL_URL / SW_MCP_SNIPPET_URL: the brief's upgrade pointers
    # default to the broker's own routes.
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.post("/v1/sessions", json={"purpose": {"kind": "demo"}})
            r.raise_for_status()
            sid, jt = r.json()["session_id"], r.json()["join_token"]
            r = await c.get(f"/invite/{sid}?jt={jt}")
            assert r.status_code == 200
            body = r.text
            assert "https://broker.internal:9000/skill.md" in body
            assert "https://broker.internal:9000/connect.mcp.json" in body
            assert "Ask your operator" not in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_brief_env_override_wins_over_native_routes() -> None:
    overrides = {
        "SW_PUBLIC_BASE_URL": PUBLIC,
        "SW_SKILL_URL": "https://elsewhere.example/skill",
    }
    async with relay_subprocess(overrides) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.post("/v1/sessions", json={"purpose": {"kind": "demo"}})
            r.raise_for_status()
            sid, jt = r.json()["session_id"], r.json()["join_token"]
            r = await c.get(f"/invite/{sid}?jt={jt}")
            body = r.text
            assert "https://elsewhere.example/skill" in body
            # native mcp route still fills the unset mcp_snippet_url
            assert "https://broker.internal:9000/connect.mcp.json" in body
