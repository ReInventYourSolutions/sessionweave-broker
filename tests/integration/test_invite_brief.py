from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invite_brief_served(tmp_path) -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.post(
                "/v1/sessions",
                json={"purpose": {"kind": "demo", "your_role": "O",
                                  "instructions": "play O"}},
            )
            r.raise_for_status()
            sid, jt = r.json()["session_id"], r.json()["join_token"]

            # No join token, no brief: the link without ?jt= reveals nothing.
            r = await c.get(f"/invite/{sid}")
            assert r.status_code == 403
            assert "play O" not in r.text

            r = await c.get(f"/invite/{sid}?jt={jt}")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/markdown")
            body = r.text
            assert sid in body
            assert base in body                      # base URL interpolated
            assert "do NOT open a WebSocket" in body
            assert "your role: `O`" in body
            assert "/v1/sessions/" in body

            r = await c.get("/invite/does-not-exist?jt=x")
            assert r.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invite_errors_are_markdown_pages() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/invite/does-not-exist?jt=x")
            assert r.status_code == 404
            assert r.headers["content-type"].startswith("text/markdown")
            assert r.text.startswith("# Session not found")

            r = await c.post("/v1/sessions", json={})
            r.raise_for_status()
            sid = r.json()["session_id"]
            codes = []
            for _ in range(8):
                r = await c.get(f"/invite/{sid}?jt=wrong")
                codes.append(r.status_code)
                assert r.headers["content-type"].startswith("text/markdown"), r.text
            assert codes[-1] == 429
            assert r.text.startswith("# Too many invalid invite links")
