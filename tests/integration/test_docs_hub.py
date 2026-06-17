from __future__ import annotations

import io
import tarfile

import httpx
import pytest

from tests.integration.harness import relay_subprocess

PUBLIC = "https://broker.internal:9000"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_api_docs_offline() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/api")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/html")
            body = r.text
            assert "SwaggerUIBundle" in body
            assert "/_static/swagger-ui/swagger-ui-bundle.js" in body
            assert "jsdelivr" not in body and "https://cdn" not in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_static_assets_and_traversal_guard() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/_static/swagger-ui/swagger-ui.css")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/css")

            r = await c.get("/_static/vendor/marked.min.js")
            assert r.status_code == 200

            # traversal / non-asset paths are rejected
            r = await c.get("/_static/sdk/pyproject.toml")
            assert r.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sdk_page_pins_install_to_broker() -> None:
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/sdk")
            assert r.status_code == 200
            body = r.text
            assert (
                f"pip install {PUBLIC}/sdk/sessionweave-client.tar.gz" in body
            )
            assert "/_static/vendor/marked.min.js" in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sdk_tarball_is_a_valid_archive() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/sdk/sessionweave-client.tar.gz")
            assert r.status_code == 200
            assert r.headers["content-type"] == "application/gzip"
            tf = tarfile.open(fileobj=io.BytesIO(r.content), mode="r:gz")
            names = tf.getnames()
            assert "sessionweave-client/pyproject.toml" in names
            assert (
                "sessionweave-client/src/sessionweave_client/__init__.py" in names
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_landing_indexes_every_resource() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            body = (await c.get("/")).text
            for href in (
                "/api",
                "/sdk",
                "/sdk.md",
                "/sdk/sessionweave-client.tar.gz",
                "/skill.md",
                "/connect.mcp.json",
                "/openapi.yaml",
                "/v1/info",
            ):
                assert href in body, f"landing missing link to {href}"
