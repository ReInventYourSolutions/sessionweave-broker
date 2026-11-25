from __future__ import annotations

from importlib import metadata

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
async def test_sdk_page_pins_install_to_sdk_version() -> None:
    v = metadata.version("sessionweave-sdk")
    async with relay_subprocess({"SW_PUBLIC_BASE_URL": PUBLIC}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/sdk")
            assert r.status_code == 200
            body = r.text
            assert (
                f"pip install sessionweave-sdk=={v}" in body
            )
            assert f'SessionWeaveClient(base_url="{PUBLIC}")' in body
            assert "/_static/vendor/marked.min.js" in body


@pytest.mark.integration
@pytest.mark.asyncio
async def test_legacy_sdk_tarball_redirects_to_pypi_sdist() -> None:
    v = metadata.version("sessionweave-sdk")
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/sdk/sessionweave-client.tar.gz")
            assert r.status_code == 307
            assert r.headers["location"] == (
                "https://files.pythonhosted.org/packages/source/s/sessionweave-sdk/"
                f"sessionweave_sdk-{v}.tar.gz"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sdk_readme_is_the_installed_package_readme() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/sdk.md")
            assert r.status_code == 200
            assert r.headers["content-type"].startswith("text/markdown")
            assert "pip install sessionweave-sdk" in r.text


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
                "/skill.md",
                "/connect.mcp.json",
                "/openapi.yaml",
                "/v1/info",
            ):
                assert href in body, f"landing missing link to {href}"


def _first_server(text: str) -> tuple[str, str]:
    lines = text.split("servers:\n", 1)[1].splitlines()
    return lines[0].split("url:", 1)[1].strip(), lines[1].split("description:", 1)[1].strip()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_openapi_servers_first_entry_is_this_broker() -> None:
    async with relay_subprocess(env_overrides={"SW_PUBLIC_BASE_URL": "https://b.example"}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/openapi.yaml")
            assert r.status_code == 200
            assert _first_server(r.text) == ("https://b.example", "This broker")
            assert "http://127.0.0.1:8765" in r.text
    async with relay_subprocess(env_overrides={"SW_PUBLIC_BASE_URL": ""}) as base:
        async with httpx.AsyncClient(base_url=base, timeout=5) as c:
            r = await c.get("/openapi.yaml")
            assert _first_server(r.text)[0] == base
