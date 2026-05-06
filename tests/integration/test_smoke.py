import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_healthz_returns_200() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{base}/healthz")
        assert r.status_code == 200


@pytest.mark.integration
@pytest.mark.asyncio
async def test_readyz_returns_200() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{base}/readyz")
        assert r.status_code == 200
