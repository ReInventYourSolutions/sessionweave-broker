"""The /v1/info response gains a capabilities object the cockpit keys off."""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_info_advertises_capabilities():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            resp = await c.get(f"{base}/v1/info")
    assert resp.status_code == 200
    body = resp.json()
    caps = body.get("capabilities")
    assert isinstance(caps, dict)
    assert caps["firehose"] is True
    assert caps["supervisor_notes"] is True
    assert caps["session_delete"] is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_info_advertises_auth_model() -> None:
    """SDK clients use ``/v1/info.auth_model`` to detect the relay's
    authentication shape (issue #62). The relay advertises the scoped-token
    model: join tokens to enter, a secret participant token per participant,
    and creator / supervisor tokens for the people running the session.
    """
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            resp = await c.get(f"{base}/v1/info")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("auth_model") == "scoped-tokens-v1"
