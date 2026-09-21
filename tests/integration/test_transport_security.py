"""DNS-rebinding protection on the MCP endpoint.

The relay enables the MCP transport's ``allowed_hosts`` check so a browser
page on an attacker's origin cannot drive a relay bound to loopback. It is
a security control that nothing tested — and in the mcp 2.x SDK the
setting moves from the server constructor to ``streamable_http_app()``, so
it is exactly the kind of thing that can be dropped in a migration without
a single test going red.

These tests assert the behaviour from outside: a request whose ``Host``
is not on the list must be refused, whatever the SDK wires internally.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess

_INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "transport-security-test", "version": "1"},
    },
}
_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_loopback_host_is_accepted() -> None:
    """The baseline — without this the rejection tests prove nothing."""
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{base}/mcp", headers=_HEADERS, json=_INITIALIZE)
    assert r.status_code == 200, r.text


@pytest.mark.integration
@pytest.mark.asyncio
async def test_foreign_host_header_is_refused() -> None:
    """A page on attacker.example driving a loopback-bound relay."""
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(
                f"{base}/mcp",
                headers={**_HEADERS, "Host": "attacker.example"},
                json=_INITIALIZE,
            )
    assert r.status_code == 421, (
        f"expected the Host check to refuse the request, got "
        f"{r.status_code}: {r.text[:200]!r}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sw_allowed_hosts_widens_the_list() -> None:
    """Operators put the relay behind a real hostname with SW_ALLOWED_HOSTS;
    this pins that the env var still reaches the transport."""
    async with relay_subprocess(
        env_overrides={"SW_ALLOWED_HOSTS": "relay.example.com"}
    ) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            allowed = await c.post(
                f"{base}/mcp",
                headers={**_HEADERS, "Host": "relay.example.com"},
                json=_INITIALIZE,
            )
            still_refused = await c.post(
                f"{base}/mcp",
                headers={**_HEADERS, "Host": "attacker.example"},
                json=_INITIALIZE,
            )
    assert allowed.status_code == 200, allowed.text
    assert still_refused.status_code == 421, (
        "widening the list must not disable the check"
    )
