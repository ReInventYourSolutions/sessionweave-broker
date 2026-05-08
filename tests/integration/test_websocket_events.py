"""End-to-end tests for the WebSocket events endpoint.

Covers the live-push contract: a client joins via REST (pull mode by
definition) and connects to ``ws://.../events?token=...``. Subsequent
``command.received``, ``result.received`` and ``command.completed``
events are pushed over the socket without the client having to poll.
"""
from __future__ import annotations

import asyncio
import json
from urllib.parse import urlparse

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tests.integration.harness import relay_subprocess


def _ws_base(http_base: str) -> str:
    parsed = urlparse(http_base)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}"


async def _recv_event(
    ws: websockets.WebSocketClientProtocol, timeout: float = 5.0
) -> dict[str, object]:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)  # type: ignore[no-any-return]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_receives_live_command_received() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid = (await c.post("/v1/sessions", json={})).json()["session_id"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", json={"role": "receiver"}
            )
        ).json()
        sender_h = {"Authorization": f"Bearer {sender['participant_token']}"}

        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={receiver['participant_token']}"
        )
        async with websockets.connect(ws_url) as ws:
            # Sender dispatches *after* the receiver subscribes — the event
            # must arrive over the socket, not via the buffer.
            send = await c.post(
                f"/v1/sessions/{sid}/commands",
                headers=sender_h,
                json={
                    "dispatch": "specific",
                    "target": {"participant_ids": [receiver["participant_id"]]},
                    "payload": {"q": "ping"},
                    "timeout_seconds": 30,
                },
            )
            assert send.status_code == 201, send.text
            cid = send.json()["command_id"]

            evt = await _recv_event(ws)
            assert evt["name"] == "command.received"
            payload = evt["payload"]
            assert isinstance(payload, dict)
            assert payload["command_id"] == cid


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_buffer_resumes_after_disconnect() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid = (await c.post("/v1/sessions", json={})).json()["session_id"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", json={"role": "receiver"}
            )
        ).json()
        sender_h = {"Authorization": f"Bearer {sender['participant_token']}"}
        receiver_h = {"Authorization": f"Bearer {receiver['participant_token']}"}

        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={receiver['participant_token']}"
        )
        async with websockets.connect(ws_url):
            pass

        # Give the server's detach_subscriber a brief moment to run after
        # the close frame. Without this, the next event would still be
        # routed to the now-orphaned subscriber queue rather than the
        # buffer. We poll the inbox up to 2s rather than sleeping blindly.
        send = await c.post(
            f"/v1/sessions/{sid}/commands",
            headers=sender_h,
            json={
                "dispatch": "specific",
                "target": {"participant_ids": [receiver["participant_id"]]},
                "payload": {"q": "post-disconnect"},
                "timeout_seconds": 30,
            },
        )
        # The command may race with subscriber teardown. Retry the send-and-drain
        # cycle a few times: once the subscriber is gone, _deliver_or_buffer
        # routes future events to the per-participant buffer where REST drain
        # can see them.
        for _ in range(20):
            await asyncio.sleep(0.1)
            drain = await c.post(
                f"/v1/sessions/{sid}/participants/me/inbox/drain",
                headers=receiver_h,
            )
            names = [m["name"] for m in drain.json()["messages"]]
            if "command.received" in names:
                break
            # Nothing yet — issue another command so we can detect the
            # transition to buffered delivery.
            send = await c.post(
                f"/v1/sessions/{sid}/commands",
                headers=sender_h,
                json={
                    "dispatch": "specific",
                    "target": {"participant_ids": [receiver["participant_id"]]},
                    "payload": {"q": "retry"},
                    "timeout_seconds": 30,
                },
            )
            assert send.status_code == 201
        else:
            pytest.fail("buffer never received post-disconnect events")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_replays_pending_buffer_on_attach() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid = (await c.post("/v1/sessions", json={})).json()["session_id"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", json={"role": "receiver"}
            )
        ).json()
        sender_h = {"Authorization": f"Bearer {sender['participant_token']}"}

        # Send *before* WS connects: event lands in the per-participant
        # buffer. attach_subscriber must drain it on connect so the WS
        # client doesn't need a separate REST drain.
        send = await c.post(
            f"/v1/sessions/{sid}/commands",
            headers=sender_h,
            json={
                "dispatch": "specific",
                "target": {"participant_ids": [receiver["participant_id"]]},
                "payload": {"q": "pre-subscribe"},
                "timeout_seconds": 30,
            },
        )
        assert send.status_code == 201
        cid = send.json()["command_id"]

        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={receiver['participant_token']}"
        )
        async with websockets.connect(ws_url) as ws:
            evt = await _recv_event(ws)
            assert evt["name"] == "command.received"
            payload = evt["payload"]
            assert isinstance(payload, dict)
            assert payload["command_id"] == cid


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_rejects_missing_token() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid = (await c.post("/v1/sessions", json={})).json()["session_id"]
        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
        )
        with pytest.raises((ConnectionClosedError, InvalidStatus, OSError)):
            async with websockets.connect(ws_url) as ws:
                await _recv_event(ws, timeout=2.0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_rejects_unknown_token() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        sid = (await c.post("/v1/sessions", json={})).json()["session_id"]
        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token=00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises((ConnectionClosedError, InvalidStatus, OSError)):
            async with websockets.connect(ws_url) as ws:
                await _recv_event(ws, timeout=2.0)
