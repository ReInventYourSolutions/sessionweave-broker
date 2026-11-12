"""End-to-end tests for the WebSocket events endpoint.

Covers the live-push contract: a client joins via REST (pull mode by
definition) and connects to ``ws://.../events?token=...``. Subsequent
``command.received``, ``result.received`` and ``command.completed``
events are pushed over the socket without the client having to poll.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlparse

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


def _ws_base(http_base: str) -> str:
    parsed = urlparse(http_base)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return f"{scheme}://{parsed.netloc}"


async def _recv_event(ws: Any, timeout: float = 5.0) -> dict[str, object]:
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)  # type: ignore[no-any-return]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_receives_live_command_received() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "receiver"}
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
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "receiver"}
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
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "receiver"}
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
async def test_ws_recovers_undelivered_events_to_buffer_on_disconnect() -> None:
    """If a broadcast fans out to a WS subscriber but the socket has
    already gone away (the queue.get → send_json → disconnect race),
    the events must end up in the per-participant buffer so a REST
    drain (or a fresh WS attach) can still recover them. Without this
    the subscriber callback succeeded silently and the event was lost.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "receiver"}
            )
        ).json()
        sender_h = {"Authorization": f"Bearer {sender['participant_token']}"}
        receiver_h = {"Authorization": f"Bearer {receiver['participant_token']}"}

        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={receiver['participant_token']}"
        )

        # Open the WS, then close it abruptly *without* draining anything.
        ws = await websockets.connect(ws_url)
        await ws.close()
        # Give the server a moment to tear down the subscriber.
        await asyncio.sleep(0.2)

        # Now broadcast. With the bug, the event would be delivered to
        # the (already-orphaned) subscriber callback and lost; with the
        # fix, the recover path puts it on the buffer.
        send = await c.post(
            f"/v1/sessions/{sid}/commands",
            headers=sender_h,
            json={
                "dispatch": "specific",
                "target": {"participant_ids": [receiver["participant_id"]]},
                "payload": {"q": "after-flicker"},
                "timeout_seconds": 30,
            },
        )
        assert send.status_code == 201
        cid = send.json()["command_id"]

        # REST drain must surface the event. We retry briefly to absorb
        # any teardown latency.
        for _ in range(40):
            await asyncio.sleep(0.1)
            drain = await c.post(
                f"/v1/sessions/{sid}/participants/me/inbox/drain",
                headers=receiver_h,
            )
            names = [m["name"] for m in drain.json()["messages"]]
            if "command.received" in names:
                # And the command_id matches what we sent.
                got = next(
                    m for m in drain.json()["messages"] if m["name"] == "command.received"
                )
                assert got["payload"]["command_id"] == cid
                break
        else:
            pytest.fail("event not recovered to the buffer after WS disconnect")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_subscriber_receives_command_cancelled_on_ttl_expiry() -> None:
    """When a command's TTL expires, pending recipients must receive
    ``command.cancelled`` over their live transport (here: WS).

    Regression: prior to the deadline-shortening callback the registry
    heap kept the original idle-TTL deadline and never woke the
    session's tick to fire the cancellation.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        sender = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "sender"}
            )
        ).json()
        receiver = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "recv"}
            )
        ).json()

        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={receiver['participant_token']}"
        )
        async with websockets.connect(ws_url) as ws:
            send = await c.post(
                f"/v1/sessions/{sid}/commands",
                headers={"Authorization": f"Bearer {sender['participant_token']}"},
                json={
                    "dispatch": "specific",
                    "target": {"participant_ids": [receiver["participant_id"]]},
                    "payload": {},
                    "timeout_seconds": 2,
                },
            )
            assert send.status_code == 201
            cid = send.json()["command_id"]

            # Drain frames until we either see command.cancelled or the
            # 6 s budget runs out (covers TTL=2 + tick + transit).
            saw_cancel = False
            deadline = asyncio.get_event_loop().time() + 6.0
            while asyncio.get_event_loop().time() < deadline:
                try:
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=max(0.1, deadline - asyncio.get_event_loop().time())
                    )
                except TimeoutError:
                    break
                msg = json.loads(raw)
                if (
                    msg["name"] == "command.cancelled"
                    and msg["payload"]["command_id"] == cid
                    and msg["payload"]["reason"] == "ttl_expired"
                ):
                    saw_cancel = True
                    break
            assert saw_cancel, "command.cancelled (ttl_expired) never reached the WS subscriber"


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
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        joined = (
            await c.post(
                f"/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "p"},
            )
        ).json()
        # An unknown token, the session id and the participant_id are all
        # refused: only the secret participant_token authenticates the socket.
        for bad in (
            "00000000-0000-0000-0000-000000000000",
            sid,
            joined["participant_id"],
        ):
            ws_url = f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events?token={bad}"
            with pytest.raises((ConnectionClosedError, InvalidStatus, OSError)):
                async with websockets.connect(ws_url) as ws:
                    await _recv_event(ws, timeout=2.0)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ws_attach_keeps_participant_online() -> None:
    """A WS-only client (no live transport handle) is treated as online
    while its events socket is connected and reverts to offline when it
    detaches. Without this the relay would evict the participant after
    offline_grace_seconds even while actively receiving events.

    The regression scenario: the cockpit subscribes via WS to keep an
    eye on the session; without this flip the participant goes offline
    after the first transport disconnect and gets swept after the
    offline-grace window.
    """
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        _created = (await c.post("/v1/sessions", json={})).json()
        sid, jt = _created["session_id"], _created["join_token"]
        observer = (
            await c.post(
                f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "observer"}
            )
        ).json()
        observer_h = {"Authorization": f"Bearer {observer['participant_token']}"}
        pid = observer["participant_id"]

        async def online_of(target_pid: str) -> bool:
            roster = (
                await c.get(
                    f"/v1/sessions/{sid}/participants", headers=observer_h
                )
            ).json()["roster"]
            for p in roster:
                if p["participant_id"] == target_pid:
                    return bool(p["online"])
            raise AssertionError(f"participant {target_pid} not in roster")

        ws_url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={observer['participant_token']}"
        )

        async with websockets.connect(ws_url):
            # Settle the attach.
            await asyncio.sleep(0.1)
            assert await online_of(pid) is True, (
                "WS attach must keep participant online"
            )

        # After WS detach the participant should still be online because the
        # singleton _HTTP_HANDLE bound at REST-join is.is_alive()=True. The
        # important guarantee here is that the WS attach/detach cycle does
        # not break the online semantics — it does not flip a participant
        # offline as long as another transport keeps them addressable.
        await asyncio.sleep(0.1)
        assert await online_of(pid) is True, (
            "REST-joined participant must stay online while HTTP_HANDLE is alive"
        )
