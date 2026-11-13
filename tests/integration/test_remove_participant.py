"""Removing an agent: creator / admin only, halt first, then a durable removal.

Removal concerns only the addressed participant (sub-agents never join, so
there is nothing to cascade to). The target receives a ``halt`` note, then
leaves the roster; everyone else gets ``participant.left`` with reason
``removed``. Every still-valid join token of the session is invalidated, and
the removal is on disk before the call returns.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import pytest
import pytest_asyncio
import websockets
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from websockets.exceptions import ConnectionClosed

from sessionweave import access
from sessionweave.access import (
    _now_ms,
    generate_token,
    hash_token,
    participant_id_for_token,
)
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import DispatchMode, PersistenceConfig, Target
from sessionweave.server import build_app
from sessionweave.session import Session
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess
from tests.unit.fakes import FakeHandle

HALT_MESSAGE = "removed by the session creator"

# ---------- in-process fixtures ----------


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "remove.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _registry(store: PersistenceStore) -> SessionRegistry:
    return SessionRegistry(
        defaults=load_deployment_defaults({}),
        connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )


async def _session(reg: SessionRegistry) -> Session:
    sid, _, _, _, _ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=hash_token(generate_token()),
    )
    return await reg.get_or_raise(sid)


async def _three(sess: Session) -> list[tuple[Any, FakeHandle]]:
    out = []
    for _ in range(3):
        h = FakeHandle()
        p = await sess.join(h, role="worker", capabilities=[])
        out.append((p, h))
    for _, h in out:
        h.sent.clear()
    return out


def _states(sess: Session) -> dict[str, str]:
    return {p.participant_id: p.supervision_state for p in sess.roster_snapshot()}


# ---------- in-process ----------


@pytest.mark.asyncio
async def test_remove_halts_the_target_then_removes_only_it(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    (a, ha), (b, hb), (c, hc) = await _three(sess)

    out = await sess.remove_participant(a.participant_id)

    assert out["removed"] == [a.participant_id]
    assert [p.participant_id for p in sess.roster_snapshot()] == [
        b.participant_id, c.participant_id
    ]
    # The target got the halt, and it was the last thing it received.
    notes = [p for n, p in ha.sent if n == "supervisor.note"]
    assert len(notes) == 1
    assert notes[0]["kind"] == "halt"
    assert notes[0]["target"] == a.participant_id
    assert notes[0]["message"] == HALT_MESSAGE
    assert ha.sent[-1][0] == "supervisor.note"
    # Nobody else was halted or changed state.
    assert _states(sess) == {b.participant_id: "active", c.participant_id: "active"}
    for h in (hb, hc):
        assert "supervisor.note" not in [n for n, _ in h.sent]
        left = [p for n, p in h.sent if n == "participant.left"]
        assert left == [{"participant_id": a.participant_id, "reason": "removed"}]


@pytest.mark.asyncio
async def test_remove_unknown_participant_is_not_found(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    await _three(sess)

    with pytest.raises(ToolError) as exc:
        await sess.remove_participant("p-nope")
    assert exc.value.code is ErrorCode.PARTICIPANT_NOT_FOUND
    assert len(sess.roster_snapshot()) == 3


@pytest.mark.asyncio
async def test_removed_token_and_connection_no_longer_resolve(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    (a, ha), _, _ = await _three(sess)
    assert a.issued_token is not None
    token = a.issued_token

    await sess.remove_participant(a.participant_id)

    with pytest.raises(ToolError) as exc:
        participant_id_for_token(sess, token)
    assert exc.value.code is ErrorCode.PARTICIPANT_REMOVED
    assert sess._connections.session_for(ha) is None


@pytest.mark.asyncio
async def test_inflight_command_the_removed_participant_sent_is_cancelled(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    (a, _), (b, hb), _ = await _three(sess)
    sent = await sess.send(
        sender_id=a.participant_id, dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]), payload={"do": "it"},
    )
    hb.sent.clear()

    await sess.remove_participant(a.participant_id)

    cancelled = [p for n, p in hb.sent if n == "command.cancelled"]
    assert cancelled == [{"command_id": sent.command_id, "reason": "sender_gone"}]
    assert sess._inflight == {}


@pytest.mark.asyncio
async def test_remove_invalidates_join_tokens_but_a_new_one_works(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    (a, _), _, _ = await _three(sess)
    jt, _ = await sess.mint_join_token()

    out = await sess.remove_participant(a.participant_id)

    assert out["join_tokens_invalidated"] == 1
    assert sess.auth.match(jt, _now_ms()) is None
    assert sess.auth.join_token_expired(jt, _now_ms())
    fresh, _ = await sess.mint_join_token()
    principal = sess.auth.match(fresh, _now_ms())
    assert principal is not None and principal.role == "joiner"


@pytest.mark.asyncio
async def test_removal_is_durable_before_the_call_returns(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    (a, _), (b, _), (c, _) = await _three(sess)
    jt, _ = await sess.mint_join_token()

    await sess.remove_participant(a.participant_id)

    # A fresh registry hydrates from disk only: the removal must already be there.
    hydrated = await _registry(store).get_or_raise(sess.session_id)
    assert sorted(_states(hydrated)) == sorted([b.participant_id, c.participant_id])
    assert hydrated.auth.match(jt, _now_ms()) is None
    page = await store.fetch_events(sid=sess.session_id, kinds=["participant.left"])
    assert [e["payload"] for e in page.events] == [
        {"participant_id": a.participant_id, "reason": "removed"}
    ]


# ---------- gate (in-process app) ----------

ADMIN = "admin-token-for-remove-tests"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SW_ADMIN_TOKEN", ADMIN)
    with TestClient(build_app()) as c:
        yield c


@pytest.mark.parametrize(
    ("role", "status"),
    [
        ("admin", 200), ("creator", 200), ("supervisor", 403),
        ("participant", 403), ("joiner", 403), ("none", 403),
    ],
)
def test_only_creator_and_admin_can_remove(client: TestClient, role: str, status: int) -> None:
    made = client.post("/v1/sessions", json={}).json()
    sid, jt = made["session_id"], made["join_token"]
    target = client.post(
        f"/v1/sessions/{sid}/participants", json={"role": "w"},
        headers=bearer(jt),
    ).json()
    other = client.post(
        f"/v1/sessions/{sid}/participants", json={"role": "w"},
        headers=bearer(jt),
    ).json()
    tokens = {
        "admin": ADMIN, "creator": made["creator_token"],
        "supervisor": made["supervisor_token"],
        "participant": other["participant_token"], "joiner": jt, "none": None,
    }
    token = tokens[role]
    headers = bearer(token) if token else {}
    r = client.delete(
        f"/v1/sessions/{sid}/participants/{target['participant_id']}", headers=headers
    )
    assert r.status_code == status, r.text
    if status == 200:
        assert r.json() == {"removed": [target["participant_id"]], "join_tokens_invalidated": 1}


def test_remove_unknown_participant_over_http_is_404(client: TestClient) -> None:
    made = client.post("/v1/sessions", json={}).json()
    sid = made["session_id"]
    r = client.delete(
        f"/v1/sessions/{sid}/participants/p-nope", headers=bearer(made["creator_token"])
    )
    assert r.status_code == 404, r.text
    assert r.json()["error"]["code"] == "participant_not_found"


# ---------- over the wire ----------


def _ws_base(http_base: str) -> str:
    parsed = urlparse(http_base)
    return f"ws://{parsed.netloc}"


async def _join(c: httpx.AsyncClient, base: str, sid: str, jt: str, role: str) -> dict[str, Any]:
    r = await c.post(
        f"{base}/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": role}
    )
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_remove_closes_the_events_ws_with_4410_after_the_halt() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(timeout=15.0) as c:
        made = (await c.post(f"{base}/v1/sessions", json={})).json()
        sid, jt, creator = made["session_id"], made["join_token"], made["creator_token"]
        a = await _join(c, base, sid, jt, "worker")
        b = await _join(c, base, sid, jt, "worker")
        url = (
            f"{_ws_base(base)}/v1/sessions/{sid}/participants/me/events"
            f"?token={a['participant_token']}"
        )
        async with websockets.connect(url) as ws:
            # A has a command in flight to B when it is removed.
            sent = await c.post(
                f"{base}/v1/sessions/{sid}/commands",
                headers=bearer(a["participant_token"]),
                json={
                    "dispatch": "specific",
                    "target": {"participant_ids": [b["participant_id"]]},
                    "payload": {"n": 1}, "timeout_seconds": 60,
                },
            )
            assert sent.status_code == 201, sent.text
            cid = sent.json()["command_id"]

            r = await c.delete(
                f"{base}/v1/sessions/{sid}/participants/{a['participant_id']}",
                headers=bearer(made["supervisor_token"]),
            )
            assert r.status_code == 403, r.text

            r = await c.delete(
                f"{base}/v1/sessions/{sid}/participants/{a['participant_id']}",
                headers=bearer(creator),
            )
            assert r.status_code == 200, r.text
            assert r.json() == {"removed": [a["participant_id"]], "join_tokens_invalidated": 1}

            frames: list[dict[str, Any]] = []
            with pytest.raises(ConnectionClosed) as closed:
                while True:
                    frames.append(json.loads(await asyncio.wait_for(ws.recv(), 5.0)))
            assert closed.value.rcvd is not None
            assert closed.value.rcvd.code == 4410
            assert closed.value.rcvd.reason == "removed"
            notes = [f["payload"] for f in frames if f["name"] == "supervisor.note"]
            assert [(n["kind"], n["message"]) for n in notes] == [("halt", HALT_MESSAGE)]
            assert frames[-1]["name"] == "supervisor.note"

        # The removed participant's token no longer works.
        r = await c.post(
            f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
            headers=bearer(a["participant_token"]),
        )
        assert r.status_code == 403, r.text
        assert r.json()["error"]["code"] == "participant_removed", r.text

        # B saw the cancellation and the removal; nobody halted B.
        r = await c.post(
            f"{base}/v1/sessions/{sid}/participants/me/inbox/drain",
            headers=bearer(b["participant_token"]),
        )
        assert r.status_code == 200, r.text
        events = r.json()["messages"]
        names = [e["name"] for e in events]
        assert "supervisor.note" not in names
        assert {"command_id": cid, "reason": "sender_gone"} in [
            e["payload"] for e in events if e["name"] == "command.cancelled"
        ]
        assert {"participant_id": a["participant_id"], "reason": "removed"} in [
            e["payload"] for e in events if e["name"] == "participant.left"
        ]

        # The old join link is expired; a freshly minted one works.
        r = await c.post(
            f"{base}/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "w"}
        )
        assert r.status_code == 403, r.text
        assert "expired" in r.json()["error"]["message"]
        r = await c.post(f"{base}/v1/sessions/{sid}/join-tokens", headers=bearer(creator))
        assert r.status_code == 201, r.text
        await _join(c, base, sid, r.json()["join_token"], "w")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_remove_participant_unbinds_the_removed_connection() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            made = (await c.post(f"{base}/v1/sessions", json={})).json()
        sid, creator = made["session_id"], made["creator_token"]

        async with sw_client(base) as agent, sw_client(base) as manager:
            joined = await agent.call_tool(
                "session_join", {"session_id": sid, "join_token": made["join_token"],
                                 "role": "worker"},
            )
            pid = joined["participant_id"]

            with pytest.raises(RuntimeError, match="permission_denied"):
                await manager.call_tool("session_remove_participant", {
                    "session_id": sid, "creator_token": made["supervisor_token"],
                    "participant_id": pid,
                })

            out = await manager.call_tool("session_remove_participant", {
                "session_id": sid, "creator_token": creator, "participant_id": pid,
            })
            assert out == {"removed": [pid], "join_tokens_invalidated": 1}

            with pytest.raises(RuntimeError, match="not_in_session"):
                await agent.call_tool("session_list_participants", {})


# ---------- fix round 1: write ordering ----------


@pytest.mark.asyncio
async def test_failed_soft_delete_changes_nothing_and_a_retry_completes(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    sess = await _session(_registry(store))
    (a, _), (_, hb), _ = await _three(sess)
    jt, _ = await sess.mint_join_token()
    real = PersistenceStore.soft_delete_participant

    async def broken(self: PersistenceStore, *args: Any, **kwargs: Any) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr(PersistenceStore, "soft_delete_participant", broken)
    with pytest.raises(RuntimeError, match="disk full"):
        await sess.remove_participant(a.participant_id)

    assert a.participant_id in _states(sess)
    assert a.issued_token is not None
    assert participant_id_for_token(sess, a.issued_token) == a.participant_id
    principal = sess.auth.match(jt, _now_ms())
    assert principal is not None and principal.role == "joiner"
    assert "participant.left" not in [n for n, _ in hb.sent]

    monkeypatch.setattr(PersistenceStore, "soft_delete_participant", real)
    out = await sess.remove_participant(a.participant_id)

    assert out == {"removed": [a.participant_id], "join_tokens_invalidated": 1}
    assert a.participant_id not in _states(sess)
    assert sess.auth.match(jt, _now_ms()) is None
    hydrated = await _registry(store).get_or_raise(sess.session_id)
    assert a.participant_id not in _states(hydrated)


@pytest.mark.asyncio
async def test_failed_left_event_write_still_removes(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    sess = await _session(_registry(store))
    (a, _), (_, hb), _ = await _three(sess)
    jt, _ = await sess.mint_join_token()
    real_dispatch = Session._dispatch_event

    async def flaky(self: Session, **kwargs: Any) -> None:
        if kwargs["kind"] == "participant.left":
            raise RuntimeError("disk full")
        await real_dispatch(self, **kwargs)

    monkeypatch.setattr(Session, "_dispatch_event", flaky)
    out = await sess.remove_participant(a.participant_id)

    assert out == {"removed": [a.participant_id], "join_tokens_invalidated": 1}
    assert a.participant_id not in _states(sess)
    assert sess.auth.match(jt, _now_ms()) is None
    left = [p for n, p in hb.sent if n == "participant.left"]
    assert left == [{"participant_id": a.participant_id, "reason": "removed"}]
    hydrated = await _registry(store).get_or_raise(sess.session_id)
    assert a.participant_id not in _states(hydrated)


# ---------- fix round 1: removal tombstone ----------

REMOVED_MESSAGE = (
    "you were removed from this session by its creator — "
    "stop your work and your sub-agents"
)


def _removed_pull_participant(client: TestClient) -> tuple[str, dict[str, Any], dict[str, Any]]:
    made = client.post("/v1/sessions", json={}).json()
    sid, jt = made["session_id"], made["join_token"]
    target = client.post(
        f"/v1/sessions/{sid}/participants", json={"role": "w", "delivery_mode": "pull"},
        headers=bearer(jt),
    ).json()
    other = client.post(
        f"/v1/sessions/{sid}/participants", json={"role": "w"}, headers=bearer(jt),
    ).json()
    r = client.delete(
        f"/v1/sessions/{sid}/participants/{target['participant_id']}",
        headers=bearer(made["creator_token"]),
    )
    assert r.status_code == 200, r.text
    return sid, target, other


def _assert_removed(r: httpx.Response) -> None:
    assert r.status_code == 403, r.text
    assert r.json()["error"] == {"code": "participant_removed", "message": REMOVED_MESSAGE}


def test_removed_pull_participant_is_told_on_its_next_call(client: TestClient) -> None:
    sid, target, other = _removed_pull_participant(client)
    h = bearer(target["participant_token"])

    _assert_removed(client.post(f"/v1/sessions/{sid}/participants/me/inbox/drain", headers=h))
    _assert_removed(client.post(
        f"/v1/sessions/{sid}/participants/me/inbox/wait", headers=h,
        json={"timeout_seconds": 1},
    ))
    _assert_removed(client.post(
        f"/v1/sessions/{sid}/commands", headers=h,
        json={"dispatch": "specific",
              "target": {"participant_ids": [other["participant_id"]]},
              "payload": {"n": 1}},
    ))
    # A reader endpoint resolving the token as an identity says the same.
    _assert_removed(client.get(f"/v1/sessions/{sid}/participants", headers=h))
    with pytest.raises(WebSocketDisconnect) as closed, client.websocket_connect(
        f"/v1/sessions/{sid}/participants/me/events?token={target['participant_token']}"
    ) as ws:
        ws.receive_json()
    assert (closed.value.code, closed.value.reason) == (4410, "removed")


def test_removal_tombstone_expires_after_an_hour(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    sid, target, _ = _removed_pull_participant(client)
    real = access._now_ms()
    monkeypatch.setattr(access, "_now_ms", lambda: real + 3_600_001)

    r = client.post(
        f"/v1/sessions/{sid}/participants/me/inbox/drain",
        headers=bearer(target["participant_token"]),
    )
    assert r.status_code == 401, r.text
    assert r.json()["error"]["code"] == "not_in_session"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_explicit_identity_of_a_removed_participant_is_refused() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            made = (await c.post(f"{base}/v1/sessions", json={})).json()
            sid = made["session_id"]
            target = await _join(c, base, sid, made["join_token"], "worker")
            r = await c.delete(
                f"{base}/v1/sessions/{sid}/participants/{target['participant_id']}",
                headers=bearer(made["creator_token"]),
            )
            assert r.status_code == 200, r.text

        async with sw_client(base) as mcp:
            with pytest.raises(RuntimeError, match="participant_removed"):
                await mcp.call_tool("session_check_inbox", {
                    "session_id": sid, "participant_token": target["participant_token"],
                })
            with pytest.raises(RuntimeError, match="participant_removed"):
                await mcp.call_tool("session_resume", {
                    "session_id": sid, "participant_token": target["participant_token"],
                })


# ---------- fix round 1: leave / session end close the events WS ----------


def _ws_participant(client: TestClient) -> tuple[dict[str, Any], dict[str, Any]]:
    made = client.post("/v1/sessions", json={}).json()
    joined = client.post(
        f"/v1/sessions/{made['session_id']}/participants", json={"role": "w"},
        headers=bearer(made["join_token"]),
    ).json()
    return made, joined


def test_leave_closes_the_events_ws_normally(client: TestClient) -> None:
    made, joined = _ws_participant(client)
    sid = made["session_id"]
    with pytest.raises(WebSocketDisconnect) as closed, client.websocket_connect(
        f"/v1/sessions/{sid}/participants/me/events?token={joined['participant_token']}"
    ) as ws:
        r = client.delete(
            f"/v1/sessions/{sid}/participants/me", headers=bearer(joined["participant_token"])
        )
        assert r.status_code == 204, r.text
        ws.receive_json()
    assert (closed.value.code, closed.value.reason) == (1000, "left")


def test_session_end_closes_the_events_ws_with_its_reason(client: TestClient) -> None:
    made, joined = _ws_participant(client)
    sid = made["session_id"]
    with pytest.raises(WebSocketDisconnect) as closed, client.websocket_connect(
        f"/v1/sessions/{sid}/participants/me/events?token={joined['participant_token']}"
    ) as ws:
        r = client.delete(f"/v1/sessions/{sid}", headers=bearer(made["creator_token"]))
        assert r.status_code == 200, r.text
        ws.receive_json()
    assert (closed.value.code, closed.value.reason) == (1000, "host_deleted")
