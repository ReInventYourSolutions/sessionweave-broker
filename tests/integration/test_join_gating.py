"""Joining needs a join token; participants authenticate with a secret token.

REST and WebSocket behaviour runs in-process (so the clock can be moved);
the MCP explicit-identity path runs against a real relay over fresh
connections.
"""
from __future__ import annotations

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from sessionweave import access
from sessionweave.server import build_app
from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess

ADMIN = "admin-secret-for-tests"


def _h(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SW_ADMIN_TOKEN", ADMIN)
    with TestClient(build_app()) as c:
        yield c


def _create(c: TestClient) -> dict:
    r = c.post("/v1/sessions", json={})
    assert r.status_code == 201, r.text
    return r.json()


def _join(c: TestClient, sid: str, token: str | None):
    headers = _h(token) if token is not None else {}
    return c.post(f"/v1/sessions/{sid}/participants", json={"role": "w"}, headers=headers)


def test_join_without_token_is_unauthenticated(client: TestClient) -> None:
    sid = _create(client)["session_id"]
    r = _join(client, sid, None)
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "not_in_session"


def test_session_id_is_not_a_join_credential(client: TestClient) -> None:
    sid = _create(client)["session_id"]
    r = _join(client, sid, sid)
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "permission_denied"


def test_join_with_join_token_returns_secret_token(client: TestClient) -> None:
    out = _create(client)
    r = _join(client, out["session_id"], out["join_token"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["participant_token"] != body["participant_id"]
    # multi-use within its lifetime
    assert _join(client, out["session_id"], out["join_token"]).status_code == 201


def test_creator_and_admin_tokens_may_join(client: TestClient) -> None:
    out = _create(client)
    assert _join(client, out["session_id"], out["creator_token"]).status_code == 201
    assert _join(client, out["session_id"], ADMIN).status_code == 201


def test_supervisor_token_may_not_join(client: TestClient) -> None:
    out = _create(client)
    r = _join(client, out["session_id"], out["supervisor_token"])
    assert r.status_code == 403
    assert r.json()["error"]["code"] == "permission_denied"


def test_expired_join_token_message(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = _create(client)
    sid = out["session_id"]
    assert client.get(f"/v1/sessions/{sid}", headers=_h(out["join_token"])).status_code == 200
    real = access._now_ms
    monkeypatch.setattr(access, "_now_ms", lambda: real() + 901_000)
    r = _join(client, sid, out["join_token"])
    assert r.status_code == 403
    err = r.json()["error"]
    assert err["code"] == "permission_denied"
    assert "expired" in err["message"]
    assert "new invite link" in err["message"]


def test_joined_participant_survives_join_token_expiry(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = _create(client)
    sid = out["session_id"]
    tok = _join(client, sid, out["join_token"]).json()["participant_token"]
    real = access._now_ms
    monkeypatch.setattr(access, "_now_ms", lambda: real() + 901_000)
    r = client.get(f"/v1/sessions/{sid}/participants", headers=_h(tok))
    assert r.status_code == 200, r.text


def test_participant_id_cannot_act(client: TestClient) -> None:
    out = _create(client)
    sid = out["session_id"]
    pid = _join(client, sid, out["join_token"]).json()["participant_id"]
    # neither the participant_id nor the session id is a credential
    for tok in (pid, sid):
        r = client.get(f"/v1/sessions/{sid}/participants", headers=_h(tok))
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "permission_denied"


def test_events_ws_takes_secret_token_only(client: TestClient) -> None:
    out = _create(client)
    sid = out["session_id"]
    joined = _join(client, sid, out["join_token"]).json()
    with client.websocket_connect(
        f"/v1/sessions/{sid}/participants/me/events?token={joined['participant_token']}"
    ):
        pass
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(
            f"/v1/sessions/{sid}/participants/me/events?token={joined['participant_id']}"
        ):
            pass
    assert exc.value.reason == "not_in_session"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_join_resume_and_explicit_identity_use_secret_token() -> None:
    async with relay_subprocess({"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with sw_client(base) as a, sw_client(base) as b, sw_client(base) as c:
            created = await a.call_tool("session_create", {})
            sid = created["session_id"]

            with pytest.raises(RuntimeError, match="permission_denied"):
                await b.call_tool("session_join", {"session_id": sid})
            with pytest.raises(RuntimeError, match="permission_denied"):
                await b.call_tool("session_join", {"session_id": sid, "join_token": sid})
            joined = await a.call_tool(
                "session_join",
                {"session_id": sid, "join_token": created["join_token"],
                 "role": "w", "delivery_mode": "pull"},
            )
            assert joined["participant_token"] != joined["participant_id"]

            # Fresh connection, explicit identity.
            ok = await c.call_tool(
                "session_list_participants",
                {"session_id": sid, "participant_token": joined["participant_token"]},
            )
            assert joined["participant_id"] in {r["participant_id"] for r in ok["roster"]}
            with pytest.raises(RuntimeError, match="not_in_session"):
                await c.call_tool(
                    "session_list_participants",
                    {"session_id": sid, "participant_token": joined["participant_id"]},
                )

            # Resume on another connection by secret token.
            resumed = await b.call_tool(
                "session_resume",
                {"session_id": sid, "participant_token": joined["participant_token"]},
            )
            assert resumed["roster"]


@pytest.mark.asyncio
async def test_mcp_layer_expired_join_token_message_and_no_plaintext_in_ram(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sessionweave.config import load_deployment_defaults
    from sessionweave.connections import ConnectionManager
    from sessionweave.errors import ErrorCode, ToolError
    from sessionweave.registry import SessionRegistry
    from sessionweave.tools import ToolDeps, session_create, session_join
    from tests.unit.fakes import FakeHandle

    defaults = load_deployment_defaults({})
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(),
        metrics=None, firehose=None, store=None,
    )
    deps = ToolDeps(registry, bounds=defaults.bounds)
    created = await session_create(deps, {})
    sid = created["session_id"]

    joined = await session_join(
        deps, FakeHandle(), {"session_id": sid, "join_token": created["join_token"]}
    )
    sess = registry.get(sid)
    assert sess._participants[joined["participant_id"]].issued_token is None
    assert joined["participant_token"]

    real = access._now_ms
    monkeypatch.setattr(access, "_now_ms", lambda: real() + 901_000)
    with pytest.raises(ToolError) as exc:
        await session_join(
            deps, FakeHandle(), {"session_id": sid, "join_token": created["join_token"]}
        )
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
    assert "expired" in exc.value.message
    assert "new invite link" in exc.value.message
