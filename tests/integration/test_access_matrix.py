"""The access matrix: who may read, change purpose, supervise, watch the firehose, delete.

Runs in-process (TestClient) so the clock can be moved; one fresh session per case.
"""
from __future__ import annotations

import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from sessionweave import access
from sessionweave.server import build_app

ADMIN = "admin-secret-for-matrix-tests"
ROLES = ["admin", "creator", "supervisor", "participant", "joiner", "none", "session_id_as_token"]
READERS = {"admin", "creator", "supervisor", "participant", "joiner"}
SUPERVISORS = {"admin", "creator", "supervisor"}
MANAGERS = {"admin", "creator"}
PURPOSE_TEXT = "TOP-SECRET-PURPOSE-TEXT"


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SW_ADMIN_TOKEN", ADMIN)
    with TestClient(build_app()) as c:
        yield c


def _h(token: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"} if token else {}


def _session(c: TestClient) -> tuple[str, dict[str, str | None]]:
    out = c.post("/v1/sessions", json={"purpose": {"instructions": PURPOSE_TEXT}}).json()
    sid = out["session_id"]
    joined = c.post(
        f"/v1/sessions/{sid}/participants", json={"role": "w"}, headers=_h(out["join_token"])
    ).json()
    tokens: dict[str, str | None] = {
        "admin": ADMIN,
        "creator": out["creator_token"],
        "supervisor": out["supervisor_token"],
        "participant": joined["participant_token"],
        "joiner": out["join_token"],
        "none": None,
        "session_id_as_token": sid,
    }
    return sid, tokens


def _allowed(status: int) -> bool:
    return status not in (401, 403, 429)


HTTP_ROWS = {
    "describe": ("GET", "/v1/sessions/{sid}", None, READERS),
    "events": ("GET", "/v1/sessions/{sid}/events", None, READERS),
    "list_participants": ("GET", "/v1/sessions/{sid}/participants", None, READERS),
    "update_purpose": ("PATCH", "/v1/sessions/{sid}/purpose", {"purpose": {"k": 1}}, MANAGERS),
    "supervisor_note": (
        "POST",
        "/v1/sessions/{sid}/supervisor/notes",
        {"kind": "note", "target": "all", "message": "m"},
        SUPERVISORS,
    ),
    "delete": ("DELETE", "/v1/sessions/{sid}", None, MANAGERS),
}


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("row", sorted(HTTP_ROWS))
def test_matrix_http(client: TestClient, row: str, role: str) -> None:
    method, path, body, allowed_roles = HTTP_ROWS[row]
    sid, tokens = _session(client)
    r = client.request(method, path.format(sid=sid), json=body, headers=_h(tokens[role]))
    assert _allowed(r.status_code) is (role in allowed_roles), (row, role, r.status_code, r.text)


@pytest.mark.parametrize("role", ROLES)
def test_matrix_invite_brief(client: TestClient, role: str) -> None:
    sid, tokens = _session(client)
    tok = tokens[role]
    r = client.get(f"/invite/{sid}", params={"jt": tok} if tok else {})
    assert (r.status_code == 200) is (role in READERS), (role, r.status_code)
    if role not in READERS:
        assert r.status_code == 403


@pytest.mark.parametrize("role", ROLES)
def test_matrix_firehose(client: TestClient, role: str) -> None:
    sid, tokens = _session(client)
    url = f"/v1/sessions/{sid}/firehose"
    if tokens[role]:
        url += f"?token={tokens[role]}"
    if role in SUPERVISORS:
        with client.websocket_connect(url):
            pass
    else:
        with pytest.raises(WebSocketDisconnect), client.websocket_connect(url) as ws:
            ws.receive_text()


def test_invite_brief_without_jt_is_403_markdown(client: TestClient) -> None:
    sid, _ = _session(client)
    r = client.get(f"/invite/{sid}")
    assert r.status_code == 403
    assert r.headers["content-type"].startswith("text/markdown")
    assert "missing its join token or has expired" in r.text
    assert "Ask the session creator for a new invite link." in r.text
    assert PURPOSE_TEXT not in r.text


def test_invite_brief_expired_jt(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    sid, tokens = _session(client)
    real = access._now_ms()
    monkeypatch.setattr(access, "_now_ms", lambda: real + 16 * 60 * 1000)
    r = client.get(f"/invite/{sid}", params={"jt": tokens["joiner"]})
    assert r.status_code == 403
    assert r.headers["content-type"].startswith("text/markdown")
    assert "expired" in r.text
    assert PURPOSE_TEXT not in r.text


def test_invite_brief_jt_with_trailing_newline_ok(client: TestClient) -> None:
    sid, tokens = _session(client)
    r = client.get(f"/invite/{sid}?jt={tokens['joiner']}%0A")
    assert r.status_code == 200
    assert PURPOSE_TEXT in r.text
    assert f"Bearer {tokens['joiner']}" in r.text


def test_invite_brief_unknown_session_is_404(client: TestClient) -> None:
    assert client.get("/invite/nope", params={"jt": "x"}).status_code == 404


def test_update_purpose_applies_for_creator(client: TestClient) -> None:
    sid, tokens = _session(client)
    r = client.patch(
        f"/v1/sessions/{sid}/purpose", json={"purpose": {"k": 2}}, headers=_h(tokens["creator"])
    )
    assert r.status_code == 200 and r.json()["purpose"] == {"k": 2}
    got = client.get(f"/v1/sessions/{sid}", headers=_h(tokens["joiner"])).json()
    assert got["purpose"] == {"k": 2}


def test_no_admin_token_session_id_refused_everywhere(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without SW_ADMIN_TOKEN there is no admin; the session id never works as a credential."""
    monkeypatch.delenv("SW_ADMIN_TOKEN", raising=False)
    with TestClient(build_app()) as c:
        # A fresh session per probe keeps the failed-token throttle out of the picture.
        for method, path, body, _roles in HTTP_ROWS.values():
            sid = _session_no_admin(c)
            r = c.request(method, path.format(sid=sid), json=body, headers=_h(sid))
            assert r.status_code in (401, 403), (method, path, r.status_code)
        sid = _session_no_admin(c)
        assert c.get(f"/invite/{sid}", params={"jt": sid}).status_code == 403
        sid = _session_no_admin(c)
        with pytest.raises(WebSocketDisconnect), c.websocket_connect(
            f"/v1/sessions/{sid}/firehose?token={sid}"
        ) as ws:
            ws.receive_text()
        sid = _session_no_admin(c)
        assert c.delete(f"/v1/sessions/{sid}", headers=_h("")).status_code in (401, 403)


def _session_no_admin(c: TestClient) -> str:
    return str(c.post("/v1/sessions", json={}).json()["session_id"])
