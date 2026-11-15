"""POST /v1/sessions rejects a bad credential before reading the body."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.testclient import TestClient

from sessionweave import http_api
from sessionweave.accounts import AccountStore
from sessionweave.persistence import PersistenceStore


def _app(accounts: AccountStore | None, mode: str) -> Starlette:
    deps: Any = SimpleNamespace(
        accounts=accounts, admin_token="adm", session_creation=mode,
        auth_limiter=SimpleNamespace(
            is_throttled=lambda *a: False, record_failure=lambda *a: None
        ),
    )
    app = Starlette(routes=http_api.routes())
    app.state.tool_deps = deps
    return app


@pytest.fixture
def body_guard(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    reads: list[int] = []

    async def _spy(request: Request) -> dict[str, Any]:
        reads.append(1)
        return {}

    monkeypatch.setattr(http_api, "_read_json_body", _spy)
    return reads


@pytest.mark.asyncio
async def test_account_mode_no_bearer_does_not_read_body(
    tmp_path: Path, body_guard: list[int]
) -> None:
    store = PersistenceStore(str(tmp_path / "t.db"))
    await store.init(pool_size=2)
    with TestClient(_app(AccountStore(store), "account")) as c:
        r = c.post("/v1/sessions", json={"x": 1})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "account_token_required"
    assert body_guard == []
    await store.close()


@pytest.mark.asyncio
async def test_invalid_bearer_does_not_read_body(
    tmp_path: Path, body_guard: list[int]
) -> None:
    store = PersistenceStore(str(tmp_path / "t.db"))
    await store.init(pool_size=2)
    with TestClient(_app(AccountStore(store), "account")) as c:
        r = c.post("/v1/sessions", json={"x": 1}, headers={"Authorization": "Bearer nope"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_account_token"
    assert body_guard == []
    await store.close()


def test_invalid_bearer_without_accounts_does_not_read_body(body_guard: list[int]) -> None:
    with TestClient(_app(None, "open")) as c:
        r = c.post("/v1/sessions", json={}, headers={"Authorization": "Bearer nope"})
    assert r.json()["error"]["code"] == "invalid_account_token"
    assert body_guard == []


def test_admin_bearer_reads_body(
    body_guard: list[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_create(deps: Any, body: dict[str, Any], **kw: Any) -> dict[str, Any]:
        assert kw["bearer"] == "adm"
        return {"session_id": "s1"}

    monkeypatch.setattr(http_api, "session_create", _fake_create)
    with TestClient(_app(None, "open")) as c:
        r = c.post("/v1/sessions", json={}, headers={"Authorization": "Bearer adm"})
    assert r.status_code == 201
    assert r.json() == {"session_id": "s1"}
    assert body_guard == [1]


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/v1/accounts"),
        ("PATCH", "/v1/accounts/a1"),
        ("POST", "/v1/accounts/a1/tokens"),
        ("POST", "/v1/me/tokens"),
    ],
)
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer nope"}])
@pytest.mark.asyncio
async def test_account_routes_do_not_read_body_unauthenticated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    headers: dict[str, str],
) -> None:
    from sessionweave import accounts_api

    reads: list[int] = []

    async def _spy(request: Request) -> dict[str, Any]:
        reads.append(1)
        return {}

    monkeypatch.setattr(accounts_api, "_read_json_body", _spy)
    store = PersistenceStore(str(tmp_path / "t.db"))
    await store.init(pool_size=2)
    app = Starlette(routes=accounts_api.routes())
    app.state.tool_deps = _app(AccountStore(store), "account").state.tool_deps
    with TestClient(app) as c:
        r = c.request(method, path, json={"x": 1}, headers=headers)
    assert r.status_code in (401, 403)
    assert reads == []
    await store.close()
