from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from sessionweave import accounts_cli


def _run(
    argv: list[str], handler: Any, monkeypatch: pytest.MonkeyPatch
) -> int:
    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        accounts_cli, "_client",
        lambda url, token: httpx.Client(
            base_url=url, transport=transport, headers={"Authorization": f"Bearer {token}"}
        ),
    )
    monkeypatch.delenv("SW_URL", raising=False)
    monkeypatch.delenv("SW_ADMIN_TOKEN", raising=False)
    return accounts_cli.main(["--admin-token", "adm", *argv])


def test_create_pending_posts_body_with_admin_bearer(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(201, json={"account_id": "acc_1", "name": "alice"})

    assert _run(["create", "alice", "--pending"], handler, monkeypatch) == 0
    req = seen[0]
    assert req.method == "POST" and req.url.path == "/v1/accounts"
    assert req.headers["authorization"] == "Bearer adm"
    assert json.loads(req.content) == {"name": "alice", "status": "pending"}
    assert json.loads(capsys.readouterr().out)["account_id"] == "acc_1"


def test_create_with_contact(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(201, json={})

    assert _run(["create", "bob", "--contact", "b@x"], handler, monkeypatch) == 0
    assert json.loads(seen[0].content) == {"name": "bob", "contact": "b@x"}


def test_conflict_exits_1_with_broker_message(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409, json={"error": {"code": "account_name_taken", "message": "name in use: alice"}}
        )

    assert _run(["create", "alice"], handler, monkeypatch) == 1
    cap = capsys.readouterr()
    assert "name in use: alice" in cap.err
    assert cap.out == ""


def test_enable_disable_show_list_token_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        if req.method == "DELETE" and "tokens" in req.url.path:
            return httpx.Response(204)
        return httpx.Response(200, json={})

    cases = [
        (["enable", "acc_1"], "PATCH", "/v1/accounts/acc_1", {"status": "active"}),
        (["disable", "acc_1"], "PATCH", "/v1/accounts/acc_1", {"status": "disabled"}),
        (["show", "acc_1"], "GET", "/v1/accounts/acc_1", None),
        (["list"], "GET", "/v1/accounts", None),
        (
            ["token-create", "acc_1", "--label", "L", "--expires-at", "2030-01-01T00:00:00Z"],
            "POST", "/v1/accounts/acc_1/tokens",
            {"label": "L", "expires_at": "2030-01-01T00:00:00Z"},
        ),
        (["token-revoke", "acc_1", "tk"], "DELETE", "/v1/accounts/acc_1/tokens/tk", None),
        (["delete", "acc_1", "--yes"], "DELETE", "/v1/accounts/acc_1", None),
    ]
    for argv, method, path, body in cases:
        seen.clear()
        assert _run(argv, handler, monkeypatch) == 0, argv
        assert (seen[0].method, seen[0].url.path) == (method, path), argv
        if body is not None:
            assert json.loads(seen[0].content) == body, argv


def test_delete_requires_yes(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise AssertionError("no request expected")

    with pytest.raises(SystemExit):
        _run(["delete", "acc_1"], handler, monkeypatch)


def test_missing_admin_token_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("SW_ADMIN_TOKEN", raising=False)
    assert accounts_cli.main(["list"]) == 1
    assert "admin token" in capsys.readouterr().err


def test_ids_are_url_quoted(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(204)

    assert _run(["token-revoke", "a/b", "t?x"], handler, monkeypatch) == 0
    assert seen[0].url.raw_path == b"/v1/accounts/a%2Fb/tokens/t%3Fx"


def test_non_json_success_body_prints_raw(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="plain text")

    assert _run(["list"], handler, monkeypatch) == 0
    assert capsys.readouterr().out.strip() == "plain text"


@pytest.mark.parametrize(
    "given", ["http://h:8080/mcp", "http://h:8080/mcp/", "http://h:8080/", "http://h:8080"]
)
def test_url_from_mcp_endpoint_is_normalised(
    given: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(200, json={"accounts": []})

    assert _run(["--url", given, "list"], handler, monkeypatch) == 0
    assert str(seen[0].url) == "http://h:8080/v1/accounts"


def test_network_commands_map_to_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        return httpx.Response(204 if req.url.path.endswith("/deny") else 200, json={})

    cases: list[tuple[list[str], str, str, str, Any]] = [
        (
            ["token-create", "acc_1", "--label", "L", "--network-policy", "first_use",
             "--network", "10.0.0.0/8", "--network", "192.0.2.1"],
            "POST", "/v1/accounts/acc_1/tokens", "",
            {"label": "L", "network_policy": "first_use",
             "approved_networks": ["10.0.0.0/8", "192.0.2.1"]},
        ),
        (["token-networks", "acc_1", "tk"], "GET",
         "/v1/accounts/acc_1/tokens/tk/networks", "", None),
        (
            ["token-networks", "acc_1", "tk", "--policy", "approved_only",
             "--network", "10.1.0.0/16"],
            "PUT", "/v1/accounts/acc_1/tokens/tk/networks", "",
            {"network_policy": "approved_only", "approved_networks": ["10.1.0.0/16"]},
        ),
        (
            ["token-networks", "acc_1", "tk", "--no-networks"],
            "PUT", "/v1/accounts/acc_1/tokens/tk/networks", "", {"approved_networks": []},
        ),
        (["approvals", "acc_1"], "GET", "/v1/accounts/acc_1/network-approvals", "", None),
        (
            ["approve", "acc_1", "apr_1"], "POST",
            "/v1/accounts/acc_1/network-approvals/apr_1/approve", "", {"scope": "ip"},
        ),
        (
            ["approve", "acc_1", "apr_1", "--scope", "network"], "POST",
            "/v1/accounts/acc_1/network-approvals/apr_1/approve", "", {"scope": "network"},
        ),
        (
            ["deny", "acc_1", "apr_1"], "POST",
            "/v1/accounts/acc_1/network-approvals/apr_1/deny", "", None,
        ),
        (["events"], "GET", "/v1/accounts/events", "", None),
        (
            ["events", "--account", "acc_1", "--after", "7", "--wait", "20"],
            "GET", "/v1/accounts/events", "account_id=acc_1&after=7&wait=20.0", None,
        ),
    ]
    for argv, method, path, query, body in cases:
        seen.clear()
        assert _run(argv, handler, monkeypatch) == 0, argv
        req = seen[0]
        assert (req.method, req.url.path, req.url.query.decode()) == (
            method, path, query,
        ), argv
        if body is not None:
            assert json.loads(req.content) == body, argv


def test_invalid_policy_and_scope_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise AssertionError("no request expected")

    for argv in (
        ["token-create", "a", "--label", "L", "--network-policy", "bogus"],
        ["approve", "a", "apr", "--scope", "all"],
    ):
        with pytest.raises(SystemExit):
            _run(argv, handler, monkeypatch)
