"""``sessionweave-accounts`` account-token mode (``--token``), against the in-process app."""
from __future__ import annotations

import contextlib
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from sessionweave import accounts_cli
from sessionweave.server import build_app

ADMIN = "adm-cli-user-mode"


class _Proxy:
    """The slice of ``httpx.Client`` the CLI uses, routed to the in-process app."""

    def __init__(self, tc: TestClient, token: str) -> None:
        self._tc = tc
        self._headers = {"Authorization": f"Bearer {token}"}

    def request(self, method: str, url: str, **kw: Any) -> httpx.Response:
        kw.pop("timeout", None)
        return self._tc.request(method, url, headers=self._headers, **kw)


@pytest.fixture
def app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("SW_ADMIN_TOKEN", ADMIN)
    monkeypatch.setenv("SW_SESSION_CREATION", "account")
    monkeypatch.setenv("SW_PERSISTENCE_DB_PATH", str(tmp_path / "cli.db"))
    with TestClient(build_app()) as tc:
        monkeypatch.setattr(
            accounts_cli, "_client",
            lambda url, token: contextlib.nullcontext(_Proxy(tc, token)),
        )
        for var in ("SW_URL", "SW_ADMIN_TOKEN", "SW_ACCOUNT_TOKEN"):
            monkeypatch.delenv(var, raising=False)
        yield tc


def _admin(tc: TestClient) -> dict[str, str]:
    return {"Authorization": f"Bearer {ADMIN}"}


def _account(tc: TestClient, name: str = "alice") -> tuple[str, str, str]:
    """(account_id, token, token_id) of a fresh account."""
    acc = tc.post("/v1/accounts", json={"name": name}, headers=_admin(tc)).json()
    tok = tc.post(
        f"/v1/accounts/{acc['account_id']}/tokens", json={"label": "first"}, headers=_admin(tc)
    ).json()
    return acc["account_id"], tok["token"], tok["token_id"]


def _cli(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, Any, str]:
    code = accounts_cli.main(argv)
    cap = capsys.readouterr()
    try:
        out = json.loads(cap.out) if cap.out.strip() else None
    except ValueError:
        out = cap.out
    return code, out, cap.err


def test_me_and_my_tokens(app: TestClient, capsys: pytest.CaptureFixture[str]) -> None:
    aid, token, tid = _account(app)
    code, out, _ = _cli(["--token", token, "me"], capsys)
    assert code == 0 and out["account_id"] == aid
    assert [t["token_id"] for t in out["tokens"]] == [tid]
    code, out, _ = _cli(["--token", token, "my-tokens"], capsys)
    assert code == 0 and [t["token_id"] for t in out["tokens"]] == [tid]


def test_token_from_env(
    app: TestClient, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    aid, token, _ = _account(app)
    monkeypatch.setenv("SW_ACCOUNT_TOKEN", token)
    code, out, _ = _cli(["me"], capsys)
    assert code == 0 and out["account_id"] == aid


def test_my_token_create_prints_plaintext_once_and_it_works(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    aid, token, _ = _account(app)
    code, out, err = _cli(
        ["--token", token, "my-token-create", "--label", "laptop",
         "--network-policy", "first_use", "--network", "10.0.0.0/8"], capsys,
    )
    assert code == 0, err
    new = out["token"]
    assert new.startswith("swa_") and out["label"] == "laptop"
    assert out["network_policy"] == "first_use" and out["approved_networks"] == ["10.0.0.0/8"]
    assert err == ""
    # (that one is network-bound; a plain one is usable from the test client)
    code, out, _ = _cli(["--token", token, "my-token-create", "--label", "plain"], capsys)
    new = out["token"]
    code, me, _ = _cli(["--token", new, "me"], capsys)
    assert code == 0 and me["account_id"] == aid
    # Listing never shows a plaintext.
    code, listed, _ = _cli(["--token", new, "my-tokens"], capsys)
    assert new not in json.dumps(listed) and token not in json.dumps(listed)


def test_my_token_create_with_revoke_replaces_the_old_token(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    _, token, tid = _account(app)
    code, out, err = _cli(
        ["--token", token, "my-token-create", "--label", "next", "--revoke", tid], capsys
    )
    assert code == 0, err
    assert out["revoked_token_id"] == tid
    code, _, err = _cli(["--token", token, "me"], capsys)
    assert code == 1 and "invalid_account_token" in err
    code, me, _ = _cli(["--token", out["token"], "my-tokens"], capsys)
    old = next(t for t in me["tokens"] if t["token_id"] == tid)
    assert old["revoked_at"] is not None


def test_my_token_create_revoke_unknown_id_mints_nothing(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    _, token, _ = _account(app)
    code, out, err = _cli(
        ["--token", token, "my-token-create", "--label", "x", "--revoke", "nope"], capsys
    )
    assert code == 1 and out is None and "token_not_found" in err and "nope" not in err
    code, listed, _ = _cli(["--token", token, "my-tokens"], capsys)
    assert len(listed["tokens"]) == 1


def test_my_token_revoke_and_networks(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    _, token, _ = _account(app)
    _, made, _ = _cli(["--token", token, "my-token-create", "--label", "b"], capsys)
    code, out, _ = _cli(["--token", token, "my-token-networks", made["token_id"]], capsys)
    assert code == 0 and out == {"network_policy": "any", "approved_networks": []}
    code, out, _ = _cli(
        ["--token", token, "my-token-networks", made["token_id"], "--policy", "approved_only",
         "--network", "192.0.2.0/24"], capsys,
    )
    assert code == 0 and out["network_policy"] == "approved_only"
    assert out["approved_networks"] == ["192.0.2.0/24"]
    code, out, _ = _cli(
        ["--token", token, "my-token-networks", made["token_id"], "--no-networks"], capsys
    )
    assert code == 0 and out["approved_networks"] == []
    code, out, err = _cli(["--token", token, "my-token-revoke", made["token_id"]], capsys)
    assert code == 0 and out is None and err == ""
    code, _, err = _cli(["--token", made["token"], "me"], capsys)
    assert code == 1 and "invalid_account_token" in err


def test_my_approvals_approve_deny(app: TestClient, capsys: pytest.CaptureFixture[str]) -> None:
    _, token, _ = _account(app)
    code, out, _ = _cli(["--token", token, "my-approvals"], capsys)
    assert code == 0 and out == {"approvals": []}
    code, _, err = _cli(["--token", token, "my-approve", "apr_missing"], capsys)
    assert code == 1 and "approval_not_found" in err
    code, _, err = _cli(["--token", token, "my-deny", "apr_missing"], capsys)
    assert code == 1 and "approval_not_found" in err


def test_user_command_with_admin_token_only_is_a_clear_error(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    code, out, err = _cli(["--admin-token", ADMIN, "me"], capsys)
    assert code == 1 and out is None
    assert "account token" in err and "--token" in err and ADMIN not in err


def test_admin_command_with_account_token_only_is_a_clear_error(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    _, token, _ = _account(app)
    code, out, err = _cli(["--token", token, "list"], capsys)
    assert code == 1 and out is None
    assert "admin token" in err and "--admin-token" in err and token not in err


def test_account_token_passed_as_admin_token_is_named(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    _, token, _ = _account(app)
    code, _, err = _cli(["--admin-token", token, "list"], capsys)
    assert code == 1 and "account token" in err and "--token" in err and token not in err


def test_non_account_token_given_as_token_is_named(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _, err = _cli(["--token", ADMIN, "me"], capsys)
    assert code == 1 and "not an account token" in err and ADMIN not in err


def test_admin_tokens_lists_an_accounts_tokens(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    aid, _, tid = _account(app)
    code, out, _ = _cli(["--admin-token", ADMIN, "tokens", aid], capsys)
    assert code == 0 and [t["token_id"] for t in out["tokens"]] == [tid]
    code, _, err = _cli(["--admin-token", ADMIN, "tokens", "acc_missing"], capsys)
    assert code == 1 and "account_not_found" in err


def test_admin_token_create_with_revoke(
    app: TestClient, capsys: pytest.CaptureFixture[str]
) -> None:
    aid, token, tid = _account(app)
    code, out, err = _cli(
        ["--admin-token", ADMIN, "token-create", aid, "--label", "repl", "--revoke", tid], capsys
    )
    assert code == 0, err
    assert out["revoked_token_id"] == tid and out["token"].startswith("swa_")
    code, _, err = _cli(["--token", token, "me"], capsys)
    assert code == 1 and "invalid_account_token" in err
    code, out2, _ = _cli(["--admin-token", ADMIN, "token-create", aid, "--label", "x",
                          "--revoke", "nope"], capsys)
    assert code == 1 and out2 is None


def test_revoke_failure_after_mint_still_prints_the_new_token(
    app: TestClient, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The minted plaintext is never lost: if the revoke step fails, the new
    token is still printed (stdout) and the failure reported (stderr, exit 1)."""
    _, token, tid = _account(app)
    real = _Proxy.request

    def failing(self: _Proxy, method: str, url: str, **kw: Any) -> httpx.Response:
        if method == "DELETE":
            return httpx.Response(
                503, json={"error": {"code": "persistence_failed", "message": "disk"}}
            )
        return real(self, method, url, **kw)

    monkeypatch.setattr(_Proxy, "request", failing)
    code, out, err = _cli(
        ["--token", token, "my-token-create", "--label", "n", "--revoke", tid], capsys
    )
    assert code == 1
    assert out["token"].startswith("swa_") and "revoked_token_id" not in out
    assert tid in err and "persistence_failed" in err
