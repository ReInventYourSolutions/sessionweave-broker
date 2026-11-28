"""``sessionweave-accounts``: broker accounts and their tokens over the REST API.

Two credentials, chosen by the command:

* admin commands (``create``, ``list``, ``show``, ``tokens``, ``token-create``,
  ...) need the relay admin token (``--admin-token`` or ``SW_ADMIN_TOKEN``);
* ``me`` and the ``my-*`` commands act on the caller's own account and need one
  of its account tokens (``--token`` or ``SW_ACCOUNT_TOKEN``).

Prints the broker's JSON answer; a non-2xx answer prints its message on stderr
and exits 1. A minted token's plaintext is printed once, on stdout; no
credential is ever echoed in an error.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.parse import quote, urlencode

import httpx

from sessionweave.accounts import TOKEN_PREFIX

DEFAULT_URL = "http://127.0.0.1:8765"  # the broker's default SW_PORT
POLICIES = ("any", "first_use", "approved_only")
# Commands that act on the caller's own account (account token).
USER_COMMANDS = frozenset({
    "me", "my-tokens", "my-token-create", "my-token-revoke", "my-token-networks",
    "my-approvals", "my-approve", "my-deny",
})


def _base_url(url: str) -> str:
    """SW_URL elsewhere is the MCP endpoint (``.../mcp``); the REST base is its parent."""
    url = url.rstrip("/")
    if url.endswith("/mcp"):
        url = url[: -len("/mcp")]
    return url.rstrip("/")


def _client(url: str, token: str) -> httpx.Client:
    return httpx.Client(
        base_url=url, headers={"Authorization": f"Bearer {token}"}, timeout=30
    )


def _token_create_args(tc: argparse.ArgumentParser) -> None:
    tc.add_argument("--label", required=True)
    tc.add_argument("--expires-at", help="ISO 8601 expiry, e.g. 2030-01-01T00:00:00Z")
    tc.add_argument("--network-policy", choices=POLICIES,
                    help="network policy of the token (default any)")
    tc.add_argument("--network", action="append", metavar="CIDR",
                    help="approved network (CIDR or IP); repeatable")
    tc.add_argument("--revoke", metavar="TOKEN_ID",
                    help="after minting, revoke this token (replace it); checked first")


def _networks_args(tn: argparse.ArgumentParser) -> None:
    tn.add_argument("token_id")
    tn.add_argument("--policy", choices=POLICIES, help="new network policy")
    tn.add_argument("--network", action="append", metavar="CIDR",
                    help="replace the approved networks with these; repeatable")
    tn.add_argument("--no-networks", action="store_true",
                    help="replace the approved networks with an empty list")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sessionweave-accounts",
        description="Manage SessionWeave broker accounts (admin token) "
        "or your own account's tokens (account token).",
    )
    p.add_argument("--url", default=os.environ.get("SW_URL") or DEFAULT_URL,
                   help="broker base URL (env SW_URL, default %(default)s)")
    p.add_argument("--admin-token", default=os.environ.get("SW_ADMIN_TOKEN"),
                   help="relay admin token, for admin commands (env SW_ADMIN_TOKEN)")
    p.add_argument("--token", default=os.environ.get("SW_ACCOUNT_TOKEN"),
                   help="your account token, for 'me' and 'my-*' commands "
                   "(env SW_ACCOUNT_TOKEN)")
    sub = p.add_subparsers(dest="command", required=True)

    # ---- admin commands (relay admin token) ----
    c = sub.add_parser("create", help="[admin] create an account")
    c.add_argument("name")
    c.add_argument("--contact")
    c.add_argument("--pending", action="store_true", help="create it pending, not active")
    sub.add_parser("list", help="[admin] list accounts")
    for name, helptext in (
        ("show", "[admin] show an account and its tokens"),
        ("tokens", "[admin] list an account's tokens"),
        ("enable", "[admin] activate an account and lift its sessions' suspension"),
        ("disable", "[admin] disable an account and suspend its sessions"),
    ):
        sub.add_parser(name, help=helptext).add_argument("account_id")
    d = sub.add_parser("delete", help="[admin] delete an account and all its sessions")
    d.add_argument("account_id")
    d.add_argument("--yes", action="store_true", required=True,
                   help="confirm the irreversible delete")
    tc = sub.add_parser("token-create", help="[admin] mint an API token (shown once)")
    tc.add_argument("account_id")
    _token_create_args(tc)
    tn = sub.add_parser("token-networks", help="[admin] show or change a token's networks")
    tn.add_argument("account_id")
    _networks_args(tn)
    ap = sub.add_parser("approvals", help="[admin] list pending network approvals")
    ap.add_argument("account_id")
    ao = sub.add_parser("approve", help="[admin] approve a pending network approval")
    ao.add_argument("account_id")
    ao.add_argument("approval_id")
    ao.add_argument("--scope", choices=("ip", "network"), default="ip",
                    help="approve the exact IP or its network (/24, /64); default ip")
    de = sub.add_parser("deny", help="[admin] deny a pending network approval")
    de.add_argument("account_id")
    de.add_argument("approval_id")
    ev = sub.add_parser("events", help="[admin] read account events (all accounts, or one)")
    ev.add_argument("--account", help="only this account id")
    ev.add_argument("--after", type=int, help="only events after this event id")
    ev.add_argument("--wait", type=float, help="long-poll up to this many seconds (max 60)")
    tr = sub.add_parser("token-revoke", help="[admin] revoke an API token")
    tr.add_argument("account_id")
    tr.add_argument("token_id")

    # ---- self-service commands (account token) ----
    sub.add_parser("me", help="[account] show your account and its tokens")
    sub.add_parser("my-tokens", help="[account] list your account's tokens")
    mc = sub.add_parser("my-token-create", help="[account] mint a token (shown once)")
    _token_create_args(mc)
    mr = sub.add_parser("my-token-revoke", help="[account] revoke one of your tokens")
    mr.add_argument("token_id")
    _networks_args(sub.add_parser(
        "my-token-networks", help="[account] show or change one of your tokens' networks"
    ))
    sub.add_parser("my-approvals", help="[account] list your pending network approvals")
    ma = sub.add_parser("my-approve", help="[account] approve a pending network approval")
    ma.add_argument("approval_id")
    ma.add_argument("--scope", choices=("ip", "network"), default="ip",
                    help="approve the exact IP or its network (/24, /64); default ip")
    md = sub.add_parser("my-deny", help="[account] deny a pending network approval")
    md.add_argument("approval_id")
    return p


def _q(value: str) -> str:
    return quote(value, safe="")


def _create_body(args: argparse.Namespace) -> dict[str, Any]:
    body: dict[str, Any] = {"label": args.label}
    if args.expires_at:
        body["expires_at"] = args.expires_at
    if args.network_policy:
        body["network_policy"] = args.network_policy
    if args.network:
        body["approved_networks"] = args.network
    return body


def _networks_request(
    args: argparse.Namespace, url: str
) -> tuple[str, str, dict[str, Any] | None]:
    changes: dict[str, Any] = {}
    if args.policy:
        changes["network_policy"] = args.policy
    if args.network:
        changes["approved_networks"] = args.network
    elif args.no_networks:
        changes["approved_networks"] = []
    return ("PUT", url, changes) if changes else ("GET", url, None)


def _tokens_path(args: argparse.Namespace) -> str:
    """Collection path of the tokens a create/revoke command acts on."""
    if args.command in USER_COMMANDS:
        return "/v1/me/tokens"
    return f"/v1/accounts/{_q(args.account_id)}/tokens"


def _request(args: argparse.Namespace) -> tuple[str, str, dict[str, Any] | None]:
    cmd = args.command
    if cmd in USER_COMMANDS:
        return _user_request(args)
    if cmd == "create":
        body: dict[str, Any] = {"name": args.name}
        if args.contact is not None:
            body["contact"] = args.contact
        if args.pending:
            body["status"] = "pending"
        return "POST", "/v1/accounts", body
    if cmd == "list":
        return "GET", "/v1/accounts", None
    if cmd == "events":
        query = {
            k: v for k, v in (
                ("account_id", args.account), ("after", args.after), ("wait", args.wait)
            ) if v is not None
        }
        return "GET", "/v1/accounts/events" + (f"?{urlencode(query)}" if query else ""), None
    path = f"/v1/accounts/{_q(getattr(args, 'account_id', ''))}"
    if cmd in ("show", "tokens"):
        return "GET", path, None
    if cmd in ("enable", "disable"):
        return "PATCH", path, {"status": "active" if cmd == "enable" else "disabled"}
    if cmd == "delete":
        return "DELETE", path, None
    if cmd == "token-create":
        return "POST", f"{path}/tokens", _create_body(args)
    if cmd == "token-networks":
        return _networks_request(args, f"{path}/tokens/{_q(args.token_id)}/networks")
    if cmd == "approvals":
        return "GET", f"{path}/network-approvals", None
    if cmd in ("approve", "deny"):
        url = f"{path}/network-approvals/{_q(args.approval_id)}/{cmd}"
        return "POST", url, ({"scope": args.scope} if cmd == "approve" else None)
    return "DELETE", f"{path}/tokens/{_q(args.token_id)}", None  # token-revoke


def _user_request(args: argparse.Namespace) -> tuple[str, str, dict[str, Any] | None]:
    cmd = args.command
    if cmd == "me":
        return "GET", "/v1/me", None
    if cmd == "my-tokens":
        return "GET", "/v1/me/tokens", None
    if cmd == "my-token-create":
        return "POST", "/v1/me/tokens", _create_body(args)
    if cmd == "my-token-revoke":
        return "DELETE", f"/v1/me/tokens/{_q(args.token_id)}", None
    if cmd == "my-token-networks":
        return _networks_request(args, f"/v1/me/tokens/{_q(args.token_id)}/networks")
    if cmd == "my-approvals":
        return "GET", "/v1/me/network-approvals", None
    url = f"/v1/me/network-approvals/{_q(args.approval_id)}"
    if cmd == "my-approve":
        return "POST", f"{url}/approve", {"scope": args.scope}
    return "POST", f"{url}/deny", None  # my-deny


def _error_message(resp: httpx.Response) -> str:
    try:
        err = resp.json()["error"]
        return f"{err['message']} ({err['code']})"
    except Exception:
        return resp.text or f"HTTP {resp.status_code}"


def _credential(args: argparse.Namespace) -> tuple[str | None, str | None]:
    """(token, None) for the command's credential, or (None, error message).

    Messages name the flags and env vars, never a credential's value."""
    if args.command in USER_COMMANDS:
        token = args.token
        if not token:
            if args.admin_token:
                return None, (
                    f"'{args.command}' acts on your own account and needs one of its "
                    "account tokens (--token or SW_ACCOUNT_TOKEN); the relay admin token "
                    "is not an account token (admins use 'show', 'tokens', 'token-create')"
                )
            return None, "an account token is required (--token or SW_ACCOUNT_TOKEN)"
        if not token.startswith(TOKEN_PREFIX):
            return None, (
                f"the value given with --token / SW_ACCOUNT_TOKEN is not an account token "
                f"(account tokens start with '{TOKEN_PREFIX}')"
            )
        return token, None
    token = args.admin_token
    if not token:
        if args.token:
            return None, (
                f"'{args.command}' is an admin command and needs the relay admin token "
                "(--admin-token or SW_ADMIN_TOKEN); an account token cannot administer "
                "accounts (use 'me' and the 'my-*' commands with it)"
            )
        return None, "an admin token is required (--admin-token or SW_ADMIN_TOKEN)"
    if token.startswith(TOKEN_PREFIX):
        return None, (
            "the value given with --admin-token / SW_ADMIN_TOKEN is an account token, "
            "not the relay admin token (use it with --token and the 'me' / 'my-*' commands)"
        )
    return token, None


def _print_json(resp: httpx.Response) -> None:
    if resp.content:
        try:
            print(json.dumps(resp.json(), indent=2))
        except ValueError:
            print(resp.text)


def _check_revocable(client: Any, args: argparse.Namespace) -> str | None:
    """None when ``--revoke`` names a token of the account; else the error."""
    listing = "/v1/me/tokens" if args.command in USER_COMMANDS else (
        f"/v1/accounts/{_q(args.account_id)}"
    )
    resp = client.request("GET", listing)
    if not resp.is_success:
        return _error_message(resp)
    tokens = resp.json().get("tokens", [])
    if not any(t.get("token_id") == args.revoke for t in tokens):
        return f"no token {args.revoke} on this account; nothing minted (token_not_found)"
    return None


def _create_and_replace(client: Any, args: argparse.Namespace) -> int:
    """Mint a token, then revoke ``--revoke``. The minted plaintext is
    always printed, even when the revoke step fails."""
    problem = _check_revocable(client, args)
    if problem is not None:
        print(f"error: {problem}", file=sys.stderr)
        return 1
    method, path, body = _request(args)
    resp = client.request(method, path, json=body)
    if not resp.is_success:
        print(f"error: {_error_message(resp)}", file=sys.stderr)
        return 1
    minted: dict[str, Any] = resp.json()
    revoked = client.request("DELETE", f"{_tokens_path(args)}/{_q(args.revoke)}")
    if revoked.is_success:
        minted["revoked_token_id"] = args.revoke
    print(json.dumps(minted, indent=2))
    if not revoked.is_success:
        print(
            f"error: new token minted (above), but token {args.revoke} was not revoked: "
            f"{_error_message(revoked)}; revoke it again",
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    args.url = _base_url(args.url)
    token, problem = _credential(args)
    if token is None:
        print(f"error: {problem}", file=sys.stderr)
        return 1
    replacing = args.command in ("token-create", "my-token-create") and args.revoke
    try:
        with _client(args.url, token) as client:
            if replacing:
                return _create_and_replace(client, args)
            method, path, body = _request(args)
            resp = client.request(
                method, path, json=body,
                timeout=max(30.0, (getattr(args, "wait", None) or 0) + 10.0),
            )
    except httpx.HTTPError as exc:
        print(f"error: cannot reach {args.url}: {exc}", file=sys.stderr)
        return 1
    if not resp.is_success:
        print(f"error: {_error_message(resp)}", file=sys.stderr)
        return 1
    if args.command == "tokens":
        print(json.dumps({"tokens": resp.json().get("tokens", [])}, indent=2))
        return 0
    _print_json(resp)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
