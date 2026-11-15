"""``sessionweave-accounts``: administer broker accounts over the REST API.

Needs the relay admin token (``--admin-token`` or ``SW_ADMIN_TOKEN``). Prints
the broker's JSON answer; a non-2xx answer prints its message on stderr and
exits 1.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.parse import quote

import httpx

DEFAULT_URL = "http://127.0.0.1:8080"


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


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sessionweave-accounts", description="Administer SessionWeave broker accounts."
    )
    p.add_argument("--url", default=os.environ.get("SW_URL") or DEFAULT_URL,
                   help="broker base URL (env SW_URL, default %(default)s)")
    p.add_argument("--admin-token", default=os.environ.get("SW_ADMIN_TOKEN"),
                   help="relay admin token (env SW_ADMIN_TOKEN)")
    sub = p.add_subparsers(dest="command", required=True)

    c = sub.add_parser("create", help="create an account")
    c.add_argument("name")
    c.add_argument("--contact")
    c.add_argument("--pending", action="store_true", help="create it pending, not active")
    sub.add_parser("list", help="list accounts")
    for name, helptext in (
        ("show", "show an account and its tokens"),
        ("enable", "activate an account and resume its sessions"),
        ("disable", "disable an account and suspend its sessions"),
    ):
        sub.add_parser(name, help=helptext).add_argument("account_id")
    d = sub.add_parser("delete", help="delete an account and all its sessions")
    d.add_argument("account_id")
    d.add_argument("--yes", action="store_true", required=True,
                   help="confirm the irreversible delete")
    tc = sub.add_parser("token-create", help="mint an API token (shown once)")
    tc.add_argument("account_id")
    tc.add_argument("--label", required=True)
    tc.add_argument("--expires-at", help="ISO 8601 expiry, e.g. 2030-01-01T00:00:00Z")
    tr = sub.add_parser("token-revoke", help="revoke an API token")
    tr.add_argument("account_id")
    tr.add_argument("token_id")
    return p


def _request(args: argparse.Namespace) -> tuple[str, str, dict[str, Any] | None]:
    cmd = args.command
    if cmd == "create":
        body: dict[str, Any] = {"name": args.name}
        if args.contact is not None:
            body["contact"] = args.contact
        if args.pending:
            body["status"] = "pending"
        return "POST", "/v1/accounts", body
    if cmd == "list":
        return "GET", "/v1/accounts", None
    path = f"/v1/accounts/{quote(getattr(args, 'account_id', ''), safe='')}"
    if cmd == "show":
        return "GET", path, None
    if cmd in ("enable", "disable"):
        return "PATCH", path, {"status": "active" if cmd == "enable" else "disabled"}
    if cmd == "delete":
        return "DELETE", path, None
    if cmd == "token-create":
        body = {"label": args.label}
        if args.expires_at:
            body["expires_at"] = args.expires_at
        return "POST", f"{path}/tokens", body
    return "DELETE", f"{path}/tokens/{quote(args.token_id, safe='')}", None  # token-revoke


def _error_message(resp: httpx.Response) -> str:
    try:
        err = resp.json()["error"]
        return f"{err['message']} ({err['code']})"
    except Exception:
        return resp.text or f"HTTP {resp.status_code}"


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    args.url = _base_url(args.url)
    if not args.admin_token:
        print("error: an admin token is required (--admin-token or SW_ADMIN_TOKEN)",
              file=sys.stderr)
        return 1
    method, path, body = _request(args)
    try:
        with _client(args.url, args.admin_token) as client:
            resp = client.request(method, path, json=body)
    except httpx.HTTPError as exc:
        print(f"error: cannot reach {args.url}: {exc}", file=sys.stderr)
        return 1
    if not resp.is_success:
        print(f"error: {_error_message(resp)}", file=sys.stderr)
        return 1
    if resp.content:
        try:
            print(json.dumps(resp.json(), indent=2))
        except ValueError:
            print(resp.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
