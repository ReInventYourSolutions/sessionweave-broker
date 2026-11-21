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
from urllib.parse import quote, urlencode

import httpx

DEFAULT_URL = "http://127.0.0.1:8080"
POLICIES = ("any", "first_use", "approved_only")


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
        ("enable", "activate an account and lift its sessions' suspension"),
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
    tc.add_argument("--network-policy", choices=POLICIES,
                    help="network policy of the token (default any)")
    tc.add_argument("--network", action="append", metavar="CIDR",
                    help="approved network (CIDR or IP); repeatable")
    tn = sub.add_parser("token-networks", help="show or change a token's networks")
    tn.add_argument("account_id")
    tn.add_argument("token_id")
    tn.add_argument("--policy", choices=POLICIES, help="new network policy")
    tn.add_argument("--network", action="append", metavar="CIDR",
                    help="replace the approved networks with these; repeatable")
    tn.add_argument("--no-networks", action="store_true",
                    help="replace the approved networks with an empty list")
    ap = sub.add_parser("approvals", help="list pending network approvals")
    ap.add_argument("account_id")
    ao = sub.add_parser("approve", help="approve a pending network approval")
    ao.add_argument("account_id")
    ao.add_argument("approval_id")
    ao.add_argument("--scope", choices=("ip", "network"), default="ip",
                    help="approve the exact IP or its network (/24, /64); default ip")
    de = sub.add_parser("deny", help="deny a pending network approval")
    de.add_argument("account_id")
    de.add_argument("approval_id")
    ev = sub.add_parser("events", help="read account events (all accounts, or one)")
    ev.add_argument("--account", help="only this account id")
    ev.add_argument("--after", type=int, help="only events after this event id")
    ev.add_argument("--wait", type=float, help="long-poll up to this many seconds (max 60)")
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
    if cmd == "events":
        query = {
            k: v for k, v in (
                ("account_id", args.account), ("after", args.after), ("wait", args.wait)
            ) if v is not None
        }
        return "GET", "/v1/accounts/events" + (f"?{urlencode(query)}" if query else ""), None
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
        if args.network_policy:
            body["network_policy"] = args.network_policy
        if args.network:
            body["approved_networks"] = args.network
        return "POST", f"{path}/tokens", body
    if cmd == "token-networks":
        url = f"{path}/tokens/{quote(args.token_id, safe='')}/networks"
        changes: dict[str, Any] = {}
        if args.policy:
            changes["network_policy"] = args.policy
        if args.network:
            changes["approved_networks"] = args.network
        elif args.no_networks:
            changes["approved_networks"] = []
        return ("PUT", url, changes) if changes else ("GET", url, None)
    if cmd == "approvals":
        return "GET", f"{path}/network-approvals", None
    if cmd in ("approve", "deny"):
        url = f"{path}/network-approvals/{quote(args.approval_id, safe='')}/{cmd}"
        return "POST", url, ({"scope": args.scope} if cmd == "approve" else None)
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
    if resp.content:
        try:
            print(json.dumps(resp.json(), indent=2))
        except ValueError:
            print(resp.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
