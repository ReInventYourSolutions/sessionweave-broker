"""``sessionweave-join`` CLI: parse an ``swv://`` invite link, fetch the
session purpose via ``session_describe``, and print a structured snapshot
the calling agent can act on.

This is intentionally a *discovery* tool, not an auto-pilot. It does NOT
join the session; the caller decides what to do with the printed payload.

Usage:
  sessionweave-join <swv://...>
  sessionweave-join <swv://...> --json     # machine-readable output

Exit codes:
  0  — link parsed, describe succeeded.
  2  — usage / argument error.
  3  — invalid invite link.
  4  — relay reachable but session_describe failed (e.g. session_not_found).
  5  — relay unreachable.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from sessionweave.invite import parse_link


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sessionweave-join")
    p.add_argument("link", help="An swv:// invite link.")
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit a single JSON object (no human-friendly summary).",
    )
    return p.parse_args(argv)


def _unwrap(res: Any) -> Any:
    sc = getattr(res, "structuredContent", None)
    if sc is None:
        if res.content:
            txt = getattr(res.content[0], "text", "")
            try:
                return json.loads(txt)
            except json.JSONDecodeError:
                return {"raw": txt}
        return {}
    if isinstance(sc, dict):
        return sc.get("result", sc)
    return sc


async def _describe(relay_url: str, session_id: str) -> dict[str, Any]:
    async with streamablehttp_client(relay_url) as (r, w, _):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(
                name="session_describe",
                arguments={"session_id": session_id},
            )
            if getattr(res, "isError", False):
                raise RuntimeError(f"session_describe error: {_unwrap(res)}")
            out = _unwrap(res)
            if not isinstance(out, dict):
                raise RuntimeError(f"unexpected describe output: {out!r}")
            return out


def _human_summary(invite: Any, desc: dict[str, Any]) -> str:
    purpose = desc.get("purpose") or {}
    kind = purpose.get("kind", "(none)")
    your_role = purpose.get("your_role")
    available = purpose.get("available_roles")
    instructions = purpose.get("instructions")

    lines = [
        f"relay      : {invite.relay_url}",
        f"session_id : {invite.session_id}",
        f"expires_at : {desc.get('expires_at', '?')}",
        f"kind       : {kind}",
    ]
    roster = desc.get("roster") or []
    online = sum(1 for p in roster if p.get("online"))
    lines.append(f"roster     : {online} online / {len(roster)} total")
    if your_role:
        caps = purpose.get("your_capabilities") or []
        lines.append(f"your_role  : {your_role}  caps={caps}")
    elif available:
        lines.append("available_roles:")
        for r in available:
            r_role = r.get("role", "?")
            r_caps = r.get("capabilities") or []
            r_desc = r.get("description") or ""
            lines.append(f"  - {r_role}  caps={r_caps}  — {r_desc}")
    if instructions:
        lines.append("")
        lines.append("instructions:")
        lines.append(instructions)
    return "\n".join(lines)


async def _async_main(args: argparse.Namespace) -> int:
    try:
        invite = parse_link(args.link)
    except ValueError as exc:
        sys.stderr.write(f"invalid invite link: {exc}\n")
        return 3

    try:
        desc = await _describe(invite.relay_url, invite.session_id)
    except OSError as exc:
        sys.stderr.write(f"relay unreachable at {invite.relay_url}: {exc}\n")
        return 5
    except Exception as exc:
        sys.stderr.write(f"describe failed: {exc}\n")
        return 4

    if args.json:
        print(
            json.dumps(
                {
                    "relay_url": invite.relay_url,
                    "session_id": invite.session_id,
                    "describe": desc,
                },
                ensure_ascii=False,
            )
        )
    else:
        print(_human_summary(invite, desc))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
