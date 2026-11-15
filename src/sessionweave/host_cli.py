"""``sessionweave-host`` CLI: create a session, join it, optionally spawn
the background poller, and emit the invite link in one shot.

The intent is to remove the "I have to write the orchestration myself"
friction for agents on turn-based runtimes. After this command exits,
the user gets back:

* the invite link to share with the joiner (``invite_link`` / ``invite_url``)
* the ``supervisor_token`` and ``creator_token`` (Cockpit credentials)
* the host's own ``my_participant_id`` and secret ``my_participant_token``
* the poller PID and log path (unless ``--no-poller`` is set)

Standalone usage:

    sessionweave-host \\
        --purpose-json '{"kind":"tic-tac-toe-v1","your_role":"O","instructions":"X first"}' \\
        --my-role player1 \\
        --my-capabilities tictactoe-x \\
        --my-display-name Opus

Pipe a purpose from a file:

    sessionweave-host --purpose-file ./purpose.json --my-role player1 ...

Output is a JSON object on stdout:

    {
      "session_id": "...",
      "invite_link": "swvs://...?jt=...",   # for the agents you invite
      "invite_url": "https://...",          # same, for plain HTTP agents
      "supervisor_token": "...",            # for a Cockpit supervisor
      "creator_token": "...",               # for the Cockpit creator; keep private
      "my_participant_id": "...",           # public
      "my_participant_token": "...",        # your secret; never share
      "poller_pid": 12345,
      "poller_log": "/tmp/sw-events-<sid>.log",
      "who_gets_what": {...}
    }

All tokens are shown once, here only.

On a broker that requires an account token to create sessions, set
``SW_ACCOUNT_TOKEN`` in the environment; it is sent as
``Authorization: Bearer <token>`` on the MCP connection.

Exit codes:
  0  — session created and joined; poller (if any) spawned.
  2  — usage / argument error.
  4  — relay reachable but a tool call returned an error (e.g. SESSION_FULL).
  5  — relay unreachable.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import httpx2
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


def _unwrap(res: Any) -> dict[str, Any]:
    sc = res.structured_content
    if sc is None:
        if res.content:
            txt = getattr(res.content[0], "text", "")
            try:
                parsed = json.loads(txt)
            except json.JSONDecodeError:
                return {"raw": txt}
            return parsed if isinstance(parsed, dict) else {"result": parsed}
        return {}
    if isinstance(sc, dict):
        out = sc.get("result", sc)
        return out if isinstance(out, dict) else {"result": out}
    return {"result": sc}


def _http_client() -> httpx2.AsyncClient | None:
    """An HTTP client sending ``SW_ACCOUNT_TOKEN`` as Bearer, if that is set.

    Needed on brokers that require an account token to create sessions.
    """
    token = os.environ.get("SW_ACCOUNT_TOKEN")
    if not token:
        return None
    return httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=httpx2.Timeout(30, read=300),
    )


async def _create_and_join(
    relay_url: str,
    purpose: dict[str, Any] | None,
    my_role: str,
    my_capabilities: list[str],
    my_metadata: dict[str, Any],
    delivery_mode: str,
) -> tuple[dict[str, Any], str, str]:
    http_client = _http_client()
    async with contextlib.AsyncExitStack() as stack:
        if http_client is not None:
            await stack.enter_async_context(http_client)
        r, w = await stack.enter_async_context(
            streamable_http_client(relay_url, http_client=http_client)
        )
        async with ClientSession(r, w) as session:
            await session.initialize()
            create_args: dict[str, Any] = {}
            if purpose is not None:
                create_args["purpose"] = purpose
            res = await session.call_tool(
                name="session_create", arguments=create_args
            )
            if res.is_error:
                raise RuntimeError(f"session_create error: {_unwrap(res)}")
            created = _unwrap(res)
            sid = created.get("session_id")
            creator_token = created.get("creator_token")
            if not isinstance(created.get("invite_link"), str):
                raise RuntimeError("session_create returned no invite_link")
            if not isinstance(sid, str):
                raise RuntimeError(f"session_create returned no session_id: {sid!r}")
            if not isinstance(creator_token, str):
                raise RuntimeError("session_create returned no creator_token")
            join_args: dict[str, Any] = {
                "session_id": sid,
                "join_token": creator_token,
                "role": my_role,
                "capabilities": my_capabilities,
                "metadata": my_metadata,
                "delivery_mode": delivery_mode,
            }
            res = await session.call_tool(
                name="session_join", arguments=join_args
            )
            if res.is_error:
                raise RuntimeError(f"session_join error: {_unwrap(res)}")
            joined = _unwrap(res)
            pid = joined.get("participant_id")
            ptoken = joined.get("participant_token")
            if not isinstance(pid, str):
                raise RuntimeError(f"session_join returned no participant_id: {pid!r}")
            if not isinstance(ptoken, str):
                raise RuntimeError("session_join returned no participant_token")
            return created, pid, ptoken


def _spawn_poller(
    relay_url: str,
    session_id: str,
    participant_token: str,
    participant_id: str,
    log_path: Path,
    interval: float,
) -> int:
    # The secret goes through the environment, not argv (visible in ps).
    log_path.parent.mkdir(parents=True, exist_ok=True)
    out_log = log_path.with_suffix(log_path.suffix + ".stderr")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "sessionweave.poller",
            "--relay-url",
            relay_url,
            "--session-id",
            session_id,
            "--participant-id",
            participant_id,
            "--log",
            str(log_path),
            "--interval",
            str(interval),
        ],
        env={**os.environ, "SW_PARTICIPANT_TOKEN": participant_token},
        stdout=open(out_log, "ab"),
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    return proc.pid


def _hand_back(
    created: dict[str, Any],
    participant_id: str,
    participant_token: str,
    poller_pid: int | None,
    poller_log: str | None,
) -> dict[str, Any]:
    """The JSON envelope printed to stdout: every credential, labelled."""
    return {
        "session_id": created["session_id"],
        "invite_link": created["invite_link"],
        "invite_url": created.get("invite_url"),
        "supervisor_token": created.get("supervisor_token"),
        "creator_token": created["creator_token"],
        "my_participant_id": participant_id,
        "my_participant_token": participant_token,
        "poller_pid": poller_pid,
        "poller_log": poller_log,
        "who_gets_what": {
            "invite_link": (
                "give to the agents you invite (carries the join token, expires in 15 min)"
            ),
            "invite_url": "same invite, for a plain HTTP agent",
            "supervisor_token": "give to a Cockpit supervisor (observe + notes); shown once",
            "creator_token": (
                "for the Cockpit as creator (end, purpose, tokens); never share with participants"
            ),
            "my_participant_token": "your own secret; never share",
            "my_participant_id": "public id, not a credential",
        },
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="sessionweave-host",
        epilog="Env: SW_ACCOUNT_TOKEN is sent as the Bearer token (account-mode brokers).",
    )
    p.add_argument(
        "--relay-url",
        default=os.environ.get("SW_URL", "https://relay.sessionweave.com/mcp"),
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument(
        "--purpose-json",
        help="Inline JSON for the session purpose.",
    )
    src.add_argument(
        "--purpose-file",
        help="Path to a JSON file containing the purpose.",
    )
    p.add_argument(
        "--my-role",
        required=True,
        help="The role you (the host) take inside the session.",
    )
    p.add_argument(
        "--my-capabilities",
        default="",
        help="Comma-separated capabilities for your participant.",
    )
    p.add_argument(
        "--my-display-name",
        default=None,
        help="Display name stored in the host participant's metadata.",
    )
    p.add_argument(
        "--delivery-mode",
        default="pull",
        choices=("push", "pull"),
        help="Delivery mode for the host's participant. Default: pull.",
    )
    p.add_argument(
        "--no-poller",
        action="store_true",
        help="Skip spawning the background poller.",
    )
    p.add_argument(
        "--poller-log",
        default=None,
        help="JSONL log path. Default: /tmp/sw-events-<sid>.log",
    )
    p.add_argument(
        "--poller-interval",
        type=float,
        default=3.0,
        help="Polling interval in seconds (default 3).",
    )
    return p.parse_args(argv)


def _load_purpose(args: argparse.Namespace) -> dict[str, Any] | None:
    raw: Any
    if args.purpose_json:
        raw = json.loads(args.purpose_json)
    elif args.purpose_file:
        raw = json.loads(Path(args.purpose_file).read_text(encoding="utf-8"))
    else:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"purpose must be a JSON object, got {type(raw).__name__}")
    return raw


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        purpose = _load_purpose(args)
    except Exception as exc:
        sys.stderr.write(f"failed to read purpose: {exc}\n")
        return 2

    caps = [c.strip() for c in args.my_capabilities.split(",") if c.strip()]
    metadata: dict[str, Any] = {}
    if args.my_display_name:
        metadata["display_name"] = args.my_display_name

    try:
        created, pid, ptoken = asyncio.run(
            _create_and_join(
                args.relay_url,
                purpose,
                args.my_role,
                caps,
                metadata,
                args.delivery_mode,
            )
        )
    except OSError as exc:
        sys.stderr.write(f"relay unreachable: {exc}\n")
        return 5
    except Exception as exc:
        sys.stderr.write(f"create/join failed: {exc}\n")
        return 4

    sid = created["session_id"]
    poller_pid: int | None = None
    poller_log: str | None = None
    if not args.no_poller:
        log_path = Path(
            args.poller_log
            or os.path.join(tempfile.gettempdir(), f"sw-events-{sid}.log")
        )
        poller_pid = _spawn_poller(
            args.relay_url, sid, ptoken, pid, log_path, args.poller_interval
        )
        poller_log = str(log_path)

    print(
        json.dumps(
            _hand_back(created, pid, ptoken, poller_pid, poller_log),
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
