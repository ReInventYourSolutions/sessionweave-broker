"""``sessionweave-host`` CLI: create a session, join it, optionally spawn
the background poller, and emit the invite link in one shot.

The intent is to remove the "I have to write the orchestration myself"
friction for agents on turn-based runtimes. After this command exits,
the user gets back:

* the ``swvs://`` invite link to share with the joiner
* the host's ``participant_id``
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
      "link": "swvs://...",
      "my_participant_id": "...",
      "poller_pid": 12345,
      "poller_log": "/tmp/sw-events-<sid>.log"
    }

Exit codes:
  0  — session created and joined; poller (if any) spawned.
  2  — usage / argument error.
  4  — relay reachable but a tool call returned an error (e.g. SESSION_FULL).
  5  — relay unreachable.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from sessionweave.invite import create_link


def _unwrap(res: Any) -> dict[str, Any]:
    sc = getattr(res, "structuredContent", None)
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


async def _create_and_join(
    relay_url: str,
    purpose: dict[str, Any] | None,
    my_role: str,
    my_capabilities: list[str],
    my_metadata: dict[str, Any],
    delivery_mode: str,
) -> tuple[str, str]:
    async with streamablehttp_client(relay_url) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            create_args: dict[str, Any] = {}
            if purpose is not None:
                create_args["purpose"] = purpose
            res = await session.call_tool(
                name="session_create", arguments=create_args
            )
            if getattr(res, "isError", False):
                raise RuntimeError(f"session_create error: {_unwrap(res)}")
            sid = _unwrap(res).get("session_id")
            if not isinstance(sid, str):
                raise RuntimeError(f"session_create returned no session_id: {sid!r}")
            join_args: dict[str, Any] = {
                "session_id": sid,
                "role": my_role,
                "capabilities": my_capabilities,
                "metadata": my_metadata,
                "delivery_mode": delivery_mode,
            }
            res = await session.call_tool(
                name="session_join", arguments=join_args
            )
            if getattr(res, "isError", False):
                raise RuntimeError(f"session_join error: {_unwrap(res)}")
            pid = _unwrap(res).get("participant_id")
            if not isinstance(pid, str):
                raise RuntimeError(f"session_join returned no participant_id: {pid!r}")
            return sid, pid


def _spawn_poller(
    relay_url: str,
    session_id: str,
    participant_id: str,
    log_path: Path,
    interval: float,
) -> int:
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
        stdout=open(out_log, "ab"),
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    return proc.pid


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sessionweave-host")
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
        sid, pid = asyncio.run(
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

    link = create_link(args.relay_url, sid)

    poller_pid: int | None = None
    poller_log: str | None = None
    if not args.no_poller:
        log_path = Path(
            args.poller_log
            or os.path.join(tempfile.gettempdir(), f"sw-events-{sid}.log")
        )
        poller_pid = _spawn_poller(
            args.relay_url, sid, pid, log_path, args.poller_interval
        )
        poller_log = str(log_path)

    print(
        json.dumps(
            {
                "session_id": sid,
                "link": link,
                "my_participant_id": pid,
                "poller_pid": poller_pid,
                "poller_log": poller_log,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
