"""Background poller: drains a participant's inbox into a JSONL log.

Designed for turn-based agent runtimes that can't run a real event loop
themselves. Spawn this once after ``session_join`` (the
``sessionweave-host`` CLI does that for you), then on every later
runtime turn the agent ``tail``s the log to discover what arrived.

Each line of the log is one JSON object: either a notification
``{"ts": ..., "name": "<event>", "payload": {...}}`` or an error
record ``{"ts": ..., "error": "<message>"}``.

Standalone usage:

    python -m sessionweave.poller \
        --relay-url https://relay.sessionweave.com/mcp \
        --session-id <sid> \
        --participant-id <pid> \
        --log /tmp/sw-events.log \
        --interval 3.0

Exit when ``session.ended`` or a matching ``participant.left`` event
is observed for the polled participant. Otherwise runs until killed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


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


async def _drain_once(
    relay_url: str, session_id: str, participant_id: str
) -> list[dict[str, Any]]:
    async with streamablehttp_client(relay_url) as (r, w, _):
        async with ClientSession(r, w) as session:
            await session.initialize()
            await session.call_tool(
                name="session_resume",
                arguments={
                    "session_id": session_id,
                    "participant_id": participant_id,
                },
            )
            res = await session.call_tool(
                name="session_check_inbox", arguments={}
            )
            out = _unwrap(res)
            return out.get("messages", []) if isinstance(out, dict) else []


def _is_terminal(msg: dict[str, Any], own_pid: str) -> bool:
    name = msg.get("name")
    payload = msg.get("payload") or {}
    if name == "session.ended":
        return True
    if name == "participant.left" and payload.get("participant_id") == own_pid:
        return True
    return False


async def run(
    relay_url: str,
    session_id: str,
    participant_id: str,
    log_path: Path,
    interval: float,
) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", buffering=1) as log_file:
        log_file.write(
            json.dumps(
                {
                    "ts": _now(),
                    "event": "poller-start",
                    "session_id": session_id,
                    "participant_id": participant_id,
                }
            )
            + "\n"
        )
        while True:
            try:
                msgs = await _drain_once(relay_url, session_id, participant_id)
            except Exception as exc:
                log_file.write(
                    json.dumps({"ts": _now(), "error": str(exc)}) + "\n"
                )
                msgs = []
            terminal = False
            for m in msgs:
                log_file.write(
                    json.dumps({"ts": _now(), **m}, ensure_ascii=False) + "\n"
                )
                if _is_terminal(m, participant_id):
                    terminal = True
            if terminal:
                log_file.write(
                    json.dumps({"ts": _now(), "event": "poller-stop"}) + "\n"
                )
                return 0
            await asyncio.sleep(interval)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sessionweave-poller")
    p.add_argument(
        "--relay-url",
        default=os.environ.get("SW_URL", "https://relay.sessionweave.com/mcp"),
        help="Full HTTP MCP endpoint of the relay.",
    )
    p.add_argument("--session-id", required=True)
    p.add_argument("--participant-id", required=True)
    p.add_argument(
        "--log",
        default="/tmp/sw-events.log",
        help="JSONL log file the poller appends events to.",
    )
    p.add_argument(
        "--interval",
        type=float,
        default=3.0,
        help="Seconds between poll cycles (default 3).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    return asyncio.run(
        run(
            args.relay_url,
            args.session_id,
            args.participant_id,
            Path(args.log),
            args.interval,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
