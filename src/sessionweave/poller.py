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
        --participant-token <participant_token> \
        --participant-id <pid> \
        --log /tmp/sw-events.log \
        --interval 3.0

``--participant-token`` is the secret ``session_join`` returned; it is the
only credential (it may instead be supplied through the
``SW_PARTICIPANT_TOKEN`` environment variable, which keeps it out of the
process list). ``--participant-id`` is the public id from the same
output; it is used ONLY to recognise this participant's own
``participant.left`` event and is never sent to the relay as a credential.

Exit when ``session.ended`` or a matching ``participant.left`` event
is observed for the polled participant (this includes ``reason: removed``,
the creator removed you), or when the relay answers ``participant_removed``.
A ``supervisor.note`` of kind ``halt`` is NOT terminal: a halted participant
stays in the session, paused, and waits for ``continue`` or ``redirect``.
Otherwise runs until killed.
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
from mcp.client.streamable_http import streamable_http_client


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _unwrap(res: Any) -> Any:
    sc = res.structured_content
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
    relay_url: str, session_id: str, participant_token: str
) -> list[dict[str, Any]]:
    async with streamable_http_client(relay_url) as (r, w):
        async with ClientSession(r, w) as session:
            await session.initialize()
            await session.call_tool(
                name="session_resume",
                arguments={
                    "session_id": session_id,
                    "participant_token": participant_token,
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
    participant_token: str,
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
                msgs = await _drain_once(
                    relay_url, session_id, participant_token
                )
            except Exception as exc:
                log_file.write(
                    json.dumps({"ts": _now(), "error": str(exc)}) + "\n"
                )
                if "participant_removed" in str(exc):
                    log_file.write(
                        json.dumps({"ts": _now(), "event": "poller-stop"})
                        + "\n"
                    )
                    return 0
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
    p.add_argument(
        "--participant-token",
        default=os.environ.get("SW_PARTICIPANT_TOKEN"),
        help="Secret participant_token from session_join (the credential). "
        "Defaults to $SW_PARTICIPANT_TOKEN.",
    )
    p.add_argument(
        "--participant-id",
        required=True,
        help="Public participant_id; used only to spot your own "
        "participant.left, never sent as a credential.",
    )
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
    args = p.parse_args(argv)
    if not args.participant_token:
        p.error("--participant-token (or $SW_PARTICIPANT_TOKEN) is required")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    return asyncio.run(
        run(
            args.relay_url,
            args.session_id,
            args.participant_token,
            args.participant_id,
            Path(args.log),
            args.interval,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
