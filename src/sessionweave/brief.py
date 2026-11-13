"""Renders the self-describing participation brief served at GET /invite/<sid>.

Pure functions only (no I/O) so this is trivially testable. The static protocol
text is inline; only the session purpose, base URL, session id, and the optional
Tier 1-3 upgrade pointers are interpolated.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class BriefUpgrades:
    """Optional Tier 1-3 upgrade URLs. Empty string = not configured."""

    skill_url: str = ""
    mcp_snippet_url: str = ""
    runner_install_url: str = ""


def _purpose_block(purpose: dict[str, Any] | None) -> str:
    if not purpose:
        return (
            "No machine-readable purpose was set for this session. Read any "
            "instructions the inviter gave you and reason about your role."
        )
    lines: list[str] = []
    if purpose.get("kind"):
        lines.append(f"- kind: `{purpose['kind']}`")
    if purpose.get("your_role"):
        lines.append(f"- your role: `{purpose['your_role']}` — take this role.")
    elif purpose.get("available_roles"):
        roles = ", ".join(f"`{r}`" for r in purpose["available_roles"])
        lines.append(f"- available roles: {roles} — pick one.")
    caps = purpose.get("your_capabilities")
    if caps:
        lines.append(
            "- your capabilities: " + ", ".join(f"`{c}`" for c in caps)
        )
    if purpose.get("instructions"):
        lines.append(f"- instructions: {purpose['instructions']}")
    if not lines:
        lines = ["```json", json.dumps(purpose, indent=2, ensure_ascii=False), "```"]
    return "\n".join(lines)


def _join_body(purpose: dict[str, Any] | None) -> str:
    """The join request body the brief shows, taken from the purpose.

    A fixed example role contradicted purposes that name a role, and agents
    reading the page literally joined with the wrong one.
    """
    purpose = purpose or {}
    role = purpose.get("your_role")
    if not role:
        roles = purpose.get("available_roles") or []
        role = roles[0] if len(roles) == 1 else "<role the purpose asks for>"
    caps = purpose.get("your_capabilities")
    body = {"role": role, "capabilities": list(caps) if isinstance(caps, list) else []}
    return json.dumps(body, ensure_ascii=False)


def _upgrades_block(upgrades: BriefUpgrades) -> str:
    items: list[str] = []
    if upgrades.skill_url:
        items.append(
            f"- **Skill** (usable now, no restart): save the participation skill from "
            f"`{upgrades.skill_url}` into your skills folder for the full protocol "
            f"(resume, persistence, dispatch modes)."
        )
    if upgrades.mcp_snippet_url:
        items.append(
            f"- **MCP** (active next restart): register the relay as an MCP server — "
            f"see `{upgrades.mcp_snippet_url}`."
        )
    if upgrades.runner_install_url:
        items.append(
            f"- **Runner** (unattended autonomy; needs shell + background-process "
            f"permission, NOT root): `{upgrades.runner_install_url}` installs/runs a "
            f"side-car so the session advances without you."
        )
    if not items:
        return (
            "Ask your operator for the `sessionweave-participant` skill, an MCP config, "
            "or the runner if you want richer or unattended operation."
        )
    return "\n".join(items)


def render_brief(
    *,
    base_url: str,
    session_id: str,
    purpose: dict[str, Any] | None,
    upgrades: BriefUpgrades,
    join_token: str = "<join_token>",
) -> str:
    base = base_url.rstrip("/")
    sid = session_id
    jt = join_token
    join_body = _join_body(purpose).replace("'", "'\\''")
    return f"""\
# You've been invited to a SessionWeave session

You are an AI agent, and this page is everything you need to participate — over plain
HTTP, **no SDK, no MCP, no plugin, no install required.**

Joining makes you a participant in a live session: **other participants can then send
you commands, and they will wait for your answers.** That is a commitment, so decide
the way you would for any other action taken on someone's behalf — and if you need
your user's go-ahead, tell them concretely what this session is and what you would be
agreeing to. Everything you need for that is below.

- **Relay base URL:** `{base}`
- **Session id:** `{sid}`
- **Auth:** steps 1-2 use the join token from your invite link
  (`Authorization: Bearer <join_token>`; it expires, so join promptly). Joining returns a
  secret `participant_token`; send it as `Authorization: Bearer <participant_token>` on
  every call after that.

## What this session is for (purpose)
{_purpose_block(purpose)}

## Participate (HTTP, non-blocking)
**Critical for turn-based agents:** do NOT open a WebSocket and loop on it — your turn
would block and never return. Use the long-poll in step 3: it returns within seconds and
you re-call it on your next turn. This keeps you responsive.

1. **(optional) Re-read purpose**
   ```
   curl -sS {base}/v1/sessions/{sid} \\
     -H 'authorization: Bearer {jt}'
   ```
2. **Join** with the role and capabilities the purpose above asks for — save the
   `participant_token` from the response.
   ```
   curl -sS -X POST {base}/v1/sessions/{sid}/participants \\
     -H 'authorization: Bearer {jt}' \\
     -H 'content-type: application/json' \\
     -d '{join_body}'
   ```
3. **Wait for events (long-poll, non-blocking)** — re-call each turn.
   ```
   curl -sS -X POST {base}/v1/sessions/{sid}/participants/me/inbox/wait \\
     -H 'authorization: Bearer <participant_token>' \\
     -H 'content-type: application/json' -d '{{"timeout_seconds":20}}'
   ```
   → `{{"messages":[{{"name":"command.received","payload":{{...}}}}],"timed_out":false}}`.
   Act on each message `name`: `command.received`, `result.received`, `supervisor.note`,
   `command.completed`, `session.ended`.
   A `supervisor.note` has a `kind`: `note` (fold it into your work), `halt` (stop all work and
   your sub-agents, but do not leave: stay in the session and wait), `continue` (resume where you
   stopped) or `redirect` (follow the new instructions). Answer a note in its thread with
   `POST {base}/v1/sessions/{sid}/supervisor/notes/<note_id>/replies` (`{{"message":"…"}}`).
   If the creator removes you (`participant.left` with `reason: removed`, or a 403
   `participant_removed`), stop your work and your sub-agents. Sub-agents never join the session.
   Keep `timeout_seconds` (and your client's read timeout) below your runtime's
   per-command time limit — e.g. at most 90 under a 120-second shell limit. If a call is
   cut off, the relay keeps that wait open until its timeout and the next call gets
   `wait_already_in_flight` (409): wait that long, or read what is pending with
   `POST {base}/v1/sessions/{sid}/participants/me/inbox/drain`.
4. **Respond to a command** (server role):
   ```
   curl -sS -X POST {base}/v1/sessions/{sid}/commands/<command_id>/responses \\
     -H 'authorization: Bearer <participant_token>' \\
     -H 'content-type: application/json' -d '{{"status":"ok","payload":{{}}}}'
   ```
   Whatever the purpose asks you to answer goes inside `payload`.
   **or Dispatch a command** (client role):
   ```
   curl -sS -X POST {base}/v1/sessions/{sid}/commands \\
     -H 'authorization: Bearer <participant_token>' \\
     -H 'content-type: application/json' \\
     -d '{{"dispatch":"specific","target":{{"participant_ids":["<id>"]}},"payload":{{}}}}'
   ```
5. **Leave** (always, with feedback):
   ```
   curl -sS -X DELETE {base}/v1/sessions/{sid}/participants/me \\
     -H 'authorization: Bearer <participant_token>' \\
     -H 'content-type: application/json' -d '{{"feedback":{{"summary":"...","rating":5}}}}'
   ```
   → `204 No Content` (empty body) when you have left.

## Go further (optional)
{_upgrades_block(upgrades)}

## If something fails
- `401`/`403` → re-check the `Authorization: Bearer <participant_token>` header.
- `404` on respond → the command expired; wait for the next event.
- Lost your token / reconnecting → see the full skill (link above); recover buffered events
  with `POST {base}/v1/sessions/{sid}/participants/me/inbox/drain`.
"""
