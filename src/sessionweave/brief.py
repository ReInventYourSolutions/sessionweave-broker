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
    if purpose.get("instructions"):
        lines.append(f"- instructions: {purpose['instructions']}")
    if not lines:
        lines = ["```json", json.dumps(purpose, indent=2, ensure_ascii=False), "```"]
    return "\n".join(lines)


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
) -> str:
    base = base_url.rstrip("/")
    sid = session_id
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
- **Auth:** after you join you receive a `participant_token`; send it as
  `Authorization: Bearer <participant_token>` on every call below except step 1.

## What this session is for (purpose)
{_purpose_block(purpose)}

## Participate (HTTP, non-blocking)
**Critical for turn-based agents:** do NOT open a WebSocket and loop on it — your turn
would block and never return. Use the long-poll in step 3: it returns within seconds and
you re-call it on your next turn. This keeps you responsive.

1. **(optional) Re-read purpose**
   ```
   curl -sS {base}/v1/sessions/{sid}
   ```
2. **Join** — save the `participant_token` from the response.
   ```
   curl -sS -X POST {base}/v1/sessions/{sid}/participants \\
     -H 'content-type: application/json' \\
     -d '{{"role":"worker","capabilities":["respond"]}}'
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
4. **Respond to a command** (server role):
   ```
   curl -sS -X POST {base}/v1/sessions/{sid}/commands/<command_id>/responses \\
     -H 'authorization: Bearer <participant_token>' \\
     -H 'content-type: application/json' -d '{{"status":"ok","payload":{{}}}}'
   ```
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

## Go further (optional)
{_upgrades_block(upgrades)}

## If something fails
- `401`/`403` → re-check the `Authorization: Bearer <participant_token>` header.
- `404` on respond → the command expired; wait for the next event.
- Lost your token / reconnecting → see the full skill (link above); recover buffered events
  with `POST {base}/v1/sessions/{sid}/participants/me/inbox/drain`.
"""
