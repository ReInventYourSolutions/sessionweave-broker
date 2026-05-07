# SessionWeaveMCP

> The MCP relay implementation behind [SessionWeave](https://github.com/EnzinoBB/SessionWeave).

This repository contains the **server side** of SessionWeave — the relay
that brokers messages between AI agents. For the **client side** (what
an agent does to participate in a session, including the Claude Code
skill and the user-facing overview) see the companion repo
[`EnzinoBB/SessionWeave`](https://github.com/EnzinoBB/SessionWeave).

SessionWeave lets two or more AI agents work together inside a shared **session**
without any of them having to know about the others up front. An agent connects
over MCP, joins a session by id, sends commands, receives results — all while
keeping its own conversation context private and local.

The relay only brokers messages. It never holds an agent's history.

> **Status.** Sub-project **#1 — MCP core + session relay** is implemented:
> in-memory, anonymous, transient sessions over MCP Streamable HTTP. Persistence,
> accounts, and the dashboard land in later sub-projects (see [roadmap](#roadmap)).
> See [`docs/VISION.md`](docs/VISION.md) for the broader project intent.

## Why

Today, getting two agents to collaborate means custom plumbing on both sides.
SessionWeave gives every MCP-capable agent the same primitive: open a session,
share what you choose to share, ignore everything else.

- **MCP-native** — the only thing an agent needs is an MCP client.
- **Local context, shared session** — agents keep their own state; only what
  they explicitly send becomes session-visible.
- **Anonymous baseline, registered upgrades** — anyone can spin up a session
  anonymously today; persistence and richer services land in later sub-projects.

## Quick start

Install in editable mode and run the relay locally:

```bash
pip install -e '.[dev]'
sessionweave --host 127.0.0.1 --port 8765
```

…or run the prebuilt container:

```bash
docker build -t sessionweave:dev .
docker run --rm -p 8765:8765 sessionweave:dev
```

The relay exposes:

- `POST /mcp` — MCP Streamable HTTP endpoint for agents
- `GET /healthz` — `200` while serving, `503` once shutdown drain begins
- `GET /readyz` — `200` once the port is bound
- `GET /metrics` — Prometheus exposition (text format)

## Talking to the relay (Python MCP client)

```python
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

async with streamablehttp_client("http://127.0.0.1:8765/mcp") as (read, write, _):
    async with ClientSession(read, write) as session:
        await session.initialize()
        out = await session.call_tool("session_create", {})
        sid = out.structuredContent["session_id"]

        await session.call_tool(
            "session_join",
            {"session_id": sid, "role": "reviewer", "capabilities": ["sec"]},
        )
```

A second agent on a different MCP connection joins the same `session_id` and
the two can now exchange commands and results through the dispatch tools.

## Invite links (`swv://...`)

The hand-off shape is a compact pointer to a session:

```
swv://<host>:<port>/<session_id>
```

The link is just an addressable reference. The session's **purpose**
(what to do, which role to take, which protocol to follow) lives
server-side and is fetched by the recipient via `session_describe` —
a read-only tool that does NOT allocate a participant.

```python
from sessionweave.invite import create_link, parse_link

# Host side: set purpose at create time, emit the compact link.
purpose = {
    "kind": "tic-tac-toe-v1",
    "your_role": "O",
    "your_capabilities": ["tictactoe-o"],
    "delivery_mode": "pull",
    "instructions": "X moves first; reply via session_respond, then send your move.",
}
out = await session.call_tool("session_create", {"purpose": purpose})
link = create_link("http://127.0.0.1:8765/mcp", out["session_id"])
# link == "swv://127.0.0.1:8765/<sid>"   — nothing else encoded

# Joiner side: parse the link, look up the purpose, decide a role, join.
inv = parse_link(link)
desc = await session.call_tool("session_describe", {"session_id": inv.session_id})
# desc["purpose"] == the host's purpose dict
```

`purpose` is capped at 16 KiB JSON; oversized payloads return
`invalid_config`. Encoding is purely client-side — the relay never
parses the link itself.

The recipient's decision tree on `desc["purpose"]`:

- `your_role` set → assume that role.
- `available_roles: [...]` set → pick one (autonomously when the choice
  is unambiguous, otherwise ask the user).
- Neither — only `instructions` — the brief is open-ended; reason about
  it, ask the user if needed.

For known protocols an agent dispatches by `purpose.kind`; for unknown
kinds it falls back to `purpose.instructions`. The
[`sessionweave-participant`](https://github.com/EnzinoBB/SessionWeave/tree/main/skills/sessionweave-participant)
skill (in the client repo) documents the full receive-side flow.

Legacy links carrying a base64url JSON fragment
(`swv://h:p/<sid>#<blob>`) are still understood by `parse_link`
(`inv.purpose` populated from the fragment) for backward compatibility.
New code should not emit them: keep the briefing on the server.

## MCP tool surface

| tool | purpose |
|---|---|
| `session_create` | Create a session; returns `session_id`, effective config, and any `purpose` you passed in. |
| `session_describe` | Read-only snapshot: `{config, purpose, roster}` without allocating a participant. Pre-flight for invite-link recipients. |
| `session_update_purpose` | Replace the session's `purpose` (or clear it) and broadcast `session.purpose_updated` to other participants. |
| `session_join` | Bind the calling MCP connection to a new participant. Optional `delivery_mode = "push"` (default) or `"pull"` chooses how server events reach you. |
| `session_resume` | Re-bind a fresh MCP connection to an existing `participant_id`. Echoes `purpose` back. |
| `session_leave` | Leave the session (broadcast to others). |
| `session_update_self` | Replace fields on the caller's participant; broadcast. |
| `session_list_participants` | Snapshot of the roster. |
| `session_check_inbox` | Drain pending events for the caller. Required when joined with `delivery_mode="pull"`. |
| `session_send` | Dispatch a command (`all` / `any` / `specific`) to others. |
| `session_respond` | Respond to a previously received command. |

Server-emitted notifications: `participant.joined`, `participant.updated`,
`participant.left`, `command.received`, `result.received`,
`command.completed`, `command.cancelled`, `session.purpose_updated`,
`session.ended`. Each carries a structured payload — see
[the design spec][spec] §3 for the schemas (the spec predates
`session.purpose_updated`; see the skill for that one).

## Dispatch model

`session_send` chooses recipients by combining a **dispatch mode** with a
**target filter**:

| dispatch | meaning |
|---|---|
| `specific` | Deliver to every `participant_id` listed in `target.participant_ids`. |
| `all` | Deliver to every online participant matching `target.role` / `target.capability`. |
| `any` | Deliver to one online participant matching the filter (round-robin within a session). |

A response from each recipient produces `result.received` on the sender;
once all expected responses are in (or the deadline expires), the sender
also gets `command.completed`.

## Configuration

Two layers:

1. **Deployment bounds** (env vars, set by the operator): `SW_DEFAULT_<NAME>`,
   `SW_MIN_<NAME>`, `SW_MAX_<NAME>` for each `SessionConfig` field. These
   define both the factory default and the `[min, max]` range a client may
   request per session.
2. **Per-session overrides** (passed to `session_create`'s `config`):
   any field within the deployment range. Out-of-range requests fail with
   `invalid_config`.

Configurable parameters:

- `max_participants`
- `max_payload_bytes`
- `idle_ttl_seconds`
- `default_command_timeout_seconds`, `max_command_timeout_seconds`
- `offline_grace_seconds`
- `max_pending_notifications_per_participant`

Process binding: `SW_HOST` (default `0.0.0.0`), `SW_PORT` (default `8765`).

## Architecture (sub-project #1)

Four in-process modules, no external dependencies:

- **Session registry** (`registry.py`) — owns `session_id → Session`; runs the
  idle-TTL housekeeping loop.
- **Session state** (`session.py`) — per-session participants, in-flight
  commands, notification buffers, and the lock that linearises writes.
- **Connection manager** (`connections.py`) — maps `(session_id, participant_id)
  ↔ ConnectionHandle`. The MCP-specific adapter lives in `mcp_adapter.py`.
- **Dispatcher** (`dispatcher.py`) — resolves a `session_send` request into a
  recipient list by combining dispatch mode, target filter, and presence.

The HTTP surface (Starlette) and tool wiring (FastMCP) live in `server.py` and
`tools.py`. Prometheus instrumentation lives in `metrics.py`.

State is in-memory only; restart loses all sessions. Persistence is
sub-project #2.

## Development

```bash
pip install -e '.[dev]'
ruff check . && mypy . && pytest -q -m "not fuzz"
```

Tests are layered:

- `tests/unit` — module-level unit tests (sync + async).
- `tests/integration` — exercise the relay through a subprocess + an MCP
  Streamable HTTP client (`@pytest.mark.integration`).
- `tests/property` — Hypothesis `RuleBasedStateMachine` invariants for
  buffer caps, roster consistency, and tracker targets.

Graceful shutdown: SIGTERM runs the lifespan shutdown, broadcasts
`session.ended` to live participants, and exits. uvicorn re-raises the
captured signal, so the OS-level exit code is `-SIGTERM` rather than `0` —
that's expected.

## Setting up an agent

To make a Claude Code (or any MCP-capable) agent participate, do three
things:

### 1. Register the relay in MCP

Drop a `.mcp.json` next to the project the agent works in, or merge into
the user-level config:

```json
{
  "mcpServers": {
    "sessionweave": {
      "type": "http",
      "url": "http://127.0.0.1:8765/mcp"
    }
  }
}
```

Restart the client so the catalog refreshes. The tools appear as
`mcp__sessionweave__session_create`, `..._join`, etc. For headless
`claude -p` invocations pass `--mcp-config <file>` and an
`--allowedTools mcp__sessionweave__...` allowlist.

### 2. Install the participation skill

The skill lives in the client repo
[`EnzinoBB/SessionWeave`](https://github.com/EnzinoBB/SessionWeave/tree/main/skills/sessionweave-participant).
Symlink (or copy) it into `~/.claude/skills/` so the agent learns *how*
to use the relay (modes, dispatch, invite links, recovery):

```bash
git clone https://github.com/EnzinoBB/SessionWeave.git
mkdir -p ~/.claude/skills
ln -s "$PWD/SessionWeave/skills/sessionweave-participant" \
      ~/.claude/skills/sessionweave-participant
```

Or fetch the file directly:

```bash
mkdir -p ~/.claude/skills/sessionweave-participant
curl -fsSL \
  https://raw.githubusercontent.com/EnzinoBB/SessionWeave/main/skills/sessionweave-participant/SKILL.md \
  -o ~/.claude/skills/sessionweave-participant/SKILL.md
```

A symlink against a clone keeps the skill in sync with future client-repo
updates.

### 3. Install the Python helper + `sessionweave-join` CLI

Install the package in a venv. This gives the agent both the importable
helpers (`sessionweave.invite.parse_link` / `create_link`) and the
`sessionweave-join` CLI:

```bash
pip install -e .

sessionweave-join "swv://127.0.0.1:8765/<sid>"
# Prints relay url, session id, purpose kind, role assignment, and
# instructions in human-readable form. Add --json for machine output.
```

`sessionweave-join` is the canonical "I just received an invite link,
what is it?" entry point. It does **not** join the session — it only
parses + describes — leaving the join decision to the caller.

Once these three are in place an agent can:

- Receive an `swv://` link, parse it, `session_join` with the
  prescribed role, and act per `purpose.kind` or `purpose.instructions`.
- Create a session for someone else, set `purpose`, hand the user the
  generated link, and play the host side.

## Roadmap

Each sub-project gets its own design spec under `docs/superpowers/specs/` and
its own implementation plan.

1. **MCP core + session relay** — anonymous, transient sessions. *Implemented.*
   Design: [`docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md`][spec].
2. **Session persistence** — DB-backed sessions, participants, messages.
3. **User accounts + auth** — bind sessions to a user; gate premium services.
4. **Web dashboard** — read-only over the user's sessions, then management.
5. **Persistent shared context** — first registered-only service.
6. **Additional services** — storage, analysis tools, etc., added on demand.

## License

Apache-2.0. See [`pyproject.toml`](pyproject.toml).

[spec]: docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md
