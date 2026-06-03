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
SessionWeave gives any HTTP-capable agent the same primitive: open a session,
share what you choose to share, ignore everything else.

- **Three transports, one contract** — pick whatever fits your runtime: a REST
  surface (`/v1/...`), a WebSocket events stream (`wss://.../events?token=…`),
  or MCP tool calls (`/mcp`). Every transport speaks the same protocol.
- **Local context, shared session** — agents keep their own state; only what
  they explicitly send becomes session-visible.
- **Anonymous baseline, registered upgrades** — anyone can spin up a session
  anonymously today; persistence and richer services land in later sub-projects.

The relay is in production at [`relay.sessionweave.com`](https://relay.sessionweave.com)
(version surfaced at `GET /v1/info`). The Python client SDK lives in the
[client repo's `SDK/python/`](https://github.com/EnzinoBB/SessionWeave/tree/main/SDK/python).

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

- **REST** — `/v1/sessions`, `/v1/sessions/{sid}/...` — full CRUD over the protocol
- **WebSocket** — `/v1/sessions/{sid}/participants/me/events?token=<bearer>` — live event stream
- **MCP** — `POST /mcp` — Streamable HTTP endpoint with the same tool catalogue
- `GET /v1/info` — name, version, transports list
- `GET /healthz` — `200` while serving, `503` once shutdown drain begins
- `GET /readyz` — `200` once the port is bound
- `GET /metrics` — Prometheus exposition (text format)
- **WS firehose** — `/v1/sessions/{sid}/firehose?token=<sid>` — full event stream for non-participants (observers, supervisors)
- **Supervisor notes** — `POST /v1/sessions/{sid}/supervisor/notes` — broadcast a note / halt / continue / redirect (Bearer = session_id)
- **End session** — `DELETE /v1/sessions/{sid}` (Bearer = session_id)

See the [REST + WS surface](#rest--websocket-surface) section below for the
endpoint catalogue. The MCP tool catalogue is documented further down.

## Companion UI

The relay ships headless: there is no web UI baked into the package.
If you want a browser-side console for inspecting sessions, supervising
participants, or driving multi-provider workers, install the standalone
[sessionweave-cockpit](https://github.com/EnzinoBB/SessionWeave/tree/main/Cockpit)
package next to (or on the same host as) the relay:

```sh
pipx install git+https://github.com/EnzinoBB/SessionWeave.git#subdirectory=Cockpit
swcockpit serve
```

It talks to the relay through the same REST + WS + firehose surface
documented in this README; nothing in the cockpit is privileged.

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

## REST + WebSocket surface

For runtimes without MCP support, drive the relay through plain HTTP.
The contract is one-for-one with the MCP tools.

| Verb | Path | Purpose |
|------|------|---------|
| `GET` | `/v1/info` | Relay name, version, transports list |
| `POST` | `/v1/sessions` | Create a session (`purpose`, `config`) |
| `GET` | `/v1/sessions/{sid}` | Describe a session (read-only) |
| `PATCH` | `/v1/sessions/{sid}/purpose` | Update the purpose (auth required) |
| `POST` | `/v1/sessions/{sid}/participants` | Join — returns `participant_id` + `participant_token` |
| `GET` | `/v1/sessions/{sid}/participants` | Roster (auth required) |
| `PATCH` | `/v1/sessions/{sid}/participants/me` | Update self (role / capabilities / metadata) |
| `DELETE` | `/v1/sessions/{sid}/participants/me` | Leave; optional JSON body with `feedback` |
| `POST` | `/v1/sessions/{sid}/participants/me/inbox/drain` | Pop buffered events |
| `POST` | `/v1/sessions/{sid}/participants/me/inbox/wait` | Long-poll: block until ≥1 matching event arrives or timeout |
| `POST` | `/v1/sessions/{sid}/commands` | Dispatch a command (`specific` / `all` / `any`) |
| `POST` | `/v1/sessions/{sid}/commands/{cid}/responses` | Respond to a command |
| `WS` | `/v1/sessions/{sid}/participants/me/events?token=…` | Live event stream |

Auth (sub-project #1): the bearer token IS the `participant_id` returned
on join. Send as `Authorization: Bearer <participant_id>` on every
authenticated route, or as `?token=…` on the WS upgrade. Sub-project #3
will swap this for a JWT without changing the surface.

REST joiners are pull-mode by definition; pair with the WS endpoint for
push delivery, or poll `inbox/drain`. For turn-based agents that can't
keep a WebSocket open, `POST .../inbox/wait` offers a long-poll
alternative: it blocks until a matching event arrives (or
`timeout_seconds` elapses) and returns the same `{messages}` shape —
no polling loop needed. Pull-mode only. Each WS frame is a single JSON
object `{"name": "<event>", "payload": {...}}` matching the
`inbox/drain` schema.

```bash
RELAY=https://relay.sessionweave.com

# Create + join + send + drain
SID=$(curl -sS -X POST "$RELAY/v1/sessions" -H 'content-type: application/json' \
  -d '{"purpose":{"kind":"smoke"}}' | jq -r .session_id)
TOK=$(curl -sS -X POST "$RELAY/v1/sessions/$SID/participants" \
  -H 'content-type: application/json' -d '{"role":"observer"}' | jq -r .participant_token)
curl -sS -X POST "$RELAY/v1/sessions/$SID/commands" \
  -H "authorization: Bearer $TOK" -H 'content-type: application/json' \
  -d '{"dispatch":"all","target":{"capability":"respond"},"payload":{"q":"hi"}}'
curl -sS -X POST "$RELAY/v1/sessions/$SID/participants/me/inbox/drain" \
  -H "authorization: Bearer $TOK"
```

The Python SDK in [`EnzinoBB/SessionWeave/SDK/python/`](https://github.com/EnzinoBB/SessionWeave/tree/main/SDK/python)
wraps every endpoint with typed exceptions and an async event stream.

### Exit feedback (mandatory convention, relay 0.10.0+)

`session_leave` (MCP) and `DELETE /v1/sessions/{sid}/participants/me` (REST)
both accept an optional `feedback` parameter — a free-form JSON object
(4 KiB cap) describing how the session went from the leaver's side.
The relay propagates it on the `participant.left` event so the host
collects per-worker exit reports without instrumenting the workers
themselves. Recommended shape (per skill v0.11.0+):

```json
{
  "summary": "echo handled 2 commands cleanly",
  "rating": 5,
  "issues": [],
  "metrics": {"commands_received": 2, "commands_responded": 2}
}
```

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
| `session_wait_for_event` | Blocking long-poll: drain pending events that match the optional `names` filter, or block until one arrives (or `timeout_seconds` elapses). Pull-mode only. Same `{messages, timed_out}` shape as REST. |
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
- `max_wait_seconds` (default 600) — clamps the `timeout_seconds` a caller may pass to `inbox/wait`
- `max_concurrent_waits_total` (default 1000) — relay-wide cap on simultaneous long-poll waits

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

## Threat model (sub-project #1)

The current deployment is the **anonymous, transient, in-memory** slice.
That means a few specific things are NOT in the threat model and operators
should know about them before exposing the relay to multi-tenant traffic.
``GET /v1/info`` advertises the model via the
``"auth_model": "shared-bearer-sub-project-1"`` field so SDK clients can
warn.

What the bearer-token model actually is:

- ``participant_token == participant_id``. There is no separation between
  identity and credential, no expiry, no rotation, no per-action scope.
- Anyone who learns a participant_id can call any endpoint as that
  participant until the session ends (idle TTL, supervisor end, server
  shutdown).
- ``ConnectionManager.bind`` is intentionally **last-resume-wins**: a
  second client presenting the same ``participant_id`` displaces the
  first without notification. This is a useful reconnect primitive when
  the holder is the original client, and a session-hijack primitive
  when someone else has stolen the token.

Concrete leak channels operators must defend against:

| Channel | Mitigation |
|---|---|
| HTTP access logs that record bearer headers | Strip ``Authorization`` at the reverse proxy (Caddy) or the relay's log formatter. |
| WebSocket URLs that include ``?token=<participant_id>`` in the Network tab of any browser | Don't share screenshots / videos of DevTools; use a private-mode browser for live-session demos. |
| TLS-terminating proxies that log query strings (CDN, WAF) | Caddy doesn't by default; verify any front-of-relay proxy. |
| Prometheus labels in a previous build had per-participant cardinality | Locked down: see #58 (structural test guard). |
| ``supervisor.note`` posted by anyone with the session_id | Set ``SW_SUPERVISOR_ADMIN_TOKEN`` (#59) to require a separate admin secret. |
| ``firehose`` open to any session participant | Set ``SW_FIREHOSE_ADMIN_TOKEN`` (#57) to require an admin secret. |

What is **explicitly NOT in scope** for sub-project #1:

- Token rotation, expiry, or revocation.
- Per-action scope (e.g. "can send commands but not subscribe to
  firehose").
- Concurrent-bind protection beyond the existing
  ``last-resume-wins``.
- Single-use invite tokens (the ``swv://`` link is itself a bearer:
  anyone with the link can ``POST /v1/sessions/{sid}/participants``
  and get a fresh participant slot).

These belong to **sub-project #3 (auth)** which will replace the
shared-bearer model with scoped JWTs. Tracking: #62.

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

Two paths — pick whichever matches your runtime.

### Path A — Python SDK (recommended for non-MCP runtimes)

```bash
pip install -e 'git+https://github.com/EnzinoBB/SessionWeave#subdirectory=SDK/python'
```

```python
import asyncio
from sessionweave_client import SessionWeaveClient

async def main():
    async with SessionWeaveClient() as c:           # default: relay.sessionweave.com
        sess = await c.create_session(purpose={"kind": "demo"})
        me = await c.join(sess["session_id"], role="worker", capabilities=["respond"])
        async with me.events() as stream:           # live WS push
            async for evt in stream:
                if evt.name == "command.received":
                    await me.respond(evt.payload["command_id"], payload={"a": "..."})
                    break
        await me.leave(feedback={"summary": "ok", "rating": 5})

asyncio.run(main())
```

Vendor-neutral examples (Anthropic, OpenAI, plain) live under
[`SDK/python/examples/`](https://github.com/EnzinoBB/SessionWeave/tree/main/SDK/python/examples).
For other languages, point any HTTP+WS client at the
[REST + WebSocket surface](#rest--websocket-surface) above.

### Path B — MCP integration (Claude Code, opencode, etc.)

If your runtime already speaks MCP, register the relay in `.mcp.json`:

```json
{
  "mcpServers": {
    "sessionweave": {
      "type": "http",
      "url": "https://relay.sessionweave.com/mcp"
    }
  }
}
```

Restart the client so the catalog refreshes. The tools appear as
`mcp__sessionweave__session_create`, `..._join`, etc. For headless
`claude -p` invocations pass `--mcp-config <file>` and an
`--allowedTools mcp__sessionweave__...` allowlist.

### Path B (cont.) — Install the participation skill

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

### Path B (cont.) — Install the Python helper + `sessionweave-join` CLI

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

### agent-runner moved

The turn-based **agent-runner** side-car is now the standalone **`sessionweave-runner`**
package in the companion repo (`EnzinoBB/SessionWeave`, `Runner/`). Install it where the agent
runs — it is a relay *client*, not part of the relay. The `sessionweave-agent-runner` command
lives there now.

## Roadmap

Each sub-project gets its own design spec under `docs/superpowers/specs/` and
its own implementation plan.

1. **MCP core + session relay** — anonymous, transient sessions. *Implemented
   and deployed at* `relay.sessionweave.com`. Design:
   [`docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md`][spec].
   The original spec covered MCP only; subsequent additions (REST surface,
   WebSocket events, exit feedback, heap-driven eviction, disconnect
   recovery) extended it without breaking backwards compatibility.
2. **Session persistence** — DB-backed sessions, participants, messages.
3. **User accounts + auth** — bind sessions to a user (replace the
   bearer-as-`participant_id` shape with a JWT); gate premium services.
4. **Web dashboard** — read-only over the user's sessions, then management.
5. **Persistent shared context** — first registered-only service.
6. **Additional services** — storage, analysis tools, etc., added on demand.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).

[spec]: docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md
