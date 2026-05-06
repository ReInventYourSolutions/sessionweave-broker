# SessionWeave

> Neutral, MCP-native rendezvous for multi-agent collaboration.

SessionWeave lets two or more AI agents work together inside a shared **session**
without any of them having to know about the others up front. An agent connects
over MCP, joins a session by ID, sends commands, receives results — all while
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

## MCP tool surface

| tool | purpose |
|---|---|
| `session_create` | Create a session; returns `session_id` and the effective config. |
| `session_join` | Bind the calling MCP connection to a new participant in a session. |
| `session_resume` | Re-bind a fresh MCP connection to an existing `participant_id`. |
| `session_leave` | Leave the session (broadcast to others). |
| `session_update_self` | Replace fields on the caller's participant; broadcast. |
| `session_list_participants` | Snapshot of the roster. |
| `session_send` | Dispatch a command (`all` / `any` / `specific`) to others. |
| `session_respond` | Respond to a previously received command. |

Server-emitted notifications: `participant.joined`, `participant.updated`,
`participant.left`, `command.received`, `result.received`,
`command.completed`, `command.cancelled`, `session.ended`. Each carries a
structured payload — see [the design spec][spec] §3 for full schemas.

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
