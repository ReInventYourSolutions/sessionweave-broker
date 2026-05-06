# sessionweave

MCP-native session relay enabling multiple AI agents to collaborate through
a shared session identifier.

See [`docs/VISION.md`](docs/VISION.md) for the project intent and
[`docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md`](docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md)
for the design of sub-project #1.

## Development

```bash
pip install -e '.[dev]'
ruff check . && mypy . && pytest -q -m "not fuzz"
```

## Operating

### Endpoints

- `/mcp` — MCP Streamable HTTP endpoint for agents
- `/healthz` — `200` while serving, `503` once shutdown drain begins
- `/readyz` — `200` once the port is bound
- `/metrics` — Prometheus exposition (text format)

### MCP tools exposed

`session_create`, `session_join`, `session_resume`, `session_leave`,
`session_update_self`, `session_list_participants`, `session_send`,
`session_respond`.

The relay also fans out application notifications: `participant.joined`,
`participant.updated`, `participant.left`, `command.received`,
`result.received`, `command.completed`, `command.cancelled`, `session.ended`.

### Deployment configuration

Every `SessionConfig` parameter is configurable per session by clients within
deployment-set bounds. Each parameter has three env vars:
`SW_DEFAULT_<NAME>`, `SW_MIN_<NAME>`, `SW_MAX_<NAME>` (e.g.
`SW_DEFAULT_IDLE_TTL_SECONDS`, `SW_MAX_PAYLOAD_BYTES`). See the design spec
§2 for the full list of parameters and their factory defaults.

Process binding is controlled by `SW_HOST` and `SW_PORT` (defaults
`0.0.0.0:8765`).

### Run

```bash
docker build -t sessionweave:dev .
docker run --rm -p 8765:8765 sessionweave:dev
```

The container runs as `nobody`, exposes port `8765`, and exits cleanly on
SIGTERM after running the lifespan shutdown (uvicorn re-raises the signal,
so the OS-level exit code is `-SIGTERM`).
