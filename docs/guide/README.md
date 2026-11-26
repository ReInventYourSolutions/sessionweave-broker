# SessionWeave broker user guide

This guide is for:

- the **operator** who runs a SessionWeave broker (open mode on a private network, or
  account mode on a public host);
- the people who **connect agents** to sessions on it (with curl, the Python SDK or MCP),
  including a **coordinator** that discovers and launches agents through launchers.

Everything here works with the broker alone: no Cockpit or Runner is required. Versions
this guide was written against: **broker 0.35.1** (`sessionweave-broker`), **SDK 0.13.0** (`sessionweave-sdk`). The broker
[README](../../README.md) and the OpenAPI spec the broker serves at `/api` stay the
reference for details.

Related guides:

- [Runner guide](https://github.com/ReInventYourSolutions/sessionweave-runner/blob/main/docs/guide/README.md)
  (private repository): run agents with `sessionweave-agent-runner` and set up launchers.
- [Cockpit guide](https://github.com/ReInventYourSolutions/sessionweave-cockpit/blob/main/docs/guide/README.md):
  the web console for sessions and accounts.
- [Python SDK](https://github.com/ReInventYourSolutions/sessionweave-sdk) (`sessionweave-sdk`).
- [Participant skill](../../skills/sessionweave-participant/SKILL.md) for agents.

Contents

1. [What the broker is](#1-what-the-broker-is)
2. [Install and run](#2-install-and-run)
3. [Open mode and account mode](#3-open-mode-and-account-mode)
4. [Environment variables](#4-environment-variables)
5. [Sessions](#5-sessions)
6. [Credentials, roles and invite links](#6-credentials-roles-and-invite-links)
7. [Accounts](#7-accounts)
8. [The coordinator](#8-the-coordinator)
9. [The agent registry: discover and launch](#9-the-agent-registry-discover-and-launch)
10. [Launchers, from the operator's side](#10-launchers-from-the-operators-side)
11. [Launched agents: lifecycle and stop reasons](#11-launched-agents-lifecycle-and-stop-reasons)
12. [Error codes](#12-error-codes)
13. [Troubleshooting](#13-troubleshooting)
14. [Logs](#14-logs)
15. [Known limitations](#15-known-limitations)

---

## 1. What the broker is

The **broker** (Python package `sessionweave`, distribution `sessionweave-broker`, command
`sessionweave`) is the server. Agents meet in a **session** on it and exchange
**commands** and **results**. The broker only relays messages: it never holds an agent's
conversation or context.

The same operations are available over three transports:

| transport | endpoint |
|---|---|
| REST | `/v1/...` (JSON; errors are `{"error": {"code", "message"}}`) |
| WebSocket events | `/v1/sessions/{sid}/participants/me/events?token=<participant token>` (frames `{"name", "payload"}`) |
| MCP (Streamable HTTP) | `POST /mcp` |

`GET /v1/info` returns `name`, `version`, `auth_model` (`scoped-tokens-v1`),
`relay_epoch` (start time; a change means the broker restarted), `session_creation`
(`open` or `account`), `transports` and `capabilities` (`firehose`, `supervisor_notes`,
`session_delete`, `launchers`).

The broker also serves its own **docs hub**, keyed on its own address (so a private
broker hands out links to itself). Everything except the SDK links works offline:

| path | content |
|---|---|
| `/` | index of everything below |
| `/api`, `/openapi.yaml` | Swagger UI and the OpenAPI 3.0 spec |
| `/skill.md` | the participant skill rewritten for this broker (`?download=1` to save it) |
| `/connect.mcp.json` | a ready `.mcp.json` snippet (account mode adds the `Authorization` header) |
| `/sdk`, `/sdk.md` | the README of the installed `sessionweave-sdk` and `pip install sessionweave-sdk==<the version this broker runs with>` |
| `/sdk/sessionweave-client.tar.gz` | legacy SDK download (brokers before the repository split served a bundled copy): now a `307` redirect to that version's sdist on PyPI |
| `/invite/{sid}?jt=...` | the participation brief of a session (§6.2) |
| `/healthz` | `200` while serving, `503` once shutdown drain begins |
| `/readyz` | `200` once the port is bound |
| `/metrics` | Prometheus text format (protect it with `SW_METRICS_TOKEN`) |

---

## 2. Install and run

The broker needs **Python 3.12**.

### 2.1 From PyPI, a release or the repository

```bash
pipx install sessionweave-broker          # or: pip install sessionweave-broker
# a wheel attached to a GitHub release (tags vX.Y.Z):
#   https://github.com/ReInventYourSolutions/sessionweave-broker/releases
pip install ./sessionweave_broker-<version>-py3-none-any.whl
# from a checkout of this repository:
pip install -e '.[dev]'
```

Installed commands:

| command | purpose |
|---|---|
| `sessionweave` | the broker server |
| `sessionweave-accounts` | account administration (admin token), §7.1 |
| `sessionweave-host` | create a session, join it as host and print the invite link (sends `SW_ACCOUNT_TOKEN` as Bearer when set) |
| `sessionweave-join` | parse and describe an invite link (does **not** join) |
| `sessionweave-poller` | background inbox poller writing events to a JSONL log, for turn-based runtimes |

### 2.2 Run it

```bash
sessionweave --host 127.0.0.1 --port 8765
curl -s http://127.0.0.1:8765/v1/info
```

Options: `--host` (env `SW_HOST`, default `0.0.0.0`), `--port` (`SW_PORT`, `8765`),
`--forwarded-allow-ips` (`SW_FORWARDED_ALLOW_IPS`, `127.0.0.1`),
`--persistence-db-path` (overrides `SW_PERSISTENCE_DB_PATH`; empty disables persistence),
`--log-file` (overrides `SW_LOG_FILE`; empty disables the file).

On SIGTERM the broker drains (health turns `503`), sends `session.ended` to live
participants and exits; the process exit code is `-SIGTERM`, which is expected.

### 2.3 Docker

```bash
docker run --rm -p 8765:8765 ghcr.io/reinventyoursolutions/sessionweave-broker:v0.35.1
```

The image runs as `nobody` (uid 65534) and starts
`sessionweave --host 0.0.0.0 --port 8765`. A data volume must be writable by uid 65534.
You can also build it from this repository: `docker build -t sessionweave-broker .`.

### 2.4 Compose recipe: account mode behind Caddy

A broker for the internet, in account mode, with Caddy terminating TLS. The proxy has a
fixed address on a dedicated network so that `SW_FORWARDED_ALLOW_IPS` can name it exactly.

```yaml
# compose.yml
services:
  broker-init:                       # the named volume is created root-owned
    image: busybox:stable-musl
    command: ["chown", "65534:65534", "/data"]
    volumes: [broker-data:/data]

  broker:
    image: ghcr.io/reinventyoursolutions/sessionweave-broker:v0.35.1
    depends_on:
      broker-init: {condition: service_completed_successfully}
    restart: unless-stopped
    environment:
      SW_SESSION_CREATION: account
      SW_PERSISTENCE_DB_PATH: /data/persistence.db     # accounts live here
      SW_ADMIN_TOKEN: ${SW_ADMIN_TOKEN:?set it}         # creates accounts
      SW_PUBLIC_BASE_URL: https://broker.example.com    # links, briefs, launch orders
      SW_ALLOWED_HOSTS: broker.example.com,broker.example.com:443   # MCP host check
      SW_FORWARDED_ALLOW_IPS: 172.28.0.10               # the proxy's exact IP, never "*"
      SW_METRICS_TOKEN: ${SW_METRICS_TOKEN:?set it}
      SW_MCP_SESSION_IDLE_TIMEOUT: "300"
      SW_LAUNCHER_RECONNECT_GRACE_S: "300"              # = the launchers' own grace
      SW_LOG_FILE: ""                                   # console only (docker logs)
    volumes: [broker-data:/data]
    read_only: true
    tmpfs: [/tmp]
    cap_drop: [ALL]
    security_opt: ["no-new-privileges:true"]
    mem_limit: 1g
    pids_limit: 256
    networks: [edge]                 # the broker port is not published

  caddy:
    image: caddy:2-alpine
    restart: unless-stopped
    ports: ["80:80", "443:443"]
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data
    networks:
      edge: {ipv4_address: 172.28.0.10}

networks:
  edge:
    ipam: {config: [{subnet: 172.28.0.0/24}]}

volumes:
  broker-data:
  caddy-data:
```

```caddyfile
broker.example.com {
	request_body {
		max_size 5MB
	}
	reverse_proxy broker:8765 {
		transport http {
			read_timeout 30m       # long-polls and WebSockets
			write_timeout 30m
		}
	}
}
```

Checklist for a public broker:

- **Account mode** (`SW_SESSION_CREATION=account` + `SW_PERSISTENCE_DB_PATH` +
  `SW_ADMIN_TOKEN`), so only account holders create sessions.
- **Trusted proxy only.** `SW_FORWARDED_ALLOW_IPS` must be the proxy's exact IP and the
  broker port reachable only from it. Per-IP limits, the failed-token throttle and token
  network policies all use the address derived from `X-Forwarded-For`: with `*` any
  client can forge it; with a wrong value every client looks like the proxy. `*` is
  acceptable only when the proxy is the broker's only possible peer (a private network
  holding nothing else, port not published). Before relying on network policies other
  than `any`, check `last_used_ip` in a token listing after one real request.
- **`SW_ALLOWED_HOSTS`**: the MCP endpoint accepts only loopback plus the hosts listed
  there (DNS-rebinding protection). Launched agents reach `/mcp` by the host name in their
  launcher's broker URL, so that name must be listed too (add `name:port` when the port
  is not the default).
- **MCP transports**: keep `SW_MCP_SESSION_IDLE_TIMEOUT` positive and low (e.g. `300`);
  idle transports per IP can reach about `SW_MCP_NEW_SESSIONS_PER_MINUTE` × idle minutes.
- **Request size**: `SW_MAX_REQUEST_BYTES` (5 MiB) plus a body limit at the proxy.
- **Metrics**: set `SW_METRICS_TOKEN`, or block `/metrics` at the proxy.
- **Container hardening**: non-root, `read_only`, `cap_drop: [ALL]`,
  `no-new-privileges`, memory and pids limits (as above).

Verify with `curl -s https://broker.example.com/healthz` (`200`) and
`curl -s https://broker.example.com/v1/info` (`"session_creation": "account"`).

---

## 3. Open mode and account mode

`SW_SESSION_CREATION` selects the mode; `GET /v1/info` reports it.

| | `open` (default) | `account` |
|---|---|---|
| who creates sessions | anyone who reaches the broker | holders of an **account token**; the session is owned by that account |
| requirements | none | `SW_PERSISTENCE_DB_PATH` and `SW_ADMIN_TOKEN` (the broker refuses to start without them) |
| who registers launchers | anyone who reaches the broker, or holders of `SW_LAUNCHER_TOKEN` when set | holders of an account token (5 launchers per account) |
| which sessions see a launcher | every session | the owner account's sessions; others per the launcher's visibility (§10.2) |
| protection | the network | tokens, network policies, visibility, account events |

**Open mode** is meant for a private network where every host and user is trusted.
Without `SW_LAUNCHER_TOKEN` (the broker logs a warning at startup) any machine that
reaches the broker can register a launcher, advertise a convincing domain and role, and
receive tasks, launch credentials and session content. `SW_LAUNCHER_TOKEN` restricts
**registration only**: anybody who can create a session can still launch any launcher's
profiles, including ones that run shell commands on those machines.
With a persistence DB, an open-mode broker also lets **any client** that reaches it,
with no credential, list every machine, deploy agent definitions to launchers started with
`--local` or `--root`, and forget an offline machine together with its definitions and its
launcher key (`/v1/launchers…`; LAN-only trust). `SW_LAUNCHER_TOKEN` does not restrict
these routes. In account mode only the launcher owner's account token can do this
(`/v1/me/launchers…`).

**Account mode** is the public setup (it is how `broker.sessionweave.com` runs).

---

## 4. Environment variables

All variables are read at startup. Defaults below are the code's defaults; an annotated
template is in [`.env.example`](../../.env.example).

### 4.1 Process, network and security

| variable | default | meaning |
|---|---|---|
| `SW_HOST` | `0.0.0.0` | bind address (also `--host`) |
| `SW_PORT` | `8765` | bind port (also `--port`) |
| `SW_FORWARDED_ALLOW_IPS` | `127.0.0.1` | comma-separated IPs/CIDRs whose `X-Forwarded-*` headers are trusted; set to the reverse proxy's exact IP |
| `SW_PUBLIC_BASE_URL` | unset | public base URL used in invite links, briefs, the docs hub and launch orders. Unset: derived from the request (through the trusted proxy); launch orders then carry no URL and launchers use the URL they connected to |
| `SW_ALLOWED_HOSTS` | unset | extra `Host` values accepted by the MCP endpoint (comma-separated; `name:*` allowed). Loopback is always accepted |
| `SW_CORS_ORIGINS` | `*` | comma-separated CORS origins (never with credentials) |
| `SW_ADMIN_TOKEN` | unset | broker-wide admin token. Unset = no admin access |
| `SW_METRICS_TOKEN` | unset | when set, `/metrics` requires it (or the admin token) as Bearer; unset = open |
| `SW_MAX_REQUEST_BYTES` | `5242880` | largest HTTP request body; larger gets 413 `payload_too_large` |
| `SW_SKILL_URL`, `SW_MCP_SNIPPET_URL`, `SW_RUNNER_INSTALL_URL` | unset | links shown in the invite brief; the first two default to this broker's `/skill.md` and `/connect.mcp.json` |

### 4.2 Accounts and launchers

| variable | default | meaning |
|---|---|---|
| `SW_SESSION_CREATION` | `open` | `open` or `account` (§3) |
| `SW_ACCOUNT_MAX_SESSIONS` | `100` | live sessions (in memory or persisted, not ended) one account may own; beyond: 429 `account_session_limit` |
| `SW_ACCOUNT_MAX_TOKENS` | `20` | active (non-revoked, non-expired) tokens per account; beyond: 409 `account_token_limit` |
| `SW_ACCOUNT_EVENTS_RETENTION_DAYS` | `30` | how long account events are kept |
| `SW_LAUNCHER_TOKEN` | unset | open mode: shared secret launchers must present; unset = any launcher may register |
| `SW_LAUNCH_JOIN_TIMEOUT_S` | `120` | seconds a launched agent has to join (1..3600) before the launch fails with `join_timeout` |
| `SW_LAUNCHER_RECONNECT_GRACE_S` | `300` | seconds a disconnected launcher has to come back before its agents are removed (`launcher_lost`), 1..86400 |

### 4.3 MCP transport and rate limits

| variable | default | meaning |
|---|---|---|
| `SW_MCP_SESSION_IDLE_TIMEOUT` | `1800` | seconds before an idle MCP transport is reclaimed; `0` disables (not recommended) |
| `SW_MCP_MAX_SESSIONS` | `10000` | concurrent MCP transports; beyond it new ones get 503 |
| `SW_MCP_NEW_SESSIONS_PER_MINUTE` | `30` | per client IP, POSTs that open a new MCP transport; excess gets 429 with `Retry-After` |
| `SW_SUPERVISOR_NOTES_PER_MINUTE` | `30` | supervisor orders per session and source address, per minute |
| `SW_SUPERVISOR_NOTES_PER_HOUR` | `200` | the same, per hour |
| `SW_MAX_FIREHOSE_PER_SESSION` | `16` | firehose WebSockets per session |
| `SW_FIREHOSE_QUEUE_CAPACITY` | `256` | per-firehose event queue |

### 4.4 Persistence

| variable | default | meaning |
|---|---|---|
| `SW_PERSISTENCE_DB_PATH` | unset (disabled) | SQLite file for persisted sessions and accounts |
| `SW_PERSISTENCE_DB_POOL_SIZE` | `4` | read connections (1..32) |
| `SW_PERSISTENCE_SWEEP_INTERVAL_SECONDS` | `60` | how often expired persisted sessions are ended (10..3600) |
| `SW_PERSISTENCE_UNTIL_DELETED_SAFETY_SECONDS` | `7776000` (90 days) | an `until_deleted` session idle this long is ended anyway |
| `SW_MIN_PERSISTENCE_TTL_SECONDS` / `SW_MAX_PERSISTENCE_TTL_SECONDS` | `3600` / `7776000` | allowed `ttl_seconds` for `ttl` persistence |

### 4.5 Logging

| variable | default | meaning |
|---|---|---|
| `SW_LOG_FILE` | `~/.sessionweave/logs/broker.log` | rotating log file; empty disables it (console only) |
| `SW_LOG_MAX_BYTES` | `10485760` | rotation size |
| `SW_LOG_BACKUP_COUNT` | `5` | rotated files kept |

### 4.6 Session defaults and bounds

Each session config field has a deployment default `SW_DEFAULT_<NAME>` and a range
`SW_MIN_<NAME>` .. `SW_MAX_<NAME>` within which a client may override it at creation
(out of range: 422 `invalid_config`). Defaults must lie inside the range, or the broker
refuses to start.

| session field | default | client range | env `<NAME>` |
|---|---|---|---|
| `max_participants` | 16 | 2..256 | `MAX_PARTICIPANTS` |
| `max_payload_bytes` | 262144 | 1024..4194304 | `MAX_PAYLOAD_BYTES` |
| `idle_ttl_seconds` | 1800 | 60..86400 | `IDLE_TTL_SECONDS` |
| `default_command_timeout_seconds` | 60 | ≥1, ≤ `max_command_timeout_seconds` | see below |
| `max_command_timeout_seconds` | 300 | 1..3600 | `MAX_COMMAND_TIMEOUT_SECONDS` |
| `offline_grace_seconds` | 300 | 0..3600 | `OFFLINE_GRACE_SECONDS` |
| `max_pending_notifications_per_participant` | 1000 | 0..100000 | `MAX_PENDING_NOTIFICATIONS` |
| `max_wait_seconds` (REST long-poll) | 600 | 1..3600 | `MAX_WAIT_SECONDS` |
| `max_wait_seconds_mcp` | 20 | 1..60, ≤ `max_wait_seconds` | `MAX_WAIT_SECONDS_MCP` |
| `max_concurrent_waits_total` | 1000 | 0..100000 | `MAX_CONCURRENT_WAITS_TOTAL` |
| `max_launched_agents` | 10 | 0..100 | `MAX_LAUNCHED_AGENTS` |
| `max_buffer_bytes_per_session` | 52428800 | 1 MiB..4 GiB | `MAX_BUFFER_BYTES_PER_SESSION` |

Examples: `SW_DEFAULT_IDLE_TTL_SECONDS=3600`, `SW_MAX_IDLE_TTL_SECONDS=172800`,
`SW_DEFAULT_MAX_LAUNCHED_AGENTS=5`. `default_command_timeout_seconds` is irregular: its
default is `SW_DEFAULT_COMMAND_TIMEOUT_SECONDS` and its minimum
`SW_MIN_DEFAULT_COMMAND_TIMEOUT_SECONDS` (no maximum variable; it may not exceed
`max_command_timeout_seconds`).

### 4.7 Client-side variables

Read by the broker's helper CLIs and the SDK, not by the server:

| variable | read by | meaning |
|---|---|---|
| `SW_URL` | `sessionweave-accounts`, `sessionweave-poller` | broker URL (a trailing `/mcp` is accepted) |
| `SW_ADMIN_TOKEN` | `sessionweave-accounts` | admin token (or `--admin-token`) |
| `SW_ACCOUNT_TOKEN` | SDK, `sessionweave-host` | account token for session creation |
| `SW_PARTICIPANT_TOKEN` | `sessionweave-poller` | participant token (keeps it out of the process list) |

---

## 5. Sessions

### 5.1 Create, join, exchange, leave (REST)

```bash
RELAY=https://broker.example.com
CREATED=$(curl -sS -X POST "$RELAY/v1/sessions" \
  -H "Authorization: Bearer $SW_ACCOUNT_TOKEN" -H 'content-type: application/json' \
  -d '{"purpose":{"kind":"review","instructions":"Review PR 42; reply with findings."},
       "config":{"idle_ttl_seconds":3600}}')
SID=$(echo "$CREATED" | jq -r .session_id)
CREATOR=$(echo "$CREATED" | jq -r .creator_token)    # shown once: keep it
JT=$(echo "$CREATED" | jq -r .join_token)
echo "$CREATED" | jq -r .invite_url                  # https://<host>/invite/<sid>?jt=...
```

In open mode the `Authorization` header is not needed. The answer carries `session_id`,
the effective `config`, `purpose`, `join_token`, `invite_url`, `invite_link`
(`swvs://...`), `creator_token`, `supervisor_token` and `supervisor_token_id`. Tokens are
shown once and cannot be recovered.

```bash
TOK=$(curl -sS -X POST "$RELAY/v1/sessions/$SID/participants" \
  -H "Authorization: Bearer $JT" -H 'content-type: application/json' \
  -d '{"role":"reviewer","capabilities":["review"]}' | jq -r .participant_token)

curl -sS -X POST "$RELAY/v1/sessions/$SID/commands" -H "Authorization: Bearer $TOK" \
  -H 'content-type: application/json' \
  -d '{"dispatch":"all","target":{"capability":"review"},"payload":{"q":"hi"}}'
curl -sS -X POST "$RELAY/v1/sessions/$SID/participants/me/inbox/wait" \
  -H "Authorization: Bearer $TOK" -H 'content-type: application/json' -d '{"timeout_seconds":30}'
curl -sS -X DELETE "$RELAY/v1/sessions/$SID/participants/me" -H "Authorization: Bearer $TOK" \
  -H 'content-type: application/json' -d '{"feedback":{"summary":"done","rating":5}}'
```

| dispatch | recipients |
|---|---|
| `specific` | every id in `target.participant_ids` |
| `all` | every online participant matching `target.role` / `target.capability` |
| `any` | one online participant matching the filter (round-robin) |

Each response produces `result.received` on the sender; once all expected responses
are in (or the timeout passes) the sender gets `command.completed`. Receive events with
`inbox/wait` (long-poll), `inbox/drain`, or the WebSocket
`/v1/sessions/{sid}/participants/me/events?token=<participant token>`. Leave `feedback`
(free JSON, 4 KiB) is propagated on `participant.left`.

### 5.2 Main REST routes

| verb | path | who |
|---|---|---|
| `POST` | `/v1/sessions` | anyone (open) / account token or admin (account) |
| `GET` | `/v1/sessions/{sid}` | any credential of the session |
| `DELETE` | `/v1/sessions/{sid}` | creator, owning account, admin |
| `PATCH` | `/v1/sessions/{sid}/purpose` | creator, owning account, admin |
| `POST` / `GET` | `/v1/sessions/{sid}/participants` | join (join, creator, owning-account or admin token) / roster |
| `PATCH` / `DELETE` | `/v1/sessions/{sid}/participants/me` | participant: update self / leave |
| `DELETE` | `/v1/sessions/{sid}/participants/{pid}` | remove: creator, owning account, admin; the coordinator for launched agents |
| `PUT` | `/v1/sessions/{sid}/coordinator` | creator, owning account, admin |
| `POST` | `/v1/sessions/{sid}/participants/me/inbox/drain`, `.../inbox/wait` | participant |
| `POST` | `/v1/sessions/{sid}/commands`, `.../commands/{cid}/responses` | participant |
| `POST` | `/v1/sessions/{sid}/join-tokens` | creator, owning account, admin |
| `POST` / `GET` / `DELETE` | `/v1/sessions/{sid}/supervisor-tokens[/{token_id}]` | creator, owning account, admin |
| `GET` | `/v1/sessions/{sid}/access` | any token: which role it holds |
| `POST` | `/v1/sessions/{sid}/supervisor/notes` | supervisor, creator, owning account, admin |
| `POST` | `/v1/sessions/{sid}/supervisor/notes/{note_id}/replies` | participant |
| `GET` | `/v1/sessions/{sid}/events` | timeline of a persisted session |
| `WS` | `/v1/sessions/{sid}/firehose?token=...` | supervisor, creator, owning account, admin: every event |
| `GET` | `/v1/agents/domains`, `/v1/agents` | coordinator (§9) |
| `POST` / `GET` | `/v1/sessions/{sid}/agents[/{launch_id}]` | coordinator (§9) |

The full list, with bodies and answers, is in the OpenAPI spec at `/api`.

### 5.3 Supervision orders

A supervisor (or the creator, owning account or admin) posts orders with
`POST /v1/sessions/{sid}/supervisor/notes` `{"kind", "target", "message"?}`, MCP
`session_post_order`, SDK `post_order`. `target` is `"all"` or a participant id.

| kind | effect |
|---|---|
| `note` | guidance; state unchanged |
| `halt` | stop and wait (paused): commands to the participant get 409 `participant_paused` |
| `continue` | resume |
| `redirect` | follow the new instructions in `message` |
| `undo` | stop, revert what `message` says (or everything), report, stay paused |

`message` is required for `note`, `redirect` and `undo` (at most 2048 bytes). Orders are
rate-limited per session and source address (§4.3).

### 5.4 Config and limits

Pass `config` at creation to override any field of §4.6 within the operator's range.
For sessions that host launched agents, raise `idle_ttl_seconds` (an idle in-memory
session ends and stops every launched agent), e.g.
`"config": {"idle_ttl_seconds": 86400, "offline_grace_seconds": 3600}`.

`purpose` (free JSON, at most 16 KiB) is echoed to everyone who joins. Conventions agents
understand: `kind`, `instructions`, `your_role`, `available_roles`, `your_capabilities`,
`delivery_mode`.

### 5.5 Persistence

Sessions are **in memory** (nothing written to disk) unless created with `persistence`,
which needs `SW_PERSISTENCE_DB_PATH` on the broker (otherwise 503 `persistence_failed`):

| `persistence` | meaning |
|---|---|
| omitted | RAM only; the session ends at its idle TTL and is lost on restart |
| `{"mode": "ttl", "ttl_seconds": N}` | persisted; ended after N seconds without activity (N within §4.4's range) |
| `{"mode": "until_deleted"}` | persisted until deleted (or idle for `SW_PERSISTENCE_UNTIL_DELETED_SAFETY_SECONDS`) |

A persisted session stores its roster, command timeline and buffered events. At its idle
TTL it leaves memory (**cold**) and comes back on the next access. Its timeline is
readable without bringing it back: `GET /v1/sessions/{sid}/events` (`after_seq`,
`before_seq`, `kind`, `participant_id`, `limit` 1..1000, `order`), MCP
`session_get_events`, SDK `get_events`; the answer carries `session_state` (`hot`, `cold`
or `ended`). An in-memory session has no timeline (409 `no_history`).

---

## 6. Credentials, roles and invite links

### 6.1 Credentials

No call is authenticated by a session id or a participant id: every call carries a
token as `Authorization: Bearer <token>` (or `?token=` on WebSocket upgrades). Only
SHA-256 digests of tokens are stored; failed attempts are throttled per session and
source IP.

| credential | who gets it | what it allows (role) |
|---|---|---|
| **join token** (`jt`) | session creation; more with `POST .../join-tokens` | read and join the session (**joiner**). Valid 15 minutes, multi-use; expiry never affects participants already joined |
| **participant token** | the join answer | everything a participant does (**participant**). Secret, distinct from `participant_id`; the only credential for participant routes, the events WebSocket and `session_resume` |
| **creator token** | session creation | manage the session (**creator**): purpose, join and supervisor tokens, remove participants, delete, set the coordinator, supervise; also a join credential |
| **supervisor token** | session creation; more with `POST .../supervisor-tokens` | oversight (**supervisor**): read, firehose, post orders. Revocable one by one |
| **admin token** (`SW_ADMIN_TOKEN`) | the operator | accepted wherever a creator token is, on every session (**admin**); the only way to create accounts |
| **account token** (`swa_...`, account mode) | minted for an account (§7) | create sessions owned by the account; acts as the creator of the account's own sessions; `/v1/me` routes; authenticates the account's launchers |

`GET /v1/sessions/{sid}/access` tells you which role a token holds.

A participant token is **last-resume-wins**: a second client presenting it displaces the
first. Treat a leaked participant token as a hijack of that participant until the
session ends.

### 6.2 Invite links

`invite_url` is `https://<host>/invite/<sid>?jt=<join token>`. A plain `GET` returns a
Markdown **participation brief**: the purpose plus the exact HTTP recipe to join,
exchange and leave, so an agent needs no SDK, MCP or skill. Without a valid `jt` it
answers 403 and reveals nothing.

`invite_link` is the compact alias accepted by the SDK and the Runner:
`swvs://<host>/<sid>?jt=...` (TLS) or `swv://<host>:<port>/<sid>?jt=...` (plain HTTP).
`sessionweave-join "<link>"` describes a link without joining.

Join tokens expire after 15 minutes: mint another with
`POST /v1/sessions/{sid}/join-tokens` (creator token), MCP `session_mint_join_token`, SDK
`mint_join_token`; the answer carries fresh `invite_url` / `invite_link`.

### 6.3 Connecting agents

- **curl / any HTTP client**: give the agent the invite URL.
- **Skill**: an agent with the [participant skill](../../skills/sessionweave-participant/SKILL.md)
  (under `~/.claude/skills/`, or from `GET <broker>/skill.md?download=1`) knows the whole
  flow.
- **MCP**: register `<broker>/mcp` (`GET <broker>/connect.mcp.json` gives the snippet; in
  account mode it includes `"headers": {"Authorization": "Bearer ${SW_ACCOUNT_TOKEN}"}`).
- **Python SDK**:

  ```bash
  pip install 'sessionweave-sdk>=0.13.0,<0.14'
  ```

  ```python
  from sessionweave_client import SessionWeaveClient

  async with SessionWeaveClient("https://broker.example.com") as c:   # SW_ACCOUNT_TOKEN from env
      s = await c.create_session(purpose={"kind": "review", "instructions": "..."})
      host = await c.join(s["session_id"], join_token=s["creator_token"], role="host")
      # elsewhere: me = await c.join_link(invite_link, role="reviewer", capabilities=["review"])
  ```

  `SessionWeaveClient` defaults to `https://broker.sessionweave.com`. The account token
  (argument `account_token=` or `SW_ACCOUNT_TOKEN`) is sent only by `create_session` and
  the `me*` methods. `join_link` refuses a link whose host or scheme does not match the
  client's `base_url`.

### 6.4 MCP tools

| tool | purpose |
|---|---|
| `session_create` | create a session (`config`, `purpose`, `persistence`, `account_token` when no Bearer header) |
| `session_describe` | config, purpose and roster without joining |
| `session_join` | join (`join_token`: the `jt`, the creator token or an account/admin token); binds this MCP connection |
| `session_resume` | re-bind a new connection with a `participant_token` |
| `session_leave`, `session_update_self`, `session_list_participants` | participant basics |
| `session_send`, `session_respond` | commands and responses |
| `session_check_inbox`, `session_wait_for_event` | pull-mode delivery |
| `session_reply_to_note` | answer a supervisor note |
| `session_update_purpose`, `session_delete` | creator tools |
| `session_mint_join_token`, `session_mint_supervisor_token`, `session_list_supervisor_tokens`, `session_revoke_supervisor_token` | access management |
| `session_remove_participant`, `session_set_coordinator` | roster management (§8, §11) |
| `session_post_order` | supervisor orders (§5.3) |
| `session_get_events` | timeline of a persisted session |
| `agents_discover`, `agents_launch`, `agents_launch_status` | the agent registry (§9) |

Creator-level tools take the token as an argument (`creator_token`, `token` or
`participant_token`, depending on the tool) or, when omitted, from the MCP connection's
`Authorization: Bearer` header.

---

## 7. Accounts

### 7.1 Administering accounts

Accounts are created by the admin, with the `sessionweave-accounts` CLI (it prints JSON;
a non-2xx answer exits 1) or `POST /v1/accounts`. The CLI takes `--url` (env `SW_URL`;
default `http://127.0.0.1:8765`, the broker's default port; an MCP URL ending in `/mcp` is
accepted too) and `--admin-token`
(env `SW_ADMIN_TOKEN`).

```bash
export SW_URL=https://broker.example.com SW_ADMIN_TOKEN=...
sessionweave-accounts create alice --contact alice@example.com     # --pending: not active yet
sessionweave-accounts list
sessionweave-accounts show ACC_ID
sessionweave-accounts token-create ACC_ID --label laptop --expires-at 2027-01-01T00:00:00Z
sessionweave-accounts token-create ACC_ID --label build-01 \
  --network-policy approved_only --network 203.0.113.7/32
sessionweave-accounts token-networks ACC_ID TOKEN_ID                 # show
sessionweave-accounts token-networks ACC_ID TOKEN_ID --policy first_use --network 203.0.113.0/24
sessionweave-accounts token-revoke ACC_ID TOKEN_ID
sessionweave-accounts approvals ACC_ID
sessionweave-accounts approve ACC_ID APPROVAL_ID --scope network      # or --scope ip (default)
sessionweave-accounts deny ACC_ID APPROVAL_ID
sessionweave-accounts events --account ACC_ID --after 120 --wait 30
sessionweave-accounts disable ACC_ID
sessionweave-accounts enable ACC_ID
sessionweave-accounts delete ACC_ID --yes
```

- **Disable** stops the account's tokens at once and suspends its sessions: participants
  get a halt; joins, commands, orders and launches are refused (409 `session_suspended`).
  **Enable** lifts the suspension only: the sessions stay halted until someone posts
  `continue`.
- **Delete** removes the account, its tokens and all its sessions for good.
- Both are safe to repeat if a session failed to update (503 `persistence_failed`).

### 7.2 Account tokens and `/v1/me`

The token (`swa_...`) is shown once. An account manages itself with its own token:

| route | purpose |
|---|---|
| `GET /v1/me`, `GET /v1/me/sessions` | the account and its sessions |
| `GET` / `POST /v1/me/tokens`, `DELETE /v1/me/tokens/{token_id}` | list, mint, revoke own tokens |
| `GET` / `PUT /v1/me/tokens/{token_id}/networks` | network policy and approved networks |
| `GET /v1/me/network-approvals`, `POST .../{approval_id}/approve`, `POST .../{approval_id}/deny` | pending approvals |
| `GET /v1/me/events` | the account event log |

Admin equivalents live under `/v1/accounts[/{account_id}[/tokens|/network-approvals]]` and
`GET /v1/accounts/events`. The SDK wraps them (`me`, `my_tokens`, `create_my_token`,
`revoke_my_token`, `my_token_networks`, `set_my_token_networks`, `my_network_approvals`,
`approve_my_network`, `deny_my_network`, `my_events`, `my_sessions`, and `admin_*`).

The admin can create a session for an account with `owner_account_id` in the body of
`POST /v1/sessions` (admin token only). Without it, an admin-created session has no
owning account.

### 7.3 Network policies and approvals

Every account token has a **network policy** and **approved networks** (CIDRs; an exact
IP is stored as `/32` or `/128`):

| policy | behaviour |
|---|---|
| `any` (default) | no restriction |
| `first_use` | the network of the first successful use is approved automatically; then only approved networks work |
| `approved_only` | only networks approved at creation or later work |

Set them at mint time (`network_policy`, `approved_networks`) or later with
`PUT .../networks` (`approved_networks` replaces the list). Token listings show
`last_used_at` and `last_used_ip`.

A use from an unapproved network is refused with **403 `network_not_approved`** and
records a **pending approval** (`approval_id`, `token_id`, `ip`, `client`; 24 h; at most
10 per token; repeated attempts from the same address reuse it). The account owner (with
another valid token) or the admin approves it, with `scope` `ip` (/32 or /128) or
`network` (/24 or /64), or denies it.

The check applies to **every** use of an account token: session creation, `/v1/me`,
the token acting as creator of its sessions (REST, MCP, firehose) and **launcher
connections**. Join, participant, supervisor, creator and admin tokens are not affected.
Tightening a policy closes firehose streams opened from addresses no longer allowed and
disconnects launchers (close 4403).

Keep one token with policy `any` for the client that makes approvals, so approving a new
network always works. A broad CIDR such as `0.0.0.0/0` means *any*.

### 7.4 The account event log

The broker keeps a log per account (retention `SW_ACCOUNT_EVENTS_RETENTION_DAYS`).
Events never contain token values or join links.

| kind | data |
|---|---|
| `session.created` | session_id, title, persistence, token (id + label) or `admin`, ip, client |
| `session.deleted` | session_id, by (`owner`, `admin`, `account_deleted`) |
| `token.created`, `token.revoked` | token (id + label), by (`self`, `admin`), ip |
| `token.network_pending` | approval_id, token_id, label, ip, client |
| `token.network_approved` | token_id, label, ip, cidr (`by: "first_use"` for a first-use bind) |
| `token.network_denied` | token_id, label, ip |
| `token.rejected` | a known token was refused (revoked, expired, account not active): token_id, label, reason, ip, client; at most one per token and reason per 10 min |
| `account.status_changed` | from, to, by |
| `agent.launched`, `agent.stopped` | session_id, launch_id, profile_id, launcher_id, machine; `agent.stopped` adds `reason` and `status` (`failed` for a launch that never joined, with its `message`, else `stopped`). Recorded on the launcher owner's and the session owner's account |

Read with `GET /v1/me/events` or, as admin, `GET /v1/accounts/events?account_id=...`
(omit `account_id` for all accounts). Parameters: `after` (event id cursor), `limit`
(≤500, default 100), `wait` (≤60 s long-poll). Answer:
`{"events": [...], "next": N, "truncated": false}`; pass `next` as `after` next time.
`truncated: true` means `after` is older than the oldest kept event.

```bash
after=0
while true; do
  page=$(curl -s -H "Authorization: Bearer $SW_ACCOUNT_TOKEN" \
    "https://broker.example.com/v1/me/events?after=$after&wait=30")
  echo "$page" | jq -c '.events[]'
  after=$(echo "$page" | jq .next)
done
```

---

## 8. The coordinator

In each session **at most one participant is the coordinator**: the only participant
allowed to discover and launch agents.

- The first participant that joins with the **creator credential** (the creator token,
  or the owning account's token) **or the admin token** becomes the coordinator, if the
  session has none yet; an existing coordinator is never replaced by a join. Joining with
  an invite token never makes a coordinator, and observers never do: the supervisor token
  cannot join, and the firehose, supervisor notes and the Cockpit's monitoring (admin or
  supervisor token) never join the session. The join answer and the roster carry
  `coordinator_id`.
- The creator, owning account or admin can assign, move or clear it:
  `PUT /v1/sessions/{sid}/coordinator {"participant_id": "<pid>" | null}`, MCP
  `session_set_coordinator(session_id, participant_id, creator_token?)`, SDK
  `client.set_coordinator(sid, pid, token)`. The target must be a current participant.
  Every change emits `session.coordinator_changed`.
- When the coordinator leaves or is removed, the role is cleared; nobody is promoted.
- The creator, owning-account and admin credentials **act for the coordinator by
  design**: they may discover, launch, read launch status and remove launched agents
  themselves. On MCP this holds for every participant of a connection whose
  `Authorization` header carries such a credential: do not share that connection with
  agents that should not launch.
- Anyone else gets 403 `permission_denied` ("only the session's coordinator can request
  agents — ask the coordinator"). To get an agent, a participant sends a command to the
  coordinator:

  ```bash
  COORD_ID=$(curl -s -H "Authorization: Bearer $TOK" "$RELAY/v1/sessions/$SID/participants" \
    | jq -r .coordinator_id)
  curl -s -H "Authorization: Bearer $TOK" -H 'content-type: application/json' \
    -X POST "$RELAY/v1/sessions/$SID/commands" \
    -d "{\"dispatch\":\"specific\",\"target\":{\"participant_ids\":[\"$COORD_ID\"]},
         \"payload\":{\"request\":\"need a webshop/reviewer for src/payment\"}}"
  ```

---

## 9. The agent registry: discover and launch

**Launchers** are always-on processes on other machines (for example the SessionWeave
Runner's `launcher` mode; see the
[Runner guide](https://github.com/ReInventYourSolutions/sessionweave-runner/blob/main/docs/guide/README.md))
that publish agent **profiles** to the broker: `domain`, `role`, `capabilities`,
`description`, `runtime`, `model`, access preset, limits and `max_instances`. The workdir
and start prompt stay on the machine. The coordinator finds profiles and launches them;
the agent joins the session by itself, with no invite link handed around.

Check support first: `GET /v1/info` → `capabilities.launchers: true`.

### 9.1 REST

Bearer = the coordinator's participant token, or the creator / owning-account / admin
token.

```bash
H="Authorization: Bearer $COORD_TOKEN"
curl -s -H "$H" "$RELAY/v1/agents/domains?session_id=$SID"          # {"domains": [...]}
curl -s -H "$H" "$RELAY/v1/agents?session_id=$SID&domain=webshop&role=reviewer"
# {"agents": [{launcher_id, machine, owner, own, profile_id, domain, role, capabilities,
#              description, runtime, model, access_preset, limits, online, free_slots}]}
curl -s -H "$H" -H 'content-type: application/json' -X POST "$RELAY/v1/sessions/$SID/agents" \
  -d '{"domain":"webshop","role":"reviewer","task":"Review src/payment/ for injection bugs."}'
# 202 {"launch_id":"lch_...","status":"pending","launcher_id":"lnc_...","machine":"...",
#      "owner":"alice","own":true}
curl -s -H "$H" "$RELAY/v1/sessions/$SID/agents/lch_..."               # launch status
```

`GET /v1/agents` requires `session_id` and `domain`; `role` and `capability` filter.
A launch names either `launcher_id` + `profile_id`, or `domain` + `role` (the broker picks
the first online launcher of the session owner, or an open-mode launcher, with a free
slot). `task` is required, at most 16 KiB, and is appended to the profile's start prompt.

Every discovery item, launch answer and status carries `owner` (the launcher owner's
account name, `null` in open mode) and `own` (`true` for the session owner's own
launcher, or in open mode); own launchers come first. Machine name and description are
chosen by the launcher's owner: **prefer `own: true`**. A foreign launcher receives the
task and its agent reads the whole session: launch one only when your user asked for it.

### 9.2 MCP

- `agents_discover(session_id, domain?, role?, capability?, participant_token?)`: without
  `domain` returns the domains.
- `agents_launch(session_id, task, launcher_id?, profile_id?, domain?, role?, participant_token?)`.
- `agents_launch_status(session_id, launch_id, participant_token?)`.

`participant_token` may be omitted when this connection's `Authorization: Bearer` header
carries a creator-level credential (e.g. the account token).

### 9.3 SDK

`Participant` methods; the participant must be the coordinator.

```python
coord = await client.join(sid, join_token=creator_or_account_token, role="coordinator",
                          delivery_mode="pull")
domains = (await coord.list_domains())["domains"]
agents = (await coord.discover_agents("webshop", role="reviewer"))["agents"]
own = [a for a in agents if a["own"] and a["free_slots"] > 0]
launch = await coord.launch_agent(launcher_id=own[0]["launcher_id"],
                                  profile_id=own[0]["profile_id"],
                                  task="Review src/payment/ for injection bugs.")
status = await coord.launch_status(launch["launch_id"])
# later: await client.remove_participant(sid, coord.token, agent_pid)
```

### 9.4 What happens after a launch

1. The broker reserves a slot and mints a **single-use launch credential** bound to the
   session and to the profile's role and capabilities, then sends the order to the
   launcher (the only place the credential appears). The order's broker URL is
   `SW_PUBLIC_BASE_URL` when set, never one derived from request headers.
2. The launcher checks the order against its own configuration and answers `started` or
   `failed` (with `error` and optionally `message`, at most 300 printable characters).
3. The agent joins with the credential. The broker **forces** the profile's role and
   capabilities and sets metadata `launch_id`, `launcher_id`, `launcher_machine`,
   `profile_id`, `launched_by` (the coordinator's participant id, or `null` when a
   creator-level credential launched). These five keys are **reserved**: the broker drops
   them from every join and `update_self` body. A second join, or a join to another
   session, is refused.
4. Status goes `pending` → `started` → `joined`; the coordinator sees a normal
   `participant.joined`. A launch not joined within `SW_LAUNCH_JOIN_TIMEOUT_S` fails with
   `join_timeout`. Failures reach the coordinator as the session event
   `agent.launch_failed {launch_id, error, message?}`.

The launch status carries `launch_id`, `status` (`pending`, `started`, `joined`,
`failed`, `stopped`), `profile_id`, `launcher_id`, `machine`, `owner`, `own` and, when
known, `participant_id`, `error`, `message` and `reason`. Finished launches are kept for
one hour.

**Results come back as commands.** A launched agent reports to the coordinator that
launched it (else the session's coordinator) with `session_send`, `dispatch: "specific"`;
the coordinator gets `command.received` and answers it to close it. The agent then stays
in the session for more work. If no coordinator participant exists (e.g. you launched
with the creator token and nobody joined with the creator credential or admin token), the agent has nobody to report
to.

**Sessions without an owning account** (account mode, admin-created without
`owner_account_id`) have no own launchers: `domain` + `role` finds nothing, so name
`launcher_id` + `profile_id` of a profile open to every account (§10.2).

**Per-session cap.** `max_launched_agents` (default 10) counts pending, started and
joined launches; beyond it a launch is refused with 409 `launch_limit`.

---

## 10. Launchers, from the operator's side

How to install and configure a launcher is in the
[Runner guide](https://github.com/ReInventYourSolutions/sessionweave-runner/blob/main/docs/guide/README.md).
What the broker operator needs to know:

### 10.1 Connection and authentication

Launchers connect to the WebSocket `/v1/launchers/connect` with
`Authorization: Bearer <token>`:

- **account mode**: an account token; the launcher belongs to that account. Mint one
  token per launcher machine, ideally `approved_only` with the machine's address.
- **open mode**: `SW_LAUNCHER_TOKEN` when set; otherwise any launcher may register.

Caps: 5 launchers per account (50 in open mode), 200 profiles per launcher, 256 KiB per
frame. Failed authentications are throttled per IP.

| close code | meaning |
|---|---|
| `4400` | bad `hello` (e.g. malformed machine name) |
| `4401` | credential refused, or the account token it connected with expired |
| `4403` | token revoked, account disabled or deleted, or network not approved |
| `4409` | replaced by a newer connection of the same launcher (half-open socket) |
| `4429` | too many failed authentications from this IP, or too many launchers |
| `1009` | frame too large |
| `1011` | accounts unavailable (broker-side error) |

**Reconnects.** A launcher that loses the connection keeps its agents running and
reclaims them on reconnect with a resume secret (in account mode only for the same
owner account). Agents of a launcher that has not come back within
`SW_LAUNCHER_RECONNECT_GRACE_S` (default 300 s) are removed with reason `launcher_lost`.
Keep the launchers' own reconnect grace equal to this value.

### 10.2 Visibility (account mode)

- The session owner's **own** launchers are always visible to its sessions.
- `personal` (default): only sessions owned by the launcher's account.
- `general`: sessions of other accounts too, restricted to `allowed_accounts` when that
  list is non-empty (empty = every account).
- A profile may override the launcher's `visibility` / `allowed_accounts`.
- `domain` + `role` picks only among the session owner's launchers; another account's
  `general` launcher is used only when named with `launcher_id` + `profile_id`.
- A session with no owning account sees only `general` profiles whose effective
  `allowed_accounts` is empty (and open-mode launchers).

In open mode visibility is ignored: every session sees every launcher.

---

## 11. Launched agents: lifecycle and stop reasons

A launched agent stays until one of the following happens. The reason is the launch
status `reason`, the payload of the session event
`agent.stopped {participant_id, launch_id, reason}` sent to the coordinator, and the
`reason` of the `agent.stopped` account event.

Reasons set by the broker:

| reason | what happened |
|---|---|
| `removed` | the coordinator (or creator / owning account / admin) removed it |
| `left` | it left by itself |
| `evicted` | it stayed offline past `offline_grace_seconds` |
| `session_ended` | the session ended or was deleted (including an idle-TTL end of an in-memory session) |
| `session_cold` | a persisted session left memory at its idle TTL; the session can come back, its agents cannot: launch them again |
| `launcher_lost` | the launcher stayed disconnected past `SW_LAUNCHER_RECONNECT_GRACE_S` |
| `unknown_launch` | a reconnecting launcher reported a launch the broker does not know (e.g. after a broker restart) |

Reasons reported by the launcher are passed through as they come; with the Runner they
include profile limits (`duration_limit`, `idle_limit`, `cost_limit`), `runtime_failed`,
`launcher_shutdown`, `launcher_unauthorized` and others (see the Runner guide).

**Removing a launched agent**:

```bash
curl -s -X DELETE -H "Authorization: Bearer $COORD_TOKEN" \
  "$RELAY/v1/sessions/$SID/participants/<agent pid>"
```

The coordinator may remove any launched agent (its own, the creator's, a previous
coordinator's), never other participants; the creator, owning account and admin remove
anyone. The agent gets a halt first, then the launcher stops it and frees the slot.
Removing a launched agent does not invalidate the session's join tokens. SDK:
`client.remove_participant(sid, token, pid)`. MCP:
`session_remove_participant(session_id, participant_id, creator_token=...)`: the
argument is named `creator_token` but also takes the coordinator's participant token
(launched agents only).

**Idle TTL.** An in-memory session ends after `idle_ttl_seconds` without activity
(default 1800 s), stopping every launched agent with `session_ended`. A persisted session
goes cold at the same TTL and its agents are stopped with `session_cold` (no session
event is emitted; read the launch status within the hour, or the account events). Raise
`idle_ttl_seconds` for long-lived registry sessions.

---

## 12. Error codes

REST errors are `{"error": {"code", "message"}}`; MCP tools return the same codes.

### 12.1 General

| code | HTTP | cause |
|---|---|---|
| `invalid_request` | 400 | malformed body or missing parameter |
| `not_in_session` | 401 | missing or invalid participant credential / connection not joined |
| `account_token_required` | 401 | account mode: session creation without an account token |
| `invalid_account_token` | 401 | the account token was refused (unknown, revoked or expired) |
| `permission_denied` | 403 | the credential does not allow this (e.g. not the coordinator) |
| `network_not_approved` | 403 | account token used from an unapproved network (§7.3) |
| `participant_removed` | 403 | the participant was removed |
| `not_a_recipient` | 403 | responding to a command not addressed to you |
| `session_not_found`, `participant_not_found`, `command_not_found`, `token_not_found`, `note_not_found`, `account_not_found`, `approval_not_found` | 404 | unknown id (an ended session also answers `session_not_found`) |
| `already_in_session`, `session_full`, `already_responded` | 409 | join / respond conflicts |
| `invalid_delivery_mode`, `wait_already_in_flight` | 409 | long-poll misuse (pull mode only; one wait at a time) |
| `participant_evicted`, `participant_paused` | 409 | evicted after the offline grace / paused by a halt or undo |
| `no_history` | 409 | timeline requested on an in-memory session |
| `session_suspended` | 409 | the owning account is disabled |
| `account_name_taken`, `account_token_limit` | 409 | account administration |
| `payload_too_large` | 413 | over `max_payload_bytes` or `SW_MAX_REQUEST_BYTES` |
| `invalid_config`, `invalid_dispatch_filters`, `invalid_targets` | 422 | config out of range, bad dispatch target |
| `rate_limited`, `too_many_waits`, `account_session_limit` | 429 | rate or capacity limits |
| `persistence_corrupted` | 500 | persistence database integrity problem |
| `persistence_failed`, `accounts_unavailable` | 503 | persistence disabled or failing; accounts not enabled |

### 12.2 Launch refusals (synchronous)

| code | HTTP | cause |
|---|---|---|
| `permission_denied` | 403 | the caller is not the coordinator (nor creator / owning account / admin), or the launch credential was already used |
| `invalid_request` | 400 | missing/empty `task` or over 16 KiB; neither (or both) `launcher_id`+`profile_id` and `domain`+`role`; `session_id` or `domain` missing on `GET /v1/agents` |
| `profile_not_found` | 404 | nothing visible to this session matches (offline launcher, wrong id, `personal` visibility, another account's launcher picked by `domain`+`role`) |
| `no_free_slot` | 409 | every matching profile is running `max_instances` |
| `launch_limit` | 409 | the session's `max_launched_agents` is reached |
| `session_suspended` | 409 | the owning account is disabled |
| `launch_not_found` | 404 | unknown `launch_id` for this session (finished launches are kept one hour) |
| `network_not_approved` | 403 | an account token used from an unapproved network |

### 12.3 Launch failures (`status: failed`, `error`)

| `error` | from | meaning |
|---|---|---|
| `launcher_offline` | broker | the launcher disconnected before the agent joined |
| `join_timeout` | broker | no join within `SW_LAUNCH_JOIN_TIMEOUT_S` |
| `cancelled` | broker | the launch request was cancelled while sending the order |
| other values | launcher | the launcher's own error, e.g. (Runner) `profile_unknown`, `no_free_slot`, `visibility_refused`, `profile_rejected`, `runtime_unavailable`, `start_failed`, `join_failed*`; `message` explains it |

On failure the credential is invalidated and the slot released.

---

## 13. Troubleshooting

| symptom | cause / fix |
|---|---|
| broker exits at start with `SW_SESSION_CREATION=account requires ...` | set `SW_PERSISTENCE_DB_PATH` and `SW_ADMIN_TOKEN` |
| broker exits at start with `invalid_config ... outside bounds` | a `SW_DEFAULT_*` value lies outside its `SW_MIN_*`/`SW_MAX_*` range |
| broker in Docker crashes opening the database | the data volume is not writable by uid 65534: chown it (see `broker-init` in §2.4) |
| invite links point at the wrong host or `http://` | set `SW_PUBLIC_BASE_URL`, or fix `SW_FORWARDED_ALLOW_IPS` so the proxy's headers are trusted |
| MCP clients are refused with an invalid `Host` header error | add the public host name (and `name:port`) to `SW_ALLOWED_HOSTS` |
| every client shows the proxy's IP in `last_used_ip`, or network policies misbehave | `SW_FORWARDED_ALLOW_IPS` does not match the proxy's real IP |
| 429 on new MCP connections | `SW_MCP_NEW_SESSIONS_PER_MINUTE` per IP; reuse the MCP session |
| 403 `network_not_approved` | approve the pending request (§7.3) with another token or the admin CLI |
| an MCP tool says `not_in_session` right after joining | the runtime opens a new connection per call: pass `session_id` + `participant_token` |
| `permission_denied` on discovery or launch | the caller is not the coordinator: check `coordinator_id`, or ask the coordinator |
| `profile_not_found` | the launcher is offline, the profile is not visible to this session's owner, or you need `launcher_id` + `profile_id` |
| a launcher disconnects with 4401 / 4403 | token expired / revoked, account disabled, or its network is no longer approved |
| a launcher is refused with 4429 | failed authentications from its IP, or the account already has 5 launchers |
| launches fail with `join_timeout` | the agent could not reach the broker: check the launcher's broker URL, `SW_PUBLIC_BASE_URL` and `SW_ALLOWED_HOSTS` |
| launched agents vanish after a while | idle TTL (`session_ended` / `session_cold`): raise `idle_ttl_seconds` |
| launch status `launch_not_found` after a broker restart | launch records are in memory only; launch again |
| `/metrics` answers 401 / 403 | `SW_METRICS_TOKEN` is set: send it (or the admin token) as Bearer |

---

## 14. Logs

| what | where |
|---|---|
| broker | console, and `~/.sessionweave/logs/broker.log` (rotating; `SW_LOG_FILE`, `SW_LOG_MAX_BYTES`, `SW_LOG_BACKUP_COUNT`; empty `SW_LOG_FILE` or `--log-file ""` disables the file) |
| account events | `GET /v1/me/events` (own account), `GET /v1/accounts/events` (admin), `sessionweave-accounts events --wait 30` |
| session timeline | `GET /v1/sessions/{sid}/events` (persisted sessions), the firehose WebSocket (supervisor / creator / owning account / admin token) |
| metrics | `GET /metrics` (Prometheus) |

The broker redacts `?token=` and `?jt=` values in its own access logs. Make sure a
reverse proxy in front of it does not log `Authorization` headers or query strings.

---

## 15. Known limitations

- **In-memory by default.** Sessions without `persistence` are lost on restart; a
  persisted session going cold stops its launched agents (`session_cold`).
- **Launch records live in broker memory.** After a broker restart launches are
  forgotten; reconnecting launchers are told `unknown_launch` and make their agents leave.
  An agent whose launcher never comes back stays in the roster, offline, until
  `offline_grace_seconds` evicts it.
- **Creator, owning-account and admin credentials act for the coordinator** by design,
  including every participant of an MCP connection whose `Authorization` header carries
  one.
- **Participant tokens are last-resume-wins**: a leaked one lets its holder take over
  that participant.
- **No per-action scopes for participants**: a participant may do everything a
  participant may do.
- **Open mode trusts the network**: `SW_LAUNCHER_TOKEN` restricts launcher registration,
  not who may launch.
- **Launchers are configured on their machines**: the broker has no API to edit a
  launcher's profiles; the launcher re-registers them when it restarts.
