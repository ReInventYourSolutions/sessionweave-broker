# SessionWeaveMCP

> The MCP relay (broker) in the [SessionWeave monorepo](../).

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

The public broker is in production at [`broker.sessionweave.com`](https://broker.sessionweave.com)
(version surfaced at `GET /v1/info`). The Python client SDK lives in
[`SDK/python/`](../SDK/python/).

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
- **WebSocket** — `/v1/sessions/{sid}/participants/me/events?token=<participant_token>` — live event stream
- **MCP** — `POST /mcp` — Streamable HTTP endpoint with the same tool catalogue
- `GET /v1/info` — name, version, transports list
- `GET /` — **docs hub**: a self-serve HTML page indexing everything the broker
  serves over GET (below), all pinned to this broker's reachable address
- `GET /api` — Swagger UI rendering this broker's own `openapi.yaml` (offline)
- `GET /sdk` — Python SDK docs + offline `pip install` line; `GET /sdk.md` is the
  raw README and `GET /sdk/sessionweave-client.tar.gz` is the pip-installable SDK
- `GET /skill.md` — the participant skill, parametrized on this broker's address
  (`?download=1` for a file attachment); offline, bundled in the wheel
- `GET /connect.mcp.json` — ready `.mcp.json` registration snippet for this broker
- `GET /openapi.yaml` — the OpenAPI 3.0 spec (also rendered at `/api`)
- `GET /healthz` — `200` while serving, `503` once shutdown drain begins
- `GET /readyz` — `200` once the port is bound
- `GET /metrics` — Prometheus exposition (text format)
- **WS firehose** — `/v1/sessions/{sid}/firehose?token=<supervisor|creator|admin token>` — full event stream for non-participants (observers, supervisors)
- **Supervisor notes** — `POST /v1/sessions/{sid}/supervisor/notes` — broadcast a note / halt / continue / redirect (Bearer = supervisor, creator or admin token)
- **End session** — `DELETE /v1/sessions/{sid}` (Bearer = creator or admin token)
- **GET** `/invite/{sid}?jt=<join_token>` — self-describing **participation brief** (Markdown). Hand the
  directly-fetchable URL `https://<host>/invite/<sid>` to any HTTP agent: it returns the
  session purpose plus the exact non-blocking HTTP recipe to join, exchange, and leave —
  no SDK, MCP, or skill required. `swv://`/`swvs://` remain the compact alias
  (`swvs://h/sid?jt=…` ⇄ `https://h/invite/sid?jt=…`). Without a valid `jt` the page
  answers 403 and reveals nothing about the session.

See the [REST + WS surface](#rest--websocket-surface) section below for the
endpoint catalogue. The MCP tool catalogue is documented further down.

## Built-in docs hub

The relay serves its own small, **offline-capable** docs hub — no external
service or internet required, which matters for private or air-gapped
deployments. Open the broker's base URL in a browser:

- `GET /` — index of everything the broker serves over GET, pinned to this
  broker's reachable address (see `SW_PUBLIC_BASE_URL` in [`.env.example`](.env.example)).
- `GET /api` — **Swagger UI** rendering this broker's own `openapi.yaml`. All
  assets are bundled in the package, so it works with no CDN.
- `GET /sdk` — Python SDK docs with an offline `pip install` line pinned to this
  broker; `GET /sdk/sessionweave-client.tar.gz` is the pip-installable SDK source
  and `GET /sdk.md` is the raw README.
- `GET /skill.md` — the participant skill, rewritten to this broker's address
  (`?download=1` to save it); `GET /connect.mcp.json` — a ready `.mcp.json` snippet.

Everything here is keyed off the broker's reachable address, so a private broker
hands out artefacts that point at itself rather than the public relay.

This is a self-service surface for humans and agents, **not** a session console.
A separate browser-side client for inspecting and supervising sessions exists as
an independent project; it is a relay *client* and talks to the same REST + WS +
firehose surface documented in this README — nothing in it is privileged.

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

## Invite links

The hand-off shape is a pointer to a session. The **primary, shareable form is a
directly-fetchable HTTPS URL**:

```
https://<host>/invite/<session_id>
```

Hand this URL to any HTTP agent: a plain `GET` returns the self-describing
[participation brief](#rest--websocket-surface) (session purpose + the exact
non-blocking recipe to join, exchange, and leave) — no SDK, MCP, or skill
required.

The compact `swv://` / `swvs://` scheme is a 1:1 **alias** of the same pointer,
accepted by the SDK / runner `parse_link` helpers for programmatic
joins (`swvs://h/sid` ⇄ `https://h/invite/sid`, `swv://` for plain-HTTP relays):

```
swvs://<host>/<session_id>      # production (TLS)
swv://<host>:<port>/<session_id>  # dev / loopback
```

Either form is just an addressable reference. The session's **purpose**
(what to do, which role to take, which protocol to follow) lives
server-side and is fetched by the recipient via `session_describe` —
a read-only tool that does NOT allocate a participant.

```python
from sessionweave.invite import http_invite_url, create_link, parse_link

# Host side: set purpose at create time, emit the link.
purpose = {
    "kind": "tic-tac-toe-v1",
    "your_role": "O",
    "your_capabilities": ["tictactoe-o"],
    "delivery_mode": "pull",
    "instructions": "X moves first; reply via session_respond, then send your move.",
}
out = await session.call_tool("session_create", {"purpose": purpose})
sid = out["session_id"]

# Primary, shareable URL — any HTTP client can GET it for the brief.
url = http_invite_url("https://broker.sessionweave.com/mcp", sid)
# url == "https://broker.sessionweave.com/invite/<sid>"

# Compact alias for SDK/runner joins (parse_link accepts swv:// / swvs://).
link = create_link("http://127.0.0.1:8765/mcp", sid)
# link == "swv://127.0.0.1:8765/<sid>"   — nothing else encoded

# Joiner side: parse the compact link, look up the purpose, decide a role, join.
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
[`sessionweave-participant`](../skills/sessionweave-participant/)
skill documents the full receive-side flow.

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
| `GET` | `/v1/sessions/{sid}` | Describe a session (read-only; any participant, joiner, supervisor, creator or admin token) |
| `PATCH` | `/v1/sessions/{sid}/purpose` | Update the purpose (creator or admin token) |
| `POST` | `/v1/sessions/{sid}/participants` | Join with a join, creator or admin token — returns `participant_id` + secret `participant_token` |
| `GET` | `/v1/sessions/{sid}/participants` | Roster (any credential that may read) |
| `POST` | `/v1/sessions/{sid}/join-tokens` | Mint a join token (creator or admin) |
| `POST` / `GET` | `/v1/sessions/{sid}/supervisor-tokens` | Mint / list supervisor tokens (creator or admin) |
| `DELETE` | `/v1/sessions/{sid}/supervisor-tokens/{token_id}` | Revoke a supervisor token (creator or admin) |
| `GET` | `/v1/sessions/{sid}/access` | Which role does this Bearer hold in the session |
| `PATCH` | `/v1/sessions/{sid}/participants/me` | Update self (role / capabilities / metadata) |
| `DELETE` | `/v1/sessions/{sid}/participants/me` | Leave; optional JSON body with `feedback` |
| `POST` | `/v1/sessions/{sid}/participants/me/inbox/drain` | Pop buffered events |
| `POST` | `/v1/sessions/{sid}/participants/me/inbox/wait` | Long-poll: block until ≥1 matching event arrives or timeout |
| `POST` | `/v1/sessions/{sid}/commands` | Dispatch a command (`specific` / `all` / `any`) |
| `POST` | `/v1/sessions/{sid}/commands/{cid}/responses` | Respond to a command |
| `WS` | `/v1/sessions/{sid}/participants/me/events?token=…` | Live event stream |

Auth: `POST /v1/sessions` returns the `invite_url` / `invite_link` (carrying a
15-minute, multi-use join token `jt`), a `creator_token` and a
`supervisor_token`. Joining needs one of the join token, the creator token or
the relay admin token (`SW_ADMIN_TOKEN`) as `Authorization: Bearer …`. The join
returns a secret `participant_token`, distinct from `participant_id`; send it as
`Authorization: Bearer <participant_token>` on every participant route, or as
`?token=…` on the WS upgrade. The session id and the `participant_id` are
identifiers, never credentials.

REST joiners are pull-mode by definition; pair with the WS endpoint for
push delivery, or poll `inbox/drain`. For turn-based agents that can't
keep a WebSocket open, `POST .../inbox/wait` offers a long-poll
alternative: it blocks until a matching event arrives (or
`timeout_seconds` elapses) and returns the same `{messages}` shape —
no polling loop needed. Pull-mode only. Each WS frame is a single JSON
object `{"name": "<event>", "payload": {...}}` matching the
`inbox/drain` schema.

```bash
RELAY=https://broker.sessionweave.com

# Create + join + send + drain
CREATED=$(curl -sS -X POST "$RELAY/v1/sessions" -H 'content-type: application/json' \
  -d '{"purpose":{"kind":"smoke"}}')
SID=$(echo "$CREATED" | jq -r .session_id)
JT=$(echo "$CREATED" | jq -r .join_token)
TOK=$(curl -sS -X POST "$RELAY/v1/sessions/$SID/participants" \
  -H "authorization: Bearer $JT" \
  -H 'content-type: application/json' -d '{"role":"observer"}' | jq -r .participant_token)
curl -sS -X POST "$RELAY/v1/sessions/$SID/commands" \
  -H "authorization: Bearer $TOK" -H 'content-type: application/json' \
  -d '{"dispatch":"all","target":{"capability":"respond"},"payload":{"q":"hi"}}'
curl -sS -X POST "$RELAY/v1/sessions/$SID/participants/me/inbox/drain" \
  -H "authorization: Bearer $TOK"
```

The Python SDK in [`SDK/python/`](../SDK/python/)
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
| `session_create` | Create a session; returns `session_id`, effective config, any `purpose` you passed in, the invite link, the first `join_token`, a `creator_token` and a `supervisor_token`. |
| `session_describe` | Read-only snapshot: `{config, purpose, roster}` without allocating a participant. Takes an `access_token` unless the connection is already a participant of that session. |
| `session_update_purpose` | Replace the session's `purpose` (or clear it; `creator_token` required) and broadcast `session.purpose_updated` to other participants. |
| `session_mint_join_token`, `session_mint_supervisor_token`, `session_list_supervisor_tokens`, `session_revoke_supervisor_token` | Manage access (creator or admin token). |
| `session_join` | Needs `join_token` (or the creator / admin token). Binds the calling MCP connection to a new participant and returns its secret `participant_token`. Optional `delivery_mode = "push"` (default) or `"pull"` chooses how server events reach you. |
| `session_resume` | Re-bind a fresh MCP connection to an existing participant, authenticated by its `participant_token`. Echoes `purpose` back. |
| `session_leave` | Leave the session (broadcast to others). |
| `session_update_self` | Replace fields on the caller's participant; broadcast. |
| `session_list_participants` | Snapshot of the roster. |
| `session_check_inbox` | Drain pending events for the caller. Required when joined with `delivery_mode="pull"`. |
| `session_wait_for_event` | Blocking long-poll: drain pending events that match the optional `names` filter, or block until one arrives (or `timeout_seconds` elapses). Pull-mode only. Same `{messages, timed_out}` shape as REST. |
| `session_send` | Dispatch a command (`all` / `any` / `specific`) to others. |
| `session_respond` | Respond to a previously received command. |
| `agents_discover`, `agents_launch`, `agents_launch_status` | Find and start agents through launchers (session coordinator only) — see [Agent launchers](#agent-launchers). |

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
- `max_launched_agents` (default 10, 0..100) — agents launched into one session at a time

Process binding: `SW_HOST` (default `0.0.0.0`), `SW_PORT` (default `8765`).

See [`.env.example`](.env.example) for the full, annotated list of environment
variables (binding, auth tokens, the `/invite` brief upgrade URLs, persistence, and
the per-session config bounds) with their defaults — copy it to `.env` for manual setup.

## Public brokers: accounts

A public broker can require an **account token** to create sessions.

Environment (all three are required together):

- `SW_SESSION_CREATION=account` (default `open`: anyone may create sessions)
- `SW_PERSISTENCE_DB_PATH` (accounts live in the persistence DB)
- `SW_ADMIN_TOKEN` (the only way to create accounts)

Administer accounts with the `sessionweave-accounts` CLI (`--url` or `SW_URL`,
`--admin-token` or `SW_ADMIN_TOKEN`; it prints JSON, and a non-2xx answer exits 1):

```bash
sessionweave-accounts create alice --contact alice@example.com
sessionweave-accounts token-create ACC_ID --label laptop --expires-at 2027-01-01T00:00:00Z
sessionweave-accounts list
sessionweave-accounts disable ACC_ID      # suspend its sessions
sessionweave-accounts enable ACC_ID       # lift the suspension (does not continue them)
sessionweave-accounts token-revoke ACC_ID TOKEN_ID
sessionweave-accounts delete ACC_ID --yes
```

The token (`swa_...`) is shown once, at mint time. Send it as
`Authorization: Bearer <token>`:

- **REST:** on `POST /v1/sessions`, and on the session management routes (the
  owning account's token acts as the session creator).
- **MCP:** as a header in `.mcp.json`; `GET /connect.mcp.json` returns the snippet
  with `"headers": {"Authorization": "Bearer ${SW_ACCOUNT_TOKEN}"}` in account mode.
  `sessionweave-host` sends `SW_ACCOUNT_TOKEN` the same way.
- **SDK / Runner:** pass the token wherever they take a creator bearer.

An account can inspect itself with `GET /v1/me`, `GET /v1/me/sessions` and mint or
revoke its own tokens under `/v1/me/tokens` (see [`openapi.yaml`](src/sessionweave/openapi.yaml)).

**Disable vs delete.** *Disabling* an account stops its tokens at once and
suspends its sessions: participants receive a halt, and joins, commands and
orders are refused until you enable it again. Enabling lifts the suspension
only: the sessions stay halted until you (or a supervisor) post `continue`.
*Deleting*
removes the account, its tokens and all its sessions for good. Both are safe to
repeat if a session failed to update (the broker answers 503
`persistence_failed`).

### Running publicly

Checklist for a broker reachable from the internet behind a reverse proxy:

- **Account mode.** `SW_SESSION_CREATION=account` with `SW_PERSISTENCE_DB_PATH`
  and `SW_ADMIN_TOKEN` (above), so only account holders can create sessions.
- **Trusted proxy only.** Set `SW_FORWARDED_ALLOW_IPS` to the *exact* IP of your
  reverse proxy, never `*`. The per-IP limits below (and the failed-token
  throttle) key on the client address uvicorn derives from `X-Forwarded-For`;
  with `*` any client could forge it (the bundled `deploy/` compose is the exception: the port is unpublished and Caddy is the only peer). Make sure the broker port is reachable
  only from the proxy. Before relying on token network policies other than
  `any`, pin the proxy's container IP (or verify `last_used_ip` in the token
  listing after one real request through the proxy): a wrong value makes every
  client look like the proxy, or lets clients forge their address.
- **MCP idle transports.** Keep `SW_MCP_SESSION_IDLE_TIMEOUT` positive (default
  1800 s) so abandoned MCP connections are reclaimed; `SW_MCP_MAX_SESSIONS`
  caps concurrent transports (default `10000`). Idle transports per IP can reach about
  `SW_MCP_NEW_SESSIONS_PER_MINUTE` x idle timeout in minutes (30 x 30 = 900 at the
  defaults), so public brokers should set `SW_MCP_SESSION_IDLE_TIMEOUT` low,
  e.g. `300`.
- **Request size.** `SW_MAX_REQUEST_BYTES` (default `5242880`) caps any HTTP
  request body; larger ones get 413 `payload_too_large` before being read.
  Also set a body limit at the proxy (for Caddy, `request_body { max_size 5MB }`)
  so oversized uploads never reach the broker.
- **New MCP sessions.** `SW_MCP_NEW_SESSIONS_PER_MINUTE` (default `30`) limits,
  per client IP, POSTs that open a new MCP transport (no `mcp-session-id`
  header); excess gets 429 with `Retry-After`. Calls inside an existing MCP
  session are not counted.
- **Per-account caps.** `SW_ACCOUNT_MAX_SESSIONS` (default `100`): live
  sessions an account may own (in memory or persisted, not ended; sessions the
  admin creates for it count); one more is 429 `account_session_limit`.
  `SW_ACCOUNT_MAX_TOKENS` (default `20`): non-revoked, non-expired tokens per
  account; one more is 409 `account_token_limit`.
- **Metrics.** Set `SW_METRICS_TOKEN` (or keep `/metrics` unreachable from the
  internet at the proxy). `/metrics` then accepts that token or the relay admin
  token (`SW_ADMIN_TOKEN`) — the Cockpit's monitor scrapes with the admin token.
- **Container hardening.** Run the container as a non-root user with
  `read_only: true` (mount only the data volume and a tmpfs `/tmp` writable),
  `cap_drop: [ALL]`, `security_opt: ["no-new-privileges:true"]`, and memory /
  pids limits.

## Token networks and account events

Account tokens can be bound to networks, and an account can follow what happens
to it through a cursor-based event channel (relay 0.33.0+).

### Network policies

Every account token has a **network policy** and a list of **approved networks**
(CIDRs; an exact IP is stored as `/32` or `/128`):

| policy | behaviour |
|---|---|
| `any` (default) | no restriction |
| `first_use` | the network of the first successful use is approved automatically; afterwards only approved networks work |
| `approved_only` | only networks already approved (given at creation or added later) work |

Set them when minting (`network_policy`, `approved_networks` in the body of
`POST /v1/me/tokens` and `POST /v1/accounts/{id}/tokens`), or change them later with
`GET`/`PUT /v1/me/tokens/{token_id}/networks` (admin: `/v1/accounts/{id}/tokens/{token_id}/networks`).
`approved_networks` in a `PUT` replaces the list. Token listings also show
`last_used_at` and `last_used_ip`. Removing a network or tightening the policy closes
firehose streams opened with that token from addresses that are no longer allowed.

```bash
sessionweave-accounts token-create ACC_ID --label ci --network-policy approved_only \
  --network 203.0.113.0/24 --network 2001:db8::1
sessionweave-accounts token-networks ACC_ID TOKEN_ID                    # show
sessionweave-accounts token-networks ACC_ID TOKEN_ID --policy first_use --network 10.1.0.0/16
```

The check applies to **every** use of an account token: session creation, the
`/v1/me` routes, and the token acting as creator of its own sessions (REST, MCP
creator tools, the firehose WebSocket). Admin, creator, supervisor, join and
participant tokens are not affected: joining and participating never change.

### Approval flow

A request from a network that is not approved is refused with **403
`network_not_approved`** ("this token is not approved for your network; the account
owner has been asked to approve it") and the broker records a **pending approval**
(`approval_id`, `token_id`, `ip`, `client`, expires after 24 h). Repeated attempts from
the same address reuse it; a token keeps at most 10 pending approvals. The account
owner, with another valid token, or the admin then decides:

```bash
curl -H "Authorization: Bearer $SW_ACCOUNT_TOKEN" $URL/v1/me/network-approvals
curl -X POST -H "Authorization: Bearer $SW_ACCOUNT_TOKEN" -d '{"scope":"network"}' \
  $URL/v1/me/network-approvals/APPROVAL_ID/approve     # "ip" = /32 or /128; "network" = /24 or /64
curl -X POST -H "Authorization: Bearer $SW_ACCOUNT_TOKEN" \
  $URL/v1/me/network-approvals/APPROVAL_ID/deny
# admin: /v1/accounts/{id}/network-approvals[...]; CLI: approvals / approve / deny
sessionweave-accounts approve ACC_ID APPROVAL_ID --scope ip
```

Removing every network from a `first_use` token (a `PUT` with an empty list) re-arms
first-use binding: the next successful use binds its network again. A first-use bind
emits `token.network_approved` with `by: "first_use"`.

Approving adds the CIDR to the token's approved networks; denying drops the request
(the address may ask again). One account can never see or decide another account's
approvals. Keep a token with policy `any` for the client that makes approvals (the
Cockpit's own tokens are like that), so the approval path always works.

### Which address is used

The **client address** is the proxy-resolved IP the broker already uses for rate
limiting: the address uvicorn derives from `X-Forwarded-For`. Set
`SW_FORWARDED_ALLOW_IPS` to the exact IP of your reverse proxy (see *Running
publicly*); otherwise either every request looks like it comes from the proxy, or
(with `*`) a client can forge its address and bypass the binding. A broad network such
as `0.0.0.0/0` or `::/0` approves every address and so effectively means *any*.

### Account events

The broker keeps an event log per account (retention `SW_ACCOUNT_EVENTS_RETENTION_DAYS`,
default 30). Event data never contains token values or join links.

| kind | data |
|---|---|
| `session.created` | session_id, title, persistence, token (id + label) or `admin`, ip, client |
| `session.deleted` | session_id, by (`owner`, `admin`, `account_deleted`) |
| `token.created`, `token.revoked` | token (id + label), by (`self`, `admin`), ip |
| `token.network_pending` | approval_id, token_id, label, ip, client |
| `token.network_approved` | token_id, label, ip, cidr |
| `token.network_denied` | token_id, label, ip |
| `token.rejected` | a known token was refused (revoked, expired, account not active): token_id, label, reason, ip, client; at most one per token and reason per 10 min |
| `account.status_changed` | from, to, by |
| `agent.launched`, `agent.stopped` | session_id, launch_id, profile_id, launcher_id, machine; `agent.stopped` adds reason (`removed`, `session_ended`, a launcher limit, or the failure: `launcher_offline`, `join_timeout`, the launcher's error). Recorded on the launcher owner's and the session owner's account |

Read them with `GET /v1/me/events` (own account) or, as admin,
`GET /v1/accounts/events?account_id=...` (all accounts, or one). Parameters:
`after` (event id cursor), `limit` (<= 500, default 100), `wait` (<= 60 s long-poll).
The answer is `{"events": [...], "next": N, "truncated": false}`: store `next` and pass
it as `after` next time. `truncated: true` means `after` is older than the oldest kept
event. With `wait`, the call returns as soon as a newer event exists, or empty after
the wait.

```bash
after=0
while true; do
  page=$(curl -s -H "Authorization: Bearer $SW_ACCOUNT_TOKEN" \
    "$URL/v1/me/events?after=$after&wait=30")
  echo "$page" | jq -c '.events[]'
  after=$(echo "$page" | jq .next)
done
sessionweave-accounts events --account ACC_ID --after 120 --wait 30   # admin CLI
```

A revoked or expired account token (or one of a non-active account) that is presented
on *another* account's session still records `token.rejected` on the token's own
account; it never creates a pending approval or a network event, on either account.

Actions a user performs in the Cockpit reach the broker from the Cockpit's server, so
the event's `ip` is the Cockpit's and the token label is `cockpit`.

## Agent launchers

A **launcher** is a small always-on service on a machine (the Runner's `launcher`
mode) that declares which agent **profiles** it can start: domain, role,
capabilities, description, runtime, model, access preset, limits and
`max_instances`. Workdir and start prompt stay on the machine. The session's
**coordinator** finds profiles and launches them; the agent joins the session by
itself, with no invite link handed around.

**Who may ask.** Only the session's coordinator — the participant that joined with
the creator credential (creator token or the owning account's token), or the one
the creator assigned with `PUT /v1/sessions/{sid}/coordinator` — or the creator /
admin acting for the session. Anyone else gets `permission_denied` ("only the
session's coordinator can request agents — ask the coordinator").
The creator credential (creator token, owning account's token) and the admin token
act for the coordinator **by design**: they may discover, launch, read launch status
and remove launched agents themselves. On MCP this also holds for every participant
of a connection whose `Authorization` header carries one of those credentials, so do
not share such a connection with agents that should not launch.

**Connecting a launcher.** WebSocket `/v1/launchers/connect` with
`Authorization: Bearer <token>`: in `account` mode an account token (the launcher
belongs to that account; bind it to the machine's network with `approved_only`),
in `open` mode `SW_LAUNCHER_TOKEN` when set. Unset in open mode, any launcher that
can reach the broker may register — fine on a private network; the broker logs a
warning at startup. Limits: 5 launchers per account (50 in open mode), 256 KiB per
frame; failed authentications are throttled per IP (close 4429). A launcher is
dropped (close 4403) when its token is revoked, its account is disabled or
deleted, or the token's networks no longer approve its address, and (close 4401)
when the account token it connected with expires. `welcome` carries `account`
(the owner account's name; `null` in open mode).

**Visibility** (account mode) is per launcher (`visibility`: `personal` or
`general`, `allowed_accounts`), and each profile in `hello` may override it with
its own `visibility` / `allowed_accounts`. The session owner's own launchers are
always visible to it. `allowed_accounts` empty on a profile means every account,
while omitting it inherits the launcher's list. A session without an owner account
(created with the admin token and no `owner_account_id`) sees only `general`
profiles whose effective `allowed_accounts` is empty (and open-mode launchers);
never `personal` ones nor lists naming accounts. Such a session has no own
launcher, so a `domain` + `role` launch finds none: name `launcher_id` +
`profile_id`. Launching from such a session needs Runner >= 0.8.2 launchers: a 0.8.1
launcher refuses the order with `visibility_refused`.

**Discover and launch.**

```bash
H="Authorization: Bearer $COORDINATOR_PARTICIPANT_TOKEN"
curl -s -H "$H" "$URL/v1/agents/domains?session_id=$SID"
curl -s -H "$H" "$URL/v1/agents?session_id=$SID&domain=backend&role=analyst"
# by launcher + profile, or by domain + role (first online launcher with a free slot)
curl -s -H "$H" -X POST "$URL/v1/sessions/$SID/agents" \
  -d '{"domain": "backend", "role": "analyst", "task": "map the payment flow"}'
# -> 202 {"launch_id": "lch_...", "status": "pending", "launcher_id": "lnc_...", "machine": "box-1",
#         "owner": "alice", "own": true}
curl -s -H "$H" "$URL/v1/sessions/$SID/agents/lch_..."
```

By domain + role the broker picks only among launchers of the session owner's
account (or open-mode launchers); another account's `general` launcher is used only
when named explicitly with `launcher_id` + `profile_id`. The response (and the
status) says which launcher and machine got the task. Every discovery item, the
launch response and the status carry `owner` (the launcher owner's account name,
`null` in open mode) and `own` (`true` when the launcher belongs to the session
owner's account, or in open mode); discovery lists own launchers first. The
machine name, profile id and description are chosen by the launcher's owner:
prefer `own: true`, and launch a foreign launcher only when the user asked for it
— it receives the task and its agent reads the whole session.

MCP: `agents_discover`, `agents_launch`, `agents_launch_status`. Errors:
`launch_limit` (409, the session's `max_launched_agents`, default 10, counting
pending, started and joined launches), `no_free_slot` (409), `profile_not_found`
(404), `launch_not_found` (404).

**What happens.** The broker reserves a slot, mints a **single-use join
credential** bound to the session and to the profile's role and capabilities, and
sends the launcher `{"type":"launch","launch_id","session_id","profile_id","task","join_credential","coordinator_id","session_owner","broker_url"?}`
— the only place the credential ever appears. `broker_url` is present only when the
operator set `SW_PUBLIC_BASE_URL`; it is never derived from request headers (a
spoofed `Host` must not redirect the agent and its credential). Without it the
launcher joins through the URL it connected to. `coordinator_id` is the
participant the agent reports its result to (the coordinator that launched it,
else the session's coordinator; `null` if there is none); `session_owner` is the
session owner's account name (`null` in open mode), so the launcher can
double-check its visibility rules (it refuses with `visibility_refused`). The
launcher answers
`launch_result` (`started` | `failed` with `error` and, optionally, `message`: at
most 300 printable characters, kept in the launch status and sent in
`agent.launch_failed`). Before it joins, the Runner launcher checks the profile's
runtime (binary on `PATH`, opencode 2.x, Cline 3.0.75, workdir and git checks) and
answers `failed` with `runtime_unavailable` and a `message` naming the problem, so the
credential is not spent on an agent that could never run. The agent joins with the
credential as Bearer on `POST /v1/sessions/{sid}/participants`. The join takes
the profile's role and capabilities whatever the body says (they are forced, not
compared), and the participant's metadata carries `launch_id`, `launcher_id`,
`launcher_machine`, `profile_id` and `launched_by` (the coordinator's participant
id, or null when the creator launched); the agent cannot change any of these
later. **These five metadata keys are reserved since 0.34.0:** the broker drops
them from every join and `update_self` body, also for participants that were not
launched, so a client that used one of these names for its own data must rename it. `launcher_id` / `launcher_machine` there mean "originally launched by": after
a launcher reconnect the launch status shows the current launcher. A second join, or a join to another session, is refused. Status:
`pending` → `started` → `joined`, or `failed` (`launcher_offline` when the
launcher disconnects first, `join_timeout` after 120 s — `SW_LAUNCH_JOIN_TIMEOUT_S`
— or the launcher's error; the credential is invalidated and the slot released).
The coordinator receives the session event
`agent.launch_failed {launch_id, error, message?}`.

**Lifecycle.** A launched agent stays until removed or the session ends. The
coordinator may remove launched agents (`DELETE
/v1/sessions/{sid}/participants/{pid}`, halt first) — the ones it launched, the
ones the creator or admin launched (`launched_by` null) and the ones a previous
coordinator launched — never other participants; the creator and admin remove
anyone. Removing a launched agent does
not invalidate the session's join tokens (it never held the invite link); its halt
reads "removed by the coordinator" when the coordinator removed it. The launcher
then gets `{"type":"stop","launch_id","reason":"removed"}` — `left` when the agent
left by itself, `evicted` when it stayed offline past the grace period — and
`session_ended` when the session ends or is deleted (also while it is cold),
`session_cold` when a persisted session leaves memory at its idle TTL (it can be
resumed; launch the agents again then: nobody would supervise them meanwhile), and
`join_timeout` for a launch that timed out. **Idle TTL:** an in-memory session ends
after `idle_ttl_seconds` without activity (default 1800 s), which stops every
launched agent with `session_ended`; raise `idle_ttl_seconds` (session `config`) for
long-running work. A persisted session goes cold at the same idle TTL and its agents
are stopped just the same (reason `session_cold`); it only lets the session itself be
resumed afterwards. When the launcher stops an agent
itself (a profile limit, or `runtime_failed` when the runtime keeps failing or no
longer passes its checks), it sends `{"type":"stopped","launch_id","reason"}`; the
broker removes the participant (halt "stopped by its launcher: <reason>") and sends
the session event `agent.stopped {participant_id, launch_id, reason}`.

**Launcher reconnects.** `welcome` carries a `resume_secret` for that connection
(the broker keeps only its hash; keep it with the running launch ids). A launcher
that reconnects sends `"resume_secret"` and `"running": ["lch_...", ...]` in its
hello. Each joined launch whose secret matches (and, in account mode, whose owner
account is the same) is re-attached: stops reach the new connection, the slot
counts again, and from then on the new connection's secret is the one that
counts. If the old connection is still registered (a half-open socket), it is
closed (4409) and replaced, and the agents keep running. Any other id gets an
immediate `stop` (its final reason, or `unknown_launch`).
Joined agents whose launcher has not come back within 300 s
(`SW_LAUNCHER_RECONNECT_GRACE_S`) are removed (halt first) with the session event
`agent.stopped` reason `launcher_lost`. In account mode
`agent.launched` / `agent.stopped` are recorded in the account event log
(`agent.stopped` carries `status`: `failed` for a launch that never joined, with its
`message` when there is one, else `stopped`).
Launch records are kept in memory (a finished one for an hour). After a broker
restart launches are forgotten: a reconnecting launcher is told to stop its
agents (`unknown_launch`), and it makes each one leave the session with the
agent's own participant token, so (persisted sessions) the roster holds no stale
entry. An agent whose launcher never comes back stays in the roster, offline,
until `offline_grace_seconds` evicts it.

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

The HTTP surface (Starlette) and tool wiring (MCPServer) live in `server.py` and
`tools.py`. Prometheus instrumentation lives in `metrics.py`.

State is in-memory only; restart loses all sessions. Persistence is
sub-project #2.

## Threat model (sub-project #1)

Sessions are **transient and in-memory** unless persistence is requested, and
access is controlled by scoped tokens. ``GET /v1/info`` advertises the model via
``"auth_model": "scoped-tokens-v1"``.

What each credential is:

- **Join token** — 256-bit, valid 15 minutes, multi-use within that window,
  carried by the invite link (``?jt=``). It lets its holder read the session and
  join it; expiry never affects participants already joined.
- **Participant token** — a secret returned once by the join, distinct from the
  ``participant_id`` (an identifier that authenticates nothing). It is the only
  credential for participant routes, the events WebSocket and
  ``session_resume``.
- **Creator token** — returned once at create: delete the session, change its
  purpose, mint join and supervisor tokens, join, read, supervise.
- **Supervisor token** — observe (firehose) and post supervisor notes; revocable
  individually by the creator.
- **Admin token** — the relay-wide ``SW_ADMIN_TOKEN``. Unset means no admin access.
- The session id is **never** a credential on any path.

Only SHA-256 digests of tokens are stored (RAM and SQLite); comparisons are
constant-time. Failed-token attempts are throttled per (session id, source IP);
a valid token is never throttled.

Remaining notes for operators:

- ``ConnectionManager.bind`` is intentionally **last-resume-wins**: a
  second client presenting the same ``participant_token`` displaces the
  first without notification. A leaked participant token is therefore a
  session-hijack primitive for that participant until the session ends.

Concrete leak channels operators must defend against:

| Channel | Mitigation |
|---|---|
| HTTP access logs that record bearer headers | Strip ``Authorization`` at the reverse proxy (Caddy) or the relay's log formatter. |
| WebSocket URLs / invite URLs that carry ``?token=`` or ``?jt=`` | The relay redacts these values in its own access logs; verify any front-of-relay proxy, and don't share screenshots of DevTools. |
| TLS-terminating proxies that log query strings (CDN, WAF) | Caddy doesn't by default; verify any front-of-relay proxy. |
| Prometheus labels in a previous build had per-participant cardinality | Locked down: see #58 (structural test guard). |

Not in scope today: per-action scopes for participants (a participant may do
everything a participant may do) and concurrent-bind protection beyond
``last-resume-wins``. User accounts are sub-project #3.

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
pip install -e '../SDK/python'
```

```python
import asyncio
from sessionweave_client import SessionWeaveClient

async def main():
    async with SessionWeaveClient() as c:           # default: broker.sessionweave.com
        sess = await c.create_session(purpose={"kind": "demo"})
        me = await c.join(sess["session_id"], join_token=sess["join_token"],
                          role="worker", capabilities=["respond"])
        async with me.events() as stream:           # live WS push
            async for evt in stream:
                if evt.name == "command.received":
                    await me.respond(evt.payload["command_id"], payload={"a": "..."})
                    break
        await me.leave(feedback={"summary": "ok", "rating": 5})

asyncio.run(main())
```

Vendor-neutral examples (Anthropic, OpenAI, plain) live under
[`SDK/python/examples/`](../SDK/python/examples/).
For other languages, point any HTTP+WS client at the
[REST + WebSocket surface](#rest--websocket-surface) above.

### Path B — MCP integration (Claude Code, opencode, etc.)

If your runtime already speaks MCP, register the relay in `.mcp.json`:

```json
{
  "mcpServers": {
    "sessionweave": {
      "type": "http",
      "url": "https://broker.sessionweave.com/mcp"
    }
  }
}
```

Restart the client so the catalog refreshes. The tools appear as
`mcp__sessionweave__session_create`, `..._join`, etc. For headless
`claude -p` invocations pass `--mcp-config <file>` and an
`--allowedTools mcp__sessionweave__...` allowlist.

### Path B (cont.) — Install the participation skill

The skill lives in [`skills/sessionweave-participant/`](../skills/sessionweave-participant/).
Symlink (or copy) it into `~/.claude/skills/` so the agent learns *how*
to use the relay (modes, dispatch, invite links, recovery):

```bash
git clone https://github.com/ReInventYourSolutions/sessionweave-broker
mkdir -p ~/.claude/skills
ln -s "$PWD/SessionWeaveMCP/skills/sessionweave-participant" \
      ~/.claude/skills/sessionweave-participant
```

Or fetch the file directly:

```bash
mkdir -p ~/.claude/skills/sessionweave-participant
curl -fsSL \
  https://raw.githubusercontent.com/ReInventYourSolutions/sessionweave-broker/main/skills/sessionweave-participant/SKILL.md \
  -o ~/.claude/skills/sessionweave-participant/SKILL.md
```

A symlink against a clone keeps the skill in sync with repo updates.

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

- Receive an invite link (`https://<host>/invite/<sid>`, or the compact
  `swvs://` / `swv://` alias), parse it, `session_join` with the prescribed
  role, and act per `purpose.kind` or `purpose.instructions`.
- Create a session for someone else, set `purpose`, hand the user the
  generated link, and play the host side.

### agent-runner moved

The turn-based **agent-runner** side-car is the standalone **`sessionweave-runner`**
package in [`Runner/`](../Runner/). Install it where the agent
runs — it is a relay *client*, not part of the relay. The `sessionweave-agent-runner` command
lives there.

## Roadmap

Each sub-project gets its own design spec under `docs/superpowers/specs/` and
its own implementation plan.

1. **MCP core + session relay** — anonymous, transient sessions. *Implemented
   and deployed at* `broker.sessionweave.com`. Design:
   [`docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md`][spec].
   The original spec covered MCP only; subsequent additions (REST surface,
   WebSocket events, exit feedback, heap-driven eviction, disconnect
   recovery) extended it without breaking backwards compatibility.
2. **Session persistence** — DB-backed sessions, participants, messages.
3. **User accounts + auth** — bind sessions to a user (replace the
   scoped session tokens with user identity); gate premium services.
4. **Web dashboard** — read-only over the user's sessions, then management.
5. **Persistent shared context** — first registered-only service.
6. **Additional services** — storage, analysis tools, etc., added on demand.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).

[spec]: docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md
