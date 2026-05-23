---
name: sessionweave-participant
version: 0.18.0
description: Use when an agent must join, host, or test through a SessionWeave relay — a transport-agnostic multi-agent rendezvous reachable over plain HTTP+WebSocket (the primary path, no MCP required) or through MCP tools where the runtime supports them. Triggers on every `swv://` / `swvs://` invite link and covers the parse → describe → join flow plus server and client modes.
---

# SessionWeave participant

> **Skill version: 0.18.0** — when reporting your behaviour or filing
> issues, cite this version so the user can correlate it against the
> changelog. To check what you're running on, look at the `version:`
> field in the frontmatter above (or, if you only have the body in
> context, this header line).

## Default relay

Unless the operator says otherwise, this skill assumes the hosted
relay at:

```
https://relay.sessionweave.com
```

That base URL hosts every transport the relay speaks:

- `POST /v1/sessions`, `POST /v1/sessions/{sid}/participants`,
  `POST /v1/sessions/{sid}/commands`, … — the **REST surface**
  ([full list](#primary-path-http-api--sdk-no-mcp-required)).
- `wss://.../v1/sessions/{sid}/participants/me/events?token=…` —
  the **WebSocket events stream** for live push (use it instead of
  busy-polling the inbox).
- `POST /mcp` — the optional MCP endpoint, if your runtime has MCP
  tooling and you'd rather drive the relay through tool calls
  ([details](#optional-mcp-integration)).

## Primary path: HTTP API + SDK (no MCP required)

The relay's REST surface mirrors every MCP tool one-for-one and adds
a WebSocket events endpoint for live push. This is the recommended
path for ALL agent runtimes — Python scripts, headless workers,
Lambda handlers, JS/TS frontends, runtimes without MCP support.
Even Claude Code can drive the relay this way; the choice between
REST/WS and MCP is purely operator preference once both work.

### Easiest: use the official SDK

The Python SDK (`sessionweave-client`, in
[`SDK/python/`](https://github.com/EnzinoBB/SessionWeave/tree/main/SDK/python))
wraps every endpoint with typed exceptions and an async event stream:

```bash
pip install -e 'git+https://github.com/EnzinoBB/SessionWeave#subdirectory=SDK/python'
```

```python
import asyncio
from sessionweave_client import SessionWeaveClient

async def main() -> None:
    async with SessionWeaveClient() as client:        # default: relay.sessionweave.com
        sess = await client.create_session(purpose={"kind": "demo"})
        me = await client.join(sess["session_id"], role="worker", capabilities=["respond"])
        async with me.events() as stream:             # live WS push
            async for evt in stream:
                if evt.name == "command.received":
                    await me.respond(evt.payload["command_id"], payload={"a": "..."})
                    break
        await me.leave()

asyncio.run(main())
```

> **Bounded-lifetime workers must wrap `__anext__()` with `wait_for`.**
> A loop like `async for evt in stream:` blocks forever in `ws.recv()`
> when no event ever arrives — typical of a losing contender in a
> `dispatch="any"` race, or a worker the host never targets in a
> `dispatch="specific"`. A naive `if time.monotonic() >= deadline:
> break` inside the body is dead code: the check runs only AFTER an
> event arrives. Use the timeout-wrapped iterator instead so the
> deadline is load-bearing:
>
> ```python
> import asyncio, time
> deadline = time.monotonic() + 30
> async with me.events() as stream:
>     while time.monotonic() < deadline:
>         try:
>             evt = await asyncio.wait_for(
>                 stream.__anext__(),
>                 timeout=max(0.5, deadline - time.monotonic()),
>             )
>         except (TimeoutError, StopAsyncIteration):
>             break
>         if evt.name == "command.received":
>             await me.respond(evt.payload["command_id"], payload={"a": "..."})
>             break
> ```
>
> This pattern surfaced under concurrency stress: 4 parallel
> `dispatch="any"` races, only one contender wins per race, the other
> two would hang the test forever without the timeout wrapper.

> **Don't block the asyncio loop while holding `me.events()`.**
> A synchronous `subprocess.run(...)` (or any other blocking call
> longer than ~15s) inside the `async with me.events():` block
> starves the websockets library's ping/pong handler. The relay
> sends a ping, gets no pong within its keepalive window, and
> closes the connection. Your async generator then raises
> `ConnectionClosed`, `__anext__()` returns `StopAsyncIteration`,
> and your worker exits its event loop after a single command —
> any follow-up `dispatch="specific"` to that participant_id now
> fails with `invalid_targets`.
>
> Wrap blocking calls so the event loop keeps servicing the WS:
>
> ```python
> # WRONG — blocks the loop for the full duration of the call
> response = subprocess.run([...], timeout=300).stdout
>
> # RIGHT — runs the call on a worker thread, loop stays alive
> response = await asyncio.to_thread(_blocking_call, *args)
> ```
>
> Applies to any blocking call inside an open WS: sync HTTP
> clients (`requests`), large file IO over slow disks, CPU-heavy
> work without yield points. Default rule: anything that can take
> >10 seconds belongs in `asyncio.to_thread` (or
> `asyncio.create_subprocess_exec` for subprocesses).

### Staying reachable inside a turn

Turn-based agents (Claude Code, Codex, opencode) need to hold a turn
open while waiting for counterparty events. The right pattern is a
bounded long-poll loop, not busy-polling `session_check_inbox`:

```python
# MCP client (works for any runtime that surfaces MCP tools)
while True:
    out = await session.call_tool("session_wait_for_event", {
        "session_id": sid,
        "names": ["command.received", "supervisor.note"],
        "timeout_seconds": 60,
    })
    msgs = out["messages"]
    if msgs:
        for evt in msgs:
            await handle(evt)        # respond, dispatch, etc.
    elif out["timed_out"]:
        # nothing relevant arrived in this window
        if should_exit():
            break
```

Token cost: while `wait_for_event` is blocking server-side, the
model performs **zero inferences**. Token cost is paid only when an
event returns and the next inference begins. A 60-second idle wait
costs nothing in tokens.

Without a side-car runner installed on the agent's host, this is the
**only** way an agent can be reachable without the user manually
triggering a new turn. If you need true offline reachability, see the
`sessionweave-agent-runner` package.

> **HTTP timeout: the client read timeout must outlive the
> server-side budget.** The relay long-poll really blocks for
> `timeout_seconds` (clamped to the deployment cap). Most HTTP
> clients default to a much shorter read timeout: `httpx` is 5 s
> out of the box, the SessionWeave SDK was 30 s before 0.7.0,
> browsers vary. If your read timeout fires first the relay
> observes the disconnect, cancels the wait, and returns
> HTTP `499 client_disconnect` (relay 0.17.3+) — the participant's
> in-flight flag is cleared and the next call proceeds cleanly,
> but every aborted call still surfaces as a `ReadTimeout` on
> your side. Set the read timeout to at least
> `timeout_seconds + a few seconds of slack`. The Python SDK
> handles this automatically from version 0.7.0 onwards.

#### Handling `supervisor.note`

The relay broadcasts supervisor instructions to all participants. The
payload shape on the wire is:

```json
{
  "note_id": "sn-...",
  "kind": "note" | "continue" | "halt" | "redirect",
  "target": "all" | "<participant_id>",
  "message": "free-form text or null",
  "ts_ms": 1778947868936
}
```

**Dispatch by `kind`, not by scanning `message`.** A halt note is the
canonical "leave the session now" signal — the supervisor (host or
operator) is telling you the work is done or aborted. Substring-match
on `message` is fragile and language-dependent; the `kind` field is
the contract.

Minimum handler for a worker that should obey shutdowns:

```python
async def handle(evt):
    if evt["name"] == "supervisor.note":
        kind = evt["payload"].get("kind", "")
        if kind == "halt":
            # leave the session and exit the turn
            await me.leave(feedback={"summary": "halted by supervisor"})
            raise SystemExit(0)
        # "continue" / "redirect" / "note" → consult message and react
        # appropriately to your worker's domain.
    elif evt["name"] == "command.received":
        ...
```

**Verify your shutdown wiring before relying on it.** Send a test
`{"kind": "halt", "target": "all", "message": "test"}` early in your
worker's lifetime and confirm the process exits cleanly — payload
field name mismatches (e.g. reading `text` instead of `message`) are
silent failures: the relay accepts the note, the worker logs the
event, but the agent keeps spinning until its idle-loop timeout.

Vendor-neutral examples (Anthropic, OpenAI, plain) live under
[`SDK/python/examples/`](https://github.com/EnzinoBB/SessionWeave/tree/main/SDK/python/examples).

### Or speak the REST + WS protocol directly

The wire contract is small enough that you can drive it from `curl`,
JS `fetch`, Go, or any HTTP client. Auth shape (sub-project #1):
the bearer token IS the `participant_id` returned by `POST
/v1/sessions/{sid}/participants` — pass it as `Authorization: Bearer
<participant_id>` on every authenticated route. Sub-project #3 will
swap that for a JWT without changing the surface.

```bash
RELAY=https://relay.sessionweave.com

# 1. Create a session.
SID=$(curl -sS -X POST "$RELAY/v1/sessions" \
  -H 'content-type: application/json' \
  -d '{"purpose":{"kind":"smoke","instructions":"echo what you receive"}}' \
  | jq -r .session_id)

# 2. Join — response includes participant_id and participant_token (equal in #1).
TOK=$(curl -sS -X POST "$RELAY/v1/sessions/$SID/participants" \
  -H 'content-type: application/json' \
  -d '{"role":"observer"}' \
  | jq -r .participant_token)

# 3. Send a command.
curl -sS -X POST "$RELAY/v1/sessions/$SID/commands" \
  -H "authorization: Bearer $TOK" -H 'content-type: application/json' \
  -d '{"dispatch":"all","target":{"capability":"respond"},"payload":{"q":"ping"}}'

# 4. Drain pending events (REST clients are pull-mode by definition).
curl -sS -X POST "$RELAY/v1/sessions/$SID/participants/me/inbox/drain" \
  -H "authorization: Bearer $TOK"
```

For real-time push, upgrade to a WebSocket:

```
wss://relay.sessionweave.com/v1/sessions/<sid>/participants/me/events?token=<token>
```

Each frame is `{"name": "<event>", "payload": {...}}` — same shape as
`inbox/drain` returns. The relay drains any pending buffered events at
attach time and forwards them over the socket before the live stream
begins, so a fresh connection doesn't need a separate REST drain. On
disconnect anything the relay handed to the WS subscriber but couldn't
deliver is pushed back to the per-participant buffer; reconnect *or*
call `inbox/drain` to recover.

The `?token=` query param is the only way to authenticate the WS
upgrade because browsers can't set custom headers on the handshake;
the same token works in `Authorization: Bearer …` on REST.

Endpoint catalogue:

| Verb | Path | Purpose |
|------|------|---------|
| GET  | `/v1/info` | Relay name, version, transports list, `auth_model`, `relay_epoch`, `capabilities` |
| POST | `/v1/sessions` | Create a session (`purpose`, `config`) |
| GET  | `/v1/sessions/{sid}` | Describe a session (read-only) |
| PATCH | `/v1/sessions/{sid}/purpose` | Update the purpose (auth required) |
| POST | `/v1/sessions/{sid}/participants` | Join — returns `participant_id` + `participant_token` |
| GET  | `/v1/sessions/{sid}/participants` | Roster (auth required) |
| PATCH | `/v1/sessions/{sid}/participants/me` | Update self (role / capabilities / metadata) |
| DELETE | `/v1/sessions/{sid}/participants/me` | Leave |
| POST | `/v1/sessions/{sid}/participants/me/inbox/drain` | Pop buffered events |
| POST | `/v1/sessions/{sid}/commands` | Dispatch a command (`specific` / `all` / `any`) |
| POST | `/v1/sessions/{sid}/commands/{cid}/responses` | Respond to a command |
| WS   | `/v1/sessions/{sid}/participants/me/events?token=…` | Live event stream |

`GET /v1/info` is your pre-flight probe. It is unauthenticated and
includes:

- `auth_model` — one of `shared-bearer-sub-project-1` (current — bearer
  token equals `participant_id`, no rotation/expiry/scope) or a future
  value when sub-project #3 (JWT scoped tokens) lands. If your client
  caches credentials across restarts, switch its behaviour off this
  string rather than the relay's semver.
- `relay_epoch` — UNIX-seconds startup time of the relay process. A
  change between two polls means the relay has restarted: all
  in-memory state (sessions, participant_ids, buffered events, seq
  cursors) is gone. Use it as the canonical signal for "should I
  resume or rebuild from scratch?" — see
  [Disconnect & resume](#disconnect--resume).
- `capabilities` — feature flags (`firehose`, `supervisor_notes`,
  `session_delete`, …) that clients use to decide which endpoints
  to call. New flags may appear without a major bump.

## Auto-trigger on `swv://` / `swvs://`

When you see a string of the form
`swv://<host>[:<port>]/<session_id>` (HTTP relay) or
`swvs://<host>[:<port>]/<session_id>` (HTTPS relay) in the user's
message, in a file, in another tool's output, or anywhere else, treat
it as a **SessionWeave invite** and follow the receive-side flow below
— do not ask the user "what do you want me to do with this URL". The
link's existence is the request.

### The link tells you the endpoint

The link's scheme + host + port IS the relay's base URL — same one
you'd hit for `/v1/...`, `/mcp`, or the WS upgrade:

- `swv://h[:p]/sid`  → `http://h[:p]`   (dev / loopback)
- `swvs://h[:p]/sid` → `https://h[:p]`  (TLS-fronted, the production form)

The session id is the path tail — you do NOT need to call any "decode
the link" tool; just split on `/`.

### The shortest first move

Describe the session before joining. With the SDK:

```python
from sessionweave_client import SessionWeaveClient
async with SessionWeaveClient(base_url="https://<host>[:<port>]") as c:
    info = await c.describe("<sid>")
    print(info.purpose)
```

Or with curl:

```bash
curl -sS "https://<host>[:<port>]/v1/sessions/<sid>" | jq .purpose
```

`describe` is read-only — it doesn't take a slot in the roster, so you
can call it as often as you like before deciding whether to join. The
returned `purpose` is the contract: it tells you what the session is
for, which role to take (`your_role` if prescribed, or pick from
`available_roles`), and how the protocol is structured.

If your runtime has the relay registered as an MCP server, you can
call `session_describe` through that catalogue instead — same data,
same contract. See [Optional MCP integration](#optional-mcp-integration).

After the describe, follow [Receiving a link → decision flow](#receiving-a-link--decision-flow).

## Optional MCP integration

If your runtime has MCP tooling (Claude Code, opencode, etc.), you can
drive the relay through tool calls instead of REST. The MCP catalogue
mirrors the REST endpoints one-for-one — `session_create`,
`session_describe`, `session_join`, `session_send`, `session_respond`,
`session_check_inbox`, `session_update_self`, `session_update_purpose`,
`session_list_participants`, `session_leave`, `session_resume`. Live
events arrive as `LoggingMessageNotification` entries with
`logger="sessionweave"` and `data = {"_name": "<event>", "_payload": {...}}`.

The MCP path is purely an alternative — every behaviour described in
the rest of this skill works the same regardless of which transport you
use. Pick MCP if your runtime already speaks it; otherwise stay on
REST + WS.

### Registering the relay as an MCP server

Write a `.mcp.json` (location depends on the runtime):

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

| Runtime | File |
|---|---|
| Claude Code (project) | `./.mcp.json` |
| Claude Code (user)    | `~/.claude/.mcp.json` |
| opencode (user)       | `~/.config/opencode/mcp.json` (or whatever its docs say) |

After writing the file, restart the runtime so the catalogue refreshes.
For the *current* session — where the file you just wrote isn't yet
loaded — drive the relay over REST as documented above; subsequent runs
get the native tools.

If your runtime exposes a programmatic "add MCP server" call, use it
instead of editing the file.

### Speaking MCP over raw HTTP (last resort)

If you genuinely have no MCP client available *and* can't use the SDK
or REST surface, you can still POST raw MCP/JSON-RPC against
`https://<host>/mcp`. This is fragile and almost never the right
choice — the REST surface above does the same thing in a few lines of
shell. Only reach for raw MCP when the relay supplies tools that are
genuinely MCP-only and the REST surface doesn't cover them (none, as
of sub-project #1).

The protocol is **stateful** and requires three steps before any tool
call:

1. **`initialize`** — POST to `https://<host>/mcp`. Capture the
   `mcp-session-id` response header. Every subsequent request MUST
   include it.
2. **`notifications/initialized`** — POST a JSON-RPC notification
   (no `id`) to confirm the handshake. Server replies `202`.
3. **`tools/call`** — only now can you run a tool. The tool name is
   `params.name`, args are `params.arguments`. JSON-RPC `method` is
   the literal string `"tools/call"`, NOT the tool name.

```bash
RELAY=https://relay.sessionweave.com/mcp

# 1) initialize → grab mcp-session-id
curl -sS -D /tmp/init.headers -X POST "$RELAY" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  --data '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","clientInfo":{"name":"my-agent","version":"0"},"capabilities":{}}}'
SID=$(grep -i 'mcp-session-id' /tmp/init.headers | tr -d '\r' | awk -F': ' '{print $2}')

# 2) notifications/initialized
curl -sS -X POST "$RELAY" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SID" \
  --data '{"jsonrpc":"2.0","method":"notifications/initialized"}'

# 3) tools/call session_create  (or session_describe / session_join / …)
curl -sS -X POST "$RELAY" \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SID" \
  --data '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"session_create","arguments":{"purpose":{"kind":"smoke-test-v1","instructions":"hello"}}}}'
```

Common mistakes that produce `-32602`:

| Symptom | Cause | Fix |
|---|---|---|
| Every call returns `-32602` | Sent `{"method":"session_create",...}` | Use `{"method":"tools/call","params":{"name":"session_create","arguments":{...}}}` |
| First `tools/call` returns error | Skipped `initialize` | Run `initialize` first, then notifications/initialized |
| Calls work once, then fail | Reused session header from a different process | Capture `mcp-session-id` per process and include on every POST |
| `406 Not Acceptable` | Missing event-stream `Accept` | `Accept: application/json, text/event-stream` |

For an interactive session that needs to *receive* notifications,
don't roll your own SSE — use the WebSocket events endpoint
(documented in [Primary path](#primary-path-http-api--sdk-no-mcp-required))
or pull a real MCP SDK and use its `ClientSession`.

## Overview

SessionWeave is a transport-agnostic relay that lets multiple agents exchange commands inside a shared **session** without holding their conversational context. As a participant you operate in one of two modes:

- **Server mode** — you offer a capability; you join, advertise it, then respond to incoming commands.
- **Client mode** — you need a capability; you create or join a session, dispatch commands, await results.

**Invariant:** one connection participates in **at most one** session at a time. The `session_id` and your `participant_token` (which equals your `participant_id` in sub-project #1) are the only credentials — keep both until the session ends.

## Lifecycle & reachability

A SessionWeave participant is reachable only while *something* is
listening on its behalf. The relay buffers events for any joined
participant forever (until the session expires), but the agent
itself sees those events only when it actively asks for them.

```
relay buffer  ─────► alive forever (until session TTL)
agent process ─────► alive only during a turn
```

There are exactly three ways to bridge that gap:

| Pattern | Listener lives in | When |
|---|---|---|
| **Long-poll inside a turn** (`session_wait_for_event`) | The agent's own MCP/REST call | Default for turn-based runtimes (Claude Code, Codex, opencode CLI) |
| **Side-car runner** (`sessionweave-agent-runner`) | A daemon next to the agent host | Orchestrators, persistent workers |
| **External orchestrator** (cron, CI, scripted relauncher) | Outside the agent entirely | When you can't run a daemon next to the agent |

If you are an agent reading this skill: assume pattern #1 unless told
otherwise. Pattern #1 means your turn stays alive by re-issuing
`session_wait_for_event` in a loop until you have something to
report or the user instructs you to stop.

## Delivery modes

Two consumption patterns; pick the one your transport allows:

- **Push** — events arrive in real time. Available via the WebSocket events endpoint (`wss://.../events?token=…`) over HTTP, or as MCP `notifications/message` (LoggingMessageNotification, `logger="sessionweave"`) when you've joined through MCP. Use if your client can surface live frames or server-side log notifications to the model.
- **Pull** — events accumulate in the per-participant buffer. Two ways to consume:
  - **Active drain** (one-shot): `session_check_inbox` (MCP) / `POST .../inbox/drain` (REST). Returns whatever is in the buffer right now, even if empty. Use when you want to peek.
  - **Blocking wait** (long-poll): `session_wait_for_event` (MCP) / `POST .../inbox/wait` (REST). Blocks until at least one matching event arrives (or `timeout_seconds` elapses) and returns all matching events. **Recommended pattern** for keeping a turn alive without busy-polling.

  Use if your runtime is request/response only or doesn't surface push notifications to the model. **All HTTP REST clients are pull-mode by definition** — REST has no persistent transport for server-pushed events; pair it with the WS endpoint if you want push.

When joining through MCP, set this explicitly: `delivery_mode: "push"` (the default — uses LoggingMessageNotification) or `delivery_mode: "pull"`. REST joiners are always pull, regardless of body argument.

> **Practical note on MCP `delivery_mode="pull"`** — observed in
> multi-agent tests with smaller free-tier models: even when the
> relay correctly buffers events, some models don't reliably parse
> the JSON-shaped `session_check_inbox` response and fail to call
> `session_respond`. **Default to `"push"` for MCP-driven workers.**
> Only flip to `"pull"` if your runtime can't surface
> `notifications/message` log entries to the model (rare). REST/SDK
> clients should pair pull with the WS events endpoint when push
> matters.

In pull mode the receive pattern is: do work → drain → handle events → repeat.

## When to use

- Two or more agents need to coordinate across machines or processes (e.g. one agent runs a server you want to test, another agent issues requests).
- You can speak HTTP + (optionally) WebSocket. That covers virtually every runtime — Python scripts, OpenAI / Anthropic SDK workers, Node, Go, Lambda, JS/TS frontends, headless coding agents.
- The user gives you a `session_id` or an `swv://` / `swvs://` invite link and asks you to join, or to expose/consume a capability.

Skip if: you only need to call tools on one MCP server directly. SessionWeave is for *agent-to-agent* coordination.

## Invite links (`swv://...`)

The hand-off shape is a compact pointer to a session:

```
swv://<host>:<port>/<session_id>
```

The link is just an addressable reference. The session's actual
**purpose** (what to do, which role to take, which protocol to follow)
is persisted server-side and read back via `session_describe` — a
read-only tool that does NOT bind your connection to a participant.
Encoding/decoding the link is purely client-side; the relay never sees
the link itself.

### Receiving a link — decision flow

1. **Parse.** Either run `sessionweave-join "<link>"` (if installed) or
   `from sessionweave.invite import parse_link; inv = parse_link(link)`
   → `inv.relay_url`, `inv.session_id`. (Legacy links may carry a
   fragment-encoded purpose; ``parse_link`` returns it as ``inv.purpose``
   for backward compat, but new code shouldn't rely on it.)
2. **Describe.** `session_describe {session_id: inv.session_id}` →
   returns `{config, purpose, roster}` without allocating a participant
   slot. (`sessionweave-join` does this for you and prints the result.)
3. **Decide your role**:
   - `purpose.your_role` is set ⇒ assume that exact role with
     `purpose.your_capabilities`.
   - `purpose.available_roles: [{role, capabilities?, description?}, ...]`
     is set ⇒ pick one. If the choice is unambiguous given your context,
     pick autonomously; otherwise show the list to the user and ask.
   - Neither is set ⇒ the brief is open-ended. Read `purpose.instructions`
     (natural language), reason about what role makes sense, and ask the
     user before joining if you're unsure.
4. **Dispatch by `purpose.kind`**:
   - Known kind with a handler/skill ⇒ run that handler with the chosen
     role + capabilities + `purpose.delivery_mode` (if any).
   - Unknown kind ⇒ follow `purpose.instructions` step by step.
5. **Join.** `session_join` with the role/capabilities you decided on.
   Save your `participant_id` for `session_resume` on disconnect.

### Creating a link — host side

When you set up a session for someone else, set `purpose` on
`session_create` and emit the compact link. Two `purpose` shapes are
common:

**Single prescribed role** (e.g. tic-tac-toe — the joiner is O):

```python
purpose = {
    "kind": "tic-tac-toe-v1",
    "your_role": "O",
    "your_capabilities": ["tictactoe-o"],
    "delivery_mode": "pull",
    "instructions": "Sei Player O. X muove per primo. ...",
}
```

**Pick-from-list** (e.g. brainstorm meeting with several seats):

```python
purpose = {
    "kind": "brainstorm-v1",
    "available_roles": [
        {"role": "moderator",  "capabilities": ["mod"],
         "description": "Drives the agenda, summarises."},
        {"role": "contributor", "capabilities": ["contrib"],
         "description": "Brings ideas and reactions."},
        {"role": "observer",    "capabilities": ["obs"],
         "description": "Read-only — listens and reports."},
    ],
    "instructions": "Topic: <…>. Pick the seat that fits your charter.",
}
```

Then:

```python
out  = await session.call_tool("session_create", {"purpose": purpose})
link = create_link(relay_http_url, out["session_id"])
print(link)  # hand THIS string to the user; nothing else needed
```

### Constraints

- The relay caps `purpose` at 16 KiB JSON; oversized payloads return
  `invalid_config`.
- Fields beyond `kind` / `your_role` / `your_capabilities` /
  `available_roles` / `instructions` / `delivery_mode` are free-form —
  add anything your protocol needs (game rules, ground truth, etc.).
- The link itself is the address; sensitive briefing material should
  live in the server-side `purpose`, not in any fragment.

## Quick reference

| Tool | When |
|---|---|
| `session_create {config?, purpose?, persistence?, creator_id?}` | Open a new session. Returns `session_id`, `purpose` echo, **`creator_token`** (one-time, save it), `creator_id` (echoed from input or uuid4 if omitted — relay 0.17.0+), and `persistence` state. `persistence` is opt-in: omit it for **Zero Data Retention** (RAM-only); pass `{mode:"ttl",ttl_seconds:N}` or `{mode:"until_deleted"}` to persist (relay 0.16.0+). |
| `session_describe {session_id}` | Read-only snapshot: returns `{config, purpose, roster, persistence?}` without allocating a participant. Use as pre-flight before deciding to join. |
| `session_update_purpose {purpose?}` | Replace the session's purpose mid-flight (or `null` to clear). Broadcasts `session.purpose_updated` to others. Caller must be in the session. |
| `session_join {session_id, role?, capabilities?, objective?, metadata?, delivery_mode?}` | Bind this connection as a new participant. Returns `participant_id`, `roster`, `config`, `purpose`, `relay_epoch`, and `persistence` (if any). `delivery_mode` is `"push"` (default) or `"pull"`. |
| `session_resume {session_id, participant_id, last_acked_seq?}` | Re-attach after a disconnect. Returns `roster`, `config`, `purpose`, `relay_epoch`. For persisted sessions resume survives across a relay restart (new `relay_epoch`). See [Disconnect & resume](#disconnect--resume). |
| `session_leave {}` | Detach explicitly. Broadcasts `participant.left` (reason `left`). |
| `session_update_self {role?, objective?, capabilities?, metadata?}` | Replace fields (no merge). Omitted = unchanged. |
| `session_list_participants {}` | Snapshot of the roster. |
| `session_check_inbox {}` | Drain pending notifications for the caller. Returns `{messages: [{name, payload}, ...]}`. Required when joined with `delivery_mode="pull"`. |
| `session_wait_for_event {session_id, names?, timeout_seconds?}` | Pull-mode long-poll. Blocks until ≥1 buffered event matches the optional `names` filter, then returns `{messages, timed_out}`. Drain shape identical to `session_check_inbox`. Required tool when joined with `delivery_mode="pull"` and you need to wait for events without busy-polling. |
| `session_send {dispatch, target, payload, timeout_seconds?, command_id?}` | Dispatch a command. Pass a caller-generated stable `command_id` (e.g. `uuid.uuid4().hex`) to make retries idempotent — a second `send` with the same id returns the original response without creating a duplicate. Relay 0.16.0+. |
| `session_respond {command_id, status?, payload}` | Respond to a command you received. `status` defaults to `"ok"`. |
| `session_delete {session_id, creator_token}` | Terminate the session. Requires the per-session `creator_token` returned at `session_create`. The HTTP variant `DELETE /v1/sessions/{id}` ALSO accepts the relay-wide `SW_SUPERVISOR_ADMIN_TOKEN` as Bearer for a supervisor override (recorded as `ended_reason=admin_deleted`). Relay 0.16.0+. |
| `session_get_events {session_id, after_seq?, before_seq?, kind?, participant_id?, limit?, order?}` | Browse the timeline of a **persisted** session without bringing it back into RAM. Returns paginated `events` + `session_state` (`hot`/`cold`/`ended`). Returns `no_history` on an ephemeral session. Relay 0.16.0+. |

**Dispatch modes:**
- `specific` — requires `target.participant_ids`. Targets exact recipients.
- `all` — requires `target.role` or `target.capability`. Fans out to every match online.
- `any` — requires `target.role` or `target.capability`. Picks one (round-robin).

## Persistence & Zero Data Retention

A session is **ephemeral by default**. The relay holds it only in RAM
and discards everything (roster, command timeline, buffered events,
inflight trackers) once `idle_ttl_seconds` elapses. Nothing is written
to disk. This is the **Zero Data Retention** mode and it is **the
caller's choice**: omit the `persistence` field on `session_create` and
the contents of your collaboration will never touch the relay's storage
layer.

You opt INTO persistence by passing a `persistence` block at create:

```json
{ "persistence": { "mode": "ttl", "ttl_seconds": 86400 } }
```

or

```json
{ "persistence": { "mode": "until_deleted" } }
```

What changes when persistence is on (relay 0.16.0+):

- The session's full state (purpose, config, roster, every observable
  event, inflight commands, per-participant buffers) is persisted to the
  relay's storage on **every event** before delivery — durability
  invariant: "observed ⇒ committed".
- The session goes **cold** (evicted from RAM) when `idle_ttl_seconds`
  elapses, but stays recoverable. The next `session_join`,
  `session_resume`, or `session_describe` **hydrates** it back
  transparently — the client is unaware of the round-trip.
- Cross-restart resume works: even after a relay process `kill -9`, the
  session and its events come back on first access.
- `session_get_events` lets observers (a cockpit, a late-joiner, an
  auditor) read the entire timeline of a hot, cold, or ended session
  **without bringing it into RAM** — useful for scroll-back UX and for
  any pattern where one party joins late and needs context.

What you get back at create:

```json
{
  "session_id": "…",
  "creator_token": "…",   // one-time, save it
  "creator_id": "…",      // relay 0.17.0+ — echoed from input or uuid4
  "persistence": { "mode": "ttl", "expires_at": "2026-…" }
}
```

**`creator_token`** is the proof of ownership for the session:
- It's the only way to terminate via `session_delete` (unless the
  relay operator has configured a relay-wide
  `SW_SUPERVISOR_ADMIN_TOKEN` for supervisor overrides).
- Relay 0.17.0+: it's also the bearer for the per-session firehose
  at `GET /v1/sessions/{sid}/firehose?token=<creator_token>` — the
  supervisor stream that delivers every event in the session
  (chat between other participants included), authenticated via a
  constant-time SHA-256 compare against the session's stored
  `creator_token_hash`. A non-creator participant cannot subscribe
  to that stream without either the relay-wide admin token or the
  creator_token.

**`creator_id`** is an opaque display handle the relay echoes back.
Pass your own to bind a stable identity to the session
(`{"creator_id": "alice@team"}` etc.); omit it and the relay mints a
uuid4 so the field is always populated in the response. Not persisted
on the relay in v0.17.0 — it's a client-side display aid for
"created by X" surfaces. Relay-side enforcement (creator-only ops
bound to a user identity) is a future change once relay auth lands.

### When to use persistence

- **Zero Data Retention** (omit `persistence`): privacy-first agents,
  one-shot tasks, scratch sessions, anything you want truly gone the
  moment the conversation ends. **Default.**
- **`ttl`**: project work, long-running tasks, conversations that
  should survive a network blip or a relay restart but eventually
  expire (default min 1 h, max 90 d; operator can tune).
- **`until_deleted`**: stateful workspaces that should live until the
  host explicitly tears them down. Subject to a relay-wide safety cap
  (default 90 d of inactivity) to prevent accidental forever-storage.

### Custodying the `creator_token`

The token is returned exactly once. Treat it like an API key:

- Agents / SDK callers: keep it in process memory; if the process dies,
  the token dies with it (delete becomes impossible without an admin
  override).
- Cockpit / human supervisor: write it down or store in a vault. The
  hosted cockpit prompts for it at click time and never persists it to
  `localStorage`.

The relay stores only `sha256(creator_token)` server-side; the
plaintext never appears again in `describe`, `join`, `events`, or any
other response.

## Permission gates — request the whole bundle upfront

Some runtimes (Claude Code's permission system, opencode allowlists,
sandboxes) gate `mcp__sessionweave__*` tools individually. The
**deeply broken state** is when the agent gets `session_join` and
`session_send` allowed, joins, sends a command — and then discovers
`session_check_inbox` is denied. Now it's inside the session,
counterparties are waiting on its replies, and it has no way to read
them.

Avoid that by requesting the bundle that matches your participation
mode **before** you call `session_join`. Tell the user, in one short
sentence, which tools you need and why; if any of them is denied,
don't join.

| Mode | Required tool bundle |
|---|---|
| Read-only inspection of an invite link | `session_describe` |
| Join a session in `delivery_mode="pull"` (the safe default) | `session_describe`, `session_join`, `session_check_inbox`, `session_wait_for_event`, `session_send`, `session_respond`, `session_leave`, `session_list_participants` |
| Join in `delivery_mode="push"` (only if the runtime surfaces MCP logs to the model) | same as above minus `session_check_inbox` |
| Host a new session | the join bundle plus `session_create` (and `session_update_purpose` if the brief evolves) |

Phrase the ask concretely. Example for the join-pull bundle:

> "To participate as `<role>` in this session I'll need:
> `session_describe`, `session_join`, `session_check_inbox`,
> `session_wait_for_event`, `session_send`, `session_respond`,
> `session_leave`, `session_list_participants`. Without
> `session_check_inbox` I can drive but not read replies. OK to
> allow all eight?"

If your runtime supports wildcard allow-rules (`mcp__sessionweave__*`,
or "trust this MCP server"), suggest that to the user — it solves the
issue once and stays solved.

### When a tool is denied mid-session

If something does get denied after you've already joined:

1. Tell the user immediately what was denied and what you can no
   longer do (e.g. "I can't read incoming events"). Don't fabricate
   replies you didn't actually receive.
2. Try `session_leave` to release your participant slot cleanly. If
   that's also denied, **don't loop on it** — say so and let the user
   decide. The relay will time you out automatically after
   `offline_grace_seconds` (deployment-configured, default 300s).
3. Don't switch to raw HTTP as an end-run around the gate. The user
   denied the tool deliberately; bypassing it via the
   [raw-HTTP fallback](#fallback--speaking-mcp-over-raw-http) breaks
   their trust model. Surface the denial, recommend allow-listing,
   and stop.

## After creating a session — stay engaged

A session is **not a one-shot tool call**. Once you `session_create` +
`session_join`, the relay considers you the active host: other
participants will join, send commands, and expect you to react. If you
hand the link to the user and stop there, the session goes silent and
the join attempts queue up against an unresponsive party.

Default behaviour, unless `purpose` says otherwise: **stay in the loop
until the session ends**. End conditions are reaching the goal stated
in `purpose`, the user telling you to leave, or `session.ended` /
`participant.left` arriving on your inbox.

### Post-join checklist (do this before returning the link)

In this exact order, BEFORE you reply to the user with the invite link:

1. **Pick a strategy for staying engaged** based on your runtime (see
   the runtime breakdown below).
2. **If you're on a turn-based runtime, start the poller NOW.** Don't
   defer it. The fastest path is the bundled CLI:

   ```bash
   sessionweave-host \
     --purpose-json '<your purpose JSON>' \
     --my-role <your-role> \
     --my-capabilities <comma-separated>
   ```

   This single command runs `session_create` + `session_join` and spawns
   the background poller for you, returning a JSON envelope with the
   `swvs://` link, your `participant_id`, the poller PID, and the log
   path. If you're already inside the session for some other reason and
   only need the poller, run it directly:

   ```bash
   nohup python -m sessionweave.poller \
     --session-id <sid> --participant-id <pid> \
     --log /tmp/sw-events-<sid>.log > /dev/null 2>&1 &
   disown
   ```

3. **Verify the poller is alive** before you reply:

   ```bash
   ps -p <poller_pid> -o pid,cmd
   tail -n 1 /tmp/sw-events-<sid>.log    # should contain "poller-start"
   ```

   If the process is gone, your runtime sandboxed/killed it — fall back
   to the [drain-on-every-turn](#turn-based-without-a-poller) variant
   and tell the user explicitly that you can't keep a background loop.
4. **Tell the user what you did**, including the log path so they can
   check progress without bothering you:

   > "Session open at `<link>`. Background poller running (PID
   > `<n>`, log at `<path>`). I'll act on incoming events whenever you
   > send me a message — say 'check' or anything else, doesn't matter."

5. **Only now** return the invite link to the user.

Skipping any of these steps is the most common reason a host appears to
"create the session and ghost it".

### Runtime breakdown

How "staying engaged" looks depends on your runtime:

- **Long-running runtime** (background process, persistent loop, headless
  CLI like `claude -p`): run an actual loop. Drain
  `session_check_inbox` (or your push notifications) every few seconds,
  process events, send replies, repeat.
- **Turn-based runtime** (interactive chat that finishes when you
  return — Claude Code, opencode, most coding agents): you don't run a
  real loop, but you have two complementary moves:
  - **Drain on every turn, no matter the topic.** As soon as the user
    sends *any* new message — even one unrelated to the session —
    your *first* action is `session_check_inbox`. Don't wait for the
    user to say "check": by then the events you should have processed
    are already stale, and worse, the user assumes you've been
    listening when you haven't.
  - **Optional: spawn a background poller via Bash.** If your runtime
    lets you start detached processes, run a tiny script that polls
    the relay every few seconds and appends events to a log file. At
    each turn `tail` the log instead of polling synchronously. See
    [Background poller pattern](#background-poller-pattern-turn-based-runtimes)
    below.

  In either mode, set expectations clearly with the user:

  > "Session is open at `<link>`. Send any message to wake me up so I
  > can check incoming events — I'll drain the inbox on every turn,
  > so 'go' or 'continue' is enough; a topic shift works too."
- **Event-aware runtime** (supports webhooks, schedulers, long polling):
  configure that mechanism so you wake on incoming events. Document
  for the user how the wake-up will look.

Whatever the runtime, on the way out be explicit:

> "I've created session `<id>` (link: `<swvs://...>`). I will stay
> engaged until <end condition from purpose>. I'll drain the inbox on
> every turn and respond to commands as they arrive."

Hard rule: **don't `session_create` and immediately drop ownership.**
Either (a) keep watching, (b) hand the host role to another participant
explicitly via the protocol, or (c) `session_leave` and tell the user
the session is closed. Silent abandonment is the worst outcome — the
session lingers until idle TTL while everyone wonders why the host
isn't responding.

### Background poller pattern (turn-based runtimes)

If you're on a runtime that lets you start detached processes (most
coding agents do — Claude Code, opencode, etc.), you can replicate the
"long-running loop" behaviour even though your model is turn-based.
The pattern: spawn a small Python script that resumes your participant
every few seconds, drains the inbox, and appends each event as a JSON
line to a log file. At every later user turn you `tail` the log to see
what's arrived since you last looked.

**Preferred path: use the bundled module.** If `pip install sessionweave`
is available in your environment, the poller is already installed —
launch it directly:

```bash
nohup python -m sessionweave.poller \
    --session-id <sid> --participant-id <pid> \
    --log /tmp/sw-events-<sid>.log \
    > /dev/null 2>&1 < /dev/null &
disown
```

Or skip the manual orchestration entirely and use `sessionweave-host`,
which does `session_create` + `session_join` + spawns this poller in
one command (see [Post-join checklist](#post-join-checklist-do-this-before-returning-the-link)
above).

**Fallback: standalone snippet.** If you can't install the package,
copy this into `/tmp/sw-poller.py` and launch with `python /tmp/sw-poller.py
<relay-url> <sid> <pid> <log>`:

```python
import asyncio, json, sys
from datetime import datetime, timezone
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

URL, SID, PID = sys.argv[1], sys.argv[2], sys.argv[3]
LOG = sys.argv[4] if len(sys.argv) > 4 else "/tmp/sw-events.log"

async def step(s):
    await s.call_tool(name="session_resume",
                      arguments={"session_id": SID, "participant_id": PID})
    res = await s.call_tool(name="session_check_inbox", arguments={})
    sc = getattr(res, "structuredContent", None) or {}
    msgs = (sc.get("result", sc) if isinstance(sc, dict) else {}).get("messages", [])
    if msgs:
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with open(LOG, "a", buffering=1) as f:
            for m in msgs:
                f.write(json.dumps({"ts": ts, **m}) + "\n")

async def main():
    while True:
        try:
            async with streamablehttp_client(URL) as (r, w, _):
                async with ClientSession(r, w) as s:
                    await s.initialize()
                    await step(s)
        except Exception as e:
            with open(LOG, "a", buffering=1) as f:
                f.write(json.dumps({"error": str(e)}) + "\n")
        await asyncio.sleep(3.0)

asyncio.run(main())
```

### Reading what the poller wrote

On every later turn, before doing anything else:

```bash
tail -n 50 /tmp/sw-events-<sid>.log
```

Then act on the new events with `session_send` / `session_respond` from
the agent's main turn (those still need to run from your model — they
are decisions, not pure forwarding).

When the session ends, kill the poller (`pkill -f 'sessionweave.poller'`
or `pkill -f sw-poller.py` for the fallback snippet). The bundled
module also self-exits when it observes `session.ended` or your own
`participant.left`.

### Turn-based without a poller

If your runtime sandbox killed the poller or you can't run detached
processes at all, fall back to **drain-on-every-turn**: at the start of
every later user message, your *first* tool call is
`session_check_inbox`. Don't wait for the user to ask. Tell them up
front:

> "I can't keep a background loop on this runtime, so the session will
> only advance when you talk to me. Any short message ('check', 'go',
> or anything) wakes me up to drain incoming events."

This pattern + drain-on-every-turn together gets you close to "real
listening" without needing a long-running runtime.

## Server mode (offering a capability)

1. **Open or join.** If you're starting fresh: `session_create` then tell the user the returned `session_id`. If they gave you one: skip ahead.
2. **Advertise.** `session_join {session_id, role: "<service-name>", capabilities: ["<capX>", "<capY>"]}`. **ALWAYS pass `role` AND `capabilities` explicitly** — they're nominally optional but downstream `dispatch="all" / "any"` filtering relies on them, and observed behaviour with smaller models shows the optional args get dropped silently when not stressed in the prompt. If you have nothing meaningful to advertise, pick a generic role like `"observer"` and capabilities `["respond"]`. Capabilities are the strings clients will filter on.
3. **Persist your `participant_id`.** You'll need it for `session_resume` on disconnect.
4. **Listen.** The relay sends `command.received` notifications: `{command_id, sender_id, dispatch_mode, dispatched_to, payload, expires_at}`. See [After creating a session — stay engaged](#after-creating-a-session--stay-engaged) for how to keep the listening loop alive on your runtime.
5. **Respond once per command.** Do the work, then `session_respond {command_id, status: "ok"|"error", payload: <result>}`. Respond before `expires_at` or you'll get `command_not_found`.

## Client mode (dispatching commands)

1. **Join the right session.** If the server agent created it, join with the `session_id` they published. Otherwise `session_create` and hand the id to the server before sending.
2. **Discover counterparts.** `session_list_participants` — pick by `role` and `capabilities`.
3. **Dispatch.** Choose the mode:
   - Targeted: `dispatch="specific"`, `target.participant_ids=[<id>]`.
   - Any matching server: `dispatch="any"`, `target.capability="<cap>"`.
   - All matching: `dispatch="all"`, `target.role="<role>"` (fan-out).
   Optionally `timeout_seconds` (clamped to deployment bounds).

   > **Practical note** — when you have a known set of recipients
   > already in the roster, prefer `dispatch="specific"` with explicit
   > `participant_ids` over `dispatch="all" + target.capability=…`.
   > Multi-agent tests showed that smaller models occasionally drop
   > the `capabilities` arg on join, which would silently exclude
   > those workers from a capability-filtered broadcast. Pinning the
   > `participant_ids` makes delivery deterministic.
4. **Watch notifications.**
   - `result.received` — one per responder, `{command_id, responder_id, status, payload}`.
   - `command.cancelled` — sent to recipients that haven't responded by deadline (`reason: ttl_expired`) or whose sender left mid-flight (`reason: sender_gone`). **Server-mode workers must accept this and stop processing the command** — there's no point responding after cancellation.
   - `command.completed` — terminal. Three variants:
     - `reason: all_responded` / `ttl_expired` — sent to the SENDER. Only at this point are all results in.
     - `reason: sender_gone` — sent to each RESPONDER that had already
       answered before the sender left (the sender's inbox is gone by
       then). Payload carries an extra `orphaned_results: [responder_id, ...]`
       array — your own participant_id will appear in it. Treat this
       as a soft cancellation of work you submitted: the result you
       sent is buffered for a sender that won't drain it. Also mirrored
       to the firehose for diagnostic visibility.

## Exit feedback (mandatory)

Before you call `session_leave`, ALWAYS attach a short structured
exit report so the host (and remaining participants) can see how the
session went from your side without having to instrument you. From
v0.10.0 onward `session_leave` accepts an optional `feedback`
parameter; the relay propagates it on the broadcast `participant.left`
event so other clients can collect it via their normal event stream
(`session_check_inbox` / WS / MCP push).

Recommended shape (the relay only enforces a 4 KiB JSON cap):

```json
{
  "summary": "echo handled 2 commands cleanly",
  "rating": 5,
  "issues": [],
  "metrics": {
    "commands_received": 2,
    "commands_responded": 2,
    "wall_seconds": 47
  }
}
```

- `summary` — one line of plain English. What did you do? Did it work?
- `rating` — integer 1..5, your subjective "how was the experience".
  Use it consistently so the host can spot bad patterns.
- `issues` — list of short strings, anything weird that shouldn't be
  ignored: a race you observed, an ambiguous instruction in `purpose`,
  a tool that didn't behave as documented. Empty list if no issues.
- `metrics` — optional client-side counters. The host can compare them
  against its own view (e.g. "I dispatched 3 commands but the worker
  only saw 2" exposes a delivery bug).

Calls (pick the surface that matches your transport):

```python
# SDK
await me.leave(feedback={"summary": "...", "rating": 5, "issues": [], "metrics": {...}})
```

```bash
# REST — JSON body on DELETE
curl -X DELETE "$RELAY/v1/sessions/$SID/participants/me" \
  -H "authorization: Bearer $TOK" -H 'content-type: application/json' \
  -d '{"feedback":{"summary":"…","rating":5,"issues":[],"metrics":{}}}'
```

```python
# MCP
await session_leave(feedback={"summary": "…", "rating": 5, "issues": [], "metrics": {...}})
```

If you genuinely have nothing to report (single-shot worker, ran clean,
no anomalies) you may pass a minimal `{"summary": "no issues",
"rating": 5, "issues": []}` — but DO send something. A worker that
walks away silently is the hardest case to diagnose later.

The host should drain `participant.left` events after a session and
either log the feedback or surface it to the operator. Sub-project #2
(persistence) will additionally archive feedback for post-mortem
analysis; in #1 it lives only in the in-memory event stream, so the
host has to capture it before draining is over.

## Disconnect & resume

If your transport drops, the relay marks you offline (others see
`participant.updated`, `online: false`) and **buffers** notifications
addressed to you. You have `offline_grace_seconds` (deployment-configured,
default 300s) to reconnect:

```
session_resume {session_id, participant_id, last_acked_seq?}
```

The buffered notifications flush in order on the new transport. Past the
grace window the relay evicts you with `participant.left`
(`reason: "offline_timeout"`).

**Resume requires the original `participant_id`** — never throw it away
mid-session.

### Did the relay restart? Check `relay_epoch`.

`session_join` and `session_resume` both return a `relay_epoch` integer
— the UNIX-seconds startup time of the relay process. Capture it at
join, compare on resume:

```python
joined = await me.client.join(sid, role="worker")
join_epoch = joined["relay_epoch"]     # remember this
# ... time passes, transport drops, you reconnect ...
resumed = await me.client.resume(sid, participant_id, last_acked_seq=last_seq)
if resumed["relay_epoch"] != join_epoch:
    # Relay has restarted. The session_id, your participant_id, and
    # every buffered event are gone — `resume` returned a NEW slot in
    # a fresh process. Treat as a cold start: discard your local seq
    # cursor, re-derive `session_id` from the invite link if you need
    # the same session (it likely no longer exists), and proceed via
    # describe → join from scratch.
    ...
```

The SDK's `Participant.relay_epoch` attribute exposes this for you
(populated from the join response; defaults to `0` when talking to a
pre-0.16.0 relay so `if me.relay_epoch == 0` is a safe "unknown" check).

### Seq cursors and `last_acked_seq`

Every push notification carries a monotonic per-participant `seq`
field. The WebSocket attach accepts `?last_acked_seq=N` and
`session_resume` accepts an optional `last_acked_seq` body field; the
relay then suppresses replay of buffered events with `seq <= N`. This
closes the WS-reconnect duplicate-frame gap and keeps the MCP/REST
resume path in parity.

If you don't track `seq` (most agents don't need to), just omit the
field — the relay will replay everything in the buffer. The deduping
matters mainly when you've already acted on events received over a
previous channel that died mid-stream.

### The buffer is bounded — don't assume free storage during grace

The per-session buffer is capped by an aggregate byte budget
(`max_buffer_bytes_per_session`, default 50 MiB) **plus** a
per-participant message count cap (`max_pending_notifications_per_participant`,
default 1000). When the aggregate cap is exceeded the relay evicts the
oldest buffered events across every participant — including events
addressed to *you* if you're the offline one and others are still
generating traffic. A `buffer_evicted` event is mirrored to the
firehose (only) so observers can see back-pressure, but you, the
recipient, get no direct signal.

Practical consequence: a long disconnect during a noisy session can
return you to a partial event history even within `offline_grace_seconds`.
Workers that must not miss anything should keep their WS attached (or
poll aggressively in pull mode), not rely on the buffer as durable
storage.

## Common mistakes

| Mistake | Fix |
|---|---|
| `dispatch="all"` with empty `target` | Always set `role` or `capability` for `all`/`any`. |
| Two `session_respond` for the same `command_id` | Returns `already_responded`. Track responded commands locally. |
| Joining twice on the same connection | Returns `already_in_session`. One MCP connection = one session. |
| Ignoring `command.completed` | Only then are all responders in (or the deadline expired). Don't act on partial results unless you mean to. |
| Calling tools without joining first | Returns `not_in_session`. Always `session_join` / `session_resume` before send/respond. |
| Treating session state as durable | State is in-memory only — restart of the relay loses everything. Idle sessions also expire after `idle_ttl_seconds` (default 30 min). |
| Driving the relay with raw HTTP without `initialize` | Returns `-32602 "Invalid request parameters"`. Streamable HTTP is stateful — see [Fallback — speaking MCP over raw HTTP](#fallback--speaking-mcp-over-raw-http). |
| Solving the request with raw HTTP and not persisting `.mcp.json` | Next time this runtime starts, the same friction is back: no `mcp__sessionweave__*` tools, agent reaches for curl again. Always write the MCP config first (see [Bootstrap order](#bootstrap-order--persist-first-then-act)), then handle the immediate session over HTTP as a bridge. |
| `session_create` then return to the user without listening | Other participants will join and queue commands against an unresponsive host until the idle TTL evicts the session. After creating, you OWN the session — see [After creating a session — stay engaged](#after-creating-a-session--stay-engaged) for the polling pattern that fits your runtime. |
| Waiting for the user to say "check the inbox" before draining | By the time they ask, events are already stale. On a turn-based runtime, drain on **every** subsequent user turn — even if the user is talking about something else. Better still, run the [background poller](#background-poller-pattern-turn-based-runtimes) and `tail` the log each turn. |
| Joined and sent, but `session_check_inbox` is denied by the permission system | You're now in the session and can't read replies. Request the full participation bundle BEFORE join (see [Permission gates](#permission-gates--request-the-whole-bundle-upfront)). If it happens mid-session, tell the user what's broken, try `session_leave`, and stop — don't bypass the gate via raw HTTP. |
| Sync `subprocess.run` (or any >10s blocking call) inside `async with me.events()` | The websockets ping/pong starves and the relay closes the WS; your stream ends after the call returns, you exit with `StopAsyncIteration`, and subsequent `dispatch="specific"` to your participant_id fails with `invalid_targets`. Wrap blocking calls with `await asyncio.to_thread(...)` so the loop keeps servicing the WS. |
| Reading `participant.joined` fields at the top of `payload` (e.g. `payload["role"]`) | The event wraps the public record under `payload["participant"]` — read `payload["participant"]["role"]`. Same for `participant.updated`. See [Notifications catalogue](#notifications-catalogue) for the full payload shapes. |
| Treating `offline_grace_seconds` as a free durable buffer | The aggregate byte budget (`max_buffer_bytes_per_session`, default 50 MiB) evicts the oldest events across the session — your events too, if others are noisy while you're offline. No direct signal to the recipient; `buffer_evicted` shows up only on the firehose. Keep your transport attached for at-least-once delivery during long disconnects. |
| Calling `session_resume` after a relay restart and trusting the result | The `participant_id` you held is gone with the previous process. Compare `relay_epoch` on resume against the value you captured at join — if it changed, the "resume" silently created a fresh slot in a new process. See [Disconnect & resume](#disconnect--resume). |

## Notifications catalogue

Each event arrives with `name` + `payload` (the shape below is what's
in `payload`). When in doubt, the authoritative shape is in the relay's
`openapi.yaml` and emitted from `session.py`; this table is a quick
reference. **Don't read top-level fields from `payload` when the event
wraps them** — e.g. `participant.joined` puts the public record under
`payload.participant`, NOT at `payload.role`.

| Event | Payload shape | Notes |
|---|---|---|
| `participant.joined` | `{"participant": ParticipantPublic}` | Wrapped — read `payload["participant"]["role"]`, not `payload["role"]`. |
| `participant.updated` | `{"participant": ParticipantPublic}` | Same wrapper. Covers online/offline flips and capability/metadata changes. |
| `participant.left` | `{"participant_id", "reason", "feedback"?}` | `reason` is `left` \| `offline_timeout`. `feedback` present iff the leaver passed one to `session_leave`. |
| `command.received` | `{"command_id", "sender_id", "dispatch_mode", "dispatched_to", "payload", "expires_at"}` | Delivered to recipients. `payload` is whatever the sender passed. |
| `result.received` | `{"command_id", "responder_id", "status", "payload"}` | Delivered to the SENDER of the command, one per responder. |
| `command.completed` | `{"command_id", "received_count", "expected_count", "reason", "orphaned_results"?}` | Terminal. `reason` is `all_responded` \| `ttl_expired` \| `sender_gone`. For `all_responded` / `ttl_expired` it goes to the sender only. For `sender_gone` it goes to every RESPONDER that had answered before the sender left (their result is now orphaned in the gone sender's buffer) — that variant carries an `orphaned_results: [responder_id, ...]` array and is also mirrored to the firehose. |
| `command.cancelled` | `{"command_id", "reason"}` | To recipients that hadn't responded yet. `reason` is `ttl_expired` \| `sender_gone`. |
| `session.purpose_updated` | `{"purpose"}` | Broadcast to other participants when someone calls `session_update_purpose`. |
| `session.ended` | `{"reason"}` | Terminal, all participants. `reason` is `idle_ttl` \| `server_shutdown` \| `host_deleted` \| `admin_deleted` \| `persistence_expired`. The last three appear only with persisted sessions (relay 0.16.0+). |
| `session.cold` *(persisted only)* | `{}` | Sent only to LIVE push subscribers (WebSocket or MCP SSE) when a persisted session hits idle TTL. The session is being evicted from RAM but its state stays on disk; a subsequent `session_join` / `session_resume` / `session_describe` rehydrates it. **Different from `session.ended`** — the session is not over, just paged out. Relay 0.16.0+. |
| `supervisor.note` | `{"note_id", "kind", "target", "message", "ts_ms"}` | See [Handling `supervisor.note`](#handling-supervisornote). |
| `buffer_evicted` *(firehose only)* | `{"reason", "evicted_count", "freed_bytes", "buffered_bytes", "buffer_byte_cap"}` | Emitted when `_enforce_session_byte_budget` drops the oldest events to bring the per-session aggregate under `max_buffer_bytes_per_session`. **Not delivered to individual participants** — only firehose subscribers see it. Treat as a back-pressure indicator; recipients of evicted events get no notification. |

`ParticipantPublic` is the public participant record:
`{"participant_id", "role", "objective", "capabilities", "metadata", "joined_at", "online"}`.

### Wire format (interop note)

To stay compatible with every MCP client, the relay tunnels application events through the standard `notifications/message` (LoggingMessageNotification) channel. Each notification arrives as a log entry with `logger = "sessionweave"` and `data = {"_name": "<event>", "_payload": {...}}`. Push it through your client's logging callback or transcript surface, then unwrap `data._name` and `data._payload`. Custom-method JSON-RPC notifications would be dropped by the standard MCP client validator — that's why we tunnel.

## Error catalogue

`not_in_session`, `already_in_session`, `session_not_found`, `session_full`, `payload_too_large`, `invalid_dispatch_filters`, `invalid_targets`, `command_not_found`, `not_a_recipient`, `already_responded`, `participant_not_found`, `invalid_config`. All carry a human-readable `message`.

**Long-poll error codes** (`wait_for_event` / `POST .../inbox/wait`):
`invalid_delivery_mode` and `wait_already_in_flight` (HTTP 409),
`participant_evicted` (HTTP 409), `too_many_waits` (HTTP 429), and
`client_disconnect` (HTTP 499 — relay 0.17.3+, see below).

> **`client_disconnect` (HTTP 499, relay 0.17.3+).** The relay
> watches the ASGI receive channel for `http.disconnect` while the
> wait blocks. When the HTTP client closes the TCP socket before
> the wait finishes (typical when the client's read timeout is
> shorter than the requested `timeout_seconds`), the relay cancels
> the wait, clears the participant's in-flight flag, and returns
> 499. The client never sees the body, but the cleanup is what
> matters: the **next** `wait_for_event` from the same participant
> proceeds normally instead of cascading with
> `wait_already_in_flight` until the original deadline expires on
> its own. To avoid generating these in the first place, make sure
> your HTTP client's read timeout is at least
> `timeout_seconds + a few seconds of slack`.

## Agent configuration

To make this skill useful, the agent needs to (a) reach the relay and
(b) discover this document. The relay is reachable two ways — pick
whichever fits your runtime.

### 1. Reach the relay

**Option A — HTTP API + SDK (no MCP required, the recommended default).**
Anything that can do HTTP and WebSocket already has what it needs; the
[Primary path](#primary-path-http-api--sdk-no-mcp-required) section above
covers the contract end-to-end. For Python:

```bash
pip install -e 'git+https://github.com/EnzinoBB/SessionWeave#subdirectory=SDK/python'
```

```python
async with SessionWeaveClient() as c:        # default: relay.sessionweave.com
    sess = await c.create_session(purpose={"kind": "demo"})
    me = await c.join(sess["session_id"], role="worker")
    ...
```

For other languages, point any HTTP+WS client at
`https://relay.sessionweave.com` and follow the [endpoint
catalogue](#or-speak-the-rest--ws-protocol-directly).

**Option B — MCP integration (only if your runtime already speaks MCP).**
Put the relay in `.mcp.json` (project-local for Claude Code and most
coding agents; user-level if your runtime prefers it):

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

Use a self-hosted URL instead (e.g. `http://127.0.0.1:8765/mcp`) when
running the relay yourself. Restart the client so the catalogue
refreshes; the tools appear as `mcp__sessionweave__session_create`,
`..._join`, etc. For headless `claude -p` invocations pass
`--mcp-config <file>` and an `--allowedTools mcp__sessionweave__...`
allowlist.

Pre-registration is convenient but not required for either option:
when an `swv://...` / `swvs://...` invite points to a relay you haven't
seen, the link itself carries the endpoint. Use the [base URL it
encodes](#the-link-tells-you-the-endpoint) directly with the SDK / REST
or, if you'd prefer MCP, add it to your MCP catalogue at that point.

### 2. Discover this skill

Symlink (or copy) the directory into the agent's skills folder:

```bash
mkdir -p ~/.claude/skills
ln -s /path/to/SessionWeave/skills/sessionweave-participant \
      ~/.claude/skills/sessionweave-participant
```

Symlinking keeps the skill in sync with future updates.

## Versioning

The `version:` field in the frontmatter follows **SemVer** for this
skill specifically (decoupled from the relay's own version):

- **Major** bump on a breaking shape change to the recommended
  protocol (e.g. an entire mode reworked).
- **Minor** bump when a new section or recipe is added (the most
  common case so far).
- **Patch** for typos and clarifying edits that don't change behaviour.

When an agent gives a status report or hits an issue, it should
include the version it's operating on. Example:

> "I'm acting under `sessionweave-participant` v0.12.1 — bootstrapped
> the MCP entry, joined as observer, polling on every turn."

That makes correlation against the changelog cheap when behaviours
diverge between two agents.

### Sub-project alignment

This skill (v0.18.0) covers **SessionWeave sub-projects #1 and #2**:

- **#1** — anonymous transient sessions, shared bearer = participant_id,
  WebSocket events, supervisor notes. Default behaviour.
- **#2** — opt-in persistence: hot/cold lifecycle, cross-restart
  resume, the events-history endpoint, the `creator_token` returned at
  create, the `session_delete` tool, the in-session participant_token
  path on `/supervisor/notes`. See the
  [Persistence & Zero Data Retention](#persistence--zero-data-retention)
  section.

The relay self-identifies on `GET /v1/info`:
- `"auth_model": "shared-bearer-sub-project-1"` — token == participant_id
- `"capabilities": {…, "session_delete": true, …}` — feature flags

Clients that care about auth posture should branch on `auth_model`
rather than the relay's semver. Clients that conditionally call
`session_delete` / `session_get_events` should check `capabilities`.

When accounts/auth (#3) lands, the `auth_model` string will change to
something JWT-shaped, and this skill will get at least a minor bump.
