---
name: sessionweave-participant
version: 0.7.0
description: Use when an agent must join, host, or test through a SessionWeave relay (MCP-based multi-agent rendezvous), and especially whenever an `swv://` or `swvs://` invite link appears in the conversation or input — covers server mode, client mode, and the parse → describe → join flow for invites.
---

# SessionWeave participant

> **Skill version: 0.7.0** — when reporting your behaviour or filing
> issues, cite this version so the user can correlate it against the
> changelog. To check what you're running on, look at the `version:`
> field in the frontmatter above (or, if you only have the body in
> context, this header line).

## Default relay

Unless the operator says otherwise, this skill assumes a hosted relay at:

```
https://relay.sessionweave.com/mcp
```

When the skill is registered (see [Agent configuration](#agent-configuration)
below), that endpoint is added to the agent's MCP catalogue so the
`mcp__sessionweave__*` tools are available immediately — no invite link
required. From there the agent can call `session_create` to host a
session of its own, or wait for an `swv://` / `swvs://` invite that
points elsewhere.

## Auto-trigger on `swv://` / `swvs://`

When you see a string of the form
`swv://<host>[:<port>]/<session_id>` (HTTP relay) or
`swvs://<host>[:<port>]/<session_id>` (HTTPS relay) in the user's
message, in a file, in another tool's output, or anywhere else, treat
it as a **SessionWeave invite** and follow the receive-side flow below
— do not ask the user "what do you want me to do with this URL". The
link's existence is the request.

### The link tells you the endpoint

The link's scheme + host + port IS the relay's MCP endpoint:

- `swv://h[:p]/sid`  → `http://h[:p]/mcp`  (dev / loopback)
- `swvs://h[:p]/sid` → `https://h[:p]/mcp` (TLS-fronted, the production form)

### Bootstrap order — persist FIRST, then act

If the relay isn't already in your MCP catalogue (no
`mcp__sessionweave__...` tools listed for you), do these two things,
**in this order**:

1. **Persist the MCP entry** so future runs of this runtime have the
   tools loaded automatically. Don't skip this step "for now" — solving
   the immediate task with raw HTTP and forgetting to write the file
   leaves the next conversation in the same friction.
2. **Drive the current session** through the [raw-HTTP fallback](#fallback--speaking-mcp-over-raw-http)
   below. The `.mcp.json` you just wrote needs a runtime restart to
   take effect, which you can't do mid-session — so the *current* call
   has to go over HTTP. Subsequent runs use the native tools.

Concretely, write a minimal `.mcp.json` (project-local for Claude Code
and most coding agents; user-level if your runtime prefers it). The
URL is the one you decoded from the link:

```json
{
  "mcpServers": {
    "sessionweave": {
      "type": "http",
      "url": "https://<host>[:<port>]/mcp"
    }
  }
}
```

Common per-runtime locations (check yours; this is a moving target):

| Runtime | File |
|---|---|
| Claude Code (project) | `./.mcp.json` |
| Claude Code (user)    | `~/.claude/.mcp.json` |
| opencode (user)       | `~/.config/opencode/mcp.json` (or whatever its docs say) |

After writing the file, **tell the user**:

> "I've added SessionWeave to your MCP config at `<path>`. Restart the
> runtime to load the native tools; until then I'm using the raw-HTTP
> fallback for this session."

If your runtime exposes a programmatic "add MCP server" call (some do),
use it instead of editing the file — the effect is the same and may
not require a restart.

The point: the link is self-routing — relay address + session id in
one string.

### The shortest first move

```bash
sessionweave-join "<the swv:// link>"
```

(installed by `pip install sessionweave`). This parses the link, calls
`session_describe` against the relay using the address from the link
itself, and prints the session purpose. If the CLI isn't available, do
the same two steps manually with the MCP tools in your catalog
(`session_describe` is read-only — no slot taken).

After the describe, follow [Receiving a link → decision flow](#receiving-a-link--decision-flow).

### Fallback — speaking MCP over raw HTTP

If your runtime has neither (a) the relay registered as an MCP server
nor (b) `pip install sessionweave` available, and you must drive the
relay from a shell or generic HTTP client, follow the **full Streamable
HTTP MCP protocol**. Cutting corners is the most common reason agents
trip over `-32602 "Invalid request parameters"`.

The protocol is **stateful** and requires three steps before any tool
call:

1. **`initialize`** — POST to the relay endpoint. Capture the
   `mcp-session-id` header from the response. All subsequent requests
   MUST include that header.
2. **`notifications/initialized`** — POST a JSON-RPC notification (no
   `id` field) to confirm the handshake. Server replies `202`.
3. **`tools/call`** — and only now you can run a tool. The tool name
   goes inside `params.name`, the arguments inside `params.arguments`.
   The JSON-RPC `method` is the literal string `"tools/call"`, NOT the
   tool name.

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

For an interactive session that needs to *receive* notifications
(`command.received`, etc.), don't roll your own SSE: pull a real MCP
SDK (Python, TypeScript, Go) and use its `ClientSession`. Raw HTTP is
fine for one-shot tool calls, painful for streams.

## Overview

SessionWeave is an MCP relay that lets multiple agents exchange commands inside a shared **session** without holding their conversational context. As a participant you operate in one of two modes:

- **Server mode** — you offer a capability; you join, advertise it, then respond to incoming commands.
- **Client mode** — you need a capability; you create or join a session, dispatch commands, await results.

**Invariant:** one MCP connection participates in **at most one** session at a time. The `session_id` and your `participant_id` are the only credentials — keep both until the session ends.

## Delivery modes

Pick one when calling `session_join`:

- **`delivery_mode: "push"`** (default) — the relay sends events as MCP `notifications/message` (LoggingMessageNotification, `logger="sessionweave"`). Use if your MCP client surfaces server logs to the model.
- **`delivery_mode: "pull"`** — the relay routes ALL events into the participant's buffer; nothing is pushed. Drain with `session_check_inbox` between actions. Use if your client doesn't surface logging notifications to the model (e.g. headless `claude -p` instances).

In `pull` mode the receive pattern becomes: do work → call `session_check_inbox` → handle events → repeat.

## When to use

- Two or more agents need to coordinate across machines or processes (e.g. one agent runs a server you want to test, another agent issues requests).
- The MCP tool catalog includes `session_create`, `session_join`, `session_send`, etc. — those are the relay tools.
- The user gives you a `session_id` (or an `swv://` invite link) and asks you to join, or asks you to expose/consume a capability.

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
| `session_create {config?, purpose?}` | Open a new session. Returns `session_id` (and `purpose` echo). |
| `session_describe {session_id}` | Read-only snapshot: returns `{config, purpose, roster}` without allocating a participant. Use as pre-flight before deciding to join. |
| `session_update_purpose {purpose?}` | Replace the session's purpose mid-flight (or `null` to clear). Broadcasts `session.purpose_updated` to others. Caller must be in the session. |
| `session_join {session_id, role?, capabilities?, objective?, metadata?, delivery_mode?}` | Bind this connection as a new participant. Returns `participant_id`. `delivery_mode` is `"push"` (default) or `"pull"`. |
| `session_resume {session_id, participant_id}` | Re-attach after a disconnect. |
| `session_leave {}` | Detach explicitly. Broadcasts `participant.left` (reason `left`). |
| `session_update_self {role?, objective?, capabilities?, metadata?}` | Replace fields (no merge). Omitted = unchanged. |
| `session_list_participants {}` | Snapshot of the roster. |
| `session_check_inbox {}` | Drain pending notifications for the caller. Returns `{messages: [{name, payload}, ...]}`. Required when joined with `delivery_mode="pull"`. |
| `session_send {dispatch, target, payload, timeout_seconds?}` | Dispatch a command. |
| `session_respond {command_id, status?, payload}` | Respond to a command you received. `status` defaults to `"ok"`. |

**Dispatch modes:**
- `specific` — requires `target.participant_ids`. Targets exact recipients.
- `all` — requires `target.role` or `target.capability`. Fans out to every match online.
- `any` — requires `target.role` or `target.capability`. Picks one (round-robin).

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
| Join a session in `delivery_mode="pull"` (the safe default) | `session_describe`, `session_join`, `session_check_inbox`, `session_send`, `session_respond`, `session_leave`, `session_list_participants` |
| Join in `delivery_mode="push"` (only if the runtime surfaces MCP logs to the model) | same as above minus `session_check_inbox` |
| Host a new session | the join bundle plus `session_create` (and `session_update_purpose` if the brief evolves) |

Phrase the ask concretely. Example for the join-pull bundle:

> "To participate as `<role>` in this session I'll need:
> `session_describe`, `session_join`, `session_check_inbox`,
> `session_send`, `session_respond`, `session_leave`,
> `session_list_participants`. Without `session_check_inbox` I can
> drive but not read replies. OK to allow all seven?"

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

Saved as e.g. `/tmp/sw-poller.py`:

```python
import asyncio, json, os, sys, time
from datetime import datetime, timezone
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

URL = sys.argv[1]      # e.g. https://relay.sessionweave.com/mcp
SID = sys.argv[2]      # session_id
PID = sys.argv[3]      # your participant_id
LOG = sys.argv[4] if len(sys.argv) > 4 else "/tmp/sw-events.log"

async def step(session):
    await session.call_tool(name="session_resume",
                            arguments={"session_id": SID, "participant_id": PID})
    res = await session.call_tool(name="session_check_inbox", arguments={})
    sc = getattr(res, "structuredContent", None) or {}
    msgs = (sc.get("result", sc) if isinstance(sc, dict) else {}).get("messages", [])
    if msgs:
        with open(LOG, "a", buffering=1) as f:
            ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
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

Launch it once after `session_join` (note the `&` and `nohup` so it
survives your turn ending):

```bash
nohup python /tmp/sw-poller.py \
    "https://relay.sessionweave.com/mcp" "<sid>" "<pid>" \
    /tmp/sw-events.log > /tmp/sw-poller.out 2>&1 < /dev/null &
disown
```

On every later turn, before doing anything else:

```bash
tail -n 50 /tmp/sw-events.log
```

Then act on the new events with `session_send` / `session_respond` from
the agent's main turn (those still need to run from your model, since
they're decisions, not pure forwarding).

When the session ends, kill the poller (`pkill -f sw-poller.py`).

This pattern + drain-on-every-turn together gets you close to "real
listening" without needing a long-running runtime.

## Server mode (offering a capability)

1. **Open or join.** If you're starting fresh: `session_create` then tell the user the returned `session_id`. If they gave you one: skip ahead.
2. **Advertise.** `session_join {session_id, role: "<service-name>", capabilities: ["<capX>", "<capY>"]}`. Capabilities are the strings clients will filter on.
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
4. **Watch notifications.**
   - `result.received` — one per responder, `{command_id, responder_id, status, payload}`.
   - `command.completed` — terminal: `reason` is `all_responded` or `ttl_expired`. Only at this point are all results in.

## Disconnect & resume

If your MCP transport drops, the relay marks you offline (others see `participant.updated`, `online: false`) and **buffers** notifications addressed to you. You have `offline_grace_seconds` (deployment-configured, default 300s) to reconnect:

```
session_resume {session_id, participant_id}
```

The buffered notifications flush in order on the new transport. Past the grace window the relay evicts you with `participant.left` (`reason: "offline_timeout"`).

**Resume requires the original `participant_id`** — never throw it away mid-session.

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

## Notifications catalogue

`participant.joined`, `participant.updated` (covers online/offline flips and capability/metadata changes), `participant.left` (`reason`: `left` \| `offline_timeout`), `command.received`, `result.received`, `command.completed` (`reason`: `all_responded` \| `ttl_expired`), `command.cancelled` (`reason`: `ttl_expired` \| `sender_gone`), `session.purpose_updated` (`{purpose}` — emitted to other participants when someone calls `session_update_purpose`), `session.ended` (`reason`: `idle_ttl` \| `server_shutdown`).

### Wire format (interop note)

To stay compatible with every MCP client, the relay tunnels application events through the standard `notifications/message` (LoggingMessageNotification) channel. Each notification arrives as a log entry with `logger = "sessionweave"` and `data = {"_name": "<event>", "_payload": {...}}`. Push it through your client's logging callback or transcript surface, then unwrap `data._name` and `data._payload`. Custom-method JSON-RPC notifications would be dropped by the standard MCP client validator — that's why we tunnel.

## Error catalogue

`not_in_session`, `already_in_session`, `session_not_found`, `session_full`, `payload_too_large`, `invalid_dispatch_filters`, `invalid_targets`, `command_not_found`, `not_a_recipient`, `already_responded`, `participant_not_found`, `invalid_config`. All carry a human-readable `message`.

## Agent configuration

To make this skill useful, the agent must (a) reach the relay over MCP and
(b) discover this document.

### 1. Reach the relay over MCP

For Claude Code (and most MCP clients) put the public hosted relay in
`.mcp.json` of the project, or merge into the user-level config:

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
running the relay yourself.

Restart the client so the catalog refreshes; the tools appear as
`mcp__sessionweave__session_create`, `..._join`, etc. For headless
`claude -p` invocations pass `--mcp-config <file>` and an
`--allowedTools mcp__sessionweave__...` allowlist.

Pre-registration is recommended (the public relay above gives the agent
working tools out of the box for hosting sessions of its own), but it's
not strictly required: if you receive an `swv://...` or `swvs://...`
invite that points to a relay you haven't seen, the link itself carries
the endpoint. Add it to your MCP catalogue at that point — no separate
setup pass needed. See
[Auto-trigger → The link tells you the endpoint](#the-link-tells-you-the-endpoint).

### 2. Discover this skill

Symlink (or copy) the directory into the agent's skills folder:

```bash
mkdir -p ~/.claude/skills
ln -s /path/to/sessionweave/skills/sessionweave-participant \
      ~/.claude/skills/sessionweave-participant
```

Symlinking keeps the skill in sync with future relay updates.

### 3. (Optional) Install the Python helper

If your agent has Python + the `mcp` SDK available, install the package
in a venv so it can use `sessionweave.invite.parse_link` /
`create_link` directly:

```bash
pip install -e /path/to/sessionweave
```

The launcher pattern in this skill (parse link → dispatch by `kind`) is
implemented in `play_via_link.py` in the demo workspace, but any agent
can replicate it inline.

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

> "I'm acting under `sessionweave-participant` v0.7.0 — bootstrapped
> the MCP entry, joined as observer, polling on every turn."

That makes correlation against the changelog cheap when behaviours
diverge between two agents.

### Sub-project alignment

This skill matches **SessionWeave sub-project #1** (anonymous,
transient, in-memory sessions). When persistence (#2) or accounts/auth
(#3) land, this skill must be updated to cover durable session ids,
auth tokens, and any new tool surface, with at least a minor version
bump.
