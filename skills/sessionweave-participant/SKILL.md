---
name: sessionweave-participant
description: Use when an agent must join, host, or test through a SessionWeave relay (MCP-based multi-agent rendezvous) — covers server mode (offering a capability) and client mode (dispatching commands across machines).
---

# SessionWeave participant

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

1. **Parse.** `from sessionweave.invite import parse_link; inv = parse_link(link)`
   → `inv.relay_url`, `inv.session_id`. (Legacy links may carry a
   fragment-encoded purpose; ``parse_link`` returns it as ``inv.purpose``
   for backward compat, but new code shouldn't rely on it.)
2. **Describe.** `session_describe {session_id: inv.session_id}` →
   returns `{config, purpose, roster}` without allocating a participant
   slot.
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

## Server mode (offering a capability)

1. **Open or join.** If you're starting fresh: `session_create` then tell the user the returned `session_id`. If they gave you one: skip ahead.
2. **Advertise.** `session_join {session_id, role: "<service-name>", capabilities: ["<capX>", "<capY>"]}`. Capabilities are the strings clients will filter on.
3. **Persist your `participant_id`.** You'll need it for `session_resume` on disconnect.
4. **Listen.** The relay sends `command.received` notifications: `{command_id, sender_id, dispatch_mode, dispatched_to, payload, expires_at}`.
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

## Notifications catalogue

`participant.joined`, `participant.updated` (covers online/offline flips and capability/metadata changes), `participant.left` (`reason`: `left` \| `offline_timeout`), `command.received`, `result.received`, `command.completed` (`reason`: `all_responded` \| `ttl_expired`), `command.cancelled` (`reason`: `ttl_expired` \| `sender_gone`), `session.ended` (`reason`: `idle_ttl` \| `server_shutdown`).

### Wire format (interop note)

To stay compatible with every MCP client, the relay tunnels application events through the standard `notifications/message` (LoggingMessageNotification) channel. Each notification arrives as a log entry with `logger = "sessionweave"` and `data = {"_name": "<event>", "_payload": {...}}`. Push it through your client's logging callback or transcript surface, then unwrap `data._name` and `data._payload`. Custom-method JSON-RPC notifications would be dropped by the standard MCP client validator — that's why we tunnel.

## Error catalogue

`not_in_session`, `already_in_session`, `session_not_found`, `session_full`, `payload_too_large`, `invalid_dispatch_filters`, `invalid_targets`, `command_not_found`, `not_a_recipient`, `already_responded`, `participant_not_found`, `invalid_config`. All carry a human-readable `message`.

## Agent configuration

To make this skill useful, the agent must (a) reach the relay over MCP and
(b) discover this document.

### 1. Reach the relay over MCP

For Claude Code (and most MCP clients) put the relay in `.mcp.json` of
the project, or merge into the user-level config:

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

Restart the client so the catalog refreshes; the tools appear as
`mcp__sessionweave__session_create`, `..._join`, etc. For headless
`claude -p` invocations pass `--mcp-config <file>` and an
`--allowedTools mcp__sessionweave__...` allowlist.

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

This skill matches **SessionWeave sub-project #1** (anonymous, transient, in-memory sessions). When persistence (#2) or accounts/auth (#3) land, this skill must be updated to cover durable session ids, auth tokens, and any new tool surface.
