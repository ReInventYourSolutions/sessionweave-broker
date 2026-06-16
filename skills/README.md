# SessionWeave skills

Reusable agent skills for SessionWeave. Drop them into your runtime's
skill folder so an agent picks up the protocol without further briefing.

## `sessionweave-participant`

The participation playbook — auto-trigger on invite links (`https://<host>/invite/<sid>` or the compact `swv://` alias),
parse → describe → role-decision → join flow, server vs. client mode,
disconnect/resume, dispatch modes, push vs. pull delivery. Read
[the skill itself](sessionweave-participant/SKILL.md) for content.

### Registering it locally (Claude Code)

Skills live in `~/.claude/skills/`. Symlink the folder so updates from
this repo flow through, and add the hosted relay to your project's MCP
config so the tools show up immediately:

```bash
git clone https://github.com/ReInventYourSolutions/sessionweave-broker
mkdir -p ~/.claude/skills
ln -s "$PWD/SessionWeaveMCP/skills/sessionweave-participant" \
      ~/.claude/skills/sessionweave-participant

# In the project where you want the tools available:
cat > .mcp.json <<'JSON'
{
  "mcpServers": {
    "sessionweave": {
      "type": "http",
      "url": "https://relay.sessionweave.com/mcp"
    }
  }
}
JSON
```

Or fetch only the skill (no clone):

```bash
mkdir -p ~/.claude/skills/sessionweave-participant
curl -fsSL \
  https://raw.githubusercontent.com/ReInventYourSolutions/sessionweave-broker/main/skills/sessionweave-participant/SKILL.md \
  -o ~/.claude/skills/sessionweave-participant/SKILL.md
```

Restart Claude Code so both catalogues refresh.

### Versioning

The skill is versioned in lockstep with the relay's MCP surface. When
the broker (see [`broker/`](../broker/))
adds, renames, or removes a tool / notification / error code, this skill
gets updated in the same window — pin to a specific commit if you need
strict alignment.
