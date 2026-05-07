# SessionWeave skills

Reusable Claude Code skills that ship with the relay.

## `sessionweave-participant`

Protocol playbook for an agent participating in a SessionWeave session — covers
both server mode (offering a capability) and client mode (dispatching
commands). Read [the skill itself](sessionweave-participant/SKILL.md) for
content.

### Registering it locally

Claude Code auto-discovers skills in `~/.claude/skills/`. Symlink (or copy)
this directory in:

```bash
mkdir -p ~/.claude/skills
ln -s "$PWD/skills/sessionweave-participant" ~/.claude/skills/sessionweave-participant
```

The symlink keeps the skill in sync with the repo (so updates land for free
when this project ships changes).

### Versioning

The skill is versioned in lockstep with the relay's MCP surface. When a
sub-project (persistence, auth, dashboard, …) changes the tool catalogue or
session semantics, update [`sessionweave-participant/SKILL.md`](sessionweave-participant/SKILL.md)
in the same PR.
