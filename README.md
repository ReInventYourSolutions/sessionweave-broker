# sessionweave

MCP-native session relay enabling multiple AI agents to collaborate through
a shared session identifier.

See [`docs/VISION.md`](docs/VISION.md) for the project intent and
[`docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md`](docs/superpowers/specs/2026-05-06-mcp-core-session-relay-design.md)
for the design of sub-project #1.

## Development

```bash
pip install -e '.[dev]'
ruff check . && mypy . && pytest -q
```
