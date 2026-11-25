"""`.env.example` lists every broker setting the server reads from the environment."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# Variables read only by the client-side CLIs (sessionweave-host / -poll /
# -accounts), not by the broker process.
CLIENT_ONLY = {"SW_URL", "SW_PARTICIPANT_TOKEN", "SW_ACCOUNT_TOKEN"}


def test_env_example_lists_every_broker_variable() -> None:
    src = ROOT / "src" / "sessionweave"
    read: set[str] = set()
    for path in src.rglob("*.py"):
        read |= set(re.findall(r"\"(SW_[A-Z0-9_]+)\"", path.read_text(encoding="utf-8")))
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    listed = set(re.findall(r"^(SW_[A-Z0-9_]+)=", example, flags=re.M))
    missing = sorted(read - CLIENT_ONLY - listed)
    assert not missing, f".env.example misses: {missing}"
