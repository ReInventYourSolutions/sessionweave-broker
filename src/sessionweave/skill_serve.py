"""Serve the participant skill and MCP snippet parametrized on the broker's
own reachability address.

Pure functions only (no I/O) for ``render_skill`` / ``render_mcp_snippet`` so
they are trivially testable; ``load_canonical_skill`` reads the bundled skill
shipped as package data.

The canonical skill hardcodes the public broker host ``broker.sessionweave.com``.
A private broker rewrites every occurrence to its own ``host[:port]``, adjusting
the scheme families (``https``/``wss``/``swvs`` → ``http``/``ws``/``swv``) when
the broker is plain-HTTP, so copy-paste examples work against the private host.
"""
from __future__ import annotations

from importlib import resources
from typing import Any
from urllib.parse import urlparse

#: The host token the canonical skill hardcodes.
CANONICAL_HOST = "broker.sessionweave.com"

#: Pre-move hostname; old skill copies may still carry it, so it is rewritten too.
LEGACY_HOST = "relay.sessionweave.com"


def render_skill(canonical_md: str, base_url: str) -> str:
    """Rewrite the canonical skill so every relay reference names ``base_url``.

    Scheme-qualified forms are replaced before the bare host so the bare-host
    pass only mops up leftover prose mentions. When ``base_url`` already names
    the public relay the result equals ``canonical_md`` (every replacement is a
    no-op).
    """
    parsed = urlparse(base_url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    if not netloc:
        raise ValueError(f"base_url missing host: {base_url!r}")

    tls = scheme == "https"
    ws_scheme = "wss" if tls else "ws"
    swv_scheme = "swvs" if tls else "swv"

    # Ordered: scheme-qualified first, bare host last.
    replacements: list[tuple[str, str]] = []
    for host in (CANONICAL_HOST, LEGACY_HOST):
        replacements += [
            (f"https://{host}", f"{scheme}://{netloc}"),
            (f"wss://{host}", f"{ws_scheme}://{netloc}"),
            (f"swvs://{host}", f"{swv_scheme}://{netloc}"),
        ]
    replacements += [(CANONICAL_HOST, netloc), (LEGACY_HOST, netloc)]
    out = canonical_md
    for old, new in replacements:
        out = out.replace(old, new)
    return out


def render_mcp_snippet(base_url: str, *, account_mode: bool = False) -> dict[str, Any]:
    """Return a ready ``.mcp.json`` registration snippet for this broker.

    In account mode the entry also carries the Authorization header that
    sends the caller's account token (read from ``SW_ACCOUNT_TOKEN``).
    """
    base = base_url.rstrip("/")
    entry: dict[str, Any] = {"type": "http", "url": f"{base}/mcp"}
    if account_mode:
        entry["headers"] = {"Authorization": "Bearer ${SW_ACCOUNT_TOKEN}"}
    return {"mcpServers": {"sessionweave": entry}}


def load_canonical_skill() -> str:
    """Read the canonical participant skill bundled as package data."""
    return (
        resources.files("sessionweave")
        .joinpath("_data/skill.md")
        .read_text(encoding="utf-8")
    )
