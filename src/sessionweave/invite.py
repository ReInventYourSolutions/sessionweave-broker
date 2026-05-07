"""Invite link helpers.

A SessionWeave invite link is a single self-describing string that an agent
can read and act on without an out-of-band briefing. Schema:

    swv://<host>[:<port>]/<session_id>[#<base64url-json-purpose>]

- The fragment, when present, is base64url-encoded JSON describing the
  ``purpose`` of the session (intent, role assignment, free-form
  instructions).
- The relay HTTP URL is reconstructed as ``http://<host>[:<port>]/mcp``.
  Use ``relay_url=`` overrides if the relay is on a different path.

The relay never sees the link itself — encoding/decoding is purely
client-side. The ``purpose`` payload is also persisted on the relay (sent
through ``session_create``), so a recipient who joins the session by id
alone still sees the same purpose echoed back from the server.
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

DEFAULT_SCHEME = "swv"
DEFAULT_HTTP_PATH = "/mcp"


@dataclass(frozen=True, slots=True)
class InviteLink:
    """Decoded view of an ``swv://`` invite link."""

    relay_url: str  # full HTTP URL of the MCP endpoint, e.g. http://h:p/mcp
    session_id: str
    purpose: dict[str, Any] | None


def _b64url_encode(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    return base64.urlsafe_b64encode(blob).rstrip(b"=").decode("ascii")


def _b64url_decode(fragment: str) -> dict[str, Any]:
    padded = fragment + "=" * (-len(fragment) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    obj = json.loads(raw.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("invite fragment must decode to a JSON object")
    return obj


def create_link(
    relay_url: str,
    session_id: str,
    purpose: dict[str, Any] | None = None,
) -> str:
    """Build an ``swv://`` invite link.

    ``relay_url`` is the relay's HTTP MCP endpoint
    (e.g. ``http://127.0.0.1:8765/mcp``). The link's host and port mirror
    the relay's; the path falls back to the part before ``/mcp``.
    """
    if not session_id:
        raise ValueError("session_id is required")
    parsed = urlparse(relay_url)
    if not parsed.netloc:
        raise ValueError(f"relay_url missing host: {relay_url!r}")
    netloc = parsed.netloc
    fragment = ""
    if purpose:
        fragment = "#" + _b64url_encode(purpose)
    return f"{DEFAULT_SCHEME}://{netloc}/{session_id}{fragment}"


def parse_link(link: str) -> InviteLink:
    """Parse a string that may be an ``swv://`` invite link."""
    parsed = urlparse(link)
    if parsed.scheme != DEFAULT_SCHEME:
        raise ValueError(
            f"unexpected scheme {parsed.scheme!r}; expected {DEFAULT_SCHEME!r}"
        )
    if not parsed.netloc:
        raise ValueError("invite link missing host")
    session_id = parsed.path.lstrip("/")
    if not session_id or "/" in session_id:
        raise ValueError(
            f"invalid invite path {parsed.path!r}; expected '/<session_id>'"
        )
    purpose: dict[str, Any] | None = None
    if parsed.fragment:
        try:
            purpose = _b64url_decode(parsed.fragment)
        except Exception as exc:
            raise ValueError(f"invalid invite fragment: {exc}") from exc
    relay_url = f"http://{parsed.netloc}{DEFAULT_HTTP_PATH}"
    return InviteLink(relay_url=relay_url, session_id=session_id, purpose=purpose)


__all__ = ["DEFAULT_HTTP_PATH", "DEFAULT_SCHEME", "InviteLink", "create_link", "parse_link"]
