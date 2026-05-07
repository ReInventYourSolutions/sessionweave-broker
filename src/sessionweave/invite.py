"""Invite link helpers.

A SessionWeave invite link is a compact, opaque pointer to a session:

    swv://<host>[:<port>]/<session_id>

The recipient calls ``session_describe`` against the relay to discover the
session's ``purpose`` (intent, role assignment, instructions). That payload
is persisted server-side, so it's the single source of truth — the link
itself stays minimal.

Legacy ``swv://...#<base64url-json>`` links carrying an embedded purpose
in the URL fragment are still parsed (``parse_link`` returns
``InviteLink.purpose``) for backward compatibility. New code should not
emit them; ``create_link`` no longer accepts a purpose argument.

The relay never sees the link — encoding/decoding is purely client-side.
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


def create_link(relay_url: str, session_id: str) -> str:
    """Build a compact ``swv://`` invite link.

    ``relay_url`` is the relay's HTTP MCP endpoint
    (e.g. ``http://127.0.0.1:8765/mcp``). The link's host and port mirror
    the relay's. To learn the session's purpose, the recipient calls
    ``session_describe`` against the relay — the link does not carry that
    payload.
    """
    if not session_id:
        raise ValueError("session_id is required")
    parsed = urlparse(relay_url)
    if not parsed.netloc:
        raise ValueError(f"relay_url missing host: {relay_url!r}")
    return f"{DEFAULT_SCHEME}://{parsed.netloc}/{session_id}"


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
