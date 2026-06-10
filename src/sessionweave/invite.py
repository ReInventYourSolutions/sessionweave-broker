"""Invite link helpers.

A SessionWeave invite link is a compact, opaque pointer to a session:

    swv://<host>[:<port>]/<session_id>     (HTTP relay)
    swvs://<host>[:<port>]/<session_id>    (HTTPS relay)

The recipient calls ``session_describe`` against the relay to discover the
session's ``purpose`` (intent, role assignment, instructions). That payload
is persisted server-side, so it's the single source of truth — the link
itself stays minimal.

The two schemes mirror ``http://`` vs ``https://``:

- ``swv://h:p/sid``  → relay URL is ``http://h:p/mcp``  (dev / loopback)
- ``swvs://h[:p]/sid`` → relay URL is ``https://h[:p]/mcp`` (production)

``create_link`` picks the scheme automatically from the ``relay_url`` you
pass in. ``parse_link`` accepts either scheme.

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

SCHEME_HTTP = "swv"
SCHEME_HTTPS = "swvs"
_VALID_SCHEMES = (SCHEME_HTTP, SCHEME_HTTPS)
DEFAULT_HTTP_PATH = "/mcp"

# Re-exported for callers that pinned the old constant name. The default
# meaning is "the HTTP-flavoured invite scheme".
DEFAULT_SCHEME = SCHEME_HTTP


@dataclass(frozen=True, slots=True)
class InviteLink:
    """Decoded view of an ``swv://`` / ``swvs://`` invite link."""

    relay_url: str  # full URL of the MCP endpoint, e.g. https://h/mcp
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


def _scheme_for(http_scheme: str) -> str:
    """Map an HTTP relay scheme to the corresponding invite scheme."""
    if http_scheme == "https":
        return SCHEME_HTTPS
    if http_scheme == "http":
        return SCHEME_HTTP
    raise ValueError(
        f"relay_url must use http or https scheme, got {http_scheme!r}"
    )


def create_link(relay_url: str, session_id: str) -> str:
    """Build a compact invite link from the relay's HTTP(S) MCP endpoint.

    Scheme mapping:

    - ``http://h:p/mcp``  →  ``swv://h:p/<sid>``
    - ``https://h[:p]/mcp`` →  ``swvs://h[:p]/<sid>``

    The recipient learns the session's purpose by calling
    ``session_describe`` against the relay — the link does not carry it.
    """
    if not session_id:
        raise ValueError("session_id is required")
    parsed = urlparse(relay_url)
    if not parsed.netloc:
        raise ValueError(f"relay_url missing host: {relay_url!r}")
    invite_scheme = _scheme_for(parsed.scheme)
    netloc = parsed.netloc
    # Strip the default port for HTTPS so links stay tidy on hosted relays.
    if invite_scheme == SCHEME_HTTPS and netloc.endswith(":443"):
        netloc = netloc[: -len(":443")]
    elif invite_scheme == SCHEME_HTTP and netloc.endswith(":80"):
        netloc = netloc[: -len(":80")]
    return f"{invite_scheme}://{netloc}/{session_id}"


def http_invite_url(relay_url: str, session_id: str) -> str:
    """Directly-fetchable invite URL: ``<scheme>://<host>/invite/<sid>``.

    Unlike ``create_link`` (the compact ``swv://`` alias) this URL can be GET-ed by any
    HTTP client to retrieve the participation brief. Derived from the relay's HTTP base.

    - ``https://h[:p]/mcp`` → ``https://h[:p]/invite/<sid>``
    - ``http://h:p/mcp``   → ``http://h:p/invite/<sid>``
    """
    if not session_id:
        raise ValueError("session_id is required")
    parsed = urlparse(relay_url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"relay_url must use http or https, got {parsed.scheme!r}")
    if not parsed.netloc:
        raise ValueError(f"relay_url missing host: {relay_url!r}")
    return f"{parsed.scheme}://{parsed.netloc}/invite/{session_id}"


def parse_link(link: str) -> InviteLink:
    """Parse a string that may be an ``swv://`` or ``swvs://`` invite link."""
    parsed = urlparse(link)
    if parsed.scheme not in _VALID_SCHEMES:
        raise ValueError(
            f"unexpected scheme {parsed.scheme!r}; "
            f"expected one of {_VALID_SCHEMES}"
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
    http_scheme = "https" if parsed.scheme == SCHEME_HTTPS else "http"
    relay_url = f"{http_scheme}://{parsed.netloc}{DEFAULT_HTTP_PATH}"
    return InviteLink(relay_url=relay_url, session_id=session_id, purpose=purpose)


__all__ = [
    "DEFAULT_HTTP_PATH",
    "DEFAULT_SCHEME",
    "SCHEME_HTTP",
    "SCHEME_HTTPS",
    "InviteLink",
    "create_link",
    "http_invite_url",
    "parse_link",
]
