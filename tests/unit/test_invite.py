from __future__ import annotations

import pytest

from sessionweave.invite import InviteLink, create_link, parse_link


def test_create_returns_compact_link() -> None:
    link = create_link("http://127.0.0.1:8765/mcp", "abc-1234")
    assert link == "swv://127.0.0.1:8765/abc-1234"


def test_parse_compact_link() -> None:
    parsed = parse_link("swv://example.com:9000/sess-xyz")
    assert parsed == InviteLink(
        relay_url="http://example.com:9000/mcp",
        session_id="sess-xyz",
        purpose=None,
    )


def test_create_rejects_blank_session() -> None:
    with pytest.raises(ValueError):
        create_link("http://h:1/mcp", "")


def test_create_rejects_relay_without_host() -> None:
    with pytest.raises(ValueError):
        create_link("/mcp", "sid")


def test_parse_rejects_wrong_scheme() -> None:
    with pytest.raises(ValueError):
        parse_link("http://h:1/sid")


def test_parse_rejects_path_with_extra_segments() -> None:
    with pytest.raises(ValueError):
        parse_link("swv://h:1/sid/extra")


def test_parse_legacy_fragment_still_decodes_purpose() -> None:
    """Forward-compat: links emitted before the compact-link change carried
    the purpose in the fragment. parse_link must still recover it so old
    links keep working — only ``create_link`` stops emitting fragments.
    """
    legacy = (
        "swv://h:1/sid"
        "#eyJraW5kIjoidGljLXRhYy10b2UtdjEiLCJ5b3VyX3JvbGUiOiJPIn0"
    )
    parsed = parse_link(legacy)
    assert parsed.purpose == {"kind": "tic-tac-toe-v1", "your_role": "O"}


def test_parse_rejects_invalid_fragment() -> None:
    with pytest.raises(ValueError):
        parse_link("swv://h:1/sid#not-base64-json!!!")
