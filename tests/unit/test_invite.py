from __future__ import annotations

import pytest

from sessionweave.invite import InviteLink, create_link, parse_link


def test_round_trip_with_purpose() -> None:
    purpose = {
        "kind": "tic-tac-toe-v1",
        "your_role": "O",
        "your_capabilities": ["tictactoe-o"],
        "instructions": "X moves first. Win → block → center → corner → edge.",
    }
    link = create_link(
        "http://127.0.0.1:8765/mcp",
        "abc-1234",
        purpose,
    )
    assert link.startswith("swv://127.0.0.1:8765/abc-1234#")
    parsed = parse_link(link)
    assert parsed == InviteLink(
        relay_url="http://127.0.0.1:8765/mcp",
        session_id="abc-1234",
        purpose=purpose,
    )


def test_link_without_purpose_has_no_fragment() -> None:
    link = create_link("http://example.com:9000/mcp", "sess-xyz", None)
    assert link == "swv://example.com:9000/sess-xyz"
    parsed = parse_link(link)
    assert parsed.purpose is None
    assert parsed.session_id == "sess-xyz"
    assert parsed.relay_url == "http://example.com:9000/mcp"


def test_create_rejects_blank_session() -> None:
    with pytest.raises(ValueError):
        create_link("http://h:1/mcp", "", {"k": "v"})


def test_create_rejects_relay_without_host() -> None:
    with pytest.raises(ValueError):
        create_link("/mcp", "sid", None)


def test_parse_rejects_wrong_scheme() -> None:
    with pytest.raises(ValueError):
        parse_link("http://h:1/sid")


def test_parse_rejects_path_with_extra_segments() -> None:
    with pytest.raises(ValueError):
        parse_link("swv://h:1/sid/extra")


def test_parse_rejects_invalid_fragment() -> None:
    with pytest.raises(ValueError):
        parse_link("swv://h:1/sid#not-base64-json!!!")


def test_purpose_with_unicode_round_trips() -> None:
    purpose = {"instructions": "Giochiamo a tris — tu sei O 🎲"}
    link = create_link("http://h:1/mcp", "uni", purpose)
    assert parse_link(link).purpose == purpose


def test_fragment_omits_padding_chars() -> None:
    # base64 padding chars (=) are stripped from URL fragments to keep them
    # tidy; parsing must restore them transparently.
    link = create_link("http://h:1/mcp", "p", {"a": 1})
    fragment = link.split("#", 1)[1]
    assert "=" not in fragment
    assert parse_link(link).purpose == {"a": 1}
