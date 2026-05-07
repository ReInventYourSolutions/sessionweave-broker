from __future__ import annotations

import pytest

from sessionweave.invite import InviteLink, create_link, parse_link


def test_http_relay_emits_swv_scheme() -> None:
    link = create_link("http://127.0.0.1:8765/mcp", "abc-1234")
    assert link == "swv://127.0.0.1:8765/abc-1234"


def test_https_relay_emits_swvs_scheme_and_strips_default_port() -> None:
    link = create_link("https://relay.sessionweave.com/mcp", "sid-1")
    assert link == "swvs://relay.sessionweave.com/sid-1"
    # Explicit :443 also gets stripped for tidy hosted-relay links.
    link2 = create_link("https://relay.sessionweave.com:443/mcp", "sid-2")
    assert link2 == "swvs://relay.sessionweave.com/sid-2"


def test_https_relay_keeps_non_default_port() -> None:
    link = create_link("https://relay.example.com:9443/mcp", "s")
    assert link == "swvs://relay.example.com:9443/s"


def test_http_relay_strips_default_port_80() -> None:
    link = create_link("http://example.com:80/mcp", "sid")
    assert link == "swv://example.com/sid"


def test_parse_compact_swv_link() -> None:
    parsed = parse_link("swv://example.com:9000/sess-xyz")
    assert parsed == InviteLink(
        relay_url="http://example.com:9000/mcp",
        session_id="sess-xyz",
        purpose=None,
    )


def test_parse_compact_swvs_link() -> None:
    parsed = parse_link("swvs://relay.sessionweave.com/sess-xyz")
    assert parsed == InviteLink(
        relay_url="https://relay.sessionweave.com/mcp",
        session_id="sess-xyz",
        purpose=None,
    )


def test_round_trip_https() -> None:
    relay = "https://relay.sessionweave.com/mcp"
    link = create_link(relay, "abc-1234")
    parsed = parse_link(link)
    assert parsed.relay_url == relay
    assert parsed.session_id == "abc-1234"


def test_create_rejects_blank_session() -> None:
    with pytest.raises(ValueError):
        create_link("http://h:1/mcp", "")


def test_create_rejects_relay_without_host() -> None:
    with pytest.raises(ValueError):
        create_link("/mcp", "sid")


def test_create_rejects_unknown_scheme() -> None:
    with pytest.raises(ValueError):
        create_link("ftp://h:1/mcp", "sid")


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
