from __future__ import annotations

from sessionweave.skill_serve import (
    load_canonical_skill,
    render_mcp_snippet,
    render_skill,
)

# A miniature stand-in for the canonical skill carrying every scheme family
# that appears in the real SKILL.md, plus a bare-host prose mention.
SAMPLE = (
    "Default relay: https://relay.sessionweave.com\n"
    "WS: wss://relay.sessionweave.com/v1/sessions/x/events\n"
    "Compact: swvs://relay.sessionweave.com/sid\n"
    "Allowed hosts include relay.sessionweave.com in prose.\n"
)


def test_https_with_port_rewrites_all_families() -> None:
    out = render_skill(SAMPLE, "https://broker.internal:9000")
    assert "https://broker.internal:9000" in out
    assert "wss://broker.internal:9000" in out
    assert "swvs://broker.internal:9000" in out
    assert "relay.sessionweave.com" not in out


def test_http_non_tls_downgrades_scheme_families() -> None:
    out = render_skill(SAMPLE, "http://127.0.0.1:8080")
    assert "http://127.0.0.1:8080" in out
    assert "ws://127.0.0.1:8080" in out
    assert "swv://127.0.0.1:8080" in out
    # the TLS-flavoured schemes must NOT survive against a plain-HTTP broker
    assert "wss://127.0.0.1:8080" not in out
    assert "swvs://127.0.0.1:8080" not in out
    assert "relay.sessionweave.com" not in out


def test_public_base_is_a_noop() -> None:
    # When the broker IS the public relay, the served skill equals canonical.
    out = render_skill(SAMPLE, "https://relay.sessionweave.com")
    assert out == SAMPLE


def test_bare_host_prose_is_rewritten() -> None:
    out = render_skill(SAMPLE, "https://broker.internal:9000")
    assert "relay include broker.internal:9000" not in out  # sanity: no mangling
    assert "broker.internal:9000 in prose" in out


def test_render_mcp_snippet_points_at_broker_mcp() -> None:
    snip = render_mcp_snippet("https://broker.internal:9000")
    assert snip["mcpServers"]["sessionweave"]["url"] == (
        "https://broker.internal:9000/mcp"
    )
    assert snip["mcpServers"]["sessionweave"]["type"] == "http"


def test_render_mcp_snippet_strips_trailing_slash() -> None:
    snip = render_mcp_snippet("https://broker.internal:9000/")
    assert snip["mcpServers"]["sessionweave"]["url"] == (
        "https://broker.internal:9000/mcp"
    )


def test_load_canonical_skill_returns_the_bundled_skill() -> None:
    md = load_canonical_skill()
    assert "sessionweave-participant" in md
    assert "Default relay" in md


def test_render_mcp_snippet_account_mode_adds_header_placeholder() -> None:
    entry = render_mcp_snippet("https://b.example", account_mode=True)["mcpServers"][
        "sessionweave"
    ]
    assert entry["headers"] == {"Authorization": "Bearer ${SW_ACCOUNT_TOKEN}"}


def test_render_mcp_snippet_open_mode_has_no_headers() -> None:
    entry = render_mcp_snippet("https://b.example")["mcpServers"]["sessionweave"]
    assert "headers" not in entry
