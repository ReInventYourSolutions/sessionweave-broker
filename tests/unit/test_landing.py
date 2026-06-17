from __future__ import annotations

from sessionweave.landing import render_landing


def test_landing_shows_address_version_and_artefacts() -> None:
    html = render_landing(
        base_url="https://broker.internal:9000", version="1.2.3", relay_epoch=42
    )
    assert "broker.internal:9000" in html
    assert "1.2.3" in html
    assert "/skill.md" in html
    assert "/connect.mcp.json" in html
    assert "https://broker.internal:9000/mcp" in html


def test_landing_strips_trailing_slash_and_escapes_version() -> None:
    html = render_landing(
        base_url="https://broker.internal:9000/", version="<x>", relay_epoch=0
    )
    assert "broker.internal:9000//mcp" not in html
    assert "<x>" not in html  # escaped
    assert "&lt;x&gt;" in html
