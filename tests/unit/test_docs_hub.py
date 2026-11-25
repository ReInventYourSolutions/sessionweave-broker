from __future__ import annotations

import pytest

from sessionweave.docs_hub import (
    load_sdk_readme,
    render_api_page,
    render_sdk_page,
    sdk_sdist_url,
    sdk_version,
    static_asset,
)

BASE = "https://broker.internal:9000"


def test_render_api_page_is_offline_and_points_at_local_spec() -> None:
    html = render_api_page()
    assert "SwaggerUIBundle" in html
    assert "/openapi.yaml" in html
    assert "/_static/swagger-ui/swagger-ui.css" in html
    assert "/_static/swagger-ui/swagger-ui-bundle.js" in html
    # nothing loaded from a CDN
    assert "jsdelivr" not in html
    assert "unpkg" not in html
    assert "https://cdn" not in html


def test_render_sdk_page_pins_install_to_sdk_version_and_embeds_readme() -> None:
    html = render_sdk_page(BASE, "# SessionWeave SDK\ninstall me", "0.13.0")
    assert "pip install sessionweave-sdk==0.13.0" in html
    assert f'SessionWeaveClient(base_url="{BASE}")' in html
    assert "/_static/vendor/marked.min.js" in html
    assert "SessionWeave SDK" in html  # README content embedded
    assert "jsdelivr" not in html


def test_render_sdk_page_escapes_malicious_base_url() -> None:
    # base_url reflects the Host header; a crafted host must not break out of
    # the HTML attribute / text context (reflected XSS).
    evil = 'https://x"/><script>alert(1)</script>'
    html = render_sdk_page(evil, "# readme", "0.13.0")
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_render_sdk_page_readme_cannot_break_out_of_script() -> None:
    html = render_sdk_page(BASE, "stuff </script><script>alert(1)</script>", "0.13.0")
    # the closing tag from the README must be neutralised inside the JS string
    assert "</script><script>alert(1)" not in html


def test_sdk_sdist_url_points_at_pypi() -> None:
    assert sdk_sdist_url("0.13.0") == (
        "https://files.pythonhosted.org/packages/source/s/sessionweave-sdk/"
        "sessionweave_sdk-0.13.0.tar.gz"
    )


def test_sdk_version_is_the_installed_distribution() -> None:
    from importlib import metadata

    assert sdk_version() == metadata.version("sessionweave-sdk")


def test_load_sdk_readme_returns_installed_readme() -> None:
    md = load_sdk_readme()
    assert "sessionweave_client" in md


def test_static_asset_serves_known_assets_with_content_type() -> None:
    body, ctype = static_asset("swagger-ui/swagger-ui.css")
    assert body.startswith(b".swagger-ui") or b"swagger-ui" in body[:200]
    assert ctype.startswith("text/css")
    body, ctype = static_asset("vendor/marked.min.js")
    assert ctype.startswith(("application/javascript", "text/javascript"))


@pytest.mark.parametrize(
    "bad",
    [
        "../skill.md",
        "swagger-ui/../../skill.md",
        "/etc/passwd",
        "sdk/pyproject.toml",   # outside the allowed asset roots
        "skill.md",
    ],
)
def test_static_asset_rejects_traversal_and_non_asset_paths(bad: str) -> None:
    with pytest.raises((FileNotFoundError, ValueError, PermissionError)):
        static_asset(bad)
