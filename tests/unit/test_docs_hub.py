from __future__ import annotations

import io
import tarfile

import pytest

from sessionweave.docs_hub import (
    build_sdk_tarball,
    load_sdk_readme,
    render_api_page,
    render_sdk_page,
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


def test_render_sdk_page_pins_install_to_broker_and_embeds_readme() -> None:
    html = render_sdk_page(BASE, "# SessionWeave SDK\ninstall me")
    assert f"pip install {BASE}/sdk/sessionweave-client.tar.gz" in html
    assert "/_static/vendor/marked.min.js" in html
    assert "SessionWeave SDK" in html  # README content embedded
    assert "jsdelivr" not in html


def test_build_sdk_tarball_is_a_valid_pip_installable_archive() -> None:
    blob = build_sdk_tarball()
    tf = tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz")
    names = tf.getnames()
    assert "sessionweave-client/pyproject.toml" in names
    assert "sessionweave-client/src/sessionweave_client/__init__.py" in names


def test_load_sdk_readme_returns_bundled_readme() -> None:
    md = load_sdk_readme()
    assert "sessionweave" in md.lower()


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
