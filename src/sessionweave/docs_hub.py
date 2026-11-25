"""Broker-hosted, offline-capable docs hub: API docs (Swagger UI over the
broker's own openapi.yaml) and the Python SDK docs.

The API docs assets are bundled as package data (shipped in the wheel), so they
work on an air-gapped broker. The SDK is not bundled: the broker depends on the
``sessionweave-sdk`` distribution, renders its README from the installed
package metadata and points ``pip install`` at the same version on PyPI.
Rendering functions are pure; ``load_*`` / ``sdk_version`` / ``static_asset``
read installed resources.
"""
from __future__ import annotations

import html
import json
from importlib import metadata, resources

# Static assets reachable via GET /_static/<path>. Only these roots are served;
# everything else under _data (the skill) has its own route.
_ALLOWED_ASSET_ROOTS = ("swagger-ui", "vendor")

_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".map": "application/json; charset=utf-8",
    ".png": "image/png",
    ".svg": "image/svg+xml",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".html": "text/html; charset=utf-8",
}

#: The Python SDK distribution the broker depends on (import package
#: ``sessionweave_client``).
SDK_DISTRIBUTION = "sessionweave-sdk"

#: Legacy route that served the vendored SDK source (broker < 0.35); it now
#: redirects to the matching sdist on PyPI, so old ``pip install`` lines work.
SDK_LEGACY_TARBALL_PATH = "/sdk/sessionweave-client.tar.gz"


def _data() -> resources.abc.Traversable:
    return resources.files("sessionweave").joinpath("_data")


def render_api_page() -> str:
    """Swagger UI page rendering the broker's own spec, fully offline.

    Spec and assets are referenced by root-relative URLs (the broker is mounted
    at the host root, same as the 0.24.0 landing's links)."""
    return """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SessionWeave relay — API</title>
<link rel="stylesheet" href="/_static/swagger-ui/swagger-ui.css">
<style>body { margin: 0; }</style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="/_static/swagger-ui/swagger-ui-bundle.js"></script>
<script>
  window.ui = SwaggerUIBundle({
    url: "/openapi.yaml",
    dom_id: "#swagger-ui",
    deepLinking: true,
  });
</script>
</body>
</html>
"""


def sdk_version() -> str:
    """Version of the installed ``sessionweave-sdk`` distribution.

    Raises ``importlib.metadata.PackageNotFoundError`` when it is missing."""
    return metadata.version(SDK_DISTRIBUTION)


def sdk_install_line(version: str) -> str:
    return f"pip install {SDK_DISTRIBUTION}=={version}"


def sdk_sdist_url(version: str) -> str:
    """PyPI download URL of the SDK sdist for ``version``.

    ``files.pythonhosted.org/packages/source/<initial>/<project>/<file>`` is
    PyPI's stable redirect to the real file, so it needs no hash."""
    filename = f"{SDK_DISTRIBUTION.replace('-', '_')}-{version}.tar.gz"
    return (
        "https://files.pythonhosted.org/packages/source/"
        f"{SDK_DISTRIBUTION[0]}/{SDK_DISTRIBUTION}/{filename}"
    )


def render_sdk_page(base_url: str, readme_md: str, version: str) -> str:
    """SDK docs page: the ``pip install`` line pinned to the SDK version this
    broker runs with, plus the README rendered client-side with the bundled
    marked.js."""
    base = base_url.rstrip("/")
    # base_url reflects the Host header — escape before interpolating into HTML
    # (attribute + text contexts), mirroring landing.py's convention.
    base_h = html.escape(base, quote=True)
    install_h = html.escape(sdk_install_line(version), quote=True)
    # README is trusted repo content, but it lands inside a <script> string —
    # neutralise any "</script>" so it can't break out of the JS context.
    readme_json = json.dumps(readme_md).replace("</", "<\\/")
    return f"""\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SessionWeave relay — SDK</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font: 15px/1.6 system-ui, sans-serif; max-width: 820px; margin: 0 auto;
         padding: 2rem 1.25rem 4rem; }}
  pre {{ background: color-mix(in srgb, currentColor 8%, transparent);
         padding: .75rem 1rem; border-radius: 8px; overflow-x: auto; }}
  code {{ font-family: ui-monospace, monospace; }}
  a {{ color: inherit; }}
</style>
</head>
<body>
<h1>SessionWeave Python SDK</h1>
<p>Install the version this broker runs with (the distribution is
<code>sessionweave-sdk</code>, the import package <code>sessionweave_client</code>):</p>
<pre><code>{install_h}</code></pre>
<p>Then point the client at this broker:</p>
<pre><code>SessionWeaveClient(base_url="{base_h}")</code></pre>
<hr>
<article id="readme">Loading README…</article>
<script src="/_static/vendor/marked.min.js"></script>
<script>
  document.getElementById("readme").innerHTML = marked.parse({readme_json});
</script>
</body>
</html>
"""


def load_sdk_readme() -> str:
    """README of the installed ``sessionweave-sdk`` (its PyPI long description).

    Raises ``importlib.metadata.PackageNotFoundError`` when the SDK is missing
    and ``LookupError`` when its metadata carries no description."""
    meta = metadata.metadata(SDK_DISTRIBUTION)
    body = meta.get_payload()  # type: ignore[attr-defined]
    if isinstance(body, str) and body.strip():
        return body
    description = meta.get("Description")
    if description:
        return str(description)
    raise LookupError(f"{SDK_DISTRIBUTION} metadata has no description")


def static_asset(rel_path: str) -> tuple[bytes, str]:
    """Return ``(bytes, content_type)`` for a bundled static asset.

    Rejects path traversal and anything outside the allowed asset roots, so a
    crafted ``/_static/../skill.md`` cannot escape ``_data/swagger-ui|vendor``.
    """
    parts = rel_path.split("/")
    if not parts or parts[0] not in _ALLOWED_ASSET_ROOTS:
        raise FileNotFoundError(rel_path)
    if any(p in ("", "..", ".") for p in parts):
        raise PermissionError(rel_path)

    node = _data()
    for part in parts:
        node = node.joinpath(part)
    if not node.is_file():
        raise FileNotFoundError(rel_path)

    ext = rel_path[rel_path.rfind(".") :] if "." in rel_path else ""
    ctype = _CONTENT_TYPES.get(ext, "application/octet-stream")
    return node.read_bytes(), ctype
