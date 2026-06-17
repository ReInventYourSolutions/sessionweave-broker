"""Broker-hosted, offline-capable docs hub: API docs (Swagger UI over the
broker's own openapi.yaml), SDK docs, and the SDK source as a pip-installable
tarball.

All assets are bundled as package data (shipped in the wheel), so nothing here
touches the network — the air-gapped broker is the design target. Rendering
functions are pure; ``build_sdk_tarball`` / ``load_*`` / ``static_asset`` read
bundled resources.
"""
from __future__ import annotations

import io
import json
import tarfile
from importlib import resources

# Static assets reachable via GET /_static/<path>. Only these roots are served;
# everything else under _data (the skill, the SDK source) has its own route.
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

#: Top-level directory inside the served SDK tarball (pip extracts and builds it).
SDK_TARBALL_ROOT = "sessionweave-client"


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


def render_sdk_page(base_url: str, readme_md: str) -> str:
    """SDK docs page: an offline ``pip install`` line pinned to this broker's
    tarball, plus the README rendered client-side with the bundled marked.js."""
    base = base_url.rstrip("/")
    install_url = f"{base}/sdk/{SDK_TARBALL_ROOT}.tar.gz"
    readme_json = json.dumps(readme_md)
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
<p>Install it offline, straight from this broker:</p>
<pre><code>pip install {install_url}</code></pre>
<p>Then point the client at this broker:</p>
<pre><code>SessionWeaveClient(base_url="{base}")</code></pre>
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
    return _data().joinpath("sdk/README.md").read_text(encoding="utf-8")


def build_sdk_tarball() -> bytes:
    """Generate a pip-installable ``.tar.gz`` of the vendored SDK source.

    Every file lives under a single top-level ``sessionweave-client/`` directory
    with ``pyproject.toml`` at its root, so ``pip install <tarball>`` extracts and
    builds it like any source archive."""
    root = _data().joinpath("sdk")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        _add_tree(tar, root, SDK_TARBALL_ROOT)
    return buf.getvalue()


def _add_tree(tar: tarfile.TarFile, node: resources.abc.Traversable, arc: str) -> None:
    for child in sorted(node.iterdir(), key=lambda c: c.name):
        # Skip caches / hidden tooling dirs so the served tarball is just source.
        if child.name.startswith(".") or child.name == "__pycache__":
            continue
        if child.name.endswith(".pyc"):
            continue
        child_arc = f"{arc}/{child.name}"
        if child.is_dir():
            _add_tree(tar, child, child_arc)
        else:
            blob = child.read_bytes()
            info = tarfile.TarInfo(name=child_arc)
            info.size = len(blob)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(blob))


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
