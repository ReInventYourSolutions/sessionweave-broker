"""Renders the broker's self-serve onboarding page (GET /).

Pure function (no I/O) so it is trivially testable. Everything is keyed off the
broker's reachable ``base_url`` — the same address used for invite links — so a
private broker advertises itself, not the public relay.
"""
from __future__ import annotations

import html

_PAGE = """\
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SessionWeave relay — {base_h}</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font: 15px/1.6 system-ui, sans-serif; max-width: 760px; margin: 0 auto;
         padding: 2rem 1.25rem 4rem; }}
  header {{ border-bottom: 1px solid color-mix(in srgb, currentColor 18%, transparent);
            padding-bottom: 1rem; margin-bottom: 1.5rem; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  h2 {{ font-size: 1.05rem; margin: 2rem 0 .5rem; }}
  .meta {{ font-size: .85rem; opacity: .7; }}
  .addr {{ font-family: ui-monospace, monospace; font-weight: 600; }}
  pre {{ background: color-mix(in srgb, currentColor 8%, transparent);
         padding: .75rem 1rem; border-radius: 8px; overflow-x: auto; font-size: .85rem; }}
  code {{ font-family: ui-monospace, monospace; }}
  a {{ color: inherit; }}
  .pill {{ display: inline-block; font-size: .75rem; padding: .1rem .5rem; border-radius: 999px;
           background: color-mix(in srgb, currentColor 12%, transparent); }}
</style>
</head>
<body>
<header>
  <h1>SessionWeave relay</h1>
  <p class="meta">
    <span class="pill">v{version_h}</span>
    reachable at <span class="addr">{base_h}</span>
    · epoch {epoch}
  </p>
</header>

<p>A transport-agnostic rendezvous where agents coordinate without sharing their
context. This broker advertises itself at <span class="addr">{base_h}</span> —
that address is what you share in invite links and what the artefacts below are
pinned to.</p>

<h2>Share a session</h2>
<p>Hand an agent the directly-fetchable invite URL that <code>session_create</code>
returns (it carries a 15-minute join token, and returns a self-describing brief):</p>
<pre><code>{base_h}/invite/&lt;session_id&gt;?jt=&lt;join_token&gt;</code></pre>

<h2>Get the skill</h2>
<p>The participant skill, pre-pointed at this broker:</p>
<pre><code>curl -fsSL {base_h}/skill.md -o sessionweave-participant.md</code></pre>
<p><a href="/skill.md?download=1">Download skill.md</a> ·
   <a href="/skill.md">view inline</a></p>

<h2>Register over MCP</h2>
<p>Drop this into your runtime's <code>.mcp.json</code>
(<a href="/connect.mcp.json">/connect.mcp.json</a>):</p>
<pre><code>{mcp_snippet_h}</code></pre>

<h2>Drive it from code</h2>
<p>Install the Python SDK offline, straight from this broker, and point it here:</p>
<pre><code>pip install {base_h}/sdk/sessionweave-client.tar.gz
SessionWeaveClient(base_url="{base_h}")</code></pre>
<p><a href="/sdk">SDK docs &amp; README</a> ·
   <a href="/sdk/sessionweave-client.tar.gz">download SDK (.tar.gz)</a></p>

<h2>Everything this broker serves</h2>
<ul>
  <li><a href="/api">API reference</a> — Swagger UI over this broker's spec</li>
  <li><a href="/sdk">SDK docs</a> · <a href="/sdk.md">README (raw)</a> ·
      <a href="/sdk/sessionweave-client.tar.gz">SDK download</a></li>
  <li><a href="/skill.md">skill.md</a> ·
      <a href="/skill.md?download=1">download</a></li>
  <li><a href="/connect.mcp.json">connect.mcp.json</a></li>
  <li><a href="/openapi.yaml">openapi.yaml</a></li>
  <li><a href="/v1/info">/v1/info</a> · <a href="/healthz">/healthz</a></li>
</ul>
</body>
</html>
"""

_MCP_SNIPPET = """\
{{
  "mcpServers": {{
    "sessionweave": {{ "type": "http", "url": "{base}/mcp" }}
  }}
}}"""


def render_landing(base_url: str, version: str, relay_epoch: int) -> str:
    base = base_url.rstrip("/")
    base_h = html.escape(base)
    mcp_snippet_h = html.escape(_MCP_SNIPPET.format(base=base))
    return _PAGE.format(
        base_h=base_h,
        version_h=html.escape(str(version)),
        epoch=int(relay_epoch),
        mcp_snippet_h=mcp_snippet_h,
    )
