from __future__ import annotations

from sessionweave.brief import BriefUpgrades, render_brief

BASE = "https://relay.sessionweave.com"
SID = "abc123"


def _render(purpose, upgrades=BriefUpgrades()):
    return render_brief(base_url=BASE, session_id=SID, purpose=purpose, upgrades=upgrades)


def test_includes_base_url_and_sid_and_bearer_rule():
    out = _render({"kind": "demo"})
    assert BASE in out
    assert SID in out
    assert "Authorization: Bearer" in out


def test_non_blocking_warning_present():
    out = _render(None)
    assert "do NOT open a WebSocket" in out
    assert "inbox/wait" in out


def test_purpose_your_role():
    out = _render({"kind": "ttt", "your_role": "O", "instructions": "X first"})
    assert "your role: `O`" in out
    assert "X first" in out


def test_purpose_available_roles():
    out = _render({"available_roles": ["X", "O"]})
    assert "available roles:" in out
    assert "`X`" in out and "`O`" in out


def test_purpose_none():
    out = _render(None)
    assert "No machine-readable purpose" in out


def test_upgrades_gated_off_by_default():
    out = _render({"kind": "demo"}, BriefUpgrades())
    assert "Ask your operator" in out


def test_upgrades_emitted_when_configured():
    up = BriefUpgrades(
        skill_url="https://x/skill",
        mcp_snippet_url="https://x/mcp",
        runner_install_url="https://x/run",
    )
    out = _render({"kind": "demo"}, up)
    assert "https://x/skill" in out
    assert "https://x/mcp" in out
    assert "https://x/run" in out
    assert "Ask your operator" not in out
