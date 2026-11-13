from __future__ import annotations

from typing import Any

from sessionweave.brief import BriefUpgrades, render_brief

BASE = "https://relay.sessionweave.com"
SID = "abc123"


def _render(
    purpose: dict[str, Any] | None, upgrades: BriefUpgrades = BriefUpgrades()  # noqa: B008
) -> str:
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


def test_join_example_uses_the_purpose_role_and_capabilities():
    out = _render({"your_role": "reviewer", "your_capabilities": ["review"]})
    assert '-d \'{"role": "reviewer", "capabilities": ["review"]}\'' in out
    assert '"role":"worker"' not in out
    assert "your capabilities: `review`" in out


def test_join_example_with_a_single_available_role():
    out = _render({"available_roles": ["solver"]})
    assert '{"role": "solver", "capabilities": []}' in out


def test_join_example_with_several_roles_names_no_fixed_role():
    out = _render({"available_roles": ["a", "b"]})
    assert '"role": "<role the purpose asks for>"' in out


def test_join_example_escapes_single_quotes_for_the_shell():
    out = _render({"your_role": "o'brien"})
    assert "o'\\''brien" in out


def test_brief_states_runtime_limit_payload_and_leave_status():
    out = _render({"kind": "demo"})
    assert "wait_already_in_flight" in out
    assert "inbox/drain" in out
    assert "goes inside `payload`" in out
    assert "204 No Content" in out
