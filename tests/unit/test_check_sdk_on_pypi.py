"""Unit tests of the release guard's parsers (ci/check_sdk_on_pypi.py)."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from packaging.specifiers import SpecifierSet


def _load() -> ModuleType:
    path = Path(__file__).resolve().parents[2] / "ci" / "check_sdk_on_pypi.py"
    spec = importlib.util.spec_from_file_location("check_sdk_on_pypi", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_sdk_on_pypi"] = mod
    spec.loader.exec_module(mod)
    return mod


guard = _load()
REPO = "ReInventYourSolutions/sessionweave-sdk"


def _prov(repo: str, workflow: str = "release.yml", kind: str = "GitHub") -> dict[str, Any]:
    return {
        "version": 1,
        "attestation_bundles": [{
            "publisher": {"kind": kind, "repository": repo, "workflow": workflow,
                          "environment": "pypi"},
            "attestations": [{"version": 1, "envelope": {}, "verification_material": {}}],
        }],
    }


def test_provenance_from_our_repository_is_accepted() -> None:
    assert guard.provenance_matches(_prov(REPO), REPO, "release.yml")
    assert guard.provenance_matches(_prov(REPO.lower()), REPO, "release.yml")


@pytest.mark.parametrize("prov", [
    _prov("attacker/sessionweave-sdk"),
    _prov("ReInventYourSolutions/sessionweave-sdk-fork"),
    _prov(REPO, workflow="other.yml"),
    _prov(REPO, kind="GitLab"),
    {"version": 1, "attestation_bundles": []},
    {"version": 1, "attestation_bundles": [{"publisher": {"kind": "GitHub", "repository": REPO,
                                                          "workflow": "release.yml"},
                                            "attestations": []}]},
    None,
    {},
])
def test_foreign_or_missing_provenance_is_refused(prov: dict[str, Any] | None) -> None:
    assert not guard.provenance_matches(prov, REPO, "release.yml")


@pytest.mark.parametrize(("urls", "ok"), [
    ({"Source": "https://github.com/ReInventYourSolutions/sessionweave-sdk"}, True),
    ({"Source": "https://github.com/reinventyoursolutions/sessionweave-sdk/"}, True),
    ({"Source": "https://github.com/ReInventYourSolutions/sessionweave-sdk.git"}, True),
    ({"Source": "https://evil.example/github.com/ReInventYourSolutions/sessionweave-sdk"}, False),
    ({"Source": "https://github.com.evil.example/ReInventYourSolutions/sessionweave-sdk"}, False),
    ({"Source": "https://github.com/ReInventYourSolutions/sessionweave-sdk-evil"}, False),
    ({"Source": "http://github.com/ReInventYourSolutions/sessionweave-sdk"}, False),
    ({}, False),
])
def test_declared_repo_url_is_parsed_exactly(urls: dict[str, str], ok: bool) -> None:
    assert guard.declares_repo_url({"home_page": None, "project_urls": urls}, REPO) is ok


def test_newest_satisfying_skips_yanked_and_out_of_range() -> None:
    releases = {
        "0.12.9": [{"yanked": False}],
        "0.13.0": [{"yanked": False}],
        "0.13.2": [{"yanked": True}],
        "0.13.1": [{"yanked": False}],
        "0.14.0": [{"yanked": False}],
        "0.13.3": [],
    }
    assert guard.newest_satisfying(releases, SpecifierSet(">=0.13.0,<0.14")) == "0.13.1"
    assert guard.newest_satisfying({}, SpecifierSet(">=0.13.0,<0.14")) is None


def test_check_fails_when_project_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guard, "_get_json", lambda url: None)
    from packaging.requirements import Requirement

    with pytest.raises(guard.GuardError, match="not on PyPI"):
        guard.check("sessionweave-sdk", Requirement("sessionweave-sdk>=0.13.0,<0.14"), REPO,
                    "release.yml")


def test_check_requires_provenance_for_every_file(monkeypatch: pytest.MonkeyPatch) -> None:
    from packaging.requirements import Requirement

    info = {"home_page": None,
            "project_urls": {"Source": "https://github.com/ReInventYourSolutions/sessionweave-sdk"}}
    files = [{"filename": "sessionweave_sdk-0.13.0.tar.gz", "yanked": False},
             {"filename": "sessionweave_sdk-0.13.0-py3-none-any.whl", "yanked": False}]

    def fake(url: str, prov_for_wheel: dict[str, Any] | None) -> dict[str, Any] | None:
        if url.endswith("/pypi/sessionweave-sdk/json"):
            return {"info": info, "releases": {"0.13.0": files}}
        if url.endswith("/pypi/sessionweave-sdk/0.13.0/json"):
            return {"urls": files}
        if url.endswith(".tar.gz/provenance"):
            return _prov(REPO)
        return prov_for_wheel

    req = Requirement("sessionweave-sdk>=0.13.0,<0.14")
    monkeypatch.setattr(guard, "_get_json", lambda url: fake(url, _prov(REPO)))
    assert guard.check("sessionweave-sdk", req, REPO, "release.yml") == "0.13.0"
    monkeypatch.setattr(guard, "_get_json", lambda url: fake(url, None))
    with pytest.raises(guard.GuardError, match="no PEP 740 provenance"):
        guard.check("sessionweave-sdk", req, REPO, "release.yml")
    monkeypatch.setattr(guard, "_get_json", lambda url: fake(url, _prov("attacker/x")))
    with pytest.raises(guard.GuardError, match="no PEP 740 provenance"):
        guard.check("sessionweave-sdk", req, REPO, "release.yml")
