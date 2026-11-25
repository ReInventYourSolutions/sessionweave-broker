"""Release guard against dependency confusion.

Users install this package from PyPI (or a wheel) and pip then resolves its
dependency (default: ``sessionweave-sdk``) *by name* from PyPI. The release must
not go out unless PyPI has a release of that project which satisfies our
requirement AND was provably published by our repository:

1. the newest release satisfying the requirement (what pip would pick) exists
   and is not yanked;
2. every file of it has PEP 740 provenance on PyPI (integrity API) with a
   GitHub trusted publisher whose repository is exactly ``--repo`` and whose
   workflow is ``--workflow``;
3. extra check: the project's declared URLs include ``https://github.com/<repo>``
   (parsed, exact host and path). Self-declared, so never sufficient alone.

Usage (repository root; needs ``packaging``):
    python ci/check_sdk_on_pypi.py
    python ci/check_sdk_on_pypi.py --project sessionweave-broker \\
        --repo ReInventYourSolutions/sessionweave-broker \\
        --requirement 'sessionweave-broker>=0.35,<0.36'
"""
from __future__ import annotations

import argparse
import json
import sys
import tomllib
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote, urlparse

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import InvalidVersion, Version

PYPI = "https://pypi.org"


class GuardError(Exception):
    pass


def newest_satisfying(releases: dict[str, list[dict[str, Any]]], spec: SpecifierSet) -> str | None:
    """Newest non-yanked release version in ``spec`` (what pip would install)."""
    best: Version | None = None
    for raw, files in releases.items():
        if not files or all(f.get("yanked") for f in files):
            continue
        try:
            v = Version(raw)
        except InvalidVersion:
            continue
        if spec.contains(v) and (best is None or v > best):
            best = v
    return None if best is None else str(best)


def provenance_matches(provenance: dict[str, Any] | None, repo: str, workflow: str) -> bool:
    """True when an attestation bundle names GitHub publisher ``repo`` + ``workflow``."""
    if not provenance:
        return False
    for bundle in provenance.get("attestation_bundles") or []:
        pub = bundle.get("publisher") or {}
        if (
            str(pub.get("kind", "")).lower() == "github"
            and str(pub.get("repository", "")).lower() == repo.lower()
            and str(pub.get("workflow", "")) == workflow
            and bundle.get("attestations")
        ):
            return True
    return False


def declares_repo_url(info: dict[str, Any], repo: str) -> bool:
    urls = [info.get("home_page") or "", *(info.get("project_urls") or {}).values()]
    want = "/" + repo.lower()
    for raw in urls:
        u = urlparse(str(raw).strip())
        if u.scheme == "https" and (u.hostname or "").lower() == "github.com":
            path = u.path.rstrip("/").lower()
            if path.endswith(".git"):
                path = path[:-4]
            if path == want:
                return True
    return False


def _get_json(url: str) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            data: dict[str, Any] = json.load(r)
            return data
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def check(project: str, req: Requirement, repo: str, workflow: str) -> str:
    meta = _get_json(f"{PYPI}/pypi/{quote(project)}/json")
    if meta is None:
        raise GuardError(f"{project} is not on PyPI yet: publish it before releasing this package")
    if not declares_repo_url(meta.get("info") or {}, repo):
        raise GuardError(f"PyPI project {project} does not declare https://github.com/{repo}")
    version = newest_satisfying(meta.get("releases") or {}, req.specifier)
    if version is None:
        raise GuardError(f"no {project} release on PyPI satisfies '{req.specifier}'")
    rel = _get_json(f"{PYPI}/pypi/{quote(project)}/{quote(version)}/json") or {}
    files = [f for f in rel.get("urls") or [] if not f.get("yanked")]
    if not files:
        raise GuardError(f"{project} {version} has no files on PyPI")
    for f in files:
        name = f["filename"]
        prov = _get_json(
            f"{PYPI}/integrity/{quote(project)}/{quote(version)}/{quote(name)}/provenance"
        )
        if not provenance_matches(prov, repo, workflow):
            raise GuardError(
                f"{project} {version} file {name}: no PEP 740 provenance from "
                f"GitHub {repo} ({workflow})"
            )
    return version


def requirement_from_pyproject(project: str) -> Requirement:
    with open("pyproject.toml", "rb") as fh:
        deps = tomllib.load(fh)["project"]["dependencies"]
    for d in deps:
        r = Requirement(d)
        if r.name == project:
            return r
    raise GuardError(f"pyproject.toml does not depend on {project}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", default="sessionweave-sdk")
    ap.add_argument("--repo", default="ReInventYourSolutions/sessionweave-sdk")
    ap.add_argument("--workflow", default="release.yml")
    ap.add_argument("--requirement", help="default: the dependency in pyproject.toml")
    a = ap.parse_args(argv)
    try:
        req = Requirement(a.requirement) if a.requirement else requirement_from_pyproject(a.project)
        version = check(a.project, req, a.repo, a.workflow)
    except GuardError as exc:
        print(f"::error::{exc}")
        return 1
    print(
        f"{a.project} {version} on PyPI satisfies '{req.specifier}'"
        f" with provenance from {a.repo}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
