"""Release guard: fail unless PyPI lists a `sessionweave-sdk` release that satisfies
this package's requirement and was published from our SDK repository.

Users install this package from PyPI (or a wheel) and pip then resolves
`sessionweave-sdk` *by name* from PyPI. Until our SDK is published there the name
is claimable by anyone (dependency confusion), so no release may go out before it.
Run from the repository root: `python ci/check-sdk-on-pypi.py` (needs `packaging`).
"""
from __future__ import annotations

import json
import sys
import tomllib
import urllib.error
import urllib.request

from packaging.requirements import Requirement
from packaging.version import InvalidVersion, Version

NAME = "sessionweave-sdk"
OUR_REPO = "github.com/reinventyoursolutions/sessionweave-sdk"


def fail(msg: str) -> None:
    print(f"::error::{msg}")
    sys.exit(1)


def main() -> None:
    with open("pyproject.toml", "rb") as fh:
        deps = tomllib.load(fh)["project"]["dependencies"]
    reqs = [Requirement(d) for d in deps if Requirement(d).name == NAME]
    if not reqs:
        fail(f"pyproject.toml does not depend on {NAME}")
    req = reqs[0]
    try:
        with urllib.request.urlopen(f"https://pypi.org/pypi/{NAME}/json", timeout=30) as r:
            data = json.load(r)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            fail(f"{NAME} is not on PyPI yet: publish the SDK before releasing this package")
        raise
    urls = " ".join(
        [str(data["info"].get("home_page") or "")]
        + [str(u) for u in (data["info"].get("project_urls") or {}).values()]
    ).lower()
    if OUR_REPO not in urls:
        fail(f"PyPI project {NAME} does not point at {OUR_REPO}: not ours?")
    ok = []
    for v, files in data.get("releases", {}).items():
        if not files or all(f.get("yanked") for f in files):
            continue
        try:
            if req.specifier.contains(Version(v)):
                ok.append(v)
        except InvalidVersion:
            continue
    if not ok:
        fail(f"no {NAME} release on PyPI satisfies '{req.specifier}'")
    print(f"{NAME} on PyPI satisfies '{req.specifier}': {', '.join(sorted(ok, key=Version))}")


if __name__ == "__main__":
    main()
