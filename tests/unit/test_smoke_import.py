import re

import sessionweave


def test_package_imports() -> None:
    # Package version comes from pyproject.toml metadata. Allow either a
    # real semver or the editable-install fallback `0.0.0+local`.
    assert re.match(r"^\d+\.\d+\.\d+(\+[\w.]+)?$", sessionweave.__version__)
