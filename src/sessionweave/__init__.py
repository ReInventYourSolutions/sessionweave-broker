"""SessionWeave broker package (distribution ``sessionweave-broker``)."""
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

try:
    __version__ = _pkg_version("sessionweave-broker")
except PackageNotFoundError:  # pragma: no cover - editable install fallback
    __version__ = "0.0.0+local"

__all__ = ["__version__"]
