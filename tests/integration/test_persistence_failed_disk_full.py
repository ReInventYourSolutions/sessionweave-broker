"""Integration test: disk-full failure path for persistence writes.

Mounting a tmpfs requires root privileges. This test is a documented
placeholder — the actual disk-full code path is already covered by unit
tests via mock. The integration test is skipped by default since it needs
a privileged tmpfs setup.

To exercise this path manually:
    sudo mount -t tmpfs -o size=64k tmpfs /mnt/tiny
    SW_PERSISTENCE_DB_PATH=/mnt/tiny/relay.db pytest \
        tests/integration/test_persistence_failed_disk_full.py -v
"""
from __future__ import annotations

import pytest


@pytest.mark.integration
def test_disk_full_requires_privileged_setup() -> None:
    """Placeholder: disk-full persistence errors require a tmpfs mount.

    The unit tests in tests/unit/ cover the disk-full code path via mock
    (PersistenceStore.persist_event_atomic raises aiosqlite.OperationalError,
    which the relay maps to ErrorCode.PERSISTENCE_FAILED → HTTP 503).

    A true integration test would:
    1. Mount a tiny tmpfs (requires root / CAP_SYS_ADMIN).
    2. Spawn the relay with SW_PERSISTENCE_DB_PATH pointing at the tmpfs.
    3. Fill the DB until the filesystem is full.
    4. Assert the next write returns 503 with error=persistence_failed.

    This cannot run unprivileged. Skipping.
    """
    pytest.skip(
        "requires privileged tmpfs setup (root or CAP_SYS_ADMIN); "
        "disk-full path is covered by unit tests via mock"
    )
