"""Publish and recover independent SQLite snapshots after semantic validation."""

from contextlib import closing
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from time import monotonic


class InvalidSnapshot(ValueError):
    """The database failed validation and was not published."""


def _read_only(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2)


def _checked_copy(source, target, verify):
    source, target = Path(source), Path(target)
    if os.path.lexists(target):
        raise FileExistsError(target)
    with TemporaryDirectory(prefix=".snapshot-", dir=target.parent) as staging:
        candidate = Path(staging) / "copy.sqlite"
        deadline = monotonic() + 3

        def check_progress(_status, _remaining, _total):
            if monotonic() > deadline:
                raise TimeoutError("SQLite copy exceeded its local time budget")

        with closing(_read_only(source)) as live, closing(sqlite3.connect(candidate)) as copy:
            live.backup(copy, pages=64, sleep=0.01, progress=check_progress)
            copy.execute("PRAGMA journal_mode=DELETE").fetchone()
        with closing(_read_only(candidate)) as copy:
            if copy.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise InvalidSnapshot("SQLite integrity check failed")
            if verify(copy) is not True:
                raise InvalidSnapshot("application check refused snapshot")
        # Same-directory hard link publishes a complete file without overwriting a winner.
        os.link(candidate, target)
    return target


def snapshot(live, target, *, verify):
    """Copy committed database state, validate the copy, then publish it."""
    return _checked_copy(live, target, verify)


def recover(archive, target, *, verify):
    """Validate a fresh private copy of the archive before making it visible."""
    return _checked_copy(archive, target, verify)
