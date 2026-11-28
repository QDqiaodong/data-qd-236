"""Publish and recover independent SQLite snapshots after semantic validation.

The module offers two layers.

``snapshot`` / ``recover`` are the original one-file helpers.  They copy a
committed SQLite database, validate the private copy and then hard-link it
into place without ever overwriting an existing target.

``VersionedArchive`` is an in-process, page-addressed archive.  It retains the
bytes needed to reproduce every live version: the active version plus every
version still referenced by an outstanding permit.  Byte-identical database
pages are stored once and shared between versions, so a short chain of
one-page edits fits in a budget of "two complete databases plus two pages"
while older permits keep resolving to their original version.  Every file the
archive writes for a caller is an independent SQLite database.

Only the Python standard library is used.  The implementation is
single-process, relies on trusted local directories and performs no locking
against other processes; it makes no power-loss guarantees.
"""

from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory, mkstemp
from time import monotonic


class InvalidSnapshot(ValueError):
    """The database failed validation and was not published."""


class SpaceError(RuntimeError):
    """The archive's byte budget cannot accommodate the requested operation."""


_MAX_BYTE_LIMIT = 32 * 1024 * 1024
_MAX_PERMITS = 4
_MAX_RETAINED_VERSIONS = 4
_SQLITE_MAGIC = b"SQLite format 3\x00"


# --------------------------------------------------------------------------
# Original single-file snapshot / recover helpers
# --------------------------------------------------------------------------


def _read_only(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2)


def _immutable_read_only(path):
    """Open a private, immutable file read-only without creating sidecars.

    ``immutable=1`` lets SQLite skip locking and WAL/SHM bookkeeping, which is
    exactly right for archive payloads and stable sources: they cannot change
    while published or while a single serial operation is in flight.
    """
    return sqlite3.connect(
        Path(path).resolve().as_uri() + "?mode=ro&immutable=1", uri=True, timeout=0.2
    )


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


# --------------------------------------------------------------------------
# Versioned archive internals
# --------------------------------------------------------------------------


class _Page:
    """One immutable database page held by the archive.

    A page is the smallest unit of storage and billing.  When two versions
    contain byte-identical pages they reference the same ``_Page`` object and
    the same backing file, so the payload is genuinely stored once and billed
    once.
    """

    __slots__ = ("digest", "path", "size")

    def __init__(self, digest, path, size):
        self.digest = digest
        self.path = path
        self.size = size

    def read(self):
        return Path(self.path).read_bytes()


class _Version:
    """A named snapshot: an ordered list of shared pages."""

    __slots__ = ("name", "layout", "page_size")

    def __init__(self, name, layout, page_size):
        self.name = name
        self.layout = layout  # tuple of _Page, in file order
        self.page_size = page_size

    def __len__(self):
        return len(self.layout)


class _Permit:
    """One-shot permission to restore a particular version."""

    __slots__ = ("version", "live")

    def __init__(self, version):
        self.version = version
        self.live = True


def _validate_directory(directory):
    root = Path(directory)
    if not root.is_dir():
        raise NotADirectoryError(root)
    root = root.resolve()
    if any(root.iterdir()):
        raise ValueError("VersionedArchive requires an empty dedicated directory")
    return root


def _ensure_outside(root, path, what):
    path = Path(path)
    resolved = path.resolve()
    if resolved == root or root in resolved.parents:
        raise ValueError("%s must live outside the archive directory" % what)
    return path


def _reject_source_sidecars(path):
    """Require a quiescent source: no hot journal, WAL, or shared-memory file."""
    for suffix in ("-journal", "-wal", "-shm"):
        if os.path.lexists(path.with_name(path.name + suffix)):
            raise InvalidSnapshot("source has an uncommitted transaction or sidecar log")


def _read_header(path):
    """Validate the SQLite header and return ``(page_size, page_count)``.

    A zero-length file is a valid, empty SQLite database and yields ``(0, 0)``.
    """
    with open(path, "rb") as handle:
        header = handle.read(100)
        size = os.fstat(handle.fileno()).st_size
    if size == 0:
        return 0, 0
    if len(header) < 100 or header[:16] != _SQLITE_MAGIC:
        raise sqlite3.DatabaseError("file is not a SQLite database")
    page_size = int.from_bytes(header[16:18], "big")
    if page_size == 1:
        page_size = 65536
    if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
        raise sqlite3.DatabaseError("invalid SQLite page size")
    if size % page_size:
        raise sqlite3.DatabaseError("truncated SQLite database")
    if int.from_bytes(header[28:32], "big") != size // page_size:
        raise sqlite3.DatabaseError("SQLite header page count disagrees with file length")
    if header[18] not in (1, 2) or header[19] not in (1, 2):
        raise sqlite3.DatabaseError("unsupported SQLite file format version")
    return page_size, size // page_size


def _hash_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.digest()


class VersionedArchive:
    """In-process archive of independently restorable SQLite versions.

    ``directory`` must be an empty dedicated directory and must not contain
    any source or restore target.  ``byte_limit`` is a hard cap on the
    database payload the archive retains, measured in whole database pages:
    pages shared by live versions, pages staged for a candidate, and any
    private in-memory data are all billed.  A single staging file used to
    atomically publish an independent restore target is exempt.
    """

    def __init__(self, directory, *, byte_limit):
        if isinstance(byte_limit, bool) or not isinstance(byte_limit, int):
            raise TypeError("byte_limit must be an integer")
        if byte_limit < 0:
            raise ValueError("byte_limit must be non-negative")
        if byte_limit > _MAX_BYTE_LIMIT:
            raise ValueError("byte_limit exceeds the 32 MiB archive maximum")
        self._root = _validate_directory(directory)
        self._limit = byte_limit
        self._index = {}  # page digest -> committed _Page (each stored once)
        self._pending = []  # pages written by a publish that has not committed
        self._pending_index = {}  # digest -> pending _Page
        self._used_names = set()
        self._active = None
        self._issued = set()  # every permit ever handed out by this archive
        self._peak = 0

    # -- accounting ------------------------------------------------------

    def _retained_versions(self):
        versions = []
        if self._active is not None:
            versions.append(self._active)
        for permit in self._issued:
            if not permit.live:
                continue
            if all(permit.version is not version for version in versions):
                versions.append(permit.version)
        return versions

    def _retained_bytes(self):
        seen = set()
        total = 0
        for version in self._retained_versions():
            for page in version.layout:
                if id(page) not in seen:
                    seen.add(id(page))
                    total += page.size
        return total

    def _pending_bytes(self):
        return sum(page.size for page in self._pending)

    def _billed_bytes(self):
        return self._retained_bytes() + self._pending_bytes()

    def _observe_peak(self):
        current = self._billed_bytes()
        if current > self._peak:
            self._peak = current
        return current

    def _collect(self):
        """Return every committed page that no retained version references."""
        retained = {id(page) for version in self._retained_versions() for page in version.layout}
        for digest, page in list(self._index.items()):
            if id(page) in retained:
                continue
            try:
                os.unlink(page.path)
            except FileNotFoundError:
                pass
            del self._index[digest]

    def _write_page(self, digest, data):
        fd, name = mkstemp(prefix="page-", dir=self._root)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            return _Page(digest, Path(name), len(data))
        except BaseException:
            try:
                os.unlink(name)
            except FileNotFoundError:
                pass
            raise

    def _discard_pending(self):
        for page in self._pending:
            try:
                os.unlink(page.path)
            except FileNotFoundError:
                pass
        self._pending = []
        self._pending_index = {}

    def _lookup_page(self, digest):
        page = self._index.get(digest)
        if page is None:
            page = self._pending_index.get(digest)
        return page

    # -- public observations --------------------------------------------

    def usage(self):
        """Database payload bytes currently billed for this archive."""
        return self._billed_bytes()

    def last_peak(self):
        """Peak billed bytes during the most recent publish or restore."""
        return self._peak

    def active(self):
        """Name of the active version, or ``None`` when nothing is active."""
        return None if self._active is None else self._active.name

    # -- publish ---------------------------------------------------------

    def _stage_candidate(self, path, page_size, page_count):
        """Read a candidate page by page, sharing bytes with every live version.

        Returns the ordered layout plus a digest of the exact bytes read.  The
        digest lets ``publish`` confirm that the callback validated the same
        bytes that were staged, which is the state that gets published.
        """
        layout = []
        hasher = hashlib.sha256()
        if page_size == 0:
            return (), hasher.digest()
        with open(path, "rb") as handle:
            for _ in range(page_count):
                data = handle.read(page_size)
                if len(data) != page_size:
                    raise sqlite3.DatabaseError("source changed while it was being read")
                hasher.update(data)
                digest = hashlib.sha256(data).hexdigest()
                page = self._lookup_page(digest)
                if page is None:
                    if self._billed_bytes() + page_size > self._limit:
                        raise SpaceError(
                            "candidate needs more than the %d byte budget" % self._limit
                        )
                    page = self._write_page(digest, data)
                    self._pending.append(page)
                    self._pending_index[digest] = page
                    self._observe_peak()
                layout.append(page)
        return tuple(layout), hasher.digest()

    def publish(self, version, source, *, verify):
        """Acquire a candidate from ``source``, validate it, then make it active.

        The previous active version and every outstanding permit stay valid
        until the candidate has passed both structural and business
        validation.  On failure the previous state is untouched, staged pages
        are returned, and the version name remains available for a retry.
        """
        if version is None:
            raise ValueError("version name must not be None")
        try:
            hash(version)
        except TypeError:
            raise TypeError("version name must be hashable")
        if version in self._used_names:
            raise ValueError("version name already used: %r" % (version,))
        path = _ensure_outside(self._root, source, "source")
        path = Path(path)
        if path.is_symlink() or not path.is_file():
            raise InvalidSnapshot("source must be a single ordinary database file")
        _reject_source_sidecars(path)

        pinned = {id(permit.version) for permit in self._issued if permit.live}
        if len(pinned) + 1 > _MAX_RETAINED_VERSIONS:
            raise RuntimeError("too many retained versions")

        self._discard_pending()
        self._peak = self._retained_bytes()
        try:
            page_size, page_count = _read_header(path)
            # Structural validation happens before any business callback.
            with closing(_immutable_read_only(path)) as probe:
                if probe.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise InvalidSnapshot("SQLite integrity check failed")

            layout, staged_digest = self._stage_candidate(path, page_size, page_count)

            # The source is the candidate: the callback sees exactly the bytes
            # that were staged, reads it read-only, and the old active version
            # is still in force while it runs.
            with closing(_immutable_read_only(path)) as candidate:
                if verify(candidate) is not True:
                    raise InvalidSnapshot("application check refused snapshot")
            if _hash_file(path) != staged_digest:
                raise InvalidSnapshot("source changed while it was being validated")

            # Commit atomically as far as in-process callers can observe.
            self._index.update(self._pending_index)
            published = _Version(version, layout, page_size)
            self._pending = []
            self._pending_index = {}
            self._used_names.add(version)
            self._active = published
            self._observe_peak()
            self._collect()
            return version
        except BaseException:
            self._discard_pending()
            raise

    # -- permits and restore --------------------------------------------

    def pin(self):
        """Return a one-shot permit for the currently active version."""
        if self._active is None:
            raise RuntimeError("no active version to pin")
        if sum(1 for permit in self._issued if permit.live) >= _MAX_PERMITS:
            raise RuntimeError("too many outstanding permits")
        permit = _Permit(self._active)
        self._issued.add(permit)
        return permit

    def _live_permit(self, permit):
        if not isinstance(permit, _Permit) or permit not in self._issued:
            raise ValueError("not a permit issued by this archive")
        if not permit.live:
            raise ValueError("permit has already been consumed or released")
        return permit

    def _finish_permit(self, permit):
        """Retire a permit and drop every page nothing else references."""
        permit.live = False
        self._collect()

    def release(self, permit):
        """Give up a permit without restoring it."""
        permit = self._live_permit(permit)
        self._finish_permit(permit)

    def restore(self, permit, target, *, verify):
        """Materialize the permit's original version into a new independent file."""
        permit = self._live_permit(permit)
        version = permit.version
        target = Path(target)
        if os.path.lexists(target):
            raise FileExistsError(target)
        _ensure_outside(self._root, target, "restore target")
        parent = target.parent
        if not parent.is_dir():
            raise FileNotFoundError(parent)

        self._peak = self._billed_bytes()

        # One staging file in the target's parent directory; a same-directory
        # hard link publishes it atomically and never overwrites a winner.
        fd, name = mkstemp(prefix=".restore-", dir=parent)
        os.close(fd)
        os.unlink(name)
        staging = Path(name)
        try:
            with open(staging, "wb") as handle:
                for page in version.layout:
                    handle.write(page.read())
                handle.flush()
                os.fsync(handle.fileno())
            with closing(_immutable_read_only(staging)) as candidate:
                if candidate.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise InvalidSnapshot("SQLite integrity check failed")
                if verify(candidate) is not True:
                    raise InvalidSnapshot("application check refused snapshot")
            os.link(staging, target)
            # Only a successful publish consumes the permit.
            self._finish_permit(permit)
            self._observe_peak()
            return target
        finally:
            try:
                os.unlink(staging)
            except FileNotFoundError:
                pass
