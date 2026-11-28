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

from collections import namedtuple
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory, mkstemp
from time import monotonic


class InvalidSnapshot(ValueError):
    """The database failed validation and was not published."""


class RouteRepairError(ValueError):
    """A business rule rejected a route-repair preview or application.

    ``reason`` is one of ``missing_route`` (a selected id is absent from one
    of the two databases), ``alias_conflict`` (a planned alias is held by a
    route outside the repair scope) or ``stale_plan`` (the live database no
    longer matches what the plan previewed, or the plan is being applied to a
    different live file).
    """

    def __init__(self, reason, detail=""):
        self.reason = reason
        message = reason if not detail else "%s: %s" % (reason, detail)
        super().__init__(message)


# Immutable, in-process route repair plan.  ``changes`` is a tuple of
# (route_id, current_alias, current_destination, planned_alias,
# planned_destination) sorted by route_id; ``alias_occupancy`` records the
# owner of every planned alias at preview time, with ``None`` meaning free.
RouteRepairPlan = namedtuple(
    "RouteRepairPlan", ("live_path", "changes", "alias_occupancy")
)


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


# --------------------------------------------------------------------------
# Local route repair driven by an independent backup file
# --------------------------------------------------------------------------


_REPAIR_MIN_COUNT = 1
_REPAIR_MAX_COUNT = 100
_REPAIR_TEMP_PREFIX = ".route-repair-temp-"


def _validate_route_ids(route_ids):
    """Normalize the caller's id list or raise plain ``ValueError``."""
    try:
        ids = list(route_ids)
    except TypeError:
        raise ValueError("route_ids must be an iterable of integers")
    if not (_REPAIR_MIN_COUNT <= len(ids) <= _REPAIR_MAX_COUNT):
        raise ValueError("route_ids must contain between 1 and 100 distinct ids")
    distinct = set()
    for value in ids:
        # ``bool`` is an ``int`` subclass but never a legal primary key here.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("route ids must be integers")
        if value in distinct:
            raise ValueError("duplicate route id: %d" % value)
        distinct.add(value)
    return sorted(distinct)


def _placeholders(count):
    return ",".join("?" * count)


def plan_route_repair(backup, live, route_ids):
    """Preview a scoped repair without writing either database.

    The backup supplies the planned alias and destination for every selected
    ``route_id``; backup subscriptions never participate.  A planned alias
    that is currently owned by a route outside the selected scope rejects the
    whole preview.  The returned plan is an immutable, in-process object and
    is only valid against this exact live file.
    """
    ids = _validate_route_ids(route_ids)
    backup_path = Path(backup).resolve()
    live_path = Path(live).resolve()
    id_set = set(ids)
    query = (
        "SELECT route_id, alias, destination FROM routes "
        "WHERE route_id IN (%s)" % _placeholders(len(ids))
    )

    with closing(_read_only(backup_path)) as bk, closing(_read_only(live_path)) as lv:
        # One read transaction per file keeps each preview internally
        # consistent even if another connection commits midway.
        bk.execute("BEGIN")
        lv.execute("BEGIN")
        try:
            backup_rows = bk.execute(query, ids).fetchall()
            live_rows = lv.execute(query, ids).fetchall()
            if len(backup_rows) != len(ids) or len(live_rows) != len(ids):
                raise RouteRepairError(
                    "missing_route", "every selected id must exist in both databases"
                )
            backup_routes = {row[0]: (row[1], row[2]) for row in backup_rows}
            live_routes = {row[0]: (row[1], row[2]) for row in live_rows}

            planned_aliases = {backup_routes[rid][0] for rid in ids}
            occupancy_rows = lv.execute(
                "SELECT alias, route_id FROM routes WHERE alias IN (%s)"
                % _placeholders(len(planned_aliases)),
                tuple(planned_aliases),
            ).fetchall()
        finally:
            bk.execute("ROLLBACK")
            lv.execute("ROLLBACK")

    occupancy = {alias: None for alias in planned_aliases}
    for alias, owner in occupancy_rows:
        occupancy[alias] = owner
    for alias, owner in occupancy.items():
        if owner is not None and owner not in id_set:
            raise RouteRepairError(
                "alias_conflict",
                "planned alias %r is held by out-of-scope route %d" % (alias, owner),
            )

    changes = tuple(
        (
            rid,
            live_routes[rid][0],
            live_routes[rid][1],
            backup_routes[rid][0],
            backup_routes[rid][1],
        )
        for rid in ids
    )
    return RouteRepairPlan(
        live_path=str(live_path),
        changes=changes,
        alias_occupancy=tuple(sorted(occupancy.items())),
    )


def _rewrite_scoped_routes(con, planned):
    """Move scoped routes to their planned aliases without ever violating
    the unique alias constraint.

    Subscriptions follow automatically through the foreign key's
    ``ON UPDATE CASCADE``; this function never touches that table.  Alias
    cycles are broken by parking one route under a unique temporary alias,
    which frees its old alias so dependent routes can proceed.
    """
    ids = tuple(sorted(planned))
    planned_aliases = {values[0] for values in planned.values()}
    remaining = set(ids)

    while remaining:
        owners = dict(
            con.execute(
                "SELECT alias, route_id FROM routes WHERE route_id IN (%s)"
                % _placeholders(len(ids)),
                ids,
            ).fetchall()
        )
        for rid in ids:
            if rid not in remaining:
                continue
            target_alias = planned[rid][0]
            holder = owners.get(target_alias)
            if holder is None or holder == rid:
                con.execute(
                    "UPDATE routes SET alias = ?, destination = ? WHERE route_id = ?",
                    (target_alias, planned[rid][1], rid),
                )
                remaining.remove(rid)
                break
        else:
            # Every remaining route waits on another remaining one: a cycle.
            # Park the lowest-id route under an alias nobody else can want.
            rid = min(remaining)
            forbidden = {row[0] for row in con.execute("SELECT alias FROM routes")}
            forbidden.update(planned_aliases)
            base = _REPAIR_TEMP_PREFIX + str(rid)
            temp_alias, counter = base, 0
            while temp_alias in forbidden:
                counter += 1
                temp_alias = "%s-%d" % (base, counter)
            con.execute(
                "UPDATE routes SET alias = ? WHERE route_id = ?", (temp_alias, rid)
            )


def apply_route_repair(live, plan, *, verify):
    """Apply a previewed plan inside one live write transaction.

    Every selected route and every planned alias occupancy is rechecked
    before any modification, so concurrent subscription inserts and changes
    to unrelated routes are tolerated while any observed drift rejects the
    whole attempt with ``stale_plan``.  ``verify`` runs once against the
    final, not-yet-committed state; only an explicit ``True`` commits.
    """
    if not isinstance(plan, RouteRepairPlan):
        raise ValueError("plan must be a RouteRepairPlan returned by plan_route_repair")
    live_path = Path(live).resolve()
    if live_path != Path(plan.live_path):
        raise RouteRepairError(
            "stale_plan", "plan was previewed against a different live database"
        )

    ids = [change[0] for change in plan.changes]
    expected_current = {
        rid: (current_alias, current_destination)
        for rid, current_alias, current_destination, _planned_alias, _planned_dest
        in plan.changes
    }
    planned = {
        rid: (planned_alias, planned_destination)
        for rid, _current_alias, _current_destination, planned_alias, planned_destination
        in plan.changes
    }
    preview_occupancy = dict(plan.alias_occupancy)

    con = sqlite3.connect(live_path, timeout=5)
    try:
        # Required so ON UPDATE CASCADE keeps subscriptions.alias in lockstep
        # while routes are renamed; must be set outside a transaction.
        con.execute("PRAGMA foreign_keys=ON")
        con.isolation_level = None
        con.execute("BEGIN IMMEDIATE")
        try:
            current_rows = con.execute(
                "SELECT route_id, alias, destination FROM routes "
                "WHERE route_id IN (%s)" % _placeholders(len(ids)),
                ids,
            ).fetchall()
            if len(current_rows) != len(ids):
                raise RouteRepairError(
                    "stale_plan", "a selected route is missing from the live database"
                )
            for rid, alias, destination in current_rows:
                if (alias, destination) != expected_current[rid]:
                    raise RouteRepairError(
                        "stale_plan", "route %d changed after the preview" % rid
                    )

            occupancy_rows = con.execute(
                "SELECT alias, route_id FROM routes WHERE alias IN (%s)"
                % _placeholders(len(preview_occupancy)),
                tuple(preview_occupancy),
            ).fetchall()
            current_occupancy = {alias: None for alias in preview_occupancy}
            for alias, owner in occupancy_rows:
                current_occupancy[alias] = owner
            for alias, preview_owner in preview_occupancy.items():
                if current_occupancy[alias] != preview_owner:
                    raise RouteRepairError(
                        "stale_plan",
                        "alias %r changed owner after the preview" % alias,
                    )

            _rewrite_scoped_routes(con, planned)

            if verify(con) is not True:
                raise InvalidSnapshot("route repair verification refused the snapshot")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
    return None
