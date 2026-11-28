"""Acceptance tests for the page-shared :class:`VersionedArchive`.

The helpers below build real SQLite databases with deterministic page
layouts.  A one-page child is produced by making a real SQLite edit and then
splicing only the changed data page back onto the parent's bytes, so the two
files are provably identical apart from exactly one database page.  This is
what lets the tests check file-byte diffs independently of the archive.
"""

from contextlib import closing
import os
from pathlib import Path
import shutil
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase

from recovery import InvalidSnapshot, SpaceError, VersionedArchive, recover, snapshot

PAGE_SIZE = 4096
ROWS = 9


# --------------------------------------------------------------------------
# Fixture helpers (real SQLite files, deterministic page layout)
# --------------------------------------------------------------------------


def _new_base(path, tag="A"):
    """Create a real database whose rows each occupy their own leaf page."""
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA page_size=%d" % PAGE_SIZE)
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("PRAGMA auto_vacuum=NONE")
        db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
        db.execute("CREATE TABLE notes(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute("INSERT INTO notes VALUES ('label', ?)", (tag,))
        filler = "x" * 3780
        for row in range(1, ROWS + 1):
            db.execute(
                "INSERT INTO items VALUES (?, ?)",
                (row, "row-%02d-%s%s" % (row, filler, tag)),
            )
        db.commit()


def _apply_row_update(path, row, tag):
    """Change one row in place; the replacement keeps the exact byte length."""
    with closing(sqlite3.connect(path)) as db:
        db.execute("PRAGMA journal_mode=DELETE")
        old = db.execute("SELECT body FROM items WHERE id=?", (row,)).fetchone()[0]
        db.execute("UPDATE items SET body=? WHERE id=?", (old[:-1] + tag, row))
        db.commit()


def _changed_data_pages(parent, edited):
    """Return the offsets of data pages that differ, ignoring the header page."""
    left = Path(parent).read_bytes()
    right = Path(edited).read_bytes()
    assert len(left) == len(right), (len(left), len(right))
    return [
        offset
        for offset in range(PAGE_SIZE, len(left), PAGE_SIZE)
        if left[offset : offset + PAGE_SIZE] != right[offset : offset + PAGE_SIZE]
    ]


def _one_page_child(parent, child, row, tag):
    """Create a valid child that differs from parent in exactly one page."""
    parent, child = Path(parent), Path(child)
    edited = child.with_name(child.name + ".edited")
    shutil.copyfile(parent, edited)
    _apply_row_update(edited, row, tag)

    left = parent.read_bytes()
    right = edited.read_bytes()
    assert len(left) == len(right)
    diffs = _changed_data_pages(parent, edited)
    assert len(diffs) == 1, "fixture edit touched pages: %r" % diffs
    result = bytearray(left)
    result[diffs[0] : diffs[0] + PAGE_SIZE] = right[diffs[0] : diffs[0] + PAGE_SIZE]
    child.write_bytes(result)
    edited.unlink()

    assert _diff_pages(parent, child) == [diffs[0] // PAGE_SIZE]
    with closing(sqlite3.connect(child)) as db:
        assert db.execute("PRAGMA quick_check").fetchall() == [("ok",)]
        assert db.execute("SELECT body FROM items WHERE id=?", (row,)).fetchone()[0][-1] == tag
    return diffs[0] // PAGE_SIZE


def _all_rows_child(parent, child, tags):
    """Create a same-length database whose rows all carry distinct tags."""
    parent, child = Path(parent), Path(child)
    shutil.copyfile(parent, child)
    with closing(sqlite3.connect(child)) as db:
        db.execute("PRAGMA journal_mode=DELETE")
        for row, tag in enumerate(tags, start=1):
            old = db.execute("SELECT body FROM items WHERE id=?", (row,)).fetchone()[0]
            db.execute("UPDATE items SET body=? WHERE id=?", (old[:-1] + tag, row))
        db.commit()
    assert Path(parent).stat().st_size == child.stat().st_size
    assert _page_size(child) == _page_size(parent)
    with closing(sqlite3.connect(child)) as db:
        assert db.execute("PRAGMA quick_check").fetchall() == [("ok",)]
    return child


def _page_size(path):
    with open(path, "rb") as handle:
        header = handle.read(100)
    size = int.from_bytes(header[16:18], "big")
    return 65536 if size == 1 else size


def _pages(path):
    raw = Path(path).read_bytes()
    size = _page_size(path)
    assert len(raw) % size == 0
    return [raw[offset : offset + size] for offset in range(0, len(raw), size)]


def _diff_pages(left, right):
    left, right = Path(left), Path(right)
    raw_left, raw_right = left.read_bytes(), right.read_bytes()
    size = _page_size(left)
    assert size == _page_size(right)
    assert len(raw_left) == len(raw_right)
    return [
        offset // size
        for offset in range(0, len(raw_left), size)
        if raw_left[offset : offset + size] != raw_right[offset : offset + size]
    ]


def _union_bytes(paths):
    seen = set()
    total = 0
    for path in paths:
        for page in _pages(path):
            if page not in seen:
                seen.add(page)
                total += len(page)
    return total


def _union_pages(paths):
    seen = set()
    for path in paths:
        seen.update(_pages(path))
    return seen


def _directory_bytes(directory):
    return sum(entry.stat().st_size for entry in Path(directory).iterdir())


def _query_tags(db):
    rows = db.execute("SELECT id, body FROM items ORDER BY id").fetchall()
    return [body[-1] for _, body in rows]


def _file_tags(path):
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("PRAGMA quick_check").fetchall() == [("ok",)]
        return _query_tags(db)


# --------------------------------------------------------------------------
# Original snapshot / recover regression
# --------------------------------------------------------------------------


class LegacyRegressionTest(TestCase):
    def test_snapshot_and_recover_still_publish_independent_files(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            live, archive, restored = root / "live.db", root / "archive.db", root / "out.db"
            with closing(sqlite3.connect(live)) as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("CREATE TABLE completed(id INTEGER PRIMARY KEY)")
                db.execute("INSERT INTO completed VALUES (1)")
                db.commit()
                db.execute("INSERT INTO completed VALUES (2)")
                snapshot(live, archive, verify=lambda copy: copy.execute(
                    "SELECT id FROM completed ORDER BY id"
                ).fetchall() == [(1,)])
                db.commit()
            with self.assertRaises(InvalidSnapshot):
                recover(archive, restored, verify=lambda copy: False)
            self.assertFalse(restored.exists())
            recover(archive, restored, verify=lambda copy: True)
            with closing(sqlite3.connect(restored)) as db:
                self.assertEqual(db.execute("SELECT id FROM completed").fetchall(), [(1,)])


# --------------------------------------------------------------------------
# Acceptance scenario 1
# --------------------------------------------------------------------------


class OnePageChainTest(TestCase):
    def test_two_old_permits_and_source_deletion_keep_original_versions(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            a, b, c = root / "a.db", root / "b.db", root / "c.db"
            _new_base(a, "A")
            ab_page = _one_page_child(a, b, 5, "B")
            bc_page = _one_page_child(b, c, 8, "C")
            self.assertNotEqual(ab_page, bc_page)

            size = a.stat().st_size
            page = _page_size(a)
            self.assertGreaterEqual(size, 8 * page)
            self.assertEqual(b.stat().st_size, size)
            self.assertEqual(c.stat().st_size, size)
            self.assertEqual(_diff_pages(a, b), [ab_page])
            self.assertEqual(_diff_pages(b, c), [bc_page])

            budget = 2 * size + 2 * page
            self.assertGreater(3 * size, budget)
            expected_usage = _union_bytes([a, b, c])
            self.assertLessEqual(expected_usage, budget)

            archive_dir = root / "archive"
            archive_dir.mkdir()
            va = VersionedArchive(archive_dir, byte_limit=budget)
            va.publish("A", a, verify=lambda con: True)
            permit_a = va.pin()
            va.publish("B", b, verify=lambda con: True)
            permit_b = va.pin()

            observed = {}

            def check_c(con):
                observed["active"] = va.active()
                observed["tags"] = _query_tags(con)
                observed["usage"] = va.usage()
                observed["peak"] = va.last_peak()
                return True

            va.publish("C", c, verify=check_c)
            peak = va.last_peak()

            # The callback saw the candidate C itself while B was still active.
            self.assertEqual(observed["active"], "B")
            self.assertEqual(observed["tags"][4], "B")
            self.assertEqual(observed["tags"][7], "C")
            self.assertEqual(va.active(), "C")
            self.assertLessEqual(peak, budget)
            self.assertLessEqual(observed["usage"], budget)
            self.assertEqual(va.usage(), expected_usage)
            self.assertEqual(_directory_bytes(archive_dir), expected_usage)

            # Deleting the original files must not affect the old permits.
            for path in (a, b, c):
                path.unlink()

            restored_a, restored_b, restored_c = root / "ra.db", root / "rb.db", root / "rc.db"
            va.restore(permit_a, restored_a, verify=lambda con: True)
            va.restore(permit_b, restored_b, verify=lambda con: True)
            current = va.pin()
            va.restore(current, restored_c, verify=lambda con: True)

            self.assertEqual(_file_tags(restored_a), ["A"] * ROWS)
            self.assertEqual(_file_tags(restored_b), ["A"] * 4 + ["B"] + ["A"] * 4)
            self.assertEqual(_file_tags(restored_c), ["A"] * 4 + ["B"] + ["A"] * 2 + ["C"] + ["A"])

            print(
                "\nC publish: peak=%d budget=%d 3S=%d S=%d P=%d; "
                "usage=%d; restored tags A=%s B=%s C=%s"
                % (
                    peak,
                    budget,
                    3 * size,
                    size,
                    page,
                    expected_usage,
                    "".join(_file_tags(restored_a)),
                    "".join(_file_tags(restored_b)),
                    "".join(_file_tags(restored_c)),
                )
            )


# --------------------------------------------------------------------------
# Acceptance scenario 2
# --------------------------------------------------------------------------


class RejectedCandidateTest(TestCase):
    def test_business_rejection_keeps_active_permits_and_frees_space(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            a, b, d = root / "a.db", root / "b.db", root / "d.db"
            _new_base(a, "A")
            _one_page_child(a, b, 5, "B")
            _one_page_child(b, d, 3, "D")
            page = _page_size(a)
            budget = 2 * a.stat().st_size + 2 * page

            archive_dir = root / "archive"
            archive_dir.mkdir()
            va = VersionedArchive(archive_dir, byte_limit=budget)
            va.publish("A", a, verify=lambda con: True)
            permit_a = va.pin()
            va.publish("B", b, verify=lambda con: True)
            permit_b = va.pin()

            used_before = va.usage()
            disk_before = _directory_bytes(archive_dir)
            observed = {}

            def reject_d(con):
                observed["active"] = va.active()
                observed["tags"] = _query_tags(con)
                observed["usage"] = va.usage()
                observed["peak"] = va.last_peak()
                return False

            with self.assertRaises(InvalidSnapshot):
                va.publish("D", d, verify=reject_d)

            self.assertEqual(observed["active"], "B")
            self.assertEqual(observed["tags"][2], "D")
            self.assertEqual(observed["tags"][4], "B")
            self.assertGreater(observed["usage"], used_before)
            self.assertLessEqual(observed["peak"], budget)
            self.assertEqual(va.active(), "B")
            self.assertEqual(va.usage(), used_before)
            self.assertEqual(_directory_bytes(archive_dir), disk_before)
            self.assertLessEqual(va.last_peak(), budget)

            restored_a, restored_b = root / "ra.db", root / "rb.db"
            va.restore(permit_a, restored_a, verify=lambda con: True)
            va.restore(permit_b, restored_b, verify=lambda con: True)
            self.assertEqual(_file_tags(restored_a), ["A"] * ROWS)
            self.assertEqual(_file_tags(restored_b), ["A"] * 4 + ["B"] + ["A"] * 4)
            # With both old permits consumed only the active version is billed.
            self.assertEqual(va.usage(), a.stat().st_size)

            # A refused restore leaves no target and can be retried as-is.
            permit = va.pin()
            target = root / "retry.db"
            with self.assertRaises(InvalidSnapshot):
                va.restore(permit, target, verify=lambda con: False)
            self.assertFalse(target.exists())
            self.assertEqual(
                [name for name in os.listdir(root) if name.startswith(".restore-")], []
            )
            va.restore(permit, target, verify=lambda con: True)
            self.assertEqual(_file_tags(target), ["A"] * 4 + ["B"] + ["A"] * 4)
            with self.assertRaises(ValueError):
                va.restore(permit, root / "again.db", verify=lambda con: True)

            print(
                "\nD rejected: active after rejection=%s, usage %d->%d "
                "(candidate peak %d, budget %d); A permit=%s B permit=%s; "
                "refused target absent, retry=%s"
                % (
                    "B",
                    used_before,
                    used_before,
                    observed["peak"],
                    budget,
                    "".join(_file_tags(restored_a)),
                    "".join(_file_tags(restored_b)),
                    "".join(_file_tags(target)),
                )
            )


# --------------------------------------------------------------------------
# Acceptance scenario 3
# --------------------------------------------------------------------------


class BudgetReclaimTest(TestCase):
    def test_pinned_old_version_cannot_be_reclaimed_and_release_unblocks(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            a, b, c = root / "a.db", root / "b.db", root / "c.db"
            _new_base(a, "A")
            _all_rows_child(a, b, list("B" * ROWS))
            _all_rows_child(a, c, list("C" * ROWS))

            size = a.stat().st_size
            page = _page_size(a)
            pages = size // page
            self.assertEqual(b.stat().st_size, size)
            self.assertEqual(c.stat().st_size, size)
            distinct = _union_pages([a, b, c])
            self.assertGreater(len(distinct) * page, 2 * size)

            budget = 2 * size
            archive_dir = root / "archive"
            archive_dir.mkdir()
            va = VersionedArchive(archive_dir, byte_limit=budget)
            va.publish("A", a, verify=lambda con: True)
            permit_a = va.pin()
            va.publish("B", b, verify=lambda con: True)
            permit_b = va.pin()
            used_with_pin = va.usage()
            self.assertEqual(used_with_pin, _union_bytes([a, b]))
            self.assertLessEqual(used_with_pin, budget)

            with self.assertRaises(SpaceError):
                va.publish("C", c, verify=lambda con: True)

            failed_peak = va.last_peak()
            self.assertLessEqual(failed_peak, budget)
            self.assertEqual(va.active(), "B")
            self.assertEqual(va.usage(), used_with_pin)
            self.assertEqual(_directory_bytes(archive_dir), used_with_pin)

            # Only A's permit is released; sources, budget and B stay untouched.
            va.release(permit_a)
            self.assertEqual(va.usage(), size)
            va.publish("C", c, verify=lambda con: True)
            c_usage = va.usage()
            c_peak = va.last_peak()
            self.assertEqual(va.active(), "C")
            self.assertEqual(c_usage, _union_bytes([b, c]))
            self.assertLessEqual(c_peak, budget)
            self.assertLessEqual(c_usage, budget)
            self.assertEqual(_directory_bytes(archive_dir), c_usage)

            restored_b = root / "rb.db"
            va.restore(permit_b, restored_b, verify=lambda con: True)
            self.assertEqual(_file_tags(restored_b), ["B"] * ROWS)
            restored_c = root / "rc.db"
            va.restore(va.pin(), restored_c, verify=lambda con: True)
            self.assertEqual(_file_tags(restored_c), ["C"] * ROWS)

            print(
                "\nbudget=%d S=%d pages=%d distinct3=%d; C refused peak=%d active=%s; "
                "after releasing A usage=%d; C online usage=%d peak=%d; "
                "independent restores B=%s C=%s"
                % (
                    budget,
                    size,
                    pages,
                    len(distinct),
                    failed_peak,
                    "B",
                    size,
                    c_usage,
                    c_peak,
                    "".join(_file_tags(restored_b)),
                    "".join(_file_tags(restored_c)),
                )
            )


# --------------------------------------------------------------------------
# Safety properties
# --------------------------------------------------------------------------


class PermitAndTargetSafetyTest(TestCase):
    def test_targets_are_never_overwritten_and_permits_are_archive_local(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_one, source_two = root / "one.db", root / "two.db"
            _new_base(source_one, "A")
            _new_base(source_two, "Z")

            first_dir, second_dir = root / "first", root / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            first = VersionedArchive(first_dir, byte_limit=1 << 20)
            second = VersionedArchive(second_dir, byte_limit=1 << 20)

            first.publish("one", source_one, verify=lambda con: True)
            second.publish("two", source_two, verify=lambda con: True)
            permit = first.pin()
            foreign = second.pin()

            existing = root / "existing.db"
            existing.write_bytes(b"do not clobber")
            with self.assertRaises(FileExistsError):
                first.restore(permit, existing, verify=lambda con: True)
            self.assertEqual(existing.read_bytes(), b"do not clobber")

            with self.assertRaises(ValueError):
                first.restore(foreign, root / "foreign.db", verify=lambda con: True)
            with self.assertRaises(ValueError):
                first.release(foreign)

            out = root / "out.db"
            first.restore(permit, out, verify=lambda con: True)
            self.assertEqual(_file_tags(out), ["A"] * ROWS)
            with self.assertRaises(ValueError):
                first.release(permit)

            empty = root / "empty"
            empty.mkdir()
            with self.assertRaises(RuntimeError):
                VersionedArchive(empty, byte_limit=1).pin()

    def test_constructor_and_path_rules(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            occupied = root / "occupied"
            occupied.mkdir()
            (occupied / "stray").write_text("x")
            with self.assertRaises(ValueError):
                VersionedArchive(occupied, byte_limit=1 << 20)
            with self.assertRaises(NotADirectoryError):
                VersionedArchive(root / "missing", byte_limit=1 << 20)

            archive_dir = root / "archive"
            archive_dir.mkdir()
            with self.assertRaises(ValueError):
                VersionedArchive(archive_dir, byte_limit=32 * 1024 * 1024 + 1)

            archive = VersionedArchive(archive_dir, byte_limit=1 << 20)
            source = root / "source.db"
            _new_base(source, "A")
            inside = archive_dir / "inside.db"
            shutil.copyfile(source, inside)
            with self.assertRaises(ValueError):
                archive.publish("inside", inside, verify=lambda con: True)
            archive.publish("outside", source, verify=lambda con: True)
            with self.assertRaises(ValueError):
                archive.restore(archive.pin(), archive_dir / "target.db", verify=lambda con: True)
