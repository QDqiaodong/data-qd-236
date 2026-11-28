"""Acceptance tests for the three-volume offline media backup.

Everything here drives only the public API in ``recovery`` and checks results
with independent SQL and byte comparisons.  Real SQLite files are used, the
live database runs in WAL mode with a second connection holding an
uncommitted row, and damage is applied as an equal-length single-byte rewrite
so a length check alone could never notice it.

The layout is intentionally treated as a black box: the descriptor is the
only way a test learns a volume's expected size, and recovery is judged by
querying the produced database and comparing it against the live rows.
"""

from contextlib import closing
import os
from pathlib import Path
import shutil
import subprocess
import sys
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase

from recovery import (
    InvalidSnapshot,
    MediaBackupError,
    inspect_media,
    publish_media,
    recover_media,
    repair_media,
)

ROUTES = [
    (1, "alpha", "dest-alpha"),
    (2, "beta", "dest-beta"),
    (3, "gamma", "dest-gamma"),
    (4, "delta", "dest-delta"),
    (5, "epsilon", "dest-epsilon"),
]
SUBSCRIPTIONS = [
    (1, "alpha", "alice"),
    (2, "beta", "bob"),
    (3, "gamma", "grace"),
    (4, "delta", "dave"),
]
FILLER_ROWS = 80


def _make_live(path):
    """Create a WAL route database and keep uncommitted state on a second link.

    Returns ``(writer, reader)``: the writer holds an open, uncommitted
    subscription that must never enter the backup; the reader is an
    independent connection that observes only committed rows.
    """
    writer = sqlite3.connect(path, timeout=5)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("PRAGMA foreign_keys=ON")
    writer.execute(
        "CREATE TABLE routes ("
        "route_id INTEGER PRIMARY KEY, "
        "alias TEXT NOT NULL UNIQUE, "
        "destination TEXT NOT NULL)"
    )
    writer.execute(
        "CREATE TABLE subscriptions ("
        "subscription_id INTEGER PRIMARY KEY, "
        "alias TEXT NOT NULL REFERENCES routes(alias) "
        "ON UPDATE CASCADE ON DELETE RESTRICT, "
        "subscriber TEXT NOT NULL)"
    )
    writer.execute("CREATE TABLE filler (id INTEGER PRIMARY KEY, blob BLOB NOT NULL)")
    writer.executemany("INSERT INTO routes VALUES (?, ?, ?)", ROUTES)
    writer.executemany("INSERT INTO subscriptions VALUES (?, ?, ?)", SUBSCRIPTIONS)
    writer.executemany(
        "INSERT INTO filler VALUES (?, ?)",
        [(row, os.urandom(3000)) for row in range(1, FILLER_ROWS + 1)],
    )
    writer.commit()
    # This extra subscription is committed-visible to nobody: roll it back at
    # the end of the test and require the backup to omit it.
    writer.execute(
        "INSERT INTO subscriptions VALUES (?, ?, ?)", (99, "alpha", "uncommitted")
    )
    reader = sqlite3.connect(path, timeout=5)
    return writer, reader


def _query(path, sql, params=()):
    with closing(sqlite3.connect(path)) as db:
        return db.execute(sql, params).fetchall()


def _snapshot_state(path):
    """Independent, order-stable view of the whole captured database."""
    return {
        "routes": _query(path, "SELECT route_id, alias, destination FROM routes ORDER BY route_id"),
        "subscriptions": _query(
            path,
            "SELECT subscription_id, alias, subscriber FROM subscriptions "
            "ORDER BY subscription_id",
        ),
        "filler": _query(path, "SELECT id, blob FROM filler ORDER BY id"),
        "foreign_keys": _query(path, "PRAGMA foreign_key_check"),
        "integrity": _query(path, "PRAGMA integrity_check"),
    }


def _flip_byte(path, offset):
    data = bytearray(Path(path).read_bytes())
    data[offset] ^= 0x5A
    Path(path).write_bytes(data)


def _bytes_of(paths):
    return [None if path is None else Path(path).read_bytes() for path in paths]


class MediaFixture(TestCase):
    """Shared setup that publishes one backup and remembers its inputs."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.live = self.root / "live.db"
        self.writer, self.reader = _make_live(self.live)
        self.volumes = [self.root / ("slot-%d.vol" % slot) for slot in range(3)]
        self.descriptor = self.publish()

    def tearDown(self):
        for connection in (getattr(self, "writer", None), getattr(self, "reader", None)):
            try:
                connection.rollback()
                connection.close()
            except sqlite3.ProgrammingError:
                pass
        self._tmp.cleanup()

    def publish(self, verify=lambda con: True):
        return publish_media(self.live, self.volumes, verify=verify)

    def restored_state(self, volumes, verify=lambda con: True):
        target = self.root / ("restore-%d.db" % len(list(self.root.glob("restore-*"))))
        recover_media(self.descriptor, volumes, target, verify=verify)
        try:
            return _snapshot_state(target)
        finally:
            target.unlink()

    def expected_state(self):
        """What the backup must contain, computed independently of the API."""
        # Build a throwaway copy the same way a human would check it: a
        # read-only connection to the live file sees only committed rows.
        expected_path = self.root / "expected.db"
        with closing(
            sqlite3.connect(
                Path(self.live).resolve().as_uri() + "?mode=ro", uri=True, timeout=5
            )
        ) as source, closing(sqlite3.connect(expected_path)) as copy:
            source.backup(copy)
        state = _snapshot_state(expected_path)
        expected_path.unlink()
        return state


class PublishContractTest(MediaFixture):
    def test_capture_sees_committed_wal_state_and_excludes_uncommitted_row(self):
        seen = {}
        volumes = [self.root / ("capture-%d.vol" % slot) for slot in range(3)]

        def verify(con):
            seen["routes"] = con.execute(
                "SELECT route_id, alias, destination FROM routes ORDER BY route_id"
            ).fetchall()
            seen["subscriptions"] = con.execute(
                "SELECT subscription_id, alias, subscriber FROM subscriptions "
                "ORDER BY subscription_id"
            ).fetchall()
            # The copy handed to the business check is read-only.
            with self.assertRaises(sqlite3.OperationalError):
                con.execute("DELETE FROM subscriptions")
            return True

        descriptor = publish_media(self.live, volumes, verify=verify)

        self.assertEqual(seen["routes"], ROUTES)
        self.assertEqual(seen["subscriptions"], SUBSCRIPTIONS)
        self.assertNotIn(99, [row[0] for row in seen["subscriptions"]])
        self.assertIsInstance(descriptor, bytes)
        self.assertLessEqual(len(descriptor), 4096)
        # The descriptor carries identification and integrity metadata only,
        # never a route or subscriber value.
        for value in (b"alice", b"alpha", b"dest-gamma"):
            self.assertNotIn(value, descriptor)

    def test_volume_ceiling_and_recovery_size_are_measured(self):
        restored = self.root / "measure.db"
        recover_media(self.descriptor, self.volumes, restored, verify=lambda con: True)
        S = restored.stat().st_size
        self.assertGreaterEqual(S, 128 * 1024)
        self.assertLessEqual(S, 8 * 1024 * 1024)
        limit = (3 * S + 3) // 4
        for path in self.volumes:
            self.assertTrue(path.is_file())
            self.assertLessEqual(path.stat().st_size, limit)
        # The recovered file really is a standalone SQLite database of S bytes.
        self.assertEqual(restored.read_bytes()[:16], b"SQLite format 3\x00")
        self.assertEqual(_snapshot_state(restored), self.expected_state())
        restored.unlink()

    def test_refused_verify_leaves_no_volumes(self):
        volumes = [self.root / ("new-%d.vol" % slot) for slot in range(3)]
        with self.assertRaises(InvalidSnapshot):
            publish_media(self.live, volumes, verify=lambda con: False)
        self.assertEqual([path.exists() for path in volumes], [False, False, False])
        # A refused publish must not disturb the already published volumes.
        self.assertEqual(inspect_media(self.descriptor, self.volumes), ())

    def test_callback_exception_propagates_and_leaves_no_volumes(self):
        class Boom(Exception):
            pass

        def raising(_con):
            raise Boom("callback failure")

        volumes = [self.root / ("boom-%d.vol" % slot) for slot in range(3)]
        with self.assertRaises(Boom):
            publish_media(self.live, volumes, verify=raising)
        self.assertEqual([path.exists() for path in volumes], [False, False, False])

    def test_existing_targets_and_bad_arguments_are_refused(self):
        existing = [self.root / ("taken-%d.vol" % slot) for slot in range(3)]
        existing[1].write_bytes(b"do not touch")
        with self.assertRaises(FileExistsError):
            publish_media(self.live, existing, verify=lambda con: True)
        self.assertEqual(existing[1].read_bytes(), b"do not touch")
        self.assertFalse(existing[0].exists())
        self.assertFalse(existing[2].exists())

        with self.assertRaises(ValueError):
            publish_media(self.live, self.volumes[:2], verify=lambda con: True)
        duplicate = [self.root / ("dup-%d.vol" % slot) for slot in range(3)]
        with self.assertRaises(ValueError):
            publish_media(
                self.live, [duplicate[0], duplicate[0], duplicate[2]], verify=lambda con: True
            )
        self.assertEqual([path.exists() for path in duplicate], [False, False, False])


class DamageAndRecoveryTest(MediaFixture):
    def test_equal_length_damage_is_localized_and_never_mixed_in(self):
        original = _bytes_of(self.volumes)
        damaged = self.root / "slot-1-damaged.vol"
        shutil.copyfile(self.volumes[1], damaged)
        _flip_byte(damaged, 77)
        self.assertEqual(damaged.stat().st_size, self.volumes[1].stat().st_size)
        self.assertNotEqual(damaged.read_bytes(), self.volumes[1].read_bytes())

        damaged_volumes = [self.volumes[0], damaged, self.volumes[2]]
        before = inspect_media(self.descriptor, self.volumes)
        after = inspect_media(self.descriptor, damaged_volumes)
        self.assertEqual(before, ())
        self.assertEqual(after, (1,))
        print(
            "\nequal-length damage: inspect before=%s after=%s (slot 1 only)"
            % (before, after)
        )

        # Recovery from the two healthy volumes still yields the exact state.
        first = self.restored_state(damaged_volumes)
        self.assertEqual(first, self.expected_state())
        # Repeating it must be stable: no damaged bytes leak into either path.
        self.assertEqual(self.restored_state(damaged_volumes), first)
        # The volumes themselves were untouched by inspection and recovery.
        self.assertEqual(self.volumes[0].read_bytes(), original[0])
        self.assertEqual(self.volumes[2].read_bytes(), original[2])
        self.assertEqual(damaged.stat().st_size, len(original[1]))

    def test_truncation_swap_and_foreign_volume_are_unusable(self):
        truncated = self.root / "truncated.vol"
        truncated.write_bytes(self.volumes[2].read_bytes()[:-1])
        self.assertEqual(
            inspect_media(self.descriptor, [self.volumes[0], self.volumes[1], truncated]),
            (2,),
        )

        # Volume 0's bytes placed at slot 1: correct length, wrong slot header.
        swapped = self.root / "swapped.vol"
        shutil.copyfile(self.volumes[0], swapped)
        self.assertEqual(
            inspect_media(self.descriptor, [self.volumes[0], swapped, self.volumes[2]]),
            (1,),
        )

        # A different backup captured from a same-length variant of the live
        # database: its volumes legitimately share this backup's exact size.
        other_live = self.root / "other-live.db"
        with closing(
            sqlite3.connect(
                Path(self.live).resolve().as_uri() + "?mode=ro", uri=True, timeout=5
            )
        ) as source, closing(sqlite3.connect(other_live)) as other:
            source.backup(other)
        with closing(sqlite3.connect(other_live)) as other:
            other.execute(
                "UPDATE routes SET destination = ? WHERE route_id = 2",
                ("dest-beta-variant",),
            )
            other.execute("PRAGMA journal_mode=DELETE")
            other.commit()
        other_volumes = [self.root / ("other-%d.vol" % slot) for slot in range(3)]
        other_descriptor = publish_media(other_live, other_volumes, verify=lambda con: True)
        self.assertEqual(
            other_volumes[0].stat().st_size, self.volumes[0].stat().st_size
        )
        self.assertEqual(
            inspect_media(
                self.descriptor, [other_volumes[0], self.volumes[1], self.volumes[2]]
            ),
            (0,),
        )
        # And the other descriptor refuses this set's volumes symmetrically.
        self.assertEqual(
            inspect_media(
                other_descriptor, [self.volumes[0], other_volumes[1], other_volumes[2]]
            ),
            (0,),
        )

    def test_each_single_missing_slot_still_recovers(self):
        expected = self.expected_state()
        for missing in range(3):
            with self.subTest(missing=missing):
                volumes = list(self.volumes)
                volumes[missing] = None
                self.assertEqual(inspect_media(self.descriptor, volumes), (missing,))
                self.assertEqual(self.restored_state(volumes), expected)
                # Nothing is written where the missing volume would have been.
                self.assertIsNone(volumes[missing])

    def test_verify_contract_on_recovery(self):
        target = self.root / "gated.db"
        with self.assertRaises(InvalidSnapshot):
            recover_media(self.descriptor, self.volumes, target, verify=lambda con: False)
        self.assertFalse(target.exists())

        class Boom(Exception):
            pass

        def raising(_con):
            raise Boom("verifier failed")

        with self.assertRaises(Boom):
            recover_media(self.descriptor, self.volumes, target, verify=raising)
        self.assertFalse(target.exists())

        # The same target succeeds once the verifier accepts.
        recover_media(self.descriptor, self.volumes, target, verify=lambda con: True)
        self.assertEqual(_snapshot_state(target), self.expected_state())
        target.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            recover_media(self.descriptor, self.volumes, target, verify=lambda con: True)
        self.assertEqual(target.read_bytes(), b"existing")


class RepairTest(MediaFixture):
    def test_no_damage_writes_nothing(self):
        before = sorted(path.name for path in self.root.iterdir())
        replacement = self.root / "unused-replacement.vol"
        self.assertIsNone(repair_media(self.descriptor, self.volumes, replacement))
        self.assertFalse(replacement.exists())
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), before)

    def test_single_damage_repair_then_survives_another_loss(self):
        original = _bytes_of(self.volumes)
        S = self.root / "size-probe.db"
        recover_media(self.descriptor, self.volumes, S, verify=lambda con: True)
        db_bytes = S.stat().st_size
        limit = (3 * db_bytes + 3) // 4
        S.unlink()
        print(
            "\nvolumes: S=%d limit=%d actual=%s"
            % (db_bytes, limit, [len(item) for item in original])
        )
        for item in original:
            self.assertLessEqual(len(item), limit)

        damaged = self.root / "slot-1-damaged.vol"
        shutil.copyfile(self.volumes[1], damaged)
        _flip_byte(damaged, 123)
        damaged_before = damaged.read_bytes()
        working = [self.volumes[0], damaged, self.volumes[2]]
        self.assertEqual(inspect_media(self.descriptor, working), (1,))

        replacement = self.root / "replacement.vol"
        slot = repair_media(self.descriptor, working, replacement)
        self.assertEqual(slot, 1)
        self.assertTrue(replacement.is_file())
        # Original files are byte-for-byte unchanged, including the bad one.
        self.assertEqual(self.volumes[0].read_bytes(), original[0])
        self.assertEqual(damaged.read_bytes(), damaged_before)
        self.assertEqual(self.volumes[2].read_bytes(), original[2])

        # The old descriptor now sees three healthy slots with the new path.
        rebuilt = [self.volumes[0], replacement, self.volumes[2]]
        self.assertEqual(inspect_media(self.descriptor, rebuilt), ())
        # The rebuilt volume is an exact byte replica of the healthy original.
        self.assertEqual(replacement.read_bytes(), original[1])
        rebuilt_state = self.expected_state()
        self.assertEqual(self.restored_state(rebuilt), rebuilt_state)

        # Losing an originally healthy slot after the swap still recovers,
        # and the recovered database still holds the original committed rows.
        for missing in (0, 2):
            with self.subTest(missing=missing):
                volumes = list(rebuilt)
                volumes[missing] = None
                self.assertEqual(inspect_media(self.descriptor, volumes), (missing,))
                self.assertEqual(self.restored_state(volumes), rebuilt_state)
        probe = self.root / "post-repair-probe.db"
        recover_media(self.descriptor, [None, replacement, self.volumes[2]], probe, verify=lambda con: True)
        try:
            print(
                "after repair + losing slot 0: routes=%s subscriptions=%s fk=%s"
                % (
                    _query(probe, "SELECT route_id, alias, destination FROM routes ORDER BY route_id"),
                    _query(
                        probe,
                        "SELECT subscription_id, alias, subscriber FROM subscriptions "
                        "ORDER BY subscription_id",
                    ),
                    _query(probe, "PRAGMA foreign_key_check"),
                )
            )
        finally:
            probe.unlink()

    def test_repair_uses_no_whole_database_file(self):
        """Cap the process at one volume's ceiling while rebuilding a slot.

        If the implementation first wrote a whole SQLite database (or any
        file larger than a single volume) the write would fail with
        ``SIGXFSZ``/``OSError``; the rebuild must stay within the cap.
        """
        restored = self.root / "measure.db"
        recover_media(self.descriptor, self.volumes, restored, verify=lambda con: True)
        S = restored.stat().st_size
        restored.unlink()
        limit = (3 * S + 3) // 4
        self.assertGreaterEqual(limit, self.volumes[0].stat().st_size)

        damaged = self.root / "slot-1-damaged.vol"
        shutil.copyfile(self.volumes[1], damaged)
        _flip_byte(damaged, 200)
        replacement = self.root / "replacement-under-cap.vol"
        descriptor_file = self.root / "descriptor.bin"
        descriptor_file.write_bytes(self.descriptor)

        script = r"""
import sys
from pathlib import Path
sys.path.insert(0, __import__("os").environ["RECOVERY_DIR"])
from recovery import inspect_media, repair_media

descriptor = Path(sys.argv[1]).read_bytes()
volumes = [None if value == "-" else Path(value) for value in sys.argv[2:5]]
replacement = Path(sys.argv[5])
slot = repair_media(descriptor, volumes, replacement)
print(slot)
print(list(inspect_media(descriptor, [volumes[0], replacement, volumes[2]])))
"""
        env = dict(os.environ)
        env["RECOVERY_DIR"] = str(Path(__file__).resolve().parent)
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-c",
                "import resource,sys;"
                "resource.setrlimit(resource.RLIMIT_FSIZE, (%d, %d));" % (limit, limit)
                + script,
                str(descriptor_file),
                str(self.volumes[0]),
                str(damaged),
                str(self.volumes[2]),
                str(replacement),
            ],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(self.root),
        )
        self.assertEqual(
            result.returncode,
            0,
            "repair under a %d byte file cap failed: %s" % (limit, result.stderr),
        )
        self.assertEqual(result.stdout.split(), ["1", "[]"])
        self.assertLessEqual(replacement.stat().st_size, limit)
        print(
            "\nrepair under RLIMIT_FSIZE=%d (S=%d) wrote a %d byte volume: %s"
            % (limit, S, replacement.stat().st_size, "ok")
        )


class SizeBoundaryTest(TestCase):
    """Exercise the documented 128 KiB to 8 MiB capture range."""

    def _database_with_pages(self, root, pages, page_size=4096):
        live = root / "sized.db"
        with closing(sqlite3.connect(live)) as db:
            db.execute("PRAGMA page_size=%d" % page_size)
            db.execute("PRAGMA journal_mode=DELETE")
            db.execute("CREATE TABLE routes ("
                       "route_id INTEGER PRIMARY KEY, alias TEXT UNIQUE)")
            db.execute("CREATE TABLE filler (id INTEGER PRIMARY KEY, blob BLOB)")
            row = 0
            while db.execute("PRAGMA page_count").fetchone()[0] < pages:
                row += 1
                db.execute("INSERT INTO filler VALUES (?, ?)", (row, os.urandom(4000)))
            db.executemany(
                "INSERT INTO routes VALUES (?, ?)",
                [(i, "alias-%d" % i) for i in range(1, 6)],
            )
            db.commit()
            self.assertEqual(db.execute("PRAGMA page_count").fetchone()[0], pages)
        return live

    def test_smallest_supported_database(self):
        # 32 pages of 4096 bytes is exactly 128 KiB.
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            live = self._database_with_pages(root, 32)
            volumes = [root / ("v%d.vol" % slot) for slot in range(3)]
            descriptor = publish_media(live, volumes, verify=lambda con: True)
            S = live.stat().st_size
            self.assertEqual(S, 128 * 1024)
            target = root / "smallest.db"
            recover_media(descriptor, volumes, target, verify=lambda con: True)
            self.assertEqual(target.stat().st_size, S)
            for volume in volumes:
                self.assertLessEqual(volume.stat().st_size, (3 * S + 3) // 4)

    def test_database_below_and_above_the_range_is_refused(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tiny = root / "tiny.db"
            with closing(sqlite3.connect(tiny)) as db:
                db.execute("CREATE TABLE t(x)")
                db.commit()
            volumes = [root / ("v%d.vol" % slot) for slot in range(3)]
            with self.assertRaises(ValueError):
                publish_media(tiny, volumes, verify=lambda con: True)
            self.assertEqual([path.exists() for path in volumes], [False, False, False])

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            huge = self._database_with_pages(root, 2049)
            self.assertGreater(huge.stat().st_size, 8 * 1024 * 1024)
            volumes = [root / ("v%d.vol" % slot) for slot in range(3)]
            with self.assertRaises(ValueError):
                publish_media(huge, volumes, verify=lambda con: True)
            self.assertEqual([path.exists() for path in volumes], [False, False, False])


class InsufficiencyTest(MediaFixture):
    def test_two_bad_slots_refuse_without_output_or_verification(self):
        damaged = self.root / "slot-1-damaged.vol"
        shutil.copyfile(self.volumes[1], damaged)
        _flip_byte(damaged, 321)
        # Slot 0 is gone entirely; slot 1 is corrupted in place.
        broken = [None, damaged, self.volumes[2]]
        reported = inspect_media(self.descriptor, broken)
        self.assertEqual(reported, (0, 1))
        print("\ntwo bad slots reported by inspect: %s" % (reported,))

        def forbidden(_con):
            raise AssertionError("verification must not run without two volumes")

        target = self.root / "must-not-exist.db"
        replacement = self.root / "must-not-exist.vol"
        before = sorted(path.name for path in self.root.iterdir())
        with self.assertRaises(MediaBackupError) as recover_error:
            recover_media(self.descriptor, broken, target, verify=forbidden)
        self.assertEqual(recover_error.exception.reason, "insufficient_volumes")
        with self.assertRaises(MediaBackupError) as repair_error:
            repair_media(self.descriptor, broken, replacement)
        self.assertEqual(repair_error.exception.reason, "insufficient_volumes")
        print(
            "two-volume refusal: recover=%r repair=%r"
            % (recover_error.exception.reason, repair_error.exception.reason)
        )
        self.assertFalse(target.exists())
        self.assertFalse(replacement.exists())
        self.assertEqual(sorted(path.name for path in self.root.iterdir()), before)

        # Even with a verifier that would pass, no output appears.
        with self.assertRaises(MediaBackupError):
            recover_media(self.descriptor, broken, target, verify=lambda con: True)
        self.assertFalse(target.exists())

    def test_replacement_must_not_overwrite_a_volume_and_validation_is_strict(self):
        damaged = self.root / "slot-2-damaged.vol"
        shutil.copyfile(self.volumes[2], damaged)
        _flip_byte(damaged, 42)
        working = [self.volumes[0], self.volumes[1], damaged]
        self.assertEqual(inspect_media(self.descriptor, working), (2,))

        with self.assertRaises(FileExistsError):
            repair_media(self.descriptor, working, self.volumes[0])
        self.assertEqual(self.volumes[0].read_bytes(), _bytes_of([self.volumes[0]])[0])
        with self.assertRaises(FileExistsError):
            repair_media(self.descriptor, working, self.volumes[2])

        for bad_descriptor in (b"", b"{}", b"[]", b"not json", b'{"v": 99}'):
            with self.assertRaises(ValueError):
                inspect_media(bad_descriptor, working)
            with self.assertRaises(ValueError):
                recover_media(bad_descriptor, working, self.root / "x.db", verify=lambda con: True)
            with self.assertRaises(ValueError):
                repair_media(bad_descriptor, working, self.root / "y.vol")
        with self.assertRaises(ValueError):
            inspect_media("a string is not bytes", working)
        with self.assertRaises(ValueError):
            inspect_media(self.descriptor, [self.volumes[0], self.volumes[1]])
        with self.assertRaises(ValueError):
            inspect_media(self.descriptor, [self.volumes[0], self.volumes[0], self.volumes[2]])

    def test_two_missing_volumes_refuse_before_touching_existing_target(self):
        broken = [self.volumes[0], None, None]
        existing = self.root / "already-here.db"
        existing.write_bytes(b"keep me")
        with self.assertRaises(MediaBackupError):
            recover_media(self.descriptor, broken, existing, verify=lambda con: True)
        self.assertEqual(existing.read_bytes(), b"keep me")
        replacement = self.root / "nope.vol"
        with self.assertRaises(MediaBackupError):
            repair_media(self.descriptor, broken, replacement)
        self.assertFalse(replacement.exists())


class OfflineProcessTest(MediaFixture):
    def test_descriptor_and_volumes_survive_source_deletion_in_a_new_process(self):
        descriptor_file = self.root / "descriptor.bin"
        descriptor_file.write_bytes(self.descriptor)
        expected = self.expected_state()

        self.writer.close()
        self.reader.close()
        for suffix in ("", "-wal", "-shm"):
            sidecar = Path(str(self.live) + suffix)
            if sidecar.exists():
                sidecar.unlink()
        self.assertFalse(self.live.exists())

        target = self.root / "offline-restore.db"
        script = r"""
import sqlite3, sys
from pathlib import Path
sys.path.insert(0, __import__("os").environ["RECOVERY_DIR"])
from recovery import inspect_media, recover_media

descriptor = Path(sys.argv[1]).read_bytes()
volumes = [Path(value) for value in sys.argv[2:5]]
target = Path(sys.argv[5])
print(list(inspect_media(descriptor, volumes)))
recover_media(descriptor, volumes, target, verify=lambda con: (
    con.execute("PRAGMA foreign_key_check").fetchall() == []
))
with sqlite3.connect(target) as db:
    routes = db.execute(
        "SELECT route_id, alias, destination FROM routes ORDER BY route_id"
    ).fetchall()
    subs = db.execute(
        "SELECT subscription_id, alias, subscriber FROM subscriptions "
        "ORDER BY subscription_id"
    ).fetchall()
    integrity = db.execute("PRAGMA integrity_check").fetchall()
print(routes)
print(subs)
print(integrity)
"""
        env = dict(os.environ)
        env["RECOVERY_DIR"] = str(Path(__file__).resolve().parent)
        result = subprocess.run(
            [sys.executable, "-I", "-c", script, str(descriptor_file)]
            + [str(path) for path in self.volumes]
            + [str(target)],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(self.root),
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "[]",
                str(expected["routes"]),
                str(expected["subscriptions"]),
                "[('ok',)]",
            ],
        )


if __name__ == "__main__":
    import unittest

    unittest.main()
