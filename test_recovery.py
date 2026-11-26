from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase

from demo import run
from recovery import InvalidSnapshot, recover, snapshot


class RecoveryTest(TestCase):
    def test_committed_wal_state_and_restore_gate(self):
        self.assertEqual(run(), (True, [(1,)]))

    def test_invalid_or_rejected_source_leaves_no_stage(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, target = root / "source", root / "target"
            source.write_text("not sqlite")
            with self.assertRaises(sqlite3.DatabaseError):
                recover(source, target, verify=lambda _: True)
            source.unlink()
            with closing(sqlite3.connect(source)) as db:
                db.execute("CREATE TABLE data(x)")
            with self.assertRaises(InvalidSnapshot):
                snapshot(source, target, verify=lambda _: False)
            self.assertEqual(list(root.iterdir()), [source])

    def test_validation_uses_read_only_copy_and_preserves_existing_target(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, target = root / "source", root / "target"
            with closing(sqlite3.connect(source)) as db:
                db.execute("CREATE TABLE data(x)")
            with self.assertRaises(sqlite3.OperationalError):
                snapshot(source, target, verify=lambda db: db.execute("DROP TABLE data"))
            self.assertFalse(target.exists())
            target.write_bytes(b"existing")
            with self.assertRaises(FileExistsError):
                snapshot(source, target, verify=lambda _: True)
            self.assertEqual(target.read_bytes(), b"existing")
