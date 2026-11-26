from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory

from recovery import InvalidSnapshot, recover, snapshot


def rows(db):
    return db.execute("SELECT id FROM completed ORDER BY id").fetchall()


def run():
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        live, archive, restored = [root / name for name in ("live.db", "archive.db", "restored.db")]
        with closing(sqlite3.connect(live)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE completed(id INTEGER PRIMARY KEY)")
            db.execute("INSERT INTO completed VALUES (1)")
            db.commit()
            db.execute("INSERT INTO completed VALUES (2)")
            snapshot(live, archive, verify=lambda copy: rows(copy) == [(1,)])
            db.commit()
        try:
            recover(archive, restored, verify=lambda copy: rows(copy) == [(1,), (2,)])
        except InvalidSnapshot:
            absent = not restored.exists()
        else:
            raise AssertionError("archive should not satisfy the newer recovery target")
        recover(archive, restored, verify=lambda copy: rows(copy) == [(1,)])
        with closing(sqlite3.connect(restored)) as db:
            recovered = rows(db)
    assert absent and recovered == [(1,)]
    return absent, recovered


if __name__ == "__main__":
    absent, recovered = run()
    print("newer recovery target refused; no output:", absent)
    print("recovered committed rows:", recovered)
