"""Acceptance tests for the scoped, backup-driven route repair.

Every test uses real SQLite files: the live database runs in WAL mode so a
second connection can inspect committed state while the repair transaction
is open, and the backup is materialized through the existing ``recover``
entry point and is therefore an independent database file.
"""

from contextlib import closing
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase

from recovery import (
    InvalidSnapshot,
    RouteRepairError,
    RouteRepairPlan,
    apply_route_repair,
    plan_route_repair,
    recover,
)

LIVE_ROUTES = [
    (10, "alpha", "dest-alpha-live"),
    (20, "beta", "dest-beta-live"),
    (30, "gamma", "dest-gamma-live"),
    (40, "delta", "dest-delta-live"),
]
LIVE_SUBSCRIPTIONS = [
    (1, "alpha", "alice"),
    (2, "beta", "bob"),
    (3, "gamma", "grace"),
    (4, "delta", "dave"),
]
SELECTED_IDS = [10, 20, 40]


def _connect(path):
    con = sqlite3.connect(path, timeout=5)
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _create_routed_database(path):
    with closing(_connect(path)) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            "CREATE TABLE routes ("
            "route_id INTEGER PRIMARY KEY, "
            "alias TEXT NOT NULL UNIQUE, "
            "destination TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TABLE subscriptions ("
            "subscription_id INTEGER PRIMARY KEY, "
            "alias TEXT NOT NULL REFERENCES routes(alias) "
            "ON UPDATE CASCADE ON DELETE RESTRICT, "
            "subscriber TEXT NOT NULL)"
        )
        db.executemany("INSERT INTO routes VALUES (?, ?, ?)", LIVE_ROUTES)
        db.executemany("INSERT INTO subscriptions VALUES (?, ?, ?)", LIVE_SUBSCRIPTIONS)
        db.commit()


def _recover_backup(tmp, route_40_alias):
    """Produce an independent backup file through the existing recover()."""
    source = tmp / "preincident.db"
    with closing(_connect(source)) as db:
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute(
            "CREATE TABLE routes ("
            "route_id INTEGER PRIMARY KEY, "
            "alias TEXT NOT NULL UNIQUE, "
            "destination TEXT NOT NULL)"
        )
        db.execute(
            "CREATE TABLE subscriptions ("
            "subscription_id INTEGER PRIMARY KEY, "
            "alias TEXT NOT NULL REFERENCES routes(alias) "
            "ON UPDATE CASCADE ON DELETE RESTRICT, "
            "subscriber TEXT NOT NULL)"
        )
        # Routes 10 and 20 have swapped aliases and repaired destinations;
        # route 30 is untouched; route 40 gets a repaired destination and the
        # alias dictated by the scenario.
        db.executemany(
            "INSERT INTO routes VALUES (?, ?, ?)",
            [
                (10, "beta", "dest-beta-backup"),
                (20, "alpha", "dest-alpha-backup"),
                (30, "gamma", "dest-gamma-live"),
                (40, route_40_alias, "dest-route40-backup"),
            ],
        )
        db.executemany(
            "INSERT INTO subscriptions VALUES (?, ?, ?)",
            [(1, "beta", "backup-only-sub")],
        )
        db.commit()
    backup = tmp / "backup.db"
    recover(source, backup, verify=lambda con: True)
    return backup


def _all_routes(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute(
            "SELECT route_id, alias, destination FROM routes ORDER BY route_id"
        ).fetchall()


def _subscription_bindings(path):
    """Independent SQL: resolve every subscription to its route and target."""
    with closing(sqlite3.connect(path)) as db:
        return db.execute(
            "SELECT s.subscription_id, s.subscriber, s.alias, "
            "r.route_id, r.destination "
            "FROM subscriptions AS s JOIN routes AS r ON s.alias = r.alias "
            "ORDER BY s.subscription_id"
        ).fetchall()


def _foreign_key_violations(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute("PRAGMA foreign_key_check").fetchall()


class RouteRepairAcceptanceTest(TestCase):
    def _run_full_scenario(self, route_40_alias):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            live = tmp / "live.db"
            _create_routed_database(live)
            backup = _recover_backup(tmp, route_40_alias)

            live_before = _all_routes(live)
            backup_before = _all_routes(backup)

            # Selecting only route 10 cannot work: its planned alias beta is
            # held by route 20, which is outside that one-route scope.
            with self.assertRaises(RouteRepairError) as conflict:
                plan_route_repair(backup, live, [10])
            self.assertEqual(conflict.exception.reason, "alias_conflict")

            plan = plan_route_repair(backup, live, SELECTED_IDS)
            self.assertIsInstance(plan, RouteRepairPlan)
            self.assertEqual(
                plan.changes,
                (
                    (10, "alpha", "dest-alpha-live", "beta", "dest-beta-backup"),
                    (20, "beta", "dest-beta-live", "alpha", "dest-alpha-backup"),
                    (
                        40,
                        "delta",
                        "dest-delta-live",
                        route_40_alias,
                        "dest-route40-backup",
                    ),
                ),
            )
            self.assertEqual(
                dict(plan.alias_occupancy),
                {"alpha": 10, "beta": 20, route_40_alias: (None if route_40_alias == "next" else 40)},
            )
            # The preview is strictly read-only on both databases.
            self.assertEqual(_all_routes(live), live_before)
            self.assertEqual(_all_routes(backup), backup_before)

            # A second connection keeps running between preview and apply:
            # a brand-new subscription plus an unrelated route edit.
            with closing(_connect(live)) as other:
                other.execute(
                    "INSERT INTO subscriptions VALUES (?, ?, ?)",
                    (5, "alpha", "newbie"),
                )
                other.execute(
                    "UPDATE routes SET destination = ? WHERE route_id = 30",
                    ("dest-gamma-external",),
                )
                other.commit()

            # The plan carries every value it needs; the backup may move away.
            moved_backup = tmp / "backup-moved.db"
            os.rename(backup, moved_backup)

            self.assertIsNone(
                apply_route_repair(live, plan, verify=lambda con: True)
            )

            routes_after = _all_routes(live)
            self.assertEqual(
                routes_after,
                [
                    (10, "beta", "dest-beta-backup"),
                    (20, "alpha", "dest-alpha-backup"),
                    (30, "gamma", "dest-gamma-external"),
                    (40, route_40_alias, "dest-route40-backup"),
                ],
            )
            # Every subscription resolves through its alias to the same
            # logical route_id it was bound to before the commit.
            bindings = _subscription_bindings(live)
            self.assertEqual(
                bindings,
                [
                    (1, "alice", "beta", 10, "dest-beta-backup"),
                    (2, "bob", "alpha", 20, "dest-alpha-backup"),
                    (3, "grace", "gamma", 30, "dest-gamma-external"),
                    (4, "dave", route_40_alias, 40, "dest-route40-backup"),
                    # Inserted after the preview, still bound pre-commit to
                    # alpha's owner (route 10), so it follows route 10.
                    (5, "newbie", "beta", 10, "dest-beta-backup"),
                ],
            )
            # Subscription ids and subscriber strings are preserved verbatim.
            with closing(sqlite3.connect(live)) as db:
                raw_subs = db.execute(
                    "SELECT subscription_id, subscriber FROM subscriptions "
                    "ORDER BY subscription_id"
                ).fetchall()
            self.assertEqual(
                raw_subs,
                [
                    (1, "alice"),
                    (2, "bob"),
                    (3, "grace"),
                    (4, "dave"),
                    (5, "newbie"),
                ],
            )
            self.assertEqual(_foreign_key_violations(live), [])

            dave_alias = next(row[2] for row in bindings if row[1] == "dave")
            dave_target = next(row[4] for row in bindings if row[1] == "dave")
            print(
                "\nscenario 1 (backup route 40 alias=%r): "
                "route 40 subscription uses alias=%r, SQL destination=%r"
                % (route_40_alias, dave_alias, dave_target)
            )
            return routes_after, bindings

    def test_two_backup_variants_both_commit_and_keep_subscription_identity(self):
        # Variant A: route 40 moves onto the previously free alias "next".
        routes_a, bindings_a = self._run_full_scenario("next")
        # Variant B: route 40 keeps "delta" and only repairs its destination.
        routes_b, bindings_b = self._run_full_scenario("delta")

        self.assertEqual(routes_a[3], (40, "next", "dest-route40-backup"))
        self.assertEqual(routes_b[3], (40, "delta", "dest-route40-backup"))
        dave_a = next(row for row in bindings_a if row[1] == "dave")
        dave_b = next(row for row in bindings_b if row[1] == "dave")
        self.assertEqual(dave_a, (4, "dave", "next", 40, "dest-route40-backup"))
        self.assertEqual(dave_b, (4, "dave", "delta", 40, "dest-route40-backup"))

    def test_out_of_scope_alias_takeover_makes_the_plan_stale(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            live = tmp / "live.db"
            _create_routed_database(live)
            backup = _recover_backup(tmp, "next")

            plan = plan_route_repair(backup, live, SELECTED_IDS)
            state_after_preview = (_all_routes(live), _subscription_bindings(live))

            def fail_verify(_con):
                raise AssertionError("verification must not run for a stale plan")

            with closing(_connect(live)) as other:
                # Only the out-of-scope route changes: it grabs "next", which
                # the preview recorded as a free planned alias.
                other.execute(
                    "UPDATE routes SET alias = ? WHERE route_id = 30", ("next",)
                )
                other.commit()
                external_state = (_all_routes(live), _subscription_bindings(live))

            with self.assertRaises(RouteRepairError) as stale:
                apply_route_repair(live, plan, verify=fail_verify)
            self.assertEqual(stale.exception.reason, "stale_plan")

            # Nothing moved: the database equals the post-external-commit state.
            self.assertEqual(_all_routes(live), external_state[0])
            self.assertEqual(_subscription_bindings(live), external_state[1])
            self.assertEqual(_foreign_key_violations(live), [])
            # And the external commit really is the only difference vs preview.
            self.assertNotEqual(external_state, state_after_preview)

    def test_verify_runs_on_final_state_while_other_readers_see_old_state(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            live = tmp / "live.db"
            _create_routed_database(live)
            backup = _recover_backup(tmp, "next")
            plan = plan_route_repair(backup, live, SELECTED_IDS)

            with closing(_connect(live)) as other:
                other.execute(
                    "INSERT INTO subscriptions VALUES (?, ?, ?)",
                    (5, "alpha", "newbie"),
                )
                other.execute(
                    "UPDATE routes SET destination = ? WHERE route_id = 30",
                    ("dest-gamma-external",),
                )
                other.commit()

            original_routes = _all_routes(live)
            original_bindings = _subscription_bindings(live)

            # A long-lived second connection; WAL lets it read committed
            # state while the writer transaction stays open.
            reader = sqlite3.connect(live, timeout=5)
            seen_inside_callback = {}

            def verify_repair(con):
                # The verifier's own connection already holds the final,
                # not-yet-committed associations.
                final = con.execute(
                    "SELECT s.subscriber, s.alias, r.route_id, r.destination "
                    "FROM subscriptions AS s JOIN routes AS r "
                    "ON s.alias = r.alias ORDER BY s.subscription_id"
                ).fetchall()
                seen_inside_callback["final"] = final
                # Meanwhile the independent reader still sees pre-repair data.
                seen_inside_callback["reader"] = reader.execute(
                    "SELECT route_id, alias FROM routes ORDER BY route_id"
                ).fetchall()
                raise RuntimeError("verifier deliberately rejects")

            with self.assertRaises(RuntimeError) as boom:
                apply_route_repair(live, plan, verify=verify_repair)
            self.assertEqual(str(boom.exception), "verifier deliberately rejects")
            reader.close()

            self.assertEqual(
                seen_inside_callback["final"],
                [
                    ("alice", "beta", 10, "dest-beta-backup"),
                    ("bob", "alpha", 20, "dest-alpha-backup"),
                    ("grace", "gamma", 30, "dest-gamma-external"),
                    ("dave", "next", 40, "dest-route40-backup"),
                    ("newbie", "beta", 10, "dest-beta-backup"),
                ],
            )
            self.assertEqual(
                seen_inside_callback["reader"],
                [(10, "alpha"), (20, "beta"), (30, "gamma"), (40, "delta")],
            )

            # Rollback is total: the external reader still sees the old world.
            self.assertEqual(_all_routes(live), original_routes)
            self.assertEqual(_subscription_bindings(live), original_bindings)
            self.assertEqual(_foreign_key_violations(live), [])
            print(
                "\nscenario 3 after verifier failure, live bindings: %s"
                % (original_bindings,)
            )

            # The plan stays valid because nothing committed; retry with a
            # passing verifier and get the full scenario-1 repair result.
            apply_route_repair(live, plan, verify=lambda con: True)
            self.assertEqual(
                _all_routes(live),
                [
                    (10, "beta", "dest-beta-backup"),
                    (20, "alpha", "dest-alpha-backup"),
                    (30, "gamma", "dest-gamma-external"),
                    (40, "next", "dest-route40-backup"),
                ],
            )
            self.assertEqual(
                _subscription_bindings(live),
                [
                    (1, "alice", "beta", 10, "dest-beta-backup"),
                    (2, "bob", "alpha", 20, "dest-alpha-backup"),
                    (3, "grace", "gamma", 30, "dest-gamma-external"),
                    (4, "dave", "next", 40, "dest-route40-backup"),
                    (5, "newbie", "beta", 10, "dest-beta-backup"),
                ],
            )
            self.assertEqual(_foreign_key_violations(live), [])


class RouteRepairRuleTest(TestCase):
    def _pair(self, tmp):
        live, backup = tmp / "live.db", tmp / "backup.db"
        _create_routed_database(live)
        _recover_backup(tmp, "next").rename(backup)
        return backup, live

    def test_invalid_route_ids_raise_plain_value_error(self):
        with TemporaryDirectory() as tmp_dir:
            backup, live = self._pair(Path(tmp_dir))
            for bad in (10, [], [1] * 0, [1, 1], [True], ["10"], [1.5], list(range(101))):
                with self.subTest(bad=bad):
                    with self.assertRaises(ValueError):
                        plan_route_repair(backup, live, bad)
            with self.assertRaises(ValueError):
                plan_route_repair(backup, live, None)

    def test_missing_route_in_either_database(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            with closing(sqlite3.connect(live)) as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("DELETE FROM subscriptions WHERE alias = 'delta'")
                db.execute("DELETE FROM routes WHERE route_id = 40")
                db.commit()
            with self.assertRaises(RouteRepairError) as err:
                plan_route_repair(backup, live, [10, 40])
            self.assertEqual(err.exception.reason, "missing_route")

            with closing(sqlite3.connect(backup)) as db:
                db.execute("DELETE FROM routes WHERE route_id = 20")
                db.commit()
            with self.assertRaises(RouteRepairError) as err:
                plan_route_repair(backup, live, [10, 20])
            self.assertEqual(err.exception.reason, "missing_route")

    def test_plan_is_immutable_and_keeps_unchanged_rows(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            # An in-scope route whose backup row is identical must still appear.
            plan = plan_route_repair(backup, live, [30])
            self.assertEqual(
                plan.changes,
                ((30, "gamma", "dest-gamma-live", "gamma", "dest-gamma-live"),),
            )
            with self.assertRaises(AttributeError):
                plan.changes = ()
            with self.assertRaises(TypeError):
                plan.changes[0] = (1,)

    def test_plan_rejects_a_different_live_file(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            other_live = tmp / "other-live.db"
            _create_routed_database(other_live)
            plan = plan_route_repair(backup, live, SELECTED_IDS)
            with self.assertRaises(RouteRepairError) as err:
                apply_route_repair(other_live, plan, verify=lambda con: True)
            self.assertEqual(err.exception.reason, "stale_plan")

    def test_false_verification_raises_and_leaves_no_partial_repair(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            plan = plan_route_repair(backup, live, SELECTED_IDS)
            before = _all_routes(live)
            with self.assertRaises(InvalidSnapshot):
                apply_route_repair(live, plan, verify=lambda con: False)
            self.assertEqual(_all_routes(live), before)
            self.assertEqual(_foreign_key_violations(live), [])
            # Retry after refusal still works.
            apply_route_repair(live, plan, verify=lambda con: True)
            self.assertEqual(
                _all_routes(live),
                [
                    (10, "beta", "dest-beta-backup"),
                    (20, "alpha", "dest-alpha-backup"),
                    (30, "gamma", "dest-gamma-live"),
                    (40, "next", "dest-route40-backup"),
                ],
            )

    def test_unrelated_destination_edit_is_tolerated(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            plan = plan_route_repair(backup, live, SELECTED_IDS)
            with closing(_connect(live)) as other:
                other.execute(
                    "UPDATE routes SET destination = ? WHERE route_id = 30",
                    ("dest-gamma-tweaked",),
                )
                other.commit()
            apply_route_repair(live, plan, verify=lambda con: True)
            self.assertEqual(
                _all_routes(live)[2], (30, "gamma", "dest-gamma-tweaked")
            )

    def test_subscription_only_changes_are_tolerated(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            plan = plan_route_repair(backup, live, SELECTED_IDS)
            with closing(_connect(live)) as other:
                other.execute(
                    "INSERT INTO subscriptions VALUES (?, ?, ?)", (6, "gamma", "gary")
                )
                other.execute(
                    "DELETE FROM subscriptions WHERE subscription_id = 2"
                )
                other.commit()
            apply_route_repair(live, plan, verify=lambda con: True)
            subscribers = {
                row[0]
                for row in sqlite3.connect(live).execute(
                    "SELECT subscriber FROM subscriptions"
                ).fetchall()
            }
            self.assertEqual(subscribers, {"alice", "grace", "dave", "gary"})
            self.assertEqual(_foreign_key_violations(live), [])

    def test_verify_exception_propagates_unchanged_and_plan_retries(self):
        class Boom(Exception):
            pass

        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            plan = plan_route_repair(backup, live, SELECTED_IDS)
            before = _all_routes(live)

            def raising_verify(_con):
                raise Boom("callback failure")

            with self.assertRaises(Boom):
                apply_route_repair(live, plan, verify=raising_verify)
            self.assertEqual(_all_routes(live), before)
            self.assertEqual(_foreign_key_violations(live), [])
            apply_route_repair(live, plan, verify=lambda con: True)
            self.assertEqual(
                _all_routes(live),
                [
                    (10, "beta", "dest-beta-backup"),
                    (20, "alpha", "dest-alpha-backup"),
                    (30, "gamma", "dest-gamma-live"),
                    (40, "next", "dest-route40-backup"),
                ],
            )

    def test_selected_route_drift_is_stale_even_without_alias_change(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            backup, live = self._pair(tmp)
            plan = plan_route_repair(backup, live, SELECTED_IDS)
            with closing(_connect(live)) as other:
                other.execute(
                    "UPDATE routes SET destination = ? WHERE route_id = 10",
                    ("dest-alpha-changed",),
                )
                other.commit()
            with self.assertRaises(RouteRepairError) as err:
                apply_route_repair(live, plan, verify=lambda con: True)
            self.assertEqual(err.exception.reason, "stale_plan")

    def test_non_plan_object_is_rejected(self):
        with TemporaryDirectory() as tmp_dir:
            live = Path(tmp_dir) / "live.db"
            _create_routed_database(live)
            with self.assertRaises(ValueError):
                apply_route_repair(live, ("not", "a plan"), verify=lambda con: True)
