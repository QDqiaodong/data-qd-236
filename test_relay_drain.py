"""Acceptance tests for offline relay-drain planning and live hand-off control.

Every test uses a real SQLite live database holding the existing ``routes``
and ``subscriptions`` tables with their foreign keys.  The maintenance state
lives beside them in private tables; the tests read the data plane through
``drain_status`` and independently verify with plain SQL that route rows and
subscription bindings are never rewritten by the drain module.
"""

from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase

from recovery import apply_route_repair, plan_route_repair, recover
from relay_drain import (
    DrainError,
    begin_handoff,
    configure_drain,
    drain_status,
    finish_handoff,
    plan_drain,
)

ADDRESSES = ("relay-a", "relay-b", "relay-c", "relay-d")
ROUTES = [
    (10, "alpha", "relay-a"),
    (20, "beta", "relay-b"),
    (30, "gamma", "relay-c"),
    (40, "delta", "relay-d"),
]
SUBSCRIPTIONS = [
    (101, "alpha", "one"),
    (202, "beta", "two"),
    (303, "gamma", "three"),
]
CAPACITIES = {address: 1 for address in ADDRESSES}
RETIRING = "relay-a"
FALLBACKS = {101: [20], 202: [30], 303: [40]}
SERVING = {101: "relay-a", 202: "relay-b", 303: "relay-c"}


def _connect(path):
    con = sqlite3.connect(path, timeout=5)
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _create_live(path):
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
        db.executemany("INSERT INTO routes VALUES (?, ?, ?)", ROUTES)
        db.executemany("INSERT INTO subscriptions VALUES (?, ?, ?)", SUBSCRIPTIONS)
        db.commit()


def _configure(live):
    return configure_drain(
        live,
        capacities=dict(CAPACITIES),
        retiring=RETIRING,
        fallbacks={key: list(value) for key, value in FALLBACKS.items()},
        serving=dict(SERVING),
    )


def _routes(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute(
            "SELECT route_id, alias, destination FROM routes ORDER BY route_id"
        ).fetchall()


def _bindings(path):
    """Independent SQL: subscription -> logical route id -> destination."""
    with closing(sqlite3.connect(path)) as db:
        return db.execute(
            "SELECT s.subscription_id, s.alias, r.route_id, r.destination "
            "FROM subscriptions AS s JOIN routes AS r ON s.alias = r.alias "
            "ORDER BY s.subscription_id"
        ).fetchall()


def _tables(path):
    with closing(sqlite3.connect(path)) as db:
        return {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }


def _backup_with_route_40(live, tmp, name, destination):
    """Materialize an independent backup through the existing recover()."""
    stage = tmp / ("preincident-%s.db" % name)
    with closing(sqlite3.connect(live)) as source, closing(sqlite3.connect(stage)) as copy:
        source.backup(copy)
        copy.execute(
            "UPDATE routes SET destination = ? WHERE route_id = 40",
            (destination,),
        )
        copy.commit()
    backup = tmp / ("backup-%s.db" % name)
    recover(stage, backup, verify=lambda con: True)
    return backup


def _repair_route_40(live, tmp, name, destination):
    """Use the pre-existing preview/apply route-repair entry points."""
    backup = _backup_with_route_40(live, tmp, name, destination)
    plan = plan_route_repair(backup, live, [40])
    self_change = plan.changes[0]
    assert self_change[0] == 40 and self_change[4] == destination
    apply_route_repair(
        live,
        plan,
        verify=lambda con: con.execute("PRAGMA foreign_key_check").fetchall() == [],
    )
    return plan


class DrainScenarioOneTest(TestCase):
    def test_no_capacity_then_planned_evacuation_and_empty_plan(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            live = tmp / "live.db"
            _create_live(live)
            routes_before = _routes(live)
            bindings_before = _bindings(live)

            self.assertIsNone(_configure(live))
            # A fresh Python connection observes the task and can continue.
            self.assertEqual(
                drain_status(live), (dict(SERVING), None)
            )

            # Route 10's fallback is route 20 at B, but B is occupied by 202:
            # the direct move must fail on capacity, not eligibility.
            with self.assertRaises(DrainError) as direct:
                begin_handoff(live, 101, "relay-b")
            self.assertEqual(direct.exception.reason, "no_capacity")
            self.assertEqual(drain_status(live), (dict(SERVING), None))

            plan = plan_drain(live)
            expected = ((303, "relay-d"), (202, "relay-c"), (101, "relay-b"))
            self.assertEqual(plan, expected)
            # Planning is read-only: status and serving addresses did not move.
            self.assertEqual(drain_status(live), (dict(SERVING), None))
            self.assertEqual(_routes(live), routes_before)

            order = []
            for subscription_id, destination in expected:
                old = drain_status(live)[0][subscription_id]
                begin_handoff(live, subscription_id, destination)
                serving, pending = drain_status(live)
                # The reservation is visible, the data plane still uses old.
                self.assertEqual(serving[subscription_id], old)
                self.assertEqual(pending, (subscription_id, destination))
                # No second hand-off may start while one is pending.
                other = 202 if subscription_id != 202 else 303
                with self.assertRaises(DrainError) as busy:
                    begin_handoff(live, other, "relay-d")
                self.assertEqual(busy.exception.reason, "handoff_pending")
                with self.assertRaises(DrainError) as busy_plan:
                    plan_drain(live)
                self.assertEqual(busy_plan.exception.reason, "handoff_pending")

                finish_handoff(live, subscription_id, ready=True)
                serving, pending = drain_status(live)
                self.assertIsNone(pending)
                self.assertEqual(serving[subscription_id], destination)
                order.append((subscription_id, old, destination))
                print(
                    "reserved %d: %s -> %s; after ready serves %s"
                    % (subscription_id, old, destination, serving[subscription_id])
                )

            self.assertEqual(
                drain_status(live)[0],
                {101: "relay-b", 202: "relay-c", 303: "relay-d"},
            )
            self.assertEqual(plan_drain(live), ())
            # The existing route and subscription rows are untouched.
            self.assertEqual(_routes(live), routes_before)
            self.assertEqual(_bindings(live), bindings_before)
            self.assertTrue(
                {"relay_drain_config", "relay_drain_serving"} <= _tables(live)
            )
            print("actual move order:", order)
            print("independent SQL bindings:", _bindings(live))


class DrainScenarioTwoTest(TestCase):
    def test_not_ready_retry_and_target_change_force_replan(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            live = tmp / "live.db"
            _create_live(live)
            routes_before = _routes(live)
            bindings_before = _bindings(live)
            _configure(live)

            # First readiness report is negative: nothing moves, slot returns.
            begin_handoff(live, 303, "relay-d")
            self.assertEqual(
                drain_status(live), (dict(SERVING), (303, "relay-d"))
            )
            self.assertIsNone(finish_handoff(live, 303, ready=False))
            self.assertEqual(drain_status(live), (dict(SERVING), None))

            # The same step can be reserved and attempted again.
            begin_handoff(live, 303, "relay-d")
            self.assertEqual(drain_status(live)[1], (303, "relay-d"))

            # The pre-existing route repair now points route 40 at B, so the
            # reserved D target leaves 303's allowed range (main route 30 -> C,
            # fallback route 40 -> B).
            repair = _repair_route_40(live, tmp, "to-b", "relay-b")
            self.assertEqual(repair.changes, ((40, "delta", "relay-d", "delta", "relay-b"),))
            self.assertEqual(_routes(live)[3], (40, "delta", "relay-b"))
            # Alias binding for 303 follows route 40 logically; the drain's
            # serving address stays C and its reservation is still pending.
            self.assertEqual(
                next(row for row in _bindings(live) if row[0] == 303),
                (303, "gamma", 30, "relay-c"),
            )
            self.assertEqual(drain_status(live)[0][303], "relay-c")
            self.assertEqual(drain_status(live)[1], (303, "relay-d"))

            with self.assertRaises(DrainError) as changed:
                finish_handoff(live, 303, ready=True)
            self.assertEqual(changed.exception.reason, "target_changed")
            # The reservation is released, the old address keeps serving.
            self.assertEqual(drain_status(live), (dict(SERVING), None))
            self.assertEqual(_routes(live)[3], (40, "delta", "relay-b"))

            # D is free but nobody can reach it; B and C occupants cannot swap.
            self.assertIsNone(plan_drain(live))

            with self.assertRaises(DrainError) as ineligible:
                begin_handoff(live, 303, "relay-d")
            self.assertEqual(ineligible.exception.reason, "not_eligible")

            # Restore route 40 to D through the same repair entry, then the
            # original three-step evacuation works.
            _repair_route_40(live, tmp, "back-to-d", "relay-d")
            self.assertEqual(_routes(live), routes_before)
            self.assertEqual(_bindings(live), bindings_before)

            planned = plan_drain(live)
            self.assertEqual(
                planned, ((303, "relay-d"), (202, "relay-c"), (101, "relay-b"))
            )
            for subscription_id, destination in planned:
                begin_handoff(live, subscription_id, destination)
                finish_handoff(live, subscription_id, ready=True)

            self.assertEqual(
                drain_status(live)[0],
                {101: "relay-b", 202: "relay-c", 303: "relay-d"},
            )
            self.assertEqual(plan_drain(live), ())
            self.assertEqual(_routes(live), routes_before)
            self.assertEqual(_bindings(live), bindings_before)
            print(
                "after route 40 -> B, plan is None; rejected begin reason=%r; "
                "final serving=%s"
                % (ineligible.exception.reason, drain_status(live)[0])
            )


class DrainRuleTest(TestCase):
    def _live(self, tmp):
        live = tmp / "live.db"
        _create_live(live)
        return live

    def test_unconfigured_operations_report_not_configured(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            for operation in (
                lambda: drain_status(live),
                lambda: plan_drain(live),
                lambda: begin_handoff(live, 101, "relay-b"),
                lambda: finish_handoff(live, 101, ready=True),
            ):
                with self.assertRaises(DrainError) as err:
                    operation()
                self.assertEqual(err.exception.reason, "not_configured")

    def test_duplicate_configuration_is_rejected_without_reset(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            _configure(live)
            begin_handoff(live, 303, "relay-d")
            before = drain_status(live)
            with self.assertRaises(DrainError) as err:
                _configure(live)
            self.assertEqual(err.exception.reason, "already_configured")
            self.assertEqual(drain_status(live), before)

    def test_invalid_configuration_is_rejected_and_leaves_no_state(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            bad_cases = [
                dict(capacities={}, retiring="relay-a", fallbacks=FALLBACKS, serving=SERVING),
                dict(
                    capacities=dict(CAPACITIES),
                    retiring="relay-z",
                    fallbacks=FALLBACKS,
                    serving=SERVING,
                ),
                dict(
                    capacities=dict(CAPACITIES),
                    retiring="relay-a",
                    fallbacks={101: [20], 202: [30]},
                    serving=SERVING,
                ),
                dict(
                    capacities=dict(CAPACITIES),
                    retiring="relay-a",
                    fallbacks=FALLBACKS,
                    serving={101: "relay-a", 202: "relay-b", 303: "relay-a"},
                ),
                dict(
                    capacities=dict(CAPACITIES),
                    retiring="relay-a",
                    fallbacks={101: [20, 20], 202: [30], 303: [40]},
                    serving=SERVING,
                ),
                dict(
                    capacities=dict(CAPACITIES),
                    retiring="relay-a",
                    fallbacks={101: [99], 202: [30], 303: [40]},
                    serving=SERVING,
                ),
                dict(
                    capacities={"relay-a": 0, "relay-b": 1, "relay-c": 1, "relay-d": 1},
                    retiring="relay-a",
                    fallbacks=FALLBACKS,
                    serving=SERVING,
                ),
                dict(
                    capacities={"relay-a": 1, "relay-b": 1, "relay-c": 1, "relay-d": 1},
                    retiring="relay-a",
                    fallbacks=FALLBACKS,
                    serving={101: "relay-a", 202: "relay-b", 303: "relay-b"},
                ),
            ]
            for kwargs in bad_cases:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaises(ValueError):
                        configure_drain(live, **kwargs)
                    self.assertEqual(_tables(live), {"routes", "subscriptions"})
            with self.assertRaises(DrainError) as err:
                drain_status(live)
            self.assertEqual(err.exception.reason, "not_configured")

    def test_invalid_arguments_are_plain_value_errors(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            _configure(live)
            for call in (
                lambda: begin_handoff(live, "101", "relay-b"),
                lambda: begin_handoff(live, True, "relay-b"),
                lambda: begin_handoff(live, 999, "relay-b"),
                lambda: begin_handoff(live, 101, "relay-z"),
                lambda: begin_handoff(live, 101, ""),
                lambda: finish_handoff(live, 101, ready=1),
                lambda: finish_handoff(live, 101, ready=None),
                lambda: finish_handoff(live, 999, ready=True),
            ):
                with self.subTest(call=call):
                    with self.assertRaises(ValueError):
                        call()
            self.assertEqual(drain_status(live), (dict(SERVING), None))

    def test_eligibility_rules_and_no_partial_reservation(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            _configure(live)
            # The serving address and the retiring address are ineligible; an
            # eligible target at a full address is no_capacity.
            with self.assertRaises(DrainError) as same:
                begin_handoff(live, 202, "relay-b")
            self.assertEqual(same.exception.reason, "not_eligible")
            with self.assertRaises(DrainError) as retiring:
                begin_handoff(live, 303, "relay-a")
            self.assertEqual(retiring.exception.reason, "not_eligible")
            with self.assertRaises(DrainError) as full:
                begin_handoff(live, 101, "relay-b")
            self.assertEqual(full.exception.reason, "no_capacity")
            # None of the failures reserved anything.
            self.assertEqual(drain_status(live), (dict(SERVING), None))

    def test_pending_belongs_to_exactly_one_subscription(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            _configure(live)
            begin_handoff(live, 303, "relay-d")
            with self.assertRaises(DrainError) as other:
                finish_handoff(live, 101, ready=True)
            self.assertEqual(other.exception.reason, "no_pending")
            # The other subscription's pending hand-off is untouched.
            self.assertEqual(drain_status(live)[1], (303, "relay-d"))
            finish_handoff(live, 303, ready=True)
            self.assertEqual(drain_status(live)[1], None)
            with self.assertRaises(DrainError) as again:
                finish_handoff(live, 303, ready=True)
            self.assertEqual(again.exception.reason, "no_pending")

    def test_capacity_is_shared_by_destination_not_route_row(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            # Add a second route to the same physical address B and use it as
            # 101's fallback.  B still has capacity 1 shared by both routes.
            with closing(_connect(live)) as db:
                db.execute(
                    "INSERT INTO routes VALUES (?, ?, ?)",
                    (21, "beta-copy", "relay-b"),
                )
                db.commit()
            configure_drain(
                live,
                capacities={"relay-a": 1, "relay-b": 1, "relay-c": 1, "relay-d": 1},
                retiring="relay-a",
                fallbacks={101: [21], 202: [30], 303: [40]},
                serving=dict(SERVING),
            )
            with self.assertRaises(DrainError) as full:
                begin_handoff(live, 101, "relay-b")
            self.assertEqual(full.exception.reason, "no_capacity")
            self.assertEqual(plan_drain(live)[-1], (101, "relay-b"))

    def test_serving_address_is_not_auto_moved_by_configuration_change(self):
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            live = self._live(tmp)
            _configure(live)
            # Route 40 now points at B: 303's allowed range becomes {C, B}.
            _repair_route_40(live, tmp, "to-b", "relay-b")
            serving, pending = drain_status(live)
            self.assertEqual(serving[303], "relay-c")
            self.assertIsNone(pending)
            # C stays occupied by 303, so 202 cannot take it; no legal plan.
            self.assertIsNone(plan_drain(live))
            self.assertEqual(drain_status(live)[0][303], "relay-c")

    def test_planning_applies_capacity_at_the_start_of_each_step(self):
        with TemporaryDirectory() as tmp_dir:
            live = self._live(Path(tmp_dir))
            _configure(live)
            plan = plan_drain(live)
            self.assertEqual(
                plan, ((303, "relay-d"), (202, "relay-c"), (101, "relay-b"))
            )
            # Replaying with a fresh connection after each committed step reads
            # the slot just freed by the previous step.
            seen = []
            for subscription_id, destination in plan:
                begin_handoff(live, subscription_id, destination)
                seen.append(drain_status(live)[0][subscription_id])
                finish_handoff(live, subscription_id, ready=True)
                seen.append(drain_status(live)[0][subscription_id])
            self.assertEqual(
                seen, ["relay-c", "relay-d", "relay-b", "relay-c", "relay-a", "relay-b"]
            )

    def test_a_subscription_off_the_retiring_relay_can_be_moved_first(self):
        with TemporaryDirectory() as tmp_dir:
            # Three single-slot relays; 7 and 8 serve B and C and block the
            # route-10 subscriber's fallback, so they move out of the way even
            # though neither is on the retiring A.
            live = Path(tmp_dir) / "live.db"
            with closing(_connect(live)) as db:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute(
                    "CREATE TABLE routes ("
                    "route_id INTEGER PRIMARY KEY, alias TEXT NOT NULL UNIQUE, "
                    "destination TEXT NOT NULL)"
                )
                db.execute(
                    "CREATE TABLE subscriptions ("
                    "subscription_id INTEGER PRIMARY KEY, "
                    "alias TEXT NOT NULL REFERENCES routes(alias) "
                    "ON UPDATE CASCADE ON DELETE RESTRICT, subscriber TEXT NOT NULL)"
                )
                db.executemany("INSERT INTO routes VALUES (?, ?, ?)", ROUTES)
                db.executemany(
                    "INSERT INTO subscriptions VALUES (?, ?, ?)",
                    [(7, "alpha", "seven"), (8, "beta", "eight"), (9, "gamma", "nine")],
                )
                db.commit()
            configure_drain(
                live,
                capacities=dict(CAPACITIES),
                retiring=RETIRING,
                fallbacks={7: [20], 8: [40], 9: [40]},
                serving={7: "relay-a", 8: "relay-b", 9: "relay-c"},
            )
            plan = plan_drain(live)
            self.assertIsNotNone(plan)
            self.assertTrue(all(destination != RETIRING for _sid, destination in plan))
            self.assertEqual(plan[-1], (7, "relay-b"))
            # A blocking subscription not on A moves first into the free D.
            self.assertIn(plan[0], ((8, "relay-d"), (9, "relay-d")))
            for subscription_id, destination in plan:
                begin_handoff(live, subscription_id, destination)
                finish_handoff(live, subscription_id, ready=True)
            self.assertEqual(plan_drain(live), ())
            self.assertNotIn(RETIRING, drain_status(live)[0].values())
            print("off-relay blocking move plan:", plan)
