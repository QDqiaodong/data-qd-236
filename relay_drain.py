"""Offline drain planning and live hand-off control for relay subscriptions.

The live SQLite database keeps the existing ``routes`` and ``subscriptions``
tables and their foreign keys.  A subscription is bound to its *primary* route
through ``subscriptions.alias``; a small side table records extra fallback
route ids, so the binding survives the existing route-repair path (which only
rewrites aliases and destinations).  Another side table records the physical
address a subscription is actually served from -- that table, not the routing
configuration and not a plan, is the data plane's source of truth.

A maintenance task is configured once.  It records capacities per *physical*
address (routes that share a destination share one capacity), the retiring
address, every subscription's fallback route ids and its current serving
address.  From then on the pair of public controls below can reserve and then
commit a hand-off; a reservation occupies a second slot but never changes the
serving address until the operator reports the peer ready.

Only the Python standard library is used.  Files are trusted and local, public
calls are serial, and no network traffic is generated: readiness is supplied
by the caller.
"""

from collections import deque
from collections.abc import Mapping
import sqlite3


class DrainError(ValueError):
    """A business rule rejected a drain operation.

    ``reason`` is one of ``not_configured`` (no maintenance task exists),
    ``already_configured`` (a task already exists), ``handoff_pending`` (a
    different hand-off is waiting for confirmation), ``not_eligible`` (the
    requested target is the serving or retiring address, or is outside the
    subscription's current range), ``no_capacity`` (eligible target, but its
    physical address is full), ``no_pending`` (no reservation for that
    subscription) or ``target_changed`` (a reserved target stopped being
    eligible before confirmation; the reservation is released).
    """

    def __init__(self, reason, detail=""):
        self.reason = reason
        message = reason if not detail else "%s: %s" % (reason, detail)
        super().__init__(message)


_CONFIG = "relay_drain_config"
_CAPACITY = "relay_drain_capacity"
_FALLBACK = "relay_drain_fallback"
_SERVING = "relay_drain_serving"
_PENDING = "relay_drain_pending"

_MAX_ADDRESS_LENGTH = 128
_MAX_FALLBACKS = 4
_MIN_CAPACITY = 1
_MAX_CAPACITY = 6


# --------------------------------------------------------------------------
# Small validation helpers
# --------------------------------------------------------------------------


def _address(value, what="address"):
    if not isinstance(value, str):
        raise ValueError("%s must be text" % what)
    if not value or len(value) > _MAX_ADDRESS_LENGTH:
        raise ValueError("%s must be 1 to %d characters" % (what, _MAX_ADDRESS_LENGTH))
    return value


def _identifier(value, what):
    # ``bool`` is an ``int`` subclass but never a legal row id here.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("%s must be an integer" % what)
    return value


def _validate_capacities(capacities):
    if not isinstance(capacities, Mapping) or not capacities:
        raise ValueError("capacities must be a non-empty address mapping")
    result = {}
    for destination, capacity in capacities.items():
        _address(destination, "capacity address")
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise ValueError("capacity for %r must be an integer" % (destination,))
        if not (_MIN_CAPACITY <= capacity <= _MAX_CAPACITY):
            raise ValueError(
                "capacity for %r must be between %d and %d"
                % (destination, _MIN_CAPACITY, _MAX_CAPACITY)
            )
        result[destination] = capacity
    return result


def _validate_id_mapping(mapping, subscription_ids, what):
    if not isinstance(mapping, Mapping):
        raise ValueError("%s must map subscription ids" % what)
    for key in mapping.keys():
        _identifier(key, "subscription id")
    if set(mapping.keys()) != set(subscription_ids):
        raise ValueError("%s must cover every subscription exactly" % what)


# --------------------------------------------------------------------------
# Schema and state readers
# --------------------------------------------------------------------------


def _connect(live):
    return sqlite3.connect(live, timeout=5)


def _table_exists(con, name):
    return (
        con.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
        ).fetchone()
        is not None
    )


def _configured(con):
    if not _table_exists(con, _CONFIG):
        return False
    return (
        con.execute(
            "SELECT 1 FROM %s WHERE singleton = 1" % _CONFIG
        ).fetchone()
        is not None
    )


def _retiring(con):
    if not _table_exists(con, _CONFIG):
        raise DrainError("not_configured")
    row = con.execute(
        "SELECT retiring FROM %s WHERE singleton = 1" % _CONFIG
    ).fetchone()
    if row is None:
        raise DrainError("not_configured")
    return row[0]


def _pending(con):
    return con.execute(
        "SELECT subscription_id, destination FROM %s WHERE singleton = 1" % _PENDING
    ).fetchone()


def _capacities(con):
    return dict(
        con.execute("SELECT destination, capacity FROM %s" % _CAPACITY).fetchall()
    )


def _serving(con):
    return dict(
        con.execute(
            "SELECT subscription_id, destination FROM %s ORDER BY subscription_id"
            % _SERVING
        ).fetchall()
    )


def _fallback_ids(con, subscription_id):
    return [
        row[0]
        for row in con.execute(
            "SELECT route_id FROM %s WHERE subscription_id = ? ORDER BY ordinal"
            % _FALLBACK,
            (subscription_id,),
        ).fetchall()
    ]


def _all_fallbacks(con):
    result = {}
    for subscription_id, route_id in con.execute(
        "SELECT subscription_id, route_id FROM %s ORDER BY subscription_id, ordinal"
        % _FALLBACK
    ).fetchall():
        result.setdefault(subscription_id, []).append(route_id)
    return result


def _subscriptions(con):
    return [
        row[0]
        for row in con.execute(
            "SELECT subscription_id FROM subscriptions ORDER BY subscription_id"
        ).fetchall()
    ]


def _require_subscription(con, subscription_id):
    if (
        con.execute(
            "SELECT 1 FROM subscriptions WHERE subscription_id = ?",
            (subscription_id,),
        ).fetchone()
        is None
    ):
        raise ValueError("unknown subscription id: %r" % (subscription_id,))


def _allowed_destinations(con, subscription_id, fallback_ids):
    """Physical addresses reachable through the primary or a fallback route.

    The primary route is resolved through ``subscriptions.alias`` at call
    time, so the existing alias-preserving route repair is honoured.  A
    fallback is stored by stable ``route_id`` and likewise resolved now.
    """
    row = con.execute(
        "SELECT r.destination FROM subscriptions AS s "
        "JOIN routes AS r ON r.alias = s.alias "
        "WHERE s.subscription_id = ?",
        (subscription_id,),
    ).fetchone()
    if row is None:
        raise ValueError("subscription %r has no primary route" % (subscription_id,))
    destinations = {row[0]}
    for route_id in fallback_ids:
        fallback = con.execute(
            "SELECT destination FROM routes WHERE route_id = ?", (route_id,)
        ).fetchone()
        if fallback is None:
            raise ValueError("unknown fallback route id: %r" % (route_id,))
        destinations.add(fallback[0])
    return destinations


def _occupancy(con):
    """Physical slots in use, including a reserved hand-off target."""
    counts = {}
    for destination, count in con.execute(
        "SELECT destination, COUNT(*) FROM %s GROUP BY destination" % _SERVING
    ).fetchall():
        counts[destination] = count
    pending = _pending(con)
    if pending is not None:
        counts[pending[1]] = counts.get(pending[1], 0) + 1
    return counts


def _create_schema(con):
    con.execute(
        "CREATE TABLE IF NOT EXISTS %s ("
        "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
        "retiring TEXT NOT NULL)" % _CONFIG
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS %s ("
        "destination TEXT PRIMARY KEY, "
        "capacity INTEGER NOT NULL CHECK (capacity > 0))" % _CAPACITY
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS %s ("
        "subscription_id INTEGER NOT NULL, "
        "route_id INTEGER NOT NULL, "
        "ordinal INTEGER NOT NULL, "
        "PRIMARY KEY (subscription_id, route_id))" % _FALLBACK
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS %s ("
        "subscription_id INTEGER PRIMARY KEY, "
        "destination TEXT NOT NULL)" % _SERVING
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS %s ("
        "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
        "subscription_id INTEGER NOT NULL, "
        "destination TEXT NOT NULL)" % _PENDING
    )


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


def configure_drain(live, *, capacities, retiring, fallbacks, serving):
    """Create the one maintenance task in an otherwise unconfigured live DB.

    ``capacities`` maps physical addresses to positive integer capacities.
    ``retiring`` names one of those addresses.  ``fallbacks`` and ``serving``
    cover every subscription row: fallback route ids are stable route ids, and
    the initial serving address must already be reachable through the
    subscription's primary or fallback routes and fit within its capacity.
    The task is stored transactionally; a second call is rejected with
    ``already_configured`` and leaves the existing task untouched.
    """
    con = _connect(live)
    try:
        con.isolation_level = None
        con.execute("BEGIN IMMEDIATE")
        try:
            if _configured(con):
                raise DrainError("already_configured")

            caps = _validate_capacities(capacities)
            _address(retiring, "retiring address")
            if retiring not in caps:
                raise ValueError("retiring address is not in the capacity table")

            subscription_ids = _subscriptions(con)
            if not subscription_ids:
                raise ValueError("the live database has no subscriptions")

            _validate_id_mapping(fallbacks, subscription_ids, "fallbacks")
            _validate_id_mapping(serving, subscription_ids, "serving")

            fallback_map = {}
            for subscription_id in subscription_ids:
                try:
                    sequence = list(fallbacks[subscription_id])
                except TypeError:
                    raise ValueError(
                        "fallbacks for subscription %r must be a sequence"
                        % (subscription_id,)
                    )
                if len(sequence) > _MAX_FALLBACKS:
                    raise ValueError(
                        "subscription %r has more than %d fallbacks"
                        % (subscription_id, _MAX_FALLBACKS)
                    )
                distinct = set()
                for route_id in sequence:
                    _identifier(route_id, "fallback route id")
                    if route_id in distinct:
                        raise ValueError(
                            "subscription %r repeats fallback route %r"
                            % (subscription_id, route_id)
                        )
                    distinct.add(route_id)
                    if (
                        con.execute(
                            "SELECT 1 FROM routes WHERE route_id = ?", (route_id,)
                        ).fetchone()
                        is None
                    ):
                        raise ValueError(
                            "unknown fallback route id: %r" % (route_id,)
                        )
                fallback_map[subscription_id] = sequence

            serving_map = {}
            for subscription_id in subscription_ids:
                destination = _address(
                    serving[subscription_id], "serving address"
                )
                if destination not in caps:
                    raise ValueError(
                        "serving address %r is not in the capacity table"
                        % (destination,)
                    )
                serving_map[subscription_id] = destination

                allowed = _allowed_destinations(
                    con, subscription_id, fallback_map[subscription_id]
                )
                if destination not in allowed:
                    raise ValueError(
                        "initial serving address %r is outside subscription "
                        "%r's route range" % (destination, subscription_id)
                    )

            used = {}
            for destination in serving_map.values():
                used[destination] = used.get(destination, 0) + 1
            for destination, count in used.items():
                if count > caps[destination]:
                    raise ValueError(
                        "initial service exceeds capacity at %r" % (destination,)
                    )

            _create_schema(con)
            # Only reachable when no task exists; clears any half-created
            # side tables without touching routes or subscriptions.
            for table in (_CONFIG, _CAPACITY, _FALLBACK, _SERVING, _PENDING):
                con.execute("DELETE FROM %s" % table)

            con.execute(
                "INSERT INTO %s (singleton, retiring) VALUES (1, ?)" % _CONFIG,
                (retiring,),
            )
            con.executemany(
                "INSERT INTO %s (destination, capacity) VALUES (?, ?)" % _CAPACITY,
                sorted(caps.items()),
            )
            for subscription_id in sorted(fallback_map):
                for ordinal, route_id in enumerate(fallback_map[subscription_id]):
                    con.execute(
                        "INSERT INTO %s (subscription_id, route_id, ordinal) "
                        "VALUES (?, ?, ?)" % _FALLBACK,
                        (subscription_id, route_id, ordinal),
                    )
            con.executemany(
                "INSERT INTO %s (subscription_id, destination) VALUES (?, ?)"
                % _SERVING,
                sorted(serving_map.items()),
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
    return None


# --------------------------------------------------------------------------
# Observations and planning
# --------------------------------------------------------------------------


def drain_status(live):
    """Return ``(serving, pending)`` from committed state only.

    ``serving`` maps every subscription to the physical address the data plane
    must currently use.  ``pending`` is the single reserved hand-off as
    ``(subscription_id, destination)`` or ``None``; while it exists the old and
    the reserved address each hold one slot, but the data plane still uses the
    old address.
    """
    con = _connect(live)
    try:
        con.execute("BEGIN")
        try:
            _retiring(con)
            serving = _serving(con)
            pending = _pending(con)
        finally:
            con.execute("ROLLBACK")
    finally:
        con.close()
    return serving, pending


def _search_plan(subscription_ids, serving_map, capacities, allowed, retiring):
    """Breadth-first search for any sequence that evacuates the retiring address.

    The state is the serving address of every subscription.  A single step
    moves exactly one subscription to an address in its own allowed range that
    still has a free physical slot *before* the source slot is released.
    """
    start = tuple(serving_map[subscription_id] for subscription_id in subscription_ids)

    def is_goal(state):
        return all(destination != retiring for destination in state)

    if is_goal(start):
        return ()

    parents = {start: None}
    queue = deque([start])
    while queue:
        state = queue.popleft()
        counts = {}
        for destination in state:
            counts[destination] = counts.get(destination, 0) + 1
        for index, subscription_id in enumerate(subscription_ids):
            current = state[index]
            for destination in sorted(allowed[subscription_id]):
                if destination == current or destination == retiring:
                    continue
                capacity = capacities.get(destination)
                if capacity is None:
                    continue
                if counts.get(destination, 0) >= capacity:
                    continue
                moved = state[:index] + (destination,) + state[index + 1 :]
                if moved in parents:
                    continue
                parents[moved] = (state, (subscription_id, destination))
                if is_goal(moved):
                    steps = []
                    cursor = moved
                    while parents[cursor] is not None:
                        previous, move = parents[cursor]
                        steps.append(move)
                        cursor = previous
                    steps.reverse()
                    return tuple(steps)
                queue.append(moved)
    return None


def plan_drain(live):
    """Return one executable sequence, ``()`` when drained, or ``None``.

    Planning reads committed state and neither changes it nor reserves slots.
    Every returned step assumes the configuration is unchanged and the peer
    is ready; capacity is evaluated at the start of each step.  Subscriptions
    not on the retiring address may be moved when they free a needed slot.
    An existing reservation raises ``handoff_pending``.
    """
    con = _connect(live)
    try:
        con.execute("BEGIN")
        try:
            retiring = _retiring(con)
            if _pending(con) is not None:
                raise DrainError("handoff_pending")
            subscription_ids = _subscriptions(con)
            serving_map = _serving(con)
            capacities = _capacities(con)
            fallback_map = _all_fallbacks(con)
            allowed = {
                subscription_id: _allowed_destinations(
                    con, subscription_id, fallback_map.get(subscription_id, [])
                )
                for subscription_id in subscription_ids
            }
            result = _search_plan(
                subscription_ids, serving_map, capacities, allowed, retiring
            )
        finally:
            con.execute("ROLLBACK")
    finally:
        con.close()
    return result


# --------------------------------------------------------------------------
# Hand-off control
# --------------------------------------------------------------------------


def begin_handoff(live, subscription_id, destination):
    """Reserve an eligible physical target without changing the serving address.

    The check uses current committed routes and serving state, never a plan.
    An existing reservation (for any subscription) is ``handoff_pending``; a
    target equal to the serving address, equal to the retiring address or
    outside the subscription's current route range is ``not_eligible``; an
    eligible target at a full physical address is ``no_capacity``.  Success
    stores a reservation (so the target holds a slot) and leaves the data
    plane on the old address.
    """
    _identifier(subscription_id, "subscription id")
    _address(destination, "destination")

    con = _connect(live)
    try:
        con.isolation_level = None
        con.execute("BEGIN IMMEDIATE")
        try:
            retiring = _retiring(con)
            capacities = _capacities(con)
            if destination not in capacities:
                raise ValueError(
                    "destination %r is not in the capacity table" % (destination,)
                )
            _require_subscription(con, subscription_id)
            if _pending(con) is not None:
                raise DrainError("handoff_pending")

            serving_map = _serving(con)
            if subscription_id not in serving_map:
                raise ValueError(
                    "subscription %r has no serving address" % (subscription_id,)
                )
            current = serving_map[subscription_id]
            allowed = _allowed_destinations(
                con, subscription_id, _fallback_ids(con, subscription_id)
            )
            if (
                destination == current
                or destination == retiring
                or destination not in allowed
            ):
                raise DrainError("not_eligible")

            occupancy = _occupancy(con)
            if occupancy.get(destination, 0) >= capacities[destination]:
                raise DrainError("no_capacity")

            con.execute(
                "INSERT INTO %s (singleton, subscription_id, destination) "
                "VALUES (1, ?, ?)" % _PENDING,
                (subscription_id, destination),
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()
    return None


def finish_handoff(live, subscription_id, *, ready):
    """Confirm the reservation belonging to ``subscription_id``.

    With ``ready=False`` the reservation is released and the serving address
    stays put.  With ``ready=True`` the current route range is re-read: if the
    reserved target is no longer eligible the reservation is released and
    ``target_changed`` is raised, again without moving the subscription.  Only
    a ready, still-eligible target switches the serving address; the switch
    and the release of both the reservation and the old slot are one commit.
    """
    _identifier(subscription_id, "subscription id")
    if not isinstance(ready, bool):
        raise ValueError("ready must be a bool")

    con = _connect(live)
    try:
        con.isolation_level = None
        con.execute("BEGIN IMMEDIATE")
        committed = False
        try:
            _retiring(con)
            _require_subscription(con, subscription_id)
            pending = _pending(con)
            if pending is None or pending[0] != subscription_id:
                raise DrainError("no_pending")
            target = pending[1]

            if not ready:
                con.execute("DELETE FROM %s WHERE singleton = 1" % _PENDING)
                con.execute("COMMIT")
                committed = True
            else:
                retiring = _retiring(con)
                allowed = _allowed_destinations(
                    con, subscription_id, _fallback_ids(con, subscription_id)
                )
                if target == retiring or target not in allowed:
                    # The reserved target stopped qualifying: drop the
                    # reservation, keep serving from the old address.
                    con.execute("DELETE FROM %s WHERE singleton = 1" % _PENDING)
                    con.execute("COMMIT")
                    committed = True
                    raise DrainError("target_changed")
                con.execute(
                    "UPDATE %s SET destination = ? WHERE subscription_id = ?"
                    % _SERVING,
                    (target, subscription_id),
                )
                con.execute("DELETE FROM %s WHERE singleton = 1" % _PENDING)
                con.execute("COMMIT")
                committed = True
        except BaseException:
            if not committed:
                con.execute("ROLLBACK")
            raise
    finally:
        con.close()
    return None
