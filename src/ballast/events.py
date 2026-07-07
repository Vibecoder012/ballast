"""Durable transactional outbox + per-subscriber cursors.

The outbox pattern, done right on one SQLite file:

* :meth:`EventBus.publish` INSERTs into ``ballast_events`` **inside the caller's open
  transaction**. The event is atomic with the state change that produced it — a rollback
  discards both, so there are never phantom events for work that didn't commit.
* Ordering is by ``events.seq`` (AUTOINCREMENT). Insert order equals commit order because
  writers serialize under ``BEGIN IMMEDIATE``. The random ``id`` is identity, not order — a
  cursor keyed on it could skip an event forever.
* Each subscriber has a durable cursor. :func:`dispatch_pending` advances every cursor past
  new events and enqueues one dispatch job per matching event **in one transaction** —
  crash-safe: either the cursor moved and the jobs exist, or neither did.
* Handlers run on the :class:`~ballast.jobs.JobWorker`, and their side effects commit in the
  same transaction as the job's completion — exactly-once by construction.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from ._util import new_id, now_iso
from .connection import Database
from .errors import EventContractError

#: A topic validator raises to reject an invalid payload; returns ``None`` to accept.
Validator = Callable[[Mapping[str, Any]], None]

PRUNE_FLOOR_DAYS = 30
DISPATCH_JOB_KIND = "ballast.dispatch"


@dataclass(frozen=True, slots=True)
class Event:
    """A committed event, as handed to a subscriber."""

    seq: int
    id: str
    topic: str
    payload: dict[str, Any]
    entity_type: str | None = None
    entity_id: str | None = None
    created_at: str = ""


#: A subscriber handler receives the worker's transaction connection and the event.
Handler = Callable[[sqlite3.Connection, Event], None]


class EventBus:
    """Declares topics, publishes events, and holds the subscription registry.

    An :class:`EventBus` is configuration + registry; it holds no connection. Create one at
    startup, declare your topics, register subscribers, and pass it to the worker.
    """

    def __init__(self) -> None:
        self._validators: dict[str, Validator | None] = {}
        self._subs: dict[str, tuple[str, Handler]] = {}

    # -- topics --------------------------------------------------------------

    def declare(self, topic: str, validator: Validator | None = None) -> None:
        """Register a topic, optionally with a payload ``validator``.

        Declaring the same topic twice is allowed only if the validator is unchanged.
        Publishing an undeclared topic is rejected — topics are a contract.
        """
        if topic in self._validators and self._validators[topic] is not validator:
            raise ValueError(f"topic {topic!r} already declared with a different validator")
        self._validators[topic] = validator

    def is_declared(self, topic: str) -> bool:
        return topic in self._validators

    # -- subscriptions -------------------------------------------------------

    def subscribe(self, subscriber: str, topic: str, handler: Handler) -> None:
        """Register ``handler`` for ``topic`` under the stable name ``subscriber``.

        The name is the cursor key — treat it as a persistent identifier; renaming a
        subscriber orphans its position and it will reprocess from the pruning floor.
        """
        if topic not in self._validators:
            raise ValueError(
                f"subscriber {subscriber!r} references undeclared topic {topic!r}; "
                f"call bus.declare({topic!r}) first"
            )
        existing = self._subs.get(subscriber)
        if existing is not None and existing != (topic, handler):
            raise ValueError(f"duplicate subscriber name: {subscriber!r}")
        self._subs[subscriber] = (topic, handler)

    def handler_for(self, subscriber: str) -> tuple[str, Handler] | None:
        return self._subs.get(subscriber)

    def subscriptions(self) -> dict[str, tuple[str, Handler]]:
        return dict(self._subs)

    # -- publishing ----------------------------------------------------------

    def publish(
        self,
        conn: sqlite3.Connection,
        topic: str,
        payload: Mapping[str, Any],
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
    ) -> str:
        """Insert an event inside the caller's open transaction. Returns the event id.

        Must be called within a :meth:`~ballast.connection.Database.transaction` block so it
        commits atomically with the state change. Raises
        :class:`~ballast.errors.EventContractError` if the topic's validator rejects the
        payload, or :class:`ValueError` if the topic is undeclared.
        """
        if topic not in self._validators:
            raise ValueError(f"cannot publish undeclared topic {topic!r}; call bus.declare() first")
        data = dict(payload)
        validator = self._validators[topic]
        if validator is not None:
            try:
                validator(data)
            except Exception as exc:  # normalise any validator failure to our error type
                raise EventContractError(f"payload for topic {topic!r} is invalid: {exc}") from exc
        event_id = new_id()
        conn.execute(
            "INSERT INTO ballast_events"
            " (id, topic, entity_type, entity_id, payload_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                event_id,
                topic,
                entity_type,
                entity_id,
                json.dumps(data, ensure_ascii=False),
                now_iso(),
            ),
        )
        return event_id


# ─── dispatcher ────────────────────────────────────────────────────────────


def dispatch_pending(db: Database, bus: EventBus, *, batch: int = 500) -> int:
    """Advance every subscriber cursor past new events, enqueuing one dispatch job each.

    The cursor move and the job inserts commit together (one transaction). Returns the
    number of dispatch jobs enqueued. Called each worker pass; safe to call manually.
    """
    enqueued = 0
    subs = bus.subscriptions()
    if not subs:
        return 0
    with db.transaction() as conn:
        for subscriber, (topic, _handler) in subs.items():
            cur = conn.execute(
                "SELECT last_seq FROM ballast_event_cursors WHERE subscriber = ?", (subscriber,)
            ).fetchone()
            if cur is None:
                conn.execute(
                    "INSERT INTO ballast_event_cursors (subscriber, last_seq, updated_at) "
                    "VALUES (?, 0, ?)",
                    (subscriber, now_iso()),
                )
                last_seq = 0
            else:
                last_seq = cur["last_seq"]
            rows = conn.execute(
                "SELECT seq, topic FROM ballast_events WHERE seq > ? ORDER BY seq LIMIT ?",
                (last_seq, batch),
            ).fetchall()
            if not rows:
                continue
            for row in rows:
                if row["topic"] == topic:
                    conn.execute(
                        "INSERT INTO ballast_jobs (id, kind, payload_json, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (
                            new_id(),
                            DISPATCH_JOB_KIND,
                            json.dumps({"subscriber": subscriber, "event_seq": row["seq"]}),
                            now_iso(),
                            now_iso(),
                        ),
                    )
                    enqueued += 1
            conn.execute(
                "UPDATE ballast_event_cursors SET last_seq = ?, updated_at = ?"
                " WHERE subscriber = ?",
                (rows[-1]["seq"], now_iso(), subscriber),
            )
    return enqueued


def make_dispatch_handler(bus: EventBus) -> Callable[[sqlite3.Connection, dict[str, Any]], None]:
    """Return the *job* handler that runs one subscriber for one event.

    Registered by the worker under :data:`DISPATCH_JOB_KIND`. It loads the event, looks up
    the subscriber's handler, and invokes it with the worker's transaction connection — so
    the handler's writes and the job's completion commit together (exactly-once). Its shape
    is a job handler ``(conn, payload)``, not a subscriber :data:`Handler` ``(conn, event)``.
    """

    def _handle(conn: sqlite3.Connection, payload: dict[str, Any]) -> None:
        subscriber = payload["subscriber"]
        event_seq = payload["event_seq"]
        registered = bus.handler_for(subscriber)
        if registered is None:
            return  # subscriber gone this run; its cursor simply won't have advanced
        _topic, handler = registered
        row = conn.execute("SELECT * FROM ballast_events WHERE seq = ?", (event_seq,)).fetchone()
        if row is None:
            return  # event pruned before dispatch (rare); nothing to do
        handler(
            conn,
            Event(
                seq=row["seq"],
                id=row["id"],
                topic=row["topic"],
                entity_type=row["entity_type"],
                entity_id=row["entity_id"],
                payload=json.loads(row["payload_json"]),
                created_at=row["created_at"],
            ),
        )

    return _handle


# ─── maintenance / reads ─────────────────────────────────────────────────────


def prune_events(db: Database, *, floor_days: int = PRUNE_FLOOR_DAYS) -> int:
    """Delete events every subscriber has consumed AND older than the floor. Returns count.

    Respects the minimum cursor across all registered subscribers *and* any cursor row on
    disk (so a subscriber that isn't registered this run still keeps its unconsumed
    events). Returns the number of rows deleted.
    """
    floor = (datetime.now(timezone.utc) - timedelta(days=floor_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with db.transaction() as conn:
        cursors = conn.execute("SELECT last_seq FROM ballast_event_cursors").fetchall()
        positions = [c["last_seq"] for c in cursors]
        min_cursor = min(positions) if positions else None
        if min_cursor is None:
            cur = conn.execute("DELETE FROM ballast_events WHERE created_at < ?", (floor,))
        else:
            cur = conn.execute(
                "DELETE FROM ballast_events WHERE created_at < ? AND seq <= ?",
                (floor, min_cursor),
            )
        return cur.rowcount


def events_after(db: Database, after_seq: int, *, limit: int = 100) -> list[Event]:
    """Read committed events with ``seq > after_seq`` — the tail for a live feed / CDC."""
    with db.connection() as conn:
        rows = conn.execute(
            "SELECT * FROM ballast_events WHERE seq > ? ORDER BY seq LIMIT ?", (after_seq, limit)
        ).fetchall()
    return [
        Event(
            seq=r["seq"],
            id=r["id"],
            topic=r["topic"],
            entity_type=r["entity_type"],
            entity_id=r["entity_id"],
            payload=json.loads(r["payload_json"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]


@dataclass(frozen=True, slots=True)
class Subscription:
    """A read-only view of a registered subscription (for introspection/debugging)."""

    subscriber: str
    topic: str
    handler: Handler = field(compare=False)
