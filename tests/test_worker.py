"""End-to-end: events -> dispatch -> handler, exactly once, plus the background loop."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone

from ballast import Database, EventBus, JobWorker, queue
from tests.conftest import count


def _bus_with_counter(db: Database) -> EventBus:
    with db.transaction() as conn:
        conn.execute("CREATE TABLE hits (event_id TEXT PRIMARY KEY, note TEXT)")
    bus = EventBus()
    bus.declare("thing.happened")

    def on_thing(conn: sqlite3.Connection, event) -> None:
        # INSERT OR IGNORE proves *at least once* would double-count; PRIMARY KEY + count==n
        # proves exactly-once (the handler runs once per event).
        conn.execute(
            "INSERT INTO hits (event_id, note) VALUES (?, ?)",
            (event.id, event.payload.get("note", "")),
        )

    bus.subscribe("counter", "thing.happened", on_thing)
    return bus


def test_event_to_handler_exactly_once(db: Database) -> None:
    bus = _bus_with_counter(db)
    with db.transaction() as conn:
        for i in range(5):
            bus.publish(conn, "thing.happened", {"note": str(i)})
    processed = JobWorker(db, bus=bus).drain()
    assert processed == 5
    assert count(db, "SELECT count(*) FROM hits") == 5
    # draining again does nothing (cursor consumed, jobs done)
    assert JobWorker(db, bus=bus).drain() == 0


def test_handler_can_publish_followups(db: Database) -> None:
    with db.transaction() as conn:
        conn.execute("CREATE TABLE log (v TEXT)")
    bus = EventBus()
    bus.declare("step.one")
    bus.declare("step.two")

    def on_one(conn: sqlite3.Connection, event) -> None:
        conn.execute("INSERT INTO log VALUES ('one')")
        bus.publish(conn, "step.two", {})  # chain a follow-up in the same transaction

    def on_two(conn: sqlite3.Connection, event) -> None:
        conn.execute("INSERT INTO log VALUES ('two')")

    bus.subscribe("a", "step.one", on_one)
    bus.subscribe("b", "step.two", on_two)
    with db.transaction() as conn:
        bus.publish(conn, "step.one", {})
    JobWorker(db, bus=bus).drain()
    with db.connection() as conn:
        logged = [r[0] for r in conn.execute("SELECT v FROM log ORDER BY rowid")]
    assert logged == ["one", "two"]


def test_self_rearming_chain_does_not_fork(db: Database) -> None:
    # Pattern: seed idempotently with enqueue_unique; the handler re-arms the next link
    # with a plain enqueue scheduled in the future (so a single pass processes one link).
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def tick(conn: sqlite3.Connection, payload: dict) -> None:
        queue.enqueue(conn, "tick", {}, run_after=future)  # re-arm the chain

    with db.transaction() as conn:
        first = queue.enqueue_unique(conn, "tick", {})
        dup = queue.enqueue_unique(conn, "tick", {})  # ignored: a re-seed can't fork the chain
    assert first is not None
    assert dup is None

    JobWorker(db, {"tick": tick}).run_once()  # runs the single due tick, which re-arms one
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE kind='tick' AND status='queued'") == 1
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE kind='tick' AND status='done'") == 1


def test_background_start_stop(db: Database) -> None:
    bus = _bus_with_counter(db)
    worker = JobWorker(db, bus=bus, poll_seconds=0.02)
    worker.start()
    try:
        with db.transaction() as conn:
            bus.publish(conn, "thing.happened", {"note": "async"})
        worker.notify()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if count(db, "SELECT count(*) FROM hits") == 1:
                break
            time.sleep(0.02)
        assert count(db, "SELECT count(*) FROM hits") == 1
    finally:
        worker.stop()
