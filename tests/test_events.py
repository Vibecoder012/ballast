"""Transactional outbox, cursors, dispatch, and pruning."""

from __future__ import annotations

import pytest

from ballast import Database, EventBus, dispatch_pending, events_after, prune_events
from ballast.errors import EventContractError


def _bus() -> EventBus:
    bus = EventBus()
    bus.declare("thing.created")
    return bus


def test_publish_is_visible_after_commit(db: Database) -> None:
    bus = _bus()
    with db.transaction() as conn:
        bus.publish(conn, "thing.created", {"id": "x1"}, entity_type="thing", entity_id="x1")
    events = events_after(db, 0)
    assert len(events) == 1
    assert events[0].topic == "thing.created"
    assert events[0].payload == {"id": "x1"}
    assert events[0].entity_id == "x1"


def test_rollback_discards_event(db: Database) -> None:
    bus = _bus()
    with pytest.raises(RuntimeError), db.transaction() as conn:
        bus.publish(conn, "thing.created", {"id": "x1"})
        raise RuntimeError("caller failed after publishing")
    assert events_after(db, 0) == []  # no phantom event


def test_undeclared_topic_rejected(db: Database) -> None:
    bus = _bus()
    with db.transaction() as conn, pytest.raises(ValueError, match="undeclared"):
        bus.publish(conn, "not.declared", {})


def test_validator_rejects_bad_payload(db: Database) -> None:
    bus = EventBus()

    def require_id(payload: dict) -> None:
        if "id" not in payload:
            raise ValueError("missing id")

    bus.declare("thing.created", require_id)
    with db.transaction() as conn, pytest.raises(EventContractError):
        bus.publish(conn, "thing.created", {"name": "no id"})
    assert events_after(db, 0) == []


def test_subscribe_requires_declared_topic(db: Database) -> None:
    bus = EventBus()
    with pytest.raises(ValueError, match="undeclared"):
        bus.subscribe("s", "nope", lambda conn, e: None)


def test_dispatch_advances_cursor_and_enqueues(db: Database) -> None:
    bus = _bus()
    bus.subscribe("worker", "thing.created", lambda conn, e: None)
    with db.transaction() as conn:
        bus.publish(conn, "thing.created", {"id": "a"})
        bus.publish(conn, "thing.created", {"id": "b"})
    assert dispatch_pending(db, bus) == 2  # two dispatch jobs enqueued
    assert dispatch_pending(db, bus) == 0  # cursor advanced; nothing new


def test_dispatch_only_matches_subscribed_topic(db: Database) -> None:
    bus = EventBus()
    bus.declare("a")
    bus.declare("b")
    bus.subscribe("only_a", "a", lambda conn, e: None)
    with db.transaction() as conn:
        bus.publish(conn, "b", {})
        bus.publish(conn, "a", {})
        bus.publish(conn, "b", {})
    assert dispatch_pending(db, bus) == 1  # only the single "a" event


def test_prune_respects_cursor_and_floor(db: Database) -> None:
    bus = _bus()
    bus.subscribe("worker", "thing.created", lambda conn, e: None)
    with db.transaction() as conn:
        bus.publish(conn, "thing.created", {"id": "old"})
    # Unconsumed + recent -> never pruned.
    assert prune_events(db) == 0
    # Consume it and backdate it past the floor, then it can be pruned.
    dispatch_pending(db, bus)
    with db.transaction() as conn:
        conn.execute("UPDATE ballast_events SET created_at = '2000-01-01T00:00:00Z'")
    assert prune_events(db) == 1
    assert events_after(db, 0) == []
