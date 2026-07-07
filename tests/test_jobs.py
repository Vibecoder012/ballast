"""Job queue: enqueue, lease, exactly-once execution, retry/backoff, recovery."""

from __future__ import annotations

import sqlite3

from ballast import Database, Job, JobWorker, execute_job, lease_next_job, queue, sweep_running_jobs
from tests.conftest import count


def _enqueue(db: Database, kind: str, payload: dict, **kw: object) -> str:
    with db.transaction() as conn:
        return queue.enqueue(conn, kind, payload, **kw)  # type: ignore[arg-type]


def test_enqueue_is_transactional(db: Database) -> None:
    import pytest

    with pytest.raises(RuntimeError), db.transaction() as conn:
        queue.enqueue(conn, "work", {"a": 1})
        raise RuntimeError("caller failed")
    assert count(db, "SELECT count(*) FROM ballast_jobs") == 0


def test_enqueue_unique(db: Database) -> None:
    with db.transaction() as conn:
        first = queue.enqueue_unique(conn, "scan", {})
        second = queue.enqueue_unique(conn, "scan", {})
    assert first is not None
    assert second is None
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE kind='scan'") == 1


def test_lease_is_fifo(db: Database) -> None:
    _enqueue(db, "work", {"n": 1})
    _enqueue(db, "work", {"n": 2})
    first = lease_next_job(db)
    assert first is not None
    assert first.payload == {"n": 1}


def test_execute_success_marks_done(sidetable: Database) -> None:
    db = sidetable

    def handler(conn: sqlite3.Connection, payload: dict) -> None:
        conn.execute("INSERT INTO side (note) VALUES (?)", (payload["note"],))

    _enqueue(db, "work", {"note": "hi"})
    job = lease_next_job(db)
    assert job is not None
    assert execute_job(db, job, {"work": handler}) is True
    assert count(db, "SELECT count(*) FROM side") == 1
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE status='done'") == 1


def test_handler_failure_rolls_back_side_effects(sidetable: Database) -> None:
    db = sidetable

    def always_fails(conn: sqlite3.Connection, payload: dict) -> None:
        conn.execute("INSERT INTO side (note) VALUES ('partial')")
        raise RuntimeError("boom")

    _enqueue(db, "work", {}, max_attempts=2)
    worker = JobWorker(db, {"work": always_fails}, retry_base_seconds=0)
    worker.drain()
    # every attempt's side effect was rolled back with its failed completion
    assert count(db, "SELECT count(*) FROM side") == 0
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE status='failed'") == 1
    with db.connection() as conn:
        row = conn.execute("SELECT attempts, last_error FROM ballast_jobs").fetchone()
    assert row["attempts"] == 2
    assert "boom" in row["last_error"]


def test_flaky_handler_eventually_succeeds_exactly_once(sidetable: Database) -> None:
    db = sidetable
    calls = {"n": 0}

    def flaky(conn: sqlite3.Connection, payload: dict) -> None:
        calls["n"] += 1
        conn.execute("INSERT INTO side (note) VALUES ('ok')")
        if calls["n"] == 1:
            raise RuntimeError("first attempt fails")

    _enqueue(db, "work", {})
    JobWorker(db, {"work": flaky}, retry_base_seconds=0).drain()
    # committed side effect appears exactly once despite the earlier failed attempt
    assert count(db, "SELECT count(*) FROM side") == 1
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE status='done'") == 1


def test_backoff_defers_retry(sidetable: Database) -> None:
    db = sidetable

    def fails(conn: sqlite3.Connection, payload: dict) -> None:
        raise RuntimeError("no")

    _enqueue(db, "work", {})
    worker = JobWorker(db, {"work": fails}, retry_base_seconds=3600)  # 1h backoff
    assert worker.run_once() == 1  # leased + failed once
    # requeued but not due again -> a second pass does nothing
    assert worker.run_once() == 0
    with db.connection() as conn:
        row = conn.execute("SELECT status, run_after FROM ballast_jobs").fetchone()
    assert row["status"] == "queued"
    assert row["run_after"] is not None


def test_sweep_reclaims_orphaned_running_jobs(db: Database) -> None:
    _enqueue(db, "work", {})
    with db.transaction() as conn:
        conn.execute("UPDATE ballast_jobs SET status='running'")  # simulate a crashed process
    assert sweep_running_jobs(db) == 1
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE status='queued'") == 1


def test_missing_handler_marks_failed(db: Database) -> None:
    _enqueue(db, "unknown", {}, max_attempts=1)
    job = lease_next_job(db)
    assert job is not None
    assert isinstance(job, Job)
    assert execute_job(db, job, {}) is False
    assert count(db, "SELECT count(*) FROM ballast_jobs WHERE status='failed'") == 1
