"""Leased job queue + a single-writer background worker with exactly-once semantics.

Guarantees, and how they're achieved:

* **Enqueue is transactional.** :meth:`JobQueue.enqueue` INSERTs inside the caller's
  transaction, so a job exists only if the work that scheduled it committed.
* **Exactly-once execution.** The worker runs a handler and marks the job ``done`` in the
  *same* transaction. A handler crash rolls back its side effects **and** the completion
  together; the job is retried, re-running a handler whose effects never landed.
* **Crash recovery.** On start the worker reclaims every ``running`` job — the process just
  started, so any job still marked running belonged to a dead process. (No runtime lease
  reaping: it could re-queue a job still executing on another thread.)
* **Retry with backoff.** Failures reschedule with exponential backoff up to
  ``max_attempts``, then land in ``failed`` for inspection.
* **Single writer.** All background writes happen on one thread, so under WAL there is never
  writer-writer contention from the worker.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ._util import new_id, now_iso
from .connection import Database
from .events import DISPATCH_JOB_KIND, EventBus, dispatch_pending, make_dispatch_handler

#: A job handler receives the worker's transaction connection and the decoded payload.
#: Anything it writes on ``conn`` commits atomically with the job's completion.
JobHandler = Callable[[sqlite3.Connection, dict[str, Any]], None]

DEFAULT_LEASE_SECONDS = 300
DEFAULT_RETRY_BASE_SECONDS = 5
DEFAULT_POLL_SECONDS = 1.0
DEFAULT_IDLE_CHECKPOINT_SECONDS = 60


@dataclass(frozen=True, slots=True)
class Job:
    """A leased unit of work."""

    seq: int
    id: str
    kind: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int


class JobQueue:
    """Enqueues jobs inside the caller's transaction."""

    def enqueue(
        self,
        conn: sqlite3.Connection,
        kind: str,
        payload: Mapping[str, Any],
        *,
        run_after: str | None = None,
        max_attempts: int = 5,
    ) -> str:
        """Insert a job in the caller's open transaction; returns its id.

        ``run_after`` is an ISO-8601 ``...Z`` timestamp before which the job won't be leased
        (use it for delays/backoff). Call within a
        :meth:`~ballast.connection.Database.transaction` block.
        """
        job_id = new_id()
        conn.execute(
            "INSERT INTO ballast_jobs (id, kind, payload_json, run_after, max_attempts,"
            " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                job_id,
                kind,
                json.dumps(dict(payload)),
                run_after,
                max_attempts,
                now_iso(),
                now_iso(),
            ),
        )
        return job_id

    def enqueue_unique(
        self,
        conn: sqlite3.Connection,
        kind: str,
        payload: Mapping[str, Any],
        *,
        run_after: str | None = None,
        max_attempts: int = 5,
    ) -> str | None:
        """Enqueue only if no ``queued``/``running`` job of this ``kind`` exists.

        For self-re-arming chains (a periodic scan that re-enqueues itself), so a re-seed at
        startup can't fork the chain. Returns the new id, or ``None`` if one already existed.
        """
        existing = conn.execute(
            "SELECT 1 FROM ballast_jobs WHERE kind = ? AND status IN ('queued', 'running') LIMIT 1",
            (kind,),
        ).fetchone()
        if existing is not None:
            return None
        return self.enqueue(conn, kind, payload, run_after=run_after, max_attempts=max_attempts)


queue = JobQueue()


def sweep_running_jobs(db: Database) -> int:
    """Reclaim all ``running`` jobs back to ``queued``. Startup only. Returns count."""
    with db.transaction() as conn:
        cur = conn.execute(
            "UPDATE ballast_jobs SET status = 'queued', lease_expires_at = NULL, updated_at = ?"
            " WHERE status = 'running'",
            (now_iso(),),
        )
        return cur.rowcount


def lease_next_job(db: Database, *, lease_seconds: int = DEFAULT_LEASE_SECONDS) -> Job | None:
    """Atomically claim the oldest due queued job, marking it ``running``. FIFO by ``seq``."""
    now = now_iso()
    lease_expiry = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM ballast_jobs WHERE status = 'queued'"
            " AND (run_after IS NULL OR run_after <= ?) ORDER BY seq LIMIT 1",
            (now,),
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            "UPDATE ballast_jobs SET status = 'running', attempts = attempts + 1,"
            " lease_expires_at = ?, updated_at = ? WHERE id = ?",
            (lease_expiry, now_iso(), row["id"]),
        )
        return Job(
            seq=row["seq"],
            id=row["id"],
            kind=row["kind"],
            payload=json.loads(row["payload_json"]),
            attempts=row["attempts"] + 1,
            max_attempts=row["max_attempts"],
        )


def execute_job(
    db: Database,
    job: Job,
    handlers: Mapping[str, JobHandler],
    *,
    retry_base_seconds: int = DEFAULT_RETRY_BASE_SECONDS,
) -> bool:
    """Run one leased job. Its side effects and the ``done`` mark commit together.

    On failure the transaction rolls back (undoing any partial side effects) and a separate
    short transaction records the error and either reschedules with backoff or marks the job
    ``failed`` once attempts are exhausted. Returns ``True`` on success.
    """
    handler = handlers.get(job.kind)
    try:
        if handler is None:
            raise LookupError(f"no handler registered for job kind {job.kind!r}")
        with db.transaction() as conn:
            handler(conn, job.payload)
            conn.execute(
                "UPDATE ballast_jobs SET status = 'done', lease_expires_at = NULL,"
                " last_error = NULL, updated_at = ? WHERE id = ?",
                (now_iso(), job.id),
            )
        return True
    except Exception as exc:  # the worker must survive any handler failure
        _record_failure(db, job, exc, retry_base_seconds)
        return False


def _record_failure(db: Database, job: Job, exc: Exception, retry_base_seconds: int) -> None:
    error = f"{type(exc).__name__}: {exc}"
    with db.transaction() as conn:
        if job.attempts >= job.max_attempts:
            conn.execute(
                "UPDATE ballast_jobs SET status = 'failed', lease_expires_at = NULL,"
                " last_error = ?, updated_at = ? WHERE id = ?",
                (error, now_iso(), job.id),
            )
        else:
            backoff = retry_base_seconds * (2 ** (job.attempts - 1))
            run_after = (datetime.now(timezone.utc) + timedelta(seconds=backoff)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            conn.execute(
                "UPDATE ballast_jobs SET status = 'queued', lease_expires_at = NULL,"
                " last_error = ?, run_after = ?, updated_at = ? WHERE id = ?",
                (error, run_after, now_iso(), job.id),
            )


class JobWorker:
    """The background worker: dispatch events → lease jobs → execute, on one writer thread.

    Register handlers as ``{kind: callable}``. Pass a :class:`~ballast.events.EventBus` to
    also drive event dispatch (the worker registers the internal dispatch handler and calls
    :func:`~ballast.events.dispatch_pending` each pass).

    Use :meth:`drain` for deterministic, synchronous processing (ideal in tests and scripts);
    use :meth:`start`/:meth:`stop` to run it in the background, and :meth:`notify` to wake it
    immediately after committing new work instead of waiting for the poll interval.
    """

    def __init__(
        self,
        db: Database,
        handlers: Mapping[str, JobHandler] | None = None,
        *,
        bus: EventBus | None = None,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        retry_base_seconds: int = DEFAULT_RETRY_BASE_SECONDS,
        idle_checkpoint_seconds: int = DEFAULT_IDLE_CHECKPOINT_SECONDS,
    ) -> None:
        self.db = db
        self.bus = bus
        self.handlers: dict[str, JobHandler] = dict(handlers or {})
        if bus is not None:
            self.handlers.setdefault(DISPATCH_JOB_KIND, make_dispatch_handler(bus))
        self.poll_seconds = poll_seconds
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.idle_checkpoint_seconds = idle_checkpoint_seconds
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_checkpoint = time.monotonic()

    def register(self, kind: str, handler: JobHandler) -> None:
        """Register (or replace) a handler for ``kind``."""
        self.handlers[kind] = handler

    # -- synchronous processing ---------------------------------------------

    def run_once(self) -> int:
        """One pass: dispatch events (if a bus is set), then lease+run due jobs to quiescence.

        Returns the number of jobs executed (successes and failures). Does not loop on jobs
        created *during* the pass — call :meth:`drain` for that.
        """
        if self.bus is not None:
            dispatch_pending(self.db, self.bus)
        done = 0
        while not self._stop.is_set():
            job = lease_next_job(self.db, lease_seconds=self.lease_seconds)
            if job is None:
                break
            execute_job(self.db, job, self.handlers, retry_base_seconds=self.retry_base_seconds)
            done += 1
        return done

    def drain(self, max_passes: int = 1000) -> int:
        """Run passes until no work remains (jobs and freshly-dispatched events). Returns total run.

        Deterministic entry point for tests and one-shot scripts. ``max_passes`` guards
        against a pathological handler that enqueues itself forever.
        """
        total = 0
        for _ in range(max_passes):
            did = self.run_once()
            total += did
            if did == 0:
                break
        return total

    # -- background lifecycle -----------------------------------------------

    def start(self) -> None:
        """Reclaim orphaned jobs, then run the worker on a daemon thread."""
        reclaimed = sweep_running_jobs(self.db)
        del reclaimed  # surfaced via logging in real apps; kept explicit here
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="ballast-worker", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the worker to stop and wait up to ``timeout`` for the current pass to finish."""
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def notify(self) -> None:
        """Wake the worker now (call after committing work so it doesn't wait for the poll)."""
        self._wake.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            with contextlib.suppress(Exception):  # the loop itself must never die
                self.run_once()
            self._maybe_checkpoint()
            self._wake.wait(timeout=self.poll_seconds)
            self._wake.clear()

    def _maybe_checkpoint(self) -> None:
        if time.monotonic() - self._last_checkpoint <= self.idle_checkpoint_seconds:
            return
        try:
            with self.db.connection() as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._last_checkpoint = time.monotonic()
        except sqlite3.Error:
            pass
