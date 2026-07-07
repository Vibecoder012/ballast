"""Durable, crash-safe background jobs with retry — no broker, one SQLite file.

Run with:  python examples/durable_jobs.py

Demonstrates: enqueue inside a transaction, a handler that fails once and is retried, and
the fact that a committed side effect appears exactly once despite the failed attempt.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from ballast import Database, JobWorker, install, queue


def main() -> None:
    db = Database(Path(tempfile.mkdtemp(prefix="ballast-jobs-")) / "app.db")
    install(db)
    with db.transaction() as conn:
        conn.execute("CREATE TABLE emails_sent (to_addr TEXT)")

    attempts = {"n": 0}

    def send_email(conn: sqlite3.Connection, payload: dict) -> None:
        attempts["n"] += 1
        # Record the effect first — it will roll back with the job if we then fail.
        conn.execute("INSERT INTO emails_sent (to_addr) VALUES (?)", (payload["to"],))
        if attempts["n"] == 1:
            raise RuntimeError("transient SMTP error — will be retried")

    # Enqueue inside a transaction (the job is real only if this commits).
    with db.transaction() as conn:
        queue.enqueue(conn, "send_email", {"to": "ada@example.com"})

    # retry_base_seconds=0 makes the demo retry immediately instead of backing off.
    JobWorker(db, {"send_email": send_email}, retry_base_seconds=0).drain()

    with db.connection() as conn:
        sent = conn.execute("SELECT count(*) FROM emails_sent").fetchone()[0]
        status = conn.execute("SELECT status, attempts FROM ballast_jobs").fetchone()
    print(f"handler invoked {attempts['n']} time(s)")
    print(f"emails actually recorded: {sent}  (exactly once, despite the first failure)")
    print(f"job status: {status['status']} after {status['attempts']} attempt(s)")


if __name__ == "__main__":
    main()
