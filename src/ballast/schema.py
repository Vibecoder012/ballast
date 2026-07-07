"""Ballast's own tables, shipped as a migration ladder under the ``ballast`` module.

Call :func:`install` once at startup to create (or upgrade) the outbox and job tables in
your database. They are namespaced with a ``ballast_`` prefix so they coexist with your
application's tables in the same file.
"""

from __future__ import annotations

import sqlite3

from .connection import Database
from .migrations import Migration, MigrationRunner

MODULE = "ballast"


# Each migration runs inside a transaction the runner owns, so DDL is executed one
# statement at a time via conn.execute() — never conn.executescript(), which forces an
# implicit COMMIT and would break that transaction.
_V1_STATEMENTS = (
    """
    CREATE TABLE ballast_events (
        seq          INTEGER PRIMARY KEY AUTOINCREMENT,
        id           TEXT NOT NULL UNIQUE,
        topic        TEXT NOT NULL,
        entity_type  TEXT,
        entity_id    TEXT,
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at   TEXT NOT NULL
    )
    """,
    "CREATE INDEX ix_ballast_events_topic   ON ballast_events (topic)",
    "CREATE INDEX ix_ballast_events_created ON ballast_events (created_at)",
    """
    CREATE TABLE ballast_event_cursors (
        subscriber TEXT PRIMARY KEY,
        last_seq   INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE ballast_jobs (
        seq              INTEGER PRIMARY KEY AUTOINCREMENT,
        id               TEXT NOT NULL UNIQUE,
        kind             TEXT NOT NULL,
        payload_json     TEXT NOT NULL DEFAULT '{}',
        status           TEXT NOT NULL DEFAULT 'queued',
        run_after        TEXT,
        attempts         INTEGER NOT NULL DEFAULT 0,
        max_attempts     INTEGER NOT NULL DEFAULT 5,
        last_error       TEXT,
        lease_expires_at TEXT,
        created_at       TEXT NOT NULL,
        updated_at       TEXT NOT NULL
    )
    """,
    "CREATE INDEX ix_ballast_jobs_status ON ballast_jobs (status)",
    "CREATE INDEX ix_ballast_jobs_kind   ON ballast_jobs (kind)",
)


def _v1(conn: sqlite3.Connection) -> None:
    for statement in _V1_STATEMENTS:
        conn.execute(statement)


#: Ballast's built-in schema, in ladder order. Additive migrations append here.
CORE_MIGRATIONS: list[Migration] = [
    Migration(version=1, apply=_v1, description="events, event_cursors, jobs"),
]


def install(db: Database) -> list[int]:
    """Create/upgrade ballast's tables in ``db``. Idempotent; returns versions applied."""
    return MigrationRunner(db).run(MODULE, CORE_MIGRATIONS)
