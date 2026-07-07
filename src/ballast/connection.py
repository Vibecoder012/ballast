"""SQLite connection & transaction discipline — the foundation everything else sits on.

Ballast serializes writers deterministically the way most hand-rolled SQLite apps forget
to: WAL journaling, foreign keys on, ``synchronous=NORMAL``, a real ``busy_timeout``, and
crucially **explicit ``BEGIN IMMEDIATE``** on every write transaction.

Why ``BEGIN IMMEDIATE`` matters: Python's default transaction handling opens *deferred*
transactions that take the write lock only when the first write happens — mid-transaction.
Under contention that upgrade fails with ``SQLITE_BUSY`` *immediately*, ignoring your
``busy_timeout``. Taking the write lock up front means ``busy_timeout`` actually applies and
writers queue instead of erroring. Ballast sets ``isolation_level=None`` (autocommit) so it
owns ``BEGIN``/``COMMIT`` explicitly and this discipline is guaranteed.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

DEFAULT_BUSY_TIMEOUT_MS = 5000


class Database:
    """A handle to one SQLite database file, handing out correctly-configured connections.

    A :class:`Database` is cheap and thread-safe to share; the *connections* it returns are
    not shared across threads (open one per thread, e.g. one for your app and one for the
    :class:`~ballast.jobs.JobWorker`).

    Parameters
    ----------
    path:
        Path to the database file. ``":memory:"`` works for tests but note each connection
        to ``":memory:"`` is a *separate* database, so it is unsuitable for the multi-
        connection worker; use a temp file for anything involving the worker.
    busy_timeout_ms:
        How long a writer waits for the lock before giving up (default 5000 ms).
    """

    __slots__ = ("busy_timeout_ms", "path")

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS) -> None:
        self.path = str(path)
        self.busy_timeout_ms = busy_timeout_ms

    def connect(self) -> sqlite3.Connection:
        """Open a new connection with ballast's pragma regime applied.

        The caller owns the connection's lifetime and should close it (or use
        :meth:`connection`). ``row_factory`` is :class:`sqlite3.Row` so columns are
        addressable by name.
        """
        conn = sqlite3.connect(
            self.path,
            isolation_level=None,  # autocommit; ballast issues BEGIN/COMMIT explicitly
            check_same_thread=False,
            timeout=self.busy_timeout_ms / 1000,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Context manager yielding a fresh connection that is closed on exit."""
        conn = self.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self, conn: sqlite3.Connection | None = None) -> Iterator[sqlite3.Connection]:
        """Run a write transaction: ``BEGIN IMMEDIATE`` → commit on clean exit, rollback on error.

        Pass an existing ``conn`` to run in a caller-managed connection (the common case in
        the worker); omit it to open and close a throwaway connection for a one-off write.
        Composing writes inside a single ``transaction()`` block is what makes ballast's
        outbox atomic — the state change and the ``publish``/``enqueue`` land together or
        not at all.
        """
        own = conn is None
        conn = conn or self.connect()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        finally:
            if own:
                conn.close()
