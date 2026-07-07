"""Forward-only migration ladder — a dependency-light alternative to Alembic for SQLite.

Each migration is an integer ``version`` plus an ``apply(conn)`` function that runs DDL.
Migrations are recorded per **module** (a namespace, so your app and ballast's own tables
version independently) in a ``ballast_migrations`` bookkeeping table. The runner:

* applies every pending migration in ascending order, each in its own transaction together
  with its bookkeeping row (a half-applied migration is impossible);
* **refuses to run** if the database records a version newer than the code ships
  (:class:`~ballast.errors.DowngradeError`) — the anti-corruption guard for
  install-over-install downgrades;
* is forward-only by design: there are no down-migrations, because silently reversing schema
  on a production database is how data gets lost. To roll back, restore a snapshot
  (:mod:`ballast.snapshots`).
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from ._util import now_iso
from .connection import Database
from .errors import DowngradeError

_LADDER_DDL = """
CREATE TABLE IF NOT EXISTS ballast_migrations (
    module TEXT NOT NULL,
    version INTEGER NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    applied_at TEXT NOT NULL,
    PRIMARY KEY (module, version)
)
"""


@dataclass(frozen=True)
class Migration:
    """One forward-only schema step.

    Parameters
    ----------
    version:
        A positive, strictly increasing integer within its module.
    apply:
        A callable ``(conn) -> None`` that runs the DDL/DML for this step. It executes
        inside a transaction the runner owns — do **not** commit or roll back inside it, and
        run one statement per ``conn.execute(...)`` rather than ``conn.executescript(...)``
        (the latter forces an implicit COMMIT and would break the runner's transaction).
    description:
        Human-readable summary, stored in the bookkeeping table.
    """

    version: int
    apply: Callable[[sqlite3.Connection], None] = field(compare=False)
    description: str = ""


class MigrationRunner:
    """Applies migration ladders against a :class:`~ballast.connection.Database`."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def _ensure_ladder(self, conn: sqlite3.Connection) -> None:
        conn.execute(_LADDER_DDL)

    def recorded_versions(self, module: str) -> set[int]:
        """The set of migration versions already applied for ``module``."""
        with self.db.connection() as conn:
            self._ensure_ladder(conn)
            rows = conn.execute(
                "SELECT version FROM ballast_migrations WHERE module = ?", (module,)
            ).fetchall()
        return {r["version"] for r in rows}

    def current_version(self, module: str) -> int:
        """The highest applied version for ``module`` (``0`` if none)."""
        return max(self.recorded_versions(module), default=0)

    def run(self, module: str, migrations: Sequence[Migration]) -> list[int]:
        """Apply all pending migrations for ``module`` and return the versions applied.

        Raises :class:`~ballast.errors.DowngradeError` if the database is ahead of the code.
        Idempotent: running again with the same migrations applies nothing.
        """
        ordered = sorted(migrations, key=lambda m: m.version)
        self._validate(module, ordered)
        recorded = self.recorded_versions(module)
        shipped_max = ordered[-1].version if ordered else 0
        recorded_max = max(recorded, default=0)
        if recorded_max > shipped_max:
            raise DowngradeError(module, recorded_max, shipped_max)

        applied: list[int] = []
        for migration in ordered:
            if migration.version in recorded:
                continue
            with self.db.transaction() as conn:
                self._ensure_ladder(conn)
                migration.apply(conn)
                conn.execute(
                    "INSERT INTO ballast_migrations (module, version, description, applied_at) "
                    "VALUES (?, ?, ?, ?)",
                    (module, migration.version, migration.description, now_iso()),
                )
            applied.append(migration.version)
        return applied

    @staticmethod
    def _validate(module: str, ordered: Sequence[Migration]) -> None:
        seen: set[int] = set()
        for m in ordered:
            if m.version <= 0:
                raise ValueError(f"{module}: migration version must be positive, got {m.version}")
            if m.version in seen:
                raise ValueError(f"{module}: duplicate migration version {m.version}")
            seen.add(m.version)
