"""Forward-only migration ladder."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ballast import Database, Migration, MigrationRunner
from ballast.errors import DowngradeError


def _mk(sql: str):
    def apply(conn: sqlite3.Connection) -> None:
        conn.execute(sql)  # one statement per migration in these tests

    return apply


def test_applies_in_order_and_records(tmp_path: Path) -> None:
    db = Database(tmp_path / "m.db")
    runner = MigrationRunner(db)
    migs = [
        Migration(1, _mk("CREATE TABLE a (x INTEGER)"), "a"),
        Migration(2, _mk("CREATE TABLE b (y INTEGER)"), "b"),
    ]
    assert runner.run("app", migs) == [1, 2]
    assert runner.current_version("app") == 2
    with db.connection() as conn:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"a", "b"} <= names


def test_idempotent(tmp_path: Path) -> None:
    db = Database(tmp_path / "m.db")
    runner = MigrationRunner(db)
    migs = [Migration(1, _mk("CREATE TABLE a (x INTEGER)"))]
    assert runner.run("app", migs) == [1]
    assert runner.run("app", migs) == []  # nothing pending the second time


def test_applies_only_pending(tmp_path: Path) -> None:
    db = Database(tmp_path / "m.db")
    runner = MigrationRunner(db)
    runner.run("app", [Migration(1, _mk("CREATE TABLE a (x INTEGER)"))])
    applied = runner.run(
        "app",
        [
            Migration(1, _mk("CREATE TABLE a (x INTEGER)")),
            Migration(2, _mk("CREATE TABLE b (y INTEGER)")),
        ],
    )
    assert applied == [2]


def test_downgrade_refused(tmp_path: Path) -> None:
    db = Database(tmp_path / "m.db")
    runner = MigrationRunner(db)
    runner.run(
        "app", [Migration(1, _mk("CREATE TABLE a (x INTEGER)")), Migration(2, _mk("SELECT 1"))]
    )
    # Now "older code" only knows version 1.
    with pytest.raises(DowngradeError) as exc:
        runner.run("app", [Migration(1, _mk("CREATE TABLE a (x INTEGER)"))])
    assert exc.value.module == "app"
    assert exc.value.recorded == 2


def test_partial_failure_is_atomic(tmp_path: Path) -> None:
    db = Database(tmp_path / "m.db")
    runner = MigrationRunner(db)

    def boom(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE half (x INTEGER)")
        raise RuntimeError("mid-migration failure")

    with pytest.raises(RuntimeError):
        runner.run("app", [Migration(1, boom)])
    # The failed migration left nothing behind and was not recorded.
    assert runner.current_version("app") == 0
    with db.connection() as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "half" not in tables


def test_duplicate_version_rejected(tmp_path: Path) -> None:
    db = Database(tmp_path / "m.db")
    runner = MigrationRunner(db)
    with pytest.raises(ValueError, match="duplicate"):
        runner.run("app", [Migration(1, _mk("SELECT 1")), Migration(1, _mk("SELECT 1"))])
