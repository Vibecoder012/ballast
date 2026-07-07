"""Connection & transaction discipline."""

from __future__ import annotations

from pathlib import Path

import pytest

from ballast import Database


def test_pragmas_applied(tmp_path: Path) -> None:
    db = Database(tmp_path / "a.db")
    with db.connection() as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


def test_transaction_commits(tmp_path: Path) -> None:
    db = Database(tmp_path / "b.db")
    with db.transaction() as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
    assert count(db) == 1


def test_transaction_rolls_back_on_error(tmp_path: Path) -> None:
    db = Database(tmp_path / "c.db")
    with db.transaction() as conn:
        conn.execute("CREATE TABLE t (v INTEGER)")
    with pytest.raises(RuntimeError), db.transaction() as conn:
        conn.execute("INSERT INTO t VALUES (1)")
        raise RuntimeError("boom")
    assert count(db) == 0


def test_transaction_reuses_caller_connection(tmp_path: Path) -> None:
    db = Database(tmp_path / "d.db")
    conn = db.connect()
    try:
        with db.transaction(conn) as c:
            c.execute("CREATE TABLE t (v INTEGER)")
            c.execute("INSERT INTO t VALUES (9)")
        # same connection still usable afterwards
        assert conn.execute("SELECT v FROM t").fetchone()[0] == 9
    finally:
        conn.close()


def count(db: Database) -> int:
    with db.connection() as conn:
        return int(conn.execute("SELECT count(*) FROM t").fetchone()[0])
