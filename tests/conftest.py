"""Shared fixtures."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ballast import Database, install


@pytest.fixture
def db(tmp_path: Path) -> Database:
    """A fresh file-backed database with ballast's tables installed."""
    database = Database(tmp_path / "test.db")
    install(database)
    return database


@pytest.fixture
def sidetable(db: Database) -> Database:
    """``db`` plus a ``side`` table handlers can write to, to observe transactional effects."""
    with db.transaction() as conn:
        conn.execute("CREATE TABLE side (id INTEGER PRIMARY KEY AUTOINCREMENT, note TEXT)")
    return db


def count(db: Database, sql: str, params: tuple[object, ...] = ()) -> int:
    with db.connection() as conn:
        row: sqlite3.Row = conn.execute(sql, params).fetchone()
    return int(row[0])
