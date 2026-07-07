"""Snapshot, rotation, and restore."""

from __future__ import annotations

from pathlib import Path

from ballast import Database, restore, snapshot


def _seed(db: Database, n: int) -> None:
    with db.transaction() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS t (v INTEGER)")
        conn.execute("INSERT INTO t VALUES (?)", (n,))


def _rows(db: Database) -> list[int]:
    with db.connection() as conn:
        return [r[0] for r in conn.execute("SELECT v FROM t ORDER BY v")]


def test_snapshot_creates_consistent_copy(tmp_path: Path) -> None:
    db = Database(tmp_path / "app.db")
    _seed(db, 1)
    snap = snapshot(db, tmp_path / "backups")
    assert snap.exists()
    copy = Database(snap)
    assert _rows(copy) == [1]


def test_rotation_keeps_n(tmp_path: Path) -> None:
    db = Database(tmp_path / "app.db")
    _seed(db, 1)
    for _ in range(5):
        snapshot(db, tmp_path / "backups", keep=2)
    assert len(list((tmp_path / "backups").glob("snapshot-*.db"))) == 2


def test_restore_rolls_back(tmp_path: Path) -> None:
    db = Database(tmp_path / "app.db")
    _seed(db, 1)
    snap = snapshot(db, tmp_path / "backups")
    _seed(db, 2)  # change made after the snapshot
    assert _rows(db) == [1, 2]
    restore(snap, db)
    assert _rows(db) == [1]  # the post-snapshot change is gone


def test_restore_missing_snapshot_errors(tmp_path: Path) -> None:
    db = Database(tmp_path / "app.db")
    _seed(db, 1)
    import pytest

    with pytest.raises(FileNotFoundError):
        restore(tmp_path / "nope.db", db)
