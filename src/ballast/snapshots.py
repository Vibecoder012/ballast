"""Consistent backups via ``VACUUM INTO``, with rotation and restore.

``VACUUM INTO`` produces a fully consistent, defragmented copy of a live SQLite database
without the corruption risk of copying the file while it is being written. Ballast wraps it
with timestamped filenames, keep-last-N rotation, and a guarded restore.

Because ballast is forward-only (there are no down-migrations), a snapshot taken *before*
an upgrade is the supported way to roll back: close the app, :func:`restore`, reopen.
"""

from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .connection import Database


def snapshot(
    db: Database | str | Path,
    backups_dir: str | Path,
    *,
    prefix: str = "snapshot",
    keep: int | None = 7,
) -> Path:
    """Write a consistent snapshot of ``db`` into ``backups_dir`` and rotate old ones.

    Parameters
    ----------
    db:
        A :class:`~ballast.connection.Database` or a path to the database file.
    backups_dir:
        Directory to write snapshots into (created if needed).
    prefix:
        Filename prefix; files are ``{prefix}-{YYYYMMDD-HHMMSS}.db``.
    keep:
        Keep at most this many snapshots sharing ``prefix`` (oldest deleted). ``None`` keeps
        all.

    Returns
    -------
    Path
        The snapshot that was written.
    """
    db_path = db.path if isinstance(db, Database) else str(db)
    backups = Path(backups_dir)
    backups.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    target = backups / f"{prefix}-{stamp}.db"
    n = 1
    while target.exists():  # same-second snapshots (e.g. tight loops / tests)
        target = backups / f"{prefix}-{stamp}-{n}.db"
        n += 1

    con = sqlite3.connect(db_path)
    try:
        con.execute("VACUUM INTO ?", (str(target),))
    finally:
        con.close()

    if keep is not None:
        existing = sorted(backups.glob(f"{prefix}-*.db"))
        for old in existing[: max(0, len(existing) - keep)]:
            old.unlink()
    return target


def restore(snapshot_path: str | Path, db: Database | str | Path) -> None:
    """Overwrite the database at ``db`` with ``snapshot_path``.

    The application must have **no open connections** to the target when calling this.
    Rolls back the database to the snapshot's contents wholesale — every change since the
    snapshot is discarded (that is the point).
    """
    src = Path(snapshot_path)
    if not src.exists():
        raise FileNotFoundError(f"snapshot not found: {src}")
    dst = Path(db.path if isinstance(db, Database) else db)
    shutil.copyfile(src, dst)
    # Remove stale WAL/SHM side files so the restored main file is authoritative.
    for suffix in ("-wal", "-shm"):
        side = dst.with_name(dst.name + suffix)
        if side.exists():
            side.unlink()
