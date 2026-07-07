"""Small internal helpers shared across ballast. Not part of the public API."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def new_id() -> str:
    """A random 32-hex identity for a row (identity, not ordering — rows order by ``seq``)."""
    return uuid.uuid4().hex


def now_iso() -> str:
    """Current UTC time as an ISO-8601 ``...Z`` string that sorts lexicographically."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
