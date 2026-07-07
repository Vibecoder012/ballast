"""ballast — durable backend correctness for single-machine Python, on one SQLite file.

The guarantees you'd normally stand up Redis + Postgres + a broker for — a transactional
outbox, exactly-once background jobs, forward-only migrations, and consistent
snapshots/restore — with **zero dependencies** and no server, against one WAL-mode SQLite
database. Built for desktop apps, on-prem/appliance software, air-gapped deployments, and
CLIs that must be crash-safe on a single box.

Quick start
-----------
>>> import tempfile, os
>>> from ballast import Database, EventBus, JobWorker, install, queue
>>> db = Database(os.path.join(tempfile.mkdtemp(), "app.db"))
>>> _ = install(db)                       # create ballast's tables
>>> bus = EventBus(); bus.declare("greeted")
>>> seen = []
>>> bus.subscribe("printer", "greeted", lambda conn, e: seen.append(e.payload["name"]))
>>> with db.transaction() as conn:        # publish atomically with your own writes
...     _ = bus.publish(conn, "greeted", {"name": "ada"})
>>> JobWorker(db, bus=bus).drain()        # process to quiescence
1
>>> seen
['ada']
"""

from __future__ import annotations

from .connection import Database
from .errors import (
    BallastError,
    DowngradeError,
    EventContractError,
    SecretsUnavailableError,
)
from .events import (
    DISPATCH_JOB_KIND,
    Event,
    EventBus,
    Handler,
    Validator,
    dispatch_pending,
    events_after,
    prune_events,
)
from .jobs import (
    Job,
    JobHandler,
    JobQueue,
    JobWorker,
    execute_job,
    lease_next_job,
    queue,
    sweep_running_jobs,
)
from .migrations import Migration, MigrationRunner
from .schema import CORE_MIGRATIONS, install
from .secrets import SecretStore
from .snapshots import restore, snapshot

__version__ = "0.1.0"

__all__ = [  # noqa: RUF022 - grouped by module for readability, not sorted
    "__version__",
    # connection
    "Database",
    # migrations
    "Migration",
    "MigrationRunner",
    # schema
    "install",
    "CORE_MIGRATIONS",
    # events
    "EventBus",
    "Event",
    "Handler",
    "Validator",
    "dispatch_pending",
    "prune_events",
    "events_after",
    "DISPATCH_JOB_KIND",
    # jobs
    "JobQueue",
    "queue",
    "Job",
    "JobHandler",
    "JobWorker",
    "lease_next_job",
    "execute_job",
    "sweep_running_jobs",
    # snapshots
    "snapshot",
    "restore",
    # secrets
    "SecretStore",
    # errors
    "BallastError",
    "DowngradeError",
    "EventContractError",
    "SecretsUnavailableError",
]
