# ballast

**Durable backend correctness for single-machine Python — on one SQLite file, with zero dependencies.**

The guarantees you'd normally stand up Redis + Postgres + a broker for, delivered against one WAL-mode SQLite database with no server and nothing to operate:

- **Transactional outbox** — publish events *inside your own transaction*, so an event exists if and only if the work that produced it committed. No phantom events, ever.
- **Exactly-once jobs** — a background worker runs a handler and marks the job done in the *same* transaction. A crash rolls back the side effects **and** the completion together; the job retries. No double-sends, no lost work.
- **Forward-only migrations** — a tiny ladder that applies pending steps atomically and **refuses to run** against a newer database (the install-over-install downgrade guard).
- **Consistent snapshots & restore** — `VACUUM INTO` backups with rotation, and a guarded restore to roll back.
- **OS-keyring secrets** — read from the keyring when present, fall back to env vars always.

Built for the deployments that Temporal, Celery, and cloud queues structurally can't serve: **desktop apps, on-prem appliances, air-gapped / regulated environments, and CLIs that must be crash-safe on a single box.**

[![CI](https://github.com/<your-username>/ballast/actions/workflows/ci.yml/badge.svg)](https://github.com/<your-username>/ballast/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/ballast.svg)](https://pypi.org/project/ballast/)
[![Python](https://img.shields.io/pypi/pyversions/ballast.svg)](https://pypi.org/project/ballast/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

---

## Why

Getting durable execution right on SQLite is a minefield most teams cross badly: writer serialization, `BEGIN IMMEDIATE`, the WAL pragma regime, the ambiguous-send problem after a crash, phantom events on rollback, idempotency, startup recovery of leased jobs. The usual escape hatch — run Redis/Postgres/Kafka — is impossible to ship to a customer's laptop or an air-gapped box, and absurd overkill for one process.

ballast packages the crash-tested version of all of that behind a small, boring API. It is deliberately **dependency-free** (standard library only), so there is nothing to audit but the code and it runs anywhere Python does.

## Install

```bash
pip install ballast              # core: zero dependencies
pip install "ballast[keyring]"   # optional: OS-keyring-backed secrets
```

Requires Python 3.10+.

## Quick start

```python
from ballast import Database, EventBus, JobWorker, install

db = Database("app.db")
install(db)                                    # create ballast's tables (idempotent)

bus = EventBus()
bus.declare("order.placed")
bus.subscribe("fulfilment", "order.placed",
              lambda conn, e: conn.execute("INSERT INTO orders VALUES (?)", (e.entity_id,)))

with db.transaction() as conn:                 # publish atomically with your own writes
    bus.publish(conn, "order.placed", {"total": 4999}, entity_type="order", entity_id="ord-1")

JobWorker(db, bus=bus).drain()                 # process to quiescence (or .start() in the background)
```

See [`examples/quickstart.py`](examples/quickstart.py) and [`examples/durable_jobs.py`](examples/durable_jobs.py).

## What you get

| Component | What it does |
|---|---|
| `Database` | Correctly-configured connections + a `transaction()` (`BEGIN IMMEDIATE` → commit/rollback). |
| `MigrationRunner` / `Migration` | Forward-only ladder; atomic per-step; `DowngradeError` on a newer DB. |
| `snapshot()` / `restore()` | `VACUUM INTO` backups with keep-N rotation; guarded restore. |
| `EventBus` | Declare topics, `publish()` inside your transaction, register subscribers. |
| `dispatch_pending` / `prune_events` | Advance cursors + enqueue dispatch jobs; prune consumed + aged events. |
| `JobQueue` / `JobWorker` | Transactional `enqueue`/`enqueue_unique`; single-writer worker; exactly-once, retry+backoff, startup recovery. |
| `SecretStore` | Keyring-first, env-var fallback secrets. |

## Design in one paragraph

Everything hangs off two ideas. First, **`BEGIN IMMEDIATE` on every write** (ballast runs SQLite in autocommit and issues transactions explicitly) so `busy_timeout` actually applies and writers queue instead of erroring. Second, **the outbox and the job-completion live in the caller's / worker's transaction**, so "publish the event" and "do the work" and "mark it done" are atomic with the state change. Ordering is by an `AUTOINCREMENT` `seq` (commit order under serialized writers), never by a random id. Full rationale in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Documentation

- [Architecture & design decisions](docs/ARCHITECTURE.md)
- [API reference](docs/API.md)
- [FAQ & troubleshooting](docs/FAQ.md)
- [Roadmap](ROADMAP.md) · [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md) · [Security policy](SECURITY.md) · [Code of conduct](CODE_OF_CONDUCT.md)

## Scope & non-goals

ballast is **single-process, single-machine** by design — that constraint is what makes the guarantees cheap and true. It is not a distributed queue, not a multi-writer cluster, and not a replacement for Temporal at scale. If you can run a broker and need multi-node fan-out, use one. If you must ship crash-safe durability inside one binary with no infrastructure, that is exactly what this is for.

## License

[Apache-2.0](LICENSE).
