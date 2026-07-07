# API reference

Everything below is importable directly from the top-level `ballast` package.

## Connection — `ballast.connection`

### `Database(path, *, busy_timeout_ms=5000)`
A handle to one SQLite file. Cheap and safe to share; the connections it returns are
per-thread.

- `.connect() -> sqlite3.Connection` — a new connection with ballast's pragma regime.
- `.connection()` — context manager yielding a connection, closed on exit.
- `.transaction(conn=None)` — context manager running `BEGIN IMMEDIATE` → commit on clean
  exit / rollback on error. Pass a connection to reuse it; omit for a one-off.

## Migrations — `ballast.migrations`, `ballast.schema`

### `Migration(version, apply, description="")`
`version` is a positive int (unique within a module); `apply(conn)` runs the DDL/DML inside
the runner's transaction — use `conn.execute()` per statement, never `executescript()`.

### `MigrationRunner(db)`
- `.run(module, migrations) -> list[int]` — apply pending steps atomically; returns versions
  applied. Raises `DowngradeError` if the DB is ahead of the code.
- `.current_version(module) -> int` / `.recorded_versions(module) -> set[int]`.

### `install(db) -> list[int]`
Create/upgrade ballast's own tables (`ballast_events`, `ballast_event_cursors`,
`ballast_jobs`). Idempotent. `CORE_MIGRATIONS` is the underlying ladder.

## Events — `ballast.events`

### `EventBus()`
- `.declare(topic, validator=None)` — register a topic; optional `validator(payload)` raises
  to reject. Publishing/subscribing an undeclared topic is an error.
- `.subscribe(subscriber, topic, handler)` — `handler(conn, event)`; `subscriber` is the
  durable cursor key (treat as a stable identifier).
- `.publish(conn, topic, payload, *, entity_type=None, entity_id=None) -> str` — INSERT the
  event in the caller's transaction; returns the event id. Raises `EventContractError` if a
  validator rejects the payload.

### `Event`
Frozen dataclass handed to subscribers: `seq`, `id`, `topic`, `payload: dict`, `entity_type`,
`entity_id`, `created_at`.

### Functions
- `dispatch_pending(db, bus, *, batch=500) -> int` — advance cursors, enqueue dispatch jobs;
  returns jobs enqueued. (The worker calls this each pass.)
- `prune_events(db, *, floor_days=30) -> int` — delete events consumed by every subscriber
  and older than the floor.
- `events_after(db, after_seq, *, limit=100) -> list[Event]` — tail committed events.

## Jobs — `ballast.jobs`

### `JobQueue` (singleton `queue`)
- `.enqueue(conn, kind, payload, *, run_after=None, max_attempts=5) -> str` — enqueue in the
  caller's transaction.
- `.enqueue_unique(conn, kind, payload, ...) -> str | None` — enqueue only if no
  `queued`/`running` job of `kind` exists (idempotent seeding of self-re-arming chains).

### `JobWorker(db, handlers=None, *, bus=None, poll_seconds=1.0, lease_seconds=300, retry_base_seconds=5, idle_checkpoint_seconds=60)`
`handlers` maps `kind -> callable(conn, payload)`. Pass `bus` to also drive event dispatch.

- `.run_once() -> int` — one pass (dispatch + lease/run to quiescence).
- `.drain(max_passes=1000) -> int` — repeat passes until no work remains (deterministic; use
  in tests/scripts).
- `.start()` / `.stop(timeout=5.0)` — background daemon thread (reclaims orphaned jobs on
  start). `.notify()` wakes it immediately.
- `.register(kind, handler)`.

### Functions & types
- `Job` — frozen dataclass (`seq`, `id`, `kind`, `payload`, `attempts`, `max_attempts`).
- `lease_next_job(db, *, lease_seconds=300) -> Job | None`.
- `execute_job(db, job, handlers, *, retry_base_seconds=5) -> bool`.
- `sweep_running_jobs(db) -> int`.
- `JobHandler` — the `(conn, payload) -> None` handler type.

## Snapshots — `ballast.snapshots`

- `snapshot(db, backups_dir, *, prefix="snapshot", keep=7) -> Path` — consistent `VACUUM
  INTO` copy + rotation.
- `restore(snapshot_path, db) -> None` — overwrite the database (no open connections).

## Secrets — `ballast.secrets`

### `SecretStore(service="ballast", *, use_keyring=True)`
- `.get(name) -> str | None` — keyring, then env (`name`, then `SERVICE_NAME` upper).
- `.set(name, value)` — keyring only; raises `SecretsUnavailableError` if unavailable.
- `.delete(name)` / `.keyring_available`.

## Errors — `ballast.errors`

`BallastError` (base) ← `DowngradeError` (also `RuntimeError`), `EventContractError` (also
`ValueError`), `SecretsUnavailableError` (also `RuntimeError`).
