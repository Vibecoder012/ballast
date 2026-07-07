# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — initial release

### Added
- **`Database`** — connection factory with the correct SQLite pragma regime (WAL, foreign
  keys, `synchronous=NORMAL`, `busy_timeout`) and a `transaction()` context manager that
  issues explicit `BEGIN IMMEDIATE` → commit/rollback.
- **Forward-only migration ladder** (`Migration`, `MigrationRunner`): atomic per-step
  application, idempotent re-runs, and `DowngradeError` when the database is newer than the
  code.
- **`install()`** — creates ballast's own tables (`ballast_events`, `ballast_event_cursors`,
  `ballast_jobs`) as a versioned ladder under the `ballast` module.
- **Transactional outbox** (`EventBus`): `publish()` inside the caller's transaction,
  declared topics with optional validators, a subscription registry, `dispatch_pending`
  (per-subscriber durable cursors), `prune_events`, and `events_after` for tailing.
- **Job queue + worker** (`JobQueue`, `JobWorker`): transactional `enqueue`/`enqueue_unique`,
  a single-writer worker with exactly-once execution, retry with exponential backoff,
  startup recovery of orphaned `running` jobs, and both synchronous (`drain`) and background
  (`start`/`stop`/`notify`) modes.
- **Snapshots** (`snapshot`, `restore`): `VACUUM INTO` backups with keep-N rotation and a
  guarded restore.
- **`SecretStore`**: OS-keyring-backed secrets (optional `keyring` extra) with an
  environment-variable fallback.
- Zero runtime dependencies; inline type information (`py.typed`); Python 3.10–3.14.
