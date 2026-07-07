# Roadmap

ballast is intentionally small. The roadmap is about depth and ergonomics, not surface
area — the core guarantees (transactional outbox, exactly-once jobs, forward-only
migrations, snapshots) are the product and are considered stable in shape.

Nothing here is a commitment or a date; it is the current thinking, in rough priority order.

## Near term
- **Observability hooks** — a pluggable logger/metrics callback on the worker (jobs run,
  failed, retried; outbox lag per subscriber) so apps can wire in their own logging without
  ballast taking a logging dependency.
- **`JobWorker` graceful drain** — a `stop(drain=True)` that finishes in-flight work within a
  deadline before exiting, for clean shutdown of long-running processes.
- **Cron / scheduled jobs** — a thin helper over `run_after` for recurring work with
  catch-up semantics (fire missed runs once at startup).
- **A small `ballast` CLI** — `ballast snapshot`, `ballast restore`, `ballast jobs ls`,
  `ballast migrate status` for operating a database from the terminal.

## Considering
- **Off-writer compute pool** — let a handler declare a pure "prepare" stage that runs off
  the single writer thread (for CPU/IO-heavy work) while the short commit stays on the
  writer, preserving exactly-once. (Kept out of 0.1 to keep the core simple and obviously
  correct.)
- **Dead-letter inspection helpers** — typed queries over `failed` jobs and a
  requeue/retry-from helper.
- **A live-tail helper** — a generator over `events_after` that blocks on `PRAGMA
  data_version` for cheap change-data-capture without busy-polling.

## Explicit non-goals
- Multi-writer or multi-node operation. ballast is single-process by design.
- Down-migrations. Roll back with a snapshot; forward-only is a safety feature, not a gap.
- A required dependency for any core feature. Extras (like `keyring`) stay optional.

Have a use case that needs one of these sooner? Open a discussion — real usage reorders this
list.
