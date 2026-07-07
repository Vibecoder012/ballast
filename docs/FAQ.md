# FAQ & troubleshooting

### How is this different from Celery / RQ / Dramatiq?
Those are job queues that need a broker (Redis/RabbitMQ) and don't give you a transactional
outbox or migrations. ballast needs no broker, and its jobs commit their side effects and
completion in one transaction (exactly-once), on the same SQLite file as your data.

### How is it different from Temporal / DBOS / Step Functions?
Those are excellent at scale and require a cluster or Postgres. ballast targets the opposite
end: one process, one file, offline-capable, nothing to operate. If you can run the
infrastructure and need multi-node durable execution, use them.

### `sqlite3.OperationalError: cannot commit - no transaction is active`
You almost certainly called `conn.executescript(...)` inside a `transaction()` or a
migration. `executescript` forces an implicit `COMMIT` first, ending ballast's transaction.
Run one statement per `conn.execute(...)` instead.

### Can I run the worker in the background?
Yes: `worker.start()` runs it on a daemon thread; `worker.stop()` stops it; `worker.notify()`
wakes it immediately after you commit new work (otherwise it wakes on the poll interval). For
tests and one-shot scripts, `worker.drain()` processes everything synchronously.

### Do I need the worker at all?
Only for background jobs and event dispatch. You can use `Database` + migrations + snapshots
purely for the connection discipline and safe schema/backup story, with no worker.

### Is `":memory:"` supported?
For simple single-connection use, yes — but each connection to `":memory:"` is a *separate*
database, so anything involving the worker (which opens its own connection) needs a real
file. Use a temp file in tests (the test suite does).

### How do I schedule work for later / build a recurring task?
Use `run_after` (an ISO-8601 `...Z` timestamp) on `enqueue`. For a recurring chain, seed once
with `enqueue_unique`, and have the handler re-enqueue the next occurrence with a future
`run_after`. See `examples/` and the worker tests.

### A handler keeps failing — where does it end up?
After `max_attempts` (default 5), the job's status becomes `failed` and `last_error` holds
the exception. Failed jobs are not retried automatically; query them and requeue if desired.

### How do I roll back a bad migration or a bad deploy?
Take a `snapshot()` before upgrading; if you need to go back, close the app and `restore()`
that snapshot. ballast is forward-only on purpose — there are no down-migrations.

### Which Python versions?
3.10 through 3.14, tested in CI on Linux, macOS, and Windows.
