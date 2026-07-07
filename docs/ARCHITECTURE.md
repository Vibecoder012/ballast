# Architecture & design decisions

ballast is a handful of small modules over one SQLite file. Each is useful alone; together
they give you broker-free durability.

```
ballast/
├── connection.py   Database: pragma regime + BEGIN IMMEDIATE transaction()
├── migrations.py   forward-only ladder + downgrade refusal
├── schema.py       ballast's own tables, shipped as migrations (install())
├── events.py       transactional outbox, cursors, dispatcher, pruning
├── jobs.py         leased queue + single-writer worker (exactly-once)
├── snapshots.py    VACUUM INTO backup/rotation + restore
├── secrets.py      keyring-first, env-fallback secrets
└── errors.py       one exception hierarchy
```

## The load-bearing decisions

### 1. `BEGIN IMMEDIATE` on every write, explicitly
ballast opens connections in autocommit (`isolation_level=None`) and issues transactions
itself. Every write transaction begins with `BEGIN IMMEDIATE`, taking the write lock up
front. Python's default *deferred* transactions take the lock only on first write —
mid-transaction — and that upgrade fails with `SQLITE_BUSY` immediately under contention,
ignoring `busy_timeout`. Taking it up front means `busy_timeout` applies and writers queue.
This one choice is why ballast is contention-safe where naive SQLite code is not.

### 2. The outbox is inside your transaction
`EventBus.publish` INSERTs into `ballast_events` on the connection you pass — the one already
in your `transaction()`. The event and your state change commit or roll back **together**.
There is no separate "publish" step that could succeed while the state change fails (or vice
versa), and a rolled-back transaction leaves no phantom event. This is the transactional
outbox pattern, made trivial by everything living in one file.

### 3. Ordering is `seq`, not id
Events and jobs have an `AUTOINCREMENT` integer `seq` (the log position) and a random hex
`id` (identity). Cursors and FIFO leasing use `seq`. Under serialized writers, insert order
equals commit order, so `seq` is a correct total order. A random/UUID id is *not* ordered —
a cursor keyed on one could step past an event and never process it. `AUTOINCREMENT` also
never reuses values across pruning or a VACUUM-restore.

### 4. Exactly-once = side effects and completion in one commit
The worker leases a job, then in a **single transaction** runs the handler (which writes on
that same connection) and marks the job `done`. If the handler raises, the transaction rolls
back — undoing its side effects *and* the completion — and the job is rescheduled. So a
handler either fully happened and is marked done, or didn't happen and will retry. Effects
never half-land. Event dispatch is a job, so subscriber handlers inherit the same guarantee.

### 5. Crash recovery is a startup sweep, not runtime reaping
On `start()`, every job still marked `running` is reclaimed to `queued` — the process just
started, so any such job belonged to a dead process. ballast deliberately does **not** reap
leases at runtime, because a still-executing job on another thread could be wrongly
re-queued. (Leases exist as a diagnostic/expiry field, not as the recovery mechanism.)

### 6. Forward-only migrations, downgrade refusal
Migrations only go forward. Applying a step and recording it happen in one transaction, so a
half-applied migration is impossible. If the database records a version newer than the code
ships, ballast raises `DowngradeError` and refuses to run rather than risk corrupting a
database a newer build already upgraded. To roll back, restore a snapshot — reversing schema
silently is how data gets lost.

### 7. Snapshots via `VACUUM INTO`
`VACUUM INTO` produces a consistent, defragmented copy of a live database without the
corruption risk of copying the file mid-write. `restore()` copies a snapshot back over the
main file and clears stale `-wal`/`-shm` side files so the restored file is authoritative.

### 8. Zero dependencies
The core uses only `sqlite3`, `threading`, `json`, `uuid`, `datetime`, `contextlib`, and
friends. `keyring` is an optional extra used only by `SecretStore`. Nothing to operate,
nothing to audit but the code, and it runs in air-gapped environments unchanged.

## Concurrency model

One database file. A `Database` is safe to share; its *connections* are per-thread (open one
for your app, one for the worker). The `JobWorker` owns a single writer thread — all
background writes serialize there, so the worker never contends with itself. Interactive
writes from your app thread queue against the worker via `BEGIN IMMEDIATE` + `busy_timeout`.

## Non-goals
Multi-writer clusters, multi-node fan-out, and down-migrations are out of scope by design —
each would trade away a guarantee that the single-file, single-process model provides for
free. If you can run a broker and need horizontal scale, use one; ballast is for the case
where you can't or shouldn't.
