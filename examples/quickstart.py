"""A tour of ballast: migrations, a transactional outbox, and exactly-once jobs.

Run with:  python examples/quickstart.py
Requires only the standard library plus ballast itself (zero third-party deps).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ballast import Database, EventBus, JobWorker, Migration, MigrationRunner, install


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="ballast-demo-"))
    db = Database(workdir / "app.db")

    # 1. Install ballast's tables, then your own via a forward-only migration.
    install(db)
    MigrationRunner(db).run(
        "app",
        [
            Migration(
                1, lambda c: c.execute("CREATE TABLE orders (id TEXT, total INTEGER)"), "orders"
            )
        ],
    )

    # 2. Declare a topic and subscribe a handler. The handler runs on the worker inside a
    #    transaction, so its writes commit together with the job's completion (exactly-once).
    bus = EventBus()
    bus.declare("order.placed")

    def fulfil(conn, event) -> None:  # types omitted for example brevity
        conn.execute(
            "INSERT INTO orders (id, total) VALUES (?, ?)",
            (event.entity_id, event.payload["total"]),
        )
        print(f"  fulfilled {event.entity_id} for {event.payload['total']} cents")

    bus.subscribe("fulfilment", "order.placed", fulfil)

    # 3. Publish an event ATOMICALLY with your own state change — same transaction.
    with db.transaction() as conn:
        bus.publish(conn, "order.placed", {"total": 4999}, entity_type="order", entity_id="ord-1")
    print("published order.placed (committed with the caller's transaction)")

    # 4. Process to quiescence. In production you'd JobWorker(...).start() instead.
    processed = JobWorker(db, bus=bus).drain()
    print(f"processed {processed} job(s)")

    with db.connection() as conn:
        rows = conn.execute("SELECT id, total FROM orders").fetchall()
    print("orders table:", [tuple(r) for r in rows])


if __name__ == "__main__":
    main()
