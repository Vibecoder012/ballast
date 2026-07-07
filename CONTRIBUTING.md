# Contributing to ballast

Thanks for your interest! ballast lives or dies on being **correct and boring** — a piece
of infrastructure people trust with their data. Contributions that protect that are very
welcome.

## Ground rules

- **Zero runtime dependencies in the core.** The library imports only the standard library.
  Optional features may add an extra (as `keyring` does); the core never does.
- **Correctness is tested, not asserted.** Any change to the outbox, job worker, or
  migration runner needs a test that would fail without it. The crash/atomicity tests are
  the crown jewels — extend them, don't weaken them.
- **Single-process, single-machine.** Proposals that require a second process, a broker, or
  multi-writer semantics are out of scope by design (see [ROADMAP.md](ROADMAP.md)).
- **Everything is typed.** `mypy --strict` must pass on `src`.

## Development setup

```bash
git clone https://github.com/Vibecoder012/ballast
cd ballast
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -e ".[dev,keyring]"
```

## The checks (all must pass)

```bash
ruff check .
ruff format --check .
mypy src
pytest
```

With [`uv`](https://docs.astral.sh/uv/): `uvx ruff check . && uvx mypy src && python -m pytest`.

## A note on migrations in tests

A migration's `apply(conn)` runs inside a transaction ballast owns. Use `conn.execute(...)`
one statement at a time — **not** `conn.executescript(...)`, which forces an implicit
`COMMIT` and would break that transaction. (This is also the rule for real migrations.)

## Pull requests

1. Fork and branch from `main`.
2. Add/extend tests; keep the atomicity and exactly-once coverage intact.
3. Update `CHANGELOG.md` under `[Unreleased]`.
4. Keep PRs focused.

By contributing you agree that your contributions are licensed under the project's
[Apache-2.0](LICENSE) license.
