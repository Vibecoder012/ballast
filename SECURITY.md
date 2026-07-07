# Security Policy

## Supported versions

The latest released `0.x` line receives security fixes. Until `1.0`, only the most recent
minor version is supported.

## Reporting a vulnerability

Please report suspected vulnerabilities privately using **GitHub Security Advisories**
("Report a vulnerability" on the repository's Security tab) rather than opening a public
issue. We aim to acknowledge within 3 business days and to coordinate a fix and disclosure
with you.

## Scope notes for this library

ballast's **core has no runtime dependencies** and does no network or subprocess I/O. It
does perform **local filesystem and SQLite** I/O (that is its job). Points worth
understanding:

- **SQL is your responsibility inside handlers and migrations.** ballast never builds SQL
  from your data; it uses parameterized statements throughout. When *you* write handlers or
  migrations, keep doing the same — never interpolate untrusted values into SQL strings.
- **`restore()` overwrites a database file.** Treat snapshot paths as trusted input and
  ensure no other process holds the target open when restoring.
- **Secrets never touch ballast's storage.** `SecretStore` reads/writes the OS keyring or
  environment; secret material is never written to the database, events, jobs, or logs. Only
  store secret *names* in your data.
- **`payload_json` is stored verbatim.** Don't put secrets or PII you wouldn't want at rest
  into event/job payloads; they live in the database until pruned.
