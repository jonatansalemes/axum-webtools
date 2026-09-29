# axum-webtools-clickhouse-migrate

A ClickHouse migration tool included with axum-webtools. It is modelled on [`pgsql-migrate`](../pgsql-migrate/README.md) and keeps the same file layout, commands and workflow, adapted to how ClickHouse works.

> Part of the [axum-webtools](../README.md) workspace.

## Crate & Docker image

- https://crates.io/crates/axum-webtools-clickhouse-migrate
- https://hub.docker.com/r/jslsolucoes/axum-webtools-clickhouse-migrate

## Installation

```bash
cargo install axum-webtools-clickhouse-migrate
```

## Connection URL

The tool talks to the ClickHouse **HTTP interface** (port `8123`, or `8443` for TLS):

```
http://user:password@host:8123/database?setting=value
https://user:password@host:8443/database
clickhouse://user:password@host:8123/database   # alias for http://
```

- `user` defaults to `default`, `database` defaults to `default`.
- Percent-encode special characters in the user, password and database (`p%40ss` → `p@ss`).
- Query-string parameters are sent as ClickHouse settings on every request, e.g. `?max_execution_time=600&mutations_sync=2`.

The URL is read from `--database`/`-d` or, for `up`, `down` and `status`, from the `CLICKHOUSE_URL` environment variable. (`DATABASE_URL` is deliberately not used, since applications usually point it at their OLTP database.)

## Basic Usage

```bash
# Create a new migration
clickhouse-migrate create -s "create_events_table"

# Run all pending migrations
clickhouse-migrate up -d "http://user:pass@localhost:8123/analytics"

# Run migrations with specific environment (default: prod)
clickhouse-migrate up -d "http://user:pass@localhost:8123/analytics" -e dev

# Run migrations with safe mode (watch for critical tables)
clickhouse-migrate up -d "http://user:pass@localhost:8123/analytics" --safe-mode "events,sessions"

# Check migration status without applying anything
clickhouse-migrate status -d "http://user:pass@localhost:8123/analytics"

# Rollback migrations (1 by default, or a specific number)
clickhouse-migrate down -d "http://user:pass@localhost:8123/analytics" 3

# Baseline existing migrations (mark as applied without running)
clickhouse-migrate baseline -d "http://user:pass@localhost:8123/analytics" -v 5

# Re-apply the dirty migration after fixing it
clickhouse-migrate redo -d "http://user:pass@localhost:8123/analytics"

# Mark a dirty migration as clean after fixing the database by hand
clickhouse-migrate force -d "http://user:pass@localhost:8123/analytics" -v 5
```

Environment variables supported by `up`, `down` and `status`: `CLICKHOUSE_URL`, `CLICKHOUSE_MIGRATIONS_DIR` (default `ch-migrations`), `ENV` (default `prod`).

## Migration Files

Migrations are pairs of `.up.sql` and `.down.sql` files, exactly like `pgsql-migrate`. The default directory is `ch-migrations` (override with `-p`/`--path`, `-d`/`--dir` for `create`, or `CLICKHOUSE_MIGRATIONS_DIR`), so it can sit next to a `pgsql-migrate` `migrations` directory:

```
ch-migrations/
├── 000001_create_events_table.up.sql
├── 000001_create_events_table.down.sql
├── 000002_add_daily_rollup.up.sql
└── 000002_add_daily_rollup.down.sql
```

A file can hold any number of statements separated by `;`:

```sql
CREATE TABLE IF NOT EXISTS events (
    id UInt64,
    name String,
    created_at DateTime
) ENGINE = MergeTree
ORDER BY (created_at, id);

CREATE MATERIALIZED VIEW IF NOT EXISTS events_daily_mv
ENGINE = SummingMergeTree ORDER BY day
AS SELECT toDate(created_at) AS day, count() AS total FROM events GROUP BY day;
```

## How it differs from pgsql-migrate

### One statement per request

The ClickHouse HTTP interface runs exactly one statement per request, so every file is split on `;` and the statements are sent one by one, in order. Semicolons inside `'strings'`, `"identifiers"`, `` `identifiers` ``, `$heredocs$` and comments (`--`, `#`, `/* */`) do not split. Chunks that contain only comments are skipped.

> Inline-data inserts (`INSERT INTO t FORMAT CSV ...`) whose data contains `;` are not supported; use `INSERT ... VALUES` with quoted values instead.

### No transactions

ClickHouse has no transactions, so `pgsql-migrate`'s `no-tx` feature does not exist here. If a statement fails, the statements before it **stay applied** and the migration is marked **dirty**. Any further `up`/`down`/`status` refuses to run until you resolve it:

1. Fix the migration file and/or the database state.
2. Either run `redo` to re-apply the migration from the start, or `force -v <version>` to mark it clean without running anything.

Because `redo` re-runs statements that already succeeded, write migrations idempotently: `CREATE TABLE IF NOT EXISTS`, `DROP TABLE IF EXISTS`, `ADD COLUMN IF NOT EXISTS`, and so on.

### No backup / restore

`pgsql-migrate`'s `backup`/`restore` wrap `pg_dump`/`pg_restore`. ClickHouse backups are server-side (`BACKUP DATABASE ... TO Disk(...)` / `S3(...)`) and depend on server configuration, so they are left to ClickHouse itself.

## Status

`status` reports whether the database has any pending migrations without applying anything. Exit codes:

| Code | Meaning |
|------|---------|
| `0` | Up to date: every on-disk migration is applied |
| `1` | One or more migrations are pending |
| `2` | A migration is dirty, or the database is unreachable |

```bash
# Block until the database is fully migrated (e.g. in an init container)
until clickhouse-migrate status; do sleep 5; done
```

## Advanced Features

### Split Statements and Skip On Environment

The `split-statements` feature groups statements into blocks with `-- split-start` / `-- split-end` markers. Blocks are run in order, and a block tagged with `-- skip-on-env` is skipped when the current environment matches:

```sql
-- features: split-statements

-- split-start
CREATE TABLE IF NOT EXISTS events (id UInt64, created_at DateTime)
ENGINE = MergeTree ORDER BY (created_at, id);
-- split-end

-- split-start
-- skip-on-env dev,test
ALTER TABLE events MODIFY TTL created_at + INTERVAL 90 DAY;
-- split-end
```

```bash
clickhouse-migrate up -e dev   # the TTL block is skipped
clickhouse-migrate up          # env defaults to prod: everything runs
```

Each block can contain several `;`-separated statements.

### Safe Mode (`--safe-mode`)

Safe mode works exactly as in `pgsql-migrate`: pending migrations that mention a watched table (as a whole identifier, outside comments) require confirmation, and acknowledgements are stored in `ch-migrations/safe-mode.yml` so they only need to be given once and can be committed.

This is particularly useful on ClickHouse, where `ALTER TABLE ... UPDATE/DELETE`, `MODIFY COLUMN` or `MATERIALIZE` on large tables start heavy background mutations.

```bash
# Locally: review and acknowledge (prompts y/N, saves safe-mode.yml)
clickhouse-migrate up --safe-mode "events,sessions"

# CI/CD: fail instead of prompting on anything not yet acknowledged
clickhouse-migrate up --safe-mode "events,sessions" --safe-mode-confirm exit-with-error
```

`down` removes the rolled-back migration from `safe-mode.yml` unless `--safe-mode-skip-auto-remove` is given.

### Pre/Post Execute Hooks

`up`, `down` and `redo` accept `--pre-execute` and `--post-execute` with comma-separated SQL files to run before and after the migrations (for example, to refresh a dictionary or grant permissions):

```bash
clickhouse-migrate up --post-execute "hooks/grants.sql,hooks/reload_dicts.sql"
```

## Migration Tracking

Applied migrations are tracked in `clickhouse_migrate_schema_migrations`, created automatically in the target database:

| Column | Type | Description |
|--------|------|-------------|
| `version` | `Int64` | Migration version |
| `dirty` | `UInt8` | `1` while the migration is running or after it failed |
| `content_hash` | `String` | SHA-256 of the `.up.sql` file |
| `is_deleted` | `UInt8` | `1` when the migration was rolled back |
| `sequence` | `UInt64` | Monotonic write order |
| `applied_at` | `DateTime64(3, 'UTC')` | When the row was written |

In-place `UPDATE`/`DELETE` are asynchronous mutations in ClickHouse, so the table is an **append-only log**: every state change inserts a row, and the current state of a version is its row with the highest `sequence`. To inspect it:

```sql
SELECT version, argMax(dirty, sequence) AS is_dirty, max(applied_at) AS last_change
FROM clickhouse_migrate_schema_migrations
GROUP BY version
HAVING argMax(is_deleted, sequence) = 0
ORDER BY version;
```

As in `pgsql-migrate`, `up` warns when an applied migration's file has changed since it was applied.

## Limitations

- The tracking table uses a plain `MergeTree` on the node you connect to. On a multi-replica cluster, always point the tool at the same node (or a load balancer with sticky sessions); write `ON CLUSTER` DDL in your migrations as needed.
