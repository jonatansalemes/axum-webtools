# Axum Web Tools

General purpose tools for the axum web framework.

`axum-webtools` is a Cargo workspace bundling a helper library and a set of operational CLI tools. Each module has its own README with full documentation — see the links below.

## Modules

| Module | Crate | Description |
|--------|-------|-------------|
| [`tools`](tools/README.md) | [`axum-webtools`](https://crates.io/crates/axum-webtools) | Axum helpers: SQLx transactions (`with_tx`), JWT auth (`Claims`), consistent HTTP responses (`HttpError`, `ok`). |
| [`pgsql-migrate`](pgsql-migrate/README.md) | [`axum-webtools-pgsql-migrate`](https://crates.io/crates/axum-webtools-pgsql-migrate) | PostgreSQL migration CLI: up/down/status, baseline, safe mode, backup & restore, split-statements, and more. |
| [`dlq-redrive`](dlq-redrive/README.md) | [`axum-webtools-dlq-redrive`](https://crates.io/crates/axum-webtools-dlq-redrive) | Kafka Dead Letter Queue redrive CLI: reprocess failed messages, route old messages to poison topics, inspect lag. |
| `macros` | [`axum-webtools-macros`](https://crates.io/crates/axum-webtools-macros) | Procedural macros supporting the `axum-webtools` library (e.g. `#[endpoint]`). |

## Docker images

axum-webtools images are available on Docker Hub:

- https://hub.docker.com/r/jslsolucoes/axum-webtools-pgsql-migrate-pg16
- https://hub.docker.com/r/jslsolucoes/axum-webtools-pgsql-migrate-pg17
- https://hub.docker.com/r/jslsolucoes/axum-webtools-pgsql-migrate-pg18
- https://hub.docker.com/r/jslsolucoes/axum-webtools-dlq-redrive

## Crates

axum-webtools is a collection of crates that provide various tools and utilities for the axum web framework.

- https://crates.io/crates/axum-webtools-pgsql-migrate
- https://crates.io/crates/axum-webtools-dlq-redrive
- https://crates.io/crates/axum-webtools

## Quick start

### Library (`axum-webtools`)

Add the helper library to your axum project — see [`tools/README.md`](tools/README.md) for a full example.

```toml
[dependencies]
axum = { version = "xxx" }
axum-webtools = { version = "xxx" }
axum-webtools-macros = { version = "xxx" }
sqlx = { version = "xxxx" }
```

### PostgreSQL migrations (`pgsql-migrate`)

```bash
cargo install axum-webtools-pgsql-migrate
pgsql-migrate up -d "postgres://user:pass@localhost/db"
```

Full documentation: [`pgsql-migrate/README.md`](pgsql-migrate/README.md).

### Kafka DLQ redrive (`dlq-redrive`)

```bash
cargo install axum-webtools-dlq-redrive
dlq-redrive status -b "localhost:9092" -s "my-dlq-topic" -g "dlq-consumer-group"
```

Full documentation: [`dlq-redrive/README.md`](dlq-redrive/README.md).
