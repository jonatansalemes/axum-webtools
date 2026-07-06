# axum-webtools-pgsql-migrate

A powerful PostgreSQL migration tool included with axum-webtools that provides database schema management with advanced features for complex operations.

> Part of the [axum-webtools](../README.md) workspace.

## Crate & Docker images

- https://crates.io/crates/axum-webtools-pgsql-migrate
- https://hub.docker.com/r/jslsolucoes/axum-webtools-pgsql-migrate-pg16
- https://hub.docker.com/r/jslsolucoes/axum-webtools-pgsql-migrate-pg17
- https://hub.docker.com/r/jslsolucoes/axum-webtools-pgsql-migrate-pg18

## Installation

Install the migration tool binary:

```bash
cargo install axum-webtools-pgsql-migrate
```

## Basic Usage

```bash
# Create a new migration
pgsql-migrate create -s "create_users_table"

# Run all pending migrations
pgsql-migrate up -d "postgres://user:pass@localhost/db"

# Run migrations with specific environment (default: prod)
pgsql-migrate up -d "postgres://user:pass@localhost/db" -e dev

# Run migrations with safe mode (watch for critical tables)
pgsql-migrate up -d "postgres://user:pass@localhost/db" --safe-mode "users,orders"

# Run migrations with safe mode in CI/CD (fail instead of prompting)
pgsql-migrate up -d "postgres://user:pass@localhost/db" --safe-mode "users,orders" --safe-mode-confirm exit-with-error

# Check migration status without applying anything (read-only)
pgsql-migrate status -d "postgres://user:pass@localhost/db"

# Rollback migrations (rollback 1 migration by default)
pgsql-migrate down -d "postgres://user:pass@localhost/db"

# Rollback specific number of migrations
pgsql-migrate down -d "postgres://user:pass@localhost/db" 3

# Rollback with specific environment
pgsql-migrate down -d "postgres://user:pass@localhost/db" -e dev 3

# Baseline existing migrations (mark as applied without running)
pgsql-migrate baseline -d "postgres://user:pass@localhost/db" -v 5
```

## Migration Files

Migrations are created as pairs of `.up.sql` and `.down.sql` files:

```
migrations/
├── 000001_create_users_table.up.sql
├── 000001_create_users_table.down.sql
├── 000002_add_indexes.up.sql
├── 000002_add_indexes.down.sql
└── 000003_create_materialized_views.up.sql
└── 000003_create_materialized_views.down.sql
```

## Migration Status (Read-Only)

The `status` command reports whether the database has any pending migrations **without applying anything**. It is a read-only readiness/ordering gate — for example, an init container that must wait for migrations to be applied before an application starts.

```bash
# Report whether the database is up to date
pgsql-migrate status -d "postgres://user:pass@localhost/db"

# With an explicit migrations directory and environment
pgsql-migrate status -p migrations -d "postgres://user:pass@localhost/db" -e prod
```

**Parameters:**
- `-p, --path`: Path to the migrations directory (falls back to `MIGRATIONS_DIR`, default: `migrations`)
- `-d, --database`: Database connection URL (falls back to `DATABASE_URL`)
- `-e, --env`: Environment name (falls back to `ENV`, default: `prod`; informational — pending state is version-based)

**Exit codes** let callers gate on status without parsing stdout:

| Exit code | Meaning |
|-----------|---------|
| `0` | Up to date — every on-disk migration has been applied |
| `1` | One or more migrations are pending |
| `2` | Error — a dirty migration was found, or the database is unreachable |

**Example output:**

```bash
$ pgsql-migrate status -d "$DATABASE_URL"
Checking migration status in environment: prod
Applied version: 2
Status: 1 migration(s) pending: 000003
$ echo $?
1
```

**Example: init-container gate (wait for migrations before starting the app)**

```bash
# Block until the database is fully migrated
until pgsql-migrate status -d "$DATABASE_URL"; do
  echo "Migrations pending or database unavailable, waiting..."
  sleep 5
done
echo "Database is up to date — starting application"
```

## Advanced Features

### 1. No Transaction Feature (`no-tx`)

Some PostgreSQL operations cannot run within transactions. Use the `no-tx` feature for operations like:
- `CREATE INDEX CONCURRENTLY`
- `CREATE MATERIALIZED VIEW`
- `ALTER TYPE ADD VALUE`

**Example:**

```sql
-- features: no-tx

-- This migration runs without a transaction wrapper
CREATE INDEX CONCURRENTLY idx_users_email ON users(email);

-- Multiple materialized views in the same script
CREATE MATERIALIZED VIEW user_stats AS
SELECT 
    DATE(created_at) as date,
    COUNT(*) as user_count
FROM users 
GROUP BY DATE(created_at);

CREATE MATERIALIZED VIEW daily_activity AS
SELECT 
    DATE(last_login) as login_date,
    COUNT(*) as active_users
FROM users 
WHERE last_login IS NOT NULL
GROUP BY DATE(last_login);
```

### 2. Split Statements Feature (`split-statements`)

When you need to execute multiple complex operations that require separate execution contexts, use the `split-statements` feature with markers:

**Example:**

```sql
-- features: split-statements

-- First block: Create base tables
-- split-start
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

INSERT INTO categories (name) VALUES 
    ('Electronics'),
    ('Books'),
    ('Clothing');
-- split-end

-- Second block: Create dependent materialized view
-- split-start
CREATE MATERIALIZED VIEW category_stats AS
SELECT 
    c.name,
    COUNT(p.id) as product_count
FROM categories c
LEFT JOIN products p ON p.category_id = c.id
GROUP BY c.id, c.name;

-- Create indexes on the materialized view
CREATE INDEX idx_category_stats_name ON category_stats(name);
-- split-end

-- Third block: Grant permissions
-- split-start
GRANT SELECT ON category_stats TO readonly_user;
GRANT ALL ON categories TO app_user;
-- split-end
```

### 3. Skip On Environment Feature (`skip-on-env`)

Skip specific SQL blocks based on the current environment. This feature works at the **block level** within split statements, allowing fine-grained control over which blocks execute in different environments.

Use the `--env` or `-e` CLI parameter to specify the current environment (default: `prod`).

**Example: Skip seed data blocks in production**

```sql
-- features: split-statements

-- Block 1: Schema changes (runs in all environments)
-- split-start
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
-- split-end

-- Block 2: Seed data (skip in production)
-- split-start
-- skip-on-env prod
INSERT INTO users (email) VALUES
    ('dev@example.com'),
    ('test@example.com');
-- split-end

-- Block 3: More schema changes (runs in all environments)
-- split-start
CREATE INDEX idx_users_email ON users(email);
-- split-end
```

**Example: Skip performance optimizations in dev/homolog**

```sql
-- features: no-tx, split-statements

-- Block 1: Basic index (runs everywhere)
-- split-start
CREATE INDEX CONCURRENTLY idx_orders_user ON orders(user_id);
-- split-end

-- Block 2: Heavy index (skip in dev and homolog)
-- split-start
-- skip-on-env dev,homolog
CREATE INDEX CONCURRENTLY idx_orders_complex ON orders(created_at, status, total);
-- split-end
```

**Running with environment:**

```bash
# Run in dev environment - blocks with "-- skip-on-env dev" will be skipped
pgsql-migrate up -d "postgres://user:pass@localhost/db" -e dev

# Run in production (default) - blocks with "-- skip-on-env prod" will be skipped
pgsql-migrate up -d "postgres://user:pass@localhost/db"

# Run in homolog environment
pgsql-migrate up -d "postgres://user:pass@localhost/db" -e homolog
```

### 4. Safe Mode (`--safe-mode`)

Safe mode lets you specify critical or high-traffic table names to watch for in pending migrations. When a pending migration's SQL references one of those tables, the tool warns you and — depending on the `--safe-mode-confirm` option — either prompts for confirmation or aborts with an error.

**CLI Flags (on `up` command):**
- `--safe-mode <TABLES>` — comma-separated list of table names to monitor
- `--safe-mode-confirm <ask|exit-with-error>` — action when an unacknowledged table is found (default: `ask`)

**Acknowledgement persistence (`safe-mode.yml`):**

Once you confirm a migration interactively (`ask` mode), the table is saved to `safe-mode.yml` inside the migrations directory. Subsequent runs will not prompt again for that migration/table combination.

```yaml
# migrations/safe-mode.yml (auto-managed)
migrations:
  000003_alter_users.up.sql:
    - users
  000005_reindex_orders.up.sql:
    - orders
```

**Example: Interactive prompt (default)**

```bash
# Watch for any migration that touches 'users' or 'orders'
pgsql-migrate up \
  -d "postgres://user:pass@localhost/db" \
  --safe-mode "users,orders"

# Output:
#   WARNING: Safe-mode table(s) [users] found in migration 000003_alter_users.up.sql.
#   This migration may affect large or critical tables — review carefully before applying to production.
#   Apply this migration? (y/N): y
#   Applying migration: 000003_alter_users.up.sql
#     Applied successfully
```

**Example: CI/CD mode (fail on unacknowledged tables)**

Use `exit-with-error` in pipelines where interactive prompts are not possible. Pre-acknowledge migrations by committing `safe-mode.yml` to your repository.

```bash
pgsql-migrate up \
  -d "postgres://user:pass@localhost/db" \
  --safe-mode "users,orders,payments" \
  --safe-mode-confirm exit-with-error

# If an unacknowledged migration is found:
#   ERROR: Unacknowledged table(s) [users] in migration 000003_alter_users.up.sql.
#   Add them to safe-mode.yml or remove --safe-mode-confirm=exit-with-error to be prompted.
#   (exits with non-zero status)
```

**Workflow: Pre-acknowledging migrations for CI/CD**

```bash
# 1. Run locally with --safe-mode to review and acknowledge migrations
pgsql-migrate up \
  -d "postgres://user:pass@localhost/db" \
  --safe-mode "users,orders"
# Answer 'y' to each prompt — acknowledgements are saved to migrations/safe-mode.yml

# 2. Commit safe-mode.yml to your repository
git add migrations/safe-mode.yml
git commit -m "acknowledge migration 000003 touching users table"

# 3. CI/CD runs without prompts — already-acknowledged migrations are skipped
pgsql-migrate up \
  -d "$DATABASE_URL" \
  --safe-mode "users,orders" \
  --safe-mode-confirm exit-with-error
  
# 4. Dev workflows
pgsql-migrate up \
  -d "$DATABASE_URL" \
  --safe-mode "users,orders" \
  --post-execute "./some-dev-script.sql,./another-dev-script.sql"
```

### 5. Combined Features

You can combine features for complex scenarios:

**Example: Multiple materialized views without transactions**

```sql
-- features: no-tx, split-statements

-- First materialized view block
-- split-start
CREATE MATERIALIZED VIEW hourly_sales AS
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    SUM(total_amount) as total_sales,
    COUNT(*) as order_count
FROM orders
GROUP BY DATE_TRUNC('hour', created_at);
-- split-end

-- Second materialized view block
-- split-start
CREATE MATERIALIZED VIEW product_performance AS
SELECT 
    p.id,
    p.name,
    COUNT(oi.id) as times_sold,
    SUM(oi.quantity) as total_quantity
FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
GROUP BY p.id, p.name;
-- split-end

-- Concurrent indexes block
-- split-start
CREATE INDEX CONCURRENTLY idx_hourly_sales_hour ON hourly_sales(hour);
CREATE INDEX CONCURRENTLY idx_product_performance_times_sold ON product_performance(times_sold DESC);
-- split-end
```

## Database Backup and Restore

The `pgsql-migrate` tool includes comprehensive backup and restore functionality using PostgreSQL's native `pg_dump` and `pg_restore` utilities.

### Backup Command

Create database backups with various formats and compression options:

```bash
# Basic backup with custom format (recommended)
pgsql-migrate backup -d "postgres://user:pass@localhost/db" -o backup.dump

# Backup with compression level 9
pgsql-migrate backup -d "postgres://user:pass@localhost/db" -o backup.dump -c 9

# Backup in plain SQL format (no compression supported)
pgsql-migrate backup -d "postgres://user:pass@localhost/db" -o backup.sql -f plain

# Backup in directory format
pgsql-migrate backup -d "postgres://user:pass@localhost/db" -o backup_dir -f directory -c 5

# Backup without ownership and ACL information
pgsql-migrate backup -d "postgres://user:pass@localhost/db" -o backup.dump --no-owner --no-acl --max-retain-days 10
```

**Backup Parameters:**
- `-d, --database`: Database connection URL (required)
- `-o, --output`: Output file/directory path (required)
- `-f, --format`: Backup format - `plain`, `custom`, `directory`, or `tar` (default: `custom`)
- `-c, --compress`: Compression level 0-9 (not supported for plain format)
- `--no-owner`: Exclude ownership information from backup
- `--no-acl`: Exclude access control list (ACL) information from backup
- `--max-retain-days`: Maximum number of days to retain backups (default: 15). Routine will use file metadata created timestamps to delete old backups.

**Supported Formats:**
- **custom** (recommended): Compressed binary format, restorable with pg_restore, allows selective restore
- **plain**: Plain SQL script, restorable with psql, human-readable but larger
- **directory**: Directory of files, one per table, supports parallel restore
- **tar**: Tar archive format, restorable with pg_restore

### Restore Command

Restore databases from backup files:

```bash
# Basic restore from custom format
pgsql-migrate restore -d "postgres://user:pass@localhost/db" -i backup.dump

# Restore from plain SQL file
pgsql-migrate restore -d "postgres://user:pass@localhost/db" -i backup.sql

# Restore with clean option (drop existing objects first)
pgsql-migrate restore -d "postgres://user:pass@localhost/db" -i backup.dump --clean

# Restore with create option (create database before restoring)
pgsql-migrate restore -d "postgres://user:pass@localhost/db" -i backup.dump --create

# Restore without ownership and ACL information
pgsql-migrate restore -d "postgres://user:pass@localhost/db" -i backup.dump --no-owner --no-acl
```

**Restore Parameters:**
- `-d, --database`: Database connection URL (required)
- `-i, --input`: Input backup file/directory path (required)
- `--clean`: Drop database objects before recreating them
- `--create`: Create the database before restoring
- `--no-owner`: Skip restoration of ownership
- `--no-acl`: Skip restoration of access privileges (ACLs)

**Format Detection:**
The tool automatically detects whether the backup is in plain SQL format (using `psql`) or binary format (using `pg_restore`) by:
1. Checking if the file extension is `.sql`
2. Reading the first few bytes to detect the `PGDMP` magic header for custom/directory/tar formats

### Use Cases

**Development Workflow:**
```bash
# 1. Backup production database (without ownership for portability)
pgsql-migrate backup \
  -d "postgres://prod_user:pass@prod.example.com/myapp" \
  -o prod_backup.dump \
  -c 9 \
  --no-owner \
  --no-acl

# 2. Restore to local development database
pgsql-migrate restore \
  -d "postgres://dev_user:pass@localhost/myapp_dev" \
  -i prod_backup.dump \
  --clean
```

**Migration Testing:**
```bash
# 1. Backup database before running migrations
pgsql-migrate backup \
  -d "postgres://user:pass@localhost/db" \
  -o pre_migration_backup.dump \
  -c 9

# 2. Run migrations
pgsql-migrate up -d "postgres://user:pass@localhost/db"

# 3. If something goes wrong, restore from backup
pgsql-migrate restore \
  -d "postgres://user:pass@localhost/db" \
  -i pre_migration_backup.dump \
  --clean
```

**Scheduled Backups:**
```bash
# Create daily backups with timestamp
pgsql-migrate backup \
  -d "postgres://user:pass@localhost/db" \
  -o "backups/db_backup_$(date +%Y%m%d_%H%M%S).dump" \
  -c 9 \
  --no-owner \
  --no-acl
```

### Requirements

The backup and restore commands require PostgreSQL client tools to be installed:

- `pg_dump` - for creating backups
- `pg_restore` - for restoring binary format backups
- `psql` - for restoring plain SQL backups

**Installation examples:**
```bash
# Ubuntu/Debian
sudo apt update && sudo apt install postgresql-client-16

# MacOS
brew install postgresql@16

# RedHat/CentOS
sudo yum install postgresql16
```

**PostgreSQL Version Compatibility:**
The tool automatically detects the installed `pg_dump` version and adjusts compression flags accordingly:
- PostgreSQL 16+: Uses `--compress=gzip:N` syntax
- PostgreSQL 15 and earlier: Uses `--compress N` syntax

## Migration Tracking

The tool automatically:
- Creates a `pgsql_migrate_schema_migrations` table to track applied migrations
- Stores content hashes to detect changes in already-applied migrations
- Marks migrations as "dirty" during execution to handle failed migrations
- Validates migration integrity before execution

## Error Handling

- **Dirty migrations**: If a migration fails, it's marked as dirty and must be manually resolved
- **Content changes**: Warns when applied migration content has changed
- **Validation**: Ensures proper marker pairing in split-statements feature
- **Transaction safety**: Automatically handles transaction wrapping based on features
- **Safe mode abort**: When `--safe-mode-confirm exit-with-error` is set and an unacknowledged critical table is found, the migration is aborted before any SQL is executed
- **Pre/Post hooks**: Allows for custom setup and teardown logic before and after migration operations

## Use Cases

**Perfect for:**
- **Database schema evolution** with complex dependencies
- **Creating multiple materialized views** that need separate execution contexts
- **Concurrent index creation** without blocking operations
- **Data migrations** that require multi-step processing
- **Permission management** across multiple database objects
- **Performance optimizations** that need specific execution patterns
- **Protecting critical tables** in production with safe mode guards
- **Deployment gating** with the read-only `status` command (init containers, health checks)

**Example: Complex E-commerce Migration**

```sql
-- features: no-tx, split-statements

-- Create core product tables
-- split-start
CREATE TABLE product_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    parent_id INTEGER REFERENCES product_categories(id)
);

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    category_id INTEGER NOT NULL REFERENCES product_categories(id),
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
-- split-end

-- Create performance materialized views
-- split-start
CREATE MATERIALIZED VIEW category_hierarchy AS
WITH RECURSIVE cat_tree AS (
    SELECT id, name, parent_id, 0 as level, ARRAY[id] as path
    FROM product_categories WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, t.level + 1, t.path || c.id
    FROM product_categories c
    JOIN cat_tree t ON c.parent_id = t.id
)
SELECT * FROM cat_tree;
-- split-end

-- Create concurrent indexes for performance
-- split-start
CREATE INDEX CONCURRENTLY idx_products_category_price ON products(category_id, price DESC);
CREATE INDEX CONCURRENTLY idx_products_created_at ON products(created_at DESC);
-- split-end
```

This comprehensive migration system ensures reliable, trackable, and flexible database schema management for complex applications.
