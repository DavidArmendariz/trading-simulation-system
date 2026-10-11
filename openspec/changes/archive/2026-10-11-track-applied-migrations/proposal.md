## Why
The gateway re-runs every migration on each boot with no record of what ran. That only works while every file is idempotent; one non-idempotent migration (an `ALTER TABLE ADD COLUMN`, a data backfill) breaks every restart after the first.

## What Changes
- Record applied migrations in a `schema_migrations` table.
- Apply only files not yet recorded, each in its own transaction with its record row.
- Existing databases: the first boot after this change re-applies 001-003 once (all idempotent) and records them.

## Impact
- Affected specs: market-data-gateway
- Affected code: `gateway/internal/store/postgres.go`
