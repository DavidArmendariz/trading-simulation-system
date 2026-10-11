package store

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

// New waits for Postgres to accept connections; the container may still be
// starting when the gateway boots.
func New(ctx context.Context, url string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parsing DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 8
	cfg.MinConns = 2

	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return &DB{Pool: pool}, nil
			}
			pool.Close()
		}
		lastErr = err
		log.Printf("store: postgres not ready (attempt %d): %v", attempt, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, fmt.Errorf("postgres unreachable: %w", lastErr)
}

// Migrate applies each .sql file in dir that has not run yet, in lexical order.
// Applied files are recorded in schema_migrations, and each file commits in the
// same transaction as its record row, so a failed migration leaves no trace.
func (d *DB) Migrate(ctx context.Context, dir string) error {
	if _, err := d.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name       TEXT        PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`); err != nil {
		return fmt.Errorf("creating schema_migrations: %w", err)
	}

	applied := make(map[string]bool)
	rows, err := d.Pool.Query(ctx, `SELECT name FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("reading schema_migrations: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("reading schema_migrations: %w", err)
		}
		applied[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading schema_migrations: %w", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading migrations dir %s: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sql" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, name := range files {
		if applied[name] {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}
		if err := d.applyMigration(ctx, name, string(body)); err != nil {
			return fmt.Errorf("applying migration %s: %w", name, err)
		}
		log.Printf("store: applied migration %s", name)
	}
	return nil
}

func (d *DB) applyMigration(ctx context.Context, name, body string) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	if _, err := tx.Exec(ctx, body); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *DB) Close() { d.Pool.Close() }
