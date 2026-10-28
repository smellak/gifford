package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	// Blank to register the sqlite driver with database/sql.
	_ "modernc.org/sqlite"

	"github.com/smellak/gifford/migrations"
)

// This file holds the store: opening one SQLite file per node in WAL mode
// and the migration entry points.

// dsnOptions configures every connection the pool opens.
const dsnOptions = "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)" +
	"&_pragma=recursive_triggers(1)&_pragma=synchronous(NORMAL)&_txlock=immediate"

// Store is one open event store. It is safe for concurrent use.
type Store struct {
	db     *sql.DB
	writer chan struct{} // one slot: the coordinated writer of this process
}

// Open opens the event store at path, creating the file if needed, and
// applies every pending migration.
func Open(ctx context.Context, path string) (*Store, error) {
	db, err := openDB(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := migrateUp(ctx, db, migrations.FS()); err != nil {
		return nil, closePool(db, err)
	}
	return &Store{db: db, writer: make(chan struct{}, 1)}, nil
}

// DB returns the connection pool for packages that own their tables
// (internal/channel's Reload). Events are written only by AppendEvents.
func (s *Store) DB() *sql.DB { return s.db }

// Close closes the connection pool.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close event store: %w", err)
	}
	return nil
}

// MigrateUp applies every pending embedded migration.
func (s *Store) MigrateUp(ctx context.Context) error {
	return s.withWriter(ctx, func() error { return migrateUp(ctx, s.db, migrations.FS()) })
}

// MigrateDown reverts the newest steps applied migrations, newest first.
func (s *Store) MigrateDown(ctx context.Context, steps int) error {
	return s.withWriter(ctx, func() error { return migrateDown(ctx, s.db, migrations.FS(), steps) })
}

// withWriter runs fn holding the process-wide writer slot, honouring the
// context while waiting for it.
func (s *Store) withWriter(ctx context.Context, fn func() error) error {
	select {
	case s.writer <- struct{}{}:
	case <-ctx.Done():
		return fmt.Errorf("wait for writer slot: %w", ctx.Err())
	}
	defer func() { <-s.writer }()
	return fn()
}

// openDB opens the file and the pool and checks the journal mode, without
// migrating. Runner tests call it with their own file systems.
func openDB(ctx context.Context, path string) (*sql.DB, error) {
	if path == "" || strings.Contains(path, "?") || strings.HasPrefix(path, "file:") || path == ":memory:" {
		return nil, ErrInvalidArgument
	}
	clean := filepath.Clean(path)
	root, err := os.OpenRoot(filepath.Dir(clean))
	if err != nil {
		return nil, fmt.Errorf("open database directory: %w", err)
	}
	file, err := root.OpenFile(filepath.Base(clean), os.O_RDWR|os.O_CREATE, 0o600)
	rootErr := root.Close()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create database file: %w", err), rootErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return nil, errors.Join(closeErr, rootErr)
	}
	if rootErr != nil {
		return nil, fmt.Errorf("close database directory: %w", rootErr)
	}
	db, err := sql.Open("sqlite", clean+"?"+dsnOptions)
	if err != nil {
		return nil, fmt.Errorf("open sqlite pool: %w", err)
	}
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return nil, closePool(db, fmt.Errorf("read journal_mode: %w", err))
	}
	if mode != "wal" {
		return nil, closePool(db, fmt.Errorf("journal_mode is %q, want wal", mode))
	}
	return db, nil
}

// closePool closes db, joining any close error onto err.
func closePool(db *sql.DB, err error) error {
	if closeErr := db.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}
