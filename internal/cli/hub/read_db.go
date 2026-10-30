package hub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const readDBOptions = "mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"

// openReadDB opens the migrated node database through a separate pool whose
// every connection is query-only and whose SQLite URI is opened read-only.
// The writer pool creates and migrates the file before this function is called.
func openReadDB(ctx context.Context, path string) (*sql.DB, error) {
	if path == "" || strings.Contains(path, "?") || strings.HasPrefix(path, "file:") || path == ":memory:" {
		return nil, fmt.Errorf("invalid read database path")
	}
	clean := filepath.Clean(path)
	root, err := os.OpenRoot(filepath.Dir(clean))
	if err != nil {
		return nil, fmt.Errorf("open read database directory: %w", err)
	}
	file, err := root.OpenFile(filepath.Base(clean), os.O_RDONLY, 0)
	rootErr := root.Close()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open read database file: %w", err), rootErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return nil, errors.Join(closeErr, rootErr)
	}
	if rootErr != nil {
		return nil, fmt.Errorf("close read database directory: %w", rootErr)
	}

	dsn := (&url.URL{Scheme: "file", Path: clean}).String() + "?" + readDBOptions
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open read sqlite pool: %w", err)
	}
	db.SetMaxOpenConns(4)
	var queryOnly int
	if err := db.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil {
		return nil, closeReadDB(db, fmt.Errorf("read query_only: %w", err))
	}
	if queryOnly != 1 {
		return nil, closeReadDB(db, fmt.Errorf("query_only is %d, want 1", queryOnly))
	}
	return db, nil
}

func closeReadDB(db *sql.DB, err error) error {
	if closeErr := db.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}
