package dataset

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	// Register the SQLite driver used by the read-only native-label pool.
	_ "modernc.org/sqlite"
)

const nativeReadDBOptions = "mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"

// openNativeReadDB opens the already-migrated node database without creating,
// migrating, or acquiring a writer. Both SQLite and every pooled connection
// enforce the read-only boundary.
func openNativeReadDB(ctx context.Context, path string) (*sql.DB, error) {
	if path == "" || strings.Contains(path, "?") || strings.HasPrefix(path, "file:") || path == ":memory:" {
		return nil, errors.New("invalid native label database path")
	}
	clean := filepath.Clean(path)
	root, err := os.OpenRoot(filepath.Dir(clean))
	if err != nil {
		return nil, fmt.Errorf("open native label database directory: %w", err)
	}
	file, err := root.OpenFile(filepath.Base(clean), os.O_RDONLY, 0)
	rootErr := root.Close()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("open native label database: %w", err), rootErr)
	}
	if closeErr := file.Close(); closeErr != nil {
		return nil, errors.Join(closeErr, rootErr)
	}
	if rootErr != nil {
		return nil, fmt.Errorf("close native label database directory: %w", rootErr)
	}

	dsn := (&url.URL{Scheme: "file", Path: clean}).String() + "?" + nativeReadDBOptions
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open native label sqlite pool: %w", err)
	}
	db.SetMaxOpenConns(4)
	var queryOnly int
	if err := db.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil {
		return nil, closeNativeReadDB(db, fmt.Errorf("read native label query_only: %w", err))
	}
	if queryOnly != 1 {
		return nil, closeNativeReadDB(db, fmt.Errorf("native label query_only is %d, want 1", queryOnly))
	}
	return db, nil
}

func closeNativeReadDB(db *sql.DB, err error) error {
	if closeErr := db.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}
