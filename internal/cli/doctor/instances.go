package doctor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"

	// Register the SQLite driver of the read-only report.
	_ "modernc.org/sqlite"

	"github.com/smellak/gifford/internal/tmuxinstance"
)

const (
	databaseName  = "gifford.db"
	readDBOptions = "mode=ro&_pragma=busy_timeout(5000)&_pragma=query_only(1)"
)

// reportInstances reports what the registry holds for each statement of
// the instances file, from the node's database opened read-only. A missing
// file only means the node has no agents in tmux.
func reportInstances(ctx context.Context, rep *reportWriter, path, dataDir string) bool {
	statements, err := tmuxinstance.LoadStatements(path)
	if errors.Is(err, fs.ErrNotExist) {
		rep.printf("%-11s %-4s no %s\n", "INSTANCES", "SKIP", path)
		return true
	}
	if err != nil {
		rep.printf("%-11s %-4s %s\n", "INSTANCES", "FAIL", err)
		return false
	}
	noun := "statements"
	if len(statements) == 1 {
		noun = "statement"
	}
	rep.printf("%-11s %-4s %d %s (%s)\n", "INSTANCES", "OK", len(statements), noun, path)
	if len(statements) == 0 {
		return true
	}
	db, err := openReadDB(ctx, filepath.Join(dataDir, databaseName))
	if errors.Is(err, fs.ErrNotExist) {
		rep.printf("%-11s %-4s no node database yet: no statement is reconciled\n", "INSTANCES", "WAIT")
		return true
	}
	if err != nil {
		rep.printf("%-11s %-4s %s\n", "INSTANCES", "FAIL", err)
		return false
	}
	defer func() { _ = db.Close() }()
	reports, err := tmuxinstance.ReportStatements(ctx, db, statements)
	if err != nil {
		rep.printf("%-11s %-4s %s\n", "INSTANCES", "FAIL", err)
		return false
	}
	ok := true
	for _, r := range reports {
		switch r.Status {
		case tmuxinstance.StatusActive:
			rep.printf("%-11s %-4s %s on pane %s since %s\n", "INSTANCE", "OK", r.AgentID, r.PaneID, r.Since)
		case tmuxinstance.StatusPending:
			rep.printf("%-11s %-4s %s: no pass has reconciled it yet\n", "INSTANCE", "WAIT", r.AgentID)
		case tmuxinstance.StatusRefused:
			ok = false
			rep.printf("%-11s %-4s %s refused: %s since %s\n", "INSTANCE", "FAIL", r.AgentID, r.Reason, r.Since)
		case tmuxinstance.StatusInvalidated:
			ok = false
			rep.printf("%-11s %-4s %s invalidated: %s since %s; raise its generation to register it again\n",
				"INSTANCE", "FAIL", r.AgentID, r.Reason, r.Since)
		}
	}
	return ok
}

// openReadDB opens the node's database without creating, migrating or
// writing it: SQLite opens it read-only, and every connection is query_only.
func openReadDB(ctx context.Context, path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("node database: %w", err)
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.Clean(path)}).String() + "?" + readDBOptions
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open node database: %w", err)
	}
	var queryOnly int
	if err := db.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly); err != nil {
		return nil, errors.Join(fmt.Errorf("read node database query_only: %w", err), db.Close())
	}
	if queryOnly != 1 {
		return nil, errors.Join(fmt.Errorf("node database query_only is %d, want 1", queryOnly), db.Close())
	}
	return db, nil
}
