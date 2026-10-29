package hub

import (
	"context"
	"database/sql"
	"fmt"
)

// This file holds the expiry sweep T1: admitted messages past their
// expiry become expired in one transaction, reporting how many moved.
// Who runs the sweep on a timer belongs to the daemon halves, not here.

// SweepExpired moves admitted messages at or past their expiry to
// expired, and returns the count. Now comes from the caller's clock so
// tests drive time.
func SweepExpired(ctx context.Context, db *sql.DB, now string) (int64, error) {
	text, err := queryText("message_expire_sweep.sql")
	if err != nil {
		return 0, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin expiry sweep: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, text, now, now)
	if err != nil {
		return 0, fmt.Errorf("sweep expired messages: %w", err)
	}
	moved, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count swept messages: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit expiry sweep: %w", err)
	}
	return moved, nil
}
