package tmuxinstance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// StatementStatus is what the registry holds for one statement.
type StatementStatus string

const (
	// StatusActive means the statement has an ACTIVE instance.
	StatusActive StatementStatus = "active"
	// StatusInvalidated means its instance was invalidated, and only a
	// raised generation registers it again.
	StatusInvalidated StatementStatus = "invalidated"
	// StatusRefused means its last registration was refused.
	StatusRefused StatementStatus = "refused"
	// StatusPending means no pass has reconciled it yet.
	StatusPending StatementStatus = "pending"
)

// Report is one statement's state in the registry.
type Report struct {
	AgentID protocol.AgentID
	Status  StatementStatus
	// PaneID is the stated pane.
	PaneID protocol.TmuxPaneID
	// Reason is the invalidation reason, or the refusal code; empty
	// otherwise.
	Reason string
	// Since is when the instance was created, invalidated or refused.
	Since string
}

// ReportStatements reads what the registry holds for each statement, in
// their order. It only reads, so db may be a query_only connection.
func ReportStatements(ctx context.Context, db *sql.DB, statements []Registration) ([]Report, error) {
	out := make([]Report, 0, len(statements))
	for _, reg := range statements {
		rep := Report{AgentID: reg.AgentID, Status: StatusPending, PaneID: reg.PaneID}
		var state State
		var reason sql.NullString
		var created, updated string
		err := db.QueryRowContext(ctx, `SELECT state, invalid_reason, created_at, updated_at
			FROM tmux_instances WHERE agent_id = ? AND source = ?
			ORDER BY created_at DESC, instance_id DESC LIMIT 1`, reg.AgentID, reg.Source).
			Scan(&state, &reason, &created, &updated)
		switch {
		case err == nil && state == StateActive:
			rep.Status, rep.Since = StatusActive, created
		case err == nil:
			rep.Status, rep.Reason, rep.Since = StatusInvalidated, reason.String, updated
		case !errors.Is(err, sql.ErrNoRows):
			return nil, fmt.Errorf("read tmux instance of %s: %w", reg.AgentID, err)
		default:
			var code, refused string
			err := db.QueryRowContext(ctx, `SELECT code, refused_at FROM tmux_instance_refusals
				WHERE agent_id = ? AND source = ?`, reg.AgentID, reg.Source).Scan(&code, &refused)
			if err == nil {
				rep.Status, rep.Reason, rep.Since = StatusRefused, code, refused
			} else if !errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("read tmux instance refusal of %s: %w", reg.AgentID, err)
			}
		}
		out = append(out, rep)
	}
	return out, nil
}
