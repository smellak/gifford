package tmuxinstance

import (
	"context"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// RefusalCode says why a registration failed. It names no user, path or
// process, so the node may store it and doctor may show it.
type RefusalCode string

// The codes Register gives its errors. The migrations of
// tmux_instance_refusals accept exactly these.
const (
	// RefusalInvalid: the registration is malformed.
	RefusalInvalid RefusalCode = "invalid"
	// RefusalShell: the stated pane process is a shell.
	RefusalShell RefusalCode = "shell"
	// RefusalExcluded: the agent is excluded from the registry.
	RefusalExcluded RefusalCode = "excluded"
	// RefusalUnknownUser: the owner or the agent user does not exist.
	RefusalUnknownUser RefusalCode = "unknown_user"
	// RefusalRootUser: the owner or the agent user has uid 0.
	RefusalRootUser RefusalCode = "root_user"
	// RefusalReservedUID: the agent user is the node's or the bridge's.
	RefusalReservedUID RefusalCode = "reserved_uid"
	// RefusalSocketDir: the socket's directory is missing, not the
	// owner's, a symlink or not 0700.
	RefusalSocketDir RefusalCode = "socket_dir"
	// RefusalTmuxUnreachable: the node could not list the socket's panes.
	RefusalTmuxUnreachable RefusalCode = "tmux_unreachable"
	// RefusalPaneMissing: the pane is not on the socket, or its process is
	// gone.
	RefusalPaneMissing RefusalCode = "pane_missing"
	// RefusalPaneComm: the pane runs another program than stated.
	RefusalPaneComm RefusalCode = "pane_comm"
	// RefusalHarnessMissing: the pane has not exactly one CLI process.
	RefusalHarnessMissing RefusalCode = "harness_missing"
	// RefusalHarnessProgram: the CLI's argv[0], or its interpreter's
	// program, is not the stated one.
	RefusalHarnessProgram RefusalCode = "harness_program"
	// RefusalAgentUID: the CLI runs as another user than stated.
	RefusalAgentUID RefusalCode = "agent_uid"
	// RefusalAlreadyActive: the agent or the pane has an ACTIVE instance.
	RefusalAlreadyActive RefusalCode = "already_active"
	// RefusalInternal: the node could not check the registration.
	RefusalInternal RefusalCode = "internal"
)

// codedError is a Register error with its code.
type codedError struct {
	code RefusalCode
	err  error
}

func (e *codedError) Error() string { return e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }

func coded(code RefusalCode, err error) error { return &codedError{code: code, err: err} }

// RefusalOf returns the code of an error from Register.
func RefusalOf(err error) RefusalCode {
	var c *codedError
	if errors.As(err, &c) {
		return c.code
	}
	if errors.Is(err, ErrInvalidArgument) {
		return RefusalInvalid
	}
	return RefusalInternal
}

// Refusal is a statement that Reconcile could not register.
type Refusal struct {
	AgentID protocol.AgentID
	Code    RefusalCode
}

type refusalRow struct {
	source string
	code   RefusalCode
}

func (r *Registry) refusals(ctx context.Context) (map[protocol.AgentID]refusalRow, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT agent_id, source, code FROM tmux_instance_refusals`)
	if err != nil {
		return nil, fmt.Errorf("select tmux instance refusals: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[protocol.AgentID]refusalRow)
	for rows.Next() {
		var agent protocol.AgentID
		var row refusalRow
		if err := rows.Scan(&agent, &row.source, &row.code); err != nil {
			return nil, fmt.Errorf("scan tmux instance refusal: %w", err)
		}
		out[agent] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read tmux instance refusals: %w", err)
	}
	return out, nil
}

func (r *Registry) recordRefusal(ctx context.Context, reg Registration, code RefusalCode) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO tmux_instance_refusals (agent_id, source, code, refused_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (agent_id) DO UPDATE SET
			source = excluded.source, code = excluded.code, refused_at = excluded.refused_at`,
		reg.AgentID, reg.Source, code, r.clock.Now().UTC().Format(timeLayout))
	if err != nil {
		return fmt.Errorf("record tmux instance refusal of %s: %w", reg.AgentID, err)
	}
	return nil
}

func (r *Registry) clearRefusal(ctx context.Context, agent protocol.AgentID) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM tmux_instance_refusals WHERE agent_id = ?`, agent); err != nil {
		return fmt.Errorf("clear tmux instance refusal of %s: %w", agent, err)
	}
	return nil
}
