package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	processBindingStateBound   = "BOUND"
	processBindingStateBlocked = "BLOCKED"
)

type processBinding struct {
	state  string
	agent  protocol.AgentID
	bootID string
	parent ProcessToken
	child  ProcessToken
}

// processGuard binds the one direct harness child on first use and then
// revalidates that exact parent, child and system boot on every use. An
// unreadable identity is recorded as BLOCKED so neither a socket reconnect nor
// a new Executor in the same node process can learn a replacement child.
func (e *Executor) processGuard(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
) (bool, string, error) {
	if !record.sessionID.Valid() {
		return false, "session process identity is unavailable", nil
	}
	binding, found, err := loadProcessBinding(ctx, tx, record.sessionID)
	if err != nil {
		return false, "", err
	}
	if found && binding.state == processBindingStateBlocked {
		return false, "session process binding is permanently blocked", nil
	}

	tree, inspectErr := e.processes.Inspect(record.process)
	if inspectErr != nil {
		if err := e.blockProcessBinding(ctx, tx, record, "OBSERVATION_FAILED"); err != nil {
			return false, "", err
		}
		return false, truncate(inspectErr.Error(), 200), nil
	}
	parent := ProcessToken{PID: record.process.PID, StartTime: record.process.ProcessStartTime}
	if tree.Root != parent {
		if err := e.blockProcessBinding(ctx, tx, record, "PARENT_CHANGED"); err != nil {
			return false, "", err
		}
		return false, "session process identity changed", nil
	}
	tree = e.withoutRegisteredEndpoint(record, tree)

	if found {
		ok, detail, err := e.revalidateStoredProcessBinding(ctx, tx, binding, record, tree, parent)
		if err != nil || !ok {
			return false, detail, err
		}
	} else {
		if len(tree.DirectChildren) != 1 {
			return false, fmt.Sprintf("expected one direct harness child, found %d", len(tree.DirectChildren)), nil
		}
		binding = processBinding{
			state: processBindingStateBound, agent: record.AgentID, bootID: tree.BootID,
			parent: parent, child: tree.DirectChildren[0],
		}
		if err := e.insertProcessBinding(ctx, tx, record.sessionID, binding); err != nil {
			return false, "", err
		}
	}

	if len(tree.Descendants) != 1 || tree.Descendants[0] != binding.child {
		return false, "untrusted harness descendant present", nil
	}
	return true, "", nil
}

// withoutRegisteredEndpoint removes only the exact live endpoint token for
// this agent session. A recycled PID, another session, a child of the endpoint,
// or any ambiguous observation stays in the tree and therefore defers.
func (e *Executor) withoutRegisteredEndpoint(
	record deliveryRecord,
	tree ProcessTree,
) ProcessTree {
	if e.endpoints == nil {
		return tree
	}
	pid, start, ok := e.endpoints.RegisteredEndpointProcess(record.AgentID, record.sessionID)
	if !ok || pid <= 0 || start < 0 {
		return tree
	}
	target := ProcessToken{PID: pid, StartTime: start}
	descendants, descendantMatches := withoutProcessToken(tree.Descendants, target)
	if descendantMatches != 1 {
		return tree
	}
	direct, directMatches := withoutProcessToken(tree.DirectChildren, target)
	if directMatches > 1 {
		return tree
	}
	tree.DirectChildren = direct
	tree.Descendants = descendants
	return tree
}

func withoutProcessToken(tokens []ProcessToken, target ProcessToken) ([]ProcessToken, int) {
	filtered := make([]ProcessToken, 0, len(tokens))
	matches := 0
	for _, token := range tokens {
		if token == target {
			matches++
			continue
		}
		filtered = append(filtered, token)
	}
	return filtered, matches
}

func (e *Executor) revalidateStoredProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	binding processBinding,
	record deliveryRecord,
	tree ProcessTree,
	parent ProcessToken,
) (bool, string, error) {
	ok, detail, blockReason := validStoredBinding(binding, record, tree, parent)
	if ok {
		return true, "", nil
	}
	if err := e.blockProcessBinding(ctx, tx, record, blockReason); err != nil {
		return false, "", err
	}
	return false, detail, nil
}

func validStoredBinding(
	binding processBinding,
	record deliveryRecord,
	tree ProcessTree,
	parent ProcessToken,
) (bool, string, string) {
	switch {
	case binding.agent != record.AgentID:
		return false, "session agent identity changed", "AGENT_CHANGED"
	case binding.bootID != tree.BootID:
		return false, "system boot identity changed", "BOOT_CHANGED"
	case binding.parent != parent:
		return false, "session process identity changed", "PARENT_CHANGED"
	}
	for _, child := range tree.DirectChildren {
		if child == binding.child {
			return true, "", ""
		}
	}
	return false, "bound harness process identity changed", "CHILD_CHANGED"
}

func loadProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	session protocol.OpaqueID,
) (processBinding, bool, error) {
	var binding processBinding
	var bootID, blockReason sql.NullString
	var parentPID, parentStart, childPID, childStart sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT state, agent_id, boot_id,
		parent_pid, parent_start_time, child_pid, child_start_time, block_reason
		FROM orchestrator_process_bindings WHERE session_id = ?`, session).Scan(
		&binding.state, &binding.agent, &bootID,
		&parentPID, &parentStart, &childPID, &childStart, &blockReason)
	if errors.Is(err, sql.ErrNoRows) {
		return processBinding{}, false, nil
	}
	if err != nil {
		return processBinding{}, false, fmt.Errorf("load process binding: %w", err)
	}
	binding.bootID = bootID.String
	binding.parent = ProcessToken{PID: parentPID.Int64, StartTime: parentStart.Int64}
	binding.child = ProcessToken{PID: childPID.Int64, StartTime: childStart.Int64}
	return binding, true, nil
}

func (e *Executor) insertProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	session protocol.OpaqueID,
	binding processBinding,
) error {
	stamp := eventstore.FormatTime(e.clock.Now().UTC())
	_, err := tx.ExecContext(ctx, `INSERT INTO orchestrator_process_bindings (
		session_id, agent_id, state, boot_id, parent_pid, parent_start_time,
		child_pid, child_start_time, block_reason, created_at, updated_at
	) VALUES (?, ?, 'BOUND', ?, ?, ?, ?, ?, NULL, ?, ?)`,
		session, binding.agent, binding.bootID,
		binding.parent.PID, binding.parent.StartTime,
		binding.child.PID, binding.child.StartTime, stamp, stamp)
	if err != nil {
		return fmt.Errorf("bind harness process: %w", err)
	}
	return nil
}

func (e *Executor) blockProcessBinding(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	reason string,
) error {
	stamp := eventstore.FormatTime(e.clock.Now().UTC())
	_, err := tx.ExecContext(ctx, `INSERT INTO orchestrator_process_bindings (
		session_id, agent_id, state, block_reason, created_at, updated_at
	) VALUES (?, ?, 'BLOCKED', ?, ?, ?)
	ON CONFLICT(session_id) DO UPDATE SET
		state = 'BLOCKED', block_reason = excluded.block_reason,
		updated_at = excluded.updated_at`,
		record.sessionID, record.AgentID, reason, stamp, stamp)
	if err != nil {
		return fmt.Errorf("block harness process binding: %w", err)
	}
	return nil
}
