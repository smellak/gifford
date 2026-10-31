package channel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

type approvalScanner interface {
	Scan(...any) error
}

// EnsureOrchestratorApproval returns the pending request for one message
// recipient, creating it in the caller's transaction when needed. Channel is
// the only package that writes approval_requests.
func (c *Controller) EnsureOrchestratorApproval(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agent protocol.AgentID,
	note string,
) (protocol.ApprovalRequest, bool, error) {
	if tx == nil || !messageID.Valid() || !agent.Valid() || utf8.RuneCountInString(note) > 500 {
		return protocol.ApprovalRequest{}, false, fmt.Errorf("orchestrator approval: %w", ErrInvalidQuery)
	}
	existing, err := selectPendingOrchestratorApproval(ctx, tx, messageID, agent)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return protocol.ApprovalRequest{}, false, err
	}
	request, err := c.CreateOrchestratorApproval(ctx, tx, messageID, agent, note)
	if err != nil {
		return protocol.ApprovalRequest{}, false, err
	}
	return request, true, nil
}

// CreateOrchestratorApproval creates a fresh request. Unlike Ensure, it never
// reuses a pending request; uncertain delivery episodes rely on that property.
func (c *Controller) CreateOrchestratorApproval(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agent protocol.AgentID,
	note string,
) (protocol.ApprovalRequest, error) {
	if tx == nil || !messageID.Valid() || !agent.Valid() || utf8.RuneCountInString(note) > 500 {
		return protocol.ApprovalRequest{}, fmt.Errorf("orchestrator approval: %w", ErrInvalidQuery)
	}
	now := c.clock.Now().UTC()
	id, err := c.mintApprovalID(now)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("mint approval id: %w", err)
	}
	request := protocol.ApprovalRequest{
		ApprovalID: id,
		Source:     protocol.ApprovalSourceOrchestrator, ObjectType: protocol.ApprovalObjectTypeMessage,
		ObjectID: string(messageID), AgentID: agent,
		Options:     []protocol.ApprovalOption{"deliver_now", "reject"},
		RequestNote: note, State: protocol.ApprovalStatePending, CreatedAt: now,
	}
	if err := request.Validate(); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("build orchestrator approval: %w", err)
	}
	options, err := json.Marshal(request.Options)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("encode approval options: %w", err)
	}
	query, err := queryText("approvals_insert.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	_, err = tx.ExecContext(ctx, query,
		request.ApprovalID, request.Source, request.ObjectType, request.ObjectID,
		request.AgentID, nil, string(options), nil, nullString(request.RequestNote),
		request.State, nil, nil, nil, formatChannelTime(request.CreatedAt), nil)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("insert orchestrator approval: %w", err)
	}
	return request, nil
}

// SupersedeOrchestratorApproval closes the pending orchestrator request for
// one exact message recipient. It neither creates a replacement nor emits an
// event; the caller decides what other work belongs in the same transaction.
func (c *Controller) SupersedeOrchestratorApproval(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agent protocol.AgentID,
) (bool, error) {
	if tx == nil || !messageID.Valid() || !agent.Valid() {
		return false, fmt.Errorf("supersede orchestrator approval: %w", ErrInvalidQuery)
	}
	query, err := queryText("approvals_supersede_pending_orchestrator.sql")
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, query, formatChannelTime(c.clock.Now().UTC()), messageID, agent)
	if err != nil {
		return false, fmt.Errorf("supersede orchestrator approval: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("supersede orchestrator approval rows affected: %w", err)
	}
	switch changed {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("supersede orchestrator approval changed %d rows: %w", changed, ErrInvalidQuery)
	}
}

// ReplaceOrchestratorApproval supersedes any pending request for the exact
// message recipient and creates a fresh request in the same transaction.
// Recovery episodes use this instead of reusing an ordinary authorization;
// another recipient's request is never touched.
func (c *Controller) ReplaceOrchestratorApproval(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agent protocol.AgentID,
	note string,
) (protocol.ApprovalRequest, error) {
	if tx == nil || !messageID.Valid() || !agent.Valid() || utf8.RuneCountInString(note) > 500 {
		return protocol.ApprovalRequest{}, fmt.Errorf("replace orchestrator approval: %w", ErrInvalidQuery)
	}
	if _, err := c.SupersedeOrchestratorApproval(ctx, tx, messageID, agent); err != nil {
		return protocol.ApprovalRequest{}, err
	}
	request, err := c.CreateOrchestratorApproval(ctx, tx, messageID, agent, note)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	return request, nil
}

// Approvals returns requests matching the optional protocol filters.
func (c *Controller) Approvals(
	ctx context.Context,
	params protocol.ListApprovalsParams,
) (protocol.ApprovalList, error) {
	if err := params.Validate(); err != nil {
		return protocol.ApprovalList{}, fmt.Errorf("list approvals: %w", ErrInvalidQuery)
	}
	query, err := queryText("approvals_select_all.sql")
	if err != nil {
		return protocol.ApprovalList{}, err
	}
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return protocol.ApprovalList{}, fmt.Errorf("select approvals: %w", err)
	}
	defer func() { _ = rows.Close() }()
	list := protocol.ApprovalList{Approvals: []protocol.ApprovalRequest{}}
	for rows.Next() {
		request, err := scanApproval(rows)
		if err != nil {
			return protocol.ApprovalList{}, err
		}
		if params.State != "" && request.State != params.State ||
			params.AgentID != "" && request.AgentID != params.AgentID ||
			params.LoopID != "" && request.LoopID != params.LoopID {
			continue
		}
		list.Approvals = append(list.Approvals, request)
	}
	if err := rows.Err(); err != nil {
		return protocol.ApprovalList{}, fmt.Errorf("read approvals: %w", err)
	}
	return list, nil
}

// Approval returns one request.
func (c *Controller) Approval(
	ctx context.Context,
	id protocol.ApprovalID,
) (protocol.ApprovalRequest, error) {
	if !id.Valid() {
		return protocol.ApprovalRequest{}, fmt.Errorf("get approval: %w", ErrInvalidQuery)
	}
	query, err := queryText("approvals_select_by_id.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	request, err := scanApproval(c.db.QueryRowContext(ctx, query, id))
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.ApprovalRequest{}, ErrNotFound
	}
	return request, err
}

// ApprovalTx returns one request through a caller-owned transaction.
func (c *Controller) ApprovalTx(
	ctx context.Context,
	tx *sql.Tx,
	id protocol.ApprovalID,
) (protocol.ApprovalRequest, error) {
	if tx == nil || !id.Valid() {
		return protocol.ApprovalRequest{}, fmt.Errorf("get approval: %w", ErrInvalidQuery)
	}
	query, err := queryText("approvals_select_by_id.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	request, err := scanApproval(tx.QueryRowContext(ctx, query, id))
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.ApprovalRequest{}, ErrNotFound
	}
	return request, err
}

// ResolveOrchestratorApproval records one human choice in the caller's
// transaction. The caller may atomically link an UNCERTAIN attempt to this
// resolution before committing.
func (c *Controller) ResolveOrchestratorApproval(
	ctx context.Context,
	tx *sql.Tx,
	params protocol.ResolveApprovalParams,
	actor Actor,
) (protocol.ApprovalRequest, error) {
	if tx == nil || actor.Type != protocol.ActorTypeHuman || actor.ID == "" ||
		utf8.RuneCountInString(actor.ID) > 128 {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval: %w", ErrInvalidQuery)
	}
	if err := params.Validate(); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval: %w", ErrInvalidQuery)
	}
	request, err := c.ApprovalTx(ctx, tx, params.ApprovalID)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	if request.Source != protocol.ApprovalSourceOrchestrator || !slices.Contains(request.Options, params.Option) {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval: %w", ErrInvalidQuery)
	}
	if request.State != protocol.ApprovalStatePending {
		if (request.State == protocol.ApprovalStateApproved || request.State == protocol.ApprovalStateRejected) &&
			request.Resolution != nil && request.Resolution.Option == params.Option &&
			request.Resolution.Reason == params.Reason && request.ResolvedBy == actor.ID {
			return request, nil
		}
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval replay: %w", ErrInvalidQuery)
	}
	if !request.ExpiresAt.IsZero() && !request.ExpiresAt.After(c.clock.Now().UTC()) {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve expired approval: %w", ErrInvalidQuery)
	}
	state := protocol.ApprovalStateApproved
	if params.Option == "reject" {
		state = protocol.ApprovalStateRejected
	} else if params.Option != "deliver_now" {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval option: %w", ErrInvalidQuery)
	}
	now := c.clock.Now().UTC()
	resolution := &protocol.ApprovalResolution{Option: params.Option, Reason: params.Reason}
	raw, err := json.Marshal(resolution)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("encode approval resolution: %w", err)
	}
	query, err := queryText("approvals_resolve.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	result, err := tx.ExecContext(ctx, query, state, string(raw), actor.ID,
		formatChannelTime(now), request.ApprovalID)
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve orchestrator approval: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve orchestrator approval rows affected: %w", err)
	}
	if changed != 1 {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolve approval changed concurrently: %w", ErrInvalidQuery)
	}
	request.State = state
	request.Resolution = resolution
	request.ResolvedBy = actor.ID
	request.ResolvedAt = now
	if err := request.Validate(); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("resolved approval: %w", err)
	}
	return request, nil
}

func selectPendingOrchestratorApproval(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	agent protocol.AgentID,
) (protocol.ApprovalRequest, error) {
	query, err := queryText("approvals_select_pending_orchestrator.sql")
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	request, err := scanApproval(tx.QueryRowContext(ctx, query, messageID, agent))
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.ApprovalRequest{}, ErrNotFound
	}
	return request, err
}

func scanApproval(row approvalScanner) (protocol.ApprovalRequest, error) {
	var request protocol.ApprovalRequest
	var options string
	var agent, loop, requestedBy, note, resolution, resolvedBy sql.NullString
	var expiresAt, createdAt, resolvedAt sql.NullString
	if err := row.Scan(
		&request.ApprovalID, &request.Source, &request.ObjectType, &request.ObjectID,
		&agent, &loop, &options, &requestedBy, &note, &request.State, &resolution,
		&resolvedBy, &expiresAt, &createdAt, &resolvedAt,
	); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("scan approval: %w", err)
	}
	request.AgentID = protocol.AgentID(agent.String)
	request.LoopID = protocol.LoopID(loop.String)
	request.RequestedBy = protocol.AgentID(requestedBy.String)
	request.RequestNote = note.String
	request.ResolvedBy = resolvedBy.String
	if err := json.Unmarshal([]byte(options), &request.Options); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("decode approval options: %w", err)
	}
	if resolution.Valid {
		request.Resolution = &protocol.ApprovalResolution{}
		if err := json.Unmarshal([]byte(resolution.String), request.Resolution); err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("decode approval resolution: %w", err)
		}
	}
	var err error
	if createdAt.Valid {
		request.CreatedAt, err = parseChannelTime(createdAt.String)
		if err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("approval created_at: %w", err)
		}
	}
	if expiresAt.Valid {
		request.ExpiresAt, err = parseChannelTime(expiresAt.String)
		if err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("approval expires_at: %w", err)
		}
	}
	if resolvedAt.Valid {
		request.ResolvedAt, err = parseChannelTime(resolvedAt.String)
		if err != nil {
			return protocol.ApprovalRequest{}, fmt.Errorf("approval resolved_at: %w", err)
		}
	}
	if err := request.Validate(); err != nil {
		return protocol.ApprovalRequest{}, fmt.Errorf("stored approval: %w", err)
	}
	return request, nil
}
