package hub

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// This file holds admit_message: the admission step. It takes routing and
// metadata only — never message content — checks the sender and recipient
// preconditions, replays stored answers for retries, and otherwise runs
// one gate evaluation in one transaction. Denials persist their decision
// rows and commit; every other failure rolls back.

// timeLayout is the store's time format, equal to eventstore.TimeLayout
// so text order is time order. A test pins the equality.
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// AdmitHandler serves admit_message behind Admitter.
type AdmitHandler struct {
	deps Deps
}

// NewAdmitHandler returns the admission handler. Deps must be complete.
func NewAdmitHandler(deps Deps) (*AdmitHandler, error) {
	if !deps.valid() {
		return nil, fmt.Errorf("new admit handler: %w", errInvalidArgument)
	}
	return &AdmitHandler{deps: deps}, nil
}

// admitRecipient is one requested recipient.
type admitRecipient struct {
	AgentID      string `json:"agent_id"`
	DeliveryMode string `json:"delivery_mode"`
	RequiresAck  bool   `json:"requires_ack"`
}

// admitRequest is the admit_message params: routing and metadata only.
type admitRequest struct {
	ClientMsgID       string           `json:"client_msg_id"`
	Recipients        []admitRecipient `json:"recipients"`
	Priority          string           `json:"priority"`
	CreatedAt         string           `json:"created_at"`
	ExpiresAt         string           `json:"expires_at"`
	ThreadID          string           `json:"thread_id,omitempty"`
	ThreadRef         string           `json:"thread_ref,omitempty"`
	RequestedAutonomy string           `json:"requested_autonomy,omitempty"`
	RequestedBudget   json.RawMessage  `json:"requested_budget,omitempty"`
	ParentMessageID   string           `json:"parent_message_id,omitempty"`
}

// admitAnswer is the admit_message answer: the verdict with the header
// fields the Endpoint signs, and whether the message is already
// submitted.
type admitAnswer struct {
	AdmissionResult json.RawMessage `json:"admission_result"`
	MessageID       string          `json:"message_id"`
	ThreadRef       string          `json:"thread_ref"`
	CreatedAt       string          `json:"created_at"`
	ExpiresAt       string          `json:"expires_at"`
	Submitted       bool            `json:"submitted"`
}

// AdmitMessage implements Admitter.
func (h *AdmitHandler) AdmitMessage(
	ctx context.Context,
	agent protocol.AgentID,
	params json.RawMessage,
) (any, error) {
	var req admitRequest
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, invalidRequest()
	}
	if err := validAdmitRequest(req); err != nil {
		return nil, err
	}
	if req.ParentMessageID != "" {
		return nil, invalidRequest()
	}
	if _, err := h.deps.Registry.ActiveSigningKey(ctx, h.deps.DB, agent); err != nil {
		return nil, serverError(protocol.ErrorCodeKeyUnavailable)
	}
	for _, recipient := range req.Recipients {
		if err := h.checkRecipient(ctx, recipient); err != nil {
			return nil, err
		}
	}
	answer, err := h.admitOnce(ctx, agent, req)
	if err != nil && isUniqueViolation(err) {
		// A concurrent admission won the client_msg_id: answer from
		// its committed row instead of failing the loser.
		return h.admitReplay(ctx, agent, req)
	}
	return answer, err
}

// admitOnce runs one admission attempt in its own transaction.
func (h *AdmitHandler) admitOnce(
	ctx context.Context,
	agent protocol.AgentID,
	req admitRequest,
) (any, error) {
	tx, err := h.deps.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError()
	}
	defer func() { _ = tx.Rollback() }()
	answer, fresh, err := h.retryOrFresh(ctx, tx, agent, req)
	if err != nil {
		return nil, err
	}
	if !fresh {
		return answer, nil
	}
	return h.evaluate(ctx, tx, agent, req)
}

// admitReplay answers from the committed row after losing a uniqueness
// race. No row means the winner rolled back: fail closed.
func (h *AdmitHandler) admitReplay(
	ctx context.Context,
	agent protocol.AgentID,
	req admitRequest,
) (any, error) {
	tx, err := h.deps.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError()
	}
	defer func() { _ = tx.Rollback() }()
	answer, fresh, err := h.retryOrFresh(ctx, tx, agent, req)
	if err != nil {
		return nil, err
	}
	if fresh {
		return nil, internalError()
	}
	return answer, nil
}

// isUniqueViolation reports a lost client_msg_id race: the store insert
// hit the idempotency index.
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// validAdmitRequest checks the shape of an admission request: ids,
// enums, times in order, and well-formed optionals.
func validAdmitRequest(req admitRequest) error {
	if req.ClientMsgID == "" || len(req.Recipients) == 0 {
		return invalidRequest()
	}
	if !protocol.Priority(req.Priority).Valid() {
		return invalidRequest()
	}
	for _, recipient := range req.Recipients {
		if !protocol.AgentID(recipient.AgentID).Valid() {
			return invalidRequest()
		}
		if !protocol.DeliveryMode(recipient.DeliveryMode).Valid() {
			return invalidRequest()
		}
	}
	created, err := time.Parse(timeLayout, req.CreatedAt)
	if err != nil {
		return invalidRequest()
	}
	expires, err := time.Parse(timeLayout, req.ExpiresAt)
	if err != nil || !expires.After(created) {
		return invalidRequest()
	}
	if !protocol.ThreadID(req.ThreadID).Valid() {
		return invalidRequest()
	}
	if req.ThreadRef != "" && !protocol.ThreadRef(req.ThreadRef).Valid() {
		return invalidRequest()
	}
	if req.RequestedAutonomy != "" && !protocol.Autonomy(req.RequestedAutonomy).Valid() {
		return invalidRequest()
	}
	if req.RequestedBudget != nil && !json.Valid(req.RequestedBudget) {
		return invalidRequest()
	}
	return nil
}

// checkRecipient refuses undeliverable, foreign, and blocked recipients
// before the gate, without writing.
func (h *AdmitHandler) checkRecipient(ctx context.Context, recipient admitRecipient) error {
	node, err := agentNode(ctx, h.deps.DB, recipient.AgentID)
	if err != nil {
		return err
	}
	if node == "" {
		return serverError(protocol.ErrorCodeRecipientNotDeliverable)
	}
	if protocol.NodeID(node) != h.deps.LocalNode {
		return serverError(protocol.ErrorCodeNotFound)
	}
	blocked, err := h.deps.Registry.Blocked(ctx, h.deps.DB, protocol.AgentID(recipient.AgentID))
	if err != nil {
		return internalError()
	}
	if blocked {
		return serverError(protocol.ErrorCodeKeyUnavailable)
	}
	return nil
}

// agentNode returns the node an agent is registered on, or empty when the
// agent is unknown.
func agentNode(ctx context.Context, q registry.Querier, agent string) (string, error) {
	text, err := queryText("agent_node.sql")
	if err != nil {
		return "", err
	}
	var node sql.NullString
	if err := q.QueryRowContext(ctx, text, agent).Scan(&node); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("select agent node: %w", err)
	}
	return node.String, nil
}

// retryRow is one messages row for the retry comparison.
type retryRow struct {
	messageID     string
	priority      string
	expiresAt     string
	state         string
	controlReason string
	result        json.RawMessage
	createdAt     string
	threadRef     string
	recipients    []admitRecipient
}

// retryOrFresh answers from the stored row when the client_msg_id was
// seen, or reports the request fresh. A stored denial or expiry answers
// without the gate; differing parameters refuse.
func (h *AdmitHandler) retryOrFresh(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
	req admitRequest,
) (any, bool, error) {
	row, err := selectRetryRow(ctx, tx, h.deps.LocalNode, agent, req.ClientMsgID)
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, true, nil
	}
	if !retryParamsEqual(*row, req) {
		return nil, false, invalidRequest()
	}
	answer := admitAnswer{
		AdmissionResult: append(json.RawMessage(nil), row.result...),
		MessageID:       row.messageID,
		ThreadRef:       row.threadRef,
		CreatedAt:       row.createdAt,
		ExpiresAt:       row.expiresAt,
	}
	switch row.state {
	case "ADMITTED":
		return answer, false, nil
	case "FAILED":
		return nil, false, serverError(protocol.ErrorCode(row.controlReason))
	case "EXPIRED":
		return nil, false, serverError(protocol.ErrorCodeExpired)
	default:
		answer.Submitted = true
		return answer, false, nil
	}
}

// retryParamsEqual compares the stored parameters with the retry: the
// sender matches by lookup, and the comparison covers recipients in
// order with delivery_mode and requires_ack, priority, and expires_at.
func retryParamsEqual(row retryRow, req admitRequest) bool {
	if row.priority != req.Priority || row.expiresAt != req.ExpiresAt {
		return false
	}
	if len(row.recipients) != len(req.Recipients) {
		return false
	}
	for i := range row.recipients {
		stored, wanted := row.recipients[i], req.Recipients[i]
		if stored.AgentID != wanted.AgentID ||
			stored.DeliveryMode != wanted.DeliveryMode ||
			stored.RequiresAck != wanted.RequiresAck {
			return false
		}
	}
	return true
}

// evaluate runs one fresh gate evaluation and stores its rows.
func (h *AdmitHandler) evaluate(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
	req admitRequest,
) (any, error) {
	now := h.deps.Clock.Now().UTC()
	messageID, err := mintULID(now)
	if err != nil {
		return nil, internalError()
	}
	threadRef := req.ThreadRef
	if threadRef == "" {
		ref, err := mintULID(now)
		if err != nil {
			return nil, internalError()
		}
		threadRef = "thr_" + string(ref)
	}
	recipients := make([]channel.Recipient, len(req.Recipients))
	for i, r := range req.Recipients {
		recipients[i] = channel.Recipient{
			Agent:       protocol.AgentID(r.AgentID),
			Deliverable: true,
			Policy:      channel.PolicyAllowed,
		}
	}
	msg := channel.Message{
		ID:       messageID,
		From:     agent,
		FromNode: h.deps.LocalNode,
		To:       recipients,
		ToNode:   h.deps.LocalNode,
		ThreadID: protocol.ThreadID(req.ThreadID),
		HopCount: 0,
	}
	row := newMessageRow(messageID, threadRef, h.deps.LocalNode, agent, req)
	_, result, err := h.deps.Engine.Admit(ctx, tx, msg,
		func(ctx context.Context, result protocol.AdmissionResult) error {
			return row.store(ctx, tx, result)
		})
	if err != nil {
		var denied *channel.DenyError
		if errors.As(err, &denied) {
			if err := tx.Commit(); err != nil {
				return nil, internalError()
			}
			return nil, serverError(denied.Code())
		}
		return nil, mapGateError(err)
	}
	if result.Result == protocol.AdmissionStatusApprovalRequired {
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, internalError()
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError()
	}
	return row.answer(result, false), nil
}

// mapGateError maps unexpected gate failures to wire errors without
// content. Replies are refused before the gate, so ErrUnsupported cannot
// escape with data.
func mapGateError(err error) error {
	if errors.Is(err, channel.ErrUnsupported) {
		return invalidRequest()
	}
	return internalError()
}

// newMessageRow builds the stored row for one fresh admission.
func newMessageRow(
	id protocol.ULID,
	threadRef string,
	node protocol.NodeID,
	agent protocol.AgentID,
	req admitRequest,
) *messageRow {
	return &messageRow{
		messageID:   string(id),
		clientMsgID: req.ClientMsgID,
		node:        string(node),
		agent:       string(agent),
		threadRef:   threadRef,
		threadID:    req.ThreadID,
		priority:    req.Priority,
		autonomy:    req.RequestedAutonomy,
		budget:      req.RequestedBudget,
		createdAt:   req.CreatedAt,
		expiresAt:   req.ExpiresAt,
		recipients:  req.Recipients,
	}
}

// messageRow is one messages row with its recipients, before storing.
type messageRow struct {
	messageID   string
	clientMsgID string
	node        string
	agent       string
	threadRef   string
	threadID    string
	priority    string
	autonomy    string
	budget      json.RawMessage
	createdAt   string
	expiresAt   string
	recipients  []admitRecipient
}

// routeDigest binds the route: the message, its origin, and its
// recipients in order.
func routeDigest(messageID, node, agent string, recipients []admitRecipient) string {
	names := make([]string, len(recipients))
	for i, r := range recipients {
		names[i] = r.AgentID
	}
	sort.Strings(names)
	sum := sha256.Sum256([]byte(messageID + "|" + node + "|" + agent + "|" + strings.Join(names, ",")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// store inserts the messages row and its recipients. Control columns keep
// their defaults: the gate owns the triples.
func (r *messageRow) store(ctx context.Context, tx *sql.Tx, result protocol.AdmissionResult) error {
	verdict, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode admission result: %w", err)
	}
	text, err := queryText("message_insert_admitted.sql")
	if err != nil {
		return err
	}
	var budget any
	if r.budget != nil {
		budget = string(r.budget)
	}
	if _, err := tx.ExecContext(ctx, text,
		r.messageID, r.clientMsgID, r.node, r.agent, r.node,
		r.threadRef, nullIfEmpty(r.threadID), r.priority,
		nullIfEmpty(r.autonomy), budget, r.createdAt, r.expiresAt, r.createdAt,
		routeDigest(r.messageID, r.node, r.agent, r.recipients), string(verdict),
	); err != nil {
		return fmt.Errorf("insert admitted message: %w", err)
	}
	text, err = queryText("recipients_insert.sql")
	if err != nil {
		return err
	}
	for i, recipient := range r.recipients {
		if _, err := tx.ExecContext(ctx, text,
			r.messageID, recipient.AgentID, i+1, recipient.DeliveryMode,
			boolToInt(recipient.RequiresAck), r.createdAt,
		); err != nil {
			return fmt.Errorf("insert message recipient: %w", err)
		}
	}
	return nil
}

// answer renders the admission answer with the header fields to sign.
func (r *messageRow) answer(result protocol.AdmissionResult, submitted bool) admitAnswer {
	verdict, err := json.Marshal(result)
	if err != nil {
		verdict = json.RawMessage("{}")
	}
	return admitAnswer{
		AdmissionResult: verdict,
		MessageID:       r.messageID,
		ThreadRef:       r.threadRef,
		CreatedAt:       r.createdAt,
		ExpiresAt:       r.expiresAt,
		Submitted:       submitted,
	}
}

// selectRetryRow reads the stored message and recipients for one retry.
func selectRetryRow(
	ctx context.Context,
	tx *sql.Tx,
	node protocol.NodeID,
	agent protocol.AgentID,
	clientMsgID string,
) (*retryRow, error) {
	text, err := queryText("message_select_retry.sql")
	if err != nil {
		return nil, err
	}
	var row retryRow
	var result []byte
	var controlReason sql.NullString
	err = tx.QueryRowContext(ctx, text, string(node), string(agent), clientMsgID).Scan(
		&row.messageID, &row.priority, &row.expiresAt, &row.state,
		&controlReason, &result, &row.createdAt, &row.threadRef,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select retry message: %w", err)
	}
	row.controlReason = controlReason.String
	row.result = append(json.RawMessage(nil), result...)
	text, err = queryText("recipients_select.sql")
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, text, row.messageID)
	if err != nil {
		return nil, fmt.Errorf("select retry recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var recipient admitRecipient
		var requiresAck int
		if err := rows.Scan(&recipient.AgentID, &recipient.DeliveryMode, &requiresAck); err != nil {
			return nil, fmt.Errorf("scan retry recipient: %w", err)
		}
		recipient.RequiresAck = requiresAck != 0
		row.recipients = append(row.recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read retry recipients: %w", err)
	}
	return &row, nil
}

// nullIfEmpty maps an empty string to NULL.
func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// boolToInt stores a bool as 0 or 1.
func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
