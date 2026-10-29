package hub

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/protocol"
)

// This file holds submit_message_envelope: the acceptance step. It binds
// one WireEnvelope to its admission — ids, sender, and recipients must
// match — checks the sender key and the ciphertext hash, and otherwise
// runs one acceptance evaluation in one transaction. The raw signature
// bytes await the frozen canonical scheme, so the check the Hub can run
// is key authority: the named key is the sender's active one. The
// envelope content itself is never inspected beyond its hash.

// SubmitHandler serves submit_message_envelope behind EnvelopeSubmitter.
type SubmitHandler struct {
	deps Deps
}

// NewSubmitHandler returns the submit handler. Deps must be complete.
func NewSubmitHandler(deps Deps) (*SubmitHandler, error) {
	if !deps.valid() {
		return nil, fmt.Errorf("new submit handler: %w", errInvalidArgument)
	}
	return &SubmitHandler{deps: deps}, nil
}

// submitParams is the submit_message_envelope params: exactly one
// envelope.
type submitParams struct {
	Envelope json.RawMessage `json:"envelope"`
}

// submitAnswer is the submit_message_envelope answer for an accepted
// envelope.
type submitAnswer struct {
	MessageID     string `json:"message_id"`
	State         string `json:"state"`
	ControlState  string `json:"control_state"`
	ControlReason string `json:"control_reason"`
}

// SubmitMessage implements EnvelopeSubmitter.
func (h *SubmitHandler) SubmitMessage(
	ctx context.Context,
	agent protocol.AgentID,
	params json.RawMessage,
) (any, error) {
	var req submitParams
	if err := json.Unmarshal(params, &req); err != nil {
		return nil, invalidRequest()
	}
	var env protocol.WireEnvelope
	if err := json.Unmarshal(req.Envelope, &env); err != nil {
		return nil, invalidRequest()
	}
	if err := env.Validate(); err != nil {
		return nil, invalidRequest()
	}
	if env.OriginAgent != agent {
		return nil, invalidRequest()
	}
	key, err := h.deps.Registry.ActiveSigningKey(ctx, h.deps.DB, agent)
	if err != nil {
		return nil, serverError(protocol.ErrorCodeKeyUnavailable)
	}
	if string(env.SenderSigningKeyID) != key.KeyID {
		return nil, serverError(protocol.ErrorCodeKeyUnavailable)
	}
	if !ciphertextMatches(env) {
		return nil, invalidRequest()
	}
	tx, err := h.deps.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError()
	}
	defer func() { _ = tx.Rollback() }()
	answer, done, err := h.replayOrAccept(ctx, tx, agent, env, req.Envelope)
	if err != nil {
		return nil, err
	}
	if done {
		return answer, nil
	}
	answer, err = h.accept(ctx, tx, agent, env, req.Envelope)
	if err != nil {
		return nil, err
	}
	return answer, nil
}

// ciphertextMatches checks the envelope's hash claim without reading its
// content: the sha256 of the ciphertext bytes must equal the declared
// digest.
func ciphertextMatches(env protocol.WireEnvelope) bool {
	sum := sha256.Sum256(env.Ciphertext)
	return "sha256:"+hex.EncodeToString(sum[:]) == string(env.CiphertextSHA256)
}

// submitRow is the admission state one submit works with.
type submitRow struct {
	messageID         string
	state             string
	controlReason     string
	controlStateValue string
	clientMsgID       string
	fromAgent         string
	expiresAt         string
	node              string
	toNode            string
	threadID          string
	recipients        []string
}

// replayOrAccept replays the stored outcome when the envelope is already
// known, refuses a different envelope, and reports the request fresh.
// Terminal states answer without the gate.
func (h *SubmitHandler) replayOrAccept(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
	env protocol.WireEnvelope,
	rawEnvelope json.RawMessage,
) (any, bool, error) {
	row, err := selectSubmitRow(ctx, tx, string(env.MessageID))
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, false, invalidRequest()
	}
	stored, err := selectStoredEnvelope(ctx, tx, row.messageID)
	if err != nil {
		return nil, false, err
	}
	if stored != nil {
		if string(stored) != string(rawEnvelope) {
			return nil, false, invalidRequest()
		}
		return h.storedOutcome(row)
	}
	switch row.state {
	case "FAILED":
		return nil, false, serverError(protocol.ErrorCode(row.controlReason))
	case "EXPIRED":
		return nil, false, serverError(protocol.ErrorCodeExpired)
	case "ADMITTED":
		if !row.fromAgentEquals(agent) {
			return nil, false, invalidRequest()
		}
		return nil, false, nil
	default:
		return nil, false, internalError()
	}
}

// storedOutcome answers a replay from the stored row.
func (h *SubmitHandler) storedOutcome(row *submitRow) (any, bool, error) {
	switch row.state {
	case "FAILED":
		return nil, false, serverError(protocol.ErrorCode(row.controlReason))
	case "EXPIRED":
		return nil, false, serverError(protocol.ErrorCodeExpired)
	default:
		return submitAnswer{
			MessageID:     row.messageID,
			State:         row.state,
			ControlState:  row.controlState(),
			ControlReason: row.controlReason,
		}, true, nil
	}
}

// fromAgentEquals reports whether the admission belongs to the caller.
func (r *submitRow) fromAgentEquals(agent protocol.AgentID) bool {
	return r.fromAgent == string(agent)
}

// controlState returns the stored control state, defaulting to the last
// written triple state for fresh rows.
func (r *submitRow) controlState() string {
	if r.controlStateValue != "" {
		return r.controlStateValue
	}
	return "ACCEPTED"
}

// accept runs one fresh acceptance evaluation and stores its rows.
func (h *SubmitHandler) accept(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
	env protocol.WireEnvelope,
	rawEnvelope json.RawMessage,
) (any, error) {
	row, err := selectSubmitRow(ctx, tx, string(env.MessageID))
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, invalidRequest()
	}
	if !envelopeBinds(row, env) {
		return nil, invalidRequest()
	}
	if expired, err := h.expired(row.expiresAt); err != nil {
		return nil, err
	} else if expired {
		if err := transitionState(ctx, tx, row.messageID, "ADMITTED", "EXPIRED", h.nowText()); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, internalError()
		}
		return nil, serverError(protocol.ErrorCodeExpired)
	}
	msg := submitMessage(row)
	acceptance, err := h.deps.Engine.Accept(ctx, tx, msg,
		func(context.Context) error {
			return storeEnvelope(ctx, tx, row.messageID, env, rawEnvelope, h.nowText())
		})
	if err != nil {
		var denied *channel.DenyError
		if errors.As(err, &denied) {
			if err := transitionState(ctx, tx, row.messageID, "ADMITTED", "FAILED", h.nowText()); err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, internalError()
			}
			return nil, serverError(denied.Code())
		}
		return nil, mapAcceptError(err)
	}
	if err := transitionState(ctx, tx, row.messageID, "ADMITTED", "PENDING", h.nowText()); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError()
	}
	return submitAnswer{
		MessageID:     row.messageID,
		State:         "PENDING",
		ControlState:  string(acceptance.State()),
		ControlReason: string(acceptance.Reason()),
	}, nil
}

// envelopeBinds checks the envelope against its admission: ids, sender,
// and recipient list.
func envelopeBinds(row *submitRow, env protocol.WireEnvelope) bool {
	if string(env.MessageID) != row.messageID ||
		string(env.ClientMsgID) != row.clientMsgID ||
		string(env.OriginAgent) != row.fromAgent {
		return false
	}
	if len(env.RecipientAgents) != len(row.recipients) {
		return false
	}
	for i := range env.RecipientAgents {
		if string(env.RecipientAgents[i]) != row.recipients[i] {
			return false
		}
	}
	return true
}

// mapAcceptError maps unexpected acceptance failures without content.
func mapAcceptError(err error) error {
	if errors.Is(err, channel.ErrInvalidQuery) {
		return invalidRequest()
	}
	return internalError()
}

// expired reports whether now reached the stored expiry.
func (h *SubmitHandler) expired(expiresAt string) (bool, error) {
	limit, err := time.Parse(timeLayout, expiresAt)
	if err != nil {
		return false, internalError()
	}
	return !h.deps.Clock.Now().UTC().Before(limit), nil
}

// nowText returns the current time in store format.
func (h *SubmitHandler) nowText() string {
	return h.deps.Clock.Now().UTC().Format(timeLayout)
}

// submitMessage builds the gate message from the admission row: ids and
// routing only, never content.
func submitMessage(row *submitRow) channel.Message {
	recipients := make([]channel.Recipient, len(row.recipients))
	for i, name := range row.recipients {
		recipients[i] = channel.Recipient{
			Agent:       protocol.AgentID(name),
			Deliverable: true,
			Policy:      channel.PolicyAllowed,
		}
	}
	return channel.Message{
		ID:       protocol.ULID(row.messageID),
		From:     protocol.AgentID(row.fromAgent),
		FromNode: protocol.NodeID(row.node),
		To:       recipients,
		ToNode:   protocol.NodeID(row.node),
		ThreadID: protocol.ThreadID(row.threadID),
		HopCount: 0,
	}
}

// selectSubmitRow reads the admission row with its recipient order.
func selectSubmitRow(ctx context.Context, tx *sql.Tx, messageID string) (*submitRow, error) {
	text, err := queryText("message_select_id.sql")
	if err != nil {
		return nil, err
	}
	var row submitRow
	var controlReason sql.NullString
	err = tx.QueryRowContext(ctx, text, messageID).Scan(
		&row.messageID, &row.state, &controlReason,
		&row.clientMsgID, &row.fromAgent, &row.expiresAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select submit message: %w", err)
	}
	row.controlReason = controlReason.String
	text, err = queryText("recipients_select.sql")
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, text, row.messageID)
	if err != nil {
		return nil, fmt.Errorf("select submit recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var agent, mode string
		var ack int
		if err := rows.Scan(&agent, &mode, &ack); err != nil {
			return nil, fmt.Errorf("scan submit recipient: %w", err)
		}
		row.recipients = append(row.recipients, agent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read submit recipients: %w", err)
	}
	if err := loadSubmitRoute(ctx, tx, &row); err != nil {
		return nil, err
	}
	return &row, nil
}

// loadSubmitRoute fills node, thread, and control state for one row.
func loadSubmitRoute(ctx context.Context, tx *sql.Tx, row *submitRow) error {
	const query = `SELECT origin_node, destination_node, thread_id, control_state ` +
		`FROM messages WHERE message_id = ?`
	var threadID sql.NullString
	err := tx.QueryRowContext(ctx, query, row.messageID).Scan(
		&row.node, &row.toNode, &threadID, &row.controlStateValue,
	)
	if err != nil {
		return fmt.Errorf("select submit route: %w", err)
	}
	row.threadID = threadID.String
	return nil
}

// selectStoredEnvelope returns the stored envelope bytes, or nil.
func selectStoredEnvelope(ctx context.Context, tx *sql.Tx, messageID string) (json.RawMessage, error) {
	text, err := queryText("envelope_select.sql")
	if err != nil {
		return nil, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, text, messageID).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select stored envelope: %w", err)
	}
	return append(json.RawMessage(nil), raw...), nil
}

// storeEnvelope inserts the envelope with every projection the table
// demands. Control columns keep their defaults: the gate owns them.
func storeEnvelope(
	ctx context.Context,
	tx *sql.Tx,
	messageID string,
	env protocol.WireEnvelope,
	rawEnvelope json.RawMessage,
	storedAt string,
) error {
	text, err := queryText("envelope_insert.sql")
	if err != nil {
		return err
	}
	recipients, err := json.Marshal(env.RecipientAgents)
	if err != nil {
		return fmt.Errorf("encode envelope recipients: %w", err)
	}
	_, err = tx.ExecContext(ctx, text,
		messageID, string(rawEnvelope), env.Protocol, env.Version,
		string(env.ClientMsgID), env.OriginNode, env.OriginAgent,
		env.DestinationNode, string(recipients), env.CreatedAt, env.ExpiresAt,
		env.Priority, env.ThreadRef, env.EncryptionProfile, env.SenderSigningKeyID,
		env.CiphertextSHA256, storedAt, env.HopCount, env.RouteDigest,
		env.ControlVersion,
	)
	if err != nil {
		return fmt.Errorf("insert message envelope: %w", err)
	}
	return nil
}

// transitionState moves one message between states exactly from the
// expected one. Zero rows means a concurrent step already moved it.
func transitionState(
	ctx context.Context,
	tx *sql.Tx,
	messageID, from, to, now string,
) error {
	text, err := queryText("message_transition.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, text, to, now, messageID, from)
	if err != nil {
		return fmt.Errorf("transition message state: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count message transition: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("transition of %s moved %d rows: %w",
			messageID, affected, errInvalidArgument)
	}
	return nil
}
