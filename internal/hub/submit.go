package hub

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/channel"
	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// SubmitMessageEnvelopeResult is the durable outcome of an accepted envelope.
type SubmitMessageEnvelopeResult struct {
	MessageID      protocol.ULID           `json:"message_id"`
	State          string                  `json:"state"`
	ControlState   protocol.ControlState   `json:"control_state"`
	ControlReason  protocol.DecisionReason `json:"control_reason"`
	ControlVersion int64                   `json:"control_version"`
}

type storedMessage struct {
	messageID       protocol.ULID
	clientMsgID     protocol.OpaqueID
	originNode      protocol.NodeID
	fromAgent       protocol.AgentID
	destinationNode protocol.NodeID
	threadRef       protocol.ThreadRef
	threadID        protocol.ThreadID
	priority        protocol.Priority
	state           string
	createdAt       string
	expiresAt       string
	parentID        protocol.ULID
	hopCount        int64
	routeDigest     protocol.Digest
	admission       protocol.AdmissionResult
	recipients      []MessageRecipient
}

// SubmitMessageEnvelope implements S1-S7 over the exact envelope bytes in
// params. The Hub decodes only the signed routing header and opaque ciphertext.
func (s *Service) SubmitMessageEnvelope(
	ctx context.Context,
	sender protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	envelope, exact, err := decodeSubmittedEnvelope(raw)
	if err != nil {
		return nil, invalidParams(err)
	}
	if err := envelope.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	if envelope.OriginAgent != sender {
		return nil, invalidParams(fmt.Errorf("origin_agent does not match authenticated agent"))
	}
	if envelope.OriginNode != s.nodeID {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("local route: %w", errInvalidArgument))
	}
	if envelope.DestinationNode != s.nodeID && envelope.ParentMessageID != "" {
		return nil, domainError(protocol.ErrorCodeControlVersionConflict, channel.ErrUnsupported)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	key, err := s.requireSenderKey(ctx, tx, sender)
	if err != nil {
		return nil, err
	}
	if string(envelope.SenderSigningKeyID) != key.KeyID {
		return nil, domainError(protocol.ErrorCodeKeyUnavailable,
			fmt.Errorf("sender signing key is not active"))
	}
	if err := verifyEnvelope(key.PublicKey, envelope); err != nil {
		return nil, invalidParams(err)
	}

	storedEnvelope, found, err := selectEnvelope(ctx, tx, envelope.MessageID)
	if err != nil {
		return nil, err
	}
	if found {
		if !bytes.Equal(storedEnvelope, exact) {
			return nil, invalidParams(fmt.Errorf("another envelope is stored: %w", errInvalidArgument))
		}
		return loadSubmitOutcome(ctx, tx, envelope.MessageID)
	}
	message, found, err := loadStoredMessage(ctx, tx, envelope.MessageID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, invalidParams(fmt.Errorf("no local admission: %w", errInvalidArgument))
	}
	switch message.state {
	case messageStateExpired:
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	case messageStateFailed:
		return nil, storedDenial(ctx, tx, envelope.MessageID)
	case messageStateAdmitted:
	default:
		return nil, invalidParams(fmt.Errorf("message state %s has no envelope: %w", message.state, errInvalidArgument))
	}
	if err := matchEnvelope(message, envelope); err != nil {
		return nil, invalidParams(err)
	}
	if !s.clock.Now().Before(envelope.ExpiresAt) {
		if err := transitionMessageState(ctx, tx, envelope.MessageID, messageStateAdmitted, messageStateExpired,
			eventstore.FormatTime(s.clock.Now())); err != nil {
			return nil, err
		}
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	}
	gateParams := AdmitMessageParams{Recipients: message.recipients}
	gated, err := s.gateRecipients(ctx, tx, gateParams)
	if err != nil {
		return nil, err
	}
	if gated.destination != envelope.DestinationNode {
		return nil, domainError(protocol.ErrorCodeRecipientNotDeliverable,
			fmt.Errorf("destination does not match recipient route"))
	}
	gateMessage := channel.Message{
		ID: envelope.MessageID, From: sender, FromNode: envelope.OriginNode,
		To: gated.recipients, ToNode: envelope.DestinationNode, ParentID: envelope.ParentMessageID,
		ThreadID: message.threadID, HopCount: envelope.HopCount,
	}
	accepted, err := s.engine.Accept(ctx, tx, gateMessage, func(ctx context.Context) error {
		if err := insertEnvelope(ctx, tx, exact, envelope, s.clock.Now()); err != nil {
			return err
		}
		return transitionMessageState(ctx, tx, envelope.MessageID, messageStateAdmitted,
			messageStatePending, eventstore.FormatTime(s.clock.Now()))
	})
	if err != nil {
		var denied *channel.DenyError
		switch {
		case errors.As(err, &denied):
			if stateErr := transitionMessageState(ctx, tx, envelope.MessageID, messageStateAdmitted,
				messageStateFailed, eventstore.FormatTime(s.clock.Now())); stateErr != nil {
				return nil, stateErr
			}
			if commitErr := commit(tx); commitErr != nil {
				return nil, commitErr
			}
			return nil, domainError(denied.Code(), denied)
		case errors.Is(err, channel.ErrUnsupported), errors.Is(err, channel.ErrInvalidQuery):
			return nil, invalidParams(err)
		default:
			return nil, fmt.Errorf("accept message envelope: %w", err)
		}
	}
	resultState := messageStatePending
	if envelope.DestinationNode == s.nodeID {
		if err := s.enqueueDeliveries(ctx, tx, message); err != nil {
			return nil, err
		}
	} else if accepted.State() == protocol.ControlStateAllowed {
		if err := s.enqueueFederatedDelivery(ctx, tx, message, envelope, exact, key, accepted); err != nil {
			return nil, err
		}
		resultState = messageStateQueuedRemote
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return SubmitMessageEnvelopeResult{
		MessageID: envelope.MessageID, State: resultState,
		ControlState: accepted.State(), ControlReason: accepted.Reason(),
		ControlVersion: accepted.Version(),
	}, nil
}

func (s *Service) enqueueDeliveries(ctx context.Context, tx *sql.Tx, message storedMessage) error {
	priority, err := eventstore.PriorityRank(message.priority)
	if err != nil {
		return fmt.Errorf("rank delivery priority: %w", err)
	}
	now := eventstore.FormatTime(s.clock.Now())
	for _, recipient := range message.recipients {
		if recipient.DeliveryMode != protocol.DeliveryModeWhenSafe {
			continue
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO delivery_queue
			(message_id, agent_id, delivery_generation, from_agent, thread_ref, state, priority,
			 created_at, expires_at, updated_at, parent_message_id, hop_count, route_digest)
			VALUES (?, ?, 1, ?, ?, 'QUEUED', ?, ?, ?, ?, ?, ?, ?)`,
			message.messageID, recipient.Agent, message.fromAgent, message.threadRef, priority,
			now, message.expiresAt, now, nullableString(string(message.parentID)),
			message.hopCount, message.routeDigest)
		if err != nil {
			return fmt.Errorf("enqueue message delivery: %w", err)
		}
		if err := s.engine.InitializeDelivery(ctx, tx, message.messageID, recipient.Agent, 1); err != nil {
			return fmt.Errorf("initialize message delivery: %w", err)
		}
	}
	return nil
}

func decodeSubmittedEnvelope(raw json.RawMessage) (protocol.WireEnvelope, []byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return protocol.WireEnvelope{}, nil, fmt.Errorf("decode submit params: %w", err)
	}
	envelopeRaw := raw
	if nested, ok := object["envelope"]; ok {
		if len(object) != 1 {
			return protocol.WireEnvelope{}, nil, fmt.Errorf("submit params: %w", errInvalidArgument)
		}
		envelopeRaw = nested
	}
	var envelope protocol.WireEnvelope
	if err := decodeParams(envelopeRaw, &envelope); err != nil {
		return protocol.WireEnvelope{}, nil, err
	}
	return envelope, bytes.Clone(bytes.TrimSpace(envelopeRaw)), nil
}

func selectEnvelope(ctx context.Context, tx *sql.Tx, id protocol.ULID) ([]byte, bool, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT envelope FROM message_envelopes WHERE message_id = ?", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("select envelope: %w", err)
	}
	return raw, true, nil
}

func loadStoredMessage(ctx context.Context, tx *sql.Tx, id protocol.ULID) (storedMessage, bool, error) {
	var found storedMessage
	var threadID, parentID sql.NullString
	var admissionJSON sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT message_id, client_msg_id, origin_node, from_agent,
		destination_node, thread_ref, thread_id, priority, state, created_at, expires_at,
		parent_message_id, hop_count, route_digest, admission_result_json
		FROM messages WHERE message_id = ?`, id).Scan(
		&found.messageID, &found.clientMsgID, &found.originNode, &found.fromAgent,
		&found.destinationNode, &found.threadRef, &threadID, &found.priority, &found.state,
		&found.createdAt, &found.expiresAt, &parentID, &found.hopCount, &found.routeDigest, &admissionJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return storedMessage{}, false, nil
	}
	if err != nil {
		return storedMessage{}, false, fmt.Errorf("select admitted message: %w", err)
	}
	found.threadID = protocol.ThreadID(threadID.String)
	found.parentID = protocol.ULID(parentID.String)
	if admissionJSON.Valid && json.Unmarshal([]byte(admissionJSON.String), &found.admission) != nil {
		return storedMessage{}, false, fmt.Errorf("stored admission result: %w", errInvalidArgument)
	}
	rows, err := tx.QueryContext(ctx, `SELECT agent_id, delivery_mode, requires_ack
		FROM message_recipients WHERE message_id = ? ORDER BY ordinal`, id)
	if err != nil {
		return storedMessage{}, false, fmt.Errorf("select message recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var recipient MessageRecipient
		if err := rows.Scan(&recipient.Agent, &recipient.DeliveryMode, &recipient.RequiresAck); err != nil {
			return storedMessage{}, false, fmt.Errorf("scan message recipient: %w", err)
		}
		found.recipients = append(found.recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return storedMessage{}, false, fmt.Errorf("read message recipients: %w", err)
	}
	return found, true, nil
}

func matchEnvelope(stored storedMessage, envelope protocol.WireEnvelope) error {
	if envelope.MessageID != stored.messageID || envelope.ClientMsgID != stored.clientMsgID ||
		envelope.OriginNode != stored.originNode || envelope.OriginAgent != stored.fromAgent ||
		envelope.DestinationNode != stored.destinationNode || envelope.ThreadRef != stored.threadRef ||
		envelope.Priority != stored.priority || eventstore.FormatTime(envelope.CreatedAt) != stored.createdAt ||
		eventstore.FormatTime(envelope.ExpiresAt) != stored.expiresAt || envelope.ParentMessageID != stored.parentID ||
		envelope.HopCount != stored.hopCount || envelope.RouteDigest != stored.routeDigest ||
		envelope.ControlVersion != stored.admission.ControlVersion ||
		len(envelope.RecipientAgents) != len(stored.recipients) {
		return fmt.Errorf("envelope differs from admission: %w", errInvalidArgument)
	}
	for i, recipient := range envelope.RecipientAgents {
		if recipient != stored.recipients[i].Agent {
			return fmt.Errorf("envelope recipients differ from admission: %w", errInvalidArgument)
		}
	}
	return nil
}

func verifyEnvelope(publicKey ed25519.PublicKey, envelope protocol.WireEnvelope) error {
	digest := sha256.Sum256(envelope.Ciphertext)
	if string(envelope.CiphertextSHA256) != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("ciphertext digest mismatch: %w", errInvalidArgument)
	}
	if err := giffordcrypto.VerifyWireEnvelope(publicKey, envelope); err != nil {
		return fmt.Errorf("envelope signature: %w", errInvalidArgument)
	}
	return nil
}

func insertEnvelope(
	ctx context.Context,
	tx *sql.Tx,
	raw []byte,
	envelope protocol.WireEnvelope,
	storedAt time.Time,
) error {
	recipients, err := json.Marshal(envelope.RecipientAgents)
	if err != nil {
		return fmt.Errorf("encode envelope recipients: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO message_envelopes
		(message_id, envelope, protocol, version, client_msg_id, origin_node,
		 origin_agent, destination_node, recipient_agents_json, created_at,
		 expires_at, priority, thread_ref, encryption_profile, sender_signing_key_id,
		 ciphertext_sha256, stored_at, parent_message_id, loop_id, loop_generation,
		 turn_index, hop_count, route_digest, wire_control_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		envelope.MessageID, raw, envelope.Protocol, envelope.Version, envelope.ClientMsgID,
		envelope.OriginNode, envelope.OriginAgent, envelope.DestinationNode, string(recipients),
		eventstore.FormatTime(envelope.CreatedAt), eventstore.FormatTime(envelope.ExpiresAt),
		envelope.Priority, envelope.ThreadRef, envelope.EncryptionProfile, envelope.SenderSigningKeyID,
		envelope.CiphertextSHA256, eventstore.FormatTime(storedAt.UTC()),
		nullableString(string(envelope.ParentMessageID)), nullableString(string(envelope.LoopID)),
		envelope.LoopGeneration, envelope.TurnIndex, envelope.HopCount, envelope.RouteDigest,
		envelope.ControlVersion)
	if err != nil {
		return fmt.Errorf("insert envelope: %w", err)
	}
	return nil
}

func transitionMessageState(
	ctx context.Context,
	tx *sql.Tx,
	id protocol.ULID,
	from string,
	to string,
	updatedAt string,
) error {
	result, err := tx.ExecContext(ctx, "UPDATE messages SET state = ?, updated_at = ? WHERE message_id = ? AND state = ?",
		to, updatedAt, id, from)
	if err != nil {
		return fmt.Errorf("transition message state: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count message transition: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("message transition changed %d rows: %w", affected, errInvalidArgument)
	}
	return nil
}

func loadSubmitOutcome(ctx context.Context, tx *sql.Tx, id protocol.ULID) (any, error) {
	var answer SubmitMessageEnvelopeResult
	answer.MessageID = id
	if err := tx.QueryRowContext(ctx, `SELECT state, control_state, control_reason, control_version
		FROM messages WHERE message_id = ?`, id).Scan(&answer.State, &answer.ControlState,
		&answer.ControlReason, &answer.ControlVersion); err != nil {
		return nil, fmt.Errorf("select submit outcome: %w", err)
	}
	if answer.State == messageStateFailed {
		return nil, storedDenial(ctx, tx, id)
	}
	if answer.State == messageStateExpired {
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	}
	return answer, nil
}

func storedDenial(ctx context.Context, tx *sql.Tx, id protocol.ULID) error {
	var reason protocol.DecisionReason
	if err := tx.QueryRowContext(ctx, "SELECT control_reason FROM messages WHERE message_id = ?", id).Scan(&reason); err != nil {
		return fmt.Errorf("select stored denial: %w", err)
	}
	code := protocol.ErrorCode(reason)
	if !code.Valid() {
		return fmt.Errorf("stored denial reason %q: %w", reason, errInvalidArgument)
	}
	return domainError(code, fmt.Errorf("message denied"))
}
