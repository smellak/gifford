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

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// This file files the operator escalation notice as a normal message:
// admitted and submitted through the usual paths, signed by the
// node's system sender, so every reader (list, claim, ack, status,
// panel, worker) treats it like any other message. The loop never
// forges a sender signature: it owns the sender's key.
//
// The notice keys on (origin node, system sender, operator-notice key)
// with a NULL namespace, never on the legacy escalation prefix agents
// can choose themselves. Stamps and the message id freeze on first
// filing: a retry after a mark failure rebuilds byte-identical input,
// and a concurrent duplicate converges instead of doubling the row.

// ErrOperatorNoticeExpired reports a retry whose prior notice already
// expired: the hub would refuse it, so there is nothing left to file.
// Callers record the mark once and stay quiet.
var ErrOperatorNoticeExpired = errors.New("hub: operator notice already expired")

// operatorNoticeClientKey spaces one notice per escalated message in
// the system sender's key space. The legacy escalation prefix stays
// readable for the upgrade path but is never filed under again.
func operatorNoticeClientKey(original protocol.ULID) protocol.OpaqueID {
	return protocol.OpaqueID("operator-notice:" + string(original))
}

// legacyNoticeClientKey is the main/rc.3 shape: sender-keyed under the
// escalation prefix. Rows under it expire through the admitted
// expirer; they are never refiled and never block the new key.
func legacyNoticeClientKey(original protocol.ULID) protocol.OpaqueID {
	return protocol.OpaqueID("escalate:" + string(original))
}

// operatorNoticeBody is the notice content: ids, the unattended agent
// and the frozen minutes unattended. The hub never reads the original
// body: only these fields cross into the notice, so the ciphertext
// stays small and content-free. encoding/json sorts map keys, keeping
// the envelope byte-identical across retries.
type operatorNoticeBody struct {
	Kind              string             `json:"kind"`
	OriginalMessageID protocol.ULID      `json:"original_message_id"`
	ThreadRef         protocol.ThreadRef `json:"thread_ref"`
	Agent             protocol.AgentID   `json:"agent"`
	MinutesUnanswered int64              `json:"minutes_unanswered"`
}

// priorNotice is one earlier filing under either notice key.
type priorNotice struct {
	messageID string
	createdAt string
	expiresAt string
	recipient protocol.AgentID
}

// lookupPriorNotice returns the earliest admission under a client key
// for the system sender: the row a retry must reuse. A missing row
// is not an error.
func lookupPriorNotice(
	ctx context.Context,
	q *sql.DB,
	node protocol.NodeID,
	key protocol.OpaqueID,
) (priorNotice, bool, error) {
	var found priorNotice
	var recipient string
	err := q.QueryRowContext(ctx, `SELECT m.message_id, m.created_at, m.expires_at, mr.agent_id
		FROM messages m JOIN message_recipients mr ON mr.message_id = m.message_id
		WHERE m.origin_node = ? AND m.from_agent = ? AND m.client_msg_id = ?
		AND m.client_msg_namespace IS NULL
		ORDER BY m.created_at ASC LIMIT 1`,
		string(node), string(registry.EmitterAgentID), string(key)).Scan(
		&found.messageID, &found.createdAt, &found.expiresAt, &recipient)
	switch {
	case err == nil:
		found.recipient = protocol.AgentID(recipient)
		return found, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return found, false, nil
	default:
		return found, false, fmt.Errorf("read prior operator notice: %w", err)
	}
}

// lookupLegacyNotice returns the earliest main/rc.3 admission under
// the escalation prefix, for the operator-change gate. New filings
// never reuse it.
func lookupLegacyNotice(
	ctx context.Context,
	q *sql.DB,
	node protocol.NodeID,
	sender protocol.AgentID,
	original protocol.ULID,
) (priorNotice, bool, error) {
	var found priorNotice
	var recipient string
	err := q.QueryRowContext(ctx, `SELECT m.message_id, m.created_at, m.expires_at, mr.agent_id
		FROM messages m JOIN message_recipients mr ON mr.message_id = m.message_id
		WHERE m.origin_node = ? AND m.from_agent = ? AND m.client_msg_id = ?
		AND m.client_msg_namespace IS NULL
		ORDER BY m.created_at ASC LIMIT 1`,
		string(node), string(sender), string(legacyNoticeClientKey(original))).Scan(
		&found.messageID, &found.createdAt, &found.expiresAt, &recipient)
	switch {
	case err == nil:
		found.recipient = protocol.AgentID(recipient)
		return found, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return found, false, nil
	default:
		return found, false, fmt.Errorf("read legacy operator notice: %w", err)
	}
}

// FileOperatorNotice files the escalation notice for one unattended
// message through the normal submit path, exactly once per notice:
// admit as the system sender, sign with its node-local key, submit
// the envelope. A retry reuses the first attempt's stamps and message
// id, so the envelope rebuilds byte-identical and converges instead
// of doubling. Concurrent filings race on the same unique keys and
// the losers retry into the winner's rows.
//
// pendingSince is the loop's first-notification stamp for the
// original; the frozen minutes derive from it, keeping retries
// identical. The caller still records the escalation mark when this
// returns nil or ErrOperatorNoticeExpired; only other errors retry
// the turn.
func (s *Service) FileOperatorNotice(
	ctx context.Context,
	sender protocol.AgentID,
	operator protocol.AgentID,
	original protocol.ULID,
	pendingSince time.Time,
) error {
	if !sender.Valid() || !operator.Valid() || !original.Valid() {
		return fmt.Errorf("file operator notice: %w", errInvalidArgument)
	}
	if sender == operator {
		// An operator's own unread message never notifies it: a
		// notice to self is a skipped filing, and the mark the
		// caller records closes the escalation.
		return nil
	}
	originalRow, err := s.escalatedOriginal(ctx, original)
	if err != nil {
		return err
	}
	newKey := operatorNoticeClientKey(original)
	prior, found, err := lookupPriorNotice(ctx, s.db, s.nodeID, newKey)
	if err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	createdAt, expiresAt := now, now.Add(24*time.Hour)
	if found {
		if prior.recipient != operator {
			// The configured operator changed after a notice to
			// the previous one: the old notice stands and no new
			// one files. The caller's mark closes the item.
			return nil
		}
		storedCreated, perr := eventstore.ParseTime(prior.createdAt)
		if perr != nil {
			return fmt.Errorf("parse prior operator notice creation: %w", perr)
		}
		storedExpires, perr := eventstore.ParseTime(prior.expiresAt)
		if perr != nil {
			return fmt.Errorf("parse prior operator notice expiry: %w", perr)
		}
		if !now.Before(storedExpires) {
			return fmt.Errorf("%w: %s", ErrOperatorNoticeExpired, prior.createdAt)
		}
		createdAt, expiresAt = storedCreated, storedExpires
	} else if legacy, found, lerr := lookupLegacyNotice(ctx, s.db, s.nodeID, sender, original); lerr != nil {
		return lerr
	} else if found && legacy.recipient != operator {
		// Same gate for the upgrade shape: a notice admitted to a
		// previous operator suppresses a fresh one to the new one.
		return nil
	}
	for attempt := range 5 {
		if err := s.fileNoticeAttempt(ctx, sender, operator, original, originalRow, newKey,
			createdAt, expiresAt, pendingSince); err != nil {
			if isUniqueViolation(err) && attempt < 4 {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("file operator notice: concurrent filings never converged: %w", errInvalidArgument)
}

// escalatedOriginal reads the thread facts the notice shares with the
// original message.
func (s *Service) escalatedOriginal(
	ctx context.Context,
	original protocol.ULID,
) (AdmitMessageParams, error) {
	var params AdmitMessageParams
	var threadID, threadRef, priority string
	// The sender here is the unattended agent the notice goes out
	// for, not the original's author: the notice shares the
	// original's thread whatever side stalled.
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(thread_id, ''), thread_ref, priority
		FROM messages WHERE message_id = ?`, string(original)).Scan(
		&threadID, &threadRef, &priority); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return params, fmt.Errorf("escalated message not found: %w", errInvalidArgument)
		}
		return params, fmt.Errorf("read escalated message: %w", err)
	}
	if threadID == "" {
		threadID = threadRef
	}
	params.ThreadID = protocol.ThreadID(threadID)
	params.Priority = protocol.Priority(priority)
	return params, nil
}

// fileNoticeAttempt runs one admit-sign-submit pass. Every input but
// the signature reuses frozen state, so a retry after a unique
// conflict rebuilds the same bytes and converges.
func (s *Service) fileNoticeAttempt(
	ctx context.Context,
	sender protocol.AgentID,
	operator protocol.AgentID,
	original protocol.ULID,
	originalRow AdmitMessageParams,
	key protocol.OpaqueID,
	createdAt, expiresAt time.Time,
	pendingSince time.Time,
) error {
	admitParams := AdmitMessageParams{
		ClientMsgID: key,
		Recipients: []MessageRecipient{{
			Agent: operator, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID:  originalRow.ThreadID,
		Priority:  originalRow.Priority,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}
	rawAdmit, err := json.Marshal(admitParams)
	if err != nil {
		return fmt.Errorf("encode operator admission: %w", err)
	}
	answer, err := s.AdmitMessage(ctx, registry.EmitterAgentID, rawAdmit)
	if err != nil {
		return fmt.Errorf("admit operator notice: %w", err)
	}
	admitted, ok := answer.(AdmitMessageResult)
	if !ok {
		return fmt.Errorf("admit operator notice: unexpected result: %w", errInvalidArgument)
	}
	minutes := int64(0)
	if !pendingSince.IsZero() && createdAt.After(pendingSince) {
		minutes = int64(createdAt.Sub(pendingSince).Truncate(time.Minute).Minutes())
	}
	body, err := json.Marshal(operatorNoticeBody{
		Kind:              "operator-escalation",
		OriginalMessageID: original,
		ThreadRef:         admitted.ThreadRef,
		Agent:             sender,
		MinutesUnanswered: minutes,
	})
	if err != nil {
		return fmt.Errorf("encode operator notice: %w", err)
	}
	signer, err := s.registry.EmitterSigningKey()
	if err != nil {
		return fmt.Errorf("load emitter key: %w", err)
	}
	digest := sha256.Sum256(body)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: admitted.MessageID, ClientMsgID: key,
		OriginNode: s.nodeID, OriginAgent: registry.EmitterAgentID,
		DestinationNode: admitted.DestinationNode, RecipientAgents: []protocol.AgentID{operator},
		CreatedAt: createdAt, ExpiresAt: expiresAt, Priority: admitParams.Priority,
		ThreadRef: admitted.ThreadRef, HopCount: 0, RouteDigest: admitted.RouteDigest,
		ControlVersion: admitted.AdmissionResult.ControlVersion, ContentType: protocol.WireContentType,
		EncryptionProfile:     protocol.EncryptionProfileNone,
		SenderSigningKeyID:    signer.KeyID,
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{}, Ciphertext: body,
		CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
	if err := giffordcrypto.SignWireEnvelope(signer.Priv, &envelope); err != nil {
		return fmt.Errorf("sign operator notice: %w", err)
	}
	rawEnvelope, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode operator envelope: %w", err)
	}
	if _, err := s.SubmitMessageEnvelope(ctx, registry.EmitterAgentID, rawEnvelope); err != nil {
		return fmt.Errorf("submit operator notice: %w", err)
	}
	return nil
}

// Concurrent filings race on the same unique keys: a uniqueness
// conflict means a twin won the row, so the retry rereads and
// converges. Anything else fails the turn. isUniqueViolation in
// bridge_reminder.go reports it; primary key conflicts surface
// under the same UNIQUE text.
