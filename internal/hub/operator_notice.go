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
	"github.com/smellak/gifford/internal/deliveryincident"
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

// operatorNoticeText is the notice's readable subject and body: the
// escalated message's id, the agent it waits on, the frozen minutes and
// the thread reference. The hub never reads the original body, so nothing
// of its content can cross into the notice.
func operatorNoticeText(
	original protocol.ULID,
	agent protocol.AgentID,
	minutes int64,
	thread protocol.ThreadRef,
) (subject, body string) {
	subject = fmt.Sprintf("Unanswered message %s for %s", original, agent)
	body = fmt.Sprintf("Message %s to %s has waited %d minutes without being read.\n"+
		"Thread: %s\n"+
		"This operator notice carries no message content. No response is needed.",
		original, agent, minutes, thread)
	return subject, body
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
		var refile bool
		createdAt, expiresAt, refile, err = priorNoticeStamps(prior, operator, now)
		if err != nil || !refile {
			return err
		}
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

// priorNoticeStamps returns the frozen stamps a retry of an earlier
// filing reuses. refile is false when the configured operator changed
// after a notice to the previous one: the old notice stands and no new
// one files. The caller's mark closes the item.
func priorNoticeStamps(
	prior priorNotice,
	operator protocol.AgentID,
	now time.Time,
) (createdAt, expiresAt time.Time, refile bool, err error) {
	if prior.recipient != operator {
		return time.Time{}, time.Time{}, false, nil
	}
	createdAt, err = eventstore.ParseTime(prior.createdAt)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("parse prior operator notice creation: %w", err)
	}
	expiresAt, err = eventstore.ParseTime(prior.expiresAt)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("parse prior operator notice expiry: %w", err)
	}
	if !now.Before(expiresAt) {
		return time.Time{}, time.Time{}, false, fmt.Errorf("%w: %s", ErrOperatorNoticeExpired, prior.createdAt)
	}
	return createdAt, expiresAt, true, nil
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
	minutes := int64(0)
	if !pendingSince.IsZero() && createdAt.After(pendingSince) {
		minutes = int64(createdAt.Sub(pendingSince).Truncate(time.Minute).Minutes())
	}
	return s.fileSystemNoticeAttempt(ctx, operator, key, originalRow.ThreadID,
		originalRow.Priority, createdAt, expiresAt,
		func(thread protocol.ThreadRef) (string, string) {
			return operatorNoticeText(original, sender, minutes, thread)
		})
}

// FileDeliveryIncidentNotice files a fixed metadata-only alert or recovery
// for an operator or original sender through the same admit-sign-submit route.
// The durable worker supplies a stable key input, so a restart after submit
// converges on the prior message instead of creating a duplicate.
func (s *Service) FileDeliveryIncidentNotice(
	ctx context.Context,
	destination protocol.AgentID,
	notice deliveryincident.Notice,
) error {
	if !destination.Valid() || !notice.Valid() {
		return fmt.Errorf("file delivery incident notice: %w", errInvalidArgument)
	}
	if destination == registry.EmitterAgentID {
		// System notices can themselves appear in the attempt ledger, but the
		// reserved sender is intentionally not routable. Closing this outbox
		// item avoids a notice-about-notice feedback loop.
		return nil
	}
	key := notice.ClientMsgID(destination)
	prior, found, err := lookupPriorNotice(ctx, s.db, s.nodeID, key)
	if err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	createdAt, expiresAt := now, now.Add(24*time.Hour)
	if found {
		var refile bool
		createdAt, expiresAt, refile, err = priorNoticeStamps(prior, destination, now)
		if errors.Is(err, ErrOperatorNoticeExpired) {
			// A prior filing existed and outlived its delivery window. The
			// worker may close its pending outbox item without refiling it.
			return nil
		}
		if err != nil || !refile {
			return err
		}
	}
	subject, body := notice.Text()
	for attempt := range 5 {
		if err := s.fileSystemNoticeAttempt(ctx, destination, key, notice.ThreadID(),
			protocol.PriorityNormal, createdAt, expiresAt,
			func(protocol.ThreadRef) (string, string) { return subject, body }); err != nil {
			if isUniqueViolation(err) && attempt < 4 {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("file delivery incident notice: concurrent filings never converged: %w", errInvalidArgument)
}

type systemNoticeText func(protocol.ThreadRef) (subject, body string)

// fileSystemNoticeAttempt is the shared normal-message path for system-sender
// notices. Callers choose only typed metadata and a bounded renderer; this
// function owns admission, the logical message, signing and submission.
func (s *Service) fileSystemNoticeAttempt(
	ctx context.Context,
	operator protocol.AgentID,
	key protocol.OpaqueID,
	threadID protocol.ThreadID,
	priority protocol.Priority,
	createdAt, expiresAt time.Time,
	text systemNoticeText,
) error {
	admitParams := AdmitMessageParams{
		ClientMsgID: key,
		Recipients: []MessageRecipient{{
			Agent: operator, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID:  threadID,
		Priority:  priority,
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
	// The ciphertext is a normal logical message bound to its envelope, so
	// read_message decodes it like any other: the same ids, nodes, times,
	// priority, route, control version, recipient and thread sequence.
	subject, bodyText := text(admitted.ThreadRef)
	logical := protocol.LogicalMessage{
		MessageID: admitted.MessageID, ClientMsgID: key, ThreadID: threadID,
		FromAgent: registry.EmitterAgentID, ToAgents: []protocol.AgentID{operator},
		OriginNode: s.nodeID, DestinationNode: admitted.DestinationNode,
		Subject: subject, Body: bodyText, Priority: admitParams.Priority,
		DeliveryMode: protocol.DeliveryModeWhenSafe, CreatedAt: createdAt, ExpiresAt: expiresAt,
		RequiresAck: true, Sensitivity: protocol.MessageSensitivityInternal,
		ThreadSequence: admitted.ThreadSequence, HopCount: admitted.HopCount,
		RouteDigest: admitted.RouteDigest, ControlVersion: admitted.AdmissionResult.ControlVersion,
	}
	if err := logical.Validate(); err != nil {
		return fmt.Errorf("build operator notice: %w", err)
	}
	body, err := json.Marshal(logical)
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
		ThreadRef: admitted.ThreadRef, HopCount: admitted.HopCount, RouteDigest: admitted.RouteDigest,
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
