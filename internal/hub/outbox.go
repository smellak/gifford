package hub

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const deliveryNonceBytes = 16

func (s *Service) enqueueFederatedDelivery(
	ctx context.Context,
	tx *sql.Tx,
	message storedMessage,
	envelope protocol.WireEnvelope,
	exactEnvelope []byte,
	key registry.KeyInfo,
	accepted channel.Acceptance,
) error {
	if s.federationRegistry == nil || envelope.DestinationNode == s.nodeID ||
		accepted.State() != protocol.ControlStateAllowed {
		return fmt.Errorf("queue federation delivery: %w", errInvalidArgument)
	}
	sender, err := s.localDirectoryRecord(ctx, tx, key)
	if err != nil {
		return err
	}
	nonce := make([]byte, deliveryNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("generate federation delivery nonce: %w", err)
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM federation_outbox_requests WHERE destination_node = ?`, envelope.DestinationNode).
		Scan(&sequence); err != nil {
		return fmt.Errorf("allocate federation delivery sequence: %w", err)
	}
	recipients := make([]federation.DeliveryRecipient, len(message.recipients))
	for i, recipient := range message.recipients {
		recipients[i] = federation.DeliveryRecipient{
			Agent: recipient.Agent, Mode: recipient.DeliveryMode, RequiresAck: recipient.RequiresAck,
		}
	}
	const generation = int64(1)
	request := federation.DeliveryRequest{
		Envelope:           json.RawMessage(append([]byte(nil), exactEnvelope...)),
		ThreadID:           message.threadID,
		Recipients:         recipients,
		DeliveryGeneration: generation,
		Nonce:              nonce,
		Sequence:           sequence,
		OriginControl: protocol.ObjectControlState{
			ObjectType:     protocol.DecisionObjectTypeMessage,
			ObjectID:       protocol.OpaqueID(message.messageID),
			ControlState:   accepted.State(),
			ControlReason:  accepted.Reason(),
			ControlVersion: accepted.Version(),
		},
		Sender: sender,
	}
	exactRequest, err := request.CanonicalBytes()
	if err != nil {
		return fmt.Errorf("build federation delivery request: %w", err)
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil {
		return fmt.Errorf("hash federation delivery request: %w", err)
	}
	now := eventstore.FormatTime(s.clock.Now().UTC())
	idempotencyKey := fmt.Sprintf("%s:%d", message.messageID, generation)
	outboxID := "message:" + idempotencyKey
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 next_attempt_at, idempotency_key, created_at, updated_at)
		VALUES (?, 'message', ?, ?, 'QUEUED', 0, ?, ?, ?, ?)`,
		outboxID, message.messageID, envelope.DestinationNode, now, idempotencyKey, now, now); err != nil {
		return fmt.Errorf("insert federation outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_outbox_requests
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, envelope.DestinationNode,
		exactRequest, requestSHA, sequence, nonce); err != nil {
		return fmt.Errorf("insert federation outbox request: %w", err)
	}
	if err := transitionMessageState(ctx, tx, message.messageID, messageStatePending,
		messageStateQueuedRemote, now); err != nil {
		return err
	}
	return nil
}

func (s *Service) localDirectoryRecord(
	ctx context.Context,
	tx *sql.Tx,
	key registry.KeyInfo,
) (registry.DirectoryRecord, error) {
	records, err := s.federationRegistry.ExportDirectory(ctx, tx)
	if err != nil {
		return registry.DirectoryRecord{}, fmt.Errorf("export local sender directory: %w", err)
	}
	for _, record := range records {
		if record.KeyID == key.KeyID && record.Agent == key.Agent && record.Node == s.nodeID &&
			record.Status == "ACTIVE" && bytes.Equal(record.PublicKey, key.PublicKey) {
			return record, nil
		}
	}
	return registry.DirectoryRecord{}, fmt.Errorf("active sender directory record is unavailable: %w",
		registry.ErrNoUsableKey)
}
