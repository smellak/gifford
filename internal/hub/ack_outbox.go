package hub

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
)

func (s *Service) enqueueFederatedAck(
	ctx context.Context,
	tx *sql.Tx,
	destination protocol.NodeID,
	messageID protocol.ULID,
	agent protocol.AgentID,
	generation int64,
	status protocol.AckStatus,
	ackedAt time.Time,
) error {
	if destination == s.nodeID || !destination.Valid() || !status.Valid() {
		return fmt.Errorf("queue federation acknowledgement: %w", errInvalidArgument)
	}
	nonce := make([]byte, deliveryNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("generate federation acknowledgement nonce: %w", err)
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM federation_outbox_acks WHERE destination_node = ?`, destination).Scan(&sequence); err != nil {
		return fmt.Errorf("allocate federation acknowledgement sequence: %w", err)
	}
	request := federation.AckRequest{
		MessageID: messageID, Agent: agent, DeliveryGeneration: generation,
		Status: status, AckedAt: ackedAt.UTC(), Nonce: nonce, Sequence: sequence,
	}
	exact, err := request.CanonicalBytes()
	if err != nil {
		return fmt.Errorf("build federation acknowledgement request: %w", err)
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil {
		return fmt.Errorf("hash federation acknowledgement request: %w", err)
	}
	now := eventstore.FormatTime(s.clock.Now().UTC())
	idempotencyKey := fmt.Sprintf("%s:%s:%d:%s", messageID, agent, generation, status)
	outboxID := "ack:" + idempotencyKey
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 next_attempt_at, idempotency_key, created_at, updated_at)
		VALUES (?, 'ack', ?, ?, 'QUEUED', 0, ?, ?, ?, ?)`,
		outboxID, idempotencyKey, destination, now, idempotencyKey, now, now); err != nil {
		return fmt.Errorf("insert federation acknowledgement outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_outbox_acks
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, destination, exact, requestSHA, sequence, nonce); err != nil {
		return fmt.Errorf("insert federation acknowledgement request: %w", err)
	}
	return nil
}
