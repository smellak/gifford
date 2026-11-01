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

func (s *Service) enqueueFederatedArtifactAck(
	ctx context.Context,
	tx *sql.Tx,
	destination protocol.NodeID,
	artifactID protocol.OpaqueID,
	recipient protocol.AgentID,
	ackedAt time.Time,
) error {
	if destination == s.nodeID || !destination.Valid() || !artifactID.Valid() || !recipient.Valid() {
		return fmt.Errorf("queue federation artifact acknowledgement: %w", errInvalidArgument)
	}
	nonce := make([]byte, deliveryNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("generate federation artifact acknowledgement nonce: %w", err)
	}
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1
		FROM federation_artifact_ack_outbox WHERE destination_node = ?`, destination).
		Scan(&sequence); err != nil {
		return fmt.Errorf("allocate federation artifact acknowledgement sequence: %w", err)
	}
	request := federation.ArtifactAckRequest{
		ArtifactID: artifactID, Recipient: recipient,
		AckedAt: ackedAt.UTC(), Nonce: nonce, Sequence: sequence,
	}
	exact, err := request.CanonicalBytes()
	if err != nil {
		return fmt.Errorf("build federation artifact acknowledgement: %w", err)
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil {
		return fmt.Errorf("hash federation artifact acknowledgement: %w", err)
	}
	now := eventstore.FormatTime(s.clock.Now().UTC())
	idempotencyKey := fmt.Sprintf("artifact-ack:%s:%s", artifactID, recipient)
	outboxID := idempotencyKey
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 next_attempt_at, idempotency_key, created_at, updated_at)
		VALUES (?, 'artifact_ack', ?, ?, 'QUEUED', 0, ?, ?, ?, ?)`,
		outboxID, artifactID, destination, now, idempotencyKey, now, now); err != nil {
		return fmt.Errorf("insert federation artifact acknowledgement outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_artifact_ack_outbox
		(outbox_id, destination_node, request, request_sha256, sequence, nonce)
		VALUES (?, ?, ?, ?, ?, ?)`, outboxID, destination, exact, requestSHA,
		sequence, nonce); err != nil {
		return fmt.Errorf("insert federation artifact acknowledgement request: %w", err)
	}
	return nil
}
