package hub

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
)

func (s *Service) enqueueFederatedArtifact(
	ctx context.Context,
	tx *sql.Tx,
	transfer artifactTransfer,
) error {
	if s.federationRegistry == nil || transfer.origin != s.nodeID ||
		transfer.destination == s.nodeID || transfer.controlState != protocol.ControlStateAllowed {
		return fmt.Errorf("queue federation artifact: %w", errInvalidArgument)
	}
	key, err := s.federationRegistry.ActiveSigningKey(ctx, tx, transfer.sender)
	if err != nil {
		return fmt.Errorf("read artifact sender key: %w", err)
	}
	if key.KeyID != string(transfer.senderKeyID) {
		return fmt.Errorf("artifact sender key is no longer active")
	}
	record, err := s.localDirectoryRecord(ctx, tx, key)
	if err != nil {
		return err
	}
	request := federation.ArtifactManifestRequest{
		ArtifactID: transfer.id, OriginNode: transfer.origin,
		DestinationNode: transfer.destination, Sender: transfer.sender,
		Reservation: protocol.ReserveArtifactParams{
			ClientMsgID: transfer.clientMsgID, Recipient: transfer.recipient.agent,
			EncryptionProfile: transfer.profile, CiphertextSize: transfer.ciphertextSize,
			ChunkCount: transfer.chunkCount, CiphertextSHA256: transfer.ciphertextSHA256,
			EncryptedManifest: append([]byte(nil), transfer.encryptedManifest...),
			RecipientKeyID:    transfer.recipient.keyID,
			KeyEnvelope:       append([]byte(nil), transfer.recipient.keyEnvelope...),
			SenderKeyID:       transfer.senderKeyID, Signature: append([]byte(nil), transfer.signature...),
			ReservedAt: transfer.createdAt, ExpiresAt: transfer.expiresAt,
		},
		SenderRecord: record,
		OriginControl: protocol.ObjectControlState{
			ObjectType: protocol.DecisionObjectTypeArtifact, ObjectID: transfer.id,
			ControlState: transfer.controlState, ControlReason: transfer.controlReason,
			ControlVersion: transfer.controlVersion,
		},
	}
	exact, err := request.CanonicalBytes()
	if err != nil {
		return fmt.Errorf("build federation artifact manifest: %w", err)
	}
	digest, err := request.RequestSHA256()
	if err != nil {
		return fmt.Errorf("hash federation artifact manifest: %w", err)
	}
	now := eventstore.FormatTime(s.clock.Now().UTC())
	idempotencyKey := "artifact:" + string(transfer.id)
	outboxID := idempotencyKey
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_outbox
		(outbox_id, object_type, object_id, destination_node, state, attempts,
		 next_attempt_at, idempotency_key, created_at, updated_at)
		VALUES (?, 'artifact', ?, ?, 'QUEUED', 0, ?, ?, ?, ?)`, outboxID,
		transfer.id, transfer.destination, now, idempotencyKey, now, now); err != nil {
		return fmt.Errorf("insert federation artifact outbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_artifact_outbox
		(outbox_id, manifest, manifest_sha256, manifest_accepted, next_chunk)
		VALUES (?, ?, ?, 0, 0)`, outboxID, exact, digest); err != nil {
		return fmt.Errorf("insert federation artifact request: %w", err)
	}
	return nil
}
