package hub

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

var errFederationArtifactConflict = errors.New("federation artifact conflict")

// ReceiveArtifactManifest validates immutable metadata and creates the
// receiver-side transfer. Exact replay returns B's current durable cursor.
func (s *Service) ReceiveArtifactManifest(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.ArtifactManifestRequest,
) ([]byte, error) {
	if s.federationRegistry == nil || s.dataDir == "" {
		return nil, fmt.Errorf("receive artifact manifest: service is not configured")
	}
	if err := request.Validate(); err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	if authenticatedPeer != request.OriginNode || request.DestinationNode != s.nodeID {
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeNotFound, errors.New("artifact route does not match authenticated peer"))
	}
	requestDigest, err := request.RequestSHA256()
	if err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	derived, err := artifactformat.DeriveArtifactID(request.Sender, request.Reservation.ClientMsgID)
	if err != nil || derived != request.ArtifactID {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "",
			errors.New("artifact id does not match sender and client id"))
	}
	now := s.clock.Now().UTC()
	if request.Reservation.ReservedAt.After(now.Add(5 * time.Minute)) {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "",
			errors.New("artifact reservation is in the future"))
	}

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	if !now.Before(request.Reservation.ExpiresAt) {
		transfer, found, lookupErr := selectArtifact(
			ctx, s.db, `WHERE t.artifact_id = ?`, request.ArtifactID,
		)
		if lookupErr != nil {
			return nil, fmt.Errorf("look up expired artifact manifest: %w", lookupErr)
		}
		if found && transfer.origin != s.nodeID && !now.Before(transfer.expiresAt) &&
			validateArtifactPeer(transfer, authenticatedPeer, request.DestinationNode) == nil {
			if purgeErr := s.purgeFederatedArtifact(ctx, transfer); purgeErr != nil {
				return nil, purgeErr
			}
		}
		return nil, s.artifactFederationError(federation.DeliveryConflict,
			protocol.ErrorCodeExpired, errors.New("artifact has expired"))
	}
	if receipt, found, replayErr := s.replayArtifactManifest(
		ctx, authenticatedPeer, request, requestDigest,
	); replayErr != nil {
		return nil, replayErr
	} else if found {
		return receipt, nil
	}

	imported, err := s.federationRegistry.ImportDirectory(
		ctx, s.db, authenticatedPeer, []registry.DirectoryRecord{request.SenderRecord},
	)
	if err != nil {
		if !registryDeliveryRejection(err) {
			return nil, fmt.Errorf("import artifact sender directory: %w", err)
		}
		return nil, s.artifactFederationError(
			federation.DeliveryForbidden, protocol.ErrorCodeKeyUnavailable, err,
		)
	}
	if imported.PeerSuspended || len(imported.SuspendedAgents) != 0 {
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeKeyUnavailable, registry.ErrNoUsableKey)
	}
	key, err := s.federationRegistry.ActiveSigningKey(ctx, s.db, request.Sender)
	if err != nil || key.KeyID != string(request.Reservation.SenderKeyID) ||
		!bytes.Equal(key.PublicKey, request.SenderRecord.PublicKey) {
		if err == nil {
			err = errors.New("active sender key does not match manifest")
		}
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeKeyUnavailable, err)
	}
	metadata := artifactformat.EnvelopeMetadata{
		ArtifactID: request.ArtifactID, ClientMsgID: request.Reservation.ClientMsgID,
		Sender: request.Sender, Recipient: request.Reservation.Recipient,
		CiphertextSize:    request.Reservation.CiphertextSize,
		ChunkCount:        request.Reservation.ChunkCount,
		CiphertextSHA256:  request.Reservation.CiphertextSHA256,
		EncryptedManifest: request.Reservation.EncryptedManifest,
		KeyEnvelope:       request.Reservation.KeyEnvelope,
		EncryptionProfile: request.Reservation.EncryptionProfile,
		ExpiresAt:         request.Reservation.ExpiresAt,
	}
	if err := artifactformat.VerifyEnvelope(key.PublicKey, metadata, request.Reservation.Signature); err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	route, err := s.federationRegistry.Route(ctx, s.db, request.Reservation.Recipient)
	if err != nil || route.Kind != registry.RouteLocal || route.Node != s.nodeID {
		if err == nil {
			err = errors.New("artifact recipient is not local")
		}
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeRecipientNotDeliverable, err)
	}
	recipientKey, err := s.federationRegistry.ActiveEncryptionKey(
		ctx, s.db, request.Reservation.Recipient,
	)
	if err != nil || recipientKey.KeyID != string(request.Reservation.RecipientKeyID) {
		if err == nil {
			err = errors.New("recipient encryption key is not active")
		}
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeKeyUnavailable, err)
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if found, err := lookupArtifactManifest(ctx, tx, request.ArtifactID, authenticatedPeer, requestDigest); err != nil {
		if errors.Is(err, errFederationArtifactConflict) {
			return nil, s.artifactFederationConflict(err)
		}
		return nil, err
	} else if found {
		return nil, s.artifactFederationConflict(errors.New("artifact manifest appeared concurrently"))
	}
	accepted, err := s.engine.AcceptFederatedArtifact(ctx, tx, channel.Artifact{
		ID: request.ArtifactID, From: request.Sender, FromNode: request.OriginNode,
		To: channel.Recipient{
			Agent: request.Reservation.Recipient, Deliverable: true,
			Policy: channel.PolicyAllowed,
		},
		ToNode: request.DestinationNode, CiphertextBytes: request.Reservation.CiphertextSize,
	}, request.OriginControl, func(callbackCtx context.Context, _ channel.Acceptance) error {
		return storeFederatedArtifact(callbackCtx, tx, request)
	})
	if err != nil {
		var gate *channel.ArtifactGateError
		switch {
		case errors.As(err, &gate):
			if err := commit(tx); err != nil {
				return nil, err
			}
			return nil, s.artifactFederationError(federation.DeliveryForbidden,
				deliveryErrorCode(gate.Reason()), gate)
		case errors.Is(err, channel.ErrInvalidQuery):
			return nil, s.artifactFederationConflict(err)
		case errors.Is(err, channel.ErrUnsupported):
			return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
		default:
			return nil, fmt.Errorf("accept federated artifact: %w", err)
		}
	}
	if accepted.State() != protocol.ControlStateAllowed {
		return nil, fmt.Errorf("accepted artifact has non-allowed control state")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_artifact_inbox
		(artifact_id, origin_node, request_sha256, received_at) VALUES (?, ?, ?, ?)`,
		request.ArtifactID, authenticatedPeer, requestDigest,
		eventstore.FormatTime(s.clock.Now().UTC())); err != nil {
		return nil, fmt.Errorf("insert artifact inbox: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return s.signArtifactReceipt(request.ArtifactID, request.OriginNode,
		request.DestinationNode, federation.ArtifactReceiptManifest, requestDigest,
		0, request.Reservation.ChunkCount, federation.ArtifactReceiptReplicating)
}

// ReceiveArtifactChunk stores one exact chunk at B's authoritative cursor.
func (s *Service) ReceiveArtifactChunk(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.ArtifactChunkRequest,
) ([]byte, error) {
	if s.federationRegistry == nil || s.dataDir == "" {
		return nil, fmt.Errorf("receive artifact chunk: service is not configured")
	}
	if err := request.Validate(); err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	if authenticatedPeer != request.OriginNode || request.DestinationNode != s.nodeID {
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeNotFound, errors.New("artifact route does not match authenticated peer"))
	}
	requestDigest, err := request.RequestSHA256()
	if err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	transfer, err := requireArtifact(ctx, s.db, request.ArtifactID)
	if err != nil {
		return nil, s.artifactFederationConflict(err)
	}
	if err := validateArtifactPeer(transfer, authenticatedPeer, request.DestinationNode); err != nil {
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeNotFound, err)
	}
	if !s.clock.Now().UTC().Before(transfer.expiresAt) {
		if err := s.purgeFederatedArtifact(ctx, transfer); err != nil {
			return nil, err
		}
		return nil, s.artifactFederationError(federation.DeliveryConflict,
			protocol.ErrorCodeExpired, errors.New("artifact has expired"))
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err = requireArtifact(ctx, tx, request.ArtifactID)
	if err != nil {
		return nil, s.artifactFederationConflict(err)
	}
	next, err := nextArtifactChunk(ctx, tx, transfer.id, transfer.chunkCount)
	if err != nil {
		return nil, err
	}
	if request.ChunkIndex < next {
		var size int64
		var digest protocol.SHA256Hex
		var path string
		err := tx.QueryRowContext(ctx, `SELECT ciphertext_size, ciphertext_sha256, storage_path
			FROM artifact_chunks WHERE artifact_id = ? AND chunk_index = ?`,
			request.ArtifactID, request.ChunkIndex).Scan(&size, &digest, &path)
		if err != nil || size != int64(len(request.Ciphertext)) || digest != request.SHA256 {
			return nil, s.artifactFederationConflict(errors.New("artifact chunk replay differs"))
		}
		stored, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(stored, request.Ciphertext) {
			return nil, s.artifactFederationConflict(errors.New("artifact chunk replay is not durable"))
		}
		if err := commit(tx); err != nil {
			return nil, err
		}
		return s.signArtifactReceipt(transfer.id, transfer.origin, transfer.destination,
			federation.ArtifactReceiptChunk, requestDigest, next, transfer.chunkCount,
			federation.ArtifactReceiptReplicating)
	}
	if request.ChunkIndex != next || request.ChunkIndex >= transfer.chunkCount {
		return nil, s.artifactFederationConflict(errors.New("artifact chunk is not at receiver cursor"))
	}
	wantSize, err := artifactformat.ExpectedChunkSize(
		transfer.ciphertextSize, transfer.chunkCount, request.ChunkIndex,
	)
	if err != nil || int64(len(request.Ciphertext)) != wantSize {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest,
			protocol.ErrorCodeChunkSizeInvalid, errors.New("artifact chunk size differs"))
	}
	if artifactformat.Digest(request.Ciphertext) != request.SHA256 {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest,
			protocol.ErrorCodeChunkDigestMismatch, errors.New("artifact chunk digest differs"))
	}
	path, err := s.writeArtifactChunk(transfer.id, request.ChunkIndex, request.Ciphertext)
	if err != nil {
		return nil, err
	}
	stamp := eventstore.FormatTime(s.clock.Now().UTC())
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_chunks
		(artifact_id, chunk_index, ciphertext_size, ciphertext_sha256, storage_path,
		 fsynced, replicated, created_at) VALUES (?, ?, ?, ?, ?, 1, 1, ?)`,
		transfer.id, request.ChunkIndex, wantSize, request.SHA256, path, stamp); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("insert federated artifact chunk: %w", err)
	}
	if err := commit(tx); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return s.signArtifactReceipt(transfer.id, transfer.origin, transfer.destination,
		federation.ArtifactReceiptChunk, requestDigest, next+1, transfer.chunkCount,
		federation.ArtifactReceiptReplicating)
}

// CompleteArtifact validates the concatenated ciphertext and publishes it to
// the local recipient only after every chunk is durable.
func (s *Service) CompleteArtifact(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.ArtifactCompleteRequest,
) ([]byte, error) {
	if s.federationRegistry == nil || s.dataDir == "" {
		return nil, fmt.Errorf("complete federated artifact: service is not configured")
	}
	if err := request.Validate(); err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	if authenticatedPeer != request.OriginNode || request.DestinationNode != s.nodeID {
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeNotFound, errors.New("artifact route does not match authenticated peer"))
	}
	requestDigest, err := request.RequestSHA256()
	if err != nil {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest, "", err)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	transfer, err := requireArtifact(ctx, s.db, request.ArtifactID)
	if err != nil {
		return nil, s.artifactFederationConflict(err)
	}
	if err := validateArtifactPeer(transfer, authenticatedPeer, request.DestinationNode); err != nil {
		return nil, s.artifactFederationError(federation.DeliveryForbidden,
			protocol.ErrorCodeNotFound, err)
	}
	if !s.clock.Now().UTC().Before(transfer.expiresAt) {
		if err := s.purgeFederatedArtifact(ctx, transfer); err != nil {
			return nil, err
		}
		return nil, s.artifactFederationError(federation.DeliveryConflict,
			protocol.ErrorCodeExpired, errors.New("artifact has expired"))
	}
	if transfer.state == protocol.ArtifactStateLocalAvailable ||
		transfer.state == protocol.ArtifactStateAcknowledged {
		return s.signArtifactReceipt(transfer.id, transfer.origin, transfer.destination,
			federation.ArtifactReceiptComplete, requestDigest, transfer.chunkCount,
			transfer.chunkCount, federation.ArtifactReceiptStoredRemote)
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	rows, err := artifactChunkRows(ctx, tx, transfer.id)
	if err != nil {
		return nil, err
	}
	if int64(len(rows)) != transfer.chunkCount {
		return nil, s.artifactFederationConflict(errors.New("artifact chunks are incomplete"))
	}
	hash := sha256.New()
	var total int64
	for index, row := range rows {
		if row.index != int64(index) {
			return nil, s.artifactFederationConflict(errors.New("artifact chunk cursor has a gap"))
		}
		content, err := os.ReadFile(row.path)
		if err != nil || int64(len(content)) != row.size || artifactformat.Digest(content) != row.digest {
			return nil, s.artifactFederationConflict(errors.New("artifact chunk is not durable"))
		}
		total += int64(len(content))
		_, _ = hash.Write(content)
	}
	if total != transfer.ciphertextSize {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest,
			protocol.ErrorCodeChunkSizeInvalid, errors.New("artifact ciphertext size differs"))
	}
	digest := protocol.SHA256Hex(hex.EncodeToString(hash.Sum(nil)))
	if digest != transfer.ciphertextSHA256 {
		return nil, s.artifactFederationError(federation.DeliveryBadRequest,
			protocol.ErrorCodeCiphertextDigestMismatch, errors.New("artifact ciphertext digest differs"))
	}
	stamp := eventstore.FormatTime(s.clock.Now().UTC())
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateLocalAvailable, stamp, transfer.id); err != nil {
		return nil, fmt.Errorf("publish federated artifact: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateLocalAvailable, transfer.id); err != nil {
		return nil, fmt.Errorf("publish federated artifact recipient: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return s.signArtifactReceipt(transfer.id, transfer.origin, transfer.destination,
		federation.ArtifactReceiptComplete, requestDigest, transfer.chunkCount,
		transfer.chunkCount, federation.ArtifactReceiptStoredRemote)
}

func storeFederatedArtifact(
	ctx context.Context,
	tx *sql.Tx,
	request federation.ArtifactManifestRequest,
) error {
	stamp := eventstore.FormatTime(request.Reservation.ReservedAt.UTC())
	expires := eventstore.FormatTime(request.Reservation.ExpiresAt.UTC())
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_transfers
		(artifact_id, client_msg_id, origin_node, sender_agent, destination_node,
		 state, encryption_profile, ciphertext_size, chunk_count, ciphertext_sha256,
		 encrypted_manifest, sender_key_id, signature, expires_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		request.ArtifactID, request.Reservation.ClientMsgID, request.OriginNode,
		request.Sender, request.DestinationNode, protocol.ArtifactStateReplicating,
		request.Reservation.EncryptionProfile, request.Reservation.CiphertextSize,
		request.Reservation.ChunkCount, request.Reservation.CiphertextSHA256,
		request.Reservation.EncryptedManifest, request.Reservation.SenderKeyID,
		request.Reservation.Signature, expires, stamp, stamp); err != nil {
		return fmt.Errorf("insert federated artifact transfer: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_recipients
		(artifact_id, recipient_agent, recipient_node, key_envelope, state,
		 acknowledged_at, recipient_key_id) VALUES (?, ?, ?, ?, ?, NULL, ?)`,
		request.ArtifactID, request.Reservation.Recipient, request.DestinationNode,
		request.Reservation.KeyEnvelope, protocol.ArtifactStateReplicating,
		request.Reservation.RecipientKeyID); err != nil {
		return fmt.Errorf("insert federated artifact recipient: %w", err)
	}
	return nil
}

func (s *Service) replayArtifactManifest(
	ctx context.Context,
	peer protocol.NodeID,
	request federation.ArtifactManifestRequest,
	digest protocol.SHA256Hex,
) ([]byte, bool, error) {
	found, err := lookupArtifactManifest(ctx, s.db, request.ArtifactID, peer, digest)
	if errors.Is(err, errFederationArtifactConflict) {
		return nil, false, s.artifactFederationConflict(err)
	}
	if err != nil || !found {
		return nil, false, err
	}
	transfer, err := requireArtifact(ctx, s.db, request.ArtifactID)
	if err != nil {
		return nil, false, s.artifactFederationConflict(err)
	}
	if !s.clock.Now().UTC().Before(transfer.expiresAt) {
		if err := s.purgeFederatedArtifact(ctx, transfer); err != nil {
			return nil, false, err
		}
		return nil, false, s.artifactFederationError(federation.DeliveryConflict,
			protocol.ErrorCodeExpired, errors.New("artifact has expired"))
	}
	next, err := nextArtifactChunk(ctx, s.db, transfer.id, transfer.chunkCount)
	if err != nil {
		return nil, false, err
	}
	receipt, err := s.signArtifactReceipt(transfer.id, transfer.origin, transfer.destination,
		federation.ArtifactReceiptManifest, digest, next, transfer.chunkCount,
		federation.ArtifactReceiptReplicating)
	return receipt, true, err
}

func lookupArtifactManifest(
	ctx context.Context,
	q registry.Querier,
	id protocol.OpaqueID,
	peer protocol.NodeID,
	digest protocol.SHA256Hex,
) (bool, error) {
	var storedDigest protocol.SHA256Hex
	var storedPeer protocol.NodeID
	err := q.QueryRowContext(ctx, `SELECT origin_node, request_sha256
		FROM federation_artifact_inbox WHERE artifact_id = ?`, id).Scan(&storedPeer, &storedDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select artifact manifest replay: %w", err)
	}
	if storedPeer != peer || storedDigest != digest {
		return false, errFederationArtifactConflict
	}
	return true, nil
}

func (s *Service) signArtifactReceipt(
	id protocol.OpaqueID,
	origin protocol.NodeID,
	destination protocol.NodeID,
	phase federation.ArtifactReceiptPhase,
	requestDigest protocol.SHA256Hex,
	next int64,
	count int64,
	status federation.ArtifactReceiptStatus,
) ([]byte, error) {
	receipt := federation.ArtifactReceipt{
		ArtifactID: id, OriginNode: origin, DestinationNode: destination,
		Phase: phase, RequestSHA256: requestDigest, NextChunk: next,
		ChunkCount: count, Status: status, ReceivedAt: s.clock.Now().UTC(),
	}
	if err := receipt.Sign(s.federationRegistry); err != nil {
		return nil, fmt.Errorf("sign artifact receipt: %w", err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("encode artifact receipt: %w", err)
	}
	return raw, nil
}

func validateArtifactPeer(
	transfer artifactTransfer,
	peer protocol.NodeID,
	destination protocol.NodeID,
) error {
	if transfer.origin != peer || transfer.destination != destination || transfer.origin == transfer.destination {
		return errors.New("stored artifact route does not match authenticated peer")
	}
	return nil
}

func (s *Service) purgeFederatedArtifact(ctx context.Context, transfer artifactTransfer) error {
	if transfer.origin == s.nodeID {
		return fmt.Errorf("refuse to purge origin artifact")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := tx.ExecContext(ctx, "DELETE FROM artifact_chunks WHERE artifact_id = ?", transfer.id); err != nil {
		return fmt.Errorf("delete expired federated chunks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM artifact_recipients WHERE artifact_id = ?", transfer.id); err != nil {
		return fmt.Errorf("delete expired federated recipient: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM federation_artifact_inbox WHERE artifact_id = ?", transfer.id); err != nil {
		return fmt.Errorf("delete expired artifact inbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM artifact_transfers WHERE artifact_id = ?", transfer.id); err != nil {
		return fmt.Errorf("delete expired federated transfer: %w", err)
	}
	if err := commit(tx); err != nil {
		return err
	}
	if err := s.removeArtifactBlobs(transfer.id); err != nil {
		return fmt.Errorf("remove expired federated blobs: %w", err)
	}
	return nil
}

func (s *Service) artifactFederationConflict(cause error) error {
	return s.artifactFederationError(
		federation.DeliveryConflict, protocol.ErrorCodeControlVersionConflict, cause,
	)
}

func (s *Service) artifactFederationError(
	kind federation.DeliveryFailure,
	code protocol.ErrorCode,
	cause error,
) error {
	return fmt.Errorf("receive federated artifact: %w",
		federation.NewDeliveryError(kind, code, nil, cause))
}

var _ federation.ArtifactReceiver = (*Service)(nil)
