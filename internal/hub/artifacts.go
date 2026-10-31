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
	"os"
	"time"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type artifactTransfer struct {
	id                protocol.OpaqueID
	clientMsgID       protocol.OpaqueID
	sender            protocol.AgentID
	destination       protocol.NodeID
	state             protocol.ArtifactState
	profile           string
	ciphertextSize    int64
	chunkCount        int64
	ciphertextSHA256  protocol.SHA256Hex
	encryptedManifest []byte
	senderKeyID       protocol.SigningKeyID
	signature         []byte
	createdAt         time.Time
	expiresAt         time.Time
	controlReason     protocol.DecisionReason
	controlVersion    int64
	recipient         artifactRecipient
}

type artifactRecipient struct {
	agent          protocol.AgentID
	node           protocol.NodeID
	keyEnvelope    []byte
	keyID          protocol.EncryptionKeyID
	state          protocol.ArtifactState
	acknowledgedAt *time.Time
}

type artifactEncryptionRegistry interface {
	ActiveEncryptionKey(context.Context, registry.Querier, protocol.AgentID) (registry.KeyInfo, error)
}

// ReserveArtifact admits and stores ciphertext-only metadata idempotently by
// authenticated sender and client message id.
func (s *Service) ReserveArtifact(
	ctx context.Context,
	sender protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var form struct {
		KeyOnly bool `json:"key_only"`
	}
	if json.Unmarshal(raw, &form) == nil && form.KeyOnly {
		var params protocol.ArtifactRecipientKeyParams
		if err := decodeParams(raw, &params); err != nil {
			return nil, invalidParams(err)
		}
		if err := params.Validate(); err != nil || params.Recipient == sender {
			if err == nil {
				err = errInvalidArgument
			}
			return nil, invalidParams(err)
		}
		return s.artifactRecipientKey(ctx, params.Recipient)
	}
	var params protocol.ReserveArtifactParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil || !sender.Valid() || params.Recipient == sender {
		if err == nil {
			err = errInvalidArgument
		}
		return nil, invalidParams(err)
	}
	now := s.clock.Now().UTC()
	if params.ReservedAt.After(now.Add(5*time.Minute)) || !params.ExpiresAt.After(now) {
		return nil, invalidParams(fmt.Errorf("artifact reservation time: %w", errInvalidArgument))
	}
	if s.dataDir == "" {
		return nil, fmt.Errorf("artifact store is not configured")
	}
	artifactID, err := artifactformat.DeriveArtifactID(sender, params.ClientMsgID)
	if err != nil {
		return nil, invalidParams(err)
	}

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	stored, found, err := selectArtifactByClient(ctx, tx, sender, params.ClientMsgID)
	if err != nil {
		return nil, err
	}
	// The idempotent path validates, expires, projects, and commits the same
	// stored reservation as one transaction.
	//nolint:nestif
	if found {
		if !sameReservation(stored, params, artifactID) {
			return nil, domainError(protocol.ErrorCodeReservationConflict, errors.New("reservation differs"))
		}
		if expired, err := s.expireArtifact(ctx, tx, stored); err != nil {
			return nil, err
		} else if expired {
			if err := commit(tx); err != nil {
				return nil, err
			}
			return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
		}
		result, err := reserveResult(ctx, tx, stored)
		if err != nil {
			return nil, err
		}
		if err := commit(tx); err != nil {
			return nil, err
		}
		return result, nil
	}

	senderKey, err := s.requireSenderKey(ctx, tx, sender)
	if err != nil {
		return nil, err
	}
	if senderKey.KeyID != string(params.SenderKeyID) {
		return nil, domainError(protocol.ErrorCodeKeyUnavailable, errors.New("sender key is not active"))
	}
	metadata := artifactformat.EnvelopeMetadata{
		ArtifactID: artifactID, ClientMsgID: params.ClientMsgID, Sender: sender,
		Recipient: params.Recipient, CiphertextSize: params.CiphertextSize,
		ChunkCount: params.ChunkCount, CiphertextSHA256: params.CiphertextSHA256,
		EncryptedManifest: params.EncryptedManifest, KeyEnvelope: params.KeyEnvelope,
		EncryptionProfile: params.EncryptionProfile, ExpiresAt: params.ExpiresAt,
	}
	if err := artifactformat.VerifyEnvelope(
		senderKey.PublicKey, metadata, params.Signature,
	); err != nil {
		return nil, invalidParams(err)
	}
	if err := s.requireUnblocked(ctx, tx, params.Recipient); err != nil {
		return nil, err
	}
	route, err := s.registry.Route(ctx, tx, params.Recipient)
	if err != nil {
		return nil, fmt.Errorf("route artifact recipient: %w", err)
	}
	if route.Kind != registry.RouteLocal || route.Node != s.nodeID {
		return nil, domainError(protocol.ErrorCodeRecipientNotDeliverable,
			errors.New("artifact recipient is not local"))
	}
	encryptionRegistry, ok := s.registry.(artifactEncryptionRegistry)
	if !ok {
		return nil, fmt.Errorf("artifact encryption registry is unavailable")
	}
	recipientKey, err := encryptionRegistry.ActiveEncryptionKey(ctx, tx, params.Recipient)
	if err != nil || recipientKey.KeyID != string(params.RecipientKeyID) {
		if err == nil {
			err = errors.New("recipient encryption key is not active")
		}
		return nil, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}

	accepted, err := s.engine.AdmitArtifact(ctx, tx, channel.Artifact{
		ID: artifactID, From: sender, FromNode: s.nodeID,
		To:     channel.Recipient{Agent: params.Recipient, Deliverable: true, Policy: channel.PolicyAllowed},
		ToNode: s.nodeID, CiphertextBytes: params.CiphertextSize,
	}, func(context.Context) error {
		stamp := eventstore.FormatTime(params.ReservedAt)
		expires := eventstore.FormatTime(params.ExpiresAt)
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_transfers
			(artifact_id, client_msg_id, origin_node, sender_agent, destination_node,
			 state, encryption_profile, ciphertext_size, chunk_count, ciphertext_sha256,
			 encrypted_manifest, sender_key_id, signature, expires_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			artifactID, params.ClientMsgID, s.nodeID, sender, s.nodeID,
			protocol.ArtifactStateReserved, params.EncryptionProfile, params.CiphertextSize,
			params.ChunkCount, params.CiphertextSHA256, params.EncryptedManifest,
			params.SenderKeyID, params.Signature, expires, stamp, stamp); err != nil {
			return fmt.Errorf("insert artifact transfer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_recipients
			(artifact_id, recipient_agent, recipient_node, key_envelope, state,
			 acknowledged_at, recipient_key_id) VALUES (?, ?, ?, ?, ?, NULL, ?)`,
			artifactID, params.Recipient, s.nodeID, params.KeyEnvelope,
			protocol.ArtifactStateReserved, params.RecipientKeyID); err != nil {
			return fmt.Errorf("insert artifact recipient: %w", err)
		}
		return nil
	})
	if err != nil {
		var gate *channel.ArtifactGateError
		if errors.As(err, &gate) {
			result := protocol.ArtifactAdmissionResult{
				DecisionID: gate.DecisionID(), Outcome: gate.Outcome(),
				ControlReason: gate.Reason(), ControlVersion: gate.Version(),
			}
			if err := commit(tx); err != nil {
				return nil, err
			}
			return nil, artifactGateError(result, err)
		}
		return nil, fmt.Errorf("admit artifact: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return protocol.ReserveArtifactResult{
		ArtifactID: artifactID, State: protocol.ArtifactStateReserved,
		ChunkSize: protocol.ArtifactChunkSize, NextChunk: 0,
		Admission: protocol.ArtifactAdmissionResult{
			DecisionID: accepted.EvaluationID(), Outcome: protocol.DecisionOutcomeAllow,
			ControlReason: accepted.Reason(), ControlVersion: accepted.Version(),
		},
	}, nil
}

func (s *Service) artifactRecipientKey(
	ctx context.Context,
	recipient protocol.AgentID,
) (protocol.ArtifactRecipientKeyResult, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return protocol.ArtifactRecipientKeyResult{}, err
	}
	defer rollback(tx)
	if err := s.requireUnblocked(ctx, tx, recipient); err != nil {
		return protocol.ArtifactRecipientKeyResult{}, err
	}
	route, err := s.registry.Route(ctx, tx, recipient)
	if err != nil {
		return protocol.ArtifactRecipientKeyResult{}, fmt.Errorf("route artifact recipient: %w", err)
	}
	if route.Kind != registry.RouteLocal || route.Node != s.nodeID {
		return protocol.ArtifactRecipientKeyResult{}, domainError(
			protocol.ErrorCodeRecipientNotDeliverable, errors.New("artifact recipient is not local"))
	}
	encryptionRegistry, ok := s.registry.(artifactEncryptionRegistry)
	if !ok {
		return protocol.ArtifactRecipientKeyResult{}, fmt.Errorf("artifact encryption registry is unavailable")
	}
	key, err := encryptionRegistry.ActiveEncryptionKey(ctx, tx, recipient)
	if err != nil {
		return protocol.ArtifactRecipientKeyResult{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	result := protocol.ArtifactRecipientKeyResult{
		KeyID: protocol.EncryptionKeyID(key.KeyID), PublicKey: append([]byte(nil), key.PublicKey...),
	}
	if err := result.Validate(); err != nil {
		return protocol.ArtifactRecipientKeyResult{}, fmt.Errorf("recipient encryption key: %w", err)
	}
	if err := commit(tx); err != nil {
		return protocol.ArtifactRecipientKeyResult{}, err
	}
	return result, nil
}

// UploadArtifactChunk atomically stores one verified ciphertext chunk.
func (s *Service) UploadArtifactChunk(
	ctx context.Context,
	sender protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params protocol.UploadArtifactChunkParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err := requireArtifact(ctx, tx, params.ArtifactID)
	if err != nil {
		return nil, err
	}
	if transfer.sender != sender {
		return nil, domainError(protocol.ErrorCodeNotSender, errors.New("caller is not sender"))
	}
	if expired, err := s.expireArtifact(ctx, tx, transfer); err != nil {
		return nil, err
	} else if expired {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
	}
	if params.ChunkIndex >= transfer.chunkCount {
		return nil, domainError(protocol.ErrorCodeChunkOutOfRange, errors.New("chunk index out of range"))
	}
	wantSize, err := artifactformat.ExpectedChunkSize(
		transfer.ciphertextSize, transfer.chunkCount, params.ChunkIndex,
	)
	if err != nil || int64(len(params.Ciphertext)) != wantSize {
		return nil, domainError(protocol.ErrorCodeChunkSizeInvalid, errors.New("chunk size invalid"))
	}
	actualDigest := artifactformat.Digest(params.Ciphertext)
	if actualDigest != params.SHA256 {
		return nil, domainError(protocol.ErrorCodeChunkDigestMismatch, errors.New("chunk digest mismatch"))
	}
	var storedSize int64
	var storedDigest protocol.SHA256Hex
	err = tx.QueryRowContext(ctx, `SELECT ciphertext_size, ciphertext_sha256
		FROM artifact_chunks WHERE artifact_id = ? AND chunk_index = ?`,
		params.ArtifactID, params.ChunkIndex).Scan(&storedSize, &storedDigest)
	if err == nil {
		if storedSize != wantSize || storedDigest != params.SHA256 {
			return nil, domainError(protocol.ErrorCodeChunkDigestMismatch, errors.New("stored chunk differs"))
		}
		next, err := nextArtifactChunk(ctx, tx, transfer.id, transfer.chunkCount)
		if err != nil {
			return nil, err
		}
		if err := commit(tx); err != nil {
			return nil, err
		}
		return protocol.UploadArtifactChunkResult{
			ArtifactID: transfer.id, State: transfer.state, ChunkIndex: params.ChunkIndex,
			NextChunk: next, Stored: false,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("select artifact chunk: %w", err)
	}
	if transfer.state != protocol.ArtifactStateReserved &&
		transfer.state != protocol.ArtifactStateUploading {
		return nil, domainError(protocol.ErrorCodeUploadIncomplete,
			errors.New("artifact upload is no longer writable"))
	}
	path, err := s.writeArtifactChunk(transfer.id, params.ChunkIndex, params.Ciphertext)
	if err != nil {
		return nil, err
	}
	stamp := eventstore.FormatTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_chunks
		(artifact_id, chunk_index, ciphertext_size, ciphertext_sha256, storage_path,
		 fsynced, replicated, created_at) VALUES (?, ?, ?, ?, ?, 1, 0, ?)`,
		transfer.id, params.ChunkIndex, wantSize, params.SHA256, path, stamp); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("insert artifact chunk: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ? AND state IN (?, ?)`, protocol.ArtifactStateUploading, stamp,
		transfer.id, protocol.ArtifactStateReserved, protocol.ArtifactStateUploading); err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("mark artifact uploading: %w", err)
	}
	next, err := nextArtifactChunk(ctx, tx, transfer.id, transfer.chunkCount)
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	if err := commit(tx); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return protocol.UploadArtifactChunkResult{
		ArtifactID: transfer.id, State: protocol.ArtifactStateUploading,
		ChunkIndex: params.ChunkIndex, NextChunk: next, Stored: true,
	}, nil
}

// CompleteArtifactUpload verifies the durable concatenated ciphertext and
// publishes it locally in one database transaction.
func (s *Service) CompleteArtifactUpload(
	ctx context.Context,
	sender protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params protocol.ArtifactIDParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err := requireArtifact(ctx, tx, params.ArtifactID)
	if err != nil {
		return nil, err
	}
	if transfer.sender != sender {
		return nil, domainError(protocol.ErrorCodeNotSender, errors.New("caller is not sender"))
	}
	if expired, err := s.expireArtifact(ctx, tx, transfer); err != nil {
		return nil, err
	} else if expired {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
	}
	if transfer.state == protocol.ArtifactStateLocalAvailable || transfer.state == protocol.ArtifactStateAcknowledged {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return protocol.ArtifactMutationResult{
			ArtifactID: transfer.id, TransferState: transfer.state,
			RecipientState: transfer.recipient.state,
		}, nil
	}
	rows, err := artifactChunkRows(ctx, tx, transfer.id)
	if err != nil {
		return nil, err
	}
	if int64(len(rows)) != transfer.chunkCount {
		return nil, domainError(protocol.ErrorCodeUploadIncomplete, errors.New("chunks missing"))
	}
	hash := sha256.New()
	var total int64
	for index, row := range rows {
		if row.index != int64(index) {
			return nil, domainError(protocol.ErrorCodeUploadIncomplete, errors.New("chunk gap"))
		}
		content, err := os.ReadFile(row.path)
		if err != nil || int64(len(content)) != row.size || artifactformat.Digest(content) != row.digest {
			return nil, domainError(protocol.ErrorCodeUploadIncomplete, errors.New("chunk is not durable"))
		}
		total += int64(len(content))
		_, _ = hash.Write(content)
	}
	if total != transfer.ciphertextSize {
		return nil, domainError(protocol.ErrorCodeUploadIncomplete, errors.New("ciphertext size differs"))
	}
	digest := protocol.SHA256Hex(hex.EncodeToString(hash.Sum(nil)))
	if digest != transfer.ciphertextSHA256 {
		return nil, domainError(protocol.ErrorCodeCiphertextDigestMismatch, errors.New("ciphertext digest differs"))
	}
	stamp := eventstore.FormatTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateStoredOrigin, stamp, transfer.id); err != nil {
		return nil, fmt.Errorf("mark artifact stored: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateLocalAvailable, stamp, transfer.id); err != nil {
		return nil, fmt.Errorf("publish local artifact: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateLocalAvailable, transfer.id); err != nil {
		return nil, fmt.Errorf("publish artifact recipient: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return protocol.ArtifactMutationResult{
		ArtifactID: transfer.id, TransferState: protocol.ArtifactStateLocalAvailable,
		RecipientState: protocol.ArtifactStateLocalAvailable,
	}, nil
}

// GetArtifactStatus returns encrypted metadata to the sender or sole recipient.
func (s *Service) GetArtifactStatus(
	ctx context.Context,
	caller protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params protocol.ArtifactIDParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err := requireArtifact(ctx, tx, params.ArtifactID)
	if err != nil {
		return nil, err
	}
	if caller != transfer.sender && caller != transfer.recipient.agent {
		return nil, domainError(protocol.ErrorCodeNotRecipient, errors.New("caller cannot inspect artifact"))
	}
	if expired, err := s.expireArtifact(ctx, tx, transfer); err != nil {
		return nil, err
	} else if expired {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
	}
	result, err := s.artifactStatus(ctx, tx, transfer)
	if err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return result, nil
}

// ListPendingArtifacts lists the recipient's locally available transfers.
func (s *Service) ListPendingArtifacts(
	ctx context.Context,
	recipient protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params protocol.ListPendingArtifactsParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	rows, err := tx.QueryContext(ctx, `SELECT t.artifact_id, t.sender_agent, r.state,
		t.ciphertext_size, t.chunk_count, t.expires_at
		FROM artifact_transfers t JOIN artifact_recipients r USING (artifact_id)
		WHERE r.recipient_agent = ? AND r.state IN (?, ?)
		ORDER BY t.created_at, t.artifact_id`, recipient,
		protocol.ArtifactStateLocalAvailable, protocol.ArtifactStateDownloading)
	if err != nil {
		return nil, fmt.Errorf("list pending artifacts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := protocol.ListPendingArtifactsResult{Artifacts: []protocol.ArtifactSummary{}}
	for rows.Next() {
		var item protocol.ArtifactSummary
		var expires string
		if err := rows.Scan(&item.ArtifactID, &item.Sender, &item.State,
			&item.CiphertextSize, &item.ChunkCount, &expires); err != nil {
			return nil, fmt.Errorf("scan pending artifact: %w", err)
		}
		item.ExpiresAt, err = eventstore.ParseTime(expires)
		if err != nil {
			return nil, fmt.Errorf("parse artifact expiry: %w", err)
		}
		if !s.clock.Now().Before(item.ExpiresAt) {
			if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ? WHERE artifact_id = ?`,
				protocol.ArtifactStateExpired, item.ArtifactID); err != nil {
				return nil, fmt.Errorf("expire pending artifact: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ? WHERE artifact_id = ?`,
				protocol.ArtifactStateExpired, item.ArtifactID); err != nil {
				return nil, fmt.Errorf("expire pending recipient: %w", err)
			}
			continue
		}
		result.Artifacts = append(result.Artifacts, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending artifacts: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close pending artifacts: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return result, nil
}

// DownloadArtifactChunk serves ciphertext only to its recipient.
func (s *Service) DownloadArtifactChunk(
	ctx context.Context,
	recipient protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params protocol.DownloadArtifactChunkParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err := requireArtifact(ctx, tx, params.ArtifactID)
	if err != nil {
		return nil, err
	}
	if transfer.recipient.agent != recipient || transfer.recipient.state == protocol.ArtifactStateFailedPermanent {
		return nil, domainError(protocol.ErrorCodeNotRecipient, errors.New("caller is not recipient"))
	}
	if expired, err := s.expireArtifact(ctx, tx, transfer); err != nil {
		return nil, err
	} else if expired {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
	}
	if transfer.state != protocol.ArtifactStateLocalAvailable || params.ChunkIndex >= transfer.chunkCount {
		return nil, domainError(protocol.ErrorCodeChunkOutOfRange, errors.New("chunk unavailable"))
	}
	var path string
	var digest protocol.SHA256Hex
	if err := tx.QueryRowContext(ctx, `SELECT storage_path, ciphertext_sha256
		FROM artifact_chunks WHERE artifact_id = ? AND chunk_index = ?`,
		transfer.id, params.ChunkIndex).Scan(&path, &digest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domainError(protocol.ErrorCodeUploadIncomplete, errors.New("chunk missing"))
		}
		return nil, fmt.Errorf("select download chunk: %w", err)
	}
	ciphertext, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read artifact chunk: %w", err)
	}
	if transfer.recipient.state == protocol.ArtifactStateLocalAvailable {
		if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?
			WHERE artifact_id = ? AND recipient_agent = ?`, protocol.ArtifactStateDownloading,
			transfer.id, recipient); err != nil {
			return nil, fmt.Errorf("mark artifact downloading: %w", err)
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return protocol.DownloadArtifactChunkResult{
		ArtifactID: transfer.id, ChunkIndex: params.ChunkIndex,
		Ciphertext: ciphertext, SHA256: digest,
	}, nil
}

// AckArtifact acknowledges a verified plaintext and removes stored blobs once
// every recipient has acknowledged.
func (s *Service) AckArtifact(
	ctx context.Context,
	recipient protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	return s.finishArtifact(ctx, recipient, raw, true)
}

// RejectArtifact permanently rejects verification and retains ciphertext.
func (s *Service) RejectArtifact(
	ctx context.Context,
	recipient protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params protocol.RejectArtifactParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	return s.rejectArtifact(ctx, recipient, params.ArtifactID)
}

func (s *Service) finishArtifact(
	ctx context.Context,
	recipient protocol.AgentID,
	raw json.RawMessage,
	ack bool,
) (any, error) {
	var params protocol.ArtifactIDParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return nil, invalidParams(err)
	}
	if !ack {
		return s.rejectArtifact(ctx, recipient, params.ArtifactID)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err := requireArtifact(ctx, tx, params.ArtifactID)
	if err != nil {
		return nil, err
	}
	if transfer.recipient.agent != recipient || transfer.recipient.state == protocol.ArtifactStateFailedPermanent {
		return nil, domainError(protocol.ErrorCodeNotRecipient, errors.New("caller is not recipient"))
	}
	if expired, err := s.expireArtifact(ctx, tx, transfer); err != nil {
		return nil, err
	} else if expired {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
	}
	if transfer.state == protocol.ArtifactStateAcknowledged &&
		transfer.recipient.state == protocol.ArtifactStateAcknowledged {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return protocol.ArtifactMutationResult{
			ArtifactID: transfer.id, TransferState: protocol.ArtifactStateAcknowledged,
			RecipientState: protocol.ArtifactStateAcknowledged,
		}, nil
	}
	if transfer.state != protocol.ArtifactStateLocalAvailable ||
		(transfer.recipient.state != protocol.ArtifactStateLocalAvailable &&
			transfer.recipient.state != protocol.ArtifactStateDownloading) {
		return nil, domainError(protocol.ErrorCodeUploadIncomplete,
			errors.New("artifact is not available for acknowledgement"))
	}
	stamp := eventstore.FormatTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients
		SET state = ?, acknowledged_at = ? WHERE artifact_id = ? AND recipient_agent = ?`,
		protocol.ArtifactStateAcknowledged, stamp, transfer.id, recipient); err != nil {
		return nil, fmt.Errorf("ack artifact recipient: %w", err)
	}
	var pending int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_recipients
		WHERE artifact_id = ? AND state != ?`, transfer.id, protocol.ArtifactStateAcknowledged).Scan(&pending); err != nil {
		return nil, fmt.Errorf("count artifact acknowledgements: %w", err)
	}
	transferState := transfer.state
	if pending == 0 {
		transferState = protocol.ArtifactStateAcknowledged
		if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
			WHERE artifact_id = ?`, transferState, stamp, transfer.id); err != nil {
			return nil, fmt.Errorf("ack artifact transfer: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_chunks WHERE artifact_id = ?`, transfer.id); err != nil {
			return nil, fmt.Errorf("delete acknowledged chunk rows: %w", err)
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	if pending == 0 {
		if err := s.removeArtifactBlobs(transfer.id); err != nil {
			return nil, err
		}
	}
	return protocol.ArtifactMutationResult{
		ArtifactID: transfer.id, TransferState: transferState,
		RecipientState: protocol.ArtifactStateAcknowledged,
	}, nil
}

func (s *Service) rejectArtifact(
	ctx context.Context,
	recipient protocol.AgentID,
	id protocol.OpaqueID,
) (any, error) {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	transfer, err := requireArtifact(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if transfer.recipient.agent != recipient {
		return nil, domainError(protocol.ErrorCodeNotRecipient, errors.New("caller is not recipient"))
	}
	if expired, err := s.expireArtifact(ctx, tx, transfer); err != nil {
		return nil, err
	} else if expired {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return nil, domainError(protocol.ErrorCodeExpired, errors.New("artifact expired"))
	}
	if transfer.recipient.state == protocol.ArtifactStateFailedPermanent {
		if err := commit(tx); err != nil {
			return nil, err
		}
		return protocol.ArtifactMutationResult{
			ArtifactID: transfer.id, TransferState: transfer.state,
			RecipientState: protocol.ArtifactStateFailedPermanent,
		}, nil
	}
	if transfer.state != protocol.ArtifactStateLocalAvailable ||
		(transfer.recipient.state != protocol.ArtifactStateLocalAvailable &&
			transfer.recipient.state != protocol.ArtifactStateDownloading) {
		return nil, domainError(protocol.ErrorCodeUploadIncomplete,
			errors.New("artifact is not available for verification"))
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients
		SET state = ?, acknowledged_at = NULL WHERE artifact_id = ? AND recipient_agent = ?`,
		protocol.ArtifactStateFailedPermanent, transfer.id, recipient); err != nil {
		return nil, fmt.Errorf("reject artifact: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return protocol.ArtifactMutationResult{
		ArtifactID: transfer.id, TransferState: transfer.state,
		RecipientState: protocol.ArtifactStateFailedPermanent,
	}, nil
}

func (s *Service) artifactStatus(
	ctx context.Context,
	tx *sql.Tx,
	transfer artifactTransfer,
) (protocol.ArtifactStatusResult, error) {
	key, err := s.registry.ActiveSigningKey(ctx, tx, transfer.sender)
	if err != nil || key.KeyID != string(transfer.senderKeyID) {
		if err == nil {
			err = errors.New("sender signing key is not active")
		}
		return protocol.ArtifactStatusResult{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	next, err := nextArtifactChunk(ctx, tx, transfer.id, transfer.chunkCount)
	if err != nil {
		return protocol.ArtifactStatusResult{}, err
	}
	result := protocol.ArtifactStatusResult{
		ArtifactID: transfer.id, ClientMsgID: transfer.clientMsgID, Sender: transfer.sender,
		State: transfer.state, EncryptionProfile: transfer.profile,
		CiphertextSize: transfer.ciphertextSize, ChunkCount: transfer.chunkCount,
		CiphertextSHA256:  transfer.ciphertextSHA256,
		EncryptedManifest: append([]byte(nil), transfer.encryptedManifest...),
		SenderKeyID:       transfer.senderKeyID,
		SenderPublicKey:   append([]byte(nil), key.PublicKey...),
		Signature:         append([]byte(nil), transfer.signature...), ExpiresAt: transfer.expiresAt,
		NextChunk: next,
		Recipients: []protocol.ArtifactRecipientStatus{{
			Agent: transfer.recipient.agent, KeyID: transfer.recipient.keyID,
			KeyEnvelope: append([]byte(nil), transfer.recipient.keyEnvelope...),
			State:       transfer.recipient.state, AcknowledgedAt: transfer.recipient.acknowledgedAt,
		}},
	}
	if err := result.Validate(); err != nil {
		return protocol.ArtifactStatusResult{}, fmt.Errorf("artifact status: %w", err)
	}
	return result, nil
}

func reserveResult(
	ctx context.Context,
	tx *sql.Tx,
	transfer artifactTransfer,
) (protocol.ReserveArtifactResult, error) {
	next, err := nextArtifactChunk(ctx, tx, transfer.id, transfer.chunkCount)
	if err != nil {
		return protocol.ReserveArtifactResult{}, err
	}
	var admission protocol.ArtifactAdmissionResult
	err = tx.QueryRowContext(ctx, `SELECT decision_id, outcome, reason
		FROM channel_control_decisions
		WHERE object_type = ? AND object_id = ? AND stage = ?
		ORDER BY created_at DESC, rowid DESC LIMIT 1`, protocol.DecisionObjectTypeArtifact,
		transfer.id, protocol.DecisionStageAdmission).Scan(
		&admission.DecisionID, &admission.Outcome, &admission.ControlReason)
	if err != nil {
		return protocol.ReserveArtifactResult{}, fmt.Errorf("select artifact admission: %w", err)
	}
	admission.ControlVersion = transfer.controlVersion
	return protocol.ReserveArtifactResult{
		ArtifactID: transfer.id, State: transfer.state, ChunkSize: protocol.ArtifactChunkSize,
		NextChunk: next, Admission: admission,
	}, nil
}

func sameReservation(
	stored artifactTransfer,
	params protocol.ReserveArtifactParams,
	id protocol.OpaqueID,
) bool {
	return stored.id == id && stored.clientMsgID == params.ClientMsgID &&
		stored.recipient.agent == params.Recipient && stored.profile == params.EncryptionProfile &&
		stored.ciphertextSize == params.CiphertextSize && stored.chunkCount == params.ChunkCount &&
		stored.ciphertextSHA256 == params.CiphertextSHA256 &&
		bytes.Equal(stored.encryptedManifest, params.EncryptedManifest) &&
		stored.recipient.keyID == params.RecipientKeyID &&
		bytes.Equal(stored.recipient.keyEnvelope, params.KeyEnvelope) &&
		stored.senderKeyID == params.SenderKeyID && bytes.Equal(stored.signature, params.Signature) &&
		stored.createdAt.Equal(params.ReservedAt) && stored.expiresAt.Equal(params.ExpiresAt)
}

func requireArtifact(ctx context.Context, q registry.Querier, id protocol.OpaqueID) (artifactTransfer, error) {
	transfer, found, err := selectArtifact(ctx, q, `WHERE t.artifact_id = ?`, id)
	if err != nil {
		return artifactTransfer{}, err
	}
	if !found {
		return artifactTransfer{}, domainError(protocol.ErrorCodeArtifactNotFound, errors.New("artifact not found"))
	}
	return transfer, nil
}

func selectArtifactByClient(
	ctx context.Context,
	q registry.Querier,
	sender protocol.AgentID,
	clientMsgID protocol.OpaqueID,
) (artifactTransfer, bool, error) {
	return selectArtifact(ctx, q, `WHERE t.sender_agent = ? AND t.client_msg_id = ?`, sender, clientMsgID)
}

func selectArtifact(
	ctx context.Context,
	q registry.Querier,
	where string,
	args ...any,
) (artifactTransfer, bool, error) {
	row := q.QueryRowContext(ctx, `SELECT t.artifact_id, t.client_msg_id, t.sender_agent,
		t.destination_node, t.state, t.encryption_profile, t.ciphertext_size,
		t.chunk_count, t.ciphertext_sha256, t.encrypted_manifest, t.sender_key_id,
		t.signature, t.created_at, t.expires_at, t.control_reason, t.control_version,
		r.recipient_agent, r.recipient_node, r.key_envelope, r.recipient_key_id,
		r.state, r.acknowledged_at
		FROM artifact_transfers t JOIN artifact_recipients r USING (artifact_id) `+where, args...)
	var transfer artifactTransfer
	var createdAt, expiresAt string
	var acknowledgedAt sql.NullString
	err := row.Scan(
		&transfer.id, &transfer.clientMsgID, &transfer.sender, &transfer.destination,
		&transfer.state, &transfer.profile, &transfer.ciphertextSize, &transfer.chunkCount,
		&transfer.ciphertextSHA256, &transfer.encryptedManifest, &transfer.senderKeyID,
		&transfer.signature, &createdAt, &expiresAt, &transfer.controlReason,
		&transfer.controlVersion, &transfer.recipient.agent, &transfer.recipient.node,
		&transfer.recipient.keyEnvelope, &transfer.recipient.keyID,
		&transfer.recipient.state, &acknowledgedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return artifactTransfer{}, false, nil
	}
	if err != nil {
		return artifactTransfer{}, false, fmt.Errorf("select artifact: %w", err)
	}
	transfer.createdAt, err = eventstore.ParseTime(createdAt)
	if err != nil {
		return artifactTransfer{}, false, fmt.Errorf("parse artifact creation time: %w", err)
	}
	transfer.expiresAt, err = eventstore.ParseTime(expiresAt)
	if err != nil {
		return artifactTransfer{}, false, fmt.Errorf("parse artifact expiry: %w", err)
	}
	if acknowledgedAt.Valid {
		parsed, err := eventstore.ParseTime(acknowledgedAt.String)
		if err != nil {
			return artifactTransfer{}, false, fmt.Errorf("parse artifact acknowledgement: %w", err)
		}
		transfer.recipient.acknowledgedAt = &parsed
	}
	if !transfer.id.Valid() || !transfer.clientMsgID.Valid() || !transfer.sender.Valid() ||
		!transfer.destination.Valid() || !transfer.state.Valid() ||
		transfer.profile != protocol.ArtifactEncryptionProfile || transfer.ciphertextSize < 16 ||
		transfer.chunkCount < 1 || !transfer.ciphertextSHA256.Valid() ||
		!transfer.senderKeyID.Valid() || len(transfer.signature) != ed25519.SignatureSize ||
		!transfer.recipient.agent.Valid() || !transfer.recipient.node.Valid() ||
		!transfer.recipient.keyID.Valid() || !transfer.recipient.state.Valid() {
		return artifactTransfer{}, false, fmt.Errorf("stored artifact metadata is invalid")
	}
	return transfer, true, nil
}

func nextArtifactChunk(
	ctx context.Context,
	q registry.Querier,
	id protocol.OpaqueID,
	count int64,
) (int64, error) {
	rows, err := q.QueryContext(ctx, `SELECT chunk_index FROM artifact_chunks
		WHERE artifact_id = ? ORDER BY chunk_index`, id)
	if err != nil {
		return 0, fmt.Errorf("select artifact chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	next := int64(0)
	for rows.Next() {
		var index int64
		if err := rows.Scan(&index); err != nil {
			return 0, fmt.Errorf("scan artifact chunk index: %w", err)
		}
		if index == next {
			next++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read artifact chunk indexes: %w", err)
	}
	if next > count {
		return 0, fmt.Errorf("artifact has too many chunks")
	}
	return next, nil
}

type artifactChunkRow struct {
	index  int64
	size   int64
	digest protocol.SHA256Hex
	path   string
}

func artifactChunkRows(
	ctx context.Context,
	q registry.Querier,
	id protocol.OpaqueID,
) ([]artifactChunkRow, error) {
	rows, err := q.QueryContext(ctx, `SELECT chunk_index, ciphertext_size,
		ciphertext_sha256, storage_path FROM artifact_chunks
		WHERE artifact_id = ? ORDER BY chunk_index`, id)
	if err != nil {
		return nil, fmt.Errorf("select artifact chunks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []artifactChunkRow
	for rows.Next() {
		var row artifactChunkRow
		if err := rows.Scan(&row.index, &row.size, &row.digest, &row.path); err != nil {
			return nil, fmt.Errorf("scan artifact chunk: %w", err)
		}
		found = append(found, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read artifact chunks: %w", err)
	}
	return found, nil
}

func (s *Service) expireArtifact(
	ctx context.Context,
	tx *sql.Tx,
	transfer artifactTransfer,
) (bool, error) {
	if transfer.state == protocol.ArtifactStateAcknowledged || s.clock.Now().Before(transfer.expiresAt) {
		return false, nil
	}
	stamp := eventstore.FormatTime(s.clock.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateExpired, stamp, transfer.id); err != nil {
		return false, fmt.Errorf("expire artifact: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?
		WHERE artifact_id = ?`, protocol.ArtifactStateExpired, transfer.id); err != nil {
		return false, fmt.Errorf("expire artifact recipients: %w", err)
	}
	return true, nil
}
