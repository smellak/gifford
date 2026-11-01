package federation

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	artifactformat "github.com/smellak/gifford/internal/artifact"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type claimedArtifact struct {
	outboxID         string
	artifactID       protocol.OpaqueID
	origin           protocol.NodeID
	destination      protocol.NodeID
	attempts         int64
	createdAt        time.Time
	expiresAt        time.Time
	manifest         []byte
	manifestDigest   protocol.SHA256Hex
	manifestAccepted bool
	nextChunk        int64
	chunkCount       int64
	leaseOwner       string
}

type artifactAttempt struct {
	phase  ArtifactReceiptPhase
	exact  []byte
	digest protocol.SHA256Hex
	sender DeliverySender
}

func (r *Replicator) replicateArtifactOnce(ctx context.Context) (bool, error) {
	claimed, found, err := r.claimArtifact(ctx)
	if err != nil || !found {
		return found, err
	}
	now := r.clock.Now().UTC()
	if !claimed.manifestAccepted && !now.Before(claimed.expiresAt) {
		return true, r.finishArtifactClaim(ctx, claimed, outboxExpired,
			protocol.ArtifactStateExpired, "EXPIRED")
	}
	if !now.Before(claimed.createdAt.Add(r.maxAge)) {
		return true, r.finishArtifactClaim(ctx, claimed, outboxDeadLetter,
			protocol.ArtifactStateFailedPermanent, "MAX_AGE")
	}
	allowed, probe, retryAt := r.breakers.allow(claimed.destination, now)
	if !allowed {
		if !retryAt.After(now) {
			retryAt = now.Add(r.lease)
		}
		return true, r.retryArtifact(ctx, claimed, peerCircuitOpenError, retryAt)
	}
	peer, err := r.registry.Peer(ctx, r.db, claimed.destination)
	if err != nil || peer.Status != registry.PeerStatusTrusted {
		r.breakers.neutral(claimed.destination, probe)
		return true, r.retryArtifact(ctx, claimed, "PEER_NOT_TRUSTED", time.Time{})
	}
	attempt, err := r.buildArtifactAttempt(ctx, claimed)
	if err != nil {
		r.breakers.neutral(claimed.destination, probe)
		finishErr := r.finishArtifactClaim(ctx, claimed, outboxDeadLetter,
			protocol.ArtifactStateFailedPermanent, "LOCAL_ARTIFACT_INVALID")
		return true, errors.Join(err, finishErr)
	}
	response, sendErr := attempt.sender.Send(ctx, peer, attempt.exact)
	if sendErr != nil {
		return true, r.retryArtifactPeerFailure(ctx, claimed, "TRANSPORT", probe)
	}
	if response.StatusCode == http.StatusConflict {
		if artifactErrorCode(response.Body) == protocol.ErrorCodeExpired {
			r.breakers.succeeded(claimed.destination)
			return true, r.finishArtifactClaim(ctx, claimed, outboxExpired,
				protocol.ArtifactStateExpired, "EXPIRED")
		}
		r.breakers.succeeded(claimed.destination)
		return true, r.finishArtifactClaim(ctx, claimed, outboxDeadLetter,
			protocol.ArtifactStateFailedPermanent, "HTTP_409")
	}
	switch response.StatusCode {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusRequestEntityTooLarge:
		r.breakers.succeeded(claimed.destination)
		return true, r.finishArtifactClaim(ctx, claimed, outboxDeadLetter,
			protocol.ArtifactStateFailedPermanent, fmt.Sprintf("HTTP_%d", response.StatusCode))
	}
	if response.StatusCode >= 500 {
		return true, r.retryArtifactPeerFailure(ctx, claimed, "HTTP_5XX", probe)
	}
	if response.StatusCode != http.StatusOK {
		return true, r.retryArtifactPeerFailure(ctx, claimed, "HTTP_RESPONSE", probe)
	}
	receipt, err := r.verifyArtifactReceipt(ctx, claimed, attempt, response.Body)
	if err != nil {
		r.emit("INVALID_ARTIFACT_RECEIPT")
		return true, r.retryArtifactPeerFailure(ctx, claimed, "INVALID_ARTIFACT_RECEIPT", probe)
	}
	r.breakers.succeeded(claimed.destination)
	return true, r.saveArtifactReceipt(ctx, claimed, receipt, response.Body)
}

func (r *Replicator) claimArtifact(ctx context.Context) (claimedArtifact, bool, error) {
	now := r.clock.Now().UTC()
	nowText := eventstore.FormatTime(now)
	leaseOwner := fmt.Sprintf("%s:artifact:%d", r.owner, r.claimID.Add(1))
	leaseUntil := eventstore.FormatTime(now.Add(r.lease))
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return claimedArtifact{}, false, fmt.Errorf("begin artifact claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE federation_outbox
		SET state = 'REPLICATING', attempts = attempts + 1, next_attempt_at = NULL,
			lease_owner = ?, lease_until = ?, updated_at = ?
		WHERE outbox_id = (
			SELECT candidate.outbox_id FROM federation_outbox AS candidate
			JOIN federation_artifact_outbox AS artifact ON artifact.outbox_id = candidate.outbox_id
			JOIN artifact_transfers AS transfer ON transfer.artifact_id = candidate.object_id
			JOIN nodes AS peer ON peer.node_id = candidate.destination_node
			WHERE candidate.object_type = 'artifact' AND peer.local = 0 AND peer.status = 'TRUSTED'
			AND (((candidate.state = 'QUEUED' OR candidate.state = 'FAILED_RETRYABLE')
				AND (candidate.next_attempt_at IS NULL OR candidate.next_attempt_at <= ?))
				OR (candidate.state = 'REPLICATING' AND candidate.lease_until IS NOT NULL
					AND candidate.lease_until <= ?))
			ORDER BY COALESCE(candidate.next_attempt_at, candidate.created_at), candidate.created_at,
				candidate.outbox_id LIMIT 1
		)
		AND (((state = 'QUEUED' OR state = 'FAILED_RETRYABLE')
			AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
			OR (state = 'REPLICATING' AND lease_until IS NOT NULL AND lease_until <= ?))`,
		leaseOwner, leaseUntil, nowText, nowText, nowText, nowText, nowText)
	if err != nil {
		return claimedArtifact{}, false, fmt.Errorf("claim artifact outbox: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return claimedArtifact{}, false, fmt.Errorf("count artifact claim: %w", err)
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return claimedArtifact{}, false, fmt.Errorf("commit empty artifact claim: %w", err)
		}
		return claimedArtifact{}, false, nil
	}
	if affected != 1 {
		return claimedArtifact{}, false, fmt.Errorf("artifact claim changed %d rows", affected)
	}
	var claimed claimedArtifact
	var createdAt, expiresAt string
	var accepted int64
	claimed.leaseOwner = leaseOwner
	err = tx.QueryRowContext(ctx, `SELECT outbox.outbox_id, outbox.object_id,
		transfer.origin_node, outbox.destination_node, outbox.attempts, outbox.created_at,
		transfer.expires_at, artifact.manifest, artifact.manifest_sha256,
		artifact.manifest_accepted, artifact.next_chunk, transfer.chunk_count
		FROM federation_outbox AS outbox
		JOIN federation_artifact_outbox AS artifact ON artifact.outbox_id = outbox.outbox_id
		JOIN artifact_transfers AS transfer ON transfer.artifact_id = outbox.object_id
		WHERE outbox.object_type = 'artifact' AND outbox.state = 'REPLICATING'
		AND outbox.lease_owner = ?`, leaseOwner).Scan(
		&claimed.outboxID, &claimed.artifactID, &claimed.origin, &claimed.destination,
		&claimed.attempts, &createdAt, &expiresAt, &claimed.manifest,
		&claimed.manifestDigest, &accepted, &claimed.nextChunk, &claimed.chunkCount)
	if err != nil {
		return claimedArtifact{}, false, fmt.Errorf("read artifact claim: %w", err)
	}
	claimed.manifestAccepted = accepted == 1
	claimed.createdAt, err = eventstore.ParseTime(createdAt)
	if err != nil {
		return claimedArtifact{}, false, fmt.Errorf("parse artifact outbox creation: %w", err)
	}
	claimed.expiresAt, err = eventstore.ParseTime(expiresAt)
	if err != nil {
		return claimedArtifact{}, false, fmt.Errorf("parse artifact expiry: %w", err)
	}
	if !claimed.artifactID.Valid() || claimed.origin != r.registry.NodeID() ||
		!claimed.destination.Valid() || claimed.destination == claimed.origin ||
		!claimed.manifestDigest.Valid() || claimed.chunkCount < 1 ||
		claimed.nextChunk < 0 || claimed.nextChunk > claimed.chunkCount ||
		(accepted != 0 && accepted != 1) {
		return claimedArtifact{}, false, fmt.Errorf("artifact claim has invalid durable metadata")
	}
	stamp := eventstore.FormatTime(now)
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ? AND state IN (?, ?)`, protocol.ArtifactStateReplicating,
		stamp, claimed.artifactID, protocol.ArtifactStateQueuedRemote,
		protocol.ArtifactStateReplicating); err != nil {
		return claimedArtifact{}, false, fmt.Errorf("mark artifact replicating: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?
		WHERE artifact_id = ? AND state IN (?, ?)`, protocol.ArtifactStateReplicating,
		claimed.artifactID, protocol.ArtifactStateQueuedRemote,
		protocol.ArtifactStateReplicating); err != nil {
		return claimedArtifact{}, false, fmt.Errorf("mark artifact recipient replicating: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return claimedArtifact{}, false, fmt.Errorf("commit artifact claim: %w", err)
	}
	return claimed, true, nil
}

func (r *Replicator) buildArtifactAttempt(
	ctx context.Context,
	claimed claimedArtifact,
) (artifactAttempt, error) {
	if !claimed.manifestAccepted {
		request, err := DecodeArtifactManifestRequest(claimed.manifest)
		if err != nil {
			return artifactAttempt{}, fmt.Errorf("decode stored artifact manifest: %w", err)
		}
		canonical, err := request.CanonicalBytes()
		if err != nil || !bytes.Equal(canonical, claimed.manifest) {
			return artifactAttempt{}, fmt.Errorf("stored artifact manifest is not canonical")
		}
		digest, err := request.RequestSHA256()
		if err != nil || digest != claimed.manifestDigest || request.ArtifactID != claimed.artifactID ||
			request.OriginNode != claimed.origin || request.DestinationNode != claimed.destination ||
			request.Reservation.ChunkCount != claimed.chunkCount {
			return artifactAttempt{}, fmt.Errorf("stored artifact manifest binding differs")
		}
		return artifactAttempt{
			phase: ArtifactReceiptManifest, exact: canonical,
			digest: digest, sender: r.artifactManifestSender,
		}, nil
	}
	if claimed.nextChunk < claimed.chunkCount {
		var size int64
		var digest protocol.SHA256Hex
		var path string
		err := r.db.QueryRowContext(ctx, `SELECT ciphertext_size, ciphertext_sha256, storage_path
			FROM artifact_chunks WHERE artifact_id = ? AND chunk_index = ?`,
			claimed.artifactID, claimed.nextChunk).Scan(&size, &digest, &path)
		if err != nil {
			return artifactAttempt{}, fmt.Errorf("read artifact chunk at cursor: %w", err)
		}
		ciphertext, err := os.ReadFile(path)
		if err != nil || int64(len(ciphertext)) != size || artifactformat.Digest(ciphertext) != digest {
			return artifactAttempt{}, fmt.Errorf("artifact chunk at cursor is not durable")
		}
		request := ArtifactChunkRequest{
			ArtifactID: claimed.artifactID, OriginNode: claimed.origin,
			DestinationNode: claimed.destination, ChunkIndex: claimed.nextChunk,
			Ciphertext: ciphertext, SHA256: digest,
		}
		exact, err := request.CanonicalBytes()
		if err != nil {
			return artifactAttempt{}, err
		}
		requestDigest, err := request.RequestSHA256()
		if err != nil {
			return artifactAttempt{}, err
		}
		return artifactAttempt{
			phase: ArtifactReceiptChunk, exact: exact,
			digest: requestDigest, sender: r.artifactChunkSender,
		}, nil
	}
	request := ArtifactCompleteRequest{
		ArtifactID: claimed.artifactID, OriginNode: claimed.origin,
		DestinationNode: claimed.destination,
	}
	exact, err := request.CanonicalBytes()
	if err != nil {
		return artifactAttempt{}, err
	}
	digest, err := request.RequestSHA256()
	if err != nil {
		return artifactAttempt{}, err
	}
	return artifactAttempt{
		phase: ArtifactReceiptComplete, exact: exact,
		digest: digest, sender: r.artifactCompleteSender,
	}, nil
}

func (r *Replicator) verifyArtifactReceipt(
	ctx context.Context,
	claimed claimedArtifact,
	attempt artifactAttempt,
	raw []byte,
) (ArtifactReceipt, error) {
	receipt, err := DecodeArtifactReceipt(raw)
	if err != nil {
		return ArtifactReceipt{}, err
	}
	publicKey, err := r.registry.PeerSigningPublicKey(ctx, r.db, claimed.destination)
	if err != nil {
		return ArtifactReceipt{}, fmt.Errorf("read artifact receipt signing key: %w", err)
	}
	if err := receipt.Verify(publicKey); err != nil {
		return ArtifactReceipt{}, err
	}
	if receipt.ArtifactID != claimed.artifactID || receipt.OriginNode != claimed.origin ||
		receipt.DestinationNode != claimed.destination || receipt.Phase != attempt.phase ||
		receipt.RequestSHA256 != attempt.digest || receipt.ChunkCount != claimed.chunkCount {
		return ArtifactReceipt{}, fmt.Errorf("artifact receipt does not match request")
	}
	switch attempt.phase {
	case ArtifactReceiptManifest:
		if receipt.Status != ArtifactReceiptReplicating || receipt.NextChunk > claimed.chunkCount {
			return ArtifactReceipt{}, fmt.Errorf("artifact manifest receipt has invalid cursor")
		}
	case ArtifactReceiptChunk:
		if receipt.Status != ArtifactReceiptReplicating || receipt.NextChunk != claimed.nextChunk+1 {
			return ArtifactReceipt{}, fmt.Errorf("artifact chunk receipt has invalid cursor")
		}
	case ArtifactReceiptComplete:
		if receipt.Status != ArtifactReceiptStoredRemote || receipt.NextChunk != claimed.chunkCount {
			return ArtifactReceipt{}, fmt.Errorf("artifact completion receipt is not final")
		}
	default:
		return ArtifactReceipt{}, fmt.Errorf("artifact receipt phase is invalid")
	}
	return receipt, nil
}

func (r *Replicator) saveArtifactReceipt(
	ctx context.Context,
	claimed claimedArtifact,
	receipt ArtifactReceipt,
	exact []byte,
) error {
	now := r.clock.Now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin artifact receipt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO federation_artifact_receipts
		(outbox_id, request_sha256, receipt, next_chunk, state, received_at)
		VALUES (?, ?, ?, ?, ?, ?)`, claimed.outboxID, receipt.RequestSHA256, exact,
		receipt.NextChunk, receipt.Status, eventstore.FormatTime(receipt.ReceivedAt.UTC())); err != nil {
		return fmt.Errorf("insert artifact receipt: %w", err)
	}
	var saveErr error
	if receipt.Status == ArtifactReceiptStoredRemote {
		saveErr = saveFinalArtifactReceipt(ctx, tx, claimed, receipt, exact, now)
	} else {
		saveErr = saveProgressArtifactReceipt(ctx, tx, claimed, receipt, exact, now)
	}
	if saveErr != nil {
		return saveErr
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit artifact receipt: %w", err)
	}
	return nil
}

func saveFinalArtifactReceipt(
	ctx context.Context,
	tx *sql.Tx,
	claimed claimedArtifact,
	receipt ArtifactReceipt,
	exact []byte,
	now time.Time,
) error {
	if err := updateClaimedArtifactOutbox(
		ctx, tx, claimed, outboxStoredRemote, nil, nil, now,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE federation_artifact_outbox
		SET manifest_accepted = 1, next_chunk = ?, last_receipt = ? WHERE outbox_id = ?`,
		receipt.NextChunk, exact, claimed.outboxID); err != nil {
		return fmt.Errorf("finalize artifact cursor: %w", err)
	}
	return updateArtifactTransferState(
		ctx, tx, claimed.artifactID, protocol.ArtifactStateStoredRemote, now,
	)
}

func saveProgressArtifactReceipt(
	ctx context.Context,
	tx *sql.Tx,
	claimed claimedArtifact,
	receipt ArtifactReceipt,
	exact []byte,
	now time.Time,
) error {
	if err := updateClaimedArtifactOutbox(
		ctx, tx, claimed, outboxQueued, nil, nil, now,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE federation_artifact_outbox
		SET manifest_accepted = 1, next_chunk = ?, last_receipt = ? WHERE outbox_id = ?`,
		receipt.NextChunk, exact, claimed.outboxID); err != nil {
		return fmt.Errorf("advance artifact cursor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_chunks SET replicated = 1
		WHERE artifact_id = ? AND chunk_index < ?`, claimed.artifactID,
		receipt.NextChunk); err != nil {
		return fmt.Errorf("mark artifact chunks replicated: %w", err)
	}
	return nil
}

func (r *Replicator) retryArtifactPeerFailure(
	ctx context.Context,
	claimed claimedArtifact,
	code string,
	probe bool,
) error {
	retryAt, opened := r.breakers.failed(claimed.destination, r.clock.Now().UTC(), probe)
	if opened {
		r.emit("PEER_CIRCUIT_OPEN")
	}
	return r.retryArtifact(ctx, claimed, code, retryAt)
}

func (r *Replicator) retryArtifact(
	ctx context.Context,
	claimed claimedArtifact,
	code string,
	peerRetryAt time.Time,
) error {
	now := r.clock.Now().UTC()
	if !claimed.manifestAccepted && !now.Before(claimed.expiresAt) {
		return r.finishArtifactClaim(ctx, claimed, outboxExpired,
			protocol.ArtifactStateExpired, "EXPIRED")
	}
	if !now.Before(claimed.createdAt.Add(r.maxAge)) {
		return r.finishArtifactClaim(ctx, claimed, outboxDeadLetter,
			protocol.ArtifactStateFailedPermanent, "MAX_AGE")
	}
	next := now.Add(r.backoff(claimed.attempts))
	if peerRetryAt.After(next) {
		next = peerRetryAt
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin artifact retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateClaimedArtifactOutbox(ctx, tx, claimed, outboxFailedRetryable,
		&code, &next, now); err != nil {
		return err
	}
	if peerRetryAt.After(now) {
		if err := deferPeerOutbox(ctx, tx, claimed.destination, peerRetryAt, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit artifact retry: %w", err)
	}
	return nil
}

func (r *Replicator) finishArtifactClaim(
	ctx context.Context,
	claimed claimedArtifact,
	outboxState string,
	artifactState protocol.ArtifactState,
	code string,
) error {
	now := r.clock.Now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin terminal artifact replication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateClaimedArtifactOutbox(ctx, tx, claimed, outboxState, &code, nil, now); err != nil {
		return err
	}
	if err := updateArtifactTransferState(ctx, tx, claimed.artifactID, artifactState, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit terminal artifact replication: %w", err)
	}
	return nil
}

func updateClaimedArtifactOutbox(
	ctx context.Context,
	tx *sql.Tx,
	claimed claimedArtifact,
	state string,
	lastError *string,
	nextAttempt *time.Time,
	now time.Time,
) error {
	var last, next any
	if lastError != nil {
		last = *lastError
	}
	if nextAttempt != nil {
		next = eventstore.FormatTime(nextAttempt.UTC())
	}
	result, err := tx.ExecContext(ctx, `UPDATE federation_outbox SET state = ?, last_error = ?,
		next_attempt_at = ?, lease_owner = NULL, lease_until = NULL, updated_at = ?
		WHERE outbox_id = ? AND object_type = 'artifact' AND state = 'REPLICATING'
		AND lease_owner = ?`, state, last, next, eventstore.FormatTime(now.UTC()),
		claimed.outboxID, claimed.leaseOwner)
	if err != nil {
		return fmt.Errorf("update claimed artifact outbox: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count artifact outbox update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("claimed artifact outbox update changed %d rows", affected)
	}
	return nil
}

func updateArtifactTransferState(
	ctx context.Context,
	tx *sql.Tx,
	id protocol.OpaqueID,
	state protocol.ArtifactState,
	now time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ? AND state IN (?, ?)`, state, eventstore.FormatTime(now.UTC()), id,
		protocol.ArtifactStateQueuedRemote, protocol.ArtifactStateReplicating)
	if err != nil {
		return fmt.Errorf("update remote artifact: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count remote artifact update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("remote artifact update changed %d rows", affected)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?
		WHERE artifact_id = ?`, state, id); err != nil {
		return fmt.Errorf("update remote artifact recipient: %w", err)
	}
	return nil
}

func artifactErrorCode(raw []byte) protocol.ErrorCode {
	var response deliveryErrorResponse
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil || requireJSONEOF(decoder) != nil {
		return ""
	}
	return response.ErrorCode
}
