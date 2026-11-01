package hub

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const federationArtifactAckSequenceWindow = int64(1024)

var errFederationArtifactAckConflict = errors.New("federation artifact acknowledgement conflict")

type boundFederatedArtifactAck struct {
	request       federation.ArtifactAckRequest
	requestDigest protocol.SHA256Hex
}

// ReceiveFederatedArtifactAck binds B's acknowledgement to the exact
// STORED_REMOTE artifact A sent and applies origin-side retention once.
func (s *Service) ReceiveFederatedArtifactAck(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.ArtifactAckRequest,
) ([]byte, error) {
	if s.federationRegistry == nil || s.dataDir == "" {
		return nil, fmt.Errorf("receive federated artifact acknowledgement: service is not configured")
	}
	if err := request.Validate(); err != nil {
		return nil, newFederatedArtifactAckError(federation.DeliveryForbidden, err)
	}
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()
	bound, err := s.bindFederatedArtifactAck(ctx, authenticatedPeer, request)
	if err != nil {
		return nil, err
	}
	replayed, found, err := lookupFederatedArtifactAckReplay(ctx, s.db, authenticatedPeer, bound)
	if err != nil {
		if errors.Is(err, errFederationArtifactAckConflict) {
			return nil, newFederatedArtifactAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	if found {
		if err := s.removeArtifactBlobs(request.ArtifactID); err != nil {
			return nil, err
		}
		return replayed, nil
	}
	if err := enforceFederatedArtifactAckWindow(
		ctx, s.db, authenticatedPeer, request.Sequence,
	); err != nil {
		if errors.Is(err, errFederationArtifactAckConflict) {
			return nil, newFederatedArtifactAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	now := s.clock.Now().UTC()
	receipt := federation.ArtifactAckReceipt{
		ArtifactID: request.ArtifactID, Recipient: request.Recipient,
		OriginNode: s.nodeID, DestinationNode: authenticatedPeer,
		RequestSHA256: bound.requestDigest, Nonce: append([]byte(nil), request.Nonce...),
		Sequence: request.Sequence, ReceivedAt: now,
	}
	if err := receipt.Sign(s.federationRegistry); err != nil {
		return nil, fmt.Errorf("sign federation artifact acknowledgement receipt: %w", err)
	}
	exactReceipt, err := json.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("encode federation artifact acknowledgement receipt: %w", err)
	}
	stored, err := s.storeFederatedArtifactAck(
		ctx, authenticatedPeer, bound, exactReceipt, now,
	)
	if err != nil {
		return nil, err
	}
	if err := s.removeArtifactBlobs(request.ArtifactID); err != nil {
		return nil, err
	}
	return stored, nil
}

func (s *Service) bindFederatedArtifactAck(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.ArtifactAckRequest,
) (boundFederatedArtifactAck, error) {
	if !authenticatedPeer.Valid() {
		return boundFederatedArtifactAck{}, newFederatedArtifactAckError(
			federation.DeliveryForbidden, errors.New("invalid artifact acknowledgement peer"),
		)
	}
	var origin, destination protocol.NodeID
	var recipient protocol.AgentID
	var state protocol.ArtifactState
	var expiresAtText string
	err := s.db.QueryRowContext(ctx, `SELECT t.origin_node, t.destination_node,
		t.state, t.expires_at, r.recipient_agent
		FROM artifact_transfers AS t JOIN artifact_recipients AS r USING (artifact_id)
		WHERE t.artifact_id = ?`, request.ArtifactID).
		Scan(&origin, &destination, &state, &expiresAtText, &recipient)
	if errors.Is(err, sql.ErrNoRows) {
		return boundFederatedArtifactAck{}, newFederatedArtifactAckError(
			federation.DeliveryForbidden, errors.New("artifact acknowledgement not found"),
		)
	}
	if err != nil {
		return boundFederatedArtifactAck{}, fmt.Errorf("select artifact acknowledgement binding: %w", err)
	}
	if origin != s.nodeID || destination != authenticatedPeer || recipient != request.Recipient ||
		(state != protocol.ArtifactStateStoredRemote && state != protocol.ArtifactStateAcknowledged) {
		return boundFederatedArtifactAck{}, newFederatedArtifactAckError(
			federation.DeliveryForbidden, errors.New("artifact acknowledgement binding failed"),
		)
	}
	expiresAt, err := eventstore.ParseTime(expiresAtText)
	if err != nil {
		return boundFederatedArtifactAck{}, fmt.Errorf("parse artifact acknowledgement expiry: %w", err)
	}
	if !request.AckedAt.Before(expiresAt) || request.AckedAt.After(s.clock.Now().UTC().Add(5*time.Minute)) {
		return boundFederatedArtifactAck{}, newFederatedArtifactAckError(
			federation.DeliveryForbidden, errors.New("artifact acknowledgement time is invalid"),
		)
	}
	var storedRemote bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1
		FROM federation_outbox AS outbox
		JOIN federation_artifact_outbox AS artifact ON artifact.outbox_id = outbox.outbox_id
		WHERE outbox.object_type = 'artifact' AND outbox.object_id = ?
		AND outbox.destination_node = ? AND outbox.state = 'STORED_REMOTE'
		AND artifact.manifest_accepted = 1 AND artifact.last_receipt IS NOT NULL)`,
		request.ArtifactID, authenticatedPeer).Scan(&storedRemote); err != nil {
		return boundFederatedArtifactAck{}, fmt.Errorf("select stored artifact delivery: %w", err)
	}
	if !storedRemote {
		return boundFederatedArtifactAck{}, newFederatedArtifactAckError(
			federation.DeliveryForbidden, errors.New("artifact acknowledgement has no stored delivery"),
		)
	}
	requestDigest, err := request.RequestSHA256()
	if err != nil {
		return boundFederatedArtifactAck{}, newFederatedArtifactAckError(federation.DeliveryForbidden, err)
	}
	return boundFederatedArtifactAck{request: request, requestDigest: requestDigest}, nil
}

func lookupFederatedArtifactAckReplay(
	ctx context.Context,
	q registry.Querier,
	authenticatedPeer protocol.NodeID,
	bound boundFederatedArtifactAck,
) ([]byte, bool, error) {
	request := bound.request
	rows, err := q.QueryContext(ctx, `SELECT sequence, nonce, artifact_id,
		recipient_agent, request_sha256, receipt
		FROM federation_artifact_ack_inbox WHERE destination_node = ?
		AND ((artifact_id = ? AND recipient_agent = ?) OR sequence = ? OR nonce = ?)`,
		authenticatedPeer, request.ArtifactID, request.Recipient,
		request.Sequence, request.Nonce)
	if err != nil {
		return nil, false, fmt.Errorf("select federation artifact acknowledgement replay: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var replay []byte
	found := false
	conflict := false
	for rows.Next() {
		var sequence int64
		var nonce, receipt []byte
		var artifactID protocol.OpaqueID
		var recipient protocol.AgentID
		var requestDigest protocol.SHA256Hex
		if err := rows.Scan(&sequence, &nonce, &artifactID, &recipient,
			&requestDigest, &receipt); err != nil {
			return nil, false, fmt.Errorf("scan federation artifact acknowledgement replay: %w", err)
		}
		exact := sequence == request.Sequence && bytes.Equal(nonce, request.Nonce) &&
			artifactID == request.ArtifactID && recipient == request.Recipient &&
			requestDigest == bound.requestDigest
		if exact {
			found = true
			replay = append([]byte(nil), receipt...)
		} else {
			conflict = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read federation artifact acknowledgement replay: %w", err)
	}
	if conflict {
		return nil, false, errFederationArtifactAckConflict
	}
	return replay, found, nil
}

func enforceFederatedArtifactAckWindow(
	ctx context.Context,
	q registry.Querier,
	authenticatedPeer protocol.NodeID,
	sequence int64,
) error {
	var highest sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(sequence)
		FROM federation_artifact_ack_inbox WHERE destination_node = ?`, authenticatedPeer).
		Scan(&highest); err != nil {
		return fmt.Errorf("select federation artifact acknowledgement sequence window: %w", err)
	}
	if highest.Valid && highest.Int64 > federationArtifactAckSequenceWindow &&
		sequence < highest.Int64-federationArtifactAckSequenceWindow {
		return errFederationArtifactAckConflict
	}
	return nil
}

func (s *Service) storeFederatedArtifactAck(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	bound boundFederatedArtifactAck,
	exactReceipt []byte,
	receivedAt time.Time,
) ([]byte, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	replayed, found, err := lookupFederatedArtifactAckReplay(ctx, tx, authenticatedPeer, bound)
	if err != nil {
		if errors.Is(err, errFederationArtifactAckConflict) {
			return nil, newFederatedArtifactAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	if found {
		return replayed, nil
	}
	if err := enforceFederatedArtifactAckWindow(
		ctx, tx, authenticatedPeer, bound.request.Sequence,
	); err != nil {
		if errors.Is(err, errFederationArtifactAckConflict) {
			return nil, newFederatedArtifactAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	request := bound.request
	stamp := eventstore.FormatTime(receivedAt.UTC())
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_artifact_ack_inbox
		(destination_node, sequence, nonce, artifact_id, recipient_agent,
		 request_sha256, receipt, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		authenticatedPeer, request.Sequence, request.Nonce, request.ArtifactID,
		request.Recipient, bound.requestDigest, exactReceipt, stamp); err != nil {
		return nil, fmt.Errorf("insert federation artifact acknowledgement inbox: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_recipients SET state = ?, acknowledged_at = ?
		WHERE artifact_id = ? AND recipient_agent = ?`, protocol.ArtifactStateAcknowledged,
		eventstore.FormatTime(request.AckedAt.UTC()), request.ArtifactID, request.Recipient); err != nil {
		return nil, fmt.Errorf("acknowledge federated artifact recipient: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE artifact_transfers SET state = ?, updated_at = ?
		WHERE artifact_id = ? AND state IN (?, ?)`, protocol.ArtifactStateAcknowledged,
		stamp, request.ArtifactID, protocol.ArtifactStateStoredRemote,
		protocol.ArtifactStateAcknowledged); err != nil {
		return nil, fmt.Errorf("acknowledge federated artifact transfer: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM artifact_chunks WHERE artifact_id = ?`,
		request.ArtifactID); err != nil {
		return nil, fmt.Errorf("delete acknowledged origin artifact chunks: %w", err)
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return exactReceipt, nil
}

func newFederatedArtifactAckError(kind federation.DeliveryFailure, cause error) error {
	return fmt.Errorf("receive federated artifact acknowledgement: %w",
		federation.NewDeliveryError(kind, "", nil, cause))
}

var _ federation.ArtifactAckReceiver = (*Service)(nil)
