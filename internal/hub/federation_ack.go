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

const federationAckSequenceWindow = int64(1024)

var errFederationAckConflict = errors.New("federation acknowledgement conflict")

type boundFederatedAck struct {
	request       federation.AckRequest
	requestDigest protocol.SHA256Hex
}

// ReceiveFederatedAck binds one authenticated remote acknowledgement to the
// exact delivery A sent, then stores its replay identity and local effect in a
// single transaction.
func (s *Service) ReceiveFederatedAck(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.AckRequest,
) ([]byte, error) {
	if s.federationRegistry == nil {
		return nil, fmt.Errorf("receive federated acknowledgement: service is not configured")
	}
	bound, err := s.bindFederatedAck(ctx, authenticatedPeer, request)
	if err != nil {
		return nil, err
	}
	replayed, found, err := lookupFederatedAckReplay(ctx, s.db, authenticatedPeer, bound)
	if err != nil {
		if errors.Is(err, errFederationAckConflict) {
			return nil, newFederatedAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	if found {
		return replayed, nil
	}
	if err := enforceFederatedAckWindow(ctx, s.db, authenticatedPeer, request.Sequence); err != nil {
		if errors.Is(err, errFederationAckConflict) {
			return nil, newFederatedAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	now := s.clock.Now().UTC()
	receipt := federation.AckReceipt{
		MessageID: request.MessageID, Agent: request.Agent,
		DeliveryGeneration: request.DeliveryGeneration, Status: request.Status,
		OriginNode: s.nodeID, DestinationNode: authenticatedPeer,
		RequestSHA256: bound.requestDigest, Nonce: append([]byte(nil), request.Nonce...),
		Sequence: request.Sequence, ReceivedAt: now,
	}
	if err := receipt.Sign(s.federationRegistry); err != nil {
		return nil, fmt.Errorf("sign federation acknowledgement receipt: %w", err)
	}
	exactReceipt, err := json.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("encode federation acknowledgement receipt: %w", err)
	}
	return s.storeFederatedAck(ctx, authenticatedPeer, bound, exactReceipt, now)
}

func (s *Service) bindFederatedAck(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.AckRequest,
) (boundFederatedAck, error) {
	if !authenticatedPeer.Valid() || !request.Status.Valid() {
		return boundFederatedAck{}, newFederatedAckError(
			federation.DeliveryForbidden, fmt.Errorf("invalid acknowledgement binding"),
		)
	}
	var originNode, destinationNode protocol.NodeID
	var state string
	err := s.db.QueryRowContext(ctx, `SELECT origin_node, destination_node, state
		FROM messages WHERE message_id = ?`, request.MessageID).
		Scan(&originNode, &destinationNode, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return boundFederatedAck{}, newFederatedAckError(
			federation.DeliveryForbidden, fmt.Errorf("acknowledgement message not found"),
		)
	}
	if err != nil {
		return boundFederatedAck{}, fmt.Errorf("select federated acknowledgement message: %w", err)
	}
	if originNode != s.nodeID || destinationNode != authenticatedPeer ||
		(state != messageStateStoredRemote && state != messageStateAcknowledged) {
		return boundFederatedAck{}, newFederatedAckError(
			federation.DeliveryForbidden, fmt.Errorf("acknowledgement message binding failed"),
		)
	}
	var recipientExists bool
	err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM message_recipients
		WHERE message_id = ? AND agent_id = ?)`, request.MessageID, request.Agent).Scan(&recipientExists)
	if err != nil {
		return boundFederatedAck{}, fmt.Errorf("select federated acknowledgement recipient: %w", err)
	}
	if !recipientExists {
		return boundFederatedAck{}, newFederatedAckError(
			federation.DeliveryForbidden, fmt.Errorf("acknowledgement recipient binding failed"),
		)
	}
	matched, err := s.sentDeliveryGeneration(
		ctx, request.MessageID, request.Agent, request.DeliveryGeneration,
	)
	if err != nil {
		return boundFederatedAck{}, err
	}
	if !matched {
		return boundFederatedAck{}, newFederatedAckError(
			federation.DeliveryForbidden, fmt.Errorf("acknowledgement generation binding failed"),
		)
	}
	requestDigest, err := request.RequestSHA256()
	if err != nil {
		return boundFederatedAck{}, newFederatedAckError(federation.DeliveryForbidden, err)
	}
	return boundFederatedAck{request: request, requestDigest: requestDigest}, nil
}

func (s *Service) sentDeliveryGeneration(
	ctx context.Context,
	messageID protocol.ULID,
	agent protocol.AgentID,
	generation int64,
) (bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT request.request
		FROM federation_outbox AS outbox
		JOIN federation_outbox_requests AS request ON request.outbox_id = outbox.outbox_id
		WHERE outbox.object_type = 'message' AND outbox.object_id = ?`, messageID)
	if err != nil {
		return false, fmt.Errorf("select sent federation delivery: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var exact []byte
		if err := rows.Scan(&exact); err != nil {
			return false, fmt.Errorf("scan sent federation delivery: %w", err)
		}
		delivery, err := federation.DecodeDeliveryRequest(exact)
		if err != nil {
			return false, fmt.Errorf("decode sent federation delivery: %w", err)
		}
		if delivery.DeliveryGeneration != generation {
			continue
		}
		for _, recipient := range delivery.Recipients {
			if recipient.Agent == agent {
				return true, nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read sent federation delivery: %w", err)
	}
	return false, nil
}

func lookupFederatedAckReplay(
	ctx context.Context,
	q registry.Querier,
	authenticatedPeer protocol.NodeID,
	bound boundFederatedAck,
) ([]byte, bool, error) {
	request := bound.request
	rows, err := q.QueryContext(ctx, `SELECT sequence, nonce, message_id, agent_id,
		delivery_generation, status, request_sha256, receipt
		FROM federation_ack_inbox WHERE origin_node = ?
		AND ((message_id = ? AND agent_id = ? AND delivery_generation = ? AND status = ?)
			OR sequence = ? OR nonce = ?)`, authenticatedPeer,
		request.MessageID, request.Agent, request.DeliveryGeneration, request.Status,
		request.Sequence, request.Nonce)
	if err != nil {
		return nil, false, fmt.Errorf("select federation acknowledgement replay: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var replay []byte
	found := false
	conflict := false
	for rows.Next() {
		var sequence, generation int64
		var nonce, receipt []byte
		var messageID protocol.ULID
		var agent protocol.AgentID
		var status protocol.AckStatus
		var requestDigest protocol.SHA256Hex
		if err := rows.Scan(
			&sequence, &nonce, &messageID, &agent, &generation, &status,
			&requestDigest, &receipt,
		); err != nil {
			return nil, false, fmt.Errorf("scan federation acknowledgement replay: %w", err)
		}
		exact := sequence == request.Sequence && bytes.Equal(nonce, request.Nonce) &&
			messageID == request.MessageID && agent == request.Agent &&
			generation == request.DeliveryGeneration && status == request.Status &&
			requestDigest == bound.requestDigest
		if exact {
			found = true
			replay = append([]byte(nil), receipt...)
		} else {
			conflict = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read federation acknowledgement replay: %w", err)
	}
	if conflict {
		return nil, false, errFederationAckConflict
	}
	return replay, found, nil
}

func enforceFederatedAckWindow(
	ctx context.Context,
	q registry.Querier,
	authenticatedPeer protocol.NodeID,
	sequence int64,
) error {
	var highest sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT MAX(sequence) FROM federation_ack_inbox
		WHERE origin_node = ?`, authenticatedPeer).Scan(&highest); err != nil {
		return fmt.Errorf("select federation acknowledgement sequence window: %w", err)
	}
	if highest.Valid && highest.Int64 > federationAckSequenceWindow &&
		sequence < highest.Int64-federationAckSequenceWindow {
		return errFederationAckConflict
	}
	return nil
}

func (s *Service) storeFederatedAck(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	bound boundFederatedAck,
	exactReceipt []byte,
	receivedAt time.Time,
) ([]byte, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	replayed, found, err := lookupFederatedAckReplay(ctx, tx, authenticatedPeer, bound)
	if err != nil {
		if errors.Is(err, errFederationAckConflict) {
			return nil, newFederatedAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	if found {
		return replayed, nil
	}
	if err := enforceFederatedAckWindow(
		ctx, tx, authenticatedPeer, bound.request.Sequence,
	); err != nil {
		if errors.Is(err, errFederationAckConflict) {
			return nil, newFederatedAckError(federation.DeliveryConflict, err)
		}
		return nil, err
	}
	request := bound.request
	if _, err := tx.ExecContext(ctx, `INSERT INTO acks
		(message_id, agent_id, delivery_generation, status, acked_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, request.MessageID,
		request.Agent, request.DeliveryGeneration, request.Status,
		eventstore.FormatTime(request.AckedAt.UTC())); err != nil {
		return nil, fmt.Errorf("insert federated acknowledgement: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_ack_inbox
		(origin_node, sequence, nonce, message_id, agent_id, delivery_generation,
		 status, request_sha256, receipt, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, authenticatedPeer,
		request.Sequence, request.Nonce, request.MessageID, request.Agent,
		request.DeliveryGeneration, request.Status, bound.requestDigest, exactReceipt,
		eventstore.FormatTime(receivedAt.UTC())); err != nil {
		return nil, fmt.Errorf("insert federation acknowledgement inbox: %w", err)
	}
	var hasRequired, allAcknowledged bool
	if err := tx.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM message_recipients
			WHERE message_id = ? AND requires_ack = 1),
		NOT EXISTS(SELECT 1 FROM message_recipients AS recipient
			WHERE recipient.message_id = ? AND recipient.requires_ack = 1
			AND NOT EXISTS(SELECT 1 FROM acks AS ack
				WHERE ack.message_id = recipient.message_id
				AND ack.agent_id = recipient.agent_id
				AND ack.delivery_generation = ? AND ack.status = 'ACKNOWLEDGED'))`,
		request.MessageID, request.MessageID, request.DeliveryGeneration).
		Scan(&hasRequired, &allAcknowledged); err != nil {
		return nil, fmt.Errorf("select federated acknowledgement completion: %w", err)
	}
	if hasRequired && allAcknowledged {
		if _, err := tx.ExecContext(ctx, `UPDATE messages SET state = 'ACKNOWLEDGED', updated_at = ?
			WHERE message_id = ? AND state = 'STORED_REMOTE'`,
			eventstore.FormatTime(receivedAt.UTC()), request.MessageID); err != nil {
			return nil, fmt.Errorf("complete federated acknowledgement message: %w", err)
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return exactReceipt, nil
}

func newFederatedAckError(kind federation.DeliveryFailure, cause error) error {
	return fmt.Errorf(
		"receive federated acknowledgement: %w",
		federation.NewDeliveryError(kind, "", nil, cause),
	)
}
