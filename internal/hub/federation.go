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
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	"github.com/smellak/gifford/internal/policy"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	federationSequenceWindow = int64(1024)
	federationClockTolerance = 5 * time.Minute
)

var errFederationConflict = errors.New("federation delivery conflict")

type receivedDelivery struct {
	request        federation.DeliveryRequest
	envelope       protocol.WireEnvelope
	exactEnvelope  []byte
	requestDigest  protocol.SHA256Hex
	envelopeDigest protocol.SHA256Hex
	recipients     []MessageRecipient
	channelTo      []channel.Recipient
}

// ReceiveDelivery validates and stores one authenticated federation request.
// Directory import intentionally commits before the message transaction so a
// key collision quarantines the peer even though the delivery is refused.
func (s *Service) ReceiveDelivery(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	request federation.DeliveryRequest,
) ([]byte, error) {
	if s.federationRegistry == nil || s.policy == nil {
		return nil, fmt.Errorf("receive federation delivery: service is not configured")
	}
	received, err := s.bindReceivedDelivery(authenticatedPeer, request)
	if err != nil {
		return nil, err
	}
	replayed, found, err := lookupFederationReplay(ctx, s.db, authenticatedPeer, received)
	if err != nil {
		if errors.Is(err, errFederationConflict) {
			return nil, s.deliveryConflict(err)
		}
		return nil, err
	}
	if found {
		return replayed, nil
	}
	if err := enforceFederationWindow(ctx, s.db, authenticatedPeer, request.Sequence); err != nil {
		if errors.Is(err, errFederationConflict) {
			return nil, s.deliveryConflict(err)
		}
		return nil, err
	}
	if received.envelope.DestinationNode != s.nodeID {
		return nil, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeNotFound, nil,
			fmt.Errorf("envelope destination is not this node"),
		)
	}
	if err := received.request.Validate(); err != nil {
		return nil, newFederationDeliveryError(federation.DeliveryForbidden, "", nil, err)
	}
	received.recipients = make([]MessageRecipient, len(request.Recipients))
	for i, recipient := range request.Recipients {
		received.recipients[i] = MessageRecipient{
			Agent: recipient.Agent, DeliveryMode: recipient.Mode, RequiresAck: recipient.RequiresAck,
		}
	}
	received.channelTo, err = s.gateFederatedRecipients(ctx, received.recipients)
	if err != nil {
		var rejected *callError
		if errors.As(err, &rejected) {
			return nil, newFederationDeliveryError(
				federation.DeliveryForbidden, rejected.domain, nil, err,
			)
		}
		return nil, err
	}

	imported, err := s.federationRegistry.ImportDirectory(
		ctx, s.db, authenticatedPeer, []registry.DirectoryRecord{request.Sender},
	)
	if err != nil {
		if registryDeliveryRejection(err) {
			return nil, newFederationDeliveryError(
				federation.DeliveryForbidden, protocol.ErrorCodeKeyUnavailable, nil, err,
			)
		}
		return nil, fmt.Errorf("import federation sender directory: %w", err)
	}
	if imported.PeerSuspended || len(imported.SuspendedAgents) != 0 {
		return nil, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeKeyUnavailable, nil,
			fmt.Errorf("sender directory import quarantined peer: %w", registry.ErrNoUsableKey),
		)
	}
	key, err := s.federationRegistry.ActiveSigningKey(ctx, s.db, received.envelope.OriginAgent)
	if err != nil {
		return nil, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeKeyUnavailable, nil, err,
		)
	}
	if key.KeyID != string(received.envelope.SenderSigningKeyID) ||
		!bytes.Equal(key.PublicKey, request.Sender.PublicKey) {
		return nil, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeKeyUnavailable, nil,
			fmt.Errorf("active sender key does not match delivery request"),
		)
	}
	if err := verifyEnvelope(key.PublicKey, received.envelope); err != nil {
		return nil, newFederationDeliveryError(federation.DeliveryBadRequest, "", nil, err)
	}

	now := s.clock.Now().UTC()
	if err := validateFederationTimes(received.envelope, now); err != nil {
		kind := federation.DeliveryBadRequest
		code := protocol.ErrorCode("")
		if !now.Before(received.envelope.ExpiresAt) {
			kind = federation.DeliveryConflict
			code = protocol.ErrorCodeExpired
		}
		return nil, newFederationDeliveryError(kind, code, nil, err)
	}
	peer, err := s.federationRegistry.Peer(ctx, s.db, authenticatedPeer)
	if err != nil {
		return nil, fmt.Errorf("read federation peer: %w", err)
	}
	if peer.Status != registry.PeerStatusTrusted {
		return nil, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeKeyUnavailable, nil,
			fmt.Errorf("peer is not trusted: %w", registry.ErrPeerNotTrusted),
		)
	}
	for _, recipient := range received.channelTo {
		evaluation := policy.Evaluate(
			s.policy, authenticatedPeer, peer.OrganizationID,
			policy.ActionDeliverMessage, recipient.Agent,
		)
		if evaluation.Decision == policy.DecisionDeny {
			denied, receiptErr := s.deliveryReceipt(
				received, federation.DeliveryReceiptControlDenied, now,
			)
			if receiptErr != nil {
				return nil, receiptErr
			}
			return nil, newFederationDeliveryError(
				federation.DeliveryForbidden, protocol.ErrorCodePolicyDenied, denied,
				fmt.Errorf("federation policy denied recipient %s", recipient.Agent),
			)
		}
	}

	storedReceipt, err := s.deliveryReceipt(received, federation.DeliveryReceiptStoredRemote, now)
	if err != nil {
		return nil, err
	}
	deniedReceipt, err := s.deliveryReceipt(received, federation.DeliveryReceiptControlDenied, now)
	if err != nil {
		return nil, err
	}
	return s.storeReceivedDelivery(ctx, authenticatedPeer, received, storedReceipt, deniedReceipt, now)
}

func (s *Service) bindReceivedDelivery(
	authenticatedPeer protocol.NodeID,
	request federation.DeliveryRequest,
) (receivedDelivery, error) {
	if !authenticatedPeer.Valid() {
		return receivedDelivery{}, newFederationDeliveryError(
			federation.DeliveryForbidden, "", nil, fmt.Errorf("authenticated peer is invalid"),
		)
	}
	envelope, exact, err := request.DecodeEnvelope()
	if err != nil {
		return receivedDelivery{}, newFederationDeliveryError(federation.DeliveryBadRequest, "", nil, err)
	}
	if !request.ThreadID.Valid() || len(request.Nonce) != 16 || request.Sequence < 1 {
		return receivedDelivery{}, newFederationDeliveryError(
			federation.DeliveryBadRequest, "", nil,
			fmt.Errorf("delivery request replay identity is invalid"),
		)
	}
	requestDigest, err := receivedRequestDigest(request)
	if err != nil {
		return receivedDelivery{}, newFederationDeliveryError(federation.DeliveryBadRequest, "", nil, err)
	}
	envelopeDigest, err := request.EnvelopeSHA256()
	if err != nil {
		return receivedDelivery{}, newFederationDeliveryError(federation.DeliveryBadRequest, "", nil, err)
	}
	if envelope.OriginNode != authenticatedPeer {
		return receivedDelivery{}, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeNotFound, nil,
			fmt.Errorf("envelope origin does not match authenticated peer"),
		)
	}
	if envelope.ParentMessageID != "" {
		return receivedDelivery{}, newFederationDeliveryError(
			federation.DeliveryConflict, protocol.ErrorCodeControlVersionConflict, nil,
			fmt.Errorf("federated loop delivery is not supported"),
		)
	}
	routeRecipients := make([]MessageRecipient, len(envelope.RecipientAgents))
	for i, recipient := range envelope.RecipientAgents {
		routeRecipients[i].Agent = recipient
	}
	if envelope.HopCount != 0 ||
		envelope.RouteDigest != localRouteDigest(
			envelope.MessageID, envelope.OriginNode, envelope.OriginAgent, routeRecipients,
		) {
		return receivedDelivery{}, newFederationDeliveryError(
			federation.DeliveryForbidden, protocol.ErrorCodeNotFound, nil,
			fmt.Errorf("federated root route metadata conflicts"),
		)
	}
	return receivedDelivery{
		request: request, envelope: envelope, exactEnvelope: exact,
		requestDigest: requestDigest, envelopeDigest: envelopeDigest,
	}, nil
}

func receivedRequestDigest(request federation.DeliveryRequest) (protocol.SHA256Hex, error) {
	canonical, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("encode federation delivery replay identity: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return protocol.SHA256Hex(hex.EncodeToString(digest[:])), nil
}

func (s *Service) gateFederatedRecipients(
	ctx context.Context,
	recipients []MessageRecipient,
) ([]channel.Recipient, error) {
	answer := make([]channel.Recipient, 0, len(recipients))
	for _, recipient := range recipients {
		blocked, err := s.federationRegistry.Blocked(ctx, s.db, recipient.Agent)
		if err != nil {
			return nil, fmt.Errorf("read federated recipient key state: %w", err)
		}
		if blocked {
			return nil, domainError(protocol.ErrorCodeKeyUnavailable, registry.ErrBlocked)
		}
		var node protocol.NodeID
		err = s.db.QueryRowContext(ctx,
			"SELECT node_id FROM agents WHERE agent_id = ?", recipient.Agent,
		).Scan(&node)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domainError(
				protocol.ErrorCodeNotFound, fmt.Errorf("recipient %s is unknown", recipient.Agent),
			)
		}
		if err != nil {
			return nil, fmt.Errorf("select federated recipient: %w", err)
		}
		if node != s.nodeID {
			return nil, domainError(
				protocol.ErrorCodeNotFound, fmt.Errorf("recipient %s is not local", recipient.Agent),
			)
		}
		var deliverable bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM sessions WHERE agent_id = ? AND state = 'CONNECTED'
			AND deliverable = 1 AND ended_at IS NULL)`, recipient.Agent).Scan(&deliverable); err != nil {
			return nil, fmt.Errorf("select federated recipient deliverability: %w", err)
		}
		if !deliverable {
			return nil, domainError(
				protocol.ErrorCodeRecipientNotDeliverable,
				fmt.Errorf("recipient %s is not deliverable", recipient.Agent),
			)
		}
		answer = append(answer, channel.Recipient{
			Agent: recipient.Agent, Deliverable: true, Policy: channel.PolicyAllowed,
		})
	}
	return answer, nil
}

func (s *Service) deliveryReceipt(
	received receivedDelivery,
	status federation.DeliveryReceiptStatus,
	receivedAt time.Time,
) ([]byte, error) {
	receipt := federation.DeliveryReceipt{
		MessageID:          received.envelope.MessageID,
		OriginNode:         received.envelope.OriginNode,
		DestinationNode:    received.envelope.DestinationNode,
		RecipientAgents:    append([]protocol.AgentID(nil), received.envelope.RecipientAgents...),
		DeliveryGeneration: received.request.DeliveryGeneration,
		Nonce:              append([]byte(nil), received.request.Nonce...),
		Sequence:           received.request.Sequence,
		EnvelopeSHA256:     received.envelopeDigest,
		RequestSHA256:      received.requestDigest,
		Status:             status,
		ReceivedAt:         receivedAt.UTC(),
	}
	if err := receipt.Sign(s.federationRegistry); err != nil {
		return nil, fmt.Errorf("sign federation delivery receipt: %w", err)
	}
	raw, err := json.Marshal(receipt)
	if err != nil {
		return nil, fmt.Errorf("encode federation delivery receipt: %w", err)
	}
	return raw, nil
}

func (s *Service) storeReceivedDelivery(
	ctx context.Context,
	authenticatedPeer protocol.NodeID,
	received receivedDelivery,
	storedReceipt []byte,
	deniedReceipt []byte,
	receivedAt time.Time,
) ([]byte, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	replayed, found, err := lookupFederationReplay(ctx, tx, authenticatedPeer, received)
	if err != nil {
		if errors.Is(err, errFederationConflict) {
			return nil, s.deliveryConflict(err)
		}
		return nil, err
	}
	if found {
		return replayed, nil
	}
	if err := enforceFederationWindow(ctx, tx, authenticatedPeer, received.request.Sequence); err != nil {
		if errors.Is(err, errFederationConflict) {
			return nil, s.deliveryConflict(err)
		}
		return nil, err
	}
	message := storedMessage{
		messageID:       received.envelope.MessageID,
		clientMsgID:     received.envelope.ClientMsgID,
		originNode:      received.envelope.OriginNode,
		fromAgent:       received.envelope.OriginAgent,
		destinationNode: received.envelope.DestinationNode,
		threadRef:       received.envelope.ThreadRef,
		threadID:        received.request.ThreadID,
		priority:        received.envelope.Priority,
		state:           messageStatePending,
		createdAt:       eventstore.FormatTime(received.envelope.CreatedAt),
		expiresAt:       eventstore.FormatTime(received.envelope.ExpiresAt),
		hopCount:        received.envelope.HopCount,
		routeDigest:     received.envelope.RouteDigest,
		recipients:      append([]MessageRecipient(nil), received.recipients...),
	}
	accepted, err := s.engine.AcceptFederated(ctx, tx, channel.Message{
		ID: message.messageID, From: message.fromAgent, FromNode: message.originNode,
		To: received.channelTo, ToNode: message.destinationNode,
		ThreadID: message.threadID, HopCount: message.hopCount,
	}, received.request.OriginControl, func(callbackCtx context.Context, _ channel.Acceptance) error {
		return storeFederatedMessage(
			callbackCtx, tx, message, received.exactEnvelope, received.envelope, receivedAt,
		)
	})
	if err != nil {
		var denied *channel.DenyError
		switch {
		case errors.As(err, &denied):
			if err := commit(tx); err != nil {
				return nil, err
			}
			return nil, newFederationDeliveryError(
				federation.DeliveryForbidden, deliveryErrorCode(denied.Reason()), deniedReceipt, denied,
			)
		case errors.Is(err, channel.ErrInvalidQuery):
			return nil, s.deliveryConflict(err)
		case errors.Is(err, channel.ErrUnsupported):
			return nil, newFederationDeliveryError(federation.DeliveryBadRequest, "", nil, err)
		default:
			return nil, fmt.Errorf("accept federated delivery: %w", err)
		}
	}
	if accepted.State() == protocol.ControlStateAllowed {
		if err := s.enqueueDeliveries(ctx, tx, message); err != nil {
			return nil, err
		}
	}
	if err := insertFederationInbox(ctx, tx, authenticatedPeer, received, storedReceipt, receivedAt); err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return storedReceipt, nil
}

func storeFederatedMessage(
	ctx context.Context,
	tx *sql.Tx,
	message storedMessage,
	exactEnvelope []byte,
	envelope protocol.WireEnvelope,
	receivedAt time.Time,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, ?)`,
		message.messageID, message.clientMsgID, message.originNode, message.fromAgent,
		message.destinationNode, message.threadRef, message.threadID, message.priority,
		message.createdAt, message.expiresAt, eventstore.FormatTime(receivedAt),
		message.hopCount, message.routeDigest)
	if err != nil {
		return fmt.Errorf("insert federated message: %w", err)
	}
	for ordinal, recipient := range message.recipients {
		if _, err := tx.ExecContext(ctx, `INSERT INTO message_recipients
			(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
			VALUES (?, ?, ?, ?, ?, ?)`, message.messageID, recipient.Agent, ordinal,
			recipient.DeliveryMode, recipient.RequiresAck, message.createdAt); err != nil {
			return fmt.Errorf("insert federated recipient: %w", err)
		}
	}
	return insertEnvelope(ctx, tx, exactEnvelope, envelope, receivedAt)
}

func insertFederationInbox(
	ctx context.Context,
	tx *sql.Tx,
	authenticatedPeer protocol.NodeID,
	received receivedDelivery,
	receipt []byte,
	receivedAt time.Time,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO federation_inbox
		(origin_node, sequence, nonce, message_id, delivery_generation,
		 request_sha256, envelope_sha256, receipt, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		authenticatedPeer, received.request.Sequence, received.request.Nonce,
		received.envelope.MessageID, received.request.DeliveryGeneration,
		received.requestDigest, received.envelopeDigest, receipt,
		eventstore.FormatTime(receivedAt))
	if err != nil {
		return fmt.Errorf("insert federation inbox receipt: %w", err)
	}
	return nil
}

func lookupFederationReplay(
	ctx context.Context,
	q registry.Querier,
	authenticatedPeer protocol.NodeID,
	received receivedDelivery,
) ([]byte, bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT sequence, nonce, message_id, delivery_generation,
		request_sha256, envelope_sha256, receipt FROM federation_inbox
		WHERE origin_node = ? AND (message_id = ? OR sequence = ? OR nonce = ?)`,
		authenticatedPeer, received.envelope.MessageID, received.request.Sequence, received.request.Nonce)
	if err != nil {
		return nil, false, fmt.Errorf("select federation replay: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var replay []byte
	found := false
	conflict := false
	for rows.Next() {
		var sequence, generation int64
		var nonce, receipt []byte
		var messageID protocol.ULID
		var requestDigest, envelopeDigest protocol.SHA256Hex
		if err := rows.Scan(
			&sequence, &nonce, &messageID, &generation,
			&requestDigest, &envelopeDigest, &receipt,
		); err != nil {
			return nil, false, fmt.Errorf("scan federation replay: %w", err)
		}
		exact := sequence == received.request.Sequence && bytes.Equal(nonce, received.request.Nonce) &&
			messageID == received.envelope.MessageID && generation == received.request.DeliveryGeneration &&
			requestDigest == received.requestDigest && envelopeDigest == received.envelopeDigest
		if exact {
			found = true
			replay = append([]byte(nil), receipt...)
		} else {
			conflict = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read federation replay: %w", err)
	}
	if conflict {
		return nil, false, errFederationConflict
	}
	return replay, found, nil
}

func enforceFederationWindow(
	ctx context.Context,
	q registry.Querier,
	authenticatedPeer protocol.NodeID,
	sequence int64,
) error {
	var highest sql.NullInt64
	if err := q.QueryRowContext(ctx,
		"SELECT MAX(sequence) FROM federation_inbox WHERE origin_node = ?", authenticatedPeer,
	).Scan(&highest); err != nil {
		return fmt.Errorf("select federation sequence window: %w", err)
	}
	if highest.Valid && highest.Int64 > federationSequenceWindow &&
		sequence < highest.Int64-federationSequenceWindow {
		return errFederationConflict
	}
	return nil
}

func validateFederationTimes(envelope protocol.WireEnvelope, now time.Time) error {
	if !envelope.ExpiresAt.After(envelope.CreatedAt) ||
		envelope.CreatedAt.After(now.Add(federationClockTolerance)) ||
		envelope.ExpiresAt.Before(now.Add(-federationClockTolerance)) ||
		!now.Before(envelope.ExpiresAt) {
		return fmt.Errorf("federation envelope time window is invalid")
	}
	return nil
}

func registryDeliveryRejection(err error) bool {
	return errors.Is(err, registry.ErrInvalidArgument) ||
		errors.Is(err, registry.ErrNoUsableKey) ||
		errors.Is(err, registry.ErrPeerNotFound) ||
		errors.Is(err, registry.ErrPeerNotTrusted) ||
		errors.Is(err, registry.ErrPeerRevoked) ||
		errors.Is(err, registry.ErrAgentConflict) ||
		errors.Is(err, registry.ErrKeyConflict)
}

func (s *Service) deliveryConflict(cause error) error {
	return newFederationDeliveryError(
		federation.DeliveryConflict, protocol.ErrorCodeControlVersionConflict, nil, cause,
	)
}

func newFederationDeliveryError(
	kind federation.DeliveryFailure,
	code protocol.ErrorCode,
	receipt []byte,
	cause error,
) error {
	return fmt.Errorf(
		"receive federated delivery: %w",
		federation.NewDeliveryError(kind, code, receipt, cause),
	)
}
