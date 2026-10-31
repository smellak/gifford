package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	messageStateAdmitted        = "ADMITTED"
	messageStatePending         = "PENDING"
	messageStateQueuedRemote    = "QUEUED_REMOTE"
	messageStateStoredRemote    = "STORED_REMOTE"
	messageStateAcknowledged    = "ACKNOWLEDGED"
	messageStateFailedPermanent = "FAILED_PERMANENT"
	messageStateFailed          = "FAILED"
	messageStateExpired         = "EXPIRED"
)

type storedAdmission struct {
	messageID         protocol.ULID
	originNode        protocol.NodeID
	destinationNode   protocol.NodeID
	priority          protocol.Priority
	createdAt         string
	expiresAt         string
	threadRef         protocol.ThreadRef
	threadSequence    int64
	threadID          protocol.ThreadID
	parentID          protocol.ULID
	requestedAutonomy protocol.Autonomy
	requestedBudget   string
	hopCount          int64
	routeDigest       protocol.Digest
	state             string
	reason            protocol.DecisionReason
	result            protocol.AdmissionResult
	recipients        []MessageRecipient
}

// AdmitMessage implements the sender-side A1-A8 matrix.
func (s *Service) AdmitMessage(
	ctx context.Context,
	sender protocol.AgentID,
	raw json.RawMessage,
) (any, error) {
	var params AdmitMessageParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := params.validate(sender); err != nil {
		return nil, invalidParams(err)
	}
	if params.OriginNode == "" {
		params.OriginNode = s.nodeID
	}
	if params.OriginNode != s.nodeID {
		return nil, domainError(protocol.ErrorCodeNotFound, fmt.Errorf("local route: %w", errInvalidArgument))
	}
	if params.ParentMessageID != "" && params.DestinationNode != "" && params.DestinationNode != s.nodeID {
		return nil, domainError(protocol.ErrorCodeControlVersionConflict, channel.ErrUnsupported)
	}
	if !s.clock.Now().Before(params.ExpiresAt) {
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	if _, err := s.requireSenderKey(ctx, tx, sender); err != nil {
		return nil, err
	}
	previous, found, err := loadAdmissionRetry(ctx, tx, s.nodeID, sender, params.ClientMsgID)
	if err != nil {
		return nil, err
	}
	if found {
		return answerAdmissionAttempt(params, previous, s.nodeID)
	}
	gated, err := s.gateRecipients(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	if params.DestinationNode != "" && params.DestinationNode != gated.destination {
		return nil, domainError(protocol.ErrorCodeRecipientNotDeliverable,
			fmt.Errorf("destination does not match recipient route"))
	}
	params.DestinationNode = gated.destination
	if params.ParentMessageID != "" {
		if params.DestinationNode != s.nodeID {
			return nil, domainError(protocol.ErrorCodeControlVersionConflict, channel.ErrUnsupported)
		}
		if err := s.bindReplyAdmission(ctx, tx, sender, &params); err != nil {
			return nil, err
		}
	}
	if err := s.prepareFreshAdmission(ctx, tx, &params, sender); err != nil {
		return nil, err
	}
	message := channel.Message{
		ID: params.MessageID, From: sender, FromNode: params.OriginNode,
		To: gated.recipients, ToNode: params.DestinationNode,
		ParentID: params.ParentMessageID, ThreadID: params.ThreadID, HopCount: params.HopCount,
	}
	var admitted protocol.AdmissionResult
	if params.ParentMessageID == "" {
		_, admitted, err = s.engine.Admit(ctx, tx, message, func(ctx context.Context, result protocol.AdmissionResult) error {
			return storeAdmission(ctx, tx, sender, params, result, eventstore.FormatTime(s.clock.Now()))
		})
	} else {
		admitted, err = s.engine.AdmitReply(ctx, tx, message,
			func(ctx context.Context, result protocol.AdmissionResult) error {
				previousDigest, digestErr := replyRoutePredecessor(ctx, tx, params.ParentMessageID, result)
				if digestErr != nil {
					return digestErr
				}
				params.RouteDigest, digestErr = localRouteDigestV1(previousDigest, params.OriginNode, sender,
					params.DestinationNode, params.Recipients)
				if digestErr != nil {
					return digestErr
				}
				return storeAdmission(ctx, tx, sender, params, result, eventstore.FormatTime(s.clock.Now()))
			},
			func(ctx context.Context, events []channel.ControlEvent) error {
				return s.appendChannelEvents(ctx, tx, events)
			})
	}
	if err != nil {
		var denied *channel.DenyError
		switch {
		case errors.As(err, &denied):
			if commitErr := commit(tx); commitErr != nil {
				return nil, commitErr
			}
			return nil, domainError(denied.Code(), denied)
		case errors.Is(err, channel.ErrUnsupported), errors.Is(err, channel.ErrInvalidQuery):
			return nil, invalidParams(err)
		case errors.Is(err, channel.ErrVersionConflict):
			return nil, domainError(protocol.ErrorCodeControlVersionConflict, err)
		default:
			return nil, fmt.Errorf("admit message: %w", err)
		}
	}
	if err := commit(tx); err != nil {
		return nil, err
	}
	return admissionAnswer(params, admitted, false), nil
}

func answerAdmissionAttempt(
	params AdmitMessageParams,
	previous storedAdmission,
	localNode protocol.NodeID,
) (any, error) {
	if params.DestinationNode == "" {
		params.DestinationNode = previous.destinationNode
	}
	if params.ParentMessageID != "" && previous.destinationNode != localNode {
		return nil, domainError(protocol.ErrorCodeControlVersionConflict, channel.ErrUnsupported)
	}
	if err := sameAdmissionParams(params, previous); err != nil {
		return nil, invalidParams(err)
	}
	return answerAdmissionRetry(previous)
}

func (s *Service) prepareFreshAdmission(
	ctx context.Context,
	tx *sql.Tx,
	params *AdmitMessageParams,
	sender protocol.AgentID,
) error {
	now := s.clock.Now()
	id, err := mintULID(now)
	if err != nil {
		return fmt.Errorf("mint message id: %w", err)
	}
	params.MessageID = id
	if params.ParentMessageID == "" {
		if err := s.bindRootThread(ctx, tx, params); err != nil {
			return err
		}
	} else if !params.ThreadRef.Valid() || params.threadSequence < 1 {
		return fmt.Errorf("prepared reply thread boundary: %w", errInvalidArgument)
	}
	params.HopCount = 0
	if params.ParentMessageID == "" {
		if params.DestinationNode == s.nodeID {
			params.RouteDigest, err = localRouteDigestV1("", params.OriginNode, sender,
				params.DestinationNode, params.Recipients)
			if err != nil {
				return err
			}
		} else {
			params.RouteDigest = localRouteDigest(params.MessageID, s.nodeID, sender, params.Recipients)
		}
	}
	return nil
}

func (s *Service) bindRootThread(
	ctx context.Context,
	tx *sql.Tx,
	params *AdmitMessageParams,
) error {
	var threadRef protocol.ThreadRef
	err := tx.QueryRowContext(ctx, `SELECT thread_ref FROM messages
		WHERE thread_id = ?
		ORDER BY CASE WHEN thread_sequence IS NULL THEN 1 ELSE 0 END,
		         thread_sequence DESC, created_at DESC, rowid DESC
		LIMIT 1`, params.ThreadID).Scan(&threadRef)
	if errors.Is(err, sql.ErrNoRows) {
		id, mintErr := mintULID(s.clock.Now())
		if mintErr != nil {
			return fmt.Errorf("mint thread reference: %w", mintErr)
		}
		threadRef = protocol.ThreadRef("thr_" + string(id))
	} else if err != nil {
		return fmt.Errorf("select thread reference: %w", err)
	}
	if !threadRef.Valid() {
		return fmt.Errorf("stored thread reference: %w", errInvalidArgument)
	}
	sequence, err := nextThreadSequence(ctx, tx, threadRef)
	if err != nil {
		return err
	}
	params.ThreadRef = threadRef
	params.threadSequence = sequence
	return nil
}

func (s *Service) bindReplyAdmission(
	ctx context.Context,
	tx *sql.Tx,
	sender protocol.AgentID,
	params *AdmitMessageParams,
) error {
	if params.RequestedAutonomy != "" || params.RequestedBudget != nil {
		return invalidParams(fmt.Errorf("a reply cannot change loop autonomy or budget: %w", errInvalidArgument))
	}
	var threadRef protocol.ThreadRef
	var threadID protocol.ThreadID
	var parentSender protocol.AgentID
	var senderWasRecipient bool
	err := tx.QueryRowContext(ctx, `SELECT m.thread_ref, m.thread_id, m.from_agent,
		EXISTS(SELECT 1 FROM message_recipients mr
		       WHERE mr.message_id = m.message_id AND mr.agent_id = ?)
		FROM messages m WHERE m.message_id = ?`, sender, params.ParentMessageID).
		Scan(&threadRef, &threadID, &parentSender, &senderWasRecipient)
	if errors.Is(err, sql.ErrNoRows) {
		return invalidParams(fmt.Errorf("reply parent not found: %w", errInvalidArgument))
	}
	if err != nil {
		return fmt.Errorf("select reply parent route: %w", err)
	}
	if !senderWasRecipient || len(params.Recipients) != 1 || params.Recipients[0].Agent != parentSender {
		return invalidParams(fmt.Errorf("reply endpoints do not reverse the parent route: %w", errInvalidArgument))
	}
	if params.ThreadID != threadID || !threadID.Valid() || !threadRef.Valid() {
		return invalidParams(fmt.Errorf("reply thread differs from parent: %w", errInvalidArgument))
	}
	if params.DestinationNode != "" && params.DestinationNode != s.nodeID {
		return domainError(protocol.ErrorCodeControlVersionConflict, channel.ErrUnsupported)
	}
	sequence, err := nextThreadSequence(ctx, tx, threadRef)
	if err != nil {
		return err
	}
	params.DestinationNode = s.nodeID
	params.ThreadRef = threadRef
	params.threadSequence = sequence
	return nil
}

func nextThreadSequence(
	ctx context.Context,
	tx *sql.Tx,
	threadRef protocol.ThreadRef,
) (int64, error) {
	var sequence int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(thread_sequence), 0) + 1
		FROM messages WHERE thread_ref = ? AND thread_sequence IS NOT NULL`, threadRef).
		Scan(&sequence); err != nil {
		return 0, fmt.Errorf("reserve thread sequence: %w", err)
	}
	if sequence < 1 {
		return 0, fmt.Errorf("reserve thread sequence overflow: %w", errInvalidArgument)
	}
	return sequence, nil
}

func replyRoutePredecessor(
	ctx context.Context,
	tx *sql.Tx,
	parentID protocol.ULID,
	result protocol.AdmissionResult,
) (protocol.Digest, error) {
	var digest protocol.Digest
	if result.LoopID != "" && result.LoopGeneration != nil {
		err := tx.QueryRowContext(ctx, `SELECT route_digest FROM messages
			WHERE loop_id = ? AND loop_generation = ? AND thread_sequence IS NOT NULL
			ORDER BY thread_sequence DESC LIMIT 1`, result.LoopID, *result.LoopGeneration).Scan(&digest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("select prior loop route: %w", err)
		}
	}
	if digest == "" {
		if err := tx.QueryRowContext(ctx, "SELECT route_digest FROM messages WHERE message_id = ?", parentID).
			Scan(&digest); err != nil {
			return "", fmt.Errorf("select parent route: %w", err)
		}
	}
	if !digest.Valid() {
		return "", fmt.Errorf("stored predecessor route digest: %w", errInvalidArgument)
	}
	return digest, nil
}

func (s *Service) appendChannelEvents(
	ctx context.Context,
	tx *sql.Tx,
	events []channel.ControlEvent,
) error {
	if len(events) == 0 {
		return nil
	}
	if s.eventStore == nil {
		return fmt.Errorf("append channel events: event store is required")
	}
	sequence, err := s.eventStore.NextEventSequenceTx(ctx, tx, "")
	if err != nil {
		return fmt.Errorf("read channel event sequence: %w", err)
	}
	now := s.clock.Now().UTC()
	envelopes := make([]protocol.EventEnvelope, 0, len(events))
	for _, source := range events {
		id, err := mintULID(now)
		if err != nil {
			return fmt.Errorf("mint channel event id: %w", err)
		}
		payload, err := json.Marshal(source.Payload)
		if err != nil {
			return fmt.Errorf("encode channel event payload: %w", err)
		}
		envelopes = append(envelopes, protocol.EventEnvelope{
			EventID: id, SchemaVersion: protocol.SchemaVersion, NodeID: s.nodeID,
			SourceQuality: protocol.SourceQualityInternal, EventType: source.Type,
			Origin: protocol.OriginSystem, WallTime: now, Sequence: sequence,
			Payload: protocol.Payload(payload), Sensitivity: protocol.SensitivityNormal,
		})
		sequence++
	}
	if err := s.eventStore.AppendEventsTx(ctx, tx, envelopes); err != nil {
		return fmt.Errorf("append channel events: %w", err)
	}
	return nil
}

func admissionAnswer(
	params AdmitMessageParams,
	result protocol.AdmissionResult,
	submitted bool,
) AdmitMessageResult {
	return AdmitMessageResult{
		AdmissionResult: result,
		MessageID:       result.MessageID,
		OriginNode:      params.OriginNode,
		DestinationNode: params.DestinationNode,
		ThreadRef:       params.ThreadRef,
		ThreadSequence:  params.threadSequence,
		CreatedAt:       eventstore.FormatTime(params.CreatedAt),
		ExpiresAt:       eventstore.FormatTime(params.ExpiresAt),
		HopCount:        params.HopCount,
		RouteDigest:     params.RouteDigest,
		Submitted:       submitted,
	}
}

func storeAdmission(
	ctx context.Context,
	tx *sql.Tx,
	sender protocol.AgentID,
	params AdmitMessageParams,
	result protocol.AdmissionResult,
	updatedAt string,
) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode admission result: %w", err)
	}
	var budget any
	if params.RequestedBudget != nil {
		raw, marshalErr := json.Marshal(params.RequestedBudget)
		if marshalErr != nil {
			return fmt.Errorf("encode requested budget: %w", marshalErr)
		}
		budget = string(raw)
	}
	stamp := eventstore.FormatTime(params.CreatedAt)
	_, err = tx.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, thread_id, priority, state, requested_autonomy,
		 requested_budget_json, created_at, expires_at, updated_at,
		 parent_message_id, thread_sequence, loop_id, loop_generation, turn_index,
		 hop_count, route_digest, admission_result_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'ADMITTED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		params.MessageID, params.ClientMsgID, params.OriginNode, sender, params.DestinationNode,
		params.ThreadRef, params.ThreadID, params.Priority, nullableString(string(params.RequestedAutonomy)),
		budget, stamp, eventstore.FormatTime(params.ExpiresAt), updatedAt,
		nullableString(string(params.ParentMessageID)), params.threadSequence,
		nullableString(string(result.LoopID)), result.LoopGeneration, result.TurnIndex,
		params.HopCount, params.RouteDigest, string(resultJSON))
	if err != nil {
		return fmt.Errorf("insert admitted message: %w", err)
	}
	for ordinal, recipient := range params.Recipients {
		if _, err := tx.ExecContext(ctx, `INSERT INTO message_recipients
			(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
			VALUES (?, ?, ?, ?, ?, ?)`, params.MessageID, recipient.Agent, ordinal,
			recipient.DeliveryMode, recipient.RequiresAck, stamp); err != nil {
			return fmt.Errorf("insert message recipient: %w", err)
		}
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullablePositive(value int64) any {
	if value < 1 {
		return nil
	}
	return value
}

func loadAdmissionRetry(
	ctx context.Context,
	tx *sql.Tx,
	node protocol.NodeID,
	sender protocol.AgentID,
	clientID protocol.OpaqueID,
) (storedAdmission, bool, error) {
	var found storedAdmission
	var rawResult, threadID, autonomy, budget, parentID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT message_id, origin_node, destination_node, priority,
		created_at, expires_at, thread_ref, COALESCE(thread_sequence, 1), thread_id, parent_message_id, requested_autonomy,
		requested_budget_json, hop_count, route_digest, state, control_reason,
		admission_result_json FROM messages
		WHERE origin_node = ? AND from_agent = ? AND client_msg_id = ?`,
		node, sender, clientID).Scan(&found.messageID, &found.originNode, &found.destinationNode,
		&found.priority, &found.createdAt, &found.expiresAt, &found.threadRef, &found.threadSequence,
		&threadID, &parentID, &autonomy, &budget, &found.hopCount, &found.routeDigest,
		&found.state, &found.reason, &rawResult)
	if errors.Is(err, sql.ErrNoRows) {
		return storedAdmission{}, false, nil
	}
	if err != nil {
		return storedAdmission{}, false, fmt.Errorf("select admission retry: %w", err)
	}
	if !rawResult.Valid || json.Unmarshal([]byte(rawResult.String), &found.result) != nil {
		return storedAdmission{}, false, fmt.Errorf("stored admission result: %w", errInvalidArgument)
	}
	found.threadID = protocol.ThreadID(threadID.String)
	found.parentID = protocol.ULID(parentID.String)
	found.requestedAutonomy = protocol.Autonomy(autonomy.String)
	found.requestedBudget = budget.String
	rows, err := tx.QueryContext(ctx, `SELECT agent_id, delivery_mode, requires_ack
		FROM message_recipients WHERE message_id = ? ORDER BY ordinal`, found.messageID)
	if err != nil {
		return storedAdmission{}, false, fmt.Errorf("select admission recipients: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var recipient MessageRecipient
		if err := rows.Scan(&recipient.Agent, &recipient.DeliveryMode, &recipient.RequiresAck); err != nil {
			return storedAdmission{}, false, fmt.Errorf("scan admission recipient: %w", err)
		}
		found.recipients = append(found.recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return storedAdmission{}, false, fmt.Errorf("read admission recipients: %w", err)
	}
	return found, true, nil
}

func sameAdmissionParams(params AdmitMessageParams, stored storedAdmission) error {
	var requestedBudget string
	if params.RequestedBudget != nil {
		raw, err := json.Marshal(params.RequestedBudget)
		if err != nil {
			return fmt.Errorf("encode requested budget: %w", err)
		}
		requestedBudget = string(raw)
	}
	if params.OriginNode != stored.originNode || params.DestinationNode != stored.destinationNode ||
		params.Priority != stored.priority || eventstore.FormatTime(params.CreatedAt) != stored.createdAt ||
		eventstore.FormatTime(params.ExpiresAt) != stored.expiresAt || params.ThreadID != stored.threadID ||
		params.ParentMessageID != stored.parentID ||
		params.RequestedAutonomy != stored.requestedAutonomy || requestedBudget != stored.requestedBudget ||
		len(params.Recipients) != len(stored.recipients) {
		return fmt.Errorf("client_msg_id binds other parameters: %w", errInvalidArgument)
	}
	for i := range params.Recipients {
		if params.Recipients[i] != stored.recipients[i] {
			return fmt.Errorf("client_msg_id binds other recipients: %w", errInvalidArgument)
		}
	}
	return nil
}

func answerAdmissionRetry(stored storedAdmission) (any, error) {
	switch stored.state {
	case messageStateExpired:
		return nil, domainError(protocol.ErrorCodeExpired, fmt.Errorf("message expired"))
	case messageStateFailed:
		code := protocol.ErrorCode(stored.reason)
		if !code.Valid() {
			return nil, fmt.Errorf("stored denial reason %q: %w", stored.reason, errInvalidArgument)
		}
		return nil, domainError(code, fmt.Errorf("message denied"))
	default:
		return AdmitMessageResult{
			AdmissionResult: stored.result,
			MessageID:       stored.messageID,
			OriginNode:      stored.originNode,
			DestinationNode: stored.destinationNode,
			ThreadRef:       stored.threadRef,
			ThreadSequence:  stored.threadSequence,
			CreatedAt:       stored.createdAt,
			ExpiresAt:       stored.expiresAt,
			HopCount:        stored.hopCount,
			RouteDigest:     stored.routeDigest,
			Submitted:       stored.state != messageStateAdmitted,
		}, nil
	}
}
