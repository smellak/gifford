package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	// MethodBridgeMirrorMessage is routed only after the node socket dispatcher
	// authenticates the configured bridge UID; ordinary dispatch never routes it.
	MethodBridgeMirrorMessage = "bridge_mirror_message"
	// MethodBridgeMarkRead is the bridge-only first-read acknowledgement.
	MethodBridgeMarkRead = "bridge_mark_read"

	bridgeSourceNodeMax = 255
	bridgeContextMax    = 512
	// bridgeNoExpiry is only Gifford's queue sentinel. It is not projected
	// from the source hub and is never returned as source metadata.
	bridgeNoExpiry = "9999-12-31T23:59:59.999999999Z"
)

// BridgeHandler is the narrow capability offered exclusively to the bridge
// connection class by the node socket dispatcher.
type BridgeHandler interface {
	MirrorMessage(context.Context, json.RawMessage) (any, error)
	MarkRead(context.Context, json.RawMessage) (any, error)
}

// BridgeMirrorMessageParams is the complete immutable source projection.
type BridgeMirrorMessageParams struct {
	SourceNodeID    string           `json:"source_node_id"`
	SourceMessageID string           `json:"source_message_id"`
	FromAgent       protocol.AgentID `json:"from_agent"`
	ToAgent         protocol.AgentID `json:"to_agent"`
	SourceCreatedAt string           `json:"source_created_at"`
	SourceContextID *string          `json:"source_context_id"`
}

// BridgeDisposition distinguishes a new durable mirror from its exact retry.
type BridgeDisposition string

const (
	// BridgeDispositionCreated reports a newly committed mirror.
	BridgeDispositionCreated BridgeDisposition = "CREATED"
	// BridgeDispositionDuplicate reports an exact replay of a committed mirror.
	BridgeDispositionDuplicate BridgeDisposition = "DUPLICATE"
)

// BridgeMirrorMessageResult binds the source identity to stable Gifford IDs.
type BridgeMirrorMessageResult struct {
	GiffordMessageID protocol.ULID     `json:"gifford_message_id"`
	GiffordThreadID  protocol.ThreadID `json:"gifford_thread_id"`
	Disposition      BridgeDisposition `json:"disposition"`
}

// BridgeMarkReadParams carries the source's immutable first-read transition.
type BridgeMarkReadParams struct {
	SourceNodeID    string           `json:"source_node_id"`
	SourceMessageID string           `json:"source_message_id"`
	RecipientAgent  protocol.AgentID `json:"recipient_agent"`
	ReadAt          string           `json:"read_at"`
}

// BridgeMarkReadResult is stable across an identical retry.
type BridgeMarkReadResult struct {
	GiffordMessageID protocol.ULID    `json:"gifford_message_id"`
	Status           AckMessageStatus `json:"status"`
	ReadAt           string           `json:"read_at"`
}

type storedBridgeMirror struct {
	messageID       protocol.ULID
	threadID        protocol.ThreadID
	fromAgent       protocol.AgentID
	toAgent         protocol.AgentID
	sourceCreatedAt string
	sourceContextID sql.NullString
}

// MirrorMessage stores one metadata-only mirror and one notification
// delivery in a single transaction. The source hub remains content authority.
func (s *Service) MirrorMessage(ctx context.Context, raw json.RawMessage) (any, error) {
	var params BridgeMirrorMessageParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	if err := requireJSONField(raw, "source_context_id"); err != nil {
		return nil, invalidParams(err)
	}
	sourceMessageID, sourceCreatedAt, err := params.validate()
	if err != nil {
		return nil, invalidParams(err)
	}

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin bridge mirror: %w", err)
	}
	defer rollback(tx)
	stored, found, err := loadBridgeMirror(ctx, tx, params.SourceNodeID, sourceMessageID)
	if err != nil {
		return nil, err
	}
	if found {
		if !sameBridgeProjection(params, stored) {
			return nil, domainError(protocol.ErrorCodeControlVersionConflict,
				fmt.Errorf("source identity is already bound to another projection"))
		}
		return BridgeMirrorMessageResult{
			GiffordMessageID: stored.messageID,
			GiffordThreadID:  stored.threadID,
			Disposition:      BridgeDispositionDuplicate,
		}, nil
	}
	if err := s.requireBridgeAgents(ctx, tx, params.FromAgent, params.ToAgent); err != nil {
		return nil, err
	}

	messageID, err := mintULID(s.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("mint bridge message id: %w", err)
	}
	threadID, threadRef, err := s.bridgeThread(
		ctx, tx, params.SourceNodeID, params.SourceContextID,
	)
	if err != nil {
		return nil, err
	}
	threadSequence, err := nextThreadSequence(ctx, tx, threadRef)
	if err != nil {
		return nil, fmt.Errorf("allocate bridge thread sequence: %w", err)
	}
	recipients := []MessageRecipient{{
		Agent: params.ToAgent, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
	}}
	routeDigest, err := localRouteDigestV1(
		"", s.nodeID, params.FromAgent, s.nodeID, recipients,
	)
	if err != nil {
		return nil, fmt.Errorf("digest bridge route: %w", err)
	}
	if err := s.insertBridgeMirror(ctx, tx, params, sourceMessageID, sourceCreatedAt,
		messageID, threadID, threadRef, threadSequence, routeDigest); err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, fmt.Errorf("commit bridge mirror: %w", err)
	}
	s.wakeNotifications()
	return BridgeMirrorMessageResult{
		GiffordMessageID: messageID,
		GiffordThreadID:  threadID,
		Disposition:      BridgeDispositionCreated,
	}, nil
}

func (p BridgeMirrorMessageParams) validate() (int64, time.Time, error) {
	messageID, err := canonicalBridgeMessageID(p.SourceMessageID)
	if err != nil {
		return 0, time.Time{}, err
	}
	createdAt, err := canonicalBridgeTime(p.SourceCreatedAt)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("source_created_at: %w", err)
	}
	switch {
	case !validBridgeOpaque(p.SourceNodeID, bridgeSourceNodeMax):
		return 0, time.Time{}, fmt.Errorf("source_node_id: %w", errInvalidArgument)
	case !p.FromAgent.Valid():
		return 0, time.Time{}, fmt.Errorf("from_agent: %w", errInvalidArgument)
	case !p.ToAgent.Valid():
		return 0, time.Time{}, fmt.Errorf("to_agent: %w", errInvalidArgument)
	case p.SourceContextID != nil && !validBridgeOpaque(*p.SourceContextID, bridgeContextMax):
		return 0, time.Time{}, fmt.Errorf("source_context_id: %w", errInvalidArgument)
	}
	return messageID, createdAt, nil
}

func (s *Service) requireBridgeAgents(
	ctx context.Context,
	tx *sql.Tx,
	fromAgent protocol.AgentID,
	toAgent protocol.AgentID,
) error {
	fromRoute, err := s.bridgeAgentRoute(ctx, tx, fromAgent)
	if err != nil {
		return err
	}
	if fromRoute.Kind != registry.RouteLocal && fromRoute.Kind != registry.RouteFederated {
		return domainError(protocol.ErrorCodeNotFound, fmt.Errorf("sender is not registered"))
	}
	toRoute, err := s.bridgeAgentRoute(ctx, tx, toAgent)
	if err != nil {
		return err
	}
	if toRoute.Kind != registry.RouteLocal || toRoute.Node != s.nodeID {
		return domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("recipient is not registered on this node"))
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tmux_instances
		WHERE agent_id = ? AND state = 'ACTIVE')`,
		toAgent).Scan(&active); err != nil {
		return fmt.Errorf("select bridge recipient instance: %w", err)
	}
	if !active {
		return domainError(protocol.ErrorCodeRecipientNotDeliverable,
			fmt.Errorf("recipient has no active local instance"))
	}
	return nil
}

func (s *Service) bridgeAgentRoute(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
) (registry.RouteResult, error) {
	blocked, err := s.registry.Blocked(ctx, tx, agent)
	if err != nil {
		return registry.RouteResult{}, fmt.Errorf("check bridge agent: %w", err)
	}
	if blocked {
		return registry.RouteResult{}, domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("agent is blocked"))
	}
	route, err := s.registry.Route(ctx, tx, agent)
	if err != nil {
		return registry.RouteResult{}, fmt.Errorf("route bridge agent: %w", err)
	}
	if route.Kind == registry.RouteNotFound || route.Kind == registry.RoutePeerBlocked {
		return registry.RouteResult{}, domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("agent is not registered"))
	}
	return route, nil
}

func loadBridgeMirror(
	ctx context.Context,
	tx *sql.Tx,
	sourceNode string,
	sourceMessageID int64,
) (storedBridgeMirror, bool, error) {
	var stored storedBridgeMirror
	err := tx.QueryRowContext(ctx, `SELECT mapping.gifford_message_id, message.thread_id,
		mapping.from_agent, mapping.to_agent, mapping.source_created_at, mapping.source_context_id
		FROM bridge_message_mappings AS mapping
		JOIN messages AS message ON message.message_id = mapping.gifford_message_id
		WHERE mapping.source_node_id = ? AND mapping.source_message_id = ?`,
		sourceNode, sourceMessageID).Scan(
		&stored.messageID, &stored.threadID, &stored.fromAgent, &stored.toAgent,
		&stored.sourceCreatedAt, &stored.sourceContextID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storedBridgeMirror{}, false, nil
	}
	if err != nil {
		return storedBridgeMirror{}, false, fmt.Errorf("select bridge mirror: %w", err)
	}
	if !stored.messageID.Valid() || !stored.threadID.Valid() {
		return storedBridgeMirror{}, false, fmt.Errorf("stored bridge identifiers: %w", errInvalidArgument)
	}
	return stored, true, nil
}

func sameBridgeProjection(params BridgeMirrorMessageParams, stored storedBridgeMirror) bool {
	if params.FromAgent != stored.fromAgent || params.ToAgent != stored.toAgent ||
		params.SourceCreatedAt != stored.sourceCreatedAt {
		return false
	}
	if params.SourceContextID == nil {
		return !stored.sourceContextID.Valid
	}
	return stored.sourceContextID.Valid && *params.SourceContextID == stored.sourceContextID.String
}

func (s *Service) bridgeThread(
	ctx context.Context,
	tx *sql.Tx,
	sourceNode string,
	sourceContext *string,
) (protocol.ThreadID, protocol.ThreadRef, error) {
	if sourceContext != nil {
		var threadID protocol.ThreadID
		var threadRef protocol.ThreadRef
		err := tx.QueryRowContext(ctx, `SELECT gifford_thread_id, gifford_thread_ref
			FROM bridge_context_mappings WHERE source_node_id = ? AND source_context_id = ?`,
			sourceNode, *sourceContext).Scan(&threadID, &threadRef)
		if err == nil {
			if !threadID.Valid() || !threadRef.Valid() {
				return "", "", fmt.Errorf("stored bridge thread: %w", errInvalidArgument)
			}
			return threadID, threadRef, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return "", "", fmt.Errorf("select bridge context mapping: %w", err)
		}
	}
	id, err := mintULID(s.clock.Now())
	if err != nil {
		return "", "", fmt.Errorf("mint bridge thread: %w", err)
	}
	threadID := protocol.ThreadID(id)
	threadRef := protocol.ThreadRef("thr_" + string(id))
	if sourceContext != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO bridge_context_mappings
			(source_node_id, source_context_id, gifford_thread_id, gifford_thread_ref, created_at)
			VALUES (?, ?, ?, ?, ?)`, sourceNode, *sourceContext, threadID, threadRef,
			eventstore.FormatTime(s.clock.Now()))
		if err != nil {
			return "", "", fmt.Errorf("insert bridge context mapping: %w", err)
		}
	}
	return threadID, threadRef, nil
}

func (s *Service) insertBridgeMirror(
	ctx context.Context,
	tx *sql.Tx,
	params BridgeMirrorMessageParams,
	sourceMessageID int64,
	sourceCreatedAt time.Time,
	messageID protocol.ULID,
	threadID protocol.ThreadID,
	threadRef protocol.ThreadRef,
	threadSequence int64,
	routeDigest protocol.Digest,
) error {
	now := eventstore.FormatTime(s.clock.Now())
	createdAt := eventstore.FormatTime(sourceCreatedAt)
	expiresAt := bridgeNoExpiry
	_, err := tx.ExecContext(ctx, `INSERT INTO messages
		(message_id, client_msg_id, client_msg_namespace, origin_node, from_agent,
		 destination_node, thread_ref, thread_id, priority, state, created_at, expires_at,
		 updated_at, thread_sequence, hop_count, route_digest)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, ?, 0, ?)`,
		messageID, params.SourceMessageID, params.SourceNodeID, s.nodeID, params.FromAgent,
		s.nodeID, threadRef, threadID, protocol.PriorityNormal, createdAt, expiresAt, now,
		threadSequence, routeDigest)
	if err != nil {
		return fmt.Errorf("insert bridge message: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO message_recipients
		(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
		VALUES (?, ?, 0, 'WHEN_SAFE', 1, ?)`, messageID, params.ToAgent, createdAt)
	if err != nil {
		return fmt.Errorf("insert bridge recipient: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state, priority,
		 created_at, expires_at, updated_at, thread_sequence, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'QUEUED', 2, ?, ?, ?, ?, 0, ?)`,
		messageID, params.ToAgent, params.FromAgent, threadRef, createdAt, expiresAt, now,
		threadSequence, routeDigest)
	if err != nil {
		return fmt.Errorf("enqueue bridge notification: %w", err)
	}
	if err := s.engine.InitializeDelivery(ctx, tx, messageID, params.ToAgent, 1); err != nil {
		return fmt.Errorf("initialize bridge notification: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		 source_created_at, source_context_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		params.SourceNodeID, sourceMessageID, messageID, params.FromAgent, params.ToAgent,
		params.SourceCreatedAt, nullableBridgeContext(params.SourceContextID), now, now)
	if err != nil {
		return fmt.Errorf("insert bridge message mapping: %w", err)
	}
	return nil
}

// MarkRead records the source's first read and the corresponding local
// ACK atomically. It never creates a missing mirror or agent.
func (s *Service) MarkRead(ctx context.Context, raw json.RawMessage) (any, error) {
	var params BridgeMarkReadParams
	if err := decodeParams(raw, &params); err != nil {
		return nil, invalidParams(err)
	}
	sourceMessageID, readAt, err := params.validate()
	if err != nil {
		return nil, invalidParams(err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin bridge read: %w", err)
	}
	defer rollback(tx)

	var messageID protocol.ULID
	var toAgent protocol.AgentID
	var storedReadAt sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT gifford_message_id, to_agent, source_read_at
		FROM bridge_message_mappings WHERE source_node_id = ? AND source_message_id = ?`,
		params.SourceNodeID, sourceMessageID).Scan(&messageID, &toAgent, &storedReadAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domainError(protocol.ErrorCodeNotFound,
			fmt.Errorf("message mirror is not committed"))
	}
	if err != nil {
		return nil, fmt.Errorf("select bridge read mapping: %w", err)
	}
	if !messageID.Valid() {
		return nil, fmt.Errorf("stored bridge message id is invalid")
	}
	if params.RecipientAgent != toAgent {
		return nil, domainError(protocol.ErrorCodeControlVersionConflict,
			fmt.Errorf("recipient differs from the mirrored projection"))
	}
	if storedReadAt.Valid {
		if storedReadAt.String != params.ReadAt {
			return nil, domainError(protocol.ErrorCodeControlVersionConflict,
				fmt.Errorf("read_at is already fixed"))
		}
		return BridgeMarkReadResult{
			GiffordMessageID: messageID, Status: AckMessageAcknowledged, ReadAt: storedReadAt.String,
		}, nil
	}

	now := eventstore.FormatTime(s.clock.Now())
	result, err := tx.ExecContext(ctx, `UPDATE bridge_message_mappings
		SET source_read_at = ?, updated_at = ?
		WHERE source_node_id = ? AND source_message_id = ? AND source_read_at IS NULL`,
		params.ReadAt, now, params.SourceNodeID, sourceMessageID)
	if err != nil {
		return nil, fmt.Errorf("store bridge read: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("count stored bridge read: %w", err)
	}
	if changed != 1 {
		return nil, fmt.Errorf("store bridge read changed %d rows", changed)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO acks
		(message_id, agent_id, delivery_generation, status, acked_at)
		VALUES (?, ?, 1, 'ACKNOWLEDGED', ?)`,
		messageID, params.RecipientAgent, eventstore.FormatTime(readAt))
	if err != nil {
		return nil, fmt.Errorf("insert bridge acknowledgement: %w", err)
	}
	if err := s.closeAcknowledgedDelivery(
		ctx, tx, messageID, params.RecipientAgent, 1, readAt,
	); err != nil {
		return nil, err
	}
	if err := commit(tx); err != nil {
		return nil, fmt.Errorf("commit bridge read: %w", err)
	}
	return BridgeMarkReadResult{
		GiffordMessageID: messageID, Status: AckMessageAcknowledged, ReadAt: params.ReadAt,
	}, nil
}

func (p BridgeMarkReadParams) validate() (int64, time.Time, error) {
	messageID, err := canonicalBridgeMessageID(p.SourceMessageID)
	if err != nil {
		return 0, time.Time{}, err
	}
	readAt, err := canonicalBridgeTime(p.ReadAt)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("read_at: %w", err)
	}
	if !validBridgeOpaque(p.SourceNodeID, bridgeSourceNodeMax) {
		return 0, time.Time{}, fmt.Errorf("source_node_id: %w", errInvalidArgument)
	}
	if !p.RecipientAgent.Valid() {
		return 0, time.Time{}, fmt.Errorf("recipient_agent: %w", errInvalidArgument)
	}
	return messageID, readAt, nil
}

func canonicalBridgeMessageID(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != value {
		return 0, fmt.Errorf("source_message_id: %w", errInvalidArgument)
	}
	return parsed, nil
}

func canonicalBridgeTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.UTC().Format(time.RFC3339Nano) != value {
		return time.Time{}, errInvalidArgument
	}
	return parsed.UTC(), nil
}

func validBridgeOpaque(value string, maximum int) bool {
	if len(value) < 1 || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func requireJSONField(raw json.RawMessage, field string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return fmt.Errorf("decode bridge params object: %w", err)
	}
	if _, ok := object[field]; !ok {
		return fmt.Errorf("%s is required: %w", field, errInvalidArgument)
	}
	return nil
}

func nullableBridgeContext(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
