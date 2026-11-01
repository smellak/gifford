package proxies

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// CapabilityStore is the node's atomic consumer and lineage verifier for a
// run's ordered capability transitions.
type CapabilityStore struct {
	store  *eventstore.Store
	db     *sql.DB
	nodeID protocol.NodeID
}

// NewCapabilityStore binds capability transitions to one node event store.
func NewCapabilityStore(store *eventstore.Store, nodeID protocol.NodeID) (*CapabilityStore, error) {
	if store == nil || store.DB() == nil || !nodeID.Valid() {
		return nil, ErrInvalidOptions
	}
	return &CapabilityStore{store: store, db: store.DB(), nodeID: nodeID}, nil
}

type capabilitySession struct {
	agent       protocol.AgentID
	state       protocol.SessionState
	capture     protocol.CaptureMode
	deliverable bool
	manifest    protocol.CapabilityManifest
	manifestRaw string
	revision    string
	ended       bool
}

// ApplyCapabilitiesChanged applies the manifest CAS and appends its audit in
// the same immediate transaction. Exact replay is a read-only success.
func (s *CapabilityStore) ApplyCapabilitiesChanged(
	ctx context.Context,
	event protocol.EventEnvelope,
) error {
	if s == nil || s.store == nil || s.db == nil || ctx == nil {
		return agentlink.ErrCapabilitiesConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin capability transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	session, err := loadCapabilitySession(ctx, tx, event.SessionID)
	if err != nil {
		return err
	}
	before, after, err := s.validateTransition(event, session)
	if err != nil {
		return agentlink.ErrCapabilitiesConflict
	}
	stored, found, err := loadCapabilityEvent(ctx, tx, event.EventID)
	if err != nil {
		return err
	}
	if found {
		if equalCapabilityEvents(stored, event) {
			return nil
		}
		return agentlink.ErrCapabilitiesConflict
	}
	if session.manifest != before || session.state != protocol.SessionStateConnected ||
		session.ended || session.capture != protocol.CaptureModePTYProxy || !session.deliverable {
		return agentlink.ErrCapabilitiesConflict
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		return agentlink.ErrCapabilitiesConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE sessions SET manifest_json = ?
		WHERE session_id = ? AND agent_id = ? AND state = ? AND ended_at IS NULL
		AND capture_mode = ? AND deliverable = 1 AND manifest_json = ? AND updated_at = ?`,
		string(afterJSON), event.SessionID, event.AgentID, protocol.SessionStateConnected,
		protocol.CaptureModePTYProxy, session.manifestRaw, session.revision)
	if err != nil {
		return fmt.Errorf("update degraded capability manifest: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify degraded capability manifest: %w", err)
	}
	if changed != 1 {
		return agentlink.ErrCapabilitiesConflict
	}
	if err := s.store.AppendEventsTx(ctx, tx, []protocol.EventEnvelope{event}); err != nil {
		if errors.Is(err, eventstore.ErrDuplicateEvent) {
			return agentlink.ErrCapabilitiesConflict
		}
		return fmt.Errorf("append capability audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit capability transition: %w", err)
	}
	return nil
}

// MatchesCapabilitiesChanged reports whether immutable capability audits form
// a complete chain from the Proxy's original greeting to the stored manifest.
func (s *CapabilityStore) MatchesCapabilitiesChanged(
	ctx context.Context,
	q registry.Querier,
	sessionID protocol.OpaqueID,
	before protocol.CapabilityManifest,
	after protocol.CapabilityManifest,
) (bool, error) {
	if s == nil || ctx == nil || q == nil || !sessionID.Valid() ||
		before.Validate() != nil || after.Validate() != nil ||
		before.AdapterType == protocol.AdapterTypeNone {
		return false, nil
	}
	beforeIdentity := before
	afterIdentity := after
	beforeIdentity.Capabilities = protocol.Capabilities{}
	afterIdentity.Capabilities = protocol.Capabilities{}
	if beforeIdentity != afterIdentity {
		return false, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT event_id, sequence, wall_time, payload_json FROM events
		WHERE schema_version = ? AND node_id = ? AND session_id = ? AND agent_id = ?
		AND adapter_type = ? AND source_quality = ? AND event_type = ?
		AND direction IS NULL AND stream IS NULL AND origin = ?
		AND monotonic_ns = 0 AND sequence >= 0 AND turn_id IS NULL
		AND message_id IS NULL AND correlation_id IS NULL
		AND sensitivity = ? AND redaction_applied = 0 AND harness = ?
		AND harness_version = ? AND tool_call_id IS NULL AND delivery_id IS NULL
		AND decision_id IS NULL AND parent_event_id IS NULL ORDER BY sequence`,
		protocol.SchemaVersion, s.nodeID, sessionID, before.AgentID, before.AdapterType,
		protocol.SourceQualityProcessMetrics, protocol.EventTypeAdapterCapabilitiesChanged,
		protocol.OriginSystem, protocol.SensitivityNormal,
		before.Harness, before.HarnessVersion)
	if err != nil {
		return false, fmt.Errorf("select capability lineage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	cursor := before
	for rows.Next() {
		var eventID, wallTime, payload string
		var sequence int64
		if err := rows.Scan(&eventID, &sequence, &wallTime, &payload); err != nil {
			return false, fmt.Errorf("scan capability lineage: %w", err)
		}
		observed, parseErr := eventstore.ParseTime(wallTime)
		if !protocol.ULID(eventID).Valid() || sequence < 0 || parseErr != nil ||
			observed.IsZero() || observed != observed.UTC() {
			return false, nil
		}
		var change adapters.CapabilitiesChangedEvent
		if decodeCapabilityJSON([]byte(payload), &change) != nil {
			return false, nil
		}
		canonical, marshalErr := json.Marshal(change)
		if marshalErr != nil || !bytes.Equal(canonical, []byte(payload)) ||
			change.EventType != protocol.EventTypeAdapterCapabilitiesChanged ||
			change.Before != cursor.Capabilities {
			return false, nil
		}
		next := cursor
		next.Capabilities = change.After
		if !validManifestTransition(cursor, next) {
			return false, nil
		}
		cursor = next
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate capability lineage: %w", err)
	}
	return cursor == after, nil
}

func (s *CapabilityStore) validateTransition(
	event protocol.EventEnvelope,
	session capabilitySession,
) (
	protocol.CapabilityManifest,
	protocol.CapabilityManifest,
	error,
) {
	if err := event.Validate(); err != nil || event.NodeID != s.nodeID ||
		event.SessionID == "" || event.AgentID != session.agent ||
		event.AgentID != session.manifest.AgentID || event.Harness != session.manifest.Harness ||
		event.HarnessVersion != session.manifest.HarnessVersion ||
		event.AdapterType != session.manifest.AdapterType ||
		event.SourceQuality != protocol.SourceQualityProcessMetrics ||
		event.EventType != protocol.EventTypeAdapterCapabilitiesChanged ||
		event.Direction != "" || event.Stream != "" || event.Origin != protocol.OriginSystem ||
		event.WallTime.IsZero() || event.WallTime != event.WallTime.UTC() ||
		event.MonotonicNS != 0 || event.Sequence < 0 || event.TurnID != "" ||
		event.ToolCallID != "" || event.MessageID != "" || event.DeliveryID != "" ||
		event.DecisionID != "" || event.ParentEventID != "" || event.CorrelationID != "" ||
		event.Sensitivity != protocol.SensitivityNormal || event.RedactionApplied {
		return protocol.CapabilityManifest{}, protocol.CapabilityManifest{},
			errors.New("invalid capability envelope")
	}
	var change adapters.CapabilitiesChangedEvent
	if err := decodeCapabilityJSON(event.Payload, &change); err != nil {
		return protocol.CapabilityManifest{}, protocol.CapabilityManifest{}, err
	}
	canonical, err := json.Marshal(change)
	if err != nil || !bytes.Equal(canonical, event.Payload) {
		return protocol.CapabilityManifest{}, protocol.CapabilityManifest{},
			errors.New("non-canonical capability payload")
	}
	before := session.manifest
	before.Capabilities = change.Before
	after := session.manifest
	after.Capabilities = change.After
	if change.EventType != protocol.EventTypeAdapterCapabilitiesChanged ||
		!validManifestTransition(before, after) {
		return protocol.CapabilityManifest{}, protocol.CapabilityManifest{},
			errors.New("invalid capability transition")
	}
	return before, after, nil
}

func validManifestTransition(
	before protocol.CapabilityManifest,
	after protocol.CapabilityManifest,
) bool {
	return adapters.ValidCapabilitiesTransition(before, after)
}

func loadCapabilitySession(
	ctx context.Context,
	q registry.Querier,
	sessionID protocol.OpaqueID,
) (capabilitySession, error) {
	var session capabilitySession
	var state string
	var capture string
	var ended sql.NullString
	err := q.QueryRowContext(ctx, `SELECT agent_id, state, capture_mode, deliverable,
		manifest_json, updated_at, ended_at FROM sessions WHERE session_id = ?`, sessionID).
		Scan(&session.agent, &state, &capture, &session.deliverable, &session.manifestRaw,
			&session.revision, &ended)
	if errors.Is(err, sql.ErrNoRows) {
		return capabilitySession{}, agentlink.ErrCapabilitiesConflict
	}
	if err != nil {
		return capabilitySession{}, fmt.Errorf("select capability session: %w", err)
	}
	session.state = protocol.SessionState(state)
	session.capture = protocol.CaptureMode(capture)
	session.ended = ended.Valid
	if decodeCapabilityJSON([]byte(session.manifestRaw), &session.manifest) != nil ||
		session.manifest.Validate() != nil || !session.agent.Valid() || !session.state.Valid() ||
		!session.capture.Valid() || session.revision == "" ||
		session.manifest.AgentID != session.agent || session.manifest.CaptureMode != session.capture ||
		session.manifest.Deliverable != session.deliverable {
		return capabilitySession{}, agentlink.ErrCapabilitiesConflict
	}
	return session, nil
}

const selectCapabilityEvent = `SELECT event_id, schema_version, node_id, session_id, agent_id,
	adapter_type, source_quality, event_type, direction, stream, origin, wall_time,
	monotonic_ns, sequence, turn_id, message_id, correlation_id, payload_json,
	sensitivity, redaction_applied, harness, harness_version, tool_call_id,
	delivery_id, decision_id, parent_event_id FROM events WHERE event_id = ?`

func loadCapabilityEvent(
	ctx context.Context,
	q registry.Querier,
	eventID protocol.ULID,
) (protocol.EventEnvelope, bool, error) {
	row := q.QueryRowContext(ctx, selectCapabilityEvent, eventID)
	var event protocol.EventEnvelope
	var direction, stream, origin sql.NullString
	var wallTime, payload string
	var turnID, messageID, correlationID sql.NullString
	var redaction int64
	var harness, harnessVersion, toolCallID, deliveryID, decisionID, parentEventID sql.NullString
	err := row.Scan(
		&event.EventID, &event.SchemaVersion, &event.NodeID, &event.SessionID,
		&event.AgentID, &event.AdapterType, &event.SourceQuality, &event.EventType,
		&direction, &stream, &origin, &wallTime, &event.MonotonicNS, &event.Sequence,
		&turnID, &messageID, &correlationID, &payload, &event.Sensitivity, &redaction,
		&harness, &harnessVersion, &toolCallID, &deliveryID, &decisionID, &parentEventID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return protocol.EventEnvelope{}, false, nil
	}
	if err != nil {
		return protocol.EventEnvelope{}, false, fmt.Errorf("select capability event: %w", err)
	}
	parsed, err := eventstore.ParseTime(wallTime)
	if err != nil || redaction < 0 || redaction > 1 {
		return protocol.EventEnvelope{}, false, agentlink.ErrCapabilitiesConflict
	}
	event.Direction = protocol.Direction(direction.String)
	event.Stream = protocol.Stream(stream.String)
	event.Origin = protocol.Origin(origin.String)
	event.WallTime = parsed
	event.TurnID = protocol.OpaqueID(turnID.String)
	event.MessageID = protocol.ULID(messageID.String)
	event.CorrelationID = protocol.OpaqueID(correlationID.String)
	event.Payload = protocol.Payload(payload)
	event.RedactionApplied = redaction == 1
	event.Harness = protocol.Harness(harness.String)
	event.HarnessVersion = harnessVersion.String
	event.ToolCallID = protocol.OpaqueID(toolCallID.String)
	event.DeliveryID = protocol.OpaqueID(deliveryID.String)
	event.DecisionID = protocol.OpaqueID(decisionID.String)
	event.ParentEventID = protocol.ULID(parentEventID.String)
	if event.Validate() != nil {
		return protocol.EventEnvelope{}, false, agentlink.ErrCapabilitiesConflict
	}
	return event, true, nil
}

func equalCapabilityEvents(left, right protocol.EventEnvelope) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func decodeCapabilityJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode capability JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

var (
	_ agentlink.CapabilityWriter  = (*CapabilityStore)(nil)
	_ agentlink.CapabilityLineage = (*CapabilityStore)(nil)
)
