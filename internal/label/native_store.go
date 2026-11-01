package label

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	maxNativeLabelEvents  = 1 << 20
	nativeRunDigestDomain = "gifford.label.native-run.v1"
)

// NativeQueryer is the read-only database surface used by native labeling.
// The CLI supplies a mode=ro, query_only SQLite pool.
type NativeQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// NativeRecord is one validated durable boundary. Emitter timestamps and
// payload ids are intentionally not retained: ordering and alignment use only
// Sequence and the Core receipt time.
type NativeRecord struct {
	EventID    protocol.ULID
	Type       protocol.EventType
	ReceivedAt time.Time
	Sequence   int64
	TurnRef    protocol.OpaqueID
	ToolRef    protocol.OpaqueID
	RequestRef protocol.OpaqueID
	Result     protocol.NativeEventResult

	CapabilityBefore *protocol.Capabilities
	CapabilityAfter  *protocol.Capabilities
}

// NativeRun is one immutable OS-process epoch linked to a registered capture.
// RunDigest commits to node, agent, process and boot identity without exposing
// those values in the output sidecar.
type NativeRun struct {
	SessionID      protocol.OpaqueID
	Harness        protocol.Harness
	HarnessVersion string
	RunDigest      protocol.Digest
	StartedAt      time.Time
	EndedAt        time.Time
	Events         []NativeRecord
}

type nativeCapabilityPayload struct {
	EventType protocol.EventType    `json:"event_type"`
	Before    protocol.Capabilities `json:"before"`
	After     protocol.Capabilities `json:"after"`
}

// ReadNativeRuns reads completed run epochs for the requested recording
// sessions. A recording without a registered session simply has no native
// source. Any malformed durable row fails the native build closed.
func ReadNativeRuns(
	ctx context.Context,
	q NativeQueryer,
	sessions []protocol.OpaqueID,
) ([]NativeRun, error) {
	if q == nil {
		return nil, errors.New("native labels: no read-only database")
	}
	ids := slices.Clone(sessions)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	runs := make([]NativeRun, 0, len(ids))
	for _, sessionID := range ids {
		if !sessionID.Valid() {
			return nil, errors.New("native labels: invalid recording session")
		}
		run, found, err := readNativeRun(ctx, q, sessionID)
		if err != nil {
			return nil, err
		}
		if found && len(run.Events) != 0 {
			runs = append(runs, run)
		}
	}
	return runs, nil
}

func readNativeRun(
	ctx context.Context,
	q NativeQueryer,
	sessionID protocol.OpaqueID,
) (NativeRun, bool, error) {
	var manifestRaw, processRaw, startedRaw, endedRaw, bootID, nodeID string
	var sessionAgent, agentHarness, stateRaw, captureRaw string
	var deliverable int64
	err := q.QueryRowContext(ctx, `SELECT s.manifest_json,
		COALESCE(s.process_json, ''), s.started_at, COALESCE(s.ended_at, ''),
		COALESCE(s.boot_id, ''), a.node_id, s.agent_id, a.harness, s.state,
		s.capture_mode, s.deliverable
		FROM sessions s JOIN agents a ON a.agent_id = s.agent_id
		WHERE s.session_id = ?`, sessionID).
		Scan(&manifestRaw, &processRaw, &startedRaw, &endedRaw, &bootID, &nodeID,
			&sessionAgent, &agentHarness, &stateRaw, &captureRaw, &deliverable)
	if errors.Is(err, sql.ErrNoRows) {
		return NativeRun{}, false, nil
	}
	if err != nil {
		return NativeRun{}, false, fmt.Errorf("native labels: read run lineage: %w", err)
	}
	var manifest protocol.CapabilityManifest
	if decodeClosedNativeJSON([]byte(manifestRaw), &manifest) != nil ||
		manifest.Validate() != nil {
		return NativeRun{}, false, errors.New("native labels: invalid run manifest")
	}
	manifestCanonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(manifestCanonical, []byte(manifestRaw)) {
		return NativeRun{}, false, errors.New("native labels: non-canonical run manifest")
	}
	state := protocol.SessionState(stateRaw)
	terminal := state == protocol.SessionStateDisconnected || state == protocol.SessionStateTerminated
	if manifest.AgentID != protocol.AgentID(sessionAgent) ||
		manifest.Harness != protocol.Harness(agentHarness) ||
		manifest.CaptureMode != protocol.CaptureMode(captureRaw) ||
		(deliverable != 0 && deliverable != 1) || manifest.Deliverable != (deliverable == 1) ||
		!protocol.AgentID(sessionAgent).Valid() {
		return NativeRun{}, false, errors.New("native labels: run manifest lineage mismatch")
	}
	var process protocol.ProcessIdentity
	if processRaw == "" || decodeClosedNativeJSON([]byte(processRaw), &process) != nil ||
		process.Validate() != nil || !validNativeBootID(bootID) || !terminal {
		return NativeRun{}, false, errors.New("native labels: incomplete run epoch")
	}
	processCanonical, err := json.Marshal(process)
	if err != nil || !bytes.Equal(processCanonical, []byte(processRaw)) {
		return NativeRun{}, false, errors.New("native labels: non-canonical process epoch")
	}
	started, startErr := eventstore.ParseTime(startedRaw)
	ended, endErr := eventstore.ParseTime(endedRaw)
	node := protocol.NodeID(nodeID)
	if startErr != nil || endErr != nil || !ended.After(started) || !node.Valid() {
		return NativeRun{}, false, errors.New("native labels: invalid run boundary")
	}
	runDigest := nativeRunDigest(node, sessionID, manifest, bootID, started, processCanonical)
	run := NativeRun{
		SessionID: sessionID, Harness: manifest.Harness,
		HarnessVersion: manifest.HarnessVersion, RunDigest: runDigest,
		StartedAt: started, EndedAt: ended,
	}
	rows, err := q.QueryContext(ctx, `SELECT event_id, schema_version, node_id,
		session_id, agent_id, adapter_type, source_quality, event_type,
		COALESCE(direction, ''), COALESCE(stream, ''), COALESCE(origin, ''),
		wall_time, monotonic_ns, sequence, COALESCE(turn_id, ''),
		COALESCE(message_id, ''), COALESCE(correlation_id, ''), payload_json,
		sensitivity, redaction_applied, COALESCE(harness, ''),
		COALESCE(harness_version, ''), COALESCE(tool_call_id, ''),
		COALESCE(delivery_id, ''), COALESCE(decision_id, ''),
		COALESCE(parent_event_id, '')
		FROM events WHERE session_id = ? AND wall_time >= ? AND wall_time <= ?
		AND (event_type = ? OR stream = ?) ORDER BY sequence`,
		sessionID, eventstore.FormatTime(started), eventstore.FormatTime(ended),
		protocol.EventTypeAdapterCapabilitiesChanged, protocol.StreamNativeEvent)
	if err != nil {
		return NativeRun{}, false, fmt.Errorf("native labels: read durable events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var lastCapability *protocol.Capabilities
	for rows.Next() {
		if len(run.Events) == maxNativeLabelEvents {
			return NativeRun{}, false, errors.New("native labels: too many durable events")
		}
		record, before, after, scanErr := scanNativeRecord(rows, node, sessionID, manifest, started, ended)
		if scanErr != nil {
			return NativeRun{}, false, scanErr
		}
		if before != nil {
			if lastCapability != nil && *lastCapability != *before {
				return NativeRun{}, false, errors.New("native labels: broken capability chain")
			}
			copyAfter := *after
			lastCapability = &copyAfter
		}
		run.Events = append(run.Events, record)
	}
	if err := rows.Err(); err != nil {
		return NativeRun{}, false, fmt.Errorf("native labels: iterate durable events: %w", err)
	}
	if lastCapability != nil && *lastCapability != manifest.Capabilities {
		return NativeRun{}, false, errors.New("native labels: capability head mismatch")
	}
	return run, true, nil
}

func validNativeBootID(bootID string) bool {
	return bootID != "" && len(bootID) <= 128 && strings.IndexFunc(bootID, func(r rune) bool {
		return unicode.IsControl(r) || unicode.IsSpace(r)
	}) < 0
}

type nativeRecordScanner interface{ Scan(...any) error }

func scanNativeRecord(
	scanner nativeRecordScanner,
	node protocol.NodeID,
	sessionID protocol.OpaqueID,
	manifest protocol.CapabilityManifest,
	started, ended time.Time,
) (NativeRecord, *protocol.Capabilities, *protocol.Capabilities, error) {
	var event protocol.EventEnvelope
	var direction, stream, origin, wallRaw, payload string
	var harness, harnessVersion string
	var redaction int64
	if err := scanner.Scan(
		&event.EventID, &event.SchemaVersion, &event.NodeID, &event.SessionID,
		&event.AgentID, &event.AdapterType, &event.SourceQuality, &event.EventType,
		&direction, &stream, &origin, &wallRaw, &event.MonotonicNS, &event.Sequence,
		&event.TurnID, &event.MessageID, &event.CorrelationID, &payload,
		&event.Sensitivity, &redaction, &harness, &harnessVersion,
		&event.ToolCallID, &event.DeliveryID, &event.DecisionID, &event.ParentEventID,
	); err != nil {
		return NativeRecord{}, nil, nil, fmt.Errorf("native labels: scan durable event: %w", err)
	}
	wall, err := eventstore.ParseTime(wallRaw)
	if err != nil {
		return NativeRecord{}, nil, nil, errors.New("native labels: invalid durable receipt time")
	}
	event.Direction, event.Stream, event.Origin = protocol.Direction(direction), protocol.Stream(stream), protocol.Origin(origin)
	event.WallTime, event.Payload = wall, protocol.Payload(payload)
	event.RedactionApplied = redaction == 1
	event.Harness, event.HarnessVersion = protocol.Harness(harness), harnessVersion
	if redaction != 0 && redaction != 1 || event.NodeID != node || event.SessionID != sessionID ||
		event.AgentID != manifest.AgentID || event.AdapterType != manifest.AdapterType ||
		event.Harness != manifest.Harness || event.HarnessVersion != manifest.HarnessVersion ||
		event.WallTime.Before(started) || event.WallTime.After(ended) {
		return NativeRecord{}, nil, nil, errors.New("native labels: durable lineage mismatch")
	}
	record := NativeRecord{
		EventID: event.EventID, Type: event.EventType, ReceivedAt: event.WallTime,
		Sequence: event.Sequence, TurnRef: event.TurnID, ToolRef: event.ToolCallID,
		RequestRef: event.CorrelationID,
	}
	if event.EventType != protocol.EventTypeAdapterCapabilitiesChanged {
		if err := protocol.ValidateNativeDurableEvent(event); err != nil {
			return NativeRecord{}, nil, nil, errors.New("native labels: invalid lifecycle event")
		}
		decoded, err := protocol.DecodeNativeDurableEventPayload(event.Payload)
		if err != nil {
			return NativeRecord{}, nil, nil, errors.New("native labels: invalid lifecycle payload")
		}
		canonical, err := protocol.CanonicalNativeDurableEventPayload(event.Payload)
		if err != nil || !bytes.Equal(canonical, event.Payload) {
			return NativeRecord{}, nil, nil, errors.New("native labels: non-canonical lifecycle payload")
		}
		record.Result = decoded.Result
		return record, nil, nil, nil
	}
	if err := event.Validate(); err != nil || event.SourceQuality != protocol.SourceQualityProcessMetrics ||
		event.Direction != "" || event.Stream != "" || event.Origin != protocol.OriginSystem ||
		event.MonotonicNS != 0 || event.Sequence < 0 || event.TurnID != "" ||
		event.ToolCallID != "" || event.MessageID != "" || event.CorrelationID != "" ||
		event.DeliveryID != "" || event.DecisionID != "" || event.ParentEventID != "" ||
		event.Sensitivity != protocol.SensitivityNormal || event.RedactionApplied {
		return NativeRecord{}, nil, nil, errors.New("native labels: invalid capability event")
	}
	var change nativeCapabilityPayload
	if decodeClosedNativeJSON(event.Payload, &change) != nil ||
		change.EventType != protocol.EventTypeAdapterCapabilitiesChanged {
		return NativeRecord{}, nil, nil, errors.New("native labels: invalid capability payload")
	}
	canonical, err := json.Marshal(change)
	if err != nil || !bytes.Equal(canonical, event.Payload) {
		return NativeRecord{}, nil, nil, errors.New("native labels: non-canonical capability payload")
	}
	beforeManifest, afterManifest := manifest, manifest
	beforeManifest.Capabilities, afterManifest.Capabilities = change.Before, change.After
	if !adapters.ValidCapabilitiesTransition(beforeManifest, afterManifest) {
		return NativeRecord{}, nil, nil, errors.New("native labels: invalid capability transition")
	}
	before, after := change.Before, change.After
	record.CapabilityBefore, record.CapabilityAfter = &before, &after
	return record, &before, &after, nil
}

func nativeRunDigest(
	node protocol.NodeID,
	session protocol.OpaqueID,
	manifest protocol.CapabilityManifest,
	bootID string,
	started time.Time,
	process []byte,
) protocol.Digest {
	h := sha256.New()
	field := func(value string) {
		_, _ = io.WriteString(h, value)
		_, _ = h.Write([]byte{0})
	}
	field(nativeRunDigestDomain)
	field(string(node))
	field(string(session))
	field(string(manifest.AgentID))
	field(string(manifest.Harness))
	field(manifest.HarnessVersion)
	field(string(manifest.AdapterType))
	field(bootID)
	field(eventstore.FormatTime(started))
	processSum := sha256.Sum256(process)
	field(hex.EncodeToString(processSum[:]))
	return protocol.Digest("sha256:" + hex.EncodeToString(h.Sum(nil)))
}

func decodeClosedNativeJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode closed native JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}

func nativeAllCapabilities(c protocol.Capabilities) bool {
	return c.StructuredEvents && c.ToolEvents && c.ApprovalEvents
}

func nativeCapabilityDirection(before, after protocol.Capabilities) int {
	beforeAll, afterAll := nativeAllCapabilities(before), nativeAllCapabilities(after)
	switch {
	case !beforeAll && afterAll:
		return 1
	case beforeAll && !afterAll:
		return -1
	default:
		return 0
	}
}

func nativeEpochDigest(run protocol.Digest, activation NativeRecord) protocol.Digest {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		"gifford.label.native-epoch.v1", string(run), string(activation.EventID),
		fmt.Sprint(activation.Sequence), eventstore.FormatTime(activation.ReceivedAt),
	}, "\x00")))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}
