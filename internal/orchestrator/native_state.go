package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type nativeStateQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type nativeStateScanner interface {
	Scan(...any) error
}

const (
	maxNativeStateReferences = 256
	nativeStateEventColumns  = `event_id, schema_version, node_id,
		session_id, agent_id, adapter_type,
		COALESCE(harness, ''), COALESCE(harness_version, ''), source_quality,
		event_type, COALESCE(stream, ''), COALESCE(origin, ''), wall_time,
		sequence, COALESCE(turn_id, ''), COALESCE(tool_call_id, ''),
		COALESCE(correlation_id, ''), payload_json`
	nativeStateRangeSQL = `SELECT ` + nativeStateEventColumns + `
		FROM events
		WHERE session_id = ? AND sequence > ? AND sequence < ?
		AND schema_version = ? AND node_id = ? AND agent_id = ?
		AND adapter_type = ? AND harness = ? AND harness_version = ?
		AND wall_time >= ? AND (event_type = ? OR stream = ?)
		ORDER BY sequence`
)

// nativeStateLineage identifies the capability store and OS-process epoch
// that produced one run's events. Session ids alone are not an epoch: a
// restarted run can reuse an externally supplied id while its sequence starts
// over. The boot and process identity invalidate a warm accumulator;
// startedAt also bounds the durable scan.
type nativeStateLineage struct {
	nodeID         protocol.NodeID
	sessionID      protocol.OpaqueID
	agentID        protocol.AgentID
	harness        protocol.Harness
	harnessVersion string
	adapterType    protocol.AdapterType
	bootID         string
	startedAt      time.Time
	processSHA256  [32]byte
}

type nativeHistoryEntry struct {
	lineage    nativeStateLineage
	history    *analyst.NativeHistory
	highWater  int64
	generation int64
	recent     []protocol.StateObservationEvent
}

// nativeHistoryCache retains only reducer state and the bounded reference
// suffix needed to authenticate the next observation. Payloads never enter
// the cache. A per-cache lock makes the clone/advance/publish transition
// atomic if tests or a future caller prepare two agents concurrently.
type nativeHistoryCache struct {
	mu       sync.Mutex
	sessions map[stateSessionKey]nativeHistoryEntry
}

type nativeCapabilityPayload struct {
	EventType protocol.EventType    `json:"event_type"`
	Before    protocol.Capabilities `json:"before"`
	After     protocol.Capabilities `json:"after"`
}

// loadNativeStateEvents treats the observation list only as references. Every
// reducer field comes back from the append-only durable event row.
func loadNativeStateEvents(
	ctx context.Context,
	q nativeStateQueryer,
	report protocol.StateObservationReport,
) ([]analyst.NativeEvent, error) {
	events := make([]analyst.NativeEvent, 0, len(report.NativeEvents))
	for i, reference := range report.NativeEvents {
		event, err := scanNativeStateEvent(q.QueryRowContext(ctx,
			`SELECT `+nativeStateEventColumns+` FROM events WHERE event_id = ?`,
			reference.EventID), report, nil, &reference, i)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

// load advances one run-local accumulator through the last visible reference.
// A stable observation performs no event query. An advancing observation
// reads only (highWater, terminal), then folds the terminal row already read
// by id. Missing durable sequences remain visible to NativeHistory as gaps.
func (c *nativeHistoryCache) load(
	ctx context.Context,
	q nativeStateQueryer,
	lineage nativeStateLineage,
	report protocol.StateObservationReport,
) (*analyst.NativeHistory, error) {
	if len(report.NativeEvents) == 0 {
		// Synthetic initial and persistence observations intentionally carry no
		// native cut. They must neither consume nor regress a live run cache.
		return &analyst.NativeHistory{}, nil
	}
	if len(report.NativeEvents) > maxNativeStateReferences ||
		report.EventGeneration < int64(len(report.NativeEvents)) {
		return nil, ErrObservationUnavailable
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessions == nil {
		c.sessions = make(map[stateSessionKey]nativeHistoryEntry)
	}
	key := stateSessionKey{agent: report.AgentID, session: report.SessionID}
	entry, exists := c.sessions[key]
	if !exists || entry.lineage != lineage {
		entry = nativeHistoryEntry{
			lineage: lineage, history: &analyst.NativeHistory{}, highWater: -1,
		}
	} else if report.EventGeneration < entry.generation {
		return nil, ErrObservationUnavailable
	}

	lastReference := report.NativeEvents[len(report.NativeEvents)-1]
	if len(entry.recent) != 0 &&
		sameNativeReference(entry.recent[len(entry.recent)-1], lastReference) {
		if !nativeReferenceSuffixMatches(entry.recent, report.NativeEvents) {
			return nil, ErrObservationUnavailable
		}
		entry.generation = report.EventGeneration
		c.sessions[key] = entry
		return entry.history.Clone(), nil
	}

	terminal, err := scanNativeStateEvent(q.QueryRowContext(ctx,
		`SELECT `+nativeStateEventColumns+` FROM events WHERE event_id = ?`,
		lastReference.EventID), report, &lineage, &lastReference,
		len(report.NativeEvents)-1)
	if err != nil {
		return nil, err
	}
	if terminal.Sequence <= entry.highWater {
		// A changed terminal at or below the published high-water is either a
		// sequence-reset epoch without a new lineage or a forged window.
		return nil, ErrObservationUnavailable
	}

	history := entry.history.Clone()
	recent := append([]protocol.StateObservationEvent(nil), entry.recent...)
	rows, err := q.QueryContext(ctx, nativeStateRangeSQL,
		lineage.sessionID, entry.highWater, terminal.Sequence,
		protocol.SchemaVersion, lineage.nodeID, lineage.agentID,
		lineage.adapterType, lineage.harness, lineage.harnessVersion,
		eventstore.FormatTime(lineage.startedAt),
		protocol.EventTypeAdapterCapabilitiesChanged, protocol.StreamNativeEvent,
	)
	if err != nil {
		return nil, fmt.Errorf("read native state increment: %w", err)
	}
	defer func() { _ = rows.Close() }()
	index := 0
	for rows.Next() {
		event, scanErr := scanNativeStateEvent(rows, report, &lineage, nil, index)
		if scanErr != nil {
			return nil, scanErr
		}
		history.Advance(event)
		recent = appendNativeHistoryEvent(recent, event)
		index++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read native state increment: %w", err)
	}
	history.Advance(terminal)
	recent = appendNativeHistoryEvent(recent, terminal)
	if !nativeReferenceSuffixMatches(recent, report.NativeEvents) {
		return nil, ErrObservationUnavailable
	}
	entry.history = history
	entry.highWater = terminal.Sequence
	entry.generation = report.EventGeneration
	entry.recent = recent
	c.sessions[key] = entry
	return history.Clone(), nil
}

func appendNativeHistoryEvent(
	recent []protocol.StateObservationEvent,
	event analyst.NativeEvent,
) []protocol.StateObservationEvent {
	recent = append(recent, protocol.StateObservationEvent{
		EventID: event.EventID, Type: event.Type, At: event.At,
	})
	if len(recent) <= maxNativeStateReferences {
		return recent
	}
	copy(recent, recent[len(recent)-maxNativeStateReferences:])
	return recent[:maxNativeStateReferences]
}

func nativeReferenceSuffixMatches(
	recent []protocol.StateObservationEvent,
	references []protocol.StateObservationEvent,
) bool {
	if len(references) > len(recent) {
		return false
	}
	offset := len(recent) - len(references)
	for i := range references {
		if !sameNativeReference(recent[offset+i], references[i]) {
			return false
		}
	}
	return true
}

func sameNativeReference(a, b protocol.StateObservationEvent) bool {
	return a.EventID == b.EventID && a.Type == b.Type && a.At.Equal(b.At)
}

func scanNativeStateEvent(
	scanner nativeStateScanner,
	report protocol.StateObservationReport,
	lineage *nativeStateLineage,
	reference *protocol.StateObservationEvent,
	index int,
) (analyst.NativeEvent, error) {
	var eventID, nodeID, sessionID, agentID, adapterType, harness, harnessVersion string
	var sourceQuality, eventType, stream, origin, wallTime string
	var turnRef, toolRef, requestRef, payload string
	var schemaVersion, sequence int64
	err := scanner.Scan(
		&eventID, &schemaVersion, &nodeID, &sessionID, &agentID, &adapterType,
		&harness, &harnessVersion, &sourceQuality, &eventType, &stream, &origin,
		&wallTime, &sequence, &turnRef,
		&toolRef, &requestRef, &payload,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return analyst.NativeEvent{}, fmt.Errorf(
			"native state event %d: %w", index, ErrObservationUnavailable,
		)
	}
	if err != nil {
		return analyst.NativeEvent{}, fmt.Errorf("read native state event %d: %w", index, err)
	}
	at, err := eventstore.ParseTime(wallTime)
	if err != nil {
		return analyst.NativeEvent{}, fmt.Errorf("parse native state event %d time: %w", index, err)
	}
	typeID := protocol.EventType(eventType)
	id := protocol.ULID(eventID)
	if schemaVersion != protocol.SchemaVersion || !protocol.NodeID(nodeID).Valid() ||
		!protocol.AdapterType(adapterType).Valid() ||
		sessionID != string(report.SessionID) || agentID != string(report.AgentID) ||
		harness != string(report.Harness) || harnessVersion != report.HarnessVersion ||
		!id.Valid() || at.After(report.ObservedAt) || sequence < 0 {
		return analyst.NativeEvent{}, fmt.Errorf(
			"native state event %d: %w", index, ErrObservationUnavailable,
		)
	}
	if lineage != nil && (nodeID != string(lineage.nodeID) ||
		adapterType != string(lineage.adapterType) ||
		sessionID != string(lineage.sessionID) || agentID != string(lineage.agentID) ||
		harness != string(lineage.harness) || harnessVersion != lineage.harnessVersion ||
		at.Before(lineage.startedAt)) {
		return analyst.NativeEvent{}, fmt.Errorf(
			"native state event %d: %w", index, ErrObservationUnavailable,
		)
	}
	if reference != nil && (id != reference.EventID || typeID != reference.Type ||
		!at.Equal(reference.At)) {
		return analyst.NativeEvent{}, fmt.Errorf(
			"native state event %d: %w", index, ErrObservationUnavailable,
		)
	}
	event := analyst.NativeEvent{
		EventID: id, Type: typeID, At: at, Sequence: sequence,
		TurnRef: protocol.OpaqueID(turnRef), ToolRef: protocol.OpaqueID(toolRef),
		RequestRef: protocol.OpaqueID(requestRef),
	}
	switch typeID {
	case protocol.EventTypeAdapterCapabilitiesChanged:
		if protocol.SourceQuality(sourceQuality) != protocol.SourceQualityProcessMetrics ||
			stream != "" || protocol.Origin(origin) != protocol.OriginSystem {
			return analyst.NativeEvent{}, fmt.Errorf(
				"native state capability event %d: %w", index, ErrObservationUnavailable,
			)
		}
		var change nativeCapabilityPayload
		if err := decodeNativeStateJSON([]byte(payload), &change); err != nil ||
			change.EventType != protocol.EventTypeAdapterCapabilitiesChanged {
			return analyst.NativeEvent{}, fmt.Errorf(
				"native state capability payload %d: %w", index, ErrObservationUnavailable,
			)
		}
		canonical, err := json.Marshal(change)
		if err != nil || !bytes.Equal(canonical, []byte(payload)) {
			return analyst.NativeEvent{}, fmt.Errorf(
				"native state capability encoding %d: %w", index, ErrObservationUnavailable,
			)
		}
		gained, lost := nativeCapabilityDelta(change.Before, change.After)
		generic := adapters.GenericManifest(
			report.AgentID, report.Harness, report.HarnessVersion,
		)
		otherChanged := adapters.NativeEventCapabilities(
			change.Before, protocol.Capabilities{},
		) != adapters.NativeEventCapabilities(
			change.After, protocol.Capabilities{},
		)
		switch {
		case change.Before == change.After || gained && lost ||
			otherChanged && change.After != generic.Capabilities:
			return analyst.NativeEvent{}, fmt.Errorf(
				"native state mixed capability direction %d: %w", index,
				ErrObservationUnavailable,
			)
		case lost:
			event.Availability = analyst.NativeUnavailable
		case gained:
			event.Availability = analyst.NativeAvailable
		default:
			event.Availability = analyst.NativeUnchanged
		}
	default:
		if !protocol.IsNativeEventType(typeID) || sequence < 1 ||
			(protocol.SourceQuality(sourceQuality) != protocol.SourceQualityNativeStructured &&
				protocol.SourceQuality(sourceQuality) != protocol.SourceQualityNativeHook) ||
			protocol.Stream(stream) != protocol.StreamNativeEvent ||
			protocol.Origin(origin) != protocol.OriginHarness {
			return analyst.NativeEvent{}, fmt.Errorf(
				"native state lifecycle event %d: %w", index, ErrObservationUnavailable,
			)
		}
		nativePayload, err := protocol.DecodeNativeDurableEventPayload(protocol.Payload(payload))
		if err != nil {
			return analyst.NativeEvent{}, fmt.Errorf(
				"decode native state payload %d: %w", index, err,
			)
		}
		event.Result = nativePayload.Result
	}
	return event, nil
}

func nativeCapabilityDelta(before, after protocol.Capabilities) (gained, lost bool) {
	for _, pair := range [][2]bool{
		{before.StructuredEvents, after.StructuredEvents},
		{before.ToolEvents, after.ToolEvents},
		{before.ApprovalEvents, after.ApprovalEvents},
	} {
		gained = gained || !pair[0] && pair[1]
		lost = lost || pair[0] && !pair[1]
	}
	return gained, lost
}

func decodeNativeStateJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode native state JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON")
	}
	return nil
}
