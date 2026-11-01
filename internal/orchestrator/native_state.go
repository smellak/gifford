package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

type nativeStateQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
		var sessionID, agentID, harness, harnessVersion string
		var sourceQuality, eventType, stream, origin, wallTime string
		var turnRef, toolRef, requestRef, payload string
		var sequence int64
		err := q.QueryRowContext(ctx, `SELECT session_id, agent_id,
			COALESCE(harness, ''), COALESCE(harness_version, ''), source_quality,
			event_type, COALESCE(stream, ''), COALESCE(origin, ''), wall_time,
			sequence, COALESCE(turn_id, ''), COALESCE(tool_call_id, ''),
			COALESCE(correlation_id, ''), payload_json
			FROM events WHERE event_id = ?`, reference.EventID).Scan(
			&sessionID, &agentID, &harness, &harnessVersion, &sourceQuality,
			&eventType, &stream, &origin, &wallTime, &sequence, &turnRef,
			&toolRef, &requestRef, &payload,
		)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("native state event %d: %w", i, ErrObservationUnavailable)
		}
		if err != nil {
			return nil, fmt.Errorf("read native state event %d: %w", i, err)
		}
		at, err := eventstore.ParseTime(wallTime)
		if err != nil {
			return nil, fmt.Errorf("parse native state event %d time: %w", i, err)
		}
		typeID := protocol.EventType(eventType)
		if sessionID != string(report.SessionID) || agentID != string(report.AgentID) ||
			harness != string(report.Harness) || harnessVersion != report.HarnessVersion ||
			typeID != reference.Type || !at.Equal(reference.At) || sequence < 0 {
			return nil, fmt.Errorf("native state event %d: %w", i, ErrObservationUnavailable)
		}
		event := analyst.NativeEvent{
			EventID: reference.EventID, Type: typeID, At: at, Sequence: sequence,
			TurnRef: protocol.OpaqueID(turnRef), ToolRef: protocol.OpaqueID(toolRef),
			RequestRef: protocol.OpaqueID(requestRef),
		}
		switch typeID {
		case protocol.EventTypeAdapterCapabilitiesChanged:
			if protocol.SourceQuality(sourceQuality) != protocol.SourceQualityProcessMetrics ||
				stream != "" || protocol.Origin(origin) != protocol.OriginSystem {
				return nil, fmt.Errorf("native state capability event %d: %w",
					i, ErrObservationUnavailable)
			}
			var change nativeCapabilityPayload
			if err := decodeNativeStateJSON([]byte(payload), &change); err != nil ||
				change.EventType != protocol.EventTypeAdapterCapabilitiesChanged {
				return nil, fmt.Errorf("native state capability payload %d: %w",
					i, ErrObservationUnavailable)
			}
			canonical, err := json.Marshal(change)
			if err != nil || !bytes.Equal(canonical, []byte(payload)) {
				return nil, fmt.Errorf("native state capability encoding %d: %w",
					i, ErrObservationUnavailable)
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
				return nil, fmt.Errorf("native state mixed capability direction %d: %w",
					i, ErrObservationUnavailable)
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
				return nil, fmt.Errorf("native state lifecycle event %d: %w",
					i, ErrObservationUnavailable)
			}
			nativePayload, err := protocol.DecodeNativeDurableEventPayload(
				protocol.Payload(payload),
			)
			if err != nil {
				return nil, fmt.Errorf("decode native state payload %d: %w", i, err)
			}
			event.Result = nativePayload.Result
		}
		events = append(events, event)
	}
	return events, nil
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
