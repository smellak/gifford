package label

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	nativeStoreNode    = protocol.NodeID("node-native-labels")
	nativeStoreAgent   = protocol.AgentID("native-label-agent")
	nativeStoreSession = protocol.OpaqueID("native-label-session")
)

func readNativeStoreFixture(t *testing.T, observedAt int64, bootID string) NativeRun {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	before := adapters.GenericManifest(nativeStoreAgent, protocol.HarnessCodex, nativeTestVersion)
	after := before
	after.Capabilities.StructuredEvents = true
	after.Capabilities.ToolEvents = true
	after.Capabilities.ApprovalEvents = true
	manifestJSON, err := json.Marshal(after)
	require.NoError(t, err)
	process := protocol.ProcessIdentity{PID: 123, ProcessStartTime: 456, UID: 1000, GID: 1000}
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	started, ended := t0, t0.Add(10*time.Second)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		nativeStoreAgent, nativeStoreNode, protocol.HarnessCodex,
		eventstore.FormatTime(started), eventstore.FormatTime(started))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		nativeStoreSession, nativeStoreAgent, protocol.SessionStateTerminated,
		after.CaptureMode, after.Deliverable, string(manifestJSON), string(processJSON),
		eventstore.FormatTime(started), eventstore.FormatTime(ended),
		eventstore.FormatTime(ended), bootID)
	require.NoError(t, err)

	capabilityPayload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    before.Capabilities,
		After:     after.Capabilities,
	})
	require.NoError(t, err)
	activation := protocol.EventEnvelope{
		EventID: "01JB0000000000000000000400", SchemaVersion: protocol.SchemaVersion,
		NodeID: nativeStoreNode, SessionID: nativeStoreSession, AgentID: nativeStoreAgent,
		AdapterType: after.AdapterType, SourceQuality: protocol.SourceQualityProcessMetrics,
		EventType: protocol.EventTypeAdapterCapabilitiesChanged, Origin: protocol.OriginSystem,
		WallTime: started.Add(time.Second), Sequence: 0, Payload: capabilityPayload,
		Sensitivity: protocol.SensitivityNormal, Harness: after.Harness,
		HarnessVersion: after.HarnessVersion,
	}
	nativeEvent := func(id protocol.ULID, sequence int64, eventType protocol.EventType, result protocol.NativeEventResult) protocol.EventEnvelope {
		payload, marshalErr := json.Marshal(protocol.NativeDurableEventPayload{
			EmitterEventID: protocol.OpaqueID("emitter-" + id[len(id)-1:]),
			ObservedAt:     &observedAt,
			SessionRef:     nativeStoreSession,
			Result:         result,
		})
		require.NoError(t, marshalErr)
		return protocol.EventEnvelope{
			EventID: id, SchemaVersion: protocol.SchemaVersion,
			NodeID: nativeStoreNode, SessionID: nativeStoreSession, AgentID: nativeStoreAgent,
			AdapterType: after.AdapterType, SourceQuality: protocol.SourceQualityNativeStructured,
			EventType: eventType, Stream: protocol.StreamNativeEvent, Origin: protocol.OriginHarness,
			WallTime: started.Add(time.Duration(sequence+1) * time.Second), Sequence: sequence,
			TurnID: nativeTestTurn, Payload: payload, Sensitivity: protocol.SensitivityNormal,
			Harness: after.Harness, HarnessVersion: after.HarnessVersion,
		}
	}
	startedEvent := nativeEvent("01JB0000000000000000000401", 1, protocol.EventTypeTurnStarted, "")
	completedEvent := nativeEvent("01JB0000000000000000000402", 2, protocol.EventTypeTurnCompleted, protocol.NativeEventResultFailed)
	require.NoError(t, store.AppendEvents(t.Context(), []protocol.EventEnvelope{activation, startedEvent, completedEvent}))

	runs, err := ReadNativeRuns(t.Context(), store.DB(), []protocol.OpaqueID{nativeStoreSession, nativeStoreSession})
	require.NoError(t, err)
	require.Len(t, runs, 1, "requested session ids are deduplicated")
	return runs[0]
}

func TestReadNativeRunsUsesReceiptTimeAndRunEpoch(t *testing.T) {
	t.Parallel()
	first := readNativeStoreFixture(t, 1, "boot-a")
	otherEmitterClock := readNativeStoreFixture(t, time.Now().UnixMilli(), "boot-a")
	require.Equal(t, first, otherEmitterClock,
		"payload observed_at is audit-only and never reaches native label records")
	require.True(t, first.RunDigest.Valid())
	require.Equal(t, []time.Time{
		t0.Add(time.Second), t0.Add(2 * time.Second), t0.Add(3 * time.Second),
	}, []time.Time{first.Events[0].ReceivedAt, first.Events[1].ReceivedAt, first.Events[2].ReceivedAt})
	require.Equal(t, protocol.NativeEventResultFailed, first.Events[2].Result)

	otherBoot := readNativeStoreFixture(t, 1, "boot-b")
	require.NotEqual(t, first.RunDigest, otherBoot.RunDigest,
		"a reused session id in another boot is a different run")
	require.NotEqual(t, nativeEpochDigest(first.RunDigest, first.Events[0]),
		nativeEpochDigest(otherBoot.RunDigest, otherBoot.Events[0]))
}

func TestReadNativeRunsRejectsIncompleteRunEpoch(t *testing.T) {
	t.Parallel()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	manifest := adapters.GenericManifest(nativeStoreAgent, protocol.HarnessCodex, nativeTestVersion)
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		nativeStoreAgent, nativeStoreNode, protocol.HarnessCodex,
		eventstore.FormatTime(t0), eventstore.FormatTime(t0))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, nativeStoreSession, nativeStoreAgent,
		protocol.SessionStateConnected, manifest.CaptureMode, manifest.Deliverable,
		string(manifestJSON), eventstore.FormatTime(t0), eventstore.FormatTime(t0))
	require.NoError(t, err)

	_, err = ReadNativeRuns(t.Context(), store.DB(), []protocol.OpaqueID{nativeStoreSession})
	require.ErrorContains(t, err, "incomplete run epoch")
}
