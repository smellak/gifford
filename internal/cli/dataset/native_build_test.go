package dataset

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func nativeBuildCaptureID() protocol.OpaqueID {
	return protocol.OpaqueID(fmt.Sprintf("run-321-%d", t0.UnixNano()))
}

func seedNativeBuildRun(t *testing.T, dataDir string) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(dataDir, "gifford.db"))
	require.NoError(t, err)
	const (
		nodeID    = protocol.NodeID("node-native-build")
		agentID   = protocol.AgentID("native-build-agent")
		sessionID = protocol.OpaqueID("sess-native")
		version   = "1.18.29"
	)
	before := adapters.GenericManifest(agentID, protocol.HarnessOpencode, version)
	after := before
	after.Capabilities.StructuredEvents = true
	after.Capabilities.ToolEvents = true
	after.Capabilities.ApprovalEvents = true
	manifestJSON, err := json.Marshal(after)
	require.NoError(t, err)
	processJSON, err := json.Marshal(protocol.ProcessIdentity{
		PID: 321, ProcessStartTime: 654, UID: 1000, GID: 1000,
	})
	require.NoError(t, err)
	started, ended := t0.Add(-time.Second), t0.Add(7*time.Second)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		agentID, nodeID, after.Harness, eventstore.FormatTime(started), eventstore.FormatTime(started))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sessionID, agentID, protocol.SessionStateTerminated, after.CaptureMode,
		after.Deliverable, string(manifestJSON), string(processJSON),
		eventstore.FormatTime(started), eventstore.FormatTime(ended),
		eventstore.FormatTime(ended), "boot-native-build")
	require.NoError(t, err)

	capabilityPayload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    before.Capabilities,
		After:     after.Capabilities,
	})
	require.NoError(t, err)
	events := make([]protocol.EventEnvelope, 1, 4)
	events[0] = protocol.EventEnvelope{
		EventID: "01JB0000000000000000000600", SchemaVersion: protocol.SchemaVersion,
		NodeID: nodeID, SessionID: sessionID, AgentID: agentID,
		AdapterType: after.AdapterType, SourceQuality: protocol.SourceQualityProcessMetrics,
		EventType: protocol.EventTypeAdapterCapabilitiesChanged, Origin: protocol.OriginSystem,
		WallTime: t0, Sequence: 0, Payload: capabilityPayload,
		Sensitivity: protocol.SensitivityNormal, Harness: after.Harness,
		HarnessVersion: after.HarnessVersion,
	}
	native := func(
		id protocol.ULID,
		sequence int64,
		at time.Time,
		eventType protocol.EventType,
		turn protocol.OpaqueID,
		result protocol.NativeEventResult,
	) protocol.EventEnvelope {
		observedAt := at.Add(-time.Hour).UnixMilli()
		payload, marshalErr := json.Marshal(protocol.NativeDurableEventPayload{
			EmitterEventID: protocol.OpaqueID("emitter-" + id[len(id)-1:]),
			ObservedAt:     &observedAt,
			SessionRef:     sessionID,
			Result:         result,
		})
		require.NoError(t, marshalErr)
		return protocol.EventEnvelope{
			EventID: id, SchemaVersion: protocol.SchemaVersion,
			NodeID: nodeID, SessionID: sessionID, AgentID: agentID,
			AdapterType: after.AdapterType, SourceQuality: protocol.SourceQualityNativeStructured,
			EventType: eventType, Stream: protocol.StreamNativeEvent, Origin: protocol.OriginHarness,
			WallTime: at, Sequence: sequence, TurnID: turn, Payload: payload,
			Sensitivity: protocol.SensitivityNormal, Harness: after.Harness,
			HarnessVersion: after.HarnessVersion,
		}
	}
	events = append(events,
		native("01JB0000000000000000000601", 1, t0.Add(50*time.Millisecond), protocol.EventTypeTurnStarted, "turn-1", ""),
		native("01JB0000000000000000000602", 2, t0.Add(2500*time.Millisecond), protocol.EventTypeTurnCompleted, "turn-1", protocol.NativeEventResultFailed),
		native("01JB0000000000000000000603", 3, t0.Add(5500*time.Millisecond), protocol.EventTypeSessionEnded, "", ""),
	)
	require.NoError(t, store.AppendEvents(t.Context(), events))
	require.NoError(t, store.Close())
}

func TestBuildNativeLabelsEndToEnd(t *testing.T) {
	e := setup(t)
	writeSessionAs(t, e.dataDir, string(nativeBuildCaptureID()), protocol.HarnessOpencode, "1.18.29", []chunk{
		{ms: 100, output: openCodeBox},
		{ms: 200, draft: draftClosed},
		{ms: 5100, output: openCodeBox},
	})
	require.NoError(t, os.WriteFile(e.alignCfg, []byte(`
margin_ms: 0
hook_harnesses: []
native_sources:
  - {harness: opencode, harness_version: "1.18.29"}
weak:
  silence_seconds: 3600
`), 0o600))
	seedNativeBuildRun(t, e.dataDir)

	out := filepath.Join(e.work, "native-dataset")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg, flagLiveForm, flagNative)
	require.NoError(t, err)
	require.Regexp(t, `native labels: native_candidate [1-9][0-9]*, native_screen_supported [1-9][0-9]*,`, stdout)
	require.Contains(t, stdout, "native_withheld 0")
	require.Contains(t, stdout, "native_unclosed 0")

	manifestRaw, err := os.ReadFile(filepath.Join(out, dataset.ManifestFile))
	require.NoError(t, err)
	manifest, err := dataset.ParseManifest(bytes.NewReader(manifestRaw))
	require.NoError(t, err)
	require.Equal(t, dataset.ScreenFormLive, manifest.ScreenForm)
	require.NotNil(t, manifest.NativeLabels)
	sidecarRaw, err := os.ReadFile(filepath.Join(out, dataset.NativeLabelsFile))
	require.NoError(t, err)
	sidecar, err := dataset.ParseNativeLabelSidecar(bytes.NewReader(sidecarRaw))
	require.NoError(t, err)
	require.NoError(t, dataset.ValidateNativeLabelSidecar(manifest, sidecar))
	require.Equal(t, manifest.NativeLabels.Entries, int64(len(sidecar.Entries)))
	info, err := os.Stat(filepath.Join(out, dataset.NativeLabelsFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	seen := map[dataset.Label]bool{}
	for _, window := range readWindows(t, out) {
		require.Equal(t, nativeBuildCaptureID(), window.Provenance.SessionID)
		require.Equal(t, protocol.SourceQualityPTYScreen, window.Provenance.SourceQuality,
			"label provenance never replaces screen provenance")
		require.Equal(t, dataset.LabelSourceNative, window.Target.Source)
		require.NotContains(t, []dataset.Label{dataset.LabelA, dataset.LabelG}, window.Target.Label)
		seen[window.Target.Label] = true
	}
	require.True(t, seen[dataset.LabelB])
	require.True(t, seen[dataset.LabelF])
	require.NotContains(t, strings.Join(mapValues(treeOf(t, out)), "\n"), "emitter-",
		"the sidecar carries durable ids and enums, never the raw payload")
}
