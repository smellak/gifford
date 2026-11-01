package proxies

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	capabilityNode      protocol.NodeID   = "node-1"
	capabilityAgent     protocol.AgentID  = "agent-1"
	capabilitySessionID protocol.OpaqueID = "session-1"
)

var capabilityObservedAt = time.Date(2026, 10, 31, 1, 30, 0, 123, time.UTC)

func TestCapabilityStoreAppliesAndReplaysWithoutLifecycleWrite(t *testing.T) {
	t.Parallel()
	store, consumer := openCapabilityStore(t)
	store.DB().SetMaxOpenConns(1)
	native := capabilityNativeManifest()
	revision := insertCapabilitySession(t, store, native)
	event := capabilityEvent(t, native, "01JB0000000000000000000041")

	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), event))
	after := native
	after.Capabilities = adapters.GenericManifest(
		native.AgentID, native.Harness, native.HarnessVersion,
	).Capabilities
	manifest, updatedAt := readCapabilityManifest(t, store)
	require.Equal(t, after, manifest)
	require.Equal(t, revision, updatedAt, "capability changes are not lifecycle revisions")
	events, err := store.SessionEvents(t.Context(), capabilitySessionID, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []protocol.EventEnvelope{event}, events)

	var changesBefore int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT total_changes()").Scan(&changesBefore))
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), event))
	var changesAfter int64
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT total_changes()").Scan(&changesAfter))
	require.Equal(t, changesBefore, changesAfter, "exact replay must execute no write")

	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	matches, err := consumer.MatchesCapabilitiesChanged(
		t.Context(), tx, capabilitySessionID, native, after,
	)
	require.NoError(t, err)
	require.True(t, matches)
	require.NoError(t, tx.Rollback())
}

func TestCapabilityStoreRejectsConflictsAndRollsBackManifest(t *testing.T) {
	t.Parallel()

	t.Run("different replay envelope", func(t *testing.T) {
		t.Parallel()
		store, consumer := openCapabilityStore(t)
		native := capabilityNativeManifest()
		insertCapabilitySession(t, store, native)
		event := capabilityEvent(t, native, "01JB0000000000000000000042")
		require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), event))
		event.WallTime = event.WallTime.Add(time.Second)
		require.ErrorIs(t, consumer.ApplyCapabilitiesChanged(t.Context(), event),
			agentlink.ErrCapabilitiesConflict)
		events, err := store.SessionEvents(t.Context(), capabilitySessionID, 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
	})

	t.Run("occupied sequence", func(t *testing.T) {
		t.Parallel()
		store, consumer := openCapabilityStore(t)
		native := capabilityNativeManifest()
		revision := insertCapabilitySession(t, store, native)
		occupied := capabilityEvent(t, native, "01JB0000000000000000000043")
		occupied.EventType = protocol.EventTypeAdapterConnected
		occupied.Payload = protocol.Payload(`{}`)
		require.NoError(t, store.AppendEvents(t.Context(), []protocol.EventEnvelope{occupied}))
		candidate := capabilityEvent(t, native, "01JB0000000000000000000044")
		require.ErrorIs(t, consumer.ApplyCapabilitiesChanged(t.Context(), candidate),
			agentlink.ErrCapabilitiesConflict)
		manifest, updatedAt := readCapabilityManifest(t, store)
		require.Equal(t, native, manifest, "failed audit append must roll the CAS back")
		require.Equal(t, revision, updatedAt)
	})

	t.Run("new id after durable transition", func(t *testing.T) {
		t.Parallel()
		store, consumer := openCapabilityStore(t)
		native := capabilityNativeManifest()
		insertCapabilitySession(t, store, native)
		first := capabilityEvent(t, native, "01JB0000000000000000000045")
		require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), first))
		second := capabilityEvent(t, native, "01JB0000000000000000000046")
		require.ErrorIs(t, consumer.ApplyCapabilitiesChanged(t.Context(), second),
			agentlink.ErrCapabilitiesConflict)
	})
}

func TestCapabilityStoreAppliesBidirectionalAuditedChain(t *testing.T) {
	t.Parallel()
	store, consumer := openCapabilityStore(t)
	native := capabilityNativeManifest()
	native.Capabilities.ToolEvents = true
	native.Capabilities.ApprovalEvents = true
	generic := adapters.GenericManifest(native.AgentID, native.Harness, native.HarnessVersion)
	initial := native
	initial.Capabilities = adapters.NativeEventCapabilities(
		native.Capabilities, generic.Capabilities,
	)
	insertCapabilitySession(t, store, initial)

	activation := capabilityTransitionEvent(
		t, native, initial.Capabilities, native.Capabilities,
		"01JB0000000000000000000052", 0,
	)
	degradation := capabilityTransitionEvent(
		t, native, native.Capabilities, initial.Capabilities,
		"01JB0000000000000000000053", 2,
	)
	reactivation := capabilityTransitionEvent(
		t, native, initial.Capabilities, native.Capabilities,
		"01JB0000000000000000000054", 3,
	)
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), activation))
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), degradation))
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), activation),
		"an exact historical replay is a read-only success")
	manifest, _ := readCapabilityManifest(t, store)
	require.Equal(t, initial, manifest)
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), reactivation))
	manifest, _ = readCapabilityManifest(t, store)
	require.Equal(t, native, manifest)

	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	matches, err := consumer.MatchesCapabilitiesChanged(
		t.Context(), tx, capabilitySessionID, initial, native,
	)
	require.NoError(t, err)
	require.True(t, matches)
	require.NoError(t, tx.Rollback())
}

func TestCapabilityStoreActivatesGenericEventGateway(t *testing.T) {
	t.Parallel()
	store, consumer := openCapabilityStore(t)
	initial := adapters.GenericManifest(
		capabilityAgent, protocol.HarnessCodex, "1.2.3",
	)
	target := initial
	target.Capabilities.StructuredEvents = true
	target.Capabilities.ToolEvents = true
	target.Capabilities.ApprovalEvents = true
	insertCapabilitySession(t, store, initial)
	event := capabilityTransitionEvent(
		t, initial, initial.Capabilities, target.Capabilities,
		"01JB0000000000000000000055", 0,
	)
	require.NoError(t, consumer.ApplyCapabilitiesChanged(t.Context(), event))
	manifest, _ := readCapabilityManifest(t, store)
	require.Equal(t, target, manifest)
}

func openCapabilityStore(t *testing.T) (*eventstore.Store, *CapabilityStore) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	consumer, err := NewCapabilityStore(store, capabilityNode)
	require.NoError(t, err)
	return store, consumer
}

func capabilityNativeManifest() protocol.CapabilityManifest {
	return protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            capabilityAgent,
		Harness:            protocol.HarnessCodex,
		HarnessVersion:     "1.2.3",
		AdapterType:        protocol.AdapterTypeNativeAPIThinPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy,
		Deliverable:        true,
		Capabilities: protocol.Capabilities{
			StructuredEvents: true,
			NativeCancel:     true,
			PTYCapture:       true,
			MCP:              true,
		},
	}
}

func insertCapabilitySession(
	t *testing.T,
	store *eventstore.Store,
	manifest protocol.CapabilityManifest,
) string {
	t.Helper()
	manifestJSON, err := json.Marshal(manifest)
	require.NoError(t, err)
	revision := eventstore.FormatTime(capabilityObservedAt.Add(-time.Minute))
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		manifest.AgentID, capabilityNode, manifest.Harness, revision, revision)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, ended_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)`, capabilitySessionID, manifest.AgentID,
		protocol.SessionStateConnected, manifest.CaptureMode, manifest.Deliverable,
		string(manifestJSON), revision, revision)
	require.NoError(t, err)
	return revision
}

func capabilityEvent(
	t *testing.T,
	native protocol.CapabilityManifest,
	eventID protocol.ULID,
) protocol.EventEnvelope {
	t.Helper()
	fallback := adapters.GenericManifest(native.AgentID, native.Harness, native.HarnessVersion)
	return capabilityTransitionEvent(
		t, native, native.Capabilities, fallback.Capabilities, eventID, 0,
	)
}

func capabilityTransitionEvent(
	t *testing.T,
	identity protocol.CapabilityManifest,
	before protocol.Capabilities,
	after protocol.Capabilities,
	eventID protocol.ULID,
	sequence int64,
) protocol.EventEnvelope {
	t.Helper()
	payload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    before,
		After:     after,
	})
	require.NoError(t, err)
	return protocol.EventEnvelope{
		EventID:        eventID,
		SchemaVersion:  protocol.SchemaVersion,
		NodeID:         capabilityNode,
		SessionID:      capabilitySessionID,
		AgentID:        identity.AgentID,
		Harness:        identity.Harness,
		HarnessVersion: identity.HarnessVersion,
		AdapterType:    identity.AdapterType,
		SourceQuality:  protocol.SourceQualityProcessMetrics,
		EventType:      protocol.EventTypeAdapterCapabilitiesChanged,
		Origin:         protocol.OriginSystem,
		WallTime:       capabilityObservedAt,
		Sequence:       sequence,
		Payload:        protocol.Payload(payload),
		Sensitivity:    protocol.SensitivityNormal,
	}
}

func readCapabilityManifest(
	t *testing.T,
	store *eventstore.Store,
) (protocol.CapabilityManifest, string) {
	t.Helper()
	var raw string
	var updatedAt string
	require.NoError(t, store.DB().QueryRowContext(t.Context(),
		"SELECT manifest_json, updated_at FROM sessions WHERE session_id = ?", capabilitySessionID).
		Scan(&raw, &updatedAt))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(raw), &manifest))
	return manifest, updatedAt
}
