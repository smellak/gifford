package proxies

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func TestDurableEventStoreReceiptsAndExactReplay(t *testing.T) {
	t.Parallel()
	store, buffer, writer := openDurableEventStore(t)
	original := durableStoreEvent("01JB0000000000000000000081", 1)

	receipt, err := writer.StoreDurableEvent(t.Context(), original)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusStored, receipt.Status)
	require.Empty(t, receipt.Reason)

	receipt, err = writer.StoreDurableEvent(t.Context(), original)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusAlreadyStored, receipt.Status)
	require.Empty(t, receipt.Reason)

	changed := original
	changed.Payload = protocol.Payload(`{"byte_count":2,"ansi_present":false,"recording_sequence":1}`)
	receipt, err = writer.StoreDurableEvent(t.Context(), changed)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusRejected, receipt.Status)
	require.Equal(t, protocol.EmitEventRejectReasonConflict, receipt.Reason)

	sequenceCollision := durableStoreEvent("01JB0000000000000000000082", original.Sequence)
	receipt, err = writer.StoreDurableEvent(t.Context(), sequenceCollision)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusRejected, receipt.Status)
	require.Equal(t, protocol.EmitEventRejectReasonConflict, receipt.Reason)

	require.NoError(t, buffer.Flush(t.Context()))
	events, err := store.SessionEvents(t.Context(), original.SessionID, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []protocol.EventEnvelope{original}, events)
}

func TestDurableEventStoreDoesNotEquatePartialEnvelope(t *testing.T) {
	t.Parallel()
	_, _, writer := openDurableEventStore(t)
	original := durableStoreEvent("01JB0000000000000000000083", 2)
	receipt, err := writer.StoreDurableEvent(t.Context(), original)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusStored, receipt.Status)

	for name, change := range map[string]func(*protocol.EventEnvelope){
		"wall time": func(event *protocol.EventEnvelope) { event.WallTime = event.WallTime.Add(time.Nanosecond) },
		"monotonic": func(event *protocol.EventEnvelope) { event.MonotonicNS++ },
		"harness":   func(event *protocol.EventEnvelope) { event.HarnessVersion = "different" },
		"payload": func(event *protocol.EventEnvelope) {
			event.Payload = protocol.Payload(`{"byte_count":1,"ansi_present":true,"recording_sequence":2}`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			replay := original
			change(&replay)
			receipt, err := writer.StoreDurableEvent(t.Context(), replay)
			require.NoError(t, err)
			require.Equal(t, protocol.EmitEventStatusRejected, receipt.Status)
			require.Equal(t, protocol.EmitEventRejectReasonConflict, receipt.Reason)
		})
	}
}

func TestDurableCaptureStateAppliesOnlyNewEventLimits(t *testing.T) {
	t.Parallel()
	const captureID = "capture-1"
	quota := durableCaptureMarker{id: captureID, quota: true}
	quotaGap := durableCaptureMarker{
		id: captureID, gap: true, cause: protocol.CaptureGapCauseQuota,
	}
	ingestGap := durableCaptureMarker{
		id: captureID, gap: true, cause: protocol.CaptureGapCauseIngestDied,
	}
	otherQuota := durableCaptureMarker{id: "capture-2", quota: true}

	var state durableCaptureState
	require.True(t, state.admits(quota))
	state.apply(quota)
	require.False(t, state.admits(quota), "only one quota marker is new-event admissible")
	require.False(t, state.admits(otherQuota), "the first capture_session_id fixes the session")
	require.True(t, state.admits(quotaGap), "a quota gap follows its quota marker")
	state.apply(quotaGap)
	require.False(t, state.admits(ingestGap), "only one gap marker is new-event admissible")

	state = durableCaptureState{}
	require.False(t, state.admits(quotaGap), "a quota gap cannot precede its quota marker")
	require.True(t, state.admits(ingestGap), "ingest death does not require a quota marker")
}

func TestDurableCaptureMarkersPersistAndReplayBeforeLimits(t *testing.T) {
	t.Parallel()
	store, _, writer := openDurableEventStore(t)
	quota := durableStoreEvent("01JB0000000000000000000084", 4)
	quota.EventType = protocol.EventTypeCaptureQuotaReached
	quota.Direction = ""
	quota.Origin = protocol.OriginSystem
	quota.Payload = protocol.Payload(
		`{"capture_session_id":"capture-1","quota_scope":"agent","bytes":101,"limit_bytes":100}`,
	)

	receipt, err := writer.StoreDurableEvent(t.Context(), quota)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusStored, receipt.Status)
	receipt, err = writer.StoreDurableEvent(t.Context(), quota)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusAlreadyStored, receipt.Status,
		"exact replay is resolved before the one-quota limit")

	secondQuota := quota
	secondQuota.EventID = "01JB0000000000000000000085"
	secondQuota.Sequence++
	receipt, err = writer.StoreDurableEvent(t.Context(), secondQuota)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusRejected, receipt.Status)
	require.Equal(t, protocol.EmitEventRejectReasonNotAllowed, receipt.Reason)

	gap := quota
	gap.EventID = "01JB0000000000000000000086"
	gap.Sequence += 2
	gap.EventType = protocol.EventTypeCaptureGapDetected
	gap.Payload = protocol.Payload(`{"capture_session_id":"capture-1","cause":"quota"}`)
	receipt, err = writer.StoreDurableEvent(t.Context(), gap)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusStored, receipt.Status)
	receipt, err = writer.StoreDurableEvent(t.Context(), gap)
	require.NoError(t, err)
	require.Equal(t, protocol.EmitEventStatusAlreadyStored, receipt.Status,
		"exact replay is resolved before the one-gap limit")

	events, err := store.SessionEvents(t.Context(), quota.SessionID, 0, 10)
	require.NoError(t, err)
	require.Equal(t, []protocol.EventEnvelope{quota, gap}, events)
}

func openDurableEventStore(
	t *testing.T,
) (*eventstore.Store, *eventstore.Buffer, *DurableEventStore) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	buffer, err := eventstore.NewBuffer(store, eventstore.BufferOptions{
		RetryInterval: time.Millisecond,
		OnRejected:    func(context.Context, eventstore.Rejection) {},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := buffer.Close(ctx)
		if err != nil {
			require.ErrorIs(t, err, eventstore.ErrBufferClosed)
		}
	})
	writer, err := NewDurableEventStore(store, buffer)
	require.NoError(t, err)
	return store, buffer, writer
}

func durableStoreEvent(id protocol.ULID, sequence int64) protocol.EventEnvelope {
	return protocol.EventEnvelope{
		EventID:        id,
		SchemaVersion:  protocol.SchemaVersion,
		NodeID:         "node-durable",
		SessionID:      "session-durable",
		AgentID:        "agent-durable",
		Harness:        protocol.HarnessCodex,
		HarnessVersion: "9.8.7",
		AdapterType:    protocol.AdapterTypeGenericPTY,
		SourceQuality:  protocol.SourceQualityPTYScreen,
		EventType:      protocol.EventTypeIOChunk,
		Direction:      protocol.DirectionOut,
		Stream:         protocol.StreamPTY,
		Origin:         protocol.OriginHarness,
		WallTime:       time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC),
		MonotonicNS:    sequence,
		Sequence:       sequence,
		Payload:        protocol.Payload(`{"byte_count":1,"ansi_present":false,"recording_sequence":1}`),
		Sensitivity:    protocol.SensitivityNormal,
	}
}
