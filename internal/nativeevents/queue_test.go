package nativeevents

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/protocol"
)

func testLineage() Lineage {
	return Lineage{
		NodeID: "node-a", SessionID: "session-1", AgentID: "agent-1",
		Harness: protocol.HarnessCodex, HarnessVersion: testHarnessVersion,
		AdapterType: protocol.AdapterTypeGenericPTY,
	}
}

func newTestQueue(t *testing.T) *Queue {
	t.Helper()
	queue, err := NewQueue(testLineage())
	require.NoError(t, err)
	return queue
}

func TestQueueBuildsNativeEnvelopeAndWaitsForDurability(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	event := wireEvent()
	receipt, err := queue.Submit(EmitterTypePlugin, event)
	require.NoError(t, err)
	require.True(t, receipt.Accepted())
	envelope, ok := queue.Head()
	require.True(t, ok)
	require.NoError(t, protocol.ValidateNativeDurableEvent(envelope))
	require.Equal(t, protocol.SourceQualityNativeStructured, envelope.SourceQuality)
	require.Equal(t, int64(1), envelope.Sequence,
		"sequence zero is reserved for adapter.capabilities_changed")
	require.Equal(t, event.TurnRef, envelope.TurnID)
	require.Equal(t, event.ToolRef, envelope.ToolCallID)
	require.Equal(t, event.RequestRef, envelope.CorrelationID)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.False(t, receipt.Wait(ctx), "nothing acknowledges before durable storage")
	queue.Settle(protocol.EmitEventResult{
		EventID: envelope.EventID, Status: protocol.EmitEventStatusStored,
	})
	require.True(t, receipt.Wait(t.Context()))
}

func TestQueueDurablyStampsReceiptTimeInsteadOfEmitterClock(t *testing.T) {
	t.Parallel()
	receivedAt := time.Date(2026, 11, 1, 7, 30, 6, 123000000, time.UTC)
	for name, observedAt := range map[string]time.Time{
		"late emitter observation": receivedAt.Add(-6 * time.Second),
		"current observation":      receivedAt,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			queue := newTestQueue(t)
			queue.now = func() time.Time { return receivedAt }
			event := wireEvent()
			event.ObservedAt = observedAt.UnixMilli()
			receipt, err := queue.Submit(EmitterTypeHook, event)
			require.NoError(t, err)
			require.True(t, receipt.Accepted())
			head, ok := queue.Head()
			require.True(t, ok)
			require.Equal(t, receivedAt, head.WallTime,
				"an emitter clock cannot shorten the native-state TTL")
			payload, err := protocol.DecodeNativeDurableEventPayload(head.Payload)
			require.NoError(t, err)
			require.NotNil(t, payload.ObservedAt)
			require.Equal(t, event.ObservedAt, *payload.ObservedAt,
				"the emitter observation remains available for durable audit")

			queue.Settle(protocol.EmitEventResult{
				EventID: head.EventID, Status: protocol.EmitEventStatusStored,
			})
			_, stored := queue.StoredSnapshot()
			require.Equal(t, receivedAt, stored[0].At,
				"the receipt-time basis survives observation replay")
		})
	}
}

func TestQueueDeduplicatesEmitterIDAndRejectsChangedContent(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	receivedAt := time.Date(2026, 11, 1, 7, 31, 0, 0, time.UTC)
	queue.now = func() time.Time { return receivedAt }
	event := wireEvent()
	first, err := queue.Submit(EmitterTypeHook, event)
	require.NoError(t, err)

	retry := event
	retry.Sequence = 99
	second, err := queue.Submit(EmitterTypeHook, retry)
	require.NoError(t, err)
	require.Same(t, first.event, second.event)

	changed := retry
	changed.Result = protocol.NativeEventResultFailed
	_, err = queue.Submit(EmitterTypeHook, changed)
	require.ErrorIs(t, err, ErrConflict)
	_, err = queue.Submit(EmitterTypePlugin, retry)
	require.ErrorIs(t, err, ErrConflict, "emitter type changes durable source quality")

	envelope, ok := queue.Head()
	require.True(t, ok)
	queue.Settle(protocol.EmitEventResult{
		EventID: envelope.EventID, Status: protocol.EmitEventStatusAlreadyStored,
	})
	require.True(t, second.Wait(t.Context()))

	receivedAt = receivedAt.Add(time.Second)
	third, err := queue.Submit(EmitterTypeHook, retry)
	require.NoError(t, err)
	require.Same(t, first.event, third.event)
	require.True(t, third.Wait(t.Context()), "a later exact retry shares the durable receipt")
	_, queued := queue.Head()
	require.False(t, queued, "an exact retry never creates a second durable row")
	require.Equal(t, envelope.EventID, third.event.envelope.EventID)
	require.Equal(t, envelope.WallTime, third.event.envelope.WallTime,
		"retry reception cannot remint the immutable event")
}

func TestQueueIsBoundedAndLeavesASequenceGap(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	for i := 1; i <= MaxQueuedEvents; i++ {
		event := wireEvent()
		event.Sequence = int64(i)
		event.EmitterEventID = protocol.OpaqueID("event-" + fmtInt(i))
		receipt, err := queue.Submit(EmitterTypeMod, event)
		require.NoError(t, err)
		require.True(t, receipt.Accepted())
	}
	dropped := wireEvent()
	dropped.Sequence = MaxQueuedEvents + 1
	dropped.EmitterEventID = "event-dropped"
	receipt, err := queue.Submit(EmitterTypeMod, dropped)
	require.NoError(t, err)
	require.False(t, receipt.Accepted())
	require.Equal(t, int64(1), queue.Dropped())

	for range MaxQueuedEvents {
		head, ok := queue.Head()
		require.True(t, ok)
		queue.Settle(protocol.EmitEventResult{EventID: head.EventID, Status: protocol.EmitEventStatusStored})
	}
	after := wireEvent()
	after.Sequence = MaxQueuedEvents + 2
	after.EmitterEventID = "event-after-gap"
	receipt, err = queue.Submit(EmitterTypeMod, after)
	require.NoError(t, err)
	require.True(t, receipt.Accepted())
	head, ok := queue.Head()
	require.True(t, ok)
	require.Equal(t, int64(MaxQueuedEvents+2), head.Sequence, "one dropped sequence remains visible")
	require.Equal(t, protocol.SourceQualityNativeHook, head.SourceQuality)
}

func TestRejectedQueueHeadNeverAcknowledges(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	receipt, err := queue.Submit(EmitterTypePlugin, wireEvent())
	require.NoError(t, err)
	head, ok := queue.Head()
	require.True(t, ok)
	queue.Settle(protocol.EmitEventResult{
		EventID: head.EventID, Status: protocol.EmitEventStatusRejected,
		Reason: protocol.EmitEventRejectReasonConflict,
	})
	require.False(t, receipt.Wait(t.Context()))
}

func TestQueueSharesSequenceWithCapabilitiesAndReferencesOnlyStored(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	nativeReceipt, err := queue.Submit(EmitterTypePlugin, wireEvent())
	require.NoError(t, err)
	require.True(t, nativeReceipt.Accepted())

	capabilityReceipt, err := queue.EnqueueCapability(queueCapabilityEvent(t))
	require.NoError(t, err)
	require.True(t, capabilityReceipt.Accepted())

	head, ok := queue.Head()
	require.True(t, ok)
	require.Equal(t, int64(1), head.Sequence)
	queue.Settle(protocol.EmitEventResult{EventID: head.EventID, Status: protocol.EmitEventStatusStored})
	generation, events := queue.StoredSnapshot()
	require.Equal(t, int64(1), generation)
	require.Equal(t, []protocol.StateObservationEvent{{
		EventID: head.EventID, Type: head.EventType, At: head.WallTime,
	}}, events)

	head, ok = queue.Head()
	require.True(t, ok)
	require.Equal(t, int64(2), head.Sequence)
	queue.Settle(protocol.EmitEventResult{
		EventID: head.EventID, Status: protocol.EmitEventStatusRejected,
		Reason: protocol.EmitEventRejectReasonConflict,
	})
	generation, events = queue.StoredSnapshot()
	require.Equal(t, int64(1), generation)
	require.Len(t, events, 1, "rejected events are never observation evidence")
}

func TestQueueStoredSnapshotIsBoundedDurableSuffix(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	receivedAt := time.Date(2026, 11, 1, 8, 55, 0, 0, time.UTC)
	queue.now = func() time.Time { return receivedAt }
	const total = MaxQueuedEvents + 56
	all := make([]protocol.StateObservationEvent, 0, total)
	for sequence := 1; sequence <= total; sequence++ {
		event := wireEvent()
		event.Sequence = int64(sequence)
		event.EmitterEventID = protocol.OpaqueID("stored-" + fmtInt(sequence))
		receipt, err := queue.Submit(EmitterTypePlugin, event)
		require.NoError(t, err)
		require.True(t, receipt.Accepted())
		head, ok := queue.Head()
		require.True(t, ok)
		all = append(all, protocol.StateObservationEvent{
			EventID: head.EventID, Type: head.EventType, At: head.WallTime,
		})
		queue.Settle(protocol.EmitEventResult{
			EventID: head.EventID, Status: protocol.EmitEventStatusStored,
		})
	}

	generation, stored := queue.StoredSnapshot()
	require.Equal(t, int64(total), generation)
	require.Len(t, stored, MaxQueuedEvents)
	require.Equal(t, all[total-MaxQueuedEvents:], stored,
		"the run cuts only the durable prefix when its reference window fills")
}

func TestCapabilityAuditPreemptsNewestNativeEventWhenQueueIsFull(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
	var newest Receipt
	for i := 1; i <= MaxQueuedEvents; i++ {
		event := wireEvent()
		event.Sequence = int64(i)
		event.EmitterEventID = protocol.OpaqueID("event-preempt-" + fmtInt(i))
		receipt, err := queue.Submit(EmitterTypePlugin, event)
		require.NoError(t, err)
		require.True(t, receipt.Accepted())
		newest = receipt
	}

	capability, err := queue.EnqueueCapability(queueCapabilityEvent(t))
	require.NoError(t, err)
	require.True(t, capability.Accepted(), "failure auditing takes priority over lifecycle detail")
	require.False(t, newest.Wait(t.Context()), "the newest unsent native event is refused")
	require.Equal(t, int64(1), queue.Dropped())

	for range MaxQueuedEvents - 1 {
		head, ok := queue.Head()
		require.True(t, ok)
		require.NotEqual(t, protocol.EventTypeAdapterCapabilitiesChanged, head.EventType)
		queue.Settle(protocol.EmitEventResult{
			EventID: head.EventID, Status: protocol.EmitEventStatusStored,
		})
	}
	head, ok := queue.Head()
	require.True(t, ok)
	require.Equal(t, protocol.EventTypeAdapterCapabilitiesChanged, head.EventType)
	require.Equal(t, int64(MaxQueuedEvents+1), head.Sequence,
		"the preempted native sequence remains a visible gap")
}

func queueCapabilityEvent(t *testing.T) protocol.EventEnvelope {
	t.Helper()
	payload, err := json.Marshal(adapters.CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    protocol.Capabilities{PTYCapture: true, MCP: true},
		After: protocol.Capabilities{
			StructuredEvents: true, PTYCapture: true, MCP: true,
		},
	})
	require.NoError(t, err)
	lineage := testLineage()
	return protocol.EventEnvelope{
		EventID: "01JB0000000000000000000051", SchemaVersion: protocol.SchemaVersion,
		NodeID: lineage.NodeID, SessionID: lineage.SessionID, AgentID: lineage.AgentID,
		Harness: lineage.Harness, HarnessVersion: lineage.HarnessVersion,
		AdapterType: lineage.AdapterType, SourceQuality: protocol.SourceQualityProcessMetrics,
		EventType: protocol.EventTypeAdapterCapabilitiesChanged, Origin: protocol.OriginSystem,
		WallTime: time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC), Payload: payload,
		Sensitivity: protocol.SensitivityNormal,
	}
}

func fmtInt(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var reversed [20]byte
	n := len(reversed)
	for value > 0 {
		n--
		reversed[n] = digits[value%10]
		value /= 10
	}
	return string(reversed[n:])
}
