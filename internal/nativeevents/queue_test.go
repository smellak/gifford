package nativeevents

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func testLineage() Lineage {
	return Lineage{
		NodeID: "node-a", SessionID: "session-1", AgentID: "agent-1",
		Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
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

func TestQueueDeduplicatesEmitterIDAndRejectsChangedContent(t *testing.T) {
	t.Parallel()
	queue := newTestQueue(t)
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
