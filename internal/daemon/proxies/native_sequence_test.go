package proxies

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

func TestNativeGatewayPreservesAdapterDegradation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		nativeFirst bool
	}{
		{name: "native first", nativeFirst: true},
		{name: "degradation first"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store, capabilityWriter := openCapabilityStore(t)
			manifest := capabilityNativeManifest()
			insertCapabilitySession(t, store, manifest)

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
			durableWriter, err := NewDurableEventStore(store, buffer)
			require.NoError(t, err)

			queue, err := nativeevents.NewQueue(nativeevents.Lineage{
				NodeID: capabilityNode, SessionID: capabilitySessionID,
				AgentID: manifest.AgentID, Harness: manifest.Harness,
				HarnessVersion: manifest.HarnessVersion, AdapterType: manifest.AdapterType,
			})
			require.NoError(t, err)
			nativeReceipt, err := queue.Submit(nativeevents.EmitterTypePlugin, nativeevents.Event{
				Operation: nativeevents.OperationEvent, Sequence: 1,
				EmitterEventID: "native-event-1", Type: protocol.EventTypeTurnStarted,
				ObservedAt: capabilityObservedAt.Add(time.Second).UnixMilli(),
				SessionRef: capabilitySessionID, TurnRef: "turn-1",
			})
			require.NoError(t, err)
			require.True(t, nativeReceipt.Accepted())
			nativeEvent, ok := queue.Head()
			require.True(t, ok)
			require.Equal(t, int64(1), nativeEvent.Sequence,
				"sequence zero remains reserved for adapter.capabilities_changed")
			capabilityChange := capabilityEvent(t, manifest, "01JB0000000000000000000047")

			storeNative := func() {
				receipt, storeErr := durableWriter.StoreDurableEvent(t.Context(), nativeEvent)
				require.NoError(t, storeErr)
				require.Equal(t, protocol.EmitEventStatusStored, receipt.Status)
				queue.Settle(protocol.EmitEventResult{
					EventID: nativeEvent.EventID, Status: receipt.Status,
				})
				require.True(t, nativeReceipt.Wait(t.Context()))
			}
			degrade := func() {
				require.NoError(t,
					capabilityWriter.ApplyCapabilitiesChanged(t.Context(), capabilityChange))
			}
			if test.nativeFirst {
				storeNative()
				degrade()
			} else {
				degrade()
				storeNative()
			}

			replay, err := durableWriter.StoreDurableEvent(t.Context(), nativeEvent)
			require.NoError(t, err)
			require.Equal(t, protocol.EmitEventStatusAlreadyStored, replay.Status)
			require.NoError(t,
				capabilityWriter.ApplyCapabilitiesChanged(t.Context(), capabilityChange))

			events, err := store.SessionEvents(t.Context(), capabilitySessionID, 0, 10)
			require.NoError(t, err)
			require.Equal(t, []protocol.EventEnvelope{capabilityChange, nativeEvent}, events)
		})
	}
}
