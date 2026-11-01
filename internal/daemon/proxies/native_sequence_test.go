package proxies

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

func TestNativeGatewaySharesSequenceAcrossCapabilityEpochs(t *testing.T) {
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
			native := capabilityNativeManifest()
			native.Capabilities.ToolEvents = true
			native.Capabilities.ApprovalEvents = true
			generic := adapters.GenericManifest(
				native.AgentID, native.Harness, native.HarnessVersion,
			)
			initial := native
			initial.Capabilities = adapters.NativeEventCapabilities(
				native.Capabilities, generic.Capabilities,
			)
			insertCapabilitySession(t, store, initial)

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
				AgentID: native.AgentID, Harness: native.Harness,
				HarnessVersion: native.HarnessVersion, AdapterType: native.AdapterType,
			})
			require.NoError(t, err)

			activation := capabilityTransitionEvent(
				t, native, initial.Capabilities, native.Capabilities,
				"01JB0000000000000000000060", 0,
			)
			degradation := capabilityTransitionEvent(
				t, native, native.Capabilities, initial.Capabilities,
				"01JB0000000000000000000061", 0,
			)
			reactivation := capabilityTransitionEvent(
				t, native, initial.Capabilities, native.Capabilities,
				"01JB0000000000000000000062", 0,
			)
			nativeReceipts := make([]nativeevents.Receipt, 0, 3)
			enqueueNative := func(number int) {
				receipt, submitErr := queue.Submit(
					nativeevents.EmitterTypePlugin,
					nativeevents.Event{
						Operation: nativeevents.OperationEvent, Sequence: int64(number),
						EmitterEventID: protocol.OpaqueID("native-event-" + fmtInt(number)),
						Type:           protocol.EventTypeTurnStarted,
						ObservedAt: capabilityObservedAt.Add(
							time.Duration(number) * time.Second,
						).UnixMilli(),
						SessionRef: capabilitySessionID,
						TurnRef:    protocol.OpaqueID("turn-" + fmtInt(number)),
					},
				)
				require.NoError(t, submitErr)
				require.True(t, receipt.Accepted())
				nativeReceipts = append(nativeReceipts, receipt)
			}
			enqueueNative(1)
			degradationReceipt, err := queue.EnqueueCapability(degradation)
			require.NoError(t, err)
			require.True(t, degradationReceipt.Accepted())
			enqueueNative(2)
			reactivationReceipt, err := queue.EnqueueCapability(reactivation)
			require.NoError(t, err)
			require.True(t, reactivationReceipt.Accepted())
			enqueueNative(3)

			sequenced := make([]protocol.EventEnvelope, 0, 5)
			for range 5 {
				event, ok := queue.Head()
				require.True(t, ok)
				sequenced = append(sequenced, event)
				queue.Refuse(event.EventID)
			}
			require.Equal(t, []int64{1, 2, 3, 4, 5}, []int64{
				sequenced[0].Sequence, sequenced[1].Sequence, sequenced[2].Sequence,
				sequenced[3].Sequence, sequenced[4].Sequence,
			})
			require.Equal(t, protocol.EventTypeAdapterCapabilitiesChanged, sequenced[1].EventType)
			require.Equal(t, protocol.EventTypeAdapterCapabilitiesChanged, sequenced[3].EventType)
			nativeEvents := []protocol.EventEnvelope{sequenced[0], sequenced[2], sequenced[4]}
			capabilityEvents := []protocol.EventEnvelope{activation, sequenced[1], sequenced[3]}

			storeNative := func(event protocol.EventEnvelope) {
				receipt, storeErr := durableWriter.StoreDurableEvent(t.Context(), event)
				require.NoError(t, storeErr)
				require.Equal(t, protocol.EmitEventStatusStored, receipt.Status)
			}
			storeCapability := func(event protocol.EventEnvelope) {
				require.NoError(t, capabilityWriter.ApplyCapabilitiesChanged(t.Context(), event))
			}
			for i := range nativeEvents {
				if test.nativeFirst {
					storeNative(nativeEvents[i])
					storeCapability(capabilityEvents[i])
				} else {
					storeCapability(capabilityEvents[i])
					storeNative(nativeEvents[i])
				}
			}

			for _, event := range nativeEvents {
				replay, replayErr := durableWriter.StoreDurableEvent(t.Context(), event)
				require.NoError(t, replayErr)
				require.Equal(t, protocol.EmitEventStatusAlreadyStored, replay.Status)
			}
			for _, event := range capabilityEvents {
				require.NoError(t, capabilityWriter.ApplyCapabilitiesChanged(t.Context(), event),
					"exact historical capability replay is read-only")
			}

			events, err := store.SessionEvents(t.Context(), capabilitySessionID, 0, 10)
			require.NoError(t, err)
			require.Equal(t, []protocol.EventEnvelope{
				activation, sequenced[0], sequenced[1], sequenced[2], sequenced[3], sequenced[4],
			}, events)
			manifest, _ := readCapabilityManifest(t, store)
			require.Equal(t, native, manifest)
			tx, err := store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			matches, err := capabilityWriter.MatchesCapabilitiesChanged(
				t.Context(), tx, capabilitySessionID, initial, native,
			)
			require.NoError(t, err)
			require.True(t, matches)
			require.NoError(t, tx.Rollback())
		})
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
