package agentlink

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeliveryResultQueueIsBoundedAndFailsClosed(t *testing.T) {
	t.Parallel()
	client := &Client{results: make(chan DeliveryResult, maxDeliveryResultBacklog)}
	for id := int64(1); id <= maxDeliveryResultBacklog; id++ {
		require.NoError(t, client.enqueueResult(DeliveryResult{FrameID: id, ReplyTo: id}))
	}
	err := client.enqueueResult(DeliveryResult{FrameID: maxDeliveryResultBacklog + 1})
	require.ErrorIs(t, err, ErrDeliveryResultQueueFull)
	require.Equal(t, ReasonResultQueueFull, reasonFor(err))
	require.Len(t, client.results, maxDeliveryResultBacklog)
}

func TestAcceptedDeliveryResultRemainsDrainableAfterClose(t *testing.T) {
	t.Parallel()
	client := &Client{results: make(chan DeliveryResult, 1)}
	want := DeliveryResult{FrameID: 7, ReplyTo: 6}
	require.NoError(t, client.enqueueResult(want))
	close(client.results)

	got, ok := <-client.DeliveryResults()
	require.True(t, ok)
	require.Equal(t, want, got)
	_, ok = <-client.DeliveryResults()
	require.False(t, ok)
}
