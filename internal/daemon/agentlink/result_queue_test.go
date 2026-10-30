package agentlink

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeliveryResultQueueIsBoundedAndBackpressuresWithoutDropping(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &Client{ctx: ctx, results: make(chan DeliveryResult, maxDeliveryResultBacklog)}
	for id := int64(1); id <= maxDeliveryResultBacklog; id++ {
		require.NoError(t, client.enqueueResult(DeliveryResult{FrameID: id, ReplyTo: id}))
	}
	want := DeliveryResult{
		FrameID: maxDeliveryResultBacklog + 1,
		ReplyTo: maxDeliveryResultBacklog + 1,
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- client.enqueueResult(want)
	}()
	<-started
	select {
	case err := <-done:
		t.Fatalf("full result queue returned instead of applying backpressure: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	require.Len(t, client.results, maxDeliveryResultBacklog)
	require.Equal(t, int64(1), (<-client.results).FrameID)
	require.NoError(t, <-done)
	require.Len(t, client.results, maxDeliveryResultBacklog)
	for id := int64(2); id <= maxDeliveryResultBacklog+1; id++ {
		require.Equal(t, id, (<-client.results).FrameID)
	}
}

func TestAcceptedDeliveryResultRemainsDrainableAfterClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &Client{ctx: ctx, results: make(chan DeliveryResult, 1)}
	want := DeliveryResult{FrameID: 7, ReplyTo: 6}
	require.NoError(t, client.enqueueResult(want))
	close(client.results)

	got, ok := <-client.DeliveryResults()
	require.True(t, ok)
	require.Equal(t, want, got)
	_, ok = <-client.DeliveryResults()
	require.False(t, ok)
}
