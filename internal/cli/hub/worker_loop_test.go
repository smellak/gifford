package hub

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type workerLoopClock struct {
	mu        sync.Mutex
	durations []time.Duration
	ticks     chan time.Time
}

func (*workerLoopClock) Now() time.Time { return time.Time{} }

func (c *workerLoopClock) After(duration time.Duration) <-chan time.Time {
	c.mu.Lock()
	c.durations = append(c.durations, duration)
	c.mu.Unlock()
	return c.ticks
}

type workerLoopBackend struct {
	mu    sync.Mutex
	calls int
}

func (w *workerLoopBackend) RunOnce(context.Context) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	if w.calls == 1 {
		return 0, errors.New("first pass failed")
	}
	return 1, nil
}

func (w *workerLoopBackend) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func TestWorkerLoopContinuesAfterErrorsAndStopsSynchronously(t *testing.T) {
	t.Parallel()

	clk := &workerLoopClock{ticks: make(chan time.Time, 1)}
	worker := &workerLoopBackend{}
	var output bytes.Buffer
	loop := startWorkerLoop(t.Context(), &output, worker, clk)
	require.Eventually(t, func() bool { return worker.count() == 1 }, time.Second, time.Millisecond)
	clk.ticks <- time.Time{}
	require.Eventually(t, func() bool { return worker.count() == 2 }, time.Second, time.Millisecond)
	loop.stop()
	require.Contains(t, output.String(), "first pass failed")
	clk.mu.Lock()
	durations := append([]time.Duration(nil), clk.durations...)
	clk.mu.Unlock()
	require.NotEmpty(t, durations)
	for _, duration := range durations {
		require.Equal(t, workerEvery, duration)
	}
}
