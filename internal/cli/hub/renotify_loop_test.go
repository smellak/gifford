package hub

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/renotify"
)

type renotifyLoopClock struct {
	ticks chan time.Time
}

func (c *renotifyLoopClock) Now() time.Time { return time.Now() }

func (c *renotifyLoopClock) After(time.Duration) <-chan time.Time { return c.ticks }

type renotifyLoopTurner struct {
	mu    sync.Mutex
	calls int
}

func (w *renotifyLoopTurner) RunOnce(_ context.Context, _ time.Time, _ []renotify.Item) (time.Time, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	return time.Time{}, nil
}

func (w *renotifyLoopTurner) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

func TestRenotifyLoopStartsAndStopsWithTheNode(t *testing.T) {
	t.Parallel()

	clk := &renotifyLoopClock{ticks: make(chan time.Time, 1)}
	turner := &renotifyLoopTurner{}
	loader := func(context.Context) ([]renotify.Item, error) { return nil, nil }
	var output bytes.Buffer
	loop := startRenotifyLoop(t.Context(), &output, turner, loader, clk)
	require.Eventually(t, func() bool { return turner.count() == 1 }, time.Second, time.Millisecond,
		"the node must start the backup loop")
	clk.ticks <- time.Now()
	require.Eventually(t, func() bool { return turner.count() == 2 }, time.Second, time.Millisecond,
		"the backup loop must keep ticking")
	loop.stop()
	calls := turner.count()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, calls, turner.count(), "no turn runs after the node stops the loop")
}
