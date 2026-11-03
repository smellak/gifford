package renotify

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeTurner struct {
	mu   sync.Mutex
	runs int
	next time.Time
}

func (f *fakeTurner) RunOnce(_ context.Context, _ time.Time, _ []Item) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs++
	return f.next, nil
}

func (f *fakeTurner) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs
}

type tickClock struct {
	now time.Time
}

func (c tickClock) Now() time.Time { return c.now }

func (c tickClock) After(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.now
	return ch
}

func TestDriverRunsTurnsUntilStopped(t *testing.T) {
	t.Parallel()

	turner := &fakeTurner{next: loopT0.Add(time.Hour)}
	loader := func(context.Context) ([]Item, error) { return []Item{loopItem(0)}, nil }
	driver := &Driver{Turner: turner, Loader: loader, Clock: tickClock{now: loopT0}, ErrW: io.Discard}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		driver.Run(ctx)
	}()
	require.Eventually(t, func() bool { return turner.count() >= 2 }, 5*time.Second, time.Millisecond,
		"driver must keep ticking")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("driver did not stop")
	}
	runs := turner.count()
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, runs, turner.count(), "no turn runs after stop")
}
