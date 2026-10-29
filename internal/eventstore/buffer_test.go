package eventstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func testBuffer(t *testing.T, store *Store, opts BufferOptions) *Buffer {
	t.Helper()
	if opts.OnRejected == nil {
		opts.OnRejected = func(context.Context, Rejection) {}
	}
	buffer, err := NewBuffer(store, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		err := buffer.Close(ctx)
		if !errors.Is(err, ErrBufferClosed) {
			require.NoError(t, err)
		}
	})
	return buffer
}

func waitBuffer(t *testing.T, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			require.FailNow(t, "buffer state timeout")
		}
	}
}

func bufferWaiters(buffer *Buffer) int {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return len(buffer.waiters)
}

func installBufferFailure(t *testing.T, store *Store) func(bool) {
	t.Helper()
	_, err := store.DB().ExecContext(t.Context(), `
		CREATE TABLE buffer_fail (active INTEGER NOT NULL);
		INSERT INTO buffer_fail VALUES (1);
		CREATE TRIGGER buffer_fail_events BEFORE INSERT ON events
		WHEN EXISTS (SELECT 1 FROM buffer_fail WHERE active = 1)
		BEGIN SELECT RAISE(FAIL, 'buffer write failed'); END;`)
	require.NoError(t, err)
	return func(active bool) {
		value := 0
		if active {
			value = 1
		}
		_, err := store.DB().ExecContext(t.Context(), "UPDATE buffer_fail SET active = ?", value)
		require.NoError(t, err)
	}
}

func eventBatch(writer int, session string, start, count int64) []protocol.EventEnvelope {
	batch := make([]protocol.EventEnvelope, 0, count)
	for sequence := start; sequence < start+count; sequence++ {
		batch = append(batch, testEvent(writer, session, sequence))
	}
	return batch
}

func closeOnce(ch chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(ch) }) }
}

func requireClosedBalanced(t *testing.T, buffer *Buffer, accepted int64) BufferStats {
	t.Helper()
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosed })
	stats := buffer.Stats()
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)
	require.Equal(t, accepted, stats.Stored+stats.Rejected+stats.NotStored)
	return stats
}

func TestBufferRoundTrip(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{})
	for batch := range int64(3) {
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "round", batch*2, 2)))
	}
	require.NoError(t, buffer.Flush(t.Context()))
	events, err := store.SessionEvents(t.Context(), "round", 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 6)
	for i := range events {
		require.Equal(t, int64(i), events[i].Sequence)
	}
	stats := buffer.Stats()
	require.Equal(t, int64(6), stats.Stored)
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)
	require.Equal(t, BufferOpen, stats.State)
}

func TestBufferOptions(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	cases := []struct {
		name  string
		store *Store
		opts  BufferOptions
	}{
		{"nil store", nil, BufferOptions{OnRejected: func(context.Context, Rejection) {}}},
		{"nil callback", store, BufferOptions{}},
		{"negative events", store, BufferOptions{MaxEvents: -1, OnRejected: func(context.Context, Rejection) {}}},
		{"many events", store, BufferOptions{MaxEvents: BufferMaxEvents + 1, OnRejected: func(context.Context, Rejection) {}}},
		{"negative bytes", store, BufferOptions{MaxBytes: -1, OnRejected: func(context.Context, Rejection) {}}},
		{"many bytes", store, BufferOptions{MaxBytes: BufferMaxBytes + 1, OnRejected: func(context.Context, Rejection) {}}},
		{"negative retry", store, BufferOptions{RetryInterval: -time.Millisecond, OnRejected: func(context.Context, Rejection) {}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			buffer, err := NewBuffer(tc.store, tc.opts)
			require.Nil(t, buffer)
			require.ErrorIs(t, err, ErrInvalidArgument)
		})
	}
	buffer := testBuffer(t, store, BufferOptions{})
	require.False(t, buffer.writerStarted)
	require.NoError(t, buffer.Close(t.Context()))
	require.False(t, buffer.writerStarted)
	require.Equal(t, BufferClosed, buffer.Stats().State)
}

func TestBufferRefusals(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	base := testEvent(1, "refuse", 0)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 2, MaxBytes: batchBytes([]protocol.EventEnvelope{base})})
	require.NoError(t, buffer.Enqueue(t.Context(), nil))
	require.ErrorIs(t, buffer.Enqueue(t.Context(), make([]protocol.EventEnvelope, MaxBatchEvents+1)), ErrInvalidArgument)
	require.ErrorIs(t, buffer.Enqueue(t.Context(), eventBatch(1, "refuse", 0, 3)), ErrInvalidArgument)
	large := base
	large.Payload = append(large.Payload, ' ')
	require.ErrorIs(t, buffer.Enqueue(t.Context(), []protocol.EventEnvelope{large}), ErrInvalidArgument)

	validationBuffer := testBuffer(t, store, BufferOptions{})
	broken := eventBatch(1, "broken", 0, 3)
	broken[2].EventID = ""
	err := validationBuffer.Enqueue(t.Context(), broken)
	require.Error(t, err)
	require.Contains(t, err.Error(), "event 2:")
	require.ErrorIs(t, err, protocol.ErrInvalid)
	for _, candidate := range []*Buffer{buffer, validationBuffer} {
		stats := candidate.Stats()
		require.Zero(t, stats.Events)
		require.Zero(t, stats.Bytes)
		require.False(t, candidate.writerStarted)
	}
}

func TestBufferCancelledContext(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := buffer.Enqueue(ctx, eventBatch(1, "cancelled", 0, 1))
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, buffer.Flush(t.Context()))
	require.False(t, buffer.writerStarted)
	require.Zero(t, testEventCount(t.Context(), t, store))
}

func TestBufferMutationAfterReturn(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{})
	batch := eventBatch(1, "mutation", 0, 2)
	want := cloneEvents(batch)
	require.NoError(t, buffer.Enqueue(t.Context(), batch))
	batch[0].Payload[0] = '!'
	batch[0].HarnessVersion = "changed"
	batch = batch[:1]
	require.Len(t, batch, 1)
	require.NoError(t, buffer.Flush(t.Context()))
	got, err := store.SessionEvents(t.Context(), "mutation", 0, 10)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestBufferBackpressureInFlight(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	setFailure := installBufferFailure(t, store)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 10, RetryInterval: time.Millisecond})
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "pressure", 0, 10)))
	waitBuffer(t, func() bool { return buffer.Stats().Degraded })
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- buffer.Enqueue(ctx, eventBatch(2, "waiting", 0, 1)) }()
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	require.Equal(t, int64(10), buffer.Stats().Events)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.Equal(t, int64(10), buffer.Stats().Events)
	setFailure(false)
	require.NoError(t, buffer.Flush(t.Context()))
	stats := buffer.Stats()
	require.Equal(t, int64(10), stats.Stored)
	require.False(t, stats.Degraded)
}

func TestBufferBackpressureBytes(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	setFailure := installBufferFailure(t, store)
	batch := eventBatch(1, "bytes", 0, 2)
	buffer := testBuffer(t, store, BufferOptions{MaxBytes: batchBytes(batch), RetryInterval: time.Millisecond})
	require.NoError(t, buffer.Enqueue(t.Context(), batch))
	waitBuffer(t, func() bool { return buffer.Stats().Degraded })
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- buffer.Enqueue(ctx, eventBatch(2, "byte-wait", 0, 1)) }()
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	setFailure(false)
	require.NoError(t, buffer.Flush(t.Context()))
	require.Equal(t, int64(2), buffer.Stats().Stored)
}

func TestBufferArrivalOrder(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	setFailure := installBufferFailure(t, store)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 10, RetryInterval: time.Millisecond})
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "held", 0, 5)))
	waitBuffer(t, func() bool { return buffer.Stats().Degraded })
	results := make(chan error, 2)
	go func() { results <- buffer.Enqueue(t.Context(), eventBatch(2, "large", 0, 6)) }()
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	go func() { results <- buffer.Enqueue(t.Context(), eventBatch(3, "small", 0, 1)) }()
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 2 })
	setFailure(false)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.NoError(t, buffer.Flush(t.Context()))
	require.Equal(t, int64(12), buffer.Stats().Stored)
}

func TestBufferDegraded(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	setFailure := installBufferFailure(t, store)
	buffer := testBuffer(t, store, BufferOptions{RetryInterval: time.Millisecond})
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "degraded", 0, 3)))
	waitBuffer(t, func() bool { return buffer.Stats().Degraded })
	stats := buffer.Stats()
	require.Error(t, stats.LastError)
	require.Zero(t, stats.Stored)
	require.Equal(t, int64(3), stats.Events)
	setFailure(false)
	require.NoError(t, buffer.Flush(t.Context()))
	stats = buffer.Stats()
	require.False(t, stats.Degraded)
	require.NoError(t, stats.LastError)
	require.Equal(t, int64(3), stats.Stored)
}

func TestBufferRejected(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	duplicate := eventBatch(1, "duplicate", 0, 3)
	require.NoError(t, store.AppendEvents(t.Context(), duplicate))
	rejections := make(chan Rejection, 1)
	buffer := testBuffer(t, store, BufferOptions{OnRejected: func(ctx context.Context, rejection Rejection) {
		require.NoError(t, ctx.Err())
		for i := range rejection.Batch {
			rejection.Batch[i].Payload = nil
		}
		rejection.Batch = rejection.Batch[:0]
		rejections <- rejection
	}})
	require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "later", 0, 1)))
	require.NoError(t, buffer.Flush(t.Context()))
	rejection := <-rejections
	require.ErrorIs(t, rejection.Err, ErrDuplicateEvent)
	stats := buffer.Stats()
	require.Equal(t, int64(3), stats.Rejected)
	require.Equal(t, int64(1), stats.Stored)
	require.Zero(t, stats.Events)
}

func TestBufferFlushBarrier(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{})
	flushMarked := make(chan struct{})
	resumeFlush := make(chan struct{})
	resumeFlushOnce := closeOnce(resumeFlush)
	suffixAttempted := make(chan struct{})
	releaseSuffix := make(chan struct{})
	releaseSuffixOnce := closeOnce(releaseSuffix)
	t.Cleanup(resumeFlushOnce)
	t.Cleanup(releaseSuffixOnce)
	buffer.hooks.afterFlushMark = func(uint64) {
		close(flushMarked)
		<-resumeFlush
	}
	var attempts atomic.Int64
	buffer.hooks.afterAttempt = func(error) {
		if attempts.Add(1) == 6 {
			close(suffixAttempted)
			<-releaseSuffix
		}
	}
	for sequence := range int64(5) {
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "prefix", sequence, 1)))
	}
	result := make(chan error, 1)
	go func() { result <- buffer.Flush(t.Context()) }()
	<-flushMarked
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "suffix", 0, 1)))
	resumeFlushOnce()
	<-suffixAttempted
	require.NoError(t, <-result)
	require.Equal(t, int64(5), buffer.Stats().Stored)
	require.Equal(t, int64(1), buffer.Stats().Events)
	releaseSuffixOnce()
	buffer.hooks.afterFlushMark = nil
	require.NoError(t, buffer.Flush(t.Context()))
}

func TestBufferCloseDeadline(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	installBufferFailure(t, store)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 6, RetryInterval: time.Millisecond})
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "close", 0, 3)))
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "queued", 0, 3)))
	waitBuffer(t, func() bool { return buffer.Stats().Degraded })
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := buffer.Close(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "6 events not stored")
	stats := requireClosedBalanced(t, buffer, 6)
	require.Equal(t, int64(6), stats.NotStored)
	require.ErrorIs(t, buffer.Enqueue(t.Context(), eventBatch(3, "no", 0, 1)), ErrBufferClosed)
	require.ErrorIs(t, buffer.Flush(t.Context()), ErrBufferClosed)
	require.ErrorIs(t, buffer.Close(t.Context()), ErrBufferClosed)
}

func TestBufferCloseWakesWaiters(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	installBufferFailure(t, store)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 3, RetryInterval: time.Millisecond})
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "accepted", 0, 3)))
	waitBuffer(t, func() bool { return buffer.Stats().Degraded })
	result := make(chan error, 1)
	go func() { result <- buffer.Enqueue(t.Context(), eventBatch(2, "wait", 0, 1)) }()
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := buffer.Close(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "3 events not stored")
	require.ErrorIs(t, <-result, ErrBufferClosed)
	stats := requireClosedBalanced(t, buffer, 3)
	require.Equal(t, int64(3), stats.NotStored)
}

func TestBufferCallbackCooperative(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	duplicate := eventBatch(1, "callback", 0, 1)
	require.NoError(t, store.AppendEvents(t.Context(), duplicate))
	callbackStarted := make(chan struct{})
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 5, OnRejected: func(ctx context.Context, _ Rejection) {
		close(callbackStarted)
		<-ctx.Done()
	}})
	require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
	<-callbackStarted
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "callback-queued", 0, 4)))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := buffer.Close(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "4 events not stored")
	require.NotContains(t, err.Error(), "still in OnRejected")
	stats := requireClosedBalanced(t, buffer, 5)
	require.Equal(t, int64(1), stats.Rejected)
	require.Equal(t, int64(4), stats.NotStored)
}

func TestBufferCommitWinsCancel(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 5})
	attempted := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := closeOnce(release)
	t.Cleanup(releaseOnce)
	var once sync.Once
	buffer.hooks.afterAttempt = func(err error) {
		if err == nil {
			once.Do(func() { close(attempted) })
			<-release
		}
	}
	buffer.closeGrace = time.Second
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "commit", 0, 3)))
	<-attempted
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "commit-queued", 0, 2)))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := make(chan error, 1)
	go func() { result <- buffer.Close(ctx) }()
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	releaseOnce()
	err := <-result
	require.ErrorIs(t, err, context.Canceled)
	require.EqualError(t, err, "close: 2 events not stored: context canceled")
	stats := requireClosedBalanced(t, buffer, 5)
	require.Equal(t, int64(3), stats.Stored)
	require.Equal(t, int64(2), stats.NotStored)
	require.Equal(t, 3, testEventCount(t.Context(), t, store))
}

func TestBufferCloseWaitsReservation(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 6})
	reserved := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := closeOnce(release)
	t.Cleanup(releaseOnce)
	buffer.hooks.afterReserve = func() {
		close(reserved)
		<-release
	}
	enqueueResult := make(chan error, 1)
	go func() { enqueueResult <- buffer.Enqueue(t.Context(), eventBatch(1, "reservation", 0, 6)) }()
	<-reserved
	closeResult := make(chan error, 1)
	go func() { closeResult <- buffer.Close(t.Context()) }()
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	require.Equal(t, int64(6), buffer.Stats().Events)
	select {
	case err := <-closeResult:
		require.Failf(t, "close returned with a live reservation", "error: %v", err)
	default:
	}
	releaseOnce()
	require.ErrorIs(t, <-enqueueResult, ErrBufferClosed)
	require.NoError(t, <-closeResult)
	stats := requireClosedBalanced(t, buffer, 0)
	require.Zero(t, stats.Stored)
	require.Zero(t, stats.NotStored)
	require.False(t, buffer.writerStarted)
}

func TestBufferCloseGrace(t *testing.T) {
	t.Parallel()

	t.Run("writer in a write", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 5})
		attempted := make(chan struct{})
		release := make(chan struct{})
		releaseOnce := closeOnce(release)
		t.Cleanup(releaseOnce)
		var once sync.Once
		buffer.hooks.afterAttempt = func(err error) {
			if err == nil {
				once.Do(func() { close(attempted) })
				<-release
			}
		}
		buffer.closeGrace = time.Millisecond
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "grace-write", 0, 3)))
		<-attempted
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "grace-write-queued", 0, 2)))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := buffer.Close(ctx)
		require.EqualError(t, err, "close: 2 events not stored, 3 events still in a write: context canceled")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, BufferClosing, buffer.Stats().State)
		require.Equal(t, int64(5), buffer.Stats().Events)
		require.ErrorIs(t, buffer.Enqueue(t.Context(), eventBatch(3, "no", 0, 1)), ErrBufferClosed)
		releaseOnce()
		stats := requireClosedBalanced(t, buffer, 5)
		require.Equal(t, int64(3), stats.Stored)
		require.Equal(t, int64(2), stats.NotStored)
	})

	t.Run("writer in OnRejected", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		duplicate := eventBatch(1, "grace-reject", 0, 3)
		require.NoError(t, store.AppendEvents(t.Context(), duplicate))
		started := make(chan struct{})
		release := make(chan struct{})
		releaseOnce := closeOnce(release)
		t.Cleanup(releaseOnce)
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 5, OnRejected: func(context.Context, Rejection) {
			close(started)
			<-release
		}})
		buffer.closeGrace = time.Millisecond
		require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
		<-started
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "grace-reject-queued", 0, 2)))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := buffer.Close(ctx)
		require.EqualError(t, err, "close: 2 events not stored, writer still in OnRejected: context canceled")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, BufferClosing, buffer.Stats().State)
		require.Equal(t, int64(5), buffer.Stats().Events)
		releaseOnce()
		stats := requireClosedBalanced(t, buffer, 5)
		require.Equal(t, int64(3), stats.Rejected)
		require.Equal(t, int64(2), stats.NotStored)
	})

	t.Run("enqueue still copying", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 3})
		reserved := make(chan struct{})
		release := make(chan struct{})
		releaseOnce := closeOnce(release)
		t.Cleanup(releaseOnce)
		buffer.hooks.afterReserve = func() {
			close(reserved)
			<-release
		}
		buffer.closeGrace = time.Millisecond
		result := make(chan error, 1)
		go func() { result <- buffer.Enqueue(t.Context(), eventBatch(1, "grace-copy", 0, 3)) }()
		<-reserved
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := buffer.Close(ctx)
		require.EqualError(t, err, "close: 0 events not stored, 1 enqueues still copying: context canceled")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, BufferClosing, buffer.Stats().State)
		require.Equal(t, int64(3), buffer.Stats().Events)
		releaseOnce()
		require.ErrorIs(t, <-result, ErrBufferClosed)
		requireClosedBalanced(t, buffer, 0)
		require.False(t, buffer.writerStarted)
	})
}

func TestBufferConcurrentEnqueue(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 100})
	results := make(chan error, 4)
	for writer := 1; writer <= 4; writer++ {
		go func() {
			for batch := range int64(50) {
				if err := buffer.Enqueue(t.Context(), eventBatch(writer, fmt.Sprintf("concurrent-%d", writer), batch*20, 20)); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
	}
	for range 4 {
		require.NoError(t, <-results)
	}
	require.NoError(t, buffer.Flush(t.Context()))
	require.Equal(t, int64(4_000), buffer.Stats().Stored)
	for writer := 1; writer <= 4; writer++ {
		events, err := store.SessionEvents(t.Context(), protocol.OpaqueID(fmt.Sprintf("concurrent-%d", writer)), 0, MaxBatchEvents)
		require.NoError(t, err)
		require.Len(t, events, 1_000)
		for sequence := range events {
			require.Equal(t, int64(sequence), events[sequence].Sequence)
		}
	}
}
