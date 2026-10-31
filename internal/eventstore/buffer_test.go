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

type bufferTestWorkers struct {
	mu   sync.Mutex
	done []<-chan struct{}
}

var bufferWorkers sync.Map

func registerBufferWorkers(t *testing.T) {
	t.Helper()
	workers := &bufferTestWorkers{}
	if _, loaded := bufferWorkers.LoadOrStore(t, workers); loaded {
		return
	}
	t.Cleanup(func() {
		defer bufferWorkers.Delete(t)
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		workers.mu.Lock()
		done := append([]<-chan struct{}(nil), workers.done...)
		workers.mu.Unlock()
		for _, workerDone := range done {
			select {
			case <-workerDone:
			case <-timer.C:
				t.Errorf("buffer test worker cleanup timed out")
				return
			}
		}
	})
}

func goBufferWorker(t *testing.T, work func()) {
	t.Helper()
	value, ok := bufferWorkers.Load(t)
	if !ok {
		t.Fatalf("buffer test worker started before testBuffer")
	}
	workers := value.(*bufferTestWorkers)
	done := make(chan struct{})
	workers.mu.Lock()
	workers.done = append(workers.done, done)
	workers.mu.Unlock()
	go func() {
		defer close(done)
		work()
	}()
}

func testBuffer(t *testing.T, store *Store, opts BufferOptions) *Buffer {
	t.Helper()
	if opts.OnRejected == nil {
		opts.OnRejected = func(context.Context, Rejection) {}
	}
	buffer, err := NewBuffer(store, opts)
	require.NoError(t, err)
	registerBufferWorkers(t)
	t.Cleanup(func() {
		if err := cleanupTestBuffer(buffer, 2*time.Second); err != nil {
			t.Errorf("buffer cleanup: %v", err)
		}
	})
	return buffer
}

func cleanupTestBuffer(buffer *Buffer, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var cleanupErrors []error
	if err := buffer.Close(ctx); err != nil && !errors.Is(err, ErrBufferClosed) {
		cleanupErrors = append(cleanupErrors, err)
	}
	select {
	case <-buffer.writerDone:
	case <-ctx.Done():
		cleanupErrors = append(cleanupErrors, fmt.Errorf("writer join: %w", ctx.Err()))
		return errors.Join(cleanupErrors...)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		buffer.mu.Lock()
		state := buffer.state
		reservations := buffer.reservations
		writerExited := buffer.writerExited
		buffer.mu.Unlock()
		if state == BufferClosed && reservations == 0 && writerExited {
			return errors.Join(cleanupErrors...)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			cleanupErrors = append(cleanupErrors, fmt.Errorf(
				"state join: state=%v reservations=%d writer_exited=%t: %w",
				state, reservations, writerExited, ctx.Err(),
			))
			return errors.Join(cleanupErrors...)
		}
	}
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

func receiveBuffer[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		require.FailNow(t, "buffer channel timeout")
	}
	var zero T
	return zero
}

func waitBufferBarrier(ch <-chan struct{}) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
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

func pauseAfterCommit(t *testing.T, buffer *Buffer) (<-chan struct{}, func()) {
	t.Helper()
	committed := make(chan struct{})
	gate := make(chan struct{})
	release := closeOnce(gate)
	t.Cleanup(release)
	var once sync.Once
	buffer.hooks.afterAttempt = func(err error) {
		if err == nil {
			once.Do(func() { close(committed) })
			waitBufferBarrier(gate)
		}
	}
	return committed, release
}

func requireClosedBalanced(t *testing.T, buffer *Buffer, accepted int64) BufferStats {
	t.Helper()
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosed })
	receiveBuffer(t, buffer.writerDone)
	stats := buffer.Stats()
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)
	require.Equal(t, accepted, stats.Stored+stats.Rejected+stats.NotStored)
	buffer.mu.Lock()
	writerExited := buffer.writerExited
	reservations := buffer.reservations
	buffer.mu.Unlock()
	require.True(t, writerExited)
	require.Zero(t, reservations)
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

func TestBufferReleasesReferences(t *testing.T) {
	t.Parallel()
	store := testStore(t)

	t.Run("resolved queue payload", func(t *testing.T) {
		t.Parallel()
		buffer := testBuffer(t, store, BufferOptions{})
		firstAttempt := make(chan struct{})
		thirdAttempt := make(chan struct{})
		releaseFirst := make(chan struct{})
		releaseFirstOnce := closeOnce(releaseFirst)
		releaseThird := make(chan struct{})
		releaseThirdOnce := closeOnce(releaseThird)
		t.Cleanup(releaseFirstOnce)
		t.Cleanup(releaseThirdOnce)
		var attempts atomic.Int64
		buffer.hooks.afterAttempt = func(error) {
			switch attempts.Add(1) {
			case 1:
				close(firstAttempt)
				waitBufferBarrier(releaseFirst)
			case 3:
				close(thirdAttempt)
				waitBufferBarrier(releaseThird)
			}
		}
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "retained", 0, 1)))
		receiveBuffer(t, firstAttempt)
		for sequence := int64(1); sequence <= 3; sequence++ {
			require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "retained", sequence, 1)))
		}
		buffer.mu.Lock()
		backing := buffer.queue
		buffer.mu.Unlock()
		require.Len(t, backing, 3)
		releaseFirstOnce()
		receiveBuffer(t, thirdAttempt)
		stats := buffer.Stats()
		require.Equal(t, int64(2), stats.Stored)
		require.Equal(t, int64(2), stats.Events)
		buffer.mu.Lock()
		queueLen := len(buffer.queue)
		sameBacking := queueLen == 1 && &buffer.queue[0] == &backing[2]
		firstCleared := backing[0] == nil
		secondCleared := backing[1] == nil
		buffer.mu.Unlock()
		require.Equal(t, 1, queueLen)
		require.True(t, sameBacking)
		require.True(t, firstCleared)
		require.True(t, secondCleared)
		releaseThirdOnce()
		require.NoError(t, buffer.Flush(t.Context()))
		require.NoError(t, buffer.Close(t.Context()))
		requireClosedBalanced(t, buffer, 4)
	})

	t.Run("queue pop", func(t *testing.T) {
		t.Parallel()
		buffer := testBuffer(t, store, BufferOptions{})
		first := &bufferedBatch{events: eventBatch(1, "first", 0, 1)}
		second := &bufferedBatch{events: eventBatch(2, "second", 0, 1)}
		third := &bufferedBatch{events: eventBatch(3, "third", 0, 1)}
		backing := []*bufferedBatch{first, second, third}
		buffer.queue = backing
		require.Same(t, first, buffer.takeBatchLocked())
		require.Nil(t, backing[0])
		require.Equal(t, []*bufferedBatch{second, third}, buffer.queue)
	})

	t.Run("empty queue", func(t *testing.T) {
		t.Parallel()
		buffer := testBuffer(t, store, BufferOptions{})
		batch := &bufferedBatch{events: eventBatch(1, "only", 0, 1)}
		backing := []*bufferedBatch{batch}
		buffer.queue = backing
		require.Same(t, batch, buffer.takeBatchLocked())
		require.Nil(t, backing[0])
		require.Nil(t, buffer.queue)
	})

	t.Run("grant waiters", func(t *testing.T) {
		t.Parallel()
		buffer := testBuffer(t, store, BufferOptions{})
		first := &roomWaiter{events: 1, bytes: 1, granted: make(chan struct{})}
		second := &roomWaiter{events: 1, bytes: 1, granted: make(chan struct{})}
		backing := []*roomWaiter{first, second}
		buffer.waiters = backing
		buffer.grantWaitersLocked()
		require.Nil(t, backing[0])
		require.Nil(t, backing[1])
		require.Nil(t, buffer.waiters)
		buffer.reservations = 0
		buffer.events = 0
		buffer.bytes = 0
	})

	t.Run("remove waiter", func(t *testing.T) {
		t.Parallel()
		buffer := testBuffer(t, store, BufferOptions{})
		first := &roomWaiter{}
		second := &roomWaiter{}
		third := &roomWaiter{}
		backing := []*roomWaiter{first, second, third}
		buffer.waiters = backing
		buffer.removeWaiterLocked(second)
		require.Equal(t, []*roomWaiter{first, third}, buffer.waiters)
		require.Nil(t, backing[2])
		buffer.waiters = nil
	})
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
	goBufferWorker(t, func() { result <- buffer.Enqueue(ctx, eventBatch(2, "waiting", 0, 1)) })
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	require.Equal(t, int64(10), buffer.Stats().Events)
	cancel()
	require.ErrorIs(t, receiveBuffer(t, result), context.Canceled)
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
	goBufferWorker(t, func() { result <- buffer.Enqueue(ctx, eventBatch(2, "byte-wait", 0, 1)) })
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	cancel()
	require.ErrorIs(t, receiveBuffer(t, result), context.Canceled)
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
	goBufferWorker(t, func() { results <- buffer.Enqueue(t.Context(), eventBatch(2, "large", 0, 6)) })
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	goBufferWorker(t, func() { results <- buffer.Enqueue(t.Context(), eventBatch(3, "small", 0, 1)) })
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 2 })
	setFailure(false)
	require.NoError(t, receiveBuffer(t, results))
	require.NoError(t, receiveBuffer(t, results))
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

func TestBufferWriteFailureHookPrecedesRetryAndReceipt(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	setFailure := installBufferFailure(t, store)
	var failures atomic.Int64
	var recoveries atomic.Int64
	var latched atomic.Bool
	attempts := make(chan bool, 16)
	buffer := testBuffer(t, store, BufferOptions{
		RetryInterval: time.Millisecond,
		OnWriteFailure: func(error) {
			latched.Store(true)
			failures.Add(1)
		},
		OnWriteRecovery: func() { recoveries.Add(1) },
	})
	buffer.hooks.afterAttempt = func(err error) {
		if err != nil {
			select {
			case attempts <- latched.Load():
			default:
			}
		}
	}

	receipt, err := buffer.EnqueueWithReceipt(t.Context(), eventBatch(1, "failure-hook-1", 0, 1))
	require.NoError(t, err)
	require.True(t, receiveBuffer(t, attempts), "the latch closes before attempt observers run")
	require.Equal(t, int64(1), failures.Load())
	select {
	case result := <-receipt:
		require.Fail(t, "receipt resolved before durable retry", "%+v", result)
	default:
	}
	require.True(t, receiveBuffer(t, attempts))
	require.Equal(t, int64(1), failures.Load(), "retries in one failed interval do not create epochs")

	setFailure(false)
	result := receiveBuffer(t, receipt)
	require.True(t, result.Stored)
	require.NoError(t, result.Err)
	require.Equal(t, int64(1), recoveries.Load())

	latched.Store(false)
	setFailure(true)
	second, err := buffer.EnqueueWithReceipt(t.Context(), eventBatch(2, "failure-hook-2", 0, 1))
	require.NoError(t, err)
	require.True(t, receiveBuffer(t, attempts))
	require.True(t, latched.Load())
	require.Equal(t, int64(2), failures.Load(), "a new failure after recovery starts a new epoch")
	setFailure(false)
	result = receiveBuffer(t, second)
	require.True(t, result.Stored)
	require.NoError(t, result.Err)
	require.Equal(t, int64(2), recoveries.Load())
}

func TestBufferDuplicateDoesNotProveOrRestartWriteRecovery(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	setFailure := installBufferFailure(t, store)
	var failures atomic.Int64
	var recoveries atomic.Int64
	attempts := make(chan struct{}, 4)
	releaseFirst := make(chan struct{})
	releaseFirstOnce := closeOnce(releaseFirst)
	t.Cleanup(releaseFirstOnce)
	buffer := testBuffer(t, store, BufferOptions{
		RetryInterval:  time.Millisecond,
		OnWriteFailure: func(error) { failures.Add(1) },
		OnWriteRecovery: func() {
			recoveries.Add(1)
		},
	})
	var first sync.Once
	buffer.hooks.afterAttempt = func(err error) {
		if err == nil || errors.Is(err, ErrDuplicateEvent) {
			return
		}
		attempts <- struct{}{}
		first.Do(func() { waitBufferBarrier(releaseFirst) })
	}

	duplicate := eventBatch(1, "duplicate-during-recovery", 0, 1)
	receipt, err := buffer.EnqueueWithReceipt(t.Context(), duplicate)
	require.NoError(t, err)
	receiveBuffer(t, attempts)
	require.Equal(t, int64(1), failures.Load())
	setFailure(false)
	require.NoError(t, store.AppendEvents(t.Context(), duplicate))
	releaseFirstOnce()
	result := receiveBuffer(t, receipt)
	require.ErrorIs(t, result.Err, ErrDuplicateEvent)
	require.Zero(t, recoveries.Load(), "duplicate rejection is not recovery evidence")
	require.True(t, buffer.Stats().Degraded)

	setFailure(true)
	second, err := buffer.EnqueueWithReceipt(
		t.Context(), eventBatch(2, "failure-after-duplicate", 0, 1),
	)
	require.NoError(t, err)
	receiveBuffer(t, attempts)
	require.Equal(t, int64(1), failures.Load(),
		"a duplicate cannot split one failed interval into two epochs")
	setFailure(false)
	result = receiveBuffer(t, second)
	require.True(t, result.Stored)
	require.NoError(t, result.Err)
	require.Equal(t, int64(1), recoveries.Load())
}

func TestBufferReceiptsNameOneDurableOutcome(t *testing.T) {
	t.Parallel()
	t.Run("stored", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		buffer := testBuffer(t, store, BufferOptions{})
		receipt, err := buffer.EnqueueWithReceipt(t.Context(), eventBatch(1, "receipt-stored", 0, 1))
		require.NoError(t, err)
		result := receiveBuffer(t, receipt)
		require.True(t, result.Stored)
		require.NoError(t, result.Err)
		_, open := <-receipt
		require.False(t, open)
	})

	t.Run("duplicate", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		batch := eventBatch(1, "receipt-duplicate", 0, 1)
		require.NoError(t, store.AppendEvents(t.Context(), batch))
		buffer := testBuffer(t, store, BufferOptions{})
		receipt, err := buffer.EnqueueWithReceipt(t.Context(), batch)
		require.NoError(t, err)
		result := receiveBuffer(t, receipt)
		require.False(t, result.Stored)
		require.ErrorIs(t, result.Err, ErrDuplicateEvent)
	})

	t.Run("shutdown before store", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		installBufferFailure(t, store)
		buffer := testBuffer(t, store, BufferOptions{RetryInterval: time.Millisecond})
		receipt, err := buffer.EnqueueWithReceipt(t.Context(), eventBatch(1, "receipt-closed", 0, 1))
		require.NoError(t, err)
		waitBuffer(t, func() bool { return buffer.Stats().Degraded })
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, buffer.Close(ctx), context.Canceled)
		result := receiveBuffer(t, receipt)
		require.False(t, result.Stored)
		require.ErrorIs(t, result.Err, ErrBufferClosed)
	})

	t.Run("transient failure", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		setFailure := installBufferFailure(t, store)
		buffer := testBuffer(t, store, BufferOptions{RetryInterval: time.Millisecond})
		receipt, err := buffer.EnqueueWithReceipt(t.Context(), eventBatch(1, "receipt-retry", 0, 1))
		require.NoError(t, err)
		waitBuffer(t, func() bool { return buffer.Stats().Degraded })
		select {
		case result := <-receipt:
			require.Fail(t, "receipt resolved before commit", "%+v", result)
		default:
		}
		setFailure(false)
		result := receiveBuffer(t, receipt)
		require.True(t, result.Stored)
		require.NoError(t, result.Err)
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		buffer := testBuffer(t, store, BufferOptions{})
		receipt, err := buffer.EnqueueWithReceipt(t.Context(), nil)
		require.NoError(t, err)
		result := receiveBuffer(t, receipt)
		require.True(t, result.Stored)
		require.NoError(t, result.Err)
	})
}

func TestBufferRejected(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	duplicate := eventBatch(1, "duplicate", 0, 3)
	later := eventBatch(2, "later", 0, 1)
	require.NoError(t, store.AppendEvents(t.Context(), duplicate))
	rejections := make(chan Rejection, 1)
	callbackStarted := make(chan struct{})
	callbackContext := make(chan error, 1)
	releaseCallback := make(chan struct{})
	releaseCallbackOnce := closeOnce(releaseCallback)
	buffer := testBuffer(t, store, BufferOptions{OnRejected: func(ctx context.Context, rejection Rejection) {
		callbackContext <- ctx.Err()
		for i := range rejection.Batch {
			rejection.Batch[i].Payload = nil
		}
		rejection.Batch = rejection.Batch[:0]
		rejections <- rejection
		close(callbackStarted)
		waitBufferBarrier(releaseCallback)
	}})
	t.Cleanup(releaseCallbackOnce)
	require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
	require.NoError(t, buffer.Enqueue(t.Context(), later))
	receiveBuffer(t, callbackStarted)
	require.NoError(t, receiveBuffer(t, callbackContext))
	rejection := receiveBuffer(t, rejections)
	require.ErrorIs(t, rejection.Err, ErrDuplicateEvent)
	require.Empty(t, rejection.Batch)
	wantBytes := batchBytes(duplicate) + batchBytes(later)
	waitBuffer(t, func() bool { return buffer.Stats().Rejected == 3 })
	stats := buffer.Stats()
	require.Equal(t, int64(4), stats.Events)
	require.Equal(t, wantBytes, stats.Bytes)

	flushResult := make(chan error, 1)
	goBufferWorker(t, func() { flushResult <- buffer.Flush(t.Context()) })
	select {
	case err := <-flushResult:
		require.Failf(t, "flush returned before OnRejected", "error: %v", err)
	default:
	}
	releaseCallbackOnce()
	require.NoError(t, receiveBuffer(t, flushResult))
	stats = buffer.Stats()
	require.Equal(t, int64(3), stats.Rejected)
	require.Equal(t, int64(1), stats.Stored)
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)
	require.NoError(t, buffer.Close(t.Context()))
	requireClosedBalanced(t, buffer, 4)
}

func TestBufferFlushWaitsForCallback(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	duplicate := eventBatch(1, "rejected", 0, 2)
	require.NoError(t, store.AppendEvents(t.Context(), duplicate))
	started := make(chan struct{})
	gate := make(chan struct{})
	release := closeOnce(gate)
	buffer := testBuffer(t, store, BufferOptions{OnRejected: func(context.Context, Rejection) {
		close(started)
		waitBufferBarrier(gate)
	}})
	t.Cleanup(release)
	marked := make(chan uint64, 1)
	buffer.hooks.afterFlushMark = func(target uint64) { marked <- target }
	require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
	receiveBuffer(t, started)
	require.Equal(t, int64(2), buffer.Stats().Rejected)
	flushed := make(chan error, 1)
	goBufferWorker(t, func() { flushed <- buffer.Flush(t.Context()) })
	require.Equal(t, uint64(1), receiveBuffer(t, marked))
	for range 50 {
		buffer.mu.Lock()
		buffer.notifyLocked()
		buffer.mu.Unlock()
		select {
		case err := <-flushed:
			require.Failf(t, "flush returned while OnRejected was running", "error: %v", err)
		default:
		}
	}
	release()
	require.NoError(t, receiveBuffer(t, flushed))
	stats := buffer.Stats()
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)
	require.NoError(t, buffer.Close(t.Context()))
	requireClosedBalanced(t, buffer, 2)
}

func TestBufferCallbackMutationKeepsAccounting(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	duplicate := eventBatch(1, "cb-old", 0, 3)
	require.NoError(t, store.AppendEvents(t.Context(), duplicate))
	size := batchBytes(duplicate)
	buffer := testBuffer(t, store, BufferOptions{
		MaxEvents: 3,
		MaxBytes:  size,
		OnRejected: func(_ context.Context, rejection Rejection) {
			for i := range rejection.Batch {
				rejection.Batch[i].Payload = protocol.Payload(make([]byte, 1<<16))
				rejection.Batch[i].SessionID = "changed-by-callee"
			}
			rejection.Batch = append(rejection.Batch, rejection.Batch...)
		},
	})
	require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
	require.NoError(t, buffer.Flush(t.Context()))
	stats := buffer.Stats()
	require.Equal(t, int64(3), stats.Rejected)
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)

	fresh := eventBatch(2, "cb-new", 0, 3)
	require.Equal(t, size, batchBytes(fresh))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, buffer.Enqueue(ctx, fresh))
	require.NoError(t, buffer.Flush(t.Context()))
	stats = buffer.Stats()
	require.Equal(t, int64(3), stats.Stored)
	require.Zero(t, stats.Events)
	require.Zero(t, stats.Bytes)
	require.NoError(t, buffer.Close(t.Context()))
	requireClosedBalanced(t, buffer, 6)
}

func TestBufferFlushBarrier(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{})
	flushMarked := make(chan struct{})
	resumeFlush := make(chan struct{})
	resumeFlushOnce := closeOnce(resumeFlush)
	prefixResolved := make(chan struct{})
	releaseWriter := make(chan struct{})
	releaseWriterOnce := closeOnce(releaseWriter)
	t.Cleanup(resumeFlushOnce)
	t.Cleanup(releaseWriterOnce)
	buffer.hooks.afterFlushMark = func(uint64) {
		close(flushMarked)
		waitBufferBarrier(resumeFlush)
	}
	var resolved atomic.Int64
	buffer.hooks.afterResolve = func() {
		if resolved.Add(1) == 5 {
			close(prefixResolved)
			waitBufferBarrier(releaseWriter)
		}
	}
	for sequence := range int64(5) {
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "prefix", sequence, 1)))
	}
	receiveBuffer(t, prefixResolved)
	result := make(chan error, 1)
	goBufferWorker(t, func() { result <- buffer.Flush(t.Context()) })
	receiveBuffer(t, flushMarked)
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "suffix", 0, 1)))
	resumeFlushOnce()
	require.NoError(t, receiveBuffer(t, result))
	stats := buffer.Stats()
	require.Equal(t, BufferOpen, stats.State)
	require.Equal(t, int64(5), stats.Stored)
	require.Equal(t, int64(1), stats.Events)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	closeResult := make(chan error, 1)
	goBufferWorker(t, func() { closeResult <- buffer.Close(ctx) })
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	releaseWriterOnce()
	err := receiveBuffer(t, closeResult)
	require.EqualError(t, err, "close: 1 events not stored: context canceled")
	require.ErrorIs(t, err, context.Canceled)
	stats = requireClosedBalanced(t, buffer, 6)
	require.Equal(t, int64(5), stats.Stored)
	require.Equal(t, int64(1), stats.NotStored)
}

func TestBufferFlushPrefixSurvivesSuffixLoss(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 10})
	committed, release := pauseAfterCommit(t, buffer)
	buffer.closeGrace = 10 * time.Second
	marked := make(chan uint64, 1)
	buffer.hooks.afterFlushMark = func(target uint64) { marked <- target }
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "prefix", 0, 2)))
	receiveBuffer(t, committed)
	flushed := make(chan error, 1)
	goBufferWorker(t, func() { flushed <- buffer.Flush(t.Context()) })
	require.Equal(t, uint64(1), receiveBuffer(t, marked))
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "suffix", 0, 3)))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	closed := make(chan error, 1)
	goBufferWorker(t, func() { closed <- buffer.Close(ctx) })
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	release()
	require.EqualError(t, receiveBuffer(t, closed), "close: 3 events not stored: context canceled")
	require.NoError(t, receiveBuffer(t, flushed))
	stats := requireClosedBalanced(t, buffer, 5)
	require.Equal(t, int64(2), stats.Stored)
	require.Equal(t, int64(3), stats.NotStored)
}

func TestBufferFlushOwnBatchLost(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 10})
	committed, release := pauseAfterCommit(t, buffer)
	buffer.closeGrace = 10 * time.Second
	marked := make(chan uint64, 1)
	buffer.hooks.afterFlushMark = func(target uint64) { marked <- target }
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "first", 0, 2)))
	receiveBuffer(t, committed)
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "own", 0, 3)))
	flushed := make(chan error, 1)
	goBufferWorker(t, func() { flushed <- buffer.Flush(t.Context()) })
	require.Equal(t, uint64(2), receiveBuffer(t, marked))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	closed := make(chan error, 1)
	goBufferWorker(t, func() { closed <- buffer.Close(ctx) })
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	release()
	require.EqualError(t, receiveBuffer(t, closed), "close: 3 events not stored: context canceled")
	require.ErrorIs(t, receiveBuffer(t, flushed), ErrBufferClosed)
	stats := requireClosedBalanced(t, buffer, 5)
	require.Equal(t, int64(2), stats.Stored)
	require.Equal(t, int64(3), stats.NotStored)
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
	goBufferWorker(t, func() { result <- buffer.Enqueue(t.Context(), eventBatch(2, "wait", 0, 1)) })
	waitBuffer(t, func() bool { return bufferWaiters(buffer) == 1 })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := buffer.Close(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, err.Error(), "3 events not stored")
	require.ErrorIs(t, receiveBuffer(t, result), ErrBufferClosed)
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
	receiveBuffer(t, callbackStarted)
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
			waitBufferBarrier(release)
		}
	}
	buffer.closeGrace = 2 * time.Second
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "commit", 0, 3)))
	receiveBuffer(t, attempted)
	require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(2, "commit-queued", 0, 2)))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result := make(chan error, 1)
	goBufferWorker(t, func() { result <- buffer.Close(ctx) })
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	releaseOnce()
	err := receiveBuffer(t, result)
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
		waitBufferBarrier(release)
	}
	enqueueResult := make(chan error, 1)
	goBufferWorker(t, func() { enqueueResult <- buffer.Enqueue(t.Context(), eventBatch(1, "reservation", 0, 6)) })
	receiveBuffer(t, reserved)
	closeResult := make(chan error, 1)
	goBufferWorker(t, func() { closeResult <- buffer.Close(t.Context()) })
	waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
	require.Equal(t, int64(6), buffer.Stats().Events)
	select {
	case err := <-closeResult:
		require.Failf(t, "close returned with a live reservation", "error: %v", err)
	default:
	}
	releaseOnce()
	require.ErrorIs(t, receiveBuffer(t, enqueueResult), ErrBufferClosed)
	require.NoError(t, receiveBuffer(t, closeResult))
	stats := requireClosedBalanced(t, buffer, 0)
	require.Zero(t, stats.Stored)
	require.Zero(t, stats.NotStored)
	require.False(t, buffer.writerStarted)
}

func TestBufferCleanupJoinsInterruptedClose(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 1})
	reserved := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := closeOnce(release)
	t.Cleanup(releaseOnce)
	buffer.hooks.afterReserve = func() {
		close(reserved)
		waitBufferBarrier(release)
	}
	buffer.closeGrace = time.Millisecond
	enqueueResult := make(chan error, 1)
	goBufferWorker(t, func() {
		enqueueResult <- buffer.Enqueue(t.Context(), eventBatch(1, "cleanup", 0, 1))
	})
	receiveBuffer(t, reserved)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := buffer.Close(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, BufferClosing, buffer.Stats().State)
	releaseOnce()
	require.NoError(t, cleanupTestBuffer(buffer, 2*time.Second))
	require.ErrorIs(t, receiveBuffer(t, enqueueResult), ErrBufferClosed)
	stats := requireClosedBalanced(t, buffer, 0)
	require.Zero(t, stats.Events)
	require.Zero(t, stats.NotStored)
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
				waitBufferBarrier(release)
			}
		}
		buffer.closeGrace = time.Millisecond
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "grace-write", 0, 3)))
		receiveBuffer(t, attempted)
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
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 5, OnRejected: func(context.Context, Rejection) {
			close(started)
			waitBufferBarrier(release)
		}})
		t.Cleanup(releaseOnce)
		buffer.closeGrace = time.Millisecond
		require.NoError(t, buffer.Enqueue(t.Context(), duplicate))
		receiveBuffer(t, started)
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
			waitBufferBarrier(release)
		}
		buffer.closeGrace = time.Millisecond
		result := make(chan error, 1)
		goBufferWorker(t, func() { result <- buffer.Enqueue(t.Context(), eventBatch(1, "grace-copy", 0, 3)) })
		receiveBuffer(t, reserved)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := buffer.Close(ctx)
		require.EqualError(t, err, "close: 0 events not stored, 1 enqueues still copying: context canceled")
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, BufferClosing, buffer.Stats().State)
		require.Equal(t, int64(3), buffer.Stats().Events)
		releaseOnce()
		require.ErrorIs(t, receiveBuffer(t, result), ErrBufferClosed)
		requireClosedBalanced(t, buffer, 0)
		require.False(t, buffer.writerStarted)
	})

	t.Run("failed retry is known not stored", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		installBufferFailure(t, store)
		retryStarted := make(chan struct{})
		releaseRetry := make(chan struct{})
		releaseRetryOnce := closeOnce(releaseRetry)
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 3, RetryInterval: time.Second})
		t.Cleanup(releaseRetryOnce)
		buffer.closeGrace = time.Millisecond
		buffer.hooks.afterRetry = func() {
			close(retryStarted)
			waitBufferBarrier(releaseRetry)
		}
		require.NoError(t, buffer.Enqueue(t.Context(), eventBatch(1, "grace-retry", 0, 3)))
		receiveBuffer(t, retryStarted)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		err := buffer.Close(ctx)
		require.EqualError(t, err, "close: 3 events not stored: context canceled")
		require.ErrorIs(t, err, context.Canceled)
		stats := buffer.Stats()
		require.Equal(t, BufferClosing, stats.State)
		require.Equal(t, int64(3), stats.Events)
		require.Zero(t, stats.NotStored)
		releaseRetryOnce()
		stats = requireClosedBalanced(t, buffer, 3)
		require.Equal(t, int64(3), stats.NotStored)
	})

	t.Run("timer rechecks closed directly", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 1})
		reserved := make(chan struct{})
		releaseReservation := make(chan struct{})
		releaseReservationOnce := closeOnce(releaseReservation)
		timerSelected := make(chan struct{})
		resumeTimer := make(chan struct{})
		resumeTimerOnce := closeOnce(resumeTimer)
		t.Cleanup(releaseReservationOnce)
		t.Cleanup(resumeTimerOnce)
		buffer.hooks.afterReserve = func() {
			close(reserved)
			waitBufferBarrier(releaseReservation)
		}
		buffer.hooks.afterGraceTimer = func() {
			close(timerSelected)
			waitBufferBarrier(resumeTimer)
		}
		buffer.closeGrace = time.Millisecond
		enqueueResult := make(chan error, 1)
		goBufferWorker(t, func() { enqueueResult <- buffer.Enqueue(t.Context(), eventBatch(1, "timer-direct", 0, 1)) })
		receiveBuffer(t, reserved)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		closeResult := make(chan error, 1)
		goBufferWorker(t, func() { closeResult <- buffer.Close(ctx) })
		receiveBuffer(t, timerSelected)
		releaseReservationOnce()
		require.ErrorIs(t, receiveBuffer(t, enqueueResult), ErrBufferClosed)
		requireClosedBalanced(t, buffer, 0)
		resumeTimerOnce()
		require.NoError(t, receiveBuffer(t, closeResult))
	})

	t.Run("timer rechecks closed after change", func(t *testing.T) {
		t.Parallel()
		store := testStore(t)
		buffer := testBuffer(t, store, BufferOptions{MaxEvents: 2})
		reserved := make(chan struct{}, 2)
		releaseFirst := make(chan struct{})
		releaseFirstOnce := closeOnce(releaseFirst)
		releaseSecond := make(chan struct{})
		releaseSecondOnce := closeOnce(releaseSecond)
		graceChanged := make(chan struct{})
		timerSelected := make(chan struct{})
		resumeTimer := make(chan struct{})
		resumeTimerOnce := closeOnce(resumeTimer)
		graceTimer := make(chan time.Time, 1)
		var fireTimerOnce sync.Once
		fireTimer := func() { fireTimerOnce.Do(func() { graceTimer <- time.Time{} }) }
		t.Cleanup(releaseFirstOnce)
		t.Cleanup(releaseSecondOnce)
		t.Cleanup(resumeTimerOnce)
		t.Cleanup(fireTimer)
		var reservations atomic.Int64
		buffer.hooks.afterReserve = func() {
			reservation := reservations.Add(1)
			reserved <- struct{}{}
			if reservation == 1 {
				waitBufferBarrier(releaseFirst)
				return
			}
			waitBufferBarrier(releaseSecond)
		}
		buffer.hooks.afterGraceChange = func() { close(graceChanged) }
		buffer.hooks.afterGraceTimer = func() {
			close(timerSelected)
			waitBufferBarrier(resumeTimer)
		}
		buffer.hooks.graceTimer = graceTimer
		enqueueResults := make(chan error, 2)
		goBufferWorker(t, func() { enqueueResults <- buffer.Enqueue(t.Context(), eventBatch(1, "timer-change-a", 0, 1)) })
		goBufferWorker(t, func() { enqueueResults <- buffer.Enqueue(t.Context(), eventBatch(2, "timer-change-b", 0, 1)) })
		receiveBuffer(t, reserved)
		receiveBuffer(t, reserved)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		closeResult := make(chan error, 1)
		goBufferWorker(t, func() { closeResult <- buffer.Close(ctx) })
		waitBuffer(t, func() bool { return buffer.Stats().State == BufferClosing })
		releaseFirstOnce()
		require.ErrorIs(t, receiveBuffer(t, enqueueResults), ErrBufferClosed)
		receiveBuffer(t, graceChanged)
		fireTimer()
		receiveBuffer(t, timerSelected)
		releaseSecondOnce()
		require.ErrorIs(t, receiveBuffer(t, enqueueResults), ErrBufferClosed)
		requireClosedBalanced(t, buffer, 0)
		resumeTimerOnce()
		require.NoError(t, receiveBuffer(t, closeResult))
	})
}

func TestBufferConcurrentEnqueue(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	buffer := testBuffer(t, store, BufferOptions{MaxEvents: 100})
	results := make(chan error, 4)
	for writer := 1; writer <= 4; writer++ {
		goBufferWorker(t, func() {
			for batch := range int64(50) {
				if err := buffer.Enqueue(t.Context(), eventBatch(writer, fmt.Sprintf("concurrent-%d", writer), batch*20, 20)); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		})
	}
	for range 4 {
		require.NoError(t, receiveBuffer(t, results))
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
