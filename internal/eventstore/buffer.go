package eventstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Buffer limits (design §23.5).
const (
	BufferMaxEvents = 10_000
	BufferMaxBytes  = 64 << 20
)

// ErrBufferClosed means the buffer is closing or closed.
var ErrBufferClosed = errors.New("event buffer closed")

// BufferState is the life cycle of a Buffer: open, then closing, then closed.
type BufferState int

const (
	// BufferOpen accepts batches.
	BufferOpen BufferState = iota
	// BufferClosing drains accepted batches and refuses new work.
	BufferClosing
	// BufferClosed has resolved every accepted batch.
	BufferClosed
)

// Rejection is a batch the store refused for good (a duplicate).
type Rejection struct {
	Batch []protocol.EventEnvelope // the buffer's own copy; the callee owns it from the call on
	Err   error                    // wraps ErrDuplicateEvent
}

// BufferReceipt is the terminal result of one accepted batch. Stored is true
// only after that exact batch commits. A duplicate carries ErrDuplicateEvent;
// a batch dropped during shutdown carries ErrBufferClosed.
type BufferReceipt struct {
	Stored bool
	Err    error
}

// BufferOptions configures a Buffer.
type BufferOptions struct {
	MaxEvents     int           // 0 means BufferMaxEvents; otherwise 1 to BufferMaxEvents
	MaxBytes      int64         // 0 means BufferMaxBytes; otherwise 1 to BufferMaxBytes
	RetryInterval time.Duration // wait between failed writes; 0 means 1 s; never negative
	// OnRejected receives each rejected batch. Required.
	OnRejected func(context.Context, Rejection)
	// OnWriteFailure runs synchronously on the first failed attempt in each
	// degraded interval, before any retry or receipt can proceed. It must return
	// promptly; successful storage resets the interval.
	OnWriteFailure func(error)
}

// BufferStats is a point-in-time view of a Buffer.
type BufferStats struct {
	State         BufferState
	Events, Bytes int64 // held: reserved, queued or in flight
	Stored        int64 // events committed since NewBuffer
	Rejected      int64 // events handed to OnRejected
	NotStored     int64 // accepted events that closing dropped unstored
	Degraded      bool  // the last write attempt failed
	LastError     error // that failure; nil when not degraded
}

type bufferedBatch struct {
	events  []protocol.EventEnvelope
	count   int64
	bytes   int64
	id      uint64
	receipt chan BufferReceipt
}

type roomWaiter struct {
	events  int64
	bytes   int64
	granted chan struct{}
	ready   bool
}

type bufferHooks struct {
	afterReserve     func()
	afterAttempt     func(error)
	afterRetry       func()
	afterResolve     func()
	afterFlushMark   func(uint64)
	afterGraceChange func()
	afterGraceTimer  func()
	graceTimer       <-chan time.Time
}

// Buffer serializes accepted batches into Store while bounding held memory.
type Buffer struct {
	mu sync.Mutex

	store          *Store
	maxEvents      int64
	maxBytes       int64
	retryInterval  time.Duration
	onRejected     func(context.Context, Rejection)
	onWriteFailure func(error)
	wctx           context.Context
	cancelWriter   context.CancelFunc

	state     BufferState
	events    int64
	bytes     int64
	stored    int64
	rejected  int64
	notStored int64
	degraded  bool
	lastError error

	queue         []*bufferedBatch
	inFlight      *bufferedBatch
	accepted      uint64
	resolved      uint64
	firstNotStore uint64
	reservations  int
	waiters       []*roomWaiter

	writerStarted bool
	writerExited  bool
	inWrite       bool
	inCallback    bool
	writerDone    chan struct{}
	doneOnce      sync.Once
	changed       chan struct{}

	hooks      bufferHooks
	closeGrace time.Duration
}

// NewBuffer validates opts and returns an idle buffer. The writer starts on
// the first accepted batch.
func NewBuffer(store *Store, opts BufferOptions) (*Buffer, error) {
	if store == nil || opts.OnRejected == nil || opts.MaxEvents < 0 || opts.MaxEvents > BufferMaxEvents ||
		opts.MaxBytes < 0 || opts.MaxBytes > BufferMaxBytes || opts.RetryInterval < 0 {
		return nil, ErrInvalidArgument
	}
	maxEvents := opts.MaxEvents
	if maxEvents == 0 {
		maxEvents = BufferMaxEvents
	}
	maxBytes := opts.MaxBytes
	if maxBytes == 0 {
		maxBytes = BufferMaxBytes
	}
	retryInterval := opts.RetryInterval
	if retryInterval == 0 {
		retryInterval = time.Second
	}
	wctx, cancel := context.WithCancel(context.Background())
	return &Buffer{
		store:          store,
		maxEvents:      int64(maxEvents),
		maxBytes:       maxBytes,
		retryInterval:  retryInterval,
		onRejected:     opts.OnRejected,
		onWriteFailure: opts.OnWriteFailure,
		wctx:           wctx,
		cancelWriter:   cancel,
		state:          BufferOpen,
		writerDone:     make(chan struct{}),
		changed:        make(chan struct{}),
		closeGrace:     time.Second,
	}, nil
}

// Enqueue accepts a private copy of batch once room is available.
func (b *Buffer) Enqueue(ctx context.Context, batch []protocol.EventEnvelope) error {
	_, err := b.enqueue(ctx, batch, false)
	return err
}

// EnqueueWithReceipt accepts a private copy of batch once room is available
// and returns its one-shot durable result. The receipt is independent of
// Flush: another batch's success or rejection cannot resolve it.
func (b *Buffer) EnqueueWithReceipt(
	ctx context.Context,
	batch []protocol.EventEnvelope,
) (<-chan BufferReceipt, error) {
	return b.enqueue(ctx, batch, true)
}

func (b *Buffer) enqueue(
	ctx context.Context,
	batch []protocol.EventEnvelope,
	wantReceipt bool,
) (<-chan BufferReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}
	b.mu.Lock()
	if b.state != BufferOpen {
		b.mu.Unlock()
		return nil, ErrBufferClosed
	}
	b.mu.Unlock()
	var receipt chan BufferReceipt
	if wantReceipt {
		receipt = make(chan BufferReceipt, 1)
	}
	if len(batch) == 0 {
		if receipt != nil {
			receipt <- BufferReceipt{Stored: true}
			close(receipt)
		}
		return receipt, nil
	}
	count := int64(len(batch))
	bytes := batchBytes(batch)
	if len(batch) > MaxBatchEvents || count > b.maxEvents || bytes > b.maxBytes {
		return nil, fmt.Errorf("batch cannot fit buffer: %w", ErrInvalidArgument)
	}
	for i := range batch {
		if err := batch[i].Validate(); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
	}
	if err := b.reserve(ctx, count, bytes); err != nil {
		return nil, err
	}
	if hook := b.hooks.afterReserve; hook != nil {
		hook()
	}
	copyBatch := cloneEvents(batch)

	b.mu.Lock()
	b.reservations--
	if b.state != BufferOpen {
		b.releaseRoomLocked(count, bytes)
		b.tryCloseLocked()
		b.notifyLocked()
		b.mu.Unlock()
		return nil, ErrBufferClosed
	}
	b.accepted++
	b.queue = append(b.queue, &bufferedBatch{
		events:  copyBatch,
		count:   count,
		bytes:   bytes,
		id:      b.accepted,
		receipt: receipt,
	})
	if !b.writerStarted {
		b.writerStarted = true
		go b.runWriter()
	}
	b.notifyLocked()
	b.mu.Unlock()
	return receipt, nil
}

func (b *Buffer) reserve(ctx context.Context, events, bytes int64) error {
	b.mu.Lock()
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return fmt.Errorf("reserve buffer room: %w", err)
	}
	if b.state != BufferOpen {
		b.mu.Unlock()
		return ErrBufferClosed
	}
	waiter := &roomWaiter{events: events, bytes: bytes, granted: make(chan struct{})}
	b.waiters = append(b.waiters, waiter)
	b.grantWaitersLocked()
	for {
		if waiter.ready {
			b.mu.Unlock()
			return nil
		}
		if b.state != BufferOpen {
			b.removeWaiterLocked(waiter)
			b.mu.Unlock()
			return ErrBufferClosed
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-waiter.granted:
			return nil
		case <-changed:
			b.mu.Lock()
		case <-ctx.Done():
			b.mu.Lock()
			if waiter.ready {
				b.reservations--
				b.releaseRoomLocked(events, bytes)
			} else {
				b.removeWaiterLocked(waiter)
			}
			b.grantWaitersLocked()
			b.tryCloseLocked()
			b.notifyLocked()
			b.mu.Unlock()
			return fmt.Errorf("reserve buffer room: %w", ctx.Err())
		}
	}
}

func (b *Buffer) grantWaitersLocked() {
	if b.state != BufferOpen {
		return
	}
	for len(b.waiters) > 0 {
		waiter := b.waiters[0]
		if b.events+waiter.events > b.maxEvents || b.bytes+waiter.bytes > b.maxBytes {
			return
		}
		b.waiters[0] = nil
		b.waiters = b.waiters[1:]
		if len(b.waiters) == 0 {
			b.waiters = nil
		}
		b.events += waiter.events
		b.bytes += waiter.bytes
		b.reservations++
		waiter.ready = true
		close(waiter.granted)
	}
}

func (b *Buffer) removeWaiterLocked(target *roomWaiter) {
	for i, waiter := range b.waiters {
		if waiter == target {
			copy(b.waiters[i:], b.waiters[i+1:])
			last := len(b.waiters) - 1
			b.waiters[last] = nil
			b.waiters = b.waiters[:last]
			if len(b.waiters) == 0 {
				b.waiters = nil
			}
			return
		}
	}
}

// Flush waits for the batches accepted before its call to resolve.
func (b *Buffer) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	b.mu.Lock()
	if b.state != BufferOpen {
		b.mu.Unlock()
		return ErrBufferClosed
	}
	target := b.accepted
	b.mu.Unlock()
	if hook := b.hooks.afterFlushMark; hook != nil {
		hook(target)
	}
	b.mu.Lock()
	for {
		if b.resolved >= target {
			notStored := b.firstNotStore != 0 && b.firstNotStore <= target
			if !notStored {
				b.mu.Unlock()
				return nil
			}
			if b.state == BufferClosed {
				b.mu.Unlock()
				return ErrBufferClosed
			}
		}
		if b.state == BufferClosed {
			b.mu.Unlock()
			return ErrBufferClosed
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("flush: %w", ctx.Err())
		}
		b.mu.Lock()
	}
}

// Close drains accepted work, then cancels it at ctx's deadline.
func (b *Buffer) Close(ctx context.Context) error {
	b.mu.Lock()
	if b.state != BufferOpen {
		b.mu.Unlock()
		return ErrBufferClosed
	}
	b.state = BufferClosing
	if !b.writerStarted {
		b.writerExited = true
	}
	b.tryCloseLocked()
	b.notifyLocked()
	for b.state != BufferClosed && ctx.Err() == nil {
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
		}
		b.mu.Lock()
	}
	if b.state == BufferClosed {
		err := b.closeResultLocked(ctx.Err())
		b.mu.Unlock()
		return err
	}
	b.cancelWriter()
	b.notifyLocked()
	grace := b.closeGrace
	changed := b.changed
	b.mu.Unlock()
	timerC := b.hooks.graceTimer
	if timerC == nil {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		timerC = timer.C
	}
	select {
	case <-changed:
		if hook := b.hooks.afterGraceChange; hook != nil {
			hook()
		}
		b.mu.Lock()
		for b.state != BufferClosed {
			changed = b.changed
			b.mu.Unlock()
			select {
			case <-changed:
			case <-timerC:
				return b.closeAfterGrace(ctx.Err())
			}
			b.mu.Lock()
		}
		err := b.closeResultLocked(ctx.Err())
		b.mu.Unlock()
		return err
	case <-timerC:
		return b.closeAfterGrace(ctx.Err())
	}
}

func (b *Buffer) closeAfterGrace(ctxErr error) error {
	if hook := b.hooks.afterGraceTimer; hook != nil {
		hook()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BufferClosed {
		return b.closeResultLocked(ctxErr)
	}
	return b.activeCloseErrorLocked(ctxErr)
}

// Stats returns a consistent snapshot without waiting for the writer.
func (b *Buffer) Stats() BufferStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return BufferStats{
		State:     b.state,
		Events:    b.events,
		Bytes:     b.bytes,
		Stored:    b.stored,
		Rejected:  b.rejected,
		NotStored: b.notStored,
		Degraded:  b.degraded,
		LastError: b.lastError,
	}
}

func (b *Buffer) runWriter() {
	for {
		b.mu.Lock()
		if b.state == BufferClosing && b.inFlight == nil &&
			(len(b.queue) == 0 || b.wctx.Err() != nil) {
			b.writerExited = true
			b.finishWriterLocked()
			b.mu.Unlock()
			return
		}
		if len(b.queue) == 0 {
			changed := b.changed
			b.mu.Unlock()
			select {
			case <-changed:
			case <-b.wctx.Done():
			}
			continue
		}
		batch := b.takeBatchLocked()
		b.inFlight = batch
		b.inWrite = true
		b.mu.Unlock()

		err := b.store.AppendEvents(b.wctx, batch.events)
		firstWriteFailure := false
		if err != nil && !errors.Is(err, ErrDuplicateEvent) {
			b.mu.Lock()
			if b.wctx.Err() == nil && !b.degraded {
				b.degraded = true
				b.lastError = err
				firstWriteFailure = true
			}
			b.mu.Unlock()
		}
		if firstWriteFailure && b.onWriteFailure != nil {
			b.onWriteFailure(err)
		}
		if hook := b.hooks.afterAttempt; hook != nil {
			hook(err)
		}
		b.mu.Lock()
		b.inWrite = false
		resolved := false
		switch {
		case err == nil:
			b.stored += batch.count
			b.degraded = false
			b.lastError = nil
			b.resolveLocked(batch, BufferReceipt{Stored: true})
			resolved = true
		case errors.Is(err, ErrDuplicateEvent):
			b.rejected += batch.count
			b.inCallback = true
			b.notifyLocked()
			b.mu.Unlock()
			b.onRejected(b.wctx, Rejection{Batch: batch.events, Err: err})
			b.mu.Lock()
			b.inCallback = false
			b.degraded = false
			b.lastError = nil
			b.resolveLocked(batch, BufferReceipt{Err: err})
			resolved = true
		case b.wctx.Err() != nil:
			b.notStored += batch.count
			b.resolveLocked(batch, BufferReceipt{Err: ErrBufferClosed})
			resolved = true
		default:
			b.degraded = true
			b.lastError = err
			b.notifyLocked()
			b.mu.Unlock()
			if hook := b.hooks.afterRetry; hook != nil {
				hook()
			}
			timer := time.NewTimer(b.retryInterval)
			select {
			case <-timer.C:
			case <-b.wctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			}
			b.mu.Lock()
			if b.wctx.Err() != nil {
				b.notStored += batch.count
				b.resolveLocked(batch, BufferReceipt{Err: ErrBufferClosed})
				resolved = true
			} else {
				b.inFlight = nil
				b.queue = append([]*bufferedBatch{batch}, b.queue...)
			}
		}
		b.inFlight = nil
		b.notifyLocked()
		b.mu.Unlock()
		if resolved {
			if hook := b.hooks.afterResolve; hook != nil {
				hook()
			}
		}
	}
}

func (b *Buffer) takeBatchLocked() *bufferedBatch {
	batch := b.queue[0]
	b.queue[0] = nil
	b.queue = b.queue[1:]
	if len(b.queue) == 0 {
		b.queue = nil
	}
	return batch
}

func (b *Buffer) resolveLocked(batch *bufferedBatch, receipt BufferReceipt) {
	b.resolved++
	if !receipt.Stored && !errors.Is(receipt.Err, ErrDuplicateEvent) && b.firstNotStore == 0 {
		b.firstNotStore = batch.id
	}
	if batch.receipt != nil {
		batch.receipt <- receipt
		close(batch.receipt)
		batch.receipt = nil
	}
	b.releaseRoomLocked(batch.count, batch.bytes)
}

func (b *Buffer) releaseRoomLocked(events, bytes int64) {
	b.events -= events
	b.bytes -= bytes
	b.grantWaitersLocked()
}

func (b *Buffer) finishWriterLocked() {
	b.doneOnce.Do(func() { close(b.writerDone) })
	b.tryCloseLocked()
	b.notifyLocked()
}

func (b *Buffer) tryCloseLocked() {
	if b.state != BufferClosing || !b.writerExited || b.reservations != 0 {
		return
	}
	for _, batch := range b.queue {
		b.notStored += batch.count
		b.resolveLocked(batch, BufferReceipt{Err: ErrBufferClosed})
	}
	b.queue = nil
	b.state = BufferClosed
	b.doneOnce.Do(func() { close(b.writerDone) })
}

func (b *Buffer) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *Buffer) closeResultLocked(ctxErr error) error {
	if b.notStored == 0 {
		return nil
	}
	if ctxErr == nil {
		ctxErr = context.Canceled
	}
	return fmt.Errorf("close: %d events not stored: %w", b.notStored, ctxErr)
}

func (b *Buffer) activeCloseErrorLocked(ctxErr error) error {
	if ctxErr == nil {
		ctxErr = context.Canceled
	}
	notStored := b.notStored
	for _, batch := range b.queue {
		notStored += batch.count
	}
	if b.inFlight != nil && !b.inWrite && !b.inCallback && b.wctx.Err() != nil {
		notStored += b.inFlight.count
	}
	var message strings.Builder
	fmt.Fprintf(&message, "close: %d events not stored", notStored)
	if b.inWrite && b.inFlight != nil {
		fmt.Fprintf(&message, ", %d events still in a write", b.inFlight.count)
	}
	if b.inCallback {
		message.WriteString(", writer still in OnRejected")
	}
	if b.reservations != 0 {
		fmt.Fprintf(&message, ", %d enqueues still copying", b.reservations)
	}
	return fmt.Errorf("%s: %w", message.String(), ctxErr)
}

func cloneEvents(events []protocol.EventEnvelope) []protocol.EventEnvelope {
	clone := make([]protocol.EventEnvelope, len(events))
	copy(clone, events)
	for i := range clone {
		clone[i].Payload = append(protocol.Payload(nil), events[i].Payload...)
	}
	return clone
}

func batchBytes(events []protocol.EventEnvelope) int64 {
	var total int64
	for i := range events {
		event := &events[i]
		total += int64(len(event.EventID) + len(event.NodeID) + len(event.SessionID) + len(event.AgentID) +
			len(event.Harness) + len(event.HarnessVersion) + len(event.AdapterType) + len(event.SourceQuality) +
			len(event.EventType) + len(event.Direction) + len(event.Stream) + len(event.Origin) + len(event.TurnID) +
			len(event.ToolCallID) + len(event.MessageID) + len(event.DeliveryID) + len(event.DecisionID) +
			len(event.ParentEventID) + len(event.CorrelationID) + len(event.Payload) + len(event.Sensitivity))
	}
	return total
}
