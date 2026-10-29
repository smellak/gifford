//go:build unix

package session

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

// Two-phase delivery constants.
const (
	// leaseBufferMax caps the human bytes held while a lease is
	// taken. Past it the writer stops receiving and pumpIn waits,
	// so no key is lost.
	leaseBufferMax = 4 * 1024
	// humanQuietPeriod is the calm required after the last human
	// submit before a commit may proceed.
	humanQuietPeriod = 300 * time.Millisecond
	// defaultSubmitDelay waits between the delivered line and its
	// submit key.
	defaultSubmitDelay = 150 * time.Millisecond
	// maxDeliveryLineBytes caps a delivered line.
	maxDeliveryLineBytes = 4096
	// pasteBegin and pasteEnd wrap a bracketed-paste delivery.
	pasteBegin = "\x1b[200~"
	pasteEnd   = "\x1b[201~"
)

// Delivery errors.
var (
	// ErrDeliveryUnknown means no Prepare with that id awaits a commit.
	ErrDeliveryUnknown = errors.New("session: unknown delivery")
	// ErrDeliveryActive means the delivery already took its lease.
	ErrDeliveryActive = errors.New("session: delivery already started")
	// ErrSessionNotRunning means delivery needs a live session.
	ErrSessionNotRunning = errors.New("session: session not running")
)

// DeliveryOutcome is one two-phase delivery answer.
type DeliveryOutcome string

// Delivery outcomes.
const (
	// DeliveryPrepared answers a Prepare that matched state,
	// generation and version.
	DeliveryPreparedOutcome DeliveryOutcome = "prepared"
	// DeliveryCommitted answers a Commit written whole.
	DeliveryCommittedOutcome DeliveryOutcome = "committed"
	// DeliveryAbortedHumanPriority answers any refusal: human
	// priority won and none of the line reached the harness.
	DeliveryAbortedHumanPriorityOutcome DeliveryOutcome = "aborted_human_priority"
	// DeliveryCancelledOutcome answers a Cancel before the commit.
	DeliveryCancelledOutcome DeliveryOutcome = "cancelled"
)

// DeliveryReport is one delivery answer with the generation it ran
// under and, for a commit, its time.
type DeliveryReport struct {
	Outcome    DeliveryOutcome
	Generation uint64
	At         time.Time
}

// guardSnapshot is the loop's published guard foto: the chunks it has
// fully processed plus the guard truth, taken after each chunk, and
// the last human submit time.
type guardSnapshot struct {
	Chunks     uint64
	State      draft.State
	Generation uint64
	Version    int64
	LastSubmit time.Time
}

// pendingDelivery is one accepted Prepare awaiting its Commit.
type pendingDelivery struct {
	generation uint64
	version    int64
	written    uint64
	arrived    uint64
}

// deliveryLease is one taken lease: later human chunks buffer here,
// in order, until the submit flushes them.
type deliveryLease struct {
	id       protocol.OpaqueID
	buf      [][]byte
	buffered int
}

// commitRequest is one Commit handed to the writer, answered on done.
type commitRequest struct {
	id      protocol.OpaqueID
	line    string
	gen     uint64
	ver     int64
	written uint64
	arrived uint64
	submit  []byte
	pasted  bool
	delay   time.Duration
	done    chan commitResult
}

// commitResult is one executed commit answer.
type commitResult struct {
	report DeliveryReport
	err    error
}

// writerOp is one unit of writer work.
type writerOp struct {
	commit *commitRequest
}

// validateDeliveryLine rejects anything but one printable line
// within the fixed byte cap, before anything is written.
func validateDeliveryLine(line string) error {
	// An empty line would turn the delivery into a bare submit:
	// refuse it before anything is written.
	if line == "" {
		return fmt.Errorf("session: delivery line empty: %w", errDeliveryLine)
	}
	if len(line) > maxDeliveryLineBytes {
		return fmt.Errorf("session: delivery line over %d bytes: %w", maxDeliveryLineBytes, errDeliveryLine)
	}
	if !utf8.ValidString(line) {
		return fmt.Errorf("session: delivery line not UTF-8: %w", errDeliveryLine)
	}
	for _, r := range line {
		if r == '\n' || !unicode.IsPrint(r) {
			return fmt.Errorf("session: delivery line not one printable line: %w", errDeliveryLine)
		}
	}
	return nil
}

// errDeliveryLine marks a rejected delivery line.
var errDeliveryLine = errors.New("session: invalid delivery line")

// PrepareDelivery checks state, generation and version at once: a
// mismatch aborts immediately, a match records the writer counter
// and answers PREPARED for a later Commit.
func (s *Session) PrepareDelivery(id protocol.OpaqueID, generation uint64, version int64) (DeliveryReport, error) {
	if !id.Valid() {
		return DeliveryReport{}, fmt.Errorf("session: prepare delivery: %w", ErrDeliveryUnknown)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return DeliveryReport{}, ErrSessionNotRunning
	}
	snap := s.snap
	if snap.State != draft.StateClosed || snap.Generation != generation || snap.Version != version {
		s.emitLocked(protocol.EventTypeInputDeliveryAbortedHumanPriority, DeliveryAborted{
			DeliveryID: id, Generation: snap.Generation, Version: snap.Version, Reason: "snapshot mismatch",
		})
		return DeliveryReport{Outcome: DeliveryAbortedHumanPriorityOutcome, Generation: snap.Generation}, nil
	}
	s.prepared[id] = pendingDelivery{generation: generation, version: version, written: s.humanWritten.Load(), arrived: s.arrived}
	s.emitLocked(protocol.EventTypeInputDeliveryPrepared, DeliveryPrepared{
		DeliveryID: id, Generation: generation, Version: version,
	})
	return DeliveryReport{Outcome: DeliveryPreparedOutcome, Generation: generation}, nil
}

// CommitDelivery validates the line first, then hands the commit to
// the writer, which runs it without releasing its turn: drain,
// revalidation, lease, line, submit wait, submit, flush.
func (s *Session) CommitDelivery(id protocol.OpaqueID, line string) (DeliveryReport, error) {
	if err := validateDeliveryLine(line); err != nil {
		s.mu.Lock()
		if s.running {
			s.emitLocked(protocol.EventTypeInputDeliveryFailed, DeliveryFailed{DeliveryID: id, Reason: err.Error()})
		}
		s.mu.Unlock()
		return DeliveryReport{}, err
	}
	if !id.Valid() {
		return DeliveryReport{}, fmt.Errorf("session: commit delivery: %w", ErrDeliveryUnknown)
	}
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return DeliveryReport{}, ErrSessionNotRunning
	}
	p, ok := s.prepared[id]
	if !ok {
		s.mu.Unlock()
		return DeliveryReport{}, fmt.Errorf("session: commit delivery: %w", ErrDeliveryUnknown)
	}
	// A commit in flight owns the writer turn: a second Commit for
	// any id, and a Cancel for this one, refuse while the
	// reservation lives, so CANCELLED plus delivered bytes and
	// double submits cannot interleave.
	if s.lease != nil || s.committing {
		s.mu.Unlock()
		return DeliveryReport{}, fmt.Errorf("session: commit delivery: %w", ErrDeliveryActive)
	}
	s.committing = true
	s.committingID = id
	delay := s.opts.SubmitDelay
	if delay <= 0 {
		delay = defaultSubmitDelay
	}
	op := writerOp{commit: &commitRequest{
		id: id, line: line, gen: p.generation, ver: p.version, written: p.written,
		arrived: p.arrived,
		submit:  []byte(s.opts.Profile.Submit), pasted: s.opts.Profile.BracketedPaste,
		delay: delay, done: make(chan commitResult, 1),
	}}
	s.mu.Unlock()
	select {
	case s.opCh <- op:
	case <-s.stopCh:
		s.mu.Lock()
		s.unreserveLocked(id)
		s.mu.Unlock()
		return DeliveryReport{}, ErrSessionNotRunning
	}
	res := <-op.commit.done
	if res.err != nil {
		return DeliveryReport{}, res.err
	}
	return res.report, nil
}

// CancelDelivery discards a Prepare that has not started and answers
// CANCELLED. The reason is recorded, never interpreted.
func (s *Session) CancelDelivery(id protocol.OpaqueID, reason string) (DeliveryReport, error) {
	if !id.Valid() {
		return DeliveryReport{}, fmt.Errorf("session: cancel delivery: %w", ErrDeliveryUnknown)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return DeliveryReport{}, ErrSessionNotRunning
	}
	p, ok := s.prepared[id]
	if !ok {
		return DeliveryReport{}, fmt.Errorf("session: cancel delivery: %w", ErrDeliveryUnknown)
	}
	// A commit already handed to the writer owns this id: refuse
	// instead of answering CANCELLED for bytes about to land.
	if (s.committing && s.committingID == id) || (s.lease != nil && s.lease.id == id) {
		return DeliveryReport{}, fmt.Errorf("session: cancel delivery: %w", ErrDeliveryActive)
	}
	delete(s.prepared, id)
	s.emitLocked(protocol.EventTypeInputDeliveryCancelled, DeliveryCancelled{DeliveryID: id, Reason: reason})
	return DeliveryReport{Outcome: DeliveryCancelledOutcome, Generation: p.generation}, nil
}

// sessionWriter is the single owner of middle PTY writes: one
// goroutine writes every human chunk the moment it arrives, counts
// them, forwards a copy to the loop, and runs commits without
// releasing its turn. Nothing else holds the descriptor.
type sessionWriter struct {
	s    *Session
	dst  *os.File
	in   <-chan []byte
	loop chan<- []byte
	ops  <-chan writerOp
	done <-chan struct{}
	perr chan<- error
	// inputGone marks pumpIn's channel closed: no drain runs after
	// it, and the loop only watches ops and teardown.
	inputGone bool
}

// run serves human chunks and commit ops until the session ends,
// then closes the loop channel so the loop drains to its end.
func (w *sessionWriter) run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(w.loop)
	for {
		var in <-chan []byte
		if !w.inputGone && !w.leaseFull() {
			in = w.in
		}
		select {
		case c, ok := <-in:
			if !ok {
				w.inputGone = true
				continue
			}
			w.handleChunk(c)
		case op := <-w.ops:
			w.handleCommit(op)
		case <-w.done:
			return
		}
	}
}

// leaseFull reports a full lease buffer: the writer stops receiving
// and pumpIn waits, so no key is lost.
func (w *sessionWriter) leaseFull() bool {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	return w.s.lease != nil && w.s.lease.buffered >= leaseBufferMax
}

// handleChunk writes one human chunk at once, counts it, and passes a
// copy to the loop. Under a lease it buffers instead, in order.
func (w *sessionWriter) handleChunk(c []byte) {
	w.s.mu.Lock()
	if w.s.lease != nil {
		l := w.s.lease
		l.buf = append(l.buf, c)
		l.buffered += len(c)
		w.s.mu.Unlock()
		return
	}
	w.s.mu.Unlock()
	w.writeHuman(c)
}

// writeHuman puts human bytes on the middle PTY, counts the chunk,
// and forwards its copy to the loop. A write failure ends the
// writer: the pump error path tears the session down.
func (w *sessionWriter) writeHuman(c []byte) {
	if err := writeFull(w.dst, c); err != nil {
		w.failPump(err)
		return
	}
	w.s.humanWritten.Add(1)
	select {
	case w.loop <- c:
	case <-w.done:
	}
}

// failPump reports a middle PTY write failure like the pumps do.
func (w *sessionWriter) failPump(err error) {
	select {
	case w.perr <- fmt.Errorf("%w: %w", errPumpInput, err):
	case <-w.done:
	}
}

// abortDelivery records an aborted commit without writing any of its
// line and answers ABORTED_HUMAN_PRIORITY.
func (w *sessionWriter) abortDelivery(r *commitRequest, reason string) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	delete(w.s.prepared, r.id)
	w.s.unreserveLocked(r.id)
	w.s.emitLocked(protocol.EventTypeInputDeliveryAbortedHumanPriority, DeliveryAborted{
		DeliveryID: r.id, Generation: r.gen, Version: r.ver, Reason: reason,
	})
	r.done <- commitResult{report: DeliveryReport{Outcome: DeliveryAbortedHumanPriorityOutcome, Generation: r.gen}}
}

// handleCommit runs one commit without releasing the writer turn:
// drain human arrivals, revalidate, lease, line, submit wait,
// submit, release, flush, answer.
func (w *sessionWriter) handleCommit(op writerOp) {
	r := op.commit
	// 1. Drain without waiting what the human already sent: any of
	// it goes out first and the commit aborts.
	drained := false
	if !w.inputGone {
		draining := true
		for draining {
			select {
			case c, ok := <-w.in:
				if !ok {
					w.inputGone = true
					draining = false
					continue
				}
				w.writeHuman(c)
				drained = true
			default:
				draining = false
			}
		}
	}
	if drained {
		w.abortDelivery(r, "human input arrived")
		return
	}
	// 2. Revalidate against the published snapshot and the live
	// lease: the foto must have seen everything written, nothing
	// human since Prepare, closed state, matching generation and
	// version, a calm 300 ms, and no other lease.
	w.s.mu.Lock()
	reason, ok := w.s.revalidateLocked(r)
	if ok {
		w.s.lease = &deliveryLease{id: r.id}
		w.s.emitLocked(protocol.EventTypeInputDeliveryRevalidated, DeliveryRevalidated{
			DeliveryID: r.id, Generation: r.gen, Version: r.ver,
		})
		w.s.emitLocked(protocol.EventTypeInputDeliveryStarted, DeliveryStarted{
			DeliveryID: r.id, Generation: r.gen, Version: r.ver,
		})
	}
	w.s.mu.Unlock()
	if !ok {
		w.abortDelivery(r, reason)
		return
	}
	// 3-4. Write the line per profile, wait out the submit delay,
	// write the submit key. A teardown in between releases the
	// lease and fails the op.
	// The line goes out per profile; the submit key follows only
	// after the delay, once, below.
	var out []byte
	if r.pasted {
		out = append([]byte(pasteBegin), append([]byte(r.line), pasteEnd...)...)
	} else {
		out = []byte(r.line)
	}
	if w.stopped() {
		w.releaseLease()
		w.s.mu.Lock()
		w.s.unreserveLocked(r.id)
		w.s.mu.Unlock()
		r.done <- commitResult{err: ErrSessionNotRunning}
		return
	}
	if err := writeFull(w.dst, out); err != nil {
		w.releaseLease()
		w.failDelivery(r, err)
		return
	}
	// The submit delay is a live wait: human chunks arriving under
	// the lease buffer in order instead of piling in the channel,
	// and a full buffer back-pressures the terminal through the
	// deselected channel, so no key is lost.
	timer := time.NewTimer(r.delay)
	waiting := true
	for waiting {
		var inch <-chan []byte
		if !w.inputGone && !w.leaseFull() {
			inch = w.in
		}
		select {
		case <-timer.C:
			waiting = false
		case <-w.done:
			timer.Stop()
			w.releaseLease()
			w.s.mu.Lock()
			w.s.unreserveLocked(r.id)
			w.s.mu.Unlock()
			r.done <- commitResult{err: ErrSessionNotRunning}
			return
		case c, ok := <-inch:
			if !ok {
				w.inputGone = true
				continue
			}
			w.handleChunk(c)
		}
	}
	timer.Stop()
	if w.stopped() {
		w.releaseLease()
		w.s.mu.Lock()
		w.s.unreserveLocked(r.id)
		w.s.mu.Unlock()
		r.done <- commitResult{err: ErrSessionNotRunning}
		return
	}
	if err := writeFull(w.dst, r.submit); err != nil {
		w.releaseLease()
		w.failDelivery(r, err)
		return
	}
	// 5. Release the lease, flush the buffer in order, answer
	// COMMITTED with the time and generation.
	w.s.mu.Lock()
	l := w.s.lease
	w.s.lease = nil
	w.s.mu.Unlock()
	var flushed int64
	for _, c := range l.buf {
		if err := writeFull(w.dst, c); err != nil {
			w.failDelivery(r, err)
			return
		}
		w.s.humanWritten.Add(1)
		select {
		case w.loop <- c:
		case <-w.done:
			w.failDelivery(r, ErrSessionNotRunning)
			return
		}
		flushed += int64(len(c))
	}
	at := w.s.clk.Now()
	w.s.mu.Lock()
	if flushed > 0 {
		w.s.emitLocked(protocol.EventTypeInputDeliveryChunk, DeliveryChunk{
			DeliveryID: r.id, Generation: r.gen, Version: r.ver, Bytes: flushed,
		})
	}
	delete(w.s.prepared, r.id)
	w.s.unreserveLocked(r.id)
	w.s.emitLocked(protocol.EventTypeInputDeliveryCommitted, DeliveryCommitted{
		DeliveryID: r.id, Generation: r.gen, Version: r.ver,
	})
	w.s.mu.Unlock()
	r.done <- commitResult{report: DeliveryReport{Outcome: DeliveryCommittedOutcome, Generation: r.gen, At: at}}
}

// releaseLease drops a taken lease without flushing.
func (w *sessionWriter) releaseLease() {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	w.s.lease = nil
}

// failDelivery records a commit that broke mid-write and answers its
// error: the line may be partial, never silent.
func (w *sessionWriter) failDelivery(r *commitRequest, err error) {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	delete(w.s.prepared, r.id)
	w.s.unreserveLocked(r.id)
	w.s.emitLocked(protocol.EventTypeInputDeliveryFailed, DeliveryFailed{
		DeliveryID: r.id, Reason: err.Error(),
	})
	r.done <- commitResult{err: err}
}

// stopped reports session teardown.
func (w *sessionWriter) stopped() bool {
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

// revalidateLocked rechecks one commit inside the same critical
// section that takes its lease: the exact reservation is alive,
// arrived equals written equals seen with no arrival since Prepare,
// closed, matching generation and version, calm, and no other lease.
// A key pumpIn read but not yet handed over still refuses, and any
// later key lands unambiguously under the lease. Callers hold s.mu.
func (s *Session) revalidateLocked(r *commitRequest) (string, bool) {
	if !s.committing || s.committingID != r.id {
		return "commit not reserved", false
	}
	if s.snap.Chunks != s.humanWritten.Load() {
		return "loop behind writer", false
	}
	if r.written != s.humanWritten.Load() {
		return "human input since prepare", false
	}
	if s.arrived != r.arrived {
		return "human input arrived", false
	}
	if s.snap.State != draft.StateClosed {
		return "draft not closed", false
	}
	if s.snap.Generation != r.gen || s.snap.Version != r.ver {
		return "generation or version moved", false
	}
	if s.clk.Now().Sub(s.snap.LastSubmit) < humanQuietPeriod {
		return "human submitted recently", false
	}
	if s.lease != nil {
		return "another lease", false
	}
	return "", true
}
