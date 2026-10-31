//go:build unix

package session

import (
	"errors"
	"fmt"
	"io"
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
	// outputQuietPeriod is the calm required after the harness's last
	// output before a delivery may prepare or commit: a line written
	// while the harness paints lands in the middle of its redraw.
	outputQuietPeriod = 300 * time.Millisecond
	// reasonOutputBusy refuses a delivery while the harness paints.
	reasonOutputBusy = "agent output not quiet"
	// reasonInputNotEmpty refuses a delivery on a measured profile
	// whose input line under the cursor holds text.
	reasonInputNotEmpty = "input line not empty"
	// reasonNoInputEvidence refuses a delivery on a measured profile
	// when the screen shows no input line under the cursor to read.
	reasonNoInputEvidence = "no input evidence"
	// reasonUnmeasured refuses every delivery on a profile not yet
	// measured: its keys and input line are xterm guesses, so nothing
	// can show the input line empty, and nothing is written.
	reasonUnmeasured = "profile not measured"
	// defaultSubmitDelay waits between the delivered line and its
	// submit key.
	defaultSubmitDelay = 150 * time.Millisecond
	// maxDeliveryLineBytes caps a delivered line.
	maxDeliveryLineBytes = 4096
	// reasonIncomplete refuses a delivery while the parser holds part
	// of a human key sequence.
	reasonIncomplete = "human key sequence incomplete"
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

// DeliveryReport is one delivery answer with the generation and
// version it ran under and, for a commit, its time.
type DeliveryReport struct {
	Outcome    DeliveryOutcome
	Generation uint64
	Version    int64
	At         time.Time
}

// guardSnapshot is the loop's published guard foto: the chunks it has
// fully processed plus the guard truth, taken after each chunk, the
// last human submit time and the harness's last output time.
type guardSnapshot struct {
	Chunks     uint64
	State      draft.State
	Generation uint64
	Version    int64
	LastSubmit time.Time
	// LastOutput is when the harness last wrote output, on the
	// session's clock; the Proxy's own notices are not counted.
	LastOutput time.Time
	// Pending marks a human key sequence the parser holds part of:
	// those bytes are on the harness, and a line written now would
	// join them. It is no guard transition, so no version counts it.
	Pending bool
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

// errInjectMode marks a commit with no inject mode the session writes.
var errInjectMode = errors.New("session: unknown inject mode")

// pastedMode reports whether mode wraps the line in bracketed paste:
// typed writes it bare, bracketed_paste between the paste markers.
func pastedMode(mode protocol.InjectMode) (bool, error) {
	switch mode {
	case protocol.InjectModeTyped:
		return false, nil
	case protocol.InjectModeBracketedPaste:
		return true, nil
	default:
		return false, fmt.Errorf("session: commit delivery: %w", errInjectMode)
	}
}

// PrepareDelivery checks state, generation and version at once, on the
// guard settled first: a mismatch aborts immediately, a match records the writer counter
// and answers PREPARED for a later Commit. A human key sequence the
// parser holds part of aborts it too: no timer ends a lone ESC, so it
// holds every delivery until the next human key resolves it. So does
// harness output within outputQuietPeriod, and on a measured profile a
// screen with no empty input line under the cursor: the Core defers
// and tries again. A profile not yet measured refuses every prepare.
func (s *Session) PrepareDelivery(id protocol.OpaqueID, generation uint64, version int64) (DeliveryReport, error) {
	if !id.Valid() {
		return DeliveryReport{}, fmt.Errorf("session: prepare delivery: %w", ErrDeliveryUnknown)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return DeliveryReport{}, ErrSessionNotRunning
	}
	s.settleLocked()
	snap := s.snap
	reason := ""
	switch {
	case !s.opts.Profile.Measured:
		reason = reasonUnmeasured
	case snap.Pending:
		reason = reasonIncomplete
	case snap.State != draft.StateClosed || snap.Generation != generation || snap.Version != version:
		reason = "snapshot mismatch"
	case s.clk.Now().Sub(snap.LastOutput) < outputQuietPeriod:
		reason = reasonOutputBusy
	default:
		reason = s.inputEvidenceLocked()
	}
	if reason != "" {
		s.emitLocked(protocol.EventTypeInputDeliveryAbortedHumanPriority, DeliveryAborted{
			DeliveryID: id, Generation: snap.Generation, Version: snap.Version, Reason: reason,
		})
		return DeliveryReport{Outcome: DeliveryAbortedHumanPriorityOutcome, Generation: snap.Generation, Version: snap.Version}, nil
	}
	s.prepared[id] = pendingDelivery{generation: generation, version: version, written: s.humanWritten.Load(), arrived: s.arrived}
	s.emitLocked(protocol.EventTypeInputDeliveryPrepared, DeliveryPrepared{
		DeliveryID: id, Generation: generation, Version: version,
	})
	return DeliveryReport{Outcome: DeliveryPreparedOutcome, Generation: generation, Version: version}, nil
}

// CommitDelivery validates the mode and the line first, then hands the
// commit to the writer, which runs it without releasing its turn:
// drain, revalidation, lease, line, submit wait, submit, flush. The
// mode is the one the prepare stored, typed or bracketed_paste, and it
// decides the paste markers whatever the profile says.
func (s *Session) CommitDelivery(id protocol.OpaqueID, line string, mode protocol.InjectMode) (DeliveryReport, error) {
	pasted, err := pastedMode(mode)
	if err == nil {
		err = validateDeliveryLine(line)
	}
	if err != nil {
		s.mu.Lock()
		if s.running {
			s.emitLocked(protocol.EventTypeInputDeliveryFailed, deliveryFailed(id, err))
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
		submit:  []byte(s.opts.Profile.Submit), pasted: pasted,
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
	return DeliveryReport{Outcome: DeliveryCancelledOutcome, Generation: p.generation, Version: p.version}, nil
}

// sessionWriter is the single owner of middle PTY writes: one
// goroutine writes every human chunk the moment it arrives, counts
// them, forwards a copy to the loop, and runs commits without
// releasing its turn. Nothing else holds the descriptor.
type sessionWriter struct {
	s    *Session
	dst  io.Writer
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
// and forwards its copy to the loop. It counts the chunk sent before
// the write: the harness may answer it before the loop takes the copy.
// A write failure ends the writer: the pump error path tears the
// session down.
func (w *sessionWriter) writeHuman(c []byte) {
	w.s.humanSent.Add(1)
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
	r.done <- commitResult{report: DeliveryReport{Outcome: DeliveryAbortedHumanPriorityOutcome, Generation: r.gen, Version: r.ver}}
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
		// Sent before the write, as writeHuman counts it.
		w.s.humanSent.Add(1)
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
	r.done <- commitResult{report: DeliveryReport{Outcome: DeliveryCommittedOutcome, Generation: r.gen, Version: r.ver, At: at}}
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
	w.s.emitLocked(protocol.EventTypeInputDeliveryFailed, deliveryFailed(r.id, err))
	r.done <- commitResult{err: err}
}

// deliveryFailed is the input.delivery_failed of err: its text for the
// events file, and its fixed code, read with errors.Is, for the durable
// event. A failure the session does not name is a failed PTY write.
func deliveryFailed(id protocol.OpaqueID, err error) DeliveryFailed {
	code := protocol.DurableDeliveryFailurePTYWriteFailed
	switch {
	case errors.Is(err, errDeliveryLine):
		code = protocol.DurableDeliveryFailureLineInvalid
	case errors.Is(err, errInjectMode):
		code = protocol.DurableDeliveryFailureModeInvalid
	case errors.Is(err, ErrSessionNotRunning):
		code = protocol.DurableDeliveryFailureSessionNotRunning
	}
	return DeliveryFailed{DeliveryID: id, Reason: err.Error(), Code: code}
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
// section that takes its lease: the exact reservation is alive, the
// profile is measured, arrived equals written equals seen with no
// arrival since Prepare, no human key sequence held in part, closed,
// matching generation and version, calm after the last submit and the
// harness's last output, an empty input line on the screen when the
// profile has an input line pattern, and no other lease.
// A key pumpIn read but not yet handed over still refuses, and any
// later key lands unambiguously under the lease. Callers hold s.mu.
func (s *Session) revalidateLocked(r *commitRequest) (string, bool) {
	if !s.committing || s.committingID != r.id {
		return "commit not reserved", false
	}
	if !s.opts.Profile.Measured {
		return reasonUnmeasured, false
	}
	// One live load: a key already read but not yet written sits
	// between arrived and written, and a key since Prepare moves
	// either counter past its captured value.
	written := s.humanWritten.Load()
	if s.snap.Chunks != written {
		return "loop behind writer", false
	}
	if s.arrived != written {
		return "human input in flight", false
	}
	if r.written != written {
		return "human input since prepare", false
	}
	if r.arrived != s.arrived {
		return "human input since prepare", false
	}
	if s.snap.Pending {
		return reasonIncomplete, false
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
	if s.clk.Now().Sub(s.snap.LastOutput) < outputQuietPeriod {
		return reasonOutputBusy, false
	}
	if reason := s.inputEvidenceLocked(); reason != "" {
		return reason, false
	}
	if s.lease != nil {
		return "another lease", false
	}
	return "", true
}

// inputEvidenceLocked is why a delivery on a measured profile, Measured
// with an input line pattern, may not go on now, or "": the emulator's
// last snapshot must show the input line under the cursor, empty, read
// as InputHolds reads it. The guard's CLOSED never reads the screen
// again, so a guard that closed on a torn frame, the prompt redrawn
// before its text, would let a delivery write over a draft that came
// back. Any other profile gives no such evidence and changes nothing.
// Callers hold s.mu.
func (s *Session) inputEvidenceLocked() string {
	p := s.opts.Profile
	if !p.Measured || p.InputLine == nil {
		return ""
	}
	if !s.hasSnap || !s.running {
		return reasonNoInputEvidence
	}
	text, ok := draft.CursorInput(p, s.lastSnap)
	switch {
	case !ok:
		return reasonNoInputEvidence
	case text != "":
		return reasonInputNotEmpty
	}
	return ""
}
