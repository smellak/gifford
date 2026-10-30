//go:build unix

package agentsock

import (
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	// ConsumeWindow is the fixed wait after a commit's submit before
	// the input line is read for evidence that the harness did not
	// consume the submit.
	ConsumeWindow = 500 * time.Millisecond
	// maxPrepared bounds the deliveries prepared and not yet closed
	// on one connection.
	maxPrepared = 16
	// maxClosed bounds the closed deliveries a connection remembers,
	// so a late commit or cancel for one is FAILED, not unknown.
	maxClosed = 16
)

// ErrDeliveryActive means a commit holds the delivery: it cannot be
// cancelled, and no other commit starts until it ends.
var ErrDeliveryActive = errors.New("agentsock: delivery commit in progress")

// Delivery is the session side of the two-phase PTY delivery.
type Delivery interface {
	// CheckLine refuses a line the session would not write.
	CheckLine(line string) error
	// Prepare checks the session against the expected generation and
	// version: PREPARED, or ABORTED_HUMAN_PRIORITY.
	Prepare(id protocol.OpaqueID, generation uint64, version int64) (DeliveryReport, error)
	// Commit writes the line of a prepared delivery in mode, then its
	// submit: COMMITTED, or ABORTED_HUMAN_PRIORITY with nothing
	// written.
	Commit(id protocol.OpaqueID, line string, mode protocol.InjectMode) (DeliveryReport, error)
	// Cancel drops a prepared delivery whose commit has not started:
	// CANCELLED, or ErrDeliveryActive.
	Cancel(id protocol.OpaqueID, reason protocol.CancelReason) (DeliveryReport, error)
	// DefaultMode is the inject mode of the harness profile.
	DefaultMode() protocol.InjectMode
	// InputHolds reports positive evidence that the harness input
	// line holds exactly line. Anything ambiguous is false.
	InputHolds(line string) bool
}

// DeliveryReport is the session's answer to one delivery call, with the
// draft generation and guard version it ran under.
type DeliveryReport struct {
	Result     protocol.DeliveryResultKind
	Generation uint64
	Version    int64
}

// The fixed errors of FAILED results and the unknown delivery frame
// error; none quotes an order.
const (
	failLine           = "notice line refused"
	failConflict       = "delivery already prepared with other content"
	failFull           = "too many prepared deliveries"
	failGeneration     = "expected draft generation out of range"
	failActive         = "delivery commit in progress"
	failClosed         = "delivery already closed"
	failSession        = "delivery failed in the session"
	msgUnknownDelivery = "unknown delivery"
)

// liveDelivery is one prepared delivery of a connection.
type liveDelivery struct {
	// order is the prepare as it came; its line, mode and control
	// versions are the ones the commit uses.
	order protocol.PrepareDelivery
	// prepared is the PREPARED answer, sent again for a repeat.
	prepared   protocol.DeliveryResult
	committing bool
}

// closedDelivery is one closed delivery a connection remembers.
type closedDelivery struct {
	controls protocol.ControlVersions
	// written means its commit ran: the line may be on the PTY, so
	// the id is never prepared again.
	written bool
}

// deliveries is one connection's delivery state.
type deliveries struct {
	mu     sync.Mutex
	live   map[protocol.OpaqueID]*liveDelivery
	closed map[protocol.OpaqueID]closedDelivery
	// order holds the closed ids, oldest first.
	order []protocol.OpaqueID
	// inFlight is the delivery whose commit runs, or "".
	inFlight protocol.OpaqueID
	paused   bool
}

// closeLocked forgets a live delivery and remembers it closed, the
// oldest closed one going once maxClosed are kept. Callers hold mu.
func (d *deliveries) closeLocked(id protocol.OpaqueID, controls protocol.ControlVersions, written bool) {
	delete(d.live, id)
	if _, ok := d.closed[id]; !ok {
		d.order = append(d.order, id)
		if len(d.order) > maxClosed {
			delete(d.closed, d.order[0])
			d.order = d.order[1:]
		}
	}
	d.closed[id] = closedDelivery{controls: controls, written: written}
}

// forgetLocked drops a closed delivery that is prepared again. Callers
// hold mu.
func (d *deliveries) forgetLocked(id protocol.OpaqueID) {
	delete(d.closed, id)
	for i, o := range d.order {
		if o == id {
			d.order = append(d.order[:i], d.order[i+1:]...)
			return
		}
	}
}

// deliver runs one delivery or automation order of a validated frame
// from the Core. Native events and notifications are not this Proxy's.
func (c *conn) deliver(f protocol.AgentFrame) bool {
	switch f.Op {
	case protocol.AgentOpPrepareDelivery:
		var o protocol.PrepareDelivery
		if json.Unmarshal(f.Body, &o) != nil || o.AgentID != c.agent {
			return c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame)
		}
		return c.prepare(f.FrameID, o)
	case protocol.AgentOpCommitDelivery:
		var o protocol.CommitDelivery
		if json.Unmarshal(f.Body, &o) != nil {
			return c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame)
		}
		return c.commit(f.FrameID, o)
	case protocol.AgentOpCancelDelivery:
		var o protocol.CancelDelivery
		if json.Unmarshal(f.Body, &o) != nil {
			return c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame)
		}
		return c.cancel(f.FrameID, o)
	case protocol.AgentOpPauseAutomation, protocol.AgentOpResumeAutomation:
		// Pause and resume answer nothing. A pause cancels every
		// prepared delivery at once, so none survives it to be
		// committed after resume. Neither touches the human's keys.
		var target protocol.AgentTarget
		if json.Unmarshal(f.Body, &target) != nil || target.AgentID != c.agent {
			return c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame)
		}
		c.dl.mu.Lock()
		c.dl.paused = f.Op == protocol.AgentOpPauseAutomation
		if c.dl.paused {
			c.cancelPreparedLocked()
		}
		c.dl.mu.Unlock()
		return true
	default:
		return c.refuse(f.FrameID, protocol.FrameErrorCodeUnsupportedOp, msgUnsupportedOp)
	}
}

// prepare stores a delivery and asks the session to check it. A repeat
// of a live prepare gets the same PREPARED again; any other prepare for
// a live id is FAILED and leaves it alone. A paused connection cancels
// what it is asked to prepare: the pause left no delivery prepared.
func (c *conn) prepare(frameID int64, o protocol.PrepareDelivery) bool {
	d := &c.dl
	d.mu.Lock()
	defer d.mu.Unlock()
	id, controls := o.DeliveryID, o.ControlVersions
	if l, ok := d.live[id]; ok {
		switch {
		case l.committing:
			return c.answer(frameID, c.failed(id, controls, failActive))
		case !samePrepare(l.order, o):
			return c.answer(frameID, c.failed(id, controls, failConflict))
		default:
			return c.answer(frameID, l.prepared)
		}
	}
	if rec, ok := d.closed[id]; ok {
		if rec.written {
			return c.answer(frameID, c.failed(id, controls, failClosed))
		}
		d.forgetLocked(id)
	}
	if d.paused {
		d.closeLocked(id, controls, false)
		return c.answer(frameID, c.cancelled(id, controls))
	}
	fail := func(msg string) bool {
		d.closeLocked(id, controls, false)
		return c.answer(frameID, c.failed(id, controls, msg))
	}
	// The contract's check, then the session's own, the one its
	// commit runs: it refuses C1 controls as well.
	if !o.NoticeLine.Valid() || c.opts.Delivery.CheckLine(string(o.NoticeLine)) != nil {
		return fail(failLine)
	}
	// The frame already holds both at zero or more; the conversion
	// below must not wrap all the same.
	if o.ExpectedDraftGeneration < 0 {
		return fail(failGeneration)
	}
	if len(d.live) >= maxPrepared {
		return fail(failFull)
	}
	// expected_state_version is the daemon's: the Proxy checks only
	// the draft generation and the guard version.
	rep, err := c.opts.Delivery.Prepare(id, uint64(o.ExpectedDraftGeneration), o.ExpectedInputGuardVersion)
	if err != nil {
		return fail(failSession)
	}
	res := c.reported(id, controls, rep)
	switch rep.Result {
	case protocol.DeliveryResultKindPrepared:
		d.live[id] = &liveDelivery{order: o, prepared: res}
	case protocol.DeliveryResultKindAbortedHumanPriority:
		d.closeLocked(id, controls, false)
	default:
		return fail(failSession)
	}
	return c.answer(frameID, res)
}

// commit starts the commit of a prepared delivery with its stored line
// and mode, and adopts the Core's writer lease. It runs apart, so the
// reader goes on: a cancel or a second commit meanwhile is FAILED.
func (c *conn) commit(frameID int64, o protocol.CommitDelivery) bool {
	d := &c.dl
	d.mu.Lock()
	id := o.DeliveryID
	l, ok := d.live[id]
	if !ok {
		rec, known := d.closed[id]
		paused := d.paused
		d.mu.Unlock()
		switch {
		case !known:
			return c.refuse(frameID, protocol.FrameErrorCodeUnknownDelivery, msgUnknownDelivery)
		case paused && !rec.written:
			// While paused every commit is CANCELLED: the pause
			// cancelled this delivery, or it closed unwritten.
			return c.answer(frameID, leased(c.cancelled(id, rec.controls), o.WriterLeaseID))
		default:
			return c.answer(frameID, leased(c.failed(id, rec.controls, failClosed), o.WriterLeaseID))
		}
	}
	// A pause cancelled every delivery not committing, so a live one
	// while paused is the commit running. A commit runs, this one or
	// another: nothing else starts.
	if d.inFlight != "" {
		d.mu.Unlock()
		return c.answer(frameID, leased(c.failed(id, l.order.ControlVersions, failActive), o.WriterLeaseID))
	}
	l.committing = true
	d.inFlight = id
	order := l.order
	d.mu.Unlock()
	mode := order.InjectMode
	if mode == "" {
		mode = c.opts.Delivery.DefaultMode()
	}
	c.jobs.Add(1)
	go func() {
		defer c.jobs.Done()
		c.runCommit(frameID, order, o.WriterLeaseID, mode)
	}()
	return true
}

// runCommit runs one commit to its end and answers it. A commit the
// session wrote waits ConsumeWindow, then reads the input line: only
// positive evidence that it still holds exactly the notice is
// COMMIT_NOT_CONSUMED, since a retry without it could deliver twice.
func (c *conn) runCommit(frameID int64, o protocol.PrepareDelivery, lease protocol.OpaqueID, mode protocol.InjectMode) {
	id, controls, line := o.DeliveryID, o.ControlVersions, string(o.NoticeLine)
	rep, err := c.opts.Delivery.Commit(id, line, mode)
	var res protocol.DeliveryResult
	written := true
	switch {
	case err != nil:
		res = c.failed(id, controls, failSession)
	case rep.Result == protocol.DeliveryResultKindCommitted:
		select {
		case <-c.opts.Clock.After(ConsumeWindow):
		case <-c.done:
			// Nobody is left to answer.
			return
		}
		if c.opts.Delivery.InputHolds(line) {
			rep.Result = protocol.DeliveryResultKindCommitNotConsumed
		}
		res = c.reported(id, controls, rep)
		res.InjectMode = mode
	case rep.Result == protocol.DeliveryResultKindAbortedHumanPriority:
		written = false
		res = c.reported(id, controls, rep)
	default:
		res = c.failed(id, controls, failSession)
	}
	c.dl.mu.Lock()
	c.dl.inFlight = ""
	c.dl.closeLocked(id, controls, written)
	c.dl.mu.Unlock()
	if !c.answer(frameID, leased(res, lease)) {
		c.close()
	}
}

// cancel drops a prepared delivery whose commit has not started.
func (c *conn) cancel(frameID int64, o protocol.CancelDelivery) bool {
	d := &c.dl
	d.mu.Lock()
	defer d.mu.Unlock()
	id := o.DeliveryID
	l, ok := d.live[id]
	if !ok {
		rec, known := d.closed[id]
		if !known {
			return c.refuse(frameID, protocol.FrameErrorCodeUnknownDelivery, msgUnknownDelivery)
		}
		return c.answer(frameID, c.failed(id, rec.controls, failClosed))
	}
	controls := l.order.ControlVersions
	if l.committing {
		return c.answer(frameID, c.failed(id, controls, failActive))
	}
	rep, err := c.opts.Delivery.Cancel(id, o.Reason)
	if errors.Is(err, ErrDeliveryActive) {
		return c.answer(frameID, c.failed(id, controls, failActive))
	}
	d.closeLocked(id, controls, false)
	if err != nil {
		return c.answer(frameID, c.failed(id, controls, failSession))
	}
	res := c.reported(id, controls, rep)
	res.Result, res.CancelReason = protocol.DeliveryResultKindCancelled, o.Reason
	return c.answer(frameID, res)
}

// dropPrepared cancels the prepared deliveries once the connection that
// prepared them is gone.
func (c *conn) dropPrepared() {
	c.dl.mu.Lock()
	defer c.dl.mu.Unlock()
	c.cancelPreparedLocked()
}

// cancelPreparedLocked cancels in the session, with STATE_CHANGED, every
// delivery whose commit has not started, and closes it unwritten: a
// pause or the end of the connection voids them. A commit already
// running goes on to its end. Callers hold c.dl.mu.
func (c *conn) cancelPreparedLocked() {
	d := &c.dl
	for id, l := range d.live {
		if l.committing {
			continue
		}
		_, _ = c.opts.Delivery.Cancel(id, protocol.CancelReasonStateChanged)
		d.closeLocked(id, l.order.ControlVersions, false)
	}
}

// answer queues one delivery result answering frameID.
func (c *conn) answer(frameID int64, res protocol.DeliveryResult) bool {
	return c.queue(outbound{op: protocol.AgentOpReportDeliveryResult, replyTo: frameID, body: res})
}

// result is one delivery result of this connection, observed now.
func (c *conn) result(id protocol.OpaqueID, kind protocol.DeliveryResultKind, controls protocol.ControlVersions, generation, version int64) protocol.DeliveryResult {
	return protocol.DeliveryResult{
		DeliveryID: id, AgentID: c.agent, Result: kind, ObservedAt: c.opts.Clock.Now(),
		ObservedDraftGeneration: generation, ObservedInputGuardVersion: &version,
		ControlVersions: controls,
	}
}

// reported is the result of a session answer, with the generation and
// version it ran under.
func (c *conn) reported(id protocol.OpaqueID, controls protocol.ControlVersions, rep DeliveryReport) protocol.DeliveryResult {
	return c.result(id, rep.Result, controls, wireGeneration(rep.Generation), rep.Version)
}

// observed is a result the session was not asked for: it carries the
// guard's current generation and version.
func (c *conn) observed(id protocol.OpaqueID, kind protocol.DeliveryResultKind, controls protocol.ControlVersions) protocol.DeliveryResult {
	g := c.opts.Guard.InputGuard()
	return c.result(id, kind, controls, g.DraftGeneration, g.InputGuardVersion)
}

// failed is a FAILED result with one of the fixed errors.
func (c *conn) failed(id protocol.OpaqueID, controls protocol.ControlVersions, msg string) protocol.DeliveryResult {
	res := c.observed(id, protocol.DeliveryResultKindFailed, controls)
	res.Error = msg
	return res
}

// cancelled is the CANCELLED result of an order that came while
// automation was paused.
func (c *conn) cancelled(id protocol.OpaqueID, controls protocol.ControlVersions) protocol.DeliveryResult {
	res := c.observed(id, protocol.DeliveryResultKindCancelled, controls)
	res.CancelReason = protocol.CancelReasonStateChanged
	return res
}

// leased is res answering a commit: it carries the Core's writer lease.
func leased(res protocol.DeliveryResult, lease protocol.OpaqueID) protocol.DeliveryResult {
	res.WriterLeaseID = lease
	return res
}

// samePrepare reports whether two prepares are the same order. They
// compare as they encode: encoding/json sorts map keys, so every field
// counts, the control versions and any field added later included.
func samePrepare(a, b protocol.PrepareDelivery) bool {
	ja, errA := json.Marshal(a)
	jb, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(ja) == string(jb)
}

// wireGeneration is a draft generation on the wire, which counts in
// int64.
func wireGeneration(g uint64) int64 {
	if g > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(g)
}
