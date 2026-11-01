//go:build unix

package agentsock

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// maxReplies bounds the replies queued on one connection. A Core that
// keeps sending orders without reading their replies fills it, and the
// connection closes.
const maxReplies = 32

// The fixed messages of the frame errors; they never quote a frame.
const (
	msgInvalidFrame  = "invalid frame"
	msgUnsupportedOp = "op not supported by this proxy"
	msgNotRegistered = "agent not registered on this connection"
)

// greetingFrameID is the frame id of register_agent, the first frame.
const greetingFrameID = 1

// outbound is one queued reply. A nil body is the guard report, read
// when it is written so it is always the latest state. An
// emit_event_result is the Core's, with a durableResult body: the writer
// settles it in the order the reader took it, and answers only a refusal.
type outbound struct {
	op      protocol.AgentOp
	replyTo int64
	body    any
}

// durableResult is the Core's result for a durable event, with the frame
// it answers.
type durableResult struct {
	answers int64
	result  protocol.EmitEventResult
}

// conn is one accepted connection: a reader that dispatches the Core's
// frames and a writer that writes every frame of the session.
type conn struct {
	c     *net.UnixConn
	opts  *Options
	agent protocol.AgentID
	// last is the server's last observation written, on any connection.
	last *lastObservation

	replies chan outbound
	// registered closes once register_agent_result is accepted;
	// sessionID is set before.
	registered chan struct{}
	sessionID  protocol.OpaqueID
	// observing closes once the Core announces, on this connection, that
	// it takes state observations. Only the reader closes it, and only the
	// reader reads announced.
	observing chan struct{}
	announced bool
	// durable closes once the Core announces, on this connection, that it
	// takes durable events, and only with a queue to drain. Only the reader
	// closes it, and only the reader reads takesDurable.
	durable      chan struct{}
	takesDurable bool
	// duplicates is the server's count of the announcements ignored.
	duplicates *atomic.Int64

	// dl is the delivery state; jobs tracks the commits running.
	dl   deliveries
	jobs sync.WaitGroup

	done chan struct{}
	once sync.Once
}

func newConn(
	c *net.UnixConn,
	opts *Options,
	agent protocol.AgentID,
	last *lastObservation,
	duplicates *atomic.Int64,
) *conn {
	return &conn{
		c: c, opts: opts, agent: agent, last: last, duplicates: duplicates,
		replies:    make(chan outbound, maxReplies),
		registered: make(chan struct{}),
		observing:  make(chan struct{}),
		durable:    make(chan struct{}),
		dl: deliveries{
			live:   make(map[protocol.OpaqueID]*liveDelivery),
			closed: make(map[protocol.OpaqueID]closedDelivery),
		},
		done: make(chan struct{}),
	}
}

// run serves the connection until either side ends it. Then the
// deliveries it prepared are cancelled, and a commit already running
// goes on to its end.
func (c *conn) run() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.write()
	}()
	go func() {
		defer wg.Done()
		c.greetingDeadline()
	}()
	c.read()
	registered := c.sessionID != ""
	c.close()
	c.dropPrepared()
	if registered && c.opts.OnDisconnect != nil {
		c.opts.OnDisconnect()
	}
	c.jobs.Wait()
	wg.Wait()
}

// close ends the connection once; it unblocks the reader and the writer.
func (c *conn) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.c.Close()
	})
}

// greetingDeadline closes a connection whose Core does not answer the
// greeting within GreetingTimeout.
func (c *conn) greetingDeadline() {
	select {
	case <-c.registered:
	case <-c.done:
	case <-c.opts.Clock.After(GreetingTimeout):
		c.close()
	}
}

// write is the connection's only writer. It greets, then writes the
// replies, a retained event, a guard report on every new guard version, the
// state observations, the durable events and a heartbeat every
// HeartbeatEvery once registered. While the Core does not read, reports,
// observations and heartbeats coalesce: the next one written carries the
// latest state, and a version already reported is not reported again
// unasked. While a durable event waits for its result, only the next durable
// event waits: everything else goes on.
func (c *conn) write() {
	defer c.close()
	var nextID int64
	// writeFrame takes the next frame id only once the frame is written,
	// so an observation refused unwritten leaves no gap in the ids.
	writeFrame := func(op protocol.AgentOp, replyTo int64, body any) error {
		raw, err := json.Marshal(body)
		if err != nil {
			return ErrInvalidFrame
		}
		f := protocol.AgentFrame{Op: op, FrameID: nextID + 1, SentAt: c.opts.Clock.Now(), Body: raw}
		if replyTo > 0 {
			f.ReplyTo = &replyTo
		}
		if err := WriteFrame(c.c, f); err != nil {
			return err
		}
		nextID++
		return nil
	}
	send := func(op protocol.AgentOp, replyTo int64, body any) bool {
		return writeFrame(op, replyTo, body) == nil
	}
	if !send(protocol.AgentOpRegisterAgent, 0, c.opts.Register) {
		return
	}
	reported := int64(-1)
	report := func(replyTo int64, always bool) bool {
		r := c.report()
		if !always && r.InputGuardVersion == reported {
			return true
		}
		reported = r.InputGuardVersion
		return send(protocol.AgentOpReportInputGuard, replyTo, r)
	}
	var sentEvent protocol.ULID
	hasSentEvent := false
	emitEvent := func() bool {
		if c.opts.Events == nil {
			return true
		}
		event, ok := c.opts.Events.CurrentEvent()
		if !ok || hasSentEvent && event.EventID == sentEvent {
			return true
		}
		manifest := c.opts.Register.Manifest
		if event.AgentID != c.agent || event.SessionID != c.sessionID ||
			event.Harness != manifest.Harness || event.HarnessVersion != manifest.HarnessVersion ||
			event.AdapterType != manifest.AdapterType || event.Validate() != nil {
			return false
		}
		if !send(protocol.AgentOpEmitEvent, 0, event) {
			return false
		}
		sentEvent = event.EventID
		hasSentEvent = true
		return true
	}
	// refresh cuts a quiet session again; gap holds a change signalled
	// within ObservationGap of the last observation until it ends.
	var refresh, gap <-chan time.Time
	held := false
	observe := func(first bool) bool {
		if c.opts.Observations == nil {
			return true
		}
		refresh = c.opts.Clock.After(ObservationEvery)
		r, ok := c.nextObservation(first)
		if !ok {
			return true
		}
		err := writeFrame(protocol.AgentOpReportStateObservation, 0, r)
		if errors.Is(err, ErrInvalidFrame) || errors.Is(err, ErrFrameTooLarge) {
			// Nothing was written: an observation is never cut to fit.
			return true
		}
		if err != nil {
			return false
		}
		c.last.set(r)
		gap = c.opts.Clock.After(ObservationGap)
		return true
	}
	observeChange := func() bool {
		if gap != nil {
			held = true
			return true
		}
		return observe(false)
	}
	// inFlight is the durable event written on this connection and not yet
	// settled, and the frame that carried it; there is none while frame is 0.
	var inFlight struct {
		frame int64
		id    protocol.ULID
	}
	// emitDurable writes the queue's head unless an event is in flight. A
	// head that cannot go is refused, never cut to fit, and the next is tried.
	emitDurable := func() bool {
		// The reserved sequence-zero capability audit is an unacknowledged
		// retained event. Write it first whenever it exists so every later
		// durable sequence observes the manifest transition before its event.
		if !emitEvent() {
			return false
		}
		for inFlight.frame == 0 {
			event, ok := c.opts.Durable.Head()
			if !ok {
				return true
			}
			if !c.ofConnection(event) || validateDurableEvent(event) != nil {
				c.opts.Durable.Refuse(event.EventID)
				continue
			}
			err := writeFrame(protocol.AgentOpEmitEvent, 0, event)
			if errors.Is(err, ErrInvalidFrame) || errors.Is(err, ErrFrameTooLarge) {
				// Nothing was written.
				c.opts.Durable.Refuse(event.EventID)
				continue
			}
			if err != nil {
				return false
			}
			inFlight.frame, inFlight.id = nextID, event.EventID
			c.opts.Durable.Sent(event.EventID)
		}
		return true
	}
	// settle takes the Core's result for the event in flight: it must answer
	// that event's frame and name that event. Any other is refused and
	// settles nothing; the event in flight goes on waiting. No result answers
	// frame 0, so none settles while nothing is in flight.
	settle := func(o outbound) bool {
		r, _ := o.body.(durableResult)
		if r.answers != inFlight.frame || r.result.EventID != inFlight.id {
			return send(protocol.AgentOpError, o.replyTo,
				protocol.FrameError{Code: protocol.FrameErrorCodeInvalidFrame, Message: msgInvalidFrame})
		}
		inFlight.frame = 0
		c.opts.Durable.Settle(r.result)
		return emitDurable()
	}
	registered := c.registered
	var heartbeat <-chan time.Time
	var changed <-chan struct{}
	var eventChanged <-chan struct{}
	var observing, observationChanged <-chan struct{}
	var durable, headChanged <-chan struct{}
	for {
		var ok bool
		select {
		case <-c.done:
			return
		case <-registered:
			registered = nil
			heartbeat = c.opts.Clock.After(HeartbeatEvery)
			changed = c.opts.Guard.GuardChanged()
			if c.opts.Events != nil {
				eventChanged = c.opts.Events.EventChanged()
			}
			if c.opts.Observations != nil {
				observing = c.observing
			}
			durable = c.durable
			ok = emitEvent() && report(0, true)
		case <-observing:
			// Nothing is cut before the Core says it takes observations.
			observing = nil
			observationChanged = c.opts.Observations.ObservationChanged()
			ok = observe(true)
		case <-durable:
			// Nothing durable goes before the Core says it takes it.
			durable = nil
			headChanged = c.opts.Durable.HeadChanged()
			ok = emitDurable()
		case <-headChanged:
			ok = emitDurable()
		case <-changed:
			ok = report(0, false)
		case <-eventChanged:
			ok = emitEvent()
		case <-observationChanged:
			ok = observeChange()
		case <-refresh:
			ok = observeChange()
		case <-gap:
			gap = nil
			ok = true
			if held {
				held = false
				ok = observe(false)
			}
		case <-heartbeat:
			heartbeat = c.opts.Clock.After(HeartbeatEvery)
			ok = send(protocol.AgentOpHeartbeat, 0, protocol.AgentTarget{AgentID: c.agent})
		case o := <-c.replies:
			switch {
			case o.op == protocol.AgentOpEmitEventResult:
				ok = settle(o)
			case o.body == nil:
				ok = report(o.replyTo, true)
			default:
				ok = send(o.op, o.replyTo, o.body)
			}
		}
		if !ok {
			return
		}
	}
}

func validateDurableEvent(event protocol.EventEnvelope) error {
	if event.EventType == protocol.EventTypeAdapterCapabilitiesChanged {
		if event.Sequence < 1 || event.SourceQuality != protocol.SourceQualityProcessMetrics ||
			event.Stream != "" || event.Origin != protocol.OriginSystem || event.Validate() != nil {
			return fmt.Errorf("validate durable capability event")
		}
		return nil
	}
	if event.Stream == protocol.StreamNativeEvent {
		if err := protocol.ValidateNativeDurableEvent(event); err != nil {
			return fmt.Errorf("validate native durable event: %w", err)
		}
		return nil
	}
	if err := protocol.ValidateDurableEventEnvelope(event); err != nil {
		return fmt.Errorf("validate durable event: %w", err)
	}
	return nil
}

// report is the guard's current state as a report of this connection.
// It never carries text: the guard reading has no field for it.
func (c *conn) report() protocol.InputGuardReport {
	r := c.opts.Guard.InputGuard()
	r.AgentID = c.agent
	r.SessionID = c.sessionID
	r.DetectionSource = protocol.InputGuardProviderPTYProxy
	r.Confidence = 1
	return r
}

// read dispatches the Core's frames until the stream ends. Nothing a
// frame asks is executed before the greeting.
func (c *conn) read() {
	isRegistered := false
	for {
		f, err := ReadFrame(c.c)
		if errors.Is(err, ErrInvalidFrame) {
			if !c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame) {
				return
			}
			continue
		}
		if err != nil {
			// A frame too large leaves the stream unreadable;
			// the end of the stream or a read error ends it.
			return
		}
		switch {
		case f.Op == protocol.AgentOpError:
			// Errors travel both ways: one from the Core is
			// read and dropped, never answered, so no two
			// sides trade errors forever.
		case !fromCore(f.Op):
			if !c.refuse(f.FrameID, protocol.FrameErrorCodeUnsupportedOp, msgUnsupportedOp) {
				return
			}
		case f.Op == protocol.AgentOpRegisterAgentResult:
			if isRegistered || *f.ReplyTo != greetingFrameID {
				if !c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame) {
					return
				}
				continue
			}
			var res protocol.RegisterAgentResult
			if json.Unmarshal(f.Body, &res) != nil || res.AgentID != c.agent ||
				(c.opts.SessionID != "" && res.SessionID != c.opts.SessionID) {
				return
			}
			c.sessionID = res.SessionID
			isRegistered = true
			close(c.registered)
		case !isRegistered:
			if !c.refuse(f.FrameID, protocol.FrameErrorCodeNotRegistered, msgNotRegistered) {
				return
			}
		case f.Op == protocol.AgentOpAnnounceSupport:
			if !c.announce(f) {
				return
			}
		case f.Op == protocol.AgentOpResync:
			if !c.resync(f) {
				return
			}
		case f.Op == protocol.AgentOpEmitEventResult && c.takesDurable:
			if !c.eventResult(f) {
				return
			}
		case c.opts.Delivery != nil:
			if !c.deliver(f) {
				return
			}
		default:
			// A socket without a delivery runs no order but
			// resync.
			if !c.refuse(f.FrameID, protocol.FrameErrorCodeUnsupportedOp, msgUnsupportedOp) {
				return
			}
		}
	}
}

// resync answers with the guard state as it is: it reads the guard and
// moves nothing, so an UNKNOWN guard stays UNKNOWN.
func (c *conn) resync(f protocol.AgentFrame) bool {
	var target protocol.AgentTarget
	if json.Unmarshal(f.Body, &target) != nil || target.AgentID != c.agent {
		return c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame)
	}
	return c.queue(outbound{op: protocol.AgentOpReportInputGuard, replyTo: f.FrameID})
}

// eventResult hands the Core's result for a durable event to the writer,
// which alone knows the event in flight. The frame validated, so it answers
// one.
func (c *conn) eventResult(f protocol.AgentFrame) bool {
	var res protocol.EmitEventResult
	if json.Unmarshal(f.Body, &res) != nil {
		return c.refuse(f.FrameID, protocol.FrameErrorCodeInvalidFrame, msgInvalidFrame)
	}
	return c.queue(outbound{
		op: protocol.AgentOpEmitEventResult, replyTo: f.FrameID,
		body: durableResult{answers: *f.ReplyTo, result: res},
	})
}

// ofConnection reports whether a durable event names the connection's
// agent and session and the registered harness and adapter.
func (c *conn) ofConnection(event protocol.EventEnvelope) bool {
	manifest := c.opts.Register.Manifest
	return event.AgentID == c.agent && event.SessionID == c.sessionID &&
		event.Harness == manifest.Harness && event.HarnessVersion == manifest.HarnessVersion &&
		event.AdapterType == manifest.AdapterType
}

// refuse queues a frame error answering frameID, or none when the frame
// had no readable id.
func (c *conn) refuse(frameID int64, code protocol.FrameErrorCode, msg string) bool {
	return c.queue(outbound{
		op: protocol.AgentOpError, replyTo: frameID,
		body: protocol.FrameError{Code: code, Message: msg},
	})
}

// queue hands a reply to the writer without waiting. A full queue
// means the Core sends orders without reading: the connection closes.
func (c *conn) queue(o outbound) bool {
	select {
	case c.replies <- o:
		return true
	default:
		return false
	}
}

// fromCore reports whether the Core sends op.
func fromCore(op protocol.AgentOp) bool {
	for _, spec := range protocol.AgentOpSpecs() {
		if spec.Op == op {
			return spec.FromCore
		}
	}
	return false
}
