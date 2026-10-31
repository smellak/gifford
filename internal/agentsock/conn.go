//go:build unix

package agentsock

import (
	"encoding/json"
	"errors"
	"net"
	"sync"
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
// when it is written so it is always the latest state.
type outbound struct {
	op      protocol.AgentOp
	replyTo int64
	body    any
}

// conn is one accepted connection: a reader that dispatches the Core's
// frames and a writer that writes every frame of the session.
type conn struct {
	c     *net.UnixConn
	opts  *Options
	agent protocol.AgentID

	replies chan outbound
	// registered closes once register_agent_result is accepted;
	// sessionID is set before.
	registered chan struct{}
	sessionID  protocol.OpaqueID

	// dl is the delivery state; jobs tracks the commits running.
	dl   deliveries
	jobs sync.WaitGroup

	done chan struct{}
	once sync.Once
}

func newConn(c *net.UnixConn, opts *Options, agent protocol.AgentID) *conn {
	return &conn{
		c: c, opts: opts, agent: agent,
		replies:    make(chan outbound, maxReplies),
		registered: make(chan struct{}),
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
// replies, a retained event, a guard report on every new guard version and a
// heartbeat every HeartbeatEvery once registered. While the Core does not
// read, reports and heartbeats coalesce: the next one written carries the
// latest state, and a version already reported is not reported again unasked.
func (c *conn) write() {
	defer c.close()
	var nextID int64
	send := func(op protocol.AgentOp, replyTo int64, body any) bool {
		raw, err := json.Marshal(body)
		if err != nil {
			return false
		}
		nextID++
		f := protocol.AgentFrame{Op: op, FrameID: nextID, SentAt: c.opts.Clock.Now(), Body: raw}
		if replyTo > 0 {
			f.ReplyTo = &replyTo
		}
		return WriteFrame(c.c, f) == nil
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
	registered := c.registered
	var heartbeat <-chan time.Time
	var changed <-chan struct{}
	var eventChanged <-chan struct{}
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
			ok = emitEvent() && report(0, true)
		case <-changed:
			ok = report(0, false)
		case <-eventChanged:
			ok = emitEvent()
		case <-heartbeat:
			heartbeat = c.opts.Clock.After(HeartbeatEvery)
			ok = send(protocol.AgentOpHeartbeat, 0, protocol.AgentTarget{AgentID: c.agent})
		case o := <-c.replies:
			if o.body == nil {
				ok = report(o.replyTo, true)
			} else {
				ok = send(o.op, o.replyTo, o.body)
			}
		}
		if !ok {
			return
		}
	}
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
		case f.Op == protocol.AgentOpResync:
			if !c.resync(f) {
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
