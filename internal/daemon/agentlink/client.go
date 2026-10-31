package agentlink

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultHandshakeTimeout  = 4 * time.Second
	defaultHeartbeatTimeout  = 7 * time.Second
	defaultMinReconnectDelay = 100 * time.Millisecond
	defaultReconnectMax      = 5 * time.Second
	// Delivery results use a bounded channel. When it fills, the socket reader
	// applies backpressure outside every client lock until the lifecycle-owned
	// consumer makes room or the link shuts down.
	maxDeliveryResultBacklog = 256
	agentsDirectory          = "agents"
)

var errGuardWriteFailed = errors.New("agentlink: guard write failed")

// Options configures one reconnecting link to an agent's Proxy socket.
type Options struct {
	RuntimeDir        string
	DB                *sql.DB
	NodeID            protocol.NodeID
	AgentID           protocol.AgentID
	SessionID         protocol.OpaqueID
	Writer            GuardWriter
	CapabilityWriter  CapabilityWriter
	CapabilityLineage CapabilityLineage
	Support           protocol.AnnounceSupport
	Clock             Clock
	HandshakeTimeout  time.Duration
	HeartbeatTimeout  time.Duration
	ReconnectMin      time.Duration
	ReconnectMax      time.Duration
}

type pendingReply struct {
	generation uint64
	answer     chan callReply
}

type callReply struct {
	frame protocol.AgentFrame
	err   error
}

type queuedReport struct {
	generation uint64
	report     protocol.InputGuardReport
}

type taggedError struct {
	generation uint64
	err        error
}

type peerCredentials struct {
	pid int64
	uid int64
	gid int64
}

// Client is one agent's authenticated, reconnecting Proxy link.
type Client struct {
	opts Options
	path string

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	wg     sync.WaitGroup

	// sendGate serializes frames while allowing callers to abandon the wait
	// when their context ends. A sync.Mutex cannot provide that guarantee.
	sendGate chan struct{}
	mu       sync.RWMutex
	snap     Snapshot
	conn     *net.UnixConn
	// generation distinguishes equal frame ids on successive connections.
	generation uint64
	nextID     int64
	pending    map[int64]pendingReply
	// supportFrameID names the one announcement sent on supportGeneration.
	// An older Proxy rejects that additive op with a correlated INVALID_FRAME;
	// the link consumes that compatibility response without reconnecting.
	supportGeneration uint64
	supportFrameID    int64

	automationSet bool
	paused        bool
	reportSeen    bool

	changes chan Snapshot
	results chan DeliveryResult

	reportMu    sync.Mutex
	reportQueue []queuedReport
	reportWake  chan struct{}
	reportErr   chan taggedError
}

// Start validates opts and starts reconnecting in the background. Socket and
// peer failures are reflected in Snapshot rather than making construction
// fail; the registry can therefore keep one client alive while a Proxy starts.
func Start(ctx context.Context, opts Options) (*Client, error) {
	if ctx == nil || opts.DB == nil || opts.Writer == nil || !filepath.IsAbs(opts.RuntimeDir) ||
		!opts.AgentID.Valid() || !opts.SessionID.Valid() {
		return nil, ErrInvalidOptions
	}
	if (opts.CapabilityWriter == nil) != (opts.CapabilityLineage == nil) ||
		opts.CapabilityWriter != nil && !opts.NodeID.Valid() {
		return nil, ErrInvalidOptions
	}
	if opts.Clock == nil {
		opts.Clock = SystemClock{}
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = defaultHandshakeTimeout
	}
	if opts.HeartbeatTimeout <= 0 {
		opts.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if opts.ReconnectMin <= 0 {
		opts.ReconnectMin = defaultMinReconnectDelay
	}
	if opts.ReconnectMax <= 0 {
		opts.ReconnectMax = defaultReconnectMax
	}
	if opts.ReconnectMax < opts.ReconnectMin {
		return nil, fmt.Errorf("%w: reconnect bounds", ErrInvalidOptions)
	}
	linkCtx, cancel := context.WithCancel(ctx)
	c := &Client{
		opts: opts,
		path: filepath.Join(filepath.Clean(opts.RuntimeDir), agentsDirectory,
			string(opts.AgentID)+".sock"),
		ctx: linkCtx, cancel: cancel, done: make(chan struct{}),
		snap:     Snapshot{AgentID: opts.AgentID, SessionID: opts.SessionID, State: StateDown},
		pending:  make(map[int64]pendingReply),
		sendGate: make(chan struct{}, 1),
		changes:  make(chan Snapshot, 1), results: make(chan DeliveryResult, maxDeliveryResultBacklog),
		reportWake: make(chan struct{}, 1), reportErr: make(chan taggedError, 1),
	}
	c.wg.Add(2)
	go c.run()
	go c.writeReports()
	go func() {
		c.wg.Wait()
		close(c.done)
	}()
	return c, nil
}

// Path is the agent socket path this client authenticates.
func (c *Client) Path() string { return c.path }

// Snapshot returns a race-safe copy of the link state and latest report.
func (c *Client) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snap
}

// Changes is a coalescing state/report stream. A slow observer never blocks
// the socket reader; Snapshot always holds the newest value.
func (c *Client) Changes() <-chan Snapshot { return c.changes }

// DeliveryResults returns every delivery result, including a result arriving
// after its Call context expired. A slow consumer applies bounded backpressure
// to the socket reader instead of dropping evidence.
func (c *Client) DeliveryResults() <-chan DeliveryResult { return c.results }

// Done closes after the link and its internal publishers have stopped.
func (c *Client) Done() <-chan struct{} { return c.done }

// Close stops the reconnect loop and the current connection. It is idempotent.
func (c *Client) Close() error {
	c.once.Do(func() {
		c.cancel()
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()
		if conn != nil {
			// net.Conn permits Close concurrently with Write. Do not wait for
			// sendGate here: closing is what unblocks a peer that stopped reading.
			_ = conn.Close()
		}
	})
	<-c.done
	return nil
}

func (c *Client) acquireSend(ctx context.Context) error {
	select {
	case c.sendGate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			c.releaseSend()
			return fmt.Errorf("agentlink: acquire writer: %w", err)
		}
		if c.ctx.Err() != nil {
			c.releaseSend()
			return ErrLinkDown
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("agentlink: acquire writer: %w", ctx.Err())
	case <-c.ctx.Done():
		return ErrLinkDown
	}
}

func (c *Client) releaseSend() { <-c.sendGate }

// Send writes one already-constructed Core operation and returns its frame id.
// It does not create delivery bodies or wait for a reply.
func (c *Client) Send(ctx context.Context, op protocol.AgentOp, body any) (int64, error) {
	if ctx == nil {
		return 0, ErrInvalidOptions
	}
	if err := c.acquireSend(ctx); err != nil {
		return 0, err
	}
	defer c.releaseSend()
	return c.sendLocked(ctx, op, body, nil)
}

// Call sends one already-constructed operation and waits for the frame whose
// reply_to names it. A late delivery result remains available through
// DeliveryResults even when ctx ends first.
func (c *Client) Call(
	ctx context.Context,
	op protocol.AgentOp,
	body any,
) (protocol.AgentFrame, error) {
	if ctx == nil {
		return protocol.AgentFrame{}, ErrInvalidOptions
	}
	wait := make(chan callReply, 1)
	if err := c.acquireSend(ctx); err != nil {
		return protocol.AgentFrame{}, err
	}
	id, err := c.sendLocked(ctx, op, body, wait)
	c.releaseSend()
	if err != nil {
		return protocol.AgentFrame{}, err
	}
	select {
	case <-ctx.Done():
		c.removePending(id, wait)
		return protocol.AgentFrame{}, fmt.Errorf("agentlink: wait for frame %d: %w", id, ctx.Err())
	case <-c.ctx.Done():
		c.removePending(id, wait)
		return protocol.AgentFrame{}, ErrLinkDown
	case answer := <-wait:
		if answer.err != nil {
			return protocol.AgentFrame{}, answer.err
		}
		if answer.frame.Op == protocol.AgentOpError {
			var remote protocol.FrameError
			if err := json.Unmarshal(answer.frame.Body, &remote); err != nil {
				return protocol.AgentFrame{}, fmt.Errorf("%w: malformed error", ErrUnexpectedFrame)
			}
			return protocol.AgentFrame{}, &RemoteError{Code: remote.Code, Message: remote.Message}
		}
		return answer.frame, nil
	}
}

// Resync asks the Proxy for its current guard and waits for the report that
// explicitly replies to this request.
func (c *Client) Resync(ctx context.Context) (protocol.InputGuardReport, error) {
	frame, err := c.Call(ctx, protocol.AgentOpResync, protocol.AgentTarget{AgentID: c.opts.AgentID})
	if err != nil {
		return protocol.InputGuardReport{}, err
	}
	if frame.Op != protocol.AgentOpReportInputGuard || frame.ReplyTo == nil {
		return protocol.InputGuardReport{}, ErrUnexpectedFrame
	}
	var report protocol.InputGuardReport
	if err := json.Unmarshal(frame.Body, &report); err != nil {
		return protocol.InputGuardReport{}, fmt.Errorf("%w: resync report", ErrUnexpectedFrame)
	}
	return report, nil
}

// Pause records the desired paused state and sends it on a live connection.
// When the link is down, the state is retained and sent after the next greeting.
func (c *Client) Pause(ctx context.Context) error { return c.setAutomation(ctx, true) }

// Resume records the desired resumed state and sends it on a live connection.
// When the link is down, the state is retained and sent after the next greeting.
func (c *Client) Resume(ctx context.Context) error { return c.setAutomation(ctx, false) }

func (c *Client) run() {
	defer c.wg.Done()
	defer close(c.changes)
	defer close(c.results)
	delay := c.opts.ReconnectMin
	for {
		if c.ctx.Err() != nil {
			c.setState(StateStopped, ReasonStopped)
			return
		}
		c.setState(StateConnecting, ReasonNone)
		connected, err := c.serveOnce()
		if c.ctx.Err() != nil {
			c.setState(StateStopped, ReasonStopped)
			return
		}
		c.setState(StateDown, reasonFor(err))
		if errors.Is(err, ErrCapabilitiesConflict) {
			<-c.ctx.Done()
			c.setState(StateStopped, ReasonStopped)
			return
		}
		if connected {
			delay = c.opts.ReconnectMin
		}
		select {
		case <-c.ctx.Done():
			c.setState(StateStopped, ReasonStopped)
			return
		case <-c.opts.Clock.After(delay):
		}
		if !connected && delay < c.opts.ReconnectMax {
			delay *= 2
			if delay > c.opts.ReconnectMax {
				delay = c.opts.ReconnectMax
			}
		}
	}
}

func (c *Client) serveOnce() (connected bool, retErr error) {
	conn, peer, err := dialVerifiedPath(c.ctx, c.path)
	if err != nil {
		return false, err
	}
	defer func() { _ = conn.Close() }()
	session, err := liveSession(c.ctx, c.opts.DB, c.opts.SessionID, c.opts.AgentID)
	if err != nil {
		return false, err
	}
	if err := verifyPeerProcess(peer, session.process); err != nil {
		return false, err
	}
	greeting, err := c.readGreeting(conn)
	if err != nil {
		return false, err
	}
	authenticatedManifest, err := c.validateGreeting(greeting, session)
	if err != nil {
		return false, err
	}
	response := protocol.RegisterAgentResult{AgentID: c.opts.AgentID, SessionID: c.opts.SessionID}
	body, err := json.Marshal(response)
	if err != nil {
		return false, fmt.Errorf("encode greeting response: %w", err)
	}
	replyTo := greeting.FrameID
	if err := agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgentResult, FrameID: 1, ReplyTo: &replyTo,
		SentAt: c.opts.Clock.Now(), Body: body,
	}); err != nil {
		return false, fmt.Errorf("%w: answer greeting: %w", ErrLinkDown, err)
	}
	generation, err := c.activate(conn)
	if err != nil {
		return false, err
	}
	connected = true
	incoming := make(chan callReply, 64)
	readerDone := make(chan struct{})
	serveDone := make(chan struct{})
	go readFrames(conn, incoming, serveDone, readerDone)
	defer func() {
		close(serveDone)
		_ = conn.Close()
		<-readerDone
		c.deactivate(generation, retErr)
	}()
	heartbeat := c.opts.Clock.After(c.opts.HeartbeatTimeout)
	for {
		select {
		case <-c.ctx.Done():
			return true, fmt.Errorf("serve agent link: %w", c.ctx.Err())
		case failure := <-c.reportErr:
			if failure.generation == generation {
				return true, failure.err
			}
		case received := <-incoming:
			if received.err != nil {
				if errors.Is(received.err, io.EOF) || errors.Is(received.err, net.ErrClosed) {
					return true, ErrLinkDown
				}
				return true, fmt.Errorf("%w: read frame: %w", ErrLinkDown, received.err)
			}
			isHeartbeat, dispatchErr := c.dispatch(generation, authenticatedManifest, received.frame)
			if dispatchErr != nil {
				return true, dispatchErr
			}
			if isHeartbeat {
				heartbeat = c.opts.Clock.After(c.opts.HeartbeatTimeout)
			}
		case <-heartbeat:
			return true, ErrHeartbeatTimeout
		}
	}
}

func (c *Client) readGreeting(conn *net.UnixConn) (protocol.AgentFrame, error) {
	read := make(chan callReply, 1)
	go func() {
		frame, err := agentsock.ReadFrame(conn)
		read <- callReply{frame: frame, err: err}
	}()
	select {
	case <-c.ctx.Done():
		_ = conn.Close()
		<-read
		return protocol.AgentFrame{}, fmt.Errorf("read greeting: %w", c.ctx.Err())
	case <-c.opts.Clock.After(c.opts.HandshakeTimeout):
		_ = conn.Close()
		<-read
		return protocol.AgentFrame{}, fmt.Errorf("%w: timeout", ErrInvalidGreeting)
	case result := <-read:
		if result.err != nil {
			return protocol.AgentFrame{}, fmt.Errorf("%w: read: %w", ErrInvalidGreeting, result.err)
		}
		return result.frame, nil
	}
}

func (c *Client) validateGreeting(
	frame protocol.AgentFrame,
	session sessionRecord,
) (protocol.CapabilityManifest, error) {
	if frame.Op != protocol.AgentOpRegisterAgent || frame.FrameID != 1 || frame.ReplyTo != nil {
		return protocol.CapabilityManifest{}, ErrInvalidGreeting
	}
	var registration protocol.RegisterAgent
	if err := json.Unmarshal(frame.Body, &registration); err != nil {
		return protocol.CapabilityManifest{}, fmt.Errorf("%w: body", ErrInvalidGreeting)
	}
	if err := registration.Validate(); err != nil {
		return protocol.CapabilityManifest{}, fmt.Errorf("%w: %w", ErrInvalidGreeting, err)
	}
	if registration.Process == nil || *registration.Process != session.process {
		return protocol.CapabilityManifest{}, ErrInvalidGreeting
	}
	if registration.Manifest == session.manifest {
		return registration.Manifest, nil
	}
	if c.opts.CapabilityLineage == nil {
		return protocol.CapabilityManifest{}, ErrInvalidGreeting
	}
	matches, err := c.opts.CapabilityLineage.MatchesCapabilitiesChanged(
		c.ctx, c.opts.DB, c.opts.SessionID, registration.Manifest, session.manifest,
	)
	if err != nil {
		return protocol.CapabilityManifest{}, fmt.Errorf("verify capability lineage: %w", err)
	}
	if !matches {
		return protocol.CapabilityManifest{}, ErrInvalidGreeting
	}
	return registration.Manifest, nil
}

func (c *Client) activate(conn *net.UnixConn) (uint64, error) {
	if err := c.acquireSend(c.ctx); err != nil {
		return 0, err
	}
	defer c.releaseSend()
	c.mu.Lock()
	c.generation++
	generation := c.generation
	c.conn = conn
	c.nextID = 2
	c.supportGeneration = 0
	c.supportFrameID = 0
	c.snap.State = StateConnecting
	c.snap.HasReport = false
	c.snap.LastHeartbeatAt = time.Time{}
	automationSet, paused := c.automationSet, c.paused
	c.mu.Unlock()
	supportFrameID, err := c.sendLocked(c.ctx, protocol.AgentOpAnnounceSupport, c.opts.Support, nil)
	if err != nil {
		c.clearConnectionLocked(generation, err)
		return 0, err
	}
	c.mu.Lock()
	if c.generation == generation {
		c.supportGeneration = generation
		c.supportFrameID = supportFrameID
	}
	c.mu.Unlock()
	if automationSet {
		op := protocol.AgentOpResumeAutomation
		if paused {
			op = protocol.AgentOpPauseAutomation
		}
		if _, err := c.sendLocked(c.ctx, op, protocol.AgentTarget{AgentID: c.opts.AgentID}, nil); err != nil {
			c.clearConnectionLocked(generation, err)
			return 0, err
		}
	}
	c.mu.Lock()
	c.snap.State = StateUp
	c.snap.Reason = ReasonNone
	c.snap.ConnectedAt = c.opts.Clock.Now()
	snapshot := c.snap
	c.mu.Unlock()
	c.publishSnapshot(snapshot)
	return generation, nil
}

func (c *Client) deactivate(generation uint64, err error) {
	// serveOnce closes conn before deactivation, so any blocked writer releases
	// the gate. Cleanup must still run after client context cancellation.
	c.sendGate <- struct{}{}
	defer c.releaseSend()
	c.clearConnectionLocked(generation, err)
}

func (c *Client) clearConnectionLocked(generation uint64, err error) {
	c.mu.Lock()
	if c.generation != generation {
		c.mu.Unlock()
		return
	}
	conn := c.conn
	c.conn = nil
	c.supportGeneration = 0
	c.supportFrameID = 0
	pendingErr := ErrLinkDown
	if err != nil {
		pendingErr = fmt.Errorf("%w: %w", ErrLinkDown, err)
	}
	for id, pending := range c.pending {
		if pending.generation != generation {
			continue
		}
		select {
		case pending.answer <- callReply{err: pendingErr}:
		default:
		}
		delete(c.pending, id)
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (c *Client) sendLocked(
	ctx context.Context,
	op protocol.AgentOp,
	body any,
	wait chan callReply,
) (int64, error) {
	if ctx.Err() != nil {
		return 0, fmt.Errorf("send agent frame: %w", ctx.Err())
	}
	if !fromCore(op) || op == protocol.AgentOpRegisterAgentResult {
		return 0, fmt.Errorf("%w: op %s", ErrInvalidOptions, op)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, fmt.Errorf("encode agent frame: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("send agent frame: %w", err)
	}
	c.mu.Lock()
	if c.snap.State != StateUp && c.snap.State != StateConnecting || c.conn == nil {
		c.mu.Unlock()
		return 0, ErrLinkDown
	}
	conn, generation := c.conn, c.generation
	id := c.nextID
	c.nextID++
	if wait != nil {
		c.pending[id] = pendingReply{generation: generation, answer: wait}
	}
	c.mu.Unlock()
	frame := protocol.AgentFrame{Op: op, FrameID: id, SentAt: c.opts.Clock.Now(), Body: raw}
	if err := c.writeFrame(ctx, conn, frame); err != nil {
		if wait != nil {
			c.removePending(id, wait)
		}
		_ = conn.Close()
		return 0, err
	}
	return id, nil
}

func (c *Client) writeFrame(
	ctx context.Context,
	conn *net.UnixConn,
	frame protocol.AgentFrame,
) error {
	stopRequest := context.AfterFunc(ctx, func() { _ = conn.Close() })
	stopLink := context.AfterFunc(c.ctx, func() { _ = conn.Close() })
	err := agentsock.WriteFrame(conn, frame)
	requestStopped := stopRequest()
	linkStopped := stopLink()
	if !requestStopped || !linkStopped {
		// A cancellation raced the write. Even a nil write result is ambiguous,
		// so this connection cannot carry another delivery operation.
		_ = conn.Close()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		_ = conn.Close()
		return fmt.Errorf("%w: write frame: %w", ErrLinkDown, ctxErr)
	}
	if c.ctx.Err() != nil {
		_ = conn.Close()
		return ErrLinkDown
	}
	if err != nil {
		return fmt.Errorf("%w: write frame: %w", ErrLinkDown, err)
	}
	return nil
}

func (c *Client) removePending(id int64, wait chan callReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending, ok := c.pending[id]
	if ok && pending.answer == wait {
		delete(c.pending, id)
	}
}

func (c *Client) setAutomation(ctx context.Context, paused bool) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrInvalidOptions
	}
	if err := c.acquireSend(ctx); err != nil {
		return err
	}
	defer c.releaseSend()
	c.mu.Lock()
	c.automationSet = true
	c.paused = paused
	up := c.snap.State == StateUp && c.conn != nil
	c.mu.Unlock()
	if !up {
		return nil
	}
	op := protocol.AgentOpResumeAutomation
	if paused {
		op = protocol.AgentOpPauseAutomation
	}
	_, err := c.sendLocked(ctx, op, protocol.AgentTarget{AgentID: c.opts.AgentID}, nil)
	return err
}

func (c *Client) dispatch(
	generation uint64,
	authenticatedManifest protocol.CapabilityManifest,
	frame protocol.AgentFrame,
) (bool, error) {
	if !fromProxy(frame.Op) || frame.Op == protocol.AgentOpRegisterAgent {
		return false, ErrUnexpectedFrame
	}
	if c.acceptLegacySupportRejection(generation, frame) {
		return false, nil
	}
	switch frame.Op {
	case protocol.AgentOpEmitEvent:
		if err := c.applyCapabilitiesChanged(authenticatedManifest, frame); err != nil {
			return false, err
		}
	case protocol.AgentOpHeartbeat:
		var target protocol.AgentTarget
		if json.Unmarshal(frame.Body, &target) != nil || target.AgentID != c.opts.AgentID {
			return false, ErrUnexpectedFrame
		}
		c.mu.Lock()
		c.snap.LastHeartbeatAt = c.opts.Clock.Now()
		snapshot := c.snap
		c.mu.Unlock()
		c.publishSnapshot(snapshot)
		return true, nil
	case protocol.AgentOpReportInputGuard:
		var report protocol.InputGuardReport
		if json.Unmarshal(frame.Body, &report) != nil || report.AgentID != c.opts.AgentID ||
			report.SessionID != c.opts.SessionID {
			return false, ErrUnexpectedFrame
		}
		c.enqueueReport(queuedReport{generation: generation, report: report})
		c.mu.Lock()
		if generation == c.generation {
			switch {
			case !c.reportSeen || report.InputGuardVersion > c.snap.LastReport.InputGuardVersion:
				c.snap.LastReport = report
				c.snap.HasReport = true
				c.reportSeen = true
			case report.InputGuardVersion == c.snap.LastReport.InputGuardVersion:
				// The high-water report is now confirmed on this connection.
				// Equal versions never replace its fields.
				c.snap.HasReport = true
			}
		}
		snapshot := c.snap
		c.mu.Unlock()
		c.publishSnapshot(snapshot)
	case protocol.AgentOpReportDeliveryResult:
		if frame.ReplyTo == nil {
			return false, ErrUnexpectedFrame
		}
		var result protocol.DeliveryResult
		if json.Unmarshal(frame.Body, &result) != nil || result.AgentID != c.opts.AgentID {
			return false, ErrUnexpectedFrame
		}
		if err := c.enqueueResult(DeliveryResult{
			FrameID: frame.FrameID, ReplyTo: *frame.ReplyTo, Result: result,
		}); err != nil {
			return false, err
		}
	default:
		// Other Proxy-originated reports are kept transport-compatible for
		// later daemon stages; a correlated caller still receives them below.
	}
	c.deliverReply(generation, frame)
	return false, nil
}

func (c *Client) acceptLegacySupportRejection(
	generation uint64,
	frame protocol.AgentFrame,
) bool {
	if frame.Op != protocol.AgentOpError || frame.ReplyTo == nil {
		return false
	}
	var remote protocol.FrameError
	if json.Unmarshal(frame.Body, &remote) != nil ||
		remote.Code != protocol.FrameErrorCodeInvalidFrame {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.supportGeneration != generation || c.supportFrameID != *frame.ReplyTo {
		return false
	}
	c.supportGeneration = 0
	c.supportFrameID = 0
	return true
}

func (c *Client) deliverReply(generation uint64, frame protocol.AgentFrame) {
	if frame.ReplyTo == nil {
		return
	}
	c.mu.Lock()
	pending, ok := c.pending[*frame.ReplyTo]
	if ok && pending.generation == generation {
		delete(c.pending, *frame.ReplyTo)
	} else {
		ok = false
	}
	c.mu.Unlock()
	if ok {
		pending.answer <- callReply{frame: frame}
	}
}

func (c *Client) enqueueReport(report queuedReport) {
	c.reportMu.Lock()
	c.reportQueue = append(c.reportQueue, report)
	c.reportMu.Unlock()
	select {
	case c.reportWake <- struct{}{}:
	default:
	}
}

func (c *Client) writeReports() {
	defer c.wg.Done()
	for {
		report, ok := c.nextReport()
		if !ok {
			return
		}
		if _, err := c.opts.Writer.WriteInputGuard(c.ctx, report.report); err != nil {
			failure := taggedError{
				generation: report.generation,
				err:        fmt.Errorf("%w: %w", errGuardWriteFailed, err),
			}
			select {
			case c.reportErr <- failure:
			case <-c.ctx.Done():
				return
			}
		}
	}
}

func (c *Client) nextReport() (queuedReport, bool) {
	for {
		c.reportMu.Lock()
		if len(c.reportQueue) > 0 {
			report := c.reportQueue[0]
			c.reportQueue[0] = queuedReport{}
			c.reportQueue = c.reportQueue[1:]
			c.reportMu.Unlock()
			return report, true
		}
		c.reportMu.Unlock()
		select {
		case <-c.ctx.Done():
			return queuedReport{}, false
		case <-c.reportWake:
		}
	}
}

func (c *Client) enqueueResult(result DeliveryResult) error {
	select {
	case c.results <- result:
		return nil
	case <-c.ctx.Done():
		return ErrLinkDown
	}
}

func (c *Client) setState(state State, reason Reason) {
	c.mu.Lock()
	c.snap.State = state
	c.snap.Reason = reason
	if state != StateUp {
		c.snap.ConnectedAt = time.Time{}
		c.snap.HasReport = false
	}
	snapshot := c.snap
	c.mu.Unlock()
	c.publishSnapshot(snapshot)
}

func (c *Client) publishSnapshot(snapshot Snapshot) {
	select {
	case c.changes <- snapshot:
		return
	default:
	}
	select {
	case <-c.changes:
	default:
	}
	select {
	case c.changes <- snapshot:
	default:
	}
}

func readFrames(
	conn *net.UnixConn,
	out chan<- callReply,
	stop <-chan struct{},
	done chan<- struct{},
) {
	defer close(done)
	for {
		frame, err := agentsock.ReadFrame(conn)
		select {
		case out <- callReply{frame: frame, err: err}:
		case <-stop:
			return
		}
		if err != nil {
			return
		}
	}
}

func fromCore(op protocol.AgentOp) bool {
	for _, spec := range protocol.AgentOpSpecs() {
		if spec.Op == op {
			return spec.FromCore
		}
	}
	return false
}

func fromProxy(op protocol.AgentOp) bool {
	for _, spec := range protocol.AgentOpSpecs() {
		if spec.Op == op {
			return !spec.FromCore || op == protocol.AgentOpError
		}
	}
	return false
}

func reasonFor(err error) Reason {
	switch {
	case errors.Is(err, ErrInsecureSocket):
		return ReasonInsecureSocket
	case errors.Is(err, ErrPeerMismatch):
		return ReasonPeerMismatch
	case errors.Is(err, ErrSessionUnavailable):
		return ReasonSessionUnavailable
	case errors.Is(err, ErrInvalidGreeting):
		return ReasonInvalidGreeting
	case errors.Is(err, ErrHeartbeatTimeout):
		return ReasonHeartbeatTimeout
	case errors.Is(err, errGuardWriteFailed):
		return ReasonGuardWriteFailed
	case errors.Is(err, ErrCapabilitiesConflict):
		return ReasonCapabilitiesConflict
	case errors.Is(err, ErrUnsupportedPlatform):
		return ReasonUnsupported
	default:
		return ReasonConnectionLost
	}
}
