package adapters

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultHandshakeTimeout = 2 * time.Second
	defaultHeartbeatEvery   = 3 * time.Second
	defaultHeartbeatTimeout = 2 * time.Second
)

var (
	// ErrInvalidOptions means Connect received an unusable identity, fallback,
	// path or duration.
	ErrInvalidOptions = errors.New("adapters: invalid options")
	// ErrInsecureSocket means the adapter endpoint is not a local Unix socket
	// with mode 0600.
	ErrInsecureSocket = errors.New("adapters: socket is not private")
	// ErrUnavailable means the adapter could not complete its handshake.
	ErrUnavailable = errors.New("adapters: unavailable")
	// ErrInvalidManifest means the adapter replied with a manifest that was not
	// valid for this run.
	ErrInvalidManifest = errors.New("adapters: invalid capability manifest")
)

// Options configures one adapter handshake and liveness monitor.
type Options struct {
	SocketPath       string
	AgentID          protocol.AgentID
	Harness          protocol.Harness
	Fallback         protocol.CapabilityManifest
	HandshakeTimeout time.Duration
	HeartbeatEvery   time.Duration
	HeartbeatTimeout time.Duration
	Clock            clock.Clock
}

// CapabilitiesChangedEvent is emitted once when a verified native adapter
// becomes unavailable and the client returns to its generic PTY manifest.
type CapabilitiesChangedEvent struct {
	EventType protocol.EventType    `json:"event_type"`
	Before    protocol.Capabilities `json:"before"`
	After     protocol.Capabilities `json:"after"`
}

// Client is one verified native-adapter link. It owns the connection and a
// heartbeat monitor. Close is safe to call more than once.
type Client struct {
	conn             net.Conn
	clock            clock.Clock
	heartbeatEvery   time.Duration
	heartbeatTimeout time.Duration
	fallback         protocol.CapabilityManifest

	mu       sync.RWMutex
	manifest protocol.CapabilityManifest

	changes  chan CapabilitiesChangedEvent
	stop     chan struct{}
	wg       sync.WaitGroup
	once     sync.Once
	closeErr error
}

// GenericManifest returns the guaranteed PTY capability baseline for one
// run. An empty harness version is normalized to "unknown".
func GenericManifest(agent protocol.AgentID, harness protocol.Harness, version string) protocol.CapabilityManifest {
	if version == "" {
		version = "unknown"
	}
	return protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            agent,
		Harness:            harness,
		HarnessVersion:     version,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy,
		Deliverable:        true,
		Capabilities: protocol.Capabilities{
			PTYCapture: true,
			MCP:        true,
		},
	}
}

// Connect performs the bounded handshake. On every failure it returns a nil
// client, the caller's generic PTY fallback and an explanatory error. A
// successful result starts liveness monitoring and returns the verified native
// manifest.
func Connect(ctx context.Context, opts Options) (*Client, protocol.CapabilityManifest, error) {
	fallback := opts.Fallback
	if err := validateOptions(&opts); err != nil {
		return nil, fallback, err
	}
	if err := privateSocket(opts.SocketPath); err != nil {
		return nil, fallback, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", opts.SocketPath)
	if err != nil {
		return nil, fallback, unavailable(ctx, "dial", err)
	}
	ok := false
	defer func() {
		if !ok {
			_ = conn.Close()
		}
	}()
	deadline := opts.Clock.Now().Add(opts.HandshakeTimeout)
	if contextDeadline, hasDeadline := ctx.Deadline(); hasDeadline && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fallback, unavailable(ctx, "set handshake deadline", err)
	}
	request := Message{Operation: OperationHandshake, ID: 1, AgentID: opts.AgentID, Harness: opts.Harness}
	if err := WriteMessage(conn, request); err != nil {
		return nil, fallback, unavailable(ctx, "write handshake", err)
	}
	reply, err := ReadMessage(conn)
	if err != nil {
		return nil, fallback, unavailable(ctx, "read handshake", err)
	}
	if reply.Operation != OperationManifest || reply.ReplyTo != request.ID {
		return nil, fallback, fmt.Errorf("%w: handshake reply", ErrInvalidManifest)
	}
	manifest := *reply.Manifest
	if err := validateNative(manifest, opts.AgentID, opts.Harness); err != nil {
		return nil, fallback, err
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fallback, unavailable(ctx, "clear handshake deadline", err)
	}
	client := &Client{
		conn:             conn,
		clock:            opts.Clock,
		heartbeatEvery:   opts.HeartbeatEvery,
		heartbeatTimeout: opts.HeartbeatTimeout,
		fallback:         fallback,
		manifest:         manifest,
		changes:          make(chan CapabilitiesChangedEvent, 1),
		stop:             make(chan struct{}),
	}
	client.wg.Add(1)
	go client.monitor(ctx)
	ok = true
	return client, manifest, nil
}

// Manifest returns the current manifest. After a liveness failure it is the
// generic PTY fallback.
func (c *Client) Manifest() protocol.CapabilityManifest {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.manifest
}

// Changes returns the degradation event stream. It closes when monitoring
// ends and contains at most the single native-to-generic transition.
func (c *Client) Changes() <-chan CapabilitiesChangedEvent { return c.changes }

// Close stops monitoring and closes the adapter link without emitting a
// degradation event.
func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.stop)
		if err := c.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			c.closeErr = fmt.Errorf("adapters: close: %w", err)
		}
		c.wg.Wait()
	})
	return c.closeErr
}

func validateOptions(opts *Options) error {
	if opts.Clock == nil {
		opts.Clock = clock.System{}
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = defaultHandshakeTimeout
	}
	if opts.HeartbeatEvery <= 0 {
		opts.HeartbeatEvery = defaultHeartbeatEvery
	}
	if opts.HeartbeatTimeout <= 0 {
		opts.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if !filepath.IsAbs(opts.SocketPath) || !opts.AgentID.Valid() || !opts.Harness.Valid() {
		return ErrInvalidOptions
	}
	if err := opts.Fallback.Validate(); err != nil {
		return fmt.Errorf("%w: fallback: %w", ErrInvalidOptions, err)
	}
	if opts.Fallback.AgentID != opts.AgentID || opts.Fallback.Harness != opts.Harness ||
		opts.Fallback.AdapterType != protocol.AdapterTypeGenericPTY ||
		opts.Fallback.InputGuardProvider != protocol.InputGuardProviderPTYProxy {
		return fmt.Errorf("%w: fallback identity", ErrInvalidOptions)
	}
	return nil
}

func privateSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if info.Mode().Type() != os.ModeSocket || info.Mode().Perm() != 0o600 {
		return ErrInsecureSocket
	}
	return nil
}

func validateNative(manifest protocol.CapabilityManifest, agent protocol.AgentID, harness protocol.Harness) error {
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}
	switch {
	case manifest.AgentID != agent:
		return fmt.Errorf("%w: agent_id mismatch", ErrInvalidManifest)
	case manifest.Harness != harness:
		return fmt.Errorf("%w: harness mismatch", ErrInvalidManifest)
	case manifest.HarnessVersion == "":
		return fmt.Errorf("%w: harness_version missing", ErrInvalidManifest)
	case !strings.HasPrefix(string(manifest.AdapterType), "native_"):
		return fmt.Errorf("%w: adapter_type is not native", ErrInvalidManifest)
	case manifest.InputGuardProvider != protocol.InputGuardProviderPTYProxy:
		return fmt.Errorf("%w: input guard is not the PTY proxy", ErrInvalidManifest)
	}
	return nil
}

func unavailable(ctx context.Context, operation string, err error) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%w: %s: %w", ErrUnavailable, operation, ctx.Err())
	}
	return fmt.Errorf("%w: %s: %w", ErrUnavailable, operation, err)
}

func (c *Client) monitor(ctx context.Context) {
	defer c.wg.Done()
	defer close(c.changes)
	var nextID int64 = 2
	for {
		timer := time.NewTimer(c.heartbeatEvery)
		select {
		case <-ctx.Done():
			_ = timer.Stop()
			return
		case <-c.stop:
			_ = timer.Stop()
			return
		case <-timer.C:
		}
		if err := c.heartbeat(nextID); err != nil {
			if c.stopping(ctx) {
				return
			}
			c.degrade()
			_ = c.conn.Close()
			return
		}
		nextID++
	}
}

func (c *Client) heartbeat(id int64) error {
	if err := c.conn.SetDeadline(c.clock.Now().Add(c.heartbeatTimeout)); err != nil {
		return fmt.Errorf("set heartbeat deadline: %w", err)
	}
	if err := WriteMessage(c.conn, Message{Operation: OperationPing, ID: id}); err != nil {
		return err
	}
	reply, err := ReadMessage(c.conn)
	if err != nil {
		return err
	}
	if reply.Operation != OperationPong || reply.ReplyTo != id {
		return ErrInvalidMessage
	}
	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("clear heartbeat deadline: %w", err)
	}
	return nil
}

func (c *Client) stopping(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	case <-c.stop:
		return true
	default:
		return false
	}
}

func (c *Client) degrade() {
	c.mu.Lock()
	before := c.manifest.Capabilities
	c.manifest = c.fallback
	after := c.manifest.Capabilities
	c.mu.Unlock()
	c.changes <- CapabilitiesChangedEvent{
		EventType: protocol.EventTypeAdapterCapabilitiesChanged,
		Before:    before,
		After:     after,
	}
}
