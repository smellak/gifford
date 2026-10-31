package tui

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultCallTimeout = 250 * time.Millisecond
	maxControlFrame    = 1 << 20
)

var (
	// ErrMutationForbidden means a Control API method is marked as mutating.
	// The panel refuses it before opening the control socket unless that exact
	// method is in the panel's closed allowlist.
	ErrMutationForbidden = errors.New("control mutation forbidden from panel")
	// ErrMethodNotAllowed means a method is not in the panel's explicit
	// allowlist, even if a future protocol version marks it read-only.
	ErrMethodNotAllowed = errors.New("control method not allowed from panel")
)

// ClientOptions configures the panel's bounded Control API client.
type ClientOptions struct {
	SocketPath string
	Timeout    time.Duration
}

// Client performs one bounded JSON-RPC call per daemon.sock connection.
// Call enforces an explicit allowlist before dialing. approval.resolve is the
// only mutation the panel exposes.
type Client struct {
	socketPath string
	timeout    time.Duration
	nextID     atomic.Int64
}

// NewClient validates options and returns a bounded Control API client.
func NewClient(opts ClientOptions) (*Client, error) {
	if opts.SocketPath == "" || !filepath.IsAbs(filepath.Clean(opts.SocketPath)) {
		return nil, fmt.Errorf("control socket path must be absolute")
	}
	if opts.Timeout < 0 {
		return nil, fmt.Errorf("control call timeout must not be negative")
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultCallTimeout
	}
	return &Client{socketPath: filepath.Clean(opts.SocketPath), timeout: opts.Timeout}, nil
}

// The allowlist is deliberately closed. Adding a method to the protocol
// does not automatically expose it through the panel.
var panelMethods = map[protocol.ControlMethod]struct{}{
	protocol.ControlMethodChannelGetGlobal:    {},
	protocol.ControlMethodChannelGetNode:      {},
	protocol.ControlMethodChannelGetEffective: {},
	protocol.ControlMethodChannelExplain:      {},
	protocol.ControlMethodAgentChannelGet:     {},
	protocol.ControlMethodLinkGet:             {},
	protocol.ControlMethodLinkList:            {},
	protocol.ControlMethodApprovalList:        {},
	protocol.ControlMethodApprovalResolve:     {},
	protocol.ControlMethodAgentList:           {},
	protocol.ControlMethodMessageList:         {},
	protocol.ControlMethodDeliveryList:        {},
	protocol.ControlMethodNotificationList:    {},
	protocol.ControlMethodDecisionList:        {},
	protocol.ControlMethodDecisionGet:         {},
	protocol.ControlMethodModelStatus:         {},
}

// Call performs one allowlisted JSON-RPC request. Each call has its own
// timeout and is interrupted promptly when ctx is cancelled.
func (c *Client) Call(ctx context.Context, method protocol.ControlMethod, params, result any) error {
	if err := allowPanelCall(method, params); err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(callCtx, "unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("connect to control socket: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if deadline, ok := callCtx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set control deadline: %w", err)
		}
	}
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(callCtx, func() {
		defer close(cancelDone)
		_ = conn.SetDeadline(time.Unix(1, 0))
	})
	defer func() {
		if !stopCancel() {
			<-cancelDone
		}
		_ = conn.SetDeadline(time.Time{})
	}()

	rawParams, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("encode control params: %w", err)
	}
	id := c.nextID.Add(1)
	request := protocol.RPCRequest{
		JSONRPC: protocol.JSONRPCVersion,
		ID:      id,
		Method:  method,
		Params:  protocol.Payload(rawParams),
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return controlIOError(callCtx, "write control request", err)
	}

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), maxControlFrame)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return controlIOError(callCtx, "read control response", err)
		}
		return fmt.Errorf("read control response: unexpected end of stream")
	}
	var response protocol.RPCResponse
	if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
		return fmt.Errorf("decode control response: %w", err)
	}
	if err := response.Validate(); err != nil {
		return fmt.Errorf("invalid control response: %w", err)
	}
	if response.ID != id {
		return fmt.Errorf("invalid control response: id %d does not match request %d", response.ID, id)
	}
	if response.Error != nil {
		if method == protocol.ControlMethodApprovalResolve &&
			response.Error.Data != nil && response.Error.Data.Detail != "" {
			return errors.New(response.Error.Data.Detail)
		}
		return fmt.Errorf("control api %s: %s", method, response.Error.Message)
	}
	if err := json.Unmarshal(response.Result, result); err != nil {
		return fmt.Errorf("decode control result: %w", err)
	}
	return nil
}

func allowPanelCall(method protocol.ControlMethod, params any) error {
	if err := allowPanelMethod(method); err != nil {
		return err
	}
	if method != protocol.ControlMethodApprovalResolve {
		return nil
	}
	resolve, ok := params.(protocol.ResolveApprovalParams)
	if !ok || resolve.Option != approvalDeliverNowOption && resolve.Option != approvalRejectOption {
		return fmt.Errorf("%w: unsupported approval option", ErrMutationForbidden)
	}
	return nil
}

func allowPanelMethod(method protocol.ControlMethod) error {
	for _, spec := range protocol.ControlMethodSpecs() {
		if spec.Method != method {
			continue
		}
		if _, ok := panelMethods[method]; ok {
			return nil
		}
		if spec.Mutates {
			return fmt.Errorf("%w: %s", ErrMutationForbidden, method)
		}
		return fmt.Errorf("%w: %s", ErrMethodNotAllowed, method)
	}
	return fmt.Errorf("%w: %s", ErrMethodNotAllowed, method)
}

func controlIOError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%s: %w", operation, ctxErr)
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		// The only connection deadline is callCtx's deadline. The network
		// poller can report it just before the context timer publishes Err().
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
