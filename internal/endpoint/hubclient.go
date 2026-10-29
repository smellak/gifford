package endpoint

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the Hub link: what the tool handlers call. Handlers run
// behind the Hub interface; the socket client implements it against
// hub.sock, and tests substitute scripted fakes. Results travel as raw
// JSON the handlers shape; only admission verdicts are typed.

// jsonRPCVersion is the only wire version the Hub link speaks.
const jsonRPCVersion = "2.0"

// Hub is the Hub RPC the handlers call. Every method takes shape in a
// later subphase; until then the real Hub answers -32603 and the fake
// answers its script.
type Hub interface {
	// WhoAmI returns the agent the Hub bound to this connection.
	WhoAmI(ctx context.Context) (protocol.AgentID, error)
	// Call invokes one Hub method with opaque params and returns its
	// raw result. Hub-side errors refuse with a HubError.
	Call(ctx context.Context, method string, params any) (json.RawMessage, error)
}

// HubError is a Hub-side RPC failure: its code with a fixed message, no
// content.
type HubError struct {
	Code int
}

// Error reports the Hub failure without content.
func (e *HubError) Error() string {
	return fmt.Sprintf("hub error %d", e.Code)
}

// SocketHub implements Hub over one hub.sock connection: line-delimited
// JSON-RPC, one message per line. It is safe for concurrent use.
type SocketHub struct {
	slot   chan struct{}
	conn   net.Conn
	reader *bufio.Reader
	next   int64
}

// DialHub connects to the Hub socket at path, bounded by ctx.
func DialHub(ctx context.Context, path string) (*SocketHub, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("dial hub: %w", ctx.Err())
		}
		return nil, fmt.Errorf("dial hub: %w", ErrHubUnavailable)
	}
	return &SocketHub{slot: make(chan struct{}, 1), conn: conn, reader: bufio.NewReader(conn)}, nil
}

// Close drops the Hub connection.
func (h *SocketHub) Close() error {
	if err := h.conn.Close(); err != nil {
		return fmt.Errorf("close hub: %w", err)
	}
	return nil
}

// WhoAmI asks the Hub for the connection's agent id.
func (h *SocketHub) WhoAmI(ctx context.Context) (protocol.AgentID, error) {
	raw, err := h.Call(ctx, "whoami", nil)
	if err != nil {
		return "", err
	}
	var answer struct {
		AgentID protocol.AgentID `json:"agent_id"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		return "", fmt.Errorf("hub whoami: %w", ErrHubUnavailable)
	}
	if !answer.AgentID.Valid() {
		return "", fmt.Errorf("hub whoami: %w", ErrHubUnavailable)
	}
	return answer.AgentID, nil
}

// hubRequest is one outgoing Hub call.
type hubRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      int64  `json:"id"`
}

// hubResponse is one Hub answer line.
type hubResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code int `json:"code"`
	} `json:"error"`
	ID int64 `json:"id"`
}

// Call invokes one Hub method and returns its raw result.
func (h *SocketHub) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("hub call: %w", err)
	}
	select {
	case h.slot <- struct{}{}:
		defer func() { <-h.slot }()
	case <-ctx.Done():
		return nil, fmt.Errorf("hub call: %w", ctx.Err())
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := h.conn.SetDeadline(deadline); err != nil {
			return nil, fmt.Errorf("hub call deadline: %w", ErrHubUnavailable)
		}
	}
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		_ = h.conn.SetDeadline(time.Unix(1, 0))
	})
	defer func() {
		if !stop() {
			<-cancelDone
		}
		_ = h.conn.SetDeadline(time.Time{})
	}()
	h.next++
	requestID := h.next
	line, err := json.Marshal(hubRequest{JSONRPC: jsonRPCVersion, Method: method, Params: params, ID: requestID})
	if err != nil {
		return nil, fmt.Errorf("hub call: %w", ErrHubUnavailable)
	}
	written := false
	healthy := false
	defer func() {
		if written && !healthy {
			_ = h.conn.Close()
		}
	}()
	line = append(line, '\n')
	n, err := h.conn.Write(line)
	written = n > 0
	if err != nil || n != len(line) {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("hub call: %w", ctx.Err())
		}
		return nil, fmt.Errorf("hub call: %w", ErrHubUnavailable)
	}
	written = true
	answer, err := h.reader.ReadBytes('\n')
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("hub call: %w", ctx.Err())
		}
		return nil, fmt.Errorf("hub call: %w", ErrHubUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("hub call: %w", err)
	}
	var resp hubResponse
	if err := json.Unmarshal(answer, &resp); err != nil {
		return nil, fmt.Errorf("hub call: %w", ErrHubUnavailable)
	}
	if resp.JSONRPC != jsonRPCVersion || resp.ID != requestID {
		return nil, fmt.Errorf("hub call: %w", ErrHubUnavailable)
	}
	healthy = true
	if resp.Error != nil {
		return nil, &HubError{Code: resp.Error.Code}
	}
	return resp.Result, nil
}
