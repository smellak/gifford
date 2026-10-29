package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// ErrTransport means the endpoint could not be reached or did not answer
// with success. Its errors never carry the endpoint URL or the
// credential: net/http's own errors quote the URL, so they are not
// wrapped.
var ErrTransport = errors.New("glimmer transport failed")

// maxResponseBytes bounds one endpoint answer.
const maxResponseBytes = 1 << 20

// Transport sends one encoded, already-redacted request to the Glimmer
// endpoint and returns its encoded answer. A deadline it hits is
// reported as context.DeadlineExceeded.
type Transport interface {
	Send(ctx context.Context, body []byte) ([]byte, error)
}

// HTTPTransport is the Transport to the Glimmer endpoint served on Modal.
type HTTPTransport struct {
	endpoint Endpoint
	client   *http.Client
}

// NewHTTPTransport returns a transport to ep. It never follows a
// redirect, so the credential goes to the configured endpoint only.
func NewHTTPTransport(ep Endpoint, client *http.Client) (*HTTPTransport, error) {
	if ep.url == nil || ep.token == "" {
		return nil, fmt.Errorf("%w: no endpoint", ErrTransport)
	}
	if client == nil {
		client = &http.Client{}
	}
	c := *client
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &HTTPTransport{endpoint: ep, client: &c}, nil
}

// Send posts body as JSON and returns the answer of a 2xx response.
func (t *HTTPTransport) Send(ctx context.Context, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint.url.String(), bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("%w: request not built", ErrTransport)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.endpoint.token)
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, transportErr(ctx, "request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%w: status %d", ErrTransport, resp.StatusCode)
	}
	out, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, transportErr(ctx, "answer unreadable")
	}
	if len(out) > maxResponseBytes {
		return nil, fmt.Errorf("%w: answer too large", ErrTransport)
	}
	return out, nil
}

// transportErr reports a deadline as such and anything else as a fixed
// transport failure.
func transportErr(ctx context.Context, what string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s: %w", ErrTransport, what, context.DeadlineExceeded)
	}
	return fmt.Errorf("%w: %s", ErrTransport, what)
}
