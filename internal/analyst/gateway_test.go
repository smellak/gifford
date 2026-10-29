package analyst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

const gatewayYAML = `
endpoint:
  url_env: U
  token_env: T
base_model:
  repo: example.invalid/glimmer
  revision: v1
runtime: vllm
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: base
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`

// manualClock reads a fixed time and fires timers only on advance.
type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []manualTimer
	armed  chan struct{}
}

type manualTimer struct {
	at time.Time
	ch chan time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.timers = append(c.timers, manualTimer{c.now.Add(d), ch})
	c.armed <- struct{}{}
	return ch
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	kept := c.timers[:0]
	for _, tm := range c.timers {
		if tm.at.After(c.now) {
			kept = append(kept, tm)
			continue
		}
		tm.ch <- c.now
	}
	c.timers = kept
}

// endpoint is a fake transport: it keeps the bodies, and fails, hangs or
// answers as set.
type endpoint struct {
	mu     sync.Mutex
	bodies []string
	answer string
	fail   bool
	hang   chan struct{}
}

func (e *endpoint) Send(ctx context.Context, body []byte) ([]byte, error) {
	e.mu.Lock()
	e.bodies = append(e.bodies, string(body))
	answer, fail, hang := e.answer, e.fail, e.hang
	e.mu.Unlock()
	if hang != nil {
		select {
		case <-hang:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail {
		return nil, errors.New("endpoint down")
	}
	return []byte(answer), nil
}

func (e *endpoint) sent() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.bodies...)
}

type nopRecorder struct{}

func (nopRecorder) RecordCall(context.Context, protocol.GlimmerCallRecord) error { return nil }

type counterIDs struct{ n atomic.Int64 }

func (c *counterIDs) NewCallID() protocol.OpaqueID {
	return protocol.OpaqueID(fmt.Sprintf("call-%d", c.n.Add(1)))
}

func realGateway(t *testing.T, ep *endpoint) (*gateway.Gateway, *manualClock) {
	t.Helper()
	cfg, err := gateway.ParseConfig(strings.NewReader(gatewayYAML), func(k string) string {
		return map[string]string{"U": "http://127.0.0.1:1", "T": "test-token"}[k]
	})
	require.NoError(t, err)
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	clk := &manualClock{now: t0, armed: make(chan struct{}, 64)}
	gw, err := gateway.New(cfg, gateway.Deps{Transport: ep, Redactor: red, Recorder: nopRecorder{}, Clock: clk, IDs: &counterIDs{}})
	require.NoError(t, err)
	return gw, clk
}

const runningAnswer = `{"label":"RUNNING_TOOL","probabilities":{"RUNNING_TOOL":0.9,"UNKNOWN":0.1},"evidence":["go test ./..."]}`

func TestAnalystThroughTheGateway(t *testing.T) {
	t.Parallel()
	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	ep := &endpoint{answer: runningAnswer}
	gw, _ := realGateway(t, ep)
	obs := baseObservation()
	obs.Screen = []string{"$ go test ./...", "token " + secret, "ok  example.invalid/pkg 0.2s"}

	got, err := newAnalyst(t, gw).Analyze(context.Background(), obs)
	require.NoError(t, err, "the Analyst payload passes the gateway's field schema")
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.InDelta(t, 0.9, got.Confidence, 1e-9)
	require.Equal(t, []string{"go test ./..."}, got.Evidence)
	require.Equal(t, protocol.GlimmerCallStatusOK, got.CallRecord.Status)

	sent := ep.sent()
	require.Len(t, sent, 1)
	require.NotContains(t, sent[0], secret, "the screen is redacted before it leaves")
	var wire struct {
		Payload analyzePayload `json:"payload"`
	}
	require.NoError(t, json.Unmarshal([]byte(sent[0]), &wire))
	require.Equal(t, PromptVersion, wire.Payload.PromptVersion, "the version is inside the digested input")
}

func TestAnalystUnknownWhenTheGatewayFails(t *testing.T) {
	t.Parallel()

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()
		ep := &endpoint{answer: runningAnswer, hang: make(chan struct{})}
		t.Cleanup(func() { close(ep.hang) })
		gw, clk := realGateway(t, ep)
		done := make(chan error, 1)
		var got Analysis
		obs := baseObservation()
		a := newAnalyst(t, gw)
		go func() {
			var err error
			got, err = a.Analyze(context.Background(), obs)
			done <- err
		}()
		<-clk.armed
		clk.advance(3 * time.Second)
		err := <-done
		require.ErrorIs(t, err, ErrModelUnavailable)
		require.ErrorIs(t, err, gateway.ErrTimeout)
		require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
		require.True(t, got.PreviousStale)
		require.Equal(t, protocol.GlimmerCallStatusTimeout, got.CallRecord.Status)
	})

	t.Run("invalid answer", func(t *testing.T) {
		t.Parallel()
		gw, _ := realGateway(t, &endpoint{answer: `{"label":"BACKGROUND_WORK","probabilities":{"BACKGROUND_WORK":1}}`})
		got, err := newAnalyst(t, gw).Analyze(context.Background(), baseObservation())
		require.ErrorIs(t, err, gateway.ErrInvalidOutput)
		require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
		require.True(t, got.PreviousStale)
	})

	t.Run("open breaker", func(t *testing.T) {
		t.Parallel()
		ep := &endpoint{fail: true}
		gw, clk := realGateway(t, ep)
		a := newAnalyst(t, gw)
		for range 3 {
			_, err := a.Analyze(context.Background(), baseObservation())
			require.ErrorIs(t, err, gateway.ErrCallFailed)
		}
		got, err := a.Analyze(context.Background(), baseObservation())
		require.ErrorIs(t, err, gateway.ErrCircuitOpen)
		require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
		require.True(t, got.PreviousStale)
		require.Len(t, ep.sent(), 3, "an open breaker sends nothing")

		clk.advance(30 * time.Second)
		ep.mu.Lock()
		ep.fail, ep.answer = false, runningAnswer
		ep.mu.Unlock()
		got, err = a.Analyze(context.Background(), baseObservation())
		require.NoError(t, err, "the probe closes it")
		require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	})

	t.Run("call in flight", func(t *testing.T) {
		t.Parallel()
		ep := &endpoint{answer: runningAnswer, hang: make(chan struct{})}
		gw, clk := realGateway(t, ep)
		a := newAnalyst(t, gw)
		obs := baseObservation()
		done := make(chan error, 1)
		go func() {
			_, err := a.Analyze(context.Background(), obs)
			done <- err
		}()
		<-clk.armed
		got, err := a.Analyze(context.Background(), obs)
		require.ErrorIs(t, err, gateway.ErrAgentBusy, "refused at once")
		require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
		require.True(t, got.PreviousStale)
		close(ep.hang)
		require.NoError(t, <-done)
	})
}
