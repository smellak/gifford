package gateway

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const unknownAnswer = `{"label":"UNKNOWN","probabilities":{"UNKNOWN":1}}`

// switchTransport fails while fail is set and counts every send.
type switchTransport struct {
	fail atomic.Bool
	n    atomic.Int64
}

func (s *switchTransport) Send(context.Context, []byte) ([]byte, error) {
	s.n.Add(1)
	if s.fail.Load() {
		return nil, errors.New("sentinel endpoint down")
	}
	return []byte(unknownAnswer), nil
}

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()
	tr := &switchTransport{}
	tr.fail.Store(true)
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	clk := f.gw.deps.Clock.(*fakeClock)
	call := func() (protocol.InferResult, error) {
		return f.gw.Infer(context.Background(), analyzeRequest("x"))
	}

	for range breakerThreshold {
		_, err := call()
		require.ErrorIs(t, err, ErrCallFailed)
	}
	require.Equal(t, int64(breakerThreshold), tr.n.Load())

	res, err := call()
	require.ErrorIs(t, err, ErrCircuitOpen, "open after three failures in a row")
	require.Equal(t, int64(breakerThreshold), tr.n.Load(), "an open breaker does not call the endpoint")
	require.Equal(t, protocol.GlimmerCallStatusError, res.CallRecord.Status)
	require.NoError(t, res.CallRecord.Validate())
	require.NotContains(t, err.Error(), "sentinel")

	clk.advance(breakerOpenFor - time.Second)
	_, err = call()
	require.ErrorIs(t, err, ErrCircuitOpen, "still open before 30 s")

	clk.advance(time.Second)
	_, err = call()
	require.ErrorIs(t, err, ErrCallFailed, "the probe reaches the endpoint")
	_, err = call()
	require.ErrorIs(t, err, ErrCircuitOpen, "a failed probe opens it again")

	clk.advance(breakerOpenFor)
	tr.fail.Store(false)
	_, err = call()
	require.NoError(t, err, "a good probe closes it")
	_, err = call()
	require.NoError(t, err)
	require.Len(t, f.recorder.all(), 9, "every call, refused or not, leaves a record")
}

func TestBreakerCountsOnlyEndpointFailuresInARow(t *testing.T) {
	t.Parallel()
	tr := &switchTransport{}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	call := func(req protocol.InferRequest) error {
		_, err := f.gw.Infer(context.Background(), req)
		return err
	}
	bad := protocol.InferRequest{Contract: protocol.ModelContractTriageMessage, Payload: protocol.Payload(`{"notes":"x"}`)}
	for range breakerThreshold + 1 {
		require.ErrorIs(t, call(bad), ErrRedactionFailed)
	}
	tr.fail.Store(true)
	for range breakerThreshold - 1 {
		require.ErrorIs(t, call(analyzeRequest("x")), ErrCallFailed)
	}
	tr.fail.Store(false)
	require.NoError(t, call(analyzeRequest("x")), "a success resets the count")
	tr.fail.Store(true)
	for range breakerThreshold - 1 {
		require.ErrorIs(t, call(analyzeRequest("x")), ErrCallFailed)
	}
	require.ErrorIs(t, call(analyzeRequest("x")), ErrCallFailed, "redaction failures and reset runs never opened it")
}

func TestBreakerAllowsOneProbe(t *testing.T) {
	t.Parallel()
	var b breaker
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for range breakerThreshold {
		ok, _ := b.allow(now)
		require.True(t, ok)
		b.done(now, false, outcomeFailure)
	}
	now = now.Add(breakerOpenFor)
	ok, probe := b.allow(now)
	require.True(t, ok)
	require.True(t, probe)
	ok, _ = b.allow(now)
	require.False(t, ok, "one probe at a time")
	b.done(now, true, outcomeNeutral)
	ok, probe = b.allow(now)
	require.True(t, ok, "a probe that never reached the endpoint frees the slot")
	require.True(t, probe)
}

// gateTransport blocks its first send until released.
type gateTransport struct {
	entered chan struct{}
	release chan struct{}
	first   atomic.Bool
}

func (g *gateTransport) Send(ctx context.Context, _ []byte) ([]byte, error) {
	if g.first.CompareAndSwap(false, true) {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return []byte(unknownAnswer), nil
}

func TestOneAnalystCallInFlightPerAgent(t *testing.T) {
	t.Parallel()
	tr := &gateTransport{entered: make(chan struct{}), release: make(chan struct{})}
	f := newFixture(t, "http://127.0.0.1:1", tr, nil)
	agent := func(id protocol.AgentID) protocol.InferRequest {
		req := analyzeRequest("x")
		req.AgentID = id
		return req
	}
	done := make(chan error, 1)
	go func() {
		_, err := f.gw.Infer(context.Background(), agent("agent-1"))
		done <- err
	}()
	<-tr.entered

	res, err := f.gw.Infer(context.Background(), agent("agent-1"))
	require.ErrorIs(t, err, ErrAgentBusy, "a second call for the same agent is refused at once")
	require.Equal(t, protocol.GlimmerCallStatusError, res.CallRecord.Status)
	require.Equal(t, protocol.AgentID("agent-1"), res.CallRecord.AgentID)

	_, err = f.gw.Infer(context.Background(), agent("agent-2"))
	require.NoError(t, err, "another agent is not held up")
	triage := protocol.InferRequest{Contract: protocol.ModelContractTriageMessage, AgentID: "agent-1", Payload: protocol.Payload(`{"text":"hi"}`)}
	_, err = f.gw.Infer(context.Background(), triage)
	require.NotErrorIs(t, err, ErrAgentBusy, "the limit is on Analyst calls only")

	close(tr.release)
	require.NoError(t, <-done)
	_, err = f.gw.Infer(context.Background(), agent("agent-1"))
	require.NoError(t, err, "the agent is free once its call ends")
}
