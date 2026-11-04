package modelprofile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

type steppingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppingClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type noticeLog struct {
	mu      sync.Mutex
	notices []Notice
}

func (l *noticeLog) notify(_ context.Context, notice Notice) {
	l.mu.Lock()
	l.notices = append(l.notices, notice)
	l.mu.Unlock()
}

func (l *noticeLog) all() []Notice {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Notice(nil), l.notices...)
}

// fallbackFixture activates the registered adapter, then lets the gateway
// fall back to base, with a clock the test steps.
func fallbackFixture(t *testing.T) (*serviceFixture, *steppingClock, *noticeLog, protocol.ModelSelection) {
	t.Helper()
	fixture := newServiceFixture(t)
	clk := &steppingClock{now: fixture.clock.now}
	fixture.service.clock = clk
	notices := &noticeLog{}
	fixture.service.SetNotifier(notices.notify)
	req := fixture.validRegistration(t)
	_, err := fixture.register(t, req)
	require.NoError(t, err)
	activated, err := fixture.service.Activate(t.Context(), protocol.ActivateAdapterRequest{
		AdapterName: req.AdapterName, AdapterRevision: req.AdapterRevision,
	}, operator())
	require.NoError(t, err)
	require.NoError(t, fixture.service.FallbackToBase(t.Context(), activated.Selection.Generation, "adapter call timed out"))
	require.Equal(t, protocol.InferenceProfileBase, fixture.publisher.CurrentModelSelection().Profile.InferenceProfile)
	return fixture, clk, notices, activated.Selection
}

func TestFallbackReactivatesTheAdapterWithBackoff(t *testing.T) {
	fixture, clk, notices, adapter := fallbackFixture(t)
	require.Equal(t, []Notice{{
		Kind: NoticeFallback, Generation: adapter.Generation + 1,
		AdapterName: adapter.Profile.AdapterName, AdapterRevision: adapter.Profile.AdapterRevision,
		Reason: "adapter call timed out",
	}}, notices.all())

	probes := len(fixture.verifier.seen)
	reactivated, err := fixture.service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, reactivated, "the first probe waits for the adapter breaker window")
	require.Len(t, fixture.verifier.seen, probes)

	fixture.verifier.err = errors.New("not served yet")
	clk.advance(reactivationFirstDelay)
	reactivated, err = fixture.service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, reactivated)
	require.Len(t, fixture.verifier.seen, probes+1)

	clk.advance(reactivationFirstDelay)
	reactivated, err = fixture.service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, reactivated)
	require.Len(t, fixture.verifier.seen, probes+1, "a failed probe doubles the wait")

	fixture.verifier.err = nil
	clk.advance(reactivationFirstDelay)
	reactivated, err = fixture.service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, reactivated)
	selected := fixture.publisher.CurrentModelSelection()
	require.Equal(t, adapter.Generation+2, selected.Generation)
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, selected.Profile.InferenceProfile)
	require.Equal(t, adapter.AdapterSHA256, selected.AdapterSHA256)
	require.Equal(t, "READY", selected.OperationalState)
	require.Equal(t, reactivationReason, selected.Reason)
	require.Equal(t, selected, fixture.verifier.seen[len(fixture.verifier.seen)-1])
	require.Equal(t, Notice{
		Kind: NoticeReactivated, Generation: selected.Generation,
		AdapterName: adapter.Profile.AdapterName, AdapterRevision: adapter.Profile.AdapterRevision,
		Reason: reactivationReason,
	}, notices.all()[1])

	clk.advance(reactivationMaxDelay)
	reactivated, err = fixture.service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, reactivated, "nothing is pending once the adapter is back")
	require.Len(t, notices.all(), 2)
}

func TestOperatorRollbackCancelsTheReactivation(t *testing.T) {
	fixture, clk, notices, _ := fallbackFixture(t)
	rolledBack, err := fixture.service.Rollback(t.Context(), protocol.RollbackToBaseRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
	}, operator())
	require.NoError(t, err)
	require.True(t, rolledBack.Applied, "the operator's choice replaces the automatic fallback")
	require.Equal(t, "READY", rolledBack.Selection.OperationalState)
	clk.advance(reactivationFirstDelay)
	probes := len(fixture.verifier.seen)
	reactivated, err := fixture.service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, reactivated)
	require.Len(t, fixture.verifier.seen, probes)
	require.Equal(t, protocol.InferenceProfileBase, fixture.publisher.CurrentModelSelection().Profile.InferenceProfile)
	require.Len(t, notices.all(), 1)

	restarted, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	opts := fixture.options(fixture.store)
	opts.Publisher = &testPublisher{current: restarted}
	opts.Verifier = fixture.verifier
	opts.Clock = clk
	service, err := New(opts)
	require.NoError(t, err)
	reactivated, err = service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.False(t, reactivated, "a restart does not undo the operator's rollback")
}

func TestRestartResumesThePendingReactivation(t *testing.T) {
	fixture, clk, _, adapter := fallbackFixture(t)
	restarted, err := Initialize(t.Context(), fixture.options(fixture.store))
	require.NoError(t, err)
	require.Equal(t, "DEGRADED", restarted.OperationalState)
	opts := fixture.options(fixture.store)
	opts.Publisher = &testPublisher{current: restarted}
	opts.Verifier = fixture.verifier
	opts.Clock = clk
	service, err := New(opts)
	require.NoError(t, err)

	reactivated, err := service.ReactivateOnce(t.Context())
	require.NoError(t, err)
	require.True(t, reactivated)
	selected := opts.Publisher.CurrentModelSelection()
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, selected.Profile.InferenceProfile)
	require.Equal(t, adapter.Profile.AdapterRevision, selected.Profile.AdapterRevision)
}

func TestReactivationBackoffDoublesToItsCap(t *testing.T) {
	require.Equal(t, reactivationFirstDelay, reactivationBackoff(0))
	require.Equal(t, 2*reactivationFirstDelay, reactivationBackoff(1))
	require.Equal(t, 4*reactivationFirstDelay, reactivationBackoff(2))
	require.Equal(t, reactivationMaxDelay, reactivationBackoff(10))
}
