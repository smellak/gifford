package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/procguard"
)

func settleTestRuntime(t *testing.T, base []procguard.Entry) (*tmuxAttachRuntime, *procguard.Guard) {
	t.Helper()
	guard := &procguard.Guard{}
	guard.Rebaseline("panel", base)
	runtime := &tmuxAttachRuntime{
		processGuard:  guard,
		baselineReady: map[string]bool{"panel": true},
		settling:      make(map[string]tmuxBaselineCandidate),
	}
	return runtime, guard
}

func TestSettleBaselineFollowsAPanelThatOnlyLostProcesses(t *testing.T) {
	t.Parallel()
	cli := procguard.Entry{PID: 10, StartTime: 1}
	loop := procguard.Entry{PID: 20, StartTime: 2}
	runtime, guard := settleTestRuntime(t, []procguard.Entry{cli, loop})
	now := time.Date(2026, time.November, 4, 10, 0, 0, 0, time.UTC)
	fresh := []procguard.Entry{cli}
	require.Equal(t, procguard.Uncertain, guard.Check("panel", fresh))

	require.Equal(t, procguard.Uncertain,
		runtime.settleBaseline("panel", fresh, procguard.Uncertain, false, now),
		"without confirmed rest the baseline stays")
	require.Equal(t, procguard.Free,
		runtime.settleBaseline("panel", fresh, procguard.Uncertain, true, now))
	require.Equal(t, procguard.Free, guard.Check("panel", fresh))
}

func TestSettleBaselineAdoptsNewProcessesOnlyAfterTheyStandAtRest(t *testing.T) {
	t.Parallel()
	cli := procguard.Entry{PID: 10, StartTime: 1}
	runtime, guard := settleTestRuntime(t, []procguard.Entry{cli})
	now := time.Date(2026, time.November, 4, 10, 0, 0, 0, time.UTC)
	server := []procguard.Entry{cli, {PID: 30, StartTime: 3}}
	require.Equal(t, procguard.Busy, guard.Check("panel", server))

	require.Equal(t, procguard.Busy, runtime.settleBaseline("panel", server, procguard.Busy, true, now))
	require.Equal(t, procguard.Busy, runtime.settleBaseline("panel", server, procguard.Busy, true,
		now.Add(tmuxBaselineSettle-time.Second)))

	// A job that keeps starting processes restarts the count each pass.
	churn := []procguard.Entry{cli, {PID: 31, StartTime: 4}}
	require.Equal(t, procguard.Busy, runtime.settleBaseline("panel", churn, procguard.Busy, true,
		now.Add(tmuxBaselineSettle)))
	require.Equal(t, procguard.Busy, runtime.settleBaseline("panel", churn, procguard.Busy, true,
		now.Add(2*tmuxBaselineSettle-time.Second)))

	// Losing rest restarts it too.
	require.Equal(t, procguard.Busy, runtime.settleBaseline("panel", churn, procguard.Busy, false,
		now.Add(2*tmuxBaselineSettle)))
	require.Equal(t, procguard.Busy, runtime.settleBaseline("panel", churn, procguard.Busy, true,
		now.Add(2*tmuxBaselineSettle)))
	require.Equal(t, procguard.Busy, guard.Check("panel", churn))

	require.Equal(t, procguard.Free, runtime.settleBaseline("panel", churn, procguard.Busy, true,
		now.Add(3*tmuxBaselineSettle)))
	require.Equal(t, procguard.Free, guard.Check("panel", churn))
}

func TestSettleBaselineNeverActsBeforeTheFirstBaseline(t *testing.T) {
	t.Parallel()
	runtime := &tmuxAttachRuntime{
		processGuard:  &procguard.Guard{},
		baselineReady: map[string]bool{},
		settling:      make(map[string]tmuxBaselineCandidate),
	}
	require.Equal(t, procguard.Uncertain, runtime.settleBaseline("panel",
		[]procguard.Entry{{PID: 10, StartTime: 1}}, procguard.Uncertain, true, time.Now()))
}
