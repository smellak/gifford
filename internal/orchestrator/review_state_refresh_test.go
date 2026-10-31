package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

type reviewRefreshAnalyzer struct {
	inner   *recordedStateAnalyzer
	started chan struct{}
	release chan struct{}
}

func (a *reviewRefreshAnalyzer) Analyze(ctx context.Context, obs analyst.Observation) (analyst.Analysis, error) {
	close(a.started)
	<-a.release
	return a.inner.Analyze(ctx, obs)
}

func TestReviewTimestampRefreshDoesNotStarveAnalysis(t *testing.T) {
	for _, tc := range []struct {
		name  string
		aging bool
	}{
		{name: "transport_time_only"},
		{name: "idle_timer_advances", aging: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExecutorFixture(t)
			profile, inner := reviewRefreshAnalysis(
				t, f, protocol.OpaqueID("call-review-refresh-"+tc.name),
			)
			at := executorTestTime.Add(time.Second)
			a := &reviewRefreshAnalyzer{
				started: make(chan struct{}), release: make(chan struct{}), inner: inner,
			}
			p := newTestStateProducer(t, f, a, profile)
			first := reviewRefreshObservation("review-refresh-1", at)
			f.clock.set(at)
			require.NoError(t, p.Submit(first))
			require.NoError(t, p.Demand(first.AgentID))
			done := make(chan error, 1)
			go func() { done <- p.produceOne(t.Context(), first.AgentID) }()
			<-a.started
			refresh := refreshReviewObservation(first, "review-refresh-2", tc.aging)
			f.clock.set(refresh.ObservedAt)
			require.NoError(t, p.Submit(refresh))
			close(a.release)
			err := <-done
			state, readErr := ReadLatestState(t.Context(), f.store.DB(), first.AgentID,
				first.SessionID, refresh.ObservedAt, &profile)
			t.Logf("produce=%v read=%v phase=%s observation=%s", err, readErr,
				state.Snapshot.AgentPhase, state.ObservationID)
			require.NoError(t, err,
				"a quiet refresh must not discard still-valid analysis of unchanged evidence")
			require.NoError(t, readErr)
			require.Equal(t, protocol.AgentPhaseIdleAtPrompt, state.Snapshot.AgentPhase)
			require.Equal(t, first.ObservationID, state.ObservationID,
				"the inference remains attached to the observation it analyzed")
			require.Equal(t, first.ObservedAt, state.Snapshot.ObservedAt,
				"the refresh must not replace the analyzed context time")
			require.Equal(t, at.Add(stateTTL), state.ExpiresAt,
				"reuse must keep the original inference deadline")
		})
	}
}

func TestStateProducerCompatibleRefreshStaysEligibleAndReusesAnalysis(t *testing.T) {
	for _, tc := range []struct {
		name  string
		aging bool
	}{
		{name: "transport_time_only"},
		{name: "idle_timer_advances", aging: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExecutorFixture(t)
			profile, analyzer := reviewRefreshAnalysis(
				t, f, protocol.OpaqueID("call-refresh-after-"+tc.name),
			)
			p := newTestStateProducer(t, f, analyzer, profile)
			at := executorTestTime.Add(time.Second)
			first := reviewRefreshObservation("refresh-after-1", at)
			f.clock.set(at)
			require.NoError(t, p.Submit(first))
			require.NoError(t, p.Demand(first.AgentID))
			require.NoError(t, p.produceOne(t.Context(), first.AgentID))
			rows := stateRowCount(t, f)

			refresh := refreshReviewObservation(first, "refresh-after-2", tc.aging)
			f.clock.set(refresh.ObservedAt)
			require.NoError(t, p.Submit(refresh))
			tx, err := f.store.DB().BeginTx(t.Context(), nil)
			require.NoError(t, err)
			queued, err := p.ReadLatestForAutomatic(t.Context(), tx, first.AgentID,
				first.SessionID, refresh.ObservedAt, &profile)
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			require.True(t, queued.AutomaticEligible,
				"a compatible queued refresh must not hide an unexpired inference")

			require.NoError(t, p.produceOne(t.Context(), first.AgentID))
			require.Equal(t, 1, analyzer.calls, "a compatible refresh reuses the original call")
			require.Equal(t, rows, stateRowCount(t, f),
				"a semantically identical refresh does not add a snapshot")
			state, err := ReadLatestState(t.Context(), f.store.DB(), first.AgentID,
				first.SessionID, refresh.ObservedAt, &profile)
			require.NoError(t, err)
			require.Equal(t, first.ObservationID, state.ObservationID)
			require.Equal(t, first.ObservedAt.Add(stateTTL), state.ExpiresAt)
		})
	}
}

func reviewRefreshAnalysis(
	t *testing.T,
	f *executorFixture,
	callID protocol.OpaqueID,
) (StateAnalysisProfile, *recordedStateAnalyzer) {
	t.Helper()
	recorder, err := gateway.NewSQLRecorder(f.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	return profile, &recordedStateAnalyzer{recorder: recorder, record: protocol.GlimmerCallRecord{
		CallID: callID, Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
		BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
		Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
		InputSHA256: digest, OutputSHA256: digest,
		Probabilities: map[string]float64{"IDLE_AT_PROMPT": .95, "THINKING": .05},
		Status:        protocol.GlimmerCallStatusOK, CreatedAt: executorTestTime.Add(time.Second),
	}}
}

func reviewRefreshObservation(id protocol.OpaqueID, at time.Time) protocol.StateObservationReport {
	report := stateObservation(id, 2, at)
	report.ScreenState = protocol.StateObservationScreenAvailable
	report.ScreenRows = []string{"ready"}
	report.Cols = 80
	report.Cursor = &protocol.StateObservationCursor{Row: 0, Col: 5}
	report.Activity.SecondsSinceLastOutput = 4
	return report
}

func refreshReviewObservation(
	first protocol.StateObservationReport,
	id protocol.OpaqueID,
	aging bool,
) protocol.StateObservationReport {
	refresh := first
	refresh.ObservationID = id
	refresh.ObservationGeneration++
	refresh.ObservedAt = first.ObservedAt.Add(time.Second)
	if aging {
		refresh.Activity.SecondsSinceLastOutput++
	}
	return refresh
}
