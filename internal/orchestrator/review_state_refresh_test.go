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
	f := newExecutorFixture(t)
	recorder, err := gateway.NewSQLRecorder(f.store.DB())
	require.NoError(t, err)
	digest := protocol.SHA256Hex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	profile := StateAnalysisProfile{
		Revision: stateTestProfileRevision, SHA256: digest,
		CalibrationRevision: stateTestCalibrationRevision, CalibrationSHA256: digest,
		Temperature: 1, IdleThreshold: .9,
	}
	at := executorTestTime.Add(time.Second)
	a := &reviewRefreshAnalyzer{
		started: make(chan struct{}), release: make(chan struct{}),
		inner: &recordedStateAnalyzer{recorder: recorder, record: protocol.GlimmerCallRecord{
			CallID: "call-review-refresh", Contract: protocol.ModelContractAnalyzeAgentState,
			AgentID: executorTestAgent, InferenceProfile: protocol.InferenceProfileBase,
			BaseModelRepo: stateTestBaseModelRepo, BaseModelRevision: stateTestBaseModelRevision,
			Runtime: stateTestRuntime, RedactionPolicyVersion: stateTestRedactionPolicy,
			InputSHA256: digest, OutputSHA256: digest,
			Probabilities: map[string]float64{"IDLE_AT_PROMPT": .95, "THINKING": .05},
			Status:        protocol.GlimmerCallStatusOK, CreatedAt: at,
		}},
	}
	p := newTestStateProducer(t, f, a, profile)
	first := stateObservation("review-refresh-1", 2, at)
	first.ScreenState = protocol.StateObservationScreenAvailable
	first.ScreenRows = []string{"ready"}
	first.Cols = 80
	first.Cursor = &protocol.StateObservationCursor{Row: 0, Col: 5}
	f.clock.set(at)
	require.NoError(t, p.Submit(first))
	require.NoError(t, p.Demand(first.AgentID))
	done := make(chan error, 1)
	go func() { done <- p.produceOne(t.Context(), first.AgentID) }()
	<-a.started
	// Only transport identity and observation time move. No output, native
	// event, human guard, screen, activity or configuration changes.
	refresh := first
	refresh.ObservationID = "review-refresh-2"
	refresh.ObservationGeneration++
	refresh.ObservedAt = at.Add(time.Second)
	f.clock.set(refresh.ObservedAt)
	require.NoError(t, p.Submit(refresh))
	close(a.release)
	err = <-done
	state, readErr := ReadLatestState(t.Context(), f.store.DB(), first.AgentID, first.SessionID, refresh.ObservedAt, &profile)
	t.Logf("produce=%v read=%v phase=%s observation=%s", err, readErr, state.Snapshot.AgentPhase, state.ObservationID)
	require.NoError(t, err, "a timer-only refresh must not discard still-valid analysis of unchanged evidence")
	require.NoError(t, readErr)
	require.Equal(t, protocol.AgentPhaseIdleAtPrompt, state.Snapshot.AgentPhase)
	require.Equal(t, first.ObservationID, state.ObservationID,
		"the inference remains attached to the observation it analyzed")
	require.Equal(t, first.ObservedAt, state.Snapshot.ObservedAt,
		"the refresh must not replace the analyzed context time")
	require.Equal(t, at.Add(stateTTL), state.ExpiresAt, "reuse must keep the original inference deadline")
}
