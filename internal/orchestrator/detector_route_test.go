package orchestrator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/protocol"
)

// detectorTestMeasuredVersion is a codex build the embedded profiles
// measured; executorTestHarnessVersion is not one.
const detectorTestMeasuredVersion = "0.154.0"

// useHarnessVersion rewrites the fixture session's manifest to version.
func useHarnessVersion(t *testing.T, f *executorFixture, version string) {
	t.Helper()
	db := f.store.DB()
	var raw string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT manifest_json FROM sessions WHERE session_id = ?", executorTestSession).Scan(&raw))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(raw), &manifest))
	manifest.HarnessVersion = version
	encoded, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "UPDATE sessions SET manifest_json = ? WHERE session_id = ?",
		string(encoded), executorTestSession)
	require.NoError(t, err)
}

// eligibleDetector is a verdict DeterministicPTYPolicy accepts at now: at
// rest on guard version 3, received a second ago, the same for six seconds.
func eligibleDetector(now time.Time) *agentlink.DetectorEvidence {
	return &agentlink.DetectorEvidence{
		Verdict: protocol.StateObservationDetector{
			Certain: true, IsCLI: true,
			ScreenDigest: protocol.Digest("sha256:" + strings.Repeat("a", 64)),
		},
		ObservationGeneration: 7, InputGuardVersion: 3,
		ReceivedAt: now.Add(-time.Second), QuietSince: now.Add(-6 * time.Second),
	}
}

func (f *executorFixture) setDetector(d *agentlink.DetectorEvidence) {
	f.links.mu.Lock()
	defer f.links.mu.Unlock()
	f.links.snapshot.Detector = d
}

// Without a model, a measured harness at rest for the quiet window is
// injected on the detector alone: INJECT is chosen without any call, the
// decision names DeterministicPTYPolicy and its p_idle_threshold passes.
func TestDetectorRouteInjectsWithoutAModel(t *testing.T) {
	t.Parallel()
	f := newExecutorFixture(t)
	require.False(t, f.executor.choosing())
	useHarnessVersion(t, f, detectorTestMeasuredVersion)
	f.setDetector(eligibleDetector(f.clock.Now()))
	candidate := f.seedDelivery(t, 1)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, choiceInject, decision.selected)
	require.Equal(t, []protocol.OrchestratorAction{choiceInject, choiceDefer}, decision.allowed)
	require.Equal(t, DeterministicPTYPolicy, decision.policy)
	require.Empty(t, decision.callID)
	require.Empty(t, decision.model)
	require.Empty(t, decision.probabilities)
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(decision.guards, protocol.GuardNamePIdleThreshold))
	// The fixture has no link client: the plan is made, then fails before
	// any Proxy byte and returns the row to the queue.
	require.Equal(t, protocol.ExecutionStatusFailed, decision.status)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
	require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
}

// A build the policy did not measure, or a Proxy that sends no verdict,
// defers to the human with the reason in the guard detail, and plans
// nothing: nothing it waits on can change that.
func TestDetectorRouteDefersWhatCannotClear(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		version string
		verdict bool
		reason  string
	}{
		"unmeasured build": {version: executorTestHarnessVersion, verdict: true, reason: reasonNotMeasured},
		"no verdict":       {version: detectorTestMeasuredVersion, reason: reasonNoVerdict},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newExecutorFixture(t)
			useHarnessVersion(t, f, tc.version)
			if tc.verdict {
				f.setDetector(eligibleDetector(f.clock.Now()))
			}
			candidate := f.seedDelivery(t, 2)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			decision := lastDecision(t, f.store.DB(), candidate.MessageID)
			require.Equal(t, []protocol.OrchestratorAction{choiceDefer}, decision.allowed)
			require.Equal(t, choiceDefer, decision.selected)
			require.Equal(t, defaultPolicyVersion, decision.policy)
			require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(decision.guards, protocol.GuardNamePIdleThreshold))
			require.Equal(t,
				"no orchestration model is available; deterministic-pty-v1 is not eligible: "+tc.reason,
				guardDetail(decision.guards, protocol.GuardNamePIdleThreshold))
			require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
			require.Len(t, pendingNotes(t, f.store.DB(), candidate.MessageID), 1)
			_, waiting := f.executor.DetectorWait(executorTestAgent)
			require.False(t, waiting)
		})
	}
}

// Any other refusal can clear by itself: the pass leaves no decision, no
// approval and no attempt, names its reason for the operator, and the next
// pass decides again.
func TestDetectorRouteWaitsOutWhatCanClear(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		change func(d **agentlink.DetectorEvidence, now time.Time)
		reason string
	}{
		"another guard version": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) { (*d).InputGuardVersion = 2 },
			reason: "detector verdict is for another guard version",
		},
		"stale verdict": {
			change: func(d **agentlink.DetectorEvidence, now time.Time) {
				(*d).ReceivedAt = now.Add(-detectorFreshness - time.Millisecond)
			},
			reason: "detector verdict is not fresh",
		},
		"uncertain": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) { (*d).Verdict.Certain = false },
			reason: "detector is not certain",
		},
		"harness not in front": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) { (*d).Verdict.IsCLI = false },
			reason: "harness is not in front",
		},
		"harness busy": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) { (*d).Verdict.Busy = true },
			reason: "harness is busy",
		},
		"agent asked a question": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) {
				(*d).Verdict.Busy, (*d).Verdict.Detail = true, detect.DetailQuestionVeto
			},
			reason: reasonQuestion,
		},
		"input box holds text": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) { (*d).Verdict.BoxBusy = true },
			reason: "input box holds text",
		},
		"quiet window not over": {
			change: func(d **agentlink.DetectorEvidence, now time.Time) {
				(*d).QuietSince = now.Add(-detectorQuietWindow + time.Millisecond)
			},
			reason: "screen is not quiet yet",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newExecutorFixture(t)
			useHarnessVersion(t, f, detectorTestMeasuredVersion)
			d := eligibleDetector(f.clock.Now())
			tc.change(&d, f.clock.Now())
			f.setDetector(d)
			candidate := f.seedDelivery(t, 2)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Empty(t, decisionsFor(t, f.store.DB(), candidate.MessageID))
			require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
			require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
			require.Equal(t, queueQueued, queueStateOf(t, f.store.DB(), candidate))
			note, waiting := f.executor.DetectorWait(executorTestAgent)
			require.True(t, waiting)
			require.Equal(t, "p_idle_threshold: no orchestration model is available; "+
				"deterministic-pty-v1 is not eligible: "+tc.reason, note)

			// Once the verdict clears, the next pass delivers.
			f.setDetector(eligibleDetector(f.clock.Now()))
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			require.Equal(t, choiceInject, lastDecision(t, f.store.DB(), candidate.MessageID).selected)
			_, waiting = f.executor.DetectorWait(executorTestAgent)
			require.False(t, waiting)
		})
	}
}

// An open human draft is waited out too: it closes by itself.
func TestDetectorRouteWaitsOutAnOpenDraft(t *testing.T) {
	t.Parallel()
	f := newExecutorFixture(t)
	useHarnessVersion(t, f, detectorTestMeasuredVersion)
	d := eligibleDetector(f.clock.Now())
	d.Verdict.BoxBusy = true
	f.setDetector(d)
	f.links.mu.Lock()
	f.links.snapshot.LastReport.DraftState = protocol.DraftStateOpen
	f.links.mu.Unlock()
	candidate := f.seedDelivery(t, 6)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	require.Empty(t, decisionsFor(t, f.store.DB(), candidate.MessageID))
	require.Empty(t, pendingNotes(t, f.store.DB(), candidate.MessageID))
	note, waiting := f.executor.DetectorWait(executorTestAgent)
	require.True(t, waiting)
	require.Equal(t, "human_draft_closed: human draft is not CLOSED", note)
}

// A wired model owns the decision: the detector verdict never bypasses it.
func TestDetectorRouteNeverRunsWithTheModelWired(t *testing.T) {
	t.Parallel()
	f := newChoiceFixture(t)
	useHarnessVersion(t, f.executorFixture, detectorTestMeasuredVersion)
	f.setDetector(eligibleDetector(f.clock.Now()))
	candidate := f.seedDelivery(t, 3)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Len(t, f.model.calls(choiceChoose), 1)
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, defaultPolicyVersion, decision.policy)
	require.Equal(t, string(choiceCallID), decision.callID)
}
