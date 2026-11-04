package orchestrator

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon/agentlink"
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

// Any verdict the policy does not accept, or a build it did not measure,
// defers to the human with the reason in the guard detail, and plans
// nothing.
func TestDetectorRouteDefersWhatThePolicyRefuses(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		version string
		change  func(d **agentlink.DetectorEvidence, now time.Time)
		reason  string
	}{
		"unmeasured build": {
			version: executorTestHarnessVersion, change: func(**agentlink.DetectorEvidence, time.Time) {},
			reason: "harness build is not measured",
		},
		"no verdict": {
			change: func(d **agentlink.DetectorEvidence, _ time.Time) { *d = nil }, reason: "no detector verdict",
		},
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
			version := tc.version
			if version == "" {
				version = detectorTestMeasuredVersion
			}
			useHarnessVersion(t, f, version)
			d := eligibleDetector(f.clock.Now())
			tc.change(&d, f.clock.Now())
			f.setDetector(d)
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
		})
	}
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
