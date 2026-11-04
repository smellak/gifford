package orchestrator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// settleFixture is the unwired executor on a measured build whose harness
// has one more process below it, an MCP server for instance, with an
// eligible detector verdict.
func settleFixture(t *testing.T) *executorFixture {
	t.Helper()
	f := newExecutorFixture(t)
	useHarnessVersion(t, f, detectorTestMeasuredVersion)
	f.processes.mu.Lock()
	harness := f.processes.tree.Descendants[0].Token
	f.processes.tree.Descendants = append(f.processes.tree.Descendants,
		testProcessNode(ProcessToken{PID: 150, StartTime: 250}, harness, "/opt/gifford-test/mcp-server"))
	f.processes.mu.Unlock()
	f.setDetector(eligibleDetector(f.clock.Now()))
	return f
}

// refreshDetector keeps the verdict received a second ago and quiet since
// quietSince.
func (f *executorFixture) refreshDetector(quietSince time.Time) {
	d := eligibleDetector(f.clock.Now())
	d.QuietSince = quietSince
	f.setDetector(d)
}

func processDetail(t *testing.T, f *executorFixture, candidate protocol.ULID) (protocol.GuardOutcome, string) {
	t.Helper()
	decision := lastDecision(t, f.store.DB(), candidate)
	return guardOutcome(decision.guards, protocol.GuardNameNoNewChildOrTool),
		guardDetail(decision.guards, protocol.GuardNameNoNewChildOrTool)
}

// A stable descendant set is trusted after descendantSettle on a screen at
// rest the whole time, and the delivery then goes ahead.
func TestDescendantsSettleOnAQuietScreen(t *testing.T) {
	t.Parallel()
	f := settleFixture(t)
	quiet := f.clock.Now().Add(-6 * time.Second)
	candidate := f.seedDelivery(t, 1)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	outcome, detail := processDetail(t, f, candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomeFail, outcome)
	require.Equal(t, untrustedProcessDetail+"; settling", detail)

	f.advance(descendantSettle - time.Second)
	f.refreshDetector(quiet)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	outcome, _ = processDetail(t, f, candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomeFail, outcome, "not yet a minute")

	f.advance(time.Second)
	f.refreshDetector(quiet)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(decision.guards, protocol.GuardNameNoNewChildOrTool))
	require.Equal(t, choiceInject, decision.selected)
	require.Equal(t, 1, attemptCount(t, f.store.DB(), candidate.MessageID))
}

// The count starts over on any break: a new process in the set, a screen
// that changed after the set was first seen, a human key in the window, or
// a screen not at rest. Without a detector verdict nothing settles.
func TestDescendantSettleStartsOverOnAnyBreak(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		change func(f *executorFixture, start time.Time)
		detail string
	}{
		"set changed": {
			change: func(f *executorFixture, start time.Time) {
				f.processes.mu.Lock()
				f.processes.tree.Descendants[1].Token.StartTime++
				f.processes.mu.Unlock()
				f.refreshDetector(start.Add(-6 * time.Second))
			},
			detail: untrustedProcessDetail + "; settling",
		},
		"screen changed": {
			change: func(f *executorFixture, start time.Time) { f.refreshDetector(start.Add(time.Second)) },
			detail: untrustedProcessDetail + "; settling: screen changed",
		},
		"human key": {
			change: func(f *executorFixture, start time.Time) {
				f.refreshDetector(start.Add(-6 * time.Second))
				f.links.mu.Lock()
				f.links.snapshot.LastReport.LastHumanKeyAt = start.Add(time.Second)
				f.links.mu.Unlock()
			},
			detail: untrustedProcessDetail + "; settling: human key",
		},
		"busy screen": {
			change: func(f *executorFixture, start time.Time) {
				f.refreshDetector(start.Add(-6 * time.Second))
				f.links.mu.Lock()
				f.links.snapshot.Detector.Verdict.Busy = true
				f.links.mu.Unlock()
			},
			detail: untrustedProcessDetail + "; not settling: screen not at rest",
		},
		"no verdict": {
			change: func(f *executorFixture, _ time.Time) { f.setDetector(nil) },
			detail: untrustedProcessDetail + "; not settling: screen not at rest",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := settleFixture(t)
			start := f.clock.Now()
			candidate := f.seedDelivery(t, 2)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			f.advance(descendantSettle)
			tc.change(f, start)
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
			outcome, detail := processDetail(t, f, candidate.MessageID)
			require.Equal(t, protocol.GuardOutcomeFail, outcome)
			require.Equal(t, tc.detail, detail)
			require.Zero(t, attemptCount(t, f.store.DB(), candidate.MessageID))
		})
	}
}

// The settle never relaxes the direct child rule: a second direct child of
// gifford run holds, and the detail names both executables.
func TestDescendantSettleKeepsOneDirectChild(t *testing.T) {
	t.Parallel()
	f := settleFixture(t)
	f.processes.mu.Lock()
	f.processes.tree.Descendants[1].Parent = f.processes.tree.Root
	f.processes.mu.Unlock()
	candidate := f.seedDelivery(t, 3)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	f.advance(descendantSettle)
	f.refreshDetector(f.clock.Now().Add(-2 * descendantSettle))
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))
	outcome, detail := processDetail(t, f, candidate.MessageID)
	require.Equal(t, protocol.GuardOutcomeFail, outcome)
	require.Equal(t, "expected one direct harness child, found 2 (codex, mcp-server)", detail)
}

// Once the harness is bound, a second direct child still holds however long
// it stands, and the detail names both executables.
func TestDescendantSettleKeepsOneDirectChildAfterBinding(t *testing.T) {
	t.Parallel()
	f := newExecutorFixture(t)
	useHarnessVersion(t, f, detectorTestMeasuredVersion)
	f.setDetector(eligibleDetector(f.clock.Now()))
	first := f.seedDelivery(t, 4)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), first))
	require.Equal(t, 1, attemptCount(t, f.store.DB(), first.MessageID), "the harness is bound")

	f.processes.mu.Lock()
	f.processes.tree.Descendants = append(f.processes.tree.Descendants,
		testProcessNode(ProcessToken{PID: 160, StartTime: 260}, f.processes.tree.Root, "/opt/gifford-test/helper"))
	f.processes.mu.Unlock()
	second := f.seedDelivery(t, 5)
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), second))
	f.advance(descendantSettle)
	f.refreshDetector(f.clock.Now().Add(-2 * descendantSettle))
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), second))
	outcome, detail := processDetail(t, f, second.MessageID)
	require.Equal(t, protocol.GuardOutcomeFail, outcome)
	require.Equal(t, untrustedProcessDetail+"; direct children: codex, helper", detail)
	require.Zero(t, attemptCount(t, f.store.DB(), second.MessageID))
}
