//go:build conformance && linux

package conformance

import (
	"strings"
	"testing"
	"time"
)

const closedIdleEvidence = "guard=CLOSED/IDLE"

func startReadinessTestRun(t *testing.T, variant string) *conformanceRun {
	t.Helper()
	if activeSuite.harness != syntheticHarness {
		t.Skip("readiness harness checks use the synthetic harness")
	}
	run, err := newConformanceRun(t, variant)
	if err != nil {
		t.Fatalf("start readiness run: %v", err)
	}
	if err := waitFor(run.ctx, 15*time.Second, 20*time.Millisecond, func() bool {
		return run.database.QueryRowContext(run.ctx, "SELECT session_id FROM sessions "+
			"WHERE agent_id = ? AND state = 'CONNECTED' AND ended_at IS NULL",
			conformanceRecipient).Scan(&run.activeSession) == nil
	}); err != nil {
		_ = run.Close()
		t.Fatalf("wait for readiness recipient: %v", err)
	}
	if err := run.waitForClosedIdle(); err != nil {
		_ = run.Close()
		t.Fatalf("wait for initial closed idle guard: %v", err)
	}
	return run
}

func closeReadinessTestRun(t *testing.T, run *conformanceRun) {
	t.Helper()
	if err := run.Close(); err != nil {
		t.Errorf("close readiness run: %v", err)
	}
}

func TestReadinessQuietForFullWindow(t *testing.T) {
	run := startReadinessTestRun(t, "")
	defer closeReadinessTestRun(t, run)
	policy := readinessPolicy{window: time.Second, timeout: 3 * time.Second, poll: 20 * time.Millisecond}
	started := time.Now()
	evidence, err := run.waitForReadiness(policy)
	if err != nil {
		t.Fatalf("wait for quiet readiness: %v", err)
	}
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Fatalf("readiness passed after %s, before the one-second window", elapsed)
	}
	for _, want := range []string{
		"readiness=yes", closedIdleEvidence, "harness_processes=1 exact=true", "stable_window=", "/1s",
	} {
		if !strings.Contains(evidence, want) {
			t.Errorf("readiness evidence %q does not contain %q", evidence, want)
		}
	}
}

func TestReadinessContinuousOutputIsNotExecuted(t *testing.T) {
	run := startReadinessTestRun(t, syntheticContinuousOutput)
	defer closeReadinessTestRun(t, run)
	policy := readinessPolicy{
		window: 150 * time.Millisecond, timeout: 400 * time.Millisecond, poll: 20 * time.Millisecond,
	}
	_, err := run.waitForReadiness(policy)
	if err == nil {
		t.Fatal("continuous output unexpectedly reached readiness")
	}
	if status := scenarioErrorStatus(err); status != statusNotExecuted {
		t.Fatalf("continuous output status = %s, want %s", status, statusNotExecuted)
	}
	for _, want := range []string{
		closedIdleEvidence, "harness_processes=1 exact=true", "output=bytes=", "stable_window=",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("timeout evidence %q does not contain %q", err, want)
		}
	}
}

func TestReadinessWaitsForTransientChild(t *testing.T) {
	run := startReadinessTestRun(t, syntheticTransientChild)
	defer closeReadinessTestRun(t, run)
	var child processIdentity
	if err := waitFor(run.ctx, time.Second, 20*time.Millisecond, func() bool {
		processes, exact := exactHarnessProcessTree(run.tmux.panePID)
		if exact || len(processes) < 2 {
			return false
		}
		child = processes[len(processes)-1].identity()
		return true
	}); err != nil {
		t.Fatalf("transient child was not observed: %v", err)
	}
	policy := readinessPolicy{
		window: 200 * time.Millisecond, timeout: 5 * time.Second, poll: 20 * time.Millisecond,
	}
	if _, err := run.waitForReadiness(policy); err != nil {
		t.Fatalf("readiness did not wait through transient child: %v", err)
	}
	if processAlive(child) {
		t.Fatal("readiness passed while the transient child was still alive")
	}
}

func TestReadinessPersistentChildIsNotExecutedWithComm(t *testing.T) {
	run := startReadinessTestRun(t, mutantChild)
	defer closeReadinessTestRun(t, run)
	processes, exact := exactHarnessProcessTree(run.tmux.panePID)
	if exact || len(processes) < 2 {
		t.Fatalf("persistent child process tree = %q, want harness plus child", processTreeEvidence(processes))
	}
	childComm := processes[len(processes)-1].comm
	policy := readinessPolicy{
		window: 100 * time.Millisecond, timeout: 300 * time.Millisecond, poll: 20 * time.Millisecond,
	}
	_, err := run.waitForReadiness(policy)
	if err == nil {
		t.Fatal("persistent child unexpectedly reached readiness")
	}
	if status := scenarioErrorStatus(err); status != statusNotExecuted {
		t.Fatalf("persistent child status = %s, want %s", status, statusNotExecuted)
	}
	for _, want := range []string{
		"harness_processes=2 exact=false", "comm=" + childComm, closedIdleEvidence, "output=bytes=",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("persistent child evidence %q does not contain %q", err, want)
		}
	}
}

func TestReadinessDeadlineIdentifiesGuard(t *testing.T) {
	run := startReadinessTestRun(t, "")
	defer closeReadinessTestRun(t, run)
	if err := run.tmux.sendBytes(run.ctx, []byte("x")); err != nil {
		t.Fatalf("open synthetic draft: %v", err)
	}
	if err := run.waitForGuardState("OPEN", 5*time.Second); err != nil {
		t.Fatalf("synthetic draft did not open: %v", err)
	}
	policy := readinessPolicy{
		window: 100 * time.Millisecond, timeout: 300 * time.Millisecond, poll: 20 * time.Millisecond,
	}
	_, err := run.waitForReadiness(policy)
	if err == nil {
		t.Fatal("open guard unexpectedly reached readiness")
	}
	if !strings.Contains(err.Error(), "guard=OPEN/") {
		t.Fatalf("guard timeout evidence did not identify OPEN: %v", err)
	}
}

func TestSyntheticPreparationSubmitsAndReturnsToReadiness(t *testing.T) {
	run := startReadinessTestRun(t, "")
	defer closeReadinessTestRun(t, run)
	policy := readinessPolicy{
		window: 200 * time.Millisecond, timeout: 3 * time.Second, poll: 20 * time.Millisecond,
	}
	evidence, _, err := run.prepareOpenCodeSession(policy)
	if err != nil {
		t.Fatalf("prepare synthetic harness: %v", err)
	}
	if !run.prepared {
		t.Fatal("successful preparation did not mark the run prepared")
	}
	for _, want := range []string{
		"mode=opencode-ok", "prompt_sent=yes", "submit=profile", "readiness=yes",
	} {
		if !strings.Contains(evidence, want) {
			t.Errorf("preparation evidence %q does not contain %q", evidence, want)
		}
	}
	if !run.tmux.containsOutput(preparePrompt) {
		t.Fatalf("synthetic harness did not receive preparation prompt %q", preparePrompt)
	}
	if err := run.waitForInput("", time.Second); err != nil {
		t.Fatal("profile submit did not leave an empty synthetic input")
	}
}
