//go:build conformance && linux

package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	scenarioS0 = "S0"
	scenarioS1 = "S1"
	scenarioS2 = "S2"

	pendingNotice    = "[GIFFORD] 1 pending message"
	humanDraft       = "borrador conformidad"
	draftClosed      = "CLOSED"
	referenceVersion = "1.0.0"
)

type conformanceRun struct {
	config      *suiteConfig
	environment privateEnvironment
	ctx         context.Context
	cancel      context.CancelFunc
	hub         *managedProcess
	tmux        *tmuxSession
	sender      *senderAgent
	database    *sql.DB
	closed      bool
}

func newConformanceRun(t *testing.T, mutant string) (*conformanceRun, error) {
	t.Helper()
	environment, err := newPrivateEnvironment(activeSuite)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	run := &conformanceRun{config: activeSuite, environment: environment, ctx: ctx, cancel: cancel}
	fail := func(cause error) (*conformanceRun, error) {
		return nil, errors.Join(cause, run.Close())
	}
	run.hub, err = startHub(ctx, activeSuite, environment)
	if err != nil {
		return fail(err)
	}
	wrapper, err := writeHarnessWrapper(activeSuite, environment, mutant)
	if err != nil {
		return fail(err)
	}
	launcher, err := writeRunLauncher(activeSuite, environment, wrapper, mutant)
	if err != nil {
		return fail(err)
	}
	run.tmux, err = startPrivateTMUX(ctx, environment, activeSuite.cwd, launcher)
	if err != nil {
		return fail(err)
	}
	run.database, err = openReadOnlyDatabase(ctx, environment.database)
	if err != nil {
		return fail(err)
	}
	return run, nil
}

func (run *conformanceRun) Close() error {
	if run == nil || run.closed {
		return nil
	}
	run.closed = true
	tracked := make(map[int]bool)
	trackProcessTree := func(process *managedProcess) {
		if process == nil || process.cmd == nil || process.cmd.Process == nil {
			return
		}
		pid := process.cmd.Process.Pid
		tracked[pid] = true
		for _, child := range descendants(pid) {
			tracked[child.pid] = true
		}
	}
	trackProcessTree(run.hub)
	if run.sender != nil {
		trackProcessTree(run.sender.process)
	}
	if run.tmux != nil && run.tmux.panePID > 0 {
		tracked[run.tmux.panePID] = true
		for _, child := range descendants(run.tmux.panePID) {
			tracked[child.pid] = true
		}
	}

	senderErr := run.sender.stop()
	tmuxErr := run.tmux.close()
	hubErr := run.hub.terminate()
	var databaseErr error
	if run.database != nil {
		databaseErr = run.database.Close()
	}
	run.cancel()
	processErr := waitPIDsGone(tracked, 5*time.Second)
	return errors.Join(senderErr, tmuxErr, hubErr, databaseErr, processErr)
}

func TestConformance(t *testing.T) {
	run, err := newConformanceRun(t, "")
	if err != nil {
		recordRemainingFailures(t, 0, fmt.Errorf("start conformance run: %w", err))
		t.Errorf("start conformance run: %v", err)
		return
	}
	scenarios := []struct {
		name string
		run  func() (string, error)
	}{
		{name: scenarioS0, run: run.scenarioS0},
		{name: scenarioS1, run: run.scenarioS1},
		{name: scenarioS2, run: run.scenarioS2},
	}
	for index, scenario := range scenarios {
		evidence, scenarioErr := scenario.run()
		if scenarioErr != nil || index == len(scenarios)-1 {
			scenarioErr = errors.Join(scenarioErr, run.Close())
		}
		if scenarioErr != nil {
			if err := activeSuite.report.Record(activeSuite.harness, activeSuite.version,
				scenario.name, "FAIL", scenarioErr.Error(), activeSuite.root); err != nil {
				t.Errorf("record %s failure: %v", scenario.name, err)
			}
			t.Errorf("%s failed: %v", scenario.name, scenarioErr)
			recordRemainingFailures(t, index+1,
				fmt.Errorf("not run after %s failed", scenario.name))
			return
		}
		if err := activeSuite.report.Record(activeSuite.harness, activeSuite.version,
			scenario.name, "PASS", evidence, ""); err != nil {
			t.Fatalf("record %s result: %v", scenario.name, err)
		}
	}
}

func recordRemainingFailures(t *testing.T, start int, cause error) {
	t.Helper()
	scenarios := []string{scenarioS0, scenarioS1, scenarioS2}
	for _, scenario := range scenarios[start:] {
		if err := activeSuite.report.Record(activeSuite.harness, activeSuite.version,
			scenario, "FAIL", cause.Error(), activeSuite.root); err != nil {
			t.Errorf("record %s failure: %v", scenario, err)
		}
	}
}

func TestSyntheticMutantsAreRejected(t *testing.T) {
	if activeSuite.harness != syntheticHarness {
		t.Skip("synthetic mutation checks only apply to the reference harness")
	}
	tests := []struct {
		name     string
		mutant   string
		scenario func(*conformanceRun) (string, error)
		want     string
		needsS0  bool
	}{
		{
			name: "unmeasured profile warning", mutant: mutantProfileWarning,
			scenario: (*conformanceRun).scenarioS0, want: "unmeasured profile warning",
		},
		{
			name: "extra harness child", mutant: mutantChild,
			scenario: (*conformanceRun).scenarioS0, want: "process tree",
		},
		{
			name: "ignored submit", mutant: mutantIgnoreSubmit,
			scenario: (*conformanceRun).scenarioS1, want: "notice was not above an empty input line", needsS0: true,
		},
		{
			name: "hidden draft", mutant: mutantNoEcho,
			scenario: (*conformanceRun).scenarioS2, want: "draft did not become visible", needsS0: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			run, err := newConformanceRun(t, test.mutant)
			if err != nil {
				t.Fatalf("start mutant run: %v", err)
			}
			if test.needsS0 {
				if _, err := run.scenarioS0(); err != nil {
					_ = run.Close()
					t.Fatalf("mutant failed S0 before its target: %v", err)
				}
			}
			_, scenarioErr := test.scenario(run)
			cleanupErr := run.Close()
			if cleanupErr != nil {
				t.Fatalf("clean mutant run: %v", cleanupErr)
			}
			if scenarioErr == nil {
				t.Fatalf("mutant %s did not fail its scenario", test.mutant)
			}
			if !strings.Contains(scenarioErr.Error(), test.want) {
				t.Fatalf("mutant %s failed for the wrong reason: %v", test.mutant, scenarioErr)
			}
		})
	}
}

func TestExactVersionRange(t *testing.T) {
	tests := []struct {
		name         string
		versionRange string
		version      string
		want         bool
	}{
		{name: "bare exact", versionRange: "1.4.13-R6996.1", version: "1.4.13-R6996.1", want: true},
		{name: "explicit exact", versionRange: "= 1.0.0", version: referenceVersion, want: true},
		{name: "wildcard", versionRange: "*", version: referenceVersion},
		{name: "bounded range", versionRange: ">=1.0.0,<2.0.0", version: referenceVersion},
		{name: "different exact", versionRange: "1.0.1", version: referenceVersion},
		{name: "missing observed", versionRange: referenceVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := exactVersionRange(test.versionRange, test.version); got != test.want {
				t.Fatalf("exactVersionRange(%q, %q) = %t, want %t",
					test.versionRange, test.version, got, test.want)
			}
		})
	}
}

func (run *conformanceRun) scenarioS0() (string, error) {
	var warning bool
	err := waitFor(run.ctx, 60*time.Second, 50*time.Millisecond, func() bool {
		if run.tmux.containsOutput(profileWarning) {
			warning = true
			return true
		}
		var connected int
		queryErr := run.database.QueryRowContext(run.ctx, `SELECT count(*) FROM sessions
			WHERE agent_id = ? AND state = 'CONNECTED' AND capture_mode = 'pty_proxy'
			AND deliverable = 1 AND ended_at IS NULL`, conformanceRecipient).Scan(&connected)
		return queryErr == nil && connected == 1
	})
	if err != nil {
		return "", errors.New("S0 recipient did not connect")
	}
	if warning || run.tmux.containsOutput(profileWarning) {
		return "", errors.New("S0 found an unmeasured profile warning")
	}
	var manifestJSON string
	if err := run.database.QueryRowContext(run.ctx, `SELECT manifest_json FROM sessions
		WHERE agent_id = ? AND state = 'CONNECTED' AND ended_at IS NULL`, conformanceRecipient).
		Scan(&manifestJSON); err != nil {
		return "", errors.New("S0 could not read the connected harness manifest")
	}
	var manifest protocol.CapabilityManifest
	if err := json.Unmarshal([]byte(manifestJSON), &manifest); err != nil {
		return "", errors.New("S0 connected harness manifest was invalid")
	}
	observedVersion := profiles.ParseVersion(manifest.HarnessVersion)
	exactMatch := exactVersionRange(run.config.profile.VersionRange, observedVersion)
	if observedVersion == "" || !exactMatch ||
		observedVersion != run.config.version || string(manifest.Harness) != run.config.profileHarness {
		return "", fmt.Errorf("S0 observed version does not match the exact measured range: observed=%s exact_match=%t",
			observedVersion, exactMatch)
	}
	var draftState string
	if err := waitFor(run.ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		return run.database.QueryRowContext(run.ctx, `SELECT draft_state FROM input_guards
			WHERE agent_id = ?`, conformanceRecipient).Scan(&draftState) == nil && draftState == draftClosed
	}); err != nil {
		return "", errors.New("S0 input guard did not reach CLOSED")
	}
	processes := descendants(run.tmux.panePID)
	if len(processes) != 1 || processes[0].ppid != run.tmux.panePID {
		return "", fmt.Errorf("S0 process tree must contain only the harness: %s", processTreeEvidence(processes))
	}
	return "guard=CLOSED; profile_warning=no; version_range=exact; harness_processes=1", nil
}

func (run *conformanceRun) scenarioS1() (string, error) {
	if err := run.waitForClosedIdle(); err != nil {
		return "", err
	}
	admitted, approval, err := run.sendMessageAndWaitApproval("conformance-s1")
	if err != nil {
		return "", err
	}
	if err := run.runCLI("ctl", "approve", string(approval.ApprovalID), "--deliver-now"); err != nil {
		return "", errors.New("S1 approve --deliver-now failed")
	}
	var queueState, attemptState string
	if err := waitFor(run.ctx, 20*time.Second, 50*time.Millisecond, func() bool {
		queryErr := run.database.QueryRowContext(run.ctx, `SELECT dq.state, da.state
			FROM delivery_queue AS dq JOIN delivery_attempts AS da
			ON da.message_id = dq.message_id AND da.agent_id = dq.agent_id
			AND da.delivery_generation = dq.delivery_generation
			WHERE dq.message_id = ? AND dq.agent_id = ?
			ORDER BY da.created_at DESC LIMIT 1`, admitted.MessageID, conformanceRecipient).
			Scan(&queueState, &attemptState)
		return queryErr == nil && queueState == "DELIVERED" && attemptState == "COMMITTED"
	}); err != nil {
		if draft, ok := run.currentInput(); ok && strings.Contains(draft, pendingNotice) {
			return "", errors.New("S1 notice was not above an empty input line")
		}
		return "", errors.New("S1 delivery did not reach DELIVERED/COMMITTED")
	}
	if err := run.waitForNoticeAboveEmptyInput(20 * time.Second); err != nil {
		return "", fmt.Errorf("S1 notice was not above an empty input line: %w", err)
	}
	return "queue=DELIVERED; attempt=COMMITTED; notice_above_input=yes; input_empty=yes", nil
}

func (run *conformanceRun) scenarioS2() (string, error) {
	if err := run.waitForClosedIdle(); err != nil {
		return "", err
	}
	if err := run.tmux.sendBytes(run.ctx, []byte(humanDraft)); err != nil {
		return "", errors.New("S2 could not type the human draft")
	}
	var draftState string
	if err := waitFor(run.ctx, 12*time.Second, 50*time.Millisecond, func() bool {
		return run.database.QueryRowContext(run.ctx, `SELECT draft_state FROM input_guards
			WHERE agent_id = ?`, conformanceRecipient).Scan(&draftState) == nil && draftState == "OPEN"
	}); err != nil {
		return "", errors.New("S2 draft did not reach OPEN")
	}
	if err := run.waitForInput(humanDraft, 5*time.Second); err != nil {
		return "", errors.New("S2 draft did not become visible in the input line")
	}
	admitted, approval, err := run.sendMessageAndWaitApproval("conformance-s2")
	if err != nil {
		return "", err
	}
	if err := run.runCLI("ctl", "approve", string(approval.ApprovalID), "--deliver-now"); err == nil {
		return "", errors.New("S2 first approve unexpectedly delivered with an open draft")
	}

	var approvalState, queueState, selected, guards string
	if err := run.database.QueryRowContext(run.ctx,
		`SELECT state FROM approval_requests WHERE approval_id = ?`, approval.ApprovalID).
		Scan(&approvalState); err != nil {
		return "", errors.New("S2 could not read the pending approval")
	}
	if err := run.database.QueryRowContext(run.ctx, `SELECT state FROM delivery_queue
		WHERE message_id = ? AND agent_id = ?`, admitted.MessageID, conformanceRecipient).
		Scan(&queueState); err != nil {
		return "", errors.New("S2 could not read the queued delivery")
	}
	if err := run.database.QueryRowContext(run.ctx, `SELECT selected_action, hard_guards_json
		FROM orchestrator_decisions WHERE message_id = ? AND agent_id = ?
		ORDER BY rowid DESC LIMIT 1`, admitted.MessageID, conformanceRecipient).
		Scan(&selected, &guards); err != nil {
		return "", errors.New("S2 could not read the defer decision")
	}
	var attempts int
	if err := run.database.QueryRowContext(run.ctx, `SELECT count(*) FROM delivery_attempts
		WHERE message_id = ? AND agent_id = ?`, admitted.MessageID, conformanceRecipient).
		Scan(&attempts); err != nil {
		return "", errors.New("S2 could not count delivery attempts")
	}
	if approvalState != "PENDING" || queueState != "QUEUED" || selected != "DEFER" ||
		!strings.Contains(guards, "human draft is not CLOSED") || attempts != 0 {
		return "", fmt.Errorf("S2 wrong deferred state: approval=%s queue=%s decision=%s attempts=%d reason_match=%t",
			approvalState, queueState, selected, attempts,
			strings.Contains(guards, "human draft is not CLOSED"))
	}
	if err := run.waitForInput(humanDraft, 2*time.Second); err != nil {
		return "", errors.New("S2 human draft changed after the deferred approval")
	}

	if err := run.tmux.sendBytes(run.ctx, []byte(run.config.profile.ClearLine)); err != nil {
		return "", errors.New("S2 could not send the measured clear_line key")
	}
	if err := waitFor(run.ctx, 15*time.Second, 50*time.Millisecond, func() bool {
		return run.database.QueryRowContext(run.ctx, `SELECT draft_state FROM input_guards
			WHERE agent_id = ?`, conformanceRecipient).Scan(&draftState) == nil && draftState == draftClosed
	}); err != nil {
		return "", errors.New("S2 guard did not return to CLOSED after clear_line")
	}
	if err := run.waitForInput("", 5*time.Second); err != nil {
		return "", errors.New("S2 input line was not empty after clear_line")
	}
	if err := run.runCLI("ctl", "approve", string(approval.ApprovalID), "--deliver-now"); err != nil {
		var secondAction, secondGuards string
		activeGuard := false
		if queryErr := run.database.QueryRowContext(run.ctx, `SELECT selected_action, hard_guards_json
			FROM orchestrator_decisions WHERE message_id = ? AND agent_id = ?
			ORDER BY rowid DESC LIMIT 1`, admitted.MessageID, conformanceRecipient).
			Scan(&secondAction, &secondGuards); queryErr == nil {
			activeGuard = secondAction == "DEFER" && strings.Contains(secondGuards, "human activity is ACTIVE")
		}
		return "", fmt.Errorf("S2 second approve failed after PENDING/QUEUED/DEFER, guard=CLOSED and attempts=0: active_guard=%t",
			activeGuard)
	}
	if err := waitFor(run.ctx, 20*time.Second, 50*time.Millisecond, func() bool {
		return run.database.QueryRowContext(run.ctx, `SELECT state FROM delivery_queue
			WHERE message_id = ? AND agent_id = ?`, admitted.MessageID, conformanceRecipient).
			Scan(&queueState) == nil && queueState == "DELIVERED"
	}); err != nil {
		return "", errors.New("S2 cleared delivery did not reach DELIVERED")
	}
	var attemptState string
	if err := run.database.QueryRowContext(run.ctx, `SELECT count(*), COALESCE(max(state), '')
		FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`, admitted.MessageID, conformanceRecipient).
		Scan(&attempts, &attemptState); err != nil {
		return "", errors.New("S2 could not read the committed attempt")
	}
	if attempts != 1 || attemptState != "COMMITTED" {
		return "", fmt.Errorf("S2 expected one COMMITTED attempt, got count=%d state=%s", attempts, attemptState)
	}
	return "first=PENDING/QUEUED/DEFER; reason=human_draft_not_closed; attempts_before=0; draft_intact=yes; second=DELIVERED/COMMITTED; attempts_after=1", nil
}

func (run *conformanceRun) waitForClosedIdle() error {
	var draftState, activityState string
	if err := waitFor(run.ctx, 15*time.Second, 50*time.Millisecond, func() bool {
		return run.database.QueryRowContext(run.ctx, `SELECT draft_state, activity_state FROM input_guards
			WHERE agent_id = ?`, conformanceRecipient).Scan(&draftState, &activityState) == nil &&
			draftState == draftClosed && activityState == "IDLE"
	}); err != nil {
		return errors.New("recipient was not CLOSED and IDLE")
	}
	return nil
}

func (run *conformanceRun) startSender() error {
	if run.sender != nil {
		return nil
	}
	sender, err := startSenderAgent(run.ctx, run.config, run.environment)
	if err != nil {
		return err
	}
	run.sender = sender
	return nil
}

func (run *conformanceRun) sendMessageAndWaitApproval(
	clientID string,
) (protocol.AdmissionResult, protocol.ApprovalRequest, error) {
	if err := run.startSender(); err != nil {
		return protocol.AdmissionResult{}, protocol.ApprovalRequest{}, fmt.Errorf("start synthetic sender: %w", err)
	}
	var admitted protocol.AdmissionResult
	if err := run.sender.callTool(run.ctx, "send_message", map[string]any{
		"to": conformanceRecipient, "subject": "conformance",
		"body": "PTY conformance delivery", "client_msg_id": clientID,
	}, &admitted); err != nil {
		return admitted, protocol.ApprovalRequest{}, fmt.Errorf("send conformance message: %w", err)
	}
	if admitted.Result != protocol.AdmissionStatusAccepted || !admitted.MessageID.Valid() {
		return admitted, protocol.ApprovalRequest{}, errors.New("conformance message was not accepted")
	}
	var found protocol.ApprovalRequest
	if err := waitFor(run.ctx, 20*time.Second, 50*time.Millisecond, func() bool {
		var pending protocol.ApprovalList
		if err := run.readCLIJSON(&pending, "ctl", "approvals", "--pending"); err != nil {
			return false
		}
		for _, approval := range pending.Approvals {
			if approval.ObjectID == string(admitted.MessageID) && approval.AgentID == conformanceRecipient {
				found = approval
				return true
			}
		}
		return false
	}); err != nil {
		return admitted, found, errors.New("worker did not open the delivery approval")
	}
	return admitted, found, nil
}

func (run *conformanceRun) readCLIJSON(result any, arguments ...string) error {
	cmd := exec.CommandContext(run.ctx, run.config.gifford, arguments...)
	cmd.Env = run.environment.env
	cmd.Stderr = io.Discard
	output, err := cmd.Output()
	if err != nil {
		return err
	}
	return json.Unmarshal(output, result)
}

func (run *conformanceRun) runCLI(arguments ...string) error {
	cmd := exec.CommandContext(run.ctx, run.config.gifford, arguments...)
	cmd.Env = run.environment.env
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func (run *conformanceRun) waitForNoticeAboveEmptyInput(timeout time.Duration) error {
	return waitFor(run.ctx, timeout, 50*time.Millisecond, func() bool {
		screen, cursor, err := run.captureInput()
		if err != nil || cursor < 0 || cursor >= len(screen) {
			return false
		}
		draft, ok := run.config.profile.Draft(screen[cursor])
		if !ok || !profileInputEmpty(run.config.profile, draft) {
			return false
		}
		for _, line := range screen[:cursor] {
			if strings.Contains(line, pendingNotice) {
				return true
			}
		}
		return false
	})
}

func (run *conformanceRun) waitForInput(expected string, timeout time.Duration) error {
	return waitFor(run.ctx, timeout, 50*time.Millisecond, func() bool {
		screen, cursor, err := run.captureInput()
		if err != nil || cursor < 0 || cursor >= len(screen) {
			return false
		}
		draft, ok := run.config.profile.Draft(screen[cursor])
		if !ok {
			return false
		}
		if expected == "" {
			return profileInputEmpty(run.config.profile, draft)
		}
		return draft == expected
	})
}

func (run *conformanceRun) captureInput() ([]string, int, error) {
	screen, err := run.tmux.capture(run.ctx)
	if err != nil {
		return nil, 0, err
	}
	cursor, err := run.tmux.cursorY(run.ctx)
	if err != nil {
		return nil, 0, err
	}
	lines := strings.Split(strings.ReplaceAll(screen, "\r", ""), "\n")
	return lines, cursor, nil
}

func (run *conformanceRun) currentInput() (string, bool) {
	screen, cursor, err := run.captureInput()
	if err != nil || cursor < 0 || cursor >= len(screen) {
		return "", false
	}
	return run.config.profile.Draft(screen[cursor])
}

func profileInputEmpty(profile profiles.Profile, draft string) bool {
	return draft == "" || profile.InputPlaceholder != "" && draft == profile.InputPlaceholder
}

func exactVersionRange(versionRange, version string) bool {
	rangeValue := strings.TrimSpace(versionRange)
	if strings.HasPrefix(rangeValue, "=") {
		rangeValue = strings.TrimSpace(strings.TrimPrefix(rangeValue, "="))
	}
	return version != "" && !strings.Contains(rangeValue, ",") && rangeValue == version
}
