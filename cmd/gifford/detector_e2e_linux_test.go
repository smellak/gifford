package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// TestDetectorRouteDeliversWithTheInitConfig is the judge's plug-and-play
// path: `gifford init` writes the config, no orchestration model is
// configured, a measured harness runs under `gifford run`, and a message
// reaches it on the detector alone, recorded under deterministic-pty-v1.
func TestDetectorRouteDeliversWithTheInitConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	run := startDetectorRoute(ctx, t, e2eCodexVersion)
	environment, sender, recipient, hubStderr := run.environment, run.sender, run.recipient, run.hubStderr
	var admitted protocol.AdmissionResult
	sender.callTool(t, "send_message", map[string]any{
		e2eToField: "recipient", e2eSubjectField: "detector",
		e2eBodyField: "detector route", e2eClientIDField: "e2e-detector-route",
	}, &admitted)
	require.Equal(t, protocol.AdmissionStatusAccepted, admitted.Result)
	db := openE2EReadDB(t, environment.database)
	deadline := time.Now().Add(30 * time.Second)
	for len(recipient.deliveredLines(t)) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if len(recipient.deliveredLines(t)) != 1 {
		require.FailNow(t, "no delivery", "decisions: %s\nhub: %s\nrun: %s",
			e2eDecisionGuards(ctx, t, db, admitted.MessageID), hubStderr.String(), recipient.stderr.String())
	}
	require.Contains(t, recipient.deliveredLines(t)[0], "[GIFFORD]")

	require.Eventually(t, func() bool {
		var delivered bool
		err := db.QueryRowContext(ctx, `SELECT state = 'DELIVERED' FROM delivery_queue
			WHERE message_id = ?`, admitted.MessageID).Scan(&delivered)
		return err == nil && delivered
	}, 10*time.Second, 20*time.Millisecond)
	var policy string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT policy_version FROM orchestrator_decisions
		WHERE message_id = ? AND selected_action = 'INJECT_PTY_PROMPT'
		ORDER BY created_at DESC LIMIT 1`, admitted.MessageID).Scan(&policy))
	require.Equal(t, "deterministic-pty-v1", policy)
	var approvals int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM approval_requests
		WHERE object_id = ?`, admitted.MessageID).Scan(&approvals))
	require.Zero(t, approvals, "no human approval on the way")
}

type detectorRouteRun struct {
	environment e2eEnvironment
	sender      *e2eAgent
	recipient   *e2eRecipient
	hubStderr   *lockedBuffer
}

// startDetectorRoute writes the config with `gifford init`, starts the hub
// with it, a sender and the synthetic Codex reporting version under
// gifford run.
func startDetectorRoute(ctx context.Context, t *testing.T, version string) detectorRouteRun {
	t.Helper()

	environment := newE2EEnvironment(t)
	configPath := ""
	for _, entry := range environment.env {
		if value, ok := strings.CutPrefix(entry, "XDG_CONFIG_HOME="); ok {
			configPath = filepath.Join(value, "gifford", "config.yaml")
		}
	}
	require.NotEmpty(t, configPath)
	require.NoError(t, os.Remove(configPath))
	out, code := runGifford(ctx, t, environment.env, "init", "--node-id", "node-a")
	require.Equal(t, 0, code, out)
	raw, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "state_analysis", "init configures no model")

	hubProcess, hubStderr := startE2EHubBinary(ctx, t, environment, e2eGiffordBinary)
	t.Cleanup(func() { _ = hubProcess.terminateAndWait() })
	sender := startE2EAgentBinary(ctx, t, environment.env, "sender", e2eGiffordBinary)
	t.Cleanup(func() { stopE2EAgentProcess(sender) })
	recipient := startE2ECodexRecipient(ctx, t, environment, "recipient", version)
	t.Cleanup(func() {
		_ = recipient.process.terminateAndWait()
		_ = recipient.outer.Close()
	})

	return detectorRouteRun{environment: environment, sender: sender, recipient: recipient, hubStderr: hubStderr}
}

// An unmeasured build of the same harness is never injected on the
// detector: the delivery waits for a human approval.
func TestDetectorRouteNeverInjectsAnUnmeasuredBuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	run := startDetectorRoute(ctx, t, "0.155.0")
	admitted, approval := sendE2EMessageAndWaitApproval(
		ctx, t, run.environment, run.sender, "recipient", "e2e-detector-unmeasured")
	require.Contains(t, approval.RequestNote, "harness build is not measured")
	require.Never(t, func() bool { return len(run.recipient.deliveredLines(t)) != 0 },
		2*time.Second, 50*time.Millisecond)
	db := openE2EReadDB(t, run.environment.database)
	var injected int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM orchestrator_decisions
		WHERE message_id = ? AND selected_action = 'INJECT_PTY_PROMPT'`, admitted.MessageID).Scan(&injected))
	require.Zero(t, injected)
}

// startE2ECodexRecipient runs the synthetic Codex under gifford run with the
// real codex profile.
func startE2ECodexRecipient(
	ctx context.Context,
	t *testing.T,
	environment e2eEnvironment,
	agent protocol.AgentID,
	version string,
) *e2eRecipient {
	t.Helper()
	outer, slave, err := openE2EPTY()
	require.NoError(t, err)
	logPath := filepath.Join(environment.dataDir, "e2e-recipient-"+string(agent)+".log")
	cmd := exec.CommandContext(ctx, e2eGiffordBinary,
		"run", "--agent", string(agent), "--harness", "codex", "--", os.Args[0])
	cmd.Env = append(append([]string{}, environment.env...),
		e2eRecipientEnv+"="+e2eCodexMode, e2eCodexVersionEnv+"="+version,
		e2eRecipientLogEnv+"="+logPath, "TERM=xterm-256color")
	cmd.Stdin = slave
	cmd.Stdout = slave
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	process, err := startE2EProcess(cmd)
	_ = slave.Close()
	if err != nil {
		_ = outer.Close()
	}
	require.NoError(t, err)
	recipient := &e2eRecipient{
		process: process, outer: outer, stderr: stderr, output: &lockedBuffer{}, logPath: logPath,
	}
	go func() { _, _ = io.Copy(recipient.output, outer) }()
	require.Eventually(t, func() bool {
		return strings.Contains(recipient.output.String(), "Ask Codex to do anything")
	}, e2eRPCTimeout, 20*time.Millisecond, "recipient stderr: %s", stderr.String())
	return recipient
}

// e2eDecisionGuards lists a message's decisions with their guards, for a
// failure message.
func e2eDecisionGuards(ctx context.Context, t *testing.T, db *sql.DB, message protocol.ULID) string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT selected_action, execution_status, policy_version,
		hard_guards_json FROM orchestrator_decisions WHERE message_id = ? ORDER BY created_at`, message)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out strings.Builder
	for rows.Next() {
		var action, status, policy, guards string
		require.NoError(t, rows.Scan(&action, &status, &policy, &guards))
		fmt.Fprintf(&out, "\n%s %s %s %s", action, status, policy, guards)
	}
	require.NoError(t, rows.Err())
	return out.String()
}
