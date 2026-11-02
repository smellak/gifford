//go:build linux

package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	proxysession "github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

const (
	h35OpenCodeVersion = "opencode 1.18.29"
	h35OpenCodeChannel = "  ┃"
)

type h35RecordedInferrer struct {
	mu       sync.Mutex
	recorder *gateway.SQLRecorder
	record   protocol.GlimmerCallRecord
	calls    int
}

func (i *h35RecordedInferrer) Infer(
	ctx context.Context,
	req protocol.InferRequest,
) (protocol.InferResult, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
	record := i.record
	record.AgentID = req.AgentID
	if err := i.recorder.RecordCall(ctx, record, analyst.PromptVersion); err != nil {
		return protocol.InferResult{}, err
	}
	return protocol.InferResult{
		Contract:         req.Contract,
		InferenceProfile: protocol.InferenceProfileBase,
		Label:            string(protocol.AgentPhaseIdleAtPrompt),
		Probabilities:    record.Probabilities,
		Evidence:         []string{"empty prompt"},
		CallRecord:       record,
	}, nil
}

func (i *h35RecordedInferrer) callCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls
}

func h35OpenCodeFrame(output string, permissionStart int) []byte {
	var frame strings.Builder
	frame.WriteString("\x1b[2J\x1b[H")
	put := func(row, column int, text string) {
		_, _ = fmt.Fprintf(&frame, "\x1b[%d;%dH%s", row+1, column+1, text)
	}
	if output != "" {
		put(42, 0, output)
	}
	for row := 43; row < 47; row++ {
		put(row, 2, "┃")
	}
	put(44, 2, "┃  ")
	put(46, 2, "┃  Model")
	put(47, 2, "╹"+strings.Repeat("▀", 115))
	if permissionStart >= 0 {
		permission := []string{
			h35OpenCodeChannel,
			"  ┃  △ Permission required",
			"  ┃  variable action",
			h35OpenCodeChannel,
			h35OpenCodeChannel,
			h35OpenCodeChannel,
			h35OpenCodeChannel,
			"  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm",
			h35OpenCodeChannel,
		}
		for offset, line := range permission {
			put(permissionStart+offset, 0, line)
		}
	}
	put(44, 5, "")
	frame.WriteString("\x1b[?25h\x1b[0m")
	return []byte(frame.String())
}

func h35PermissionFrame() []byte {
	var frame strings.Builder
	frame.WriteString("\x1b[2J\x1b[H")
	permission := []string{
		h35OpenCodeChannel,
		"  ┃  △ Permission required",
		"  ┃  variable action",
		h35OpenCodeChannel,
		h35OpenCodeChannel,
		h35OpenCodeChannel,
		h35OpenCodeChannel,
		"  ┃  Allow once   Allow always   Reject       ctrl+f fullscreen  ⇆ select  enter confirm",
		h35OpenCodeChannel,
	}
	for offset, line := range permission {
		_, _ = fmt.Fprintf(&frame, "\x1b[%d;1H%s", 40+offset, line)
	}
	frame.WriteString("\x1b[1;1H\x1b[?25h\x1b[0m")
	return []byte(frame.String())
}

func h35ObserveEventually(
	t *testing.T,
	s *proxysession.Session,
	match func(protocol.StateObservationReport) bool,
) protocol.StateObservationReport {
	t.Helper()
	var matched protocol.StateObservationReport
	var last protocol.StateObservationReport
	if !assert.Eventually(t, func() bool {
		report, ok := s.Observe()
		last = report
		if ok && match(report) {
			matched = report
			return true
		}
		return false
	}, 5*time.Second, 5*time.Millisecond) {
		t.Fatalf("last observation: screen=%s output=%d guard=%d rows=%q input_guard=%+v",
			last.ScreenState, last.OutputGeneration, last.InputGuardVersion,
			last.ScreenRows, s.InputGuard())
	}
	return matched
}

func h35Contains(rows []string, value string) bool {
	for _, row := range rows {
		if row == value {
			return true
		}
	}
	return false
}

// The residual frame combines the measured OpenCode 1.18.29 permission rows
// and idle layout used by the bisection. It is constructed, not a preserved D
// or #299 capture. The production path must still reach the Analyst once idle.
func TestConstructedResolvedOpenCodePermissionScrollbackReachesStateAnalysis(t *testing.T) {
	fixture := newExecutorFixture(t)
	profileSet, err := profiles.Load()
	require.NoError(t, err)
	profile, err := profileSet.Profile("opencode", "1.18.29")
	require.NoError(t, err)

	outer, terminal, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(terminal, &pty.Winsize{Cols: 120, Rows: 50}))

	harnessDir := t.TempDir()
	initialPath := filepath.Join(harnessDir, "initial.ansi")
	reactionPath := filepath.Join(harnessDir, "reaction.ansi")
	priorPath := filepath.Join(harnessDir, "prior.ansi")
	modalPath := filepath.Join(harnessDir, "modal.ansi")
	resolvedReactionPath := filepath.Join(harnessDir, "resolved-reaction.ansi")
	resolvedPath := filepath.Join(harnessDir, "resolved.ansi")
	reactionReady := filepath.Join(harnessDir, "reaction.ready")
	priorReady := filepath.Join(harnessDir, "prior.ready")
	modalReady := filepath.Join(harnessDir, "modal.ready")
	resolvedReactionReady := filepath.Join(harnessDir, "resolved-reaction.ready")
	resolvedReady := filepath.Join(harnessDir, "resolved.ready")
	require.NoError(t, os.WriteFile(initialPath, h35OpenCodeFrame("initial output", -1), 0o600))
	require.NoError(t, os.WriteFile(reactionPath, h35OpenCodeFrame("reaction output", -1), 0o600))
	require.NoError(t, os.WriteFile(priorPath, h35OpenCodeFrame("settled prior output", -1), 0o600))
	require.NoError(t, os.WriteFile(modalPath, h35PermissionFrame(), 0o600))
	require.NoError(t, os.WriteFile(resolvedReactionPath, h35OpenCodeFrame("resolved reaction output", 9), 0o600))
	require.NoError(t, os.WriteFile(resolvedPath, h35OpenCodeFrame("synthetic output", 9), 0o600))
	script := filepath.Join(harnessDir, "opencode-harness")
	require.NoError(t, os.WriteFile(script, []byte(`#!/bin/sh
set -eu
stty -echo
cat "$1"
IFS= read -r first
while [ ! -f "$7" ]; do sleep 0.01; done
cat "$2"
while [ ! -f "$8" ]; do sleep 0.01; done
cat "$3"
while [ ! -f "$9" ]; do sleep 0.01; done
cat "$4"
IFS= read -r second
while [ ! -f "${10}" ]; do sleep 0.01; done
cat "$5"
while [ ! -f "${11}" ]; do sleep 0.01; done
cat "$6"
while IFS= read -r keep; do :; done
`), 0o700))

	realSession, err := proxysession.NewSession(proxysession.Options{
		Command: []string{
			script, initialPath, reactionPath, priorPath, modalPath,
			resolvedReactionPath, resolvedPath, reactionReady, priorReady,
			modalReady, resolvedReactionReady, resolvedReady,
		},
		In: terminal, Out: terminal, DataDir: t.TempDir(), SessionID: "h35-opencode",
		Profile: profile, Clock: fixture.clock, Sink: &realSessionSink{},
		RecordTo: io.Discard, Notice: io.Discard, HarnessVersion: h35OpenCodeVersion,
		PermissionDialogs: true,
	})
	require.NoError(t, err)
	runCtx, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		_, runErr := realSession.Run(runCtx)
		runDone <- runErr
	}()
	t.Cleanup(func() {
		cancelRun()
		_ = outer.Close()
		_ = terminal.Close()
		select {
		case runErr := <-runDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("real proxy session stopped: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("real proxy session did not stop")
		}
	})

	initial := h35ObserveEventually(t, realSession, func(report protocol.StateObservationReport) bool {
		return report.ScreenState == protocol.StateObservationScreenAvailable &&
			h35Contains(report.ScreenRows, "initial output")
	})
	writeInput := func(line string) {
		t.Helper()
		before := realSession.GuardVersion()
		_, writeErr := outer.Write([]byte(line + "\r"))
		require.NoError(t, writeErr)
		require.Eventually(t, func() bool {
			return realSession.GuardVersion() > before
		}, 5*time.Second, time.Millisecond)
	}
	release := func(ready string) {
		t.Helper()
		fixture.clock.set(fixture.clock.Now().Add(time.Millisecond))
		require.NoError(t, os.WriteFile(ready, []byte("ready\n"), 0o600))
	}
	writeInput("warmup")
	release(reactionReady)
	reaction := h35ObserveEventually(t, realSession, func(report protocol.StateObservationReport) bool {
		return report.OutputGeneration > initial.OutputGeneration &&
			h35Contains(report.ScreenRows, "reaction output") ||
			report.OutputGeneration > initial.OutputGeneration &&
				report.ScreenState == protocol.StateObservationScreenWithheld
	})
	fixture.clock.set(fixture.clock.Now().Add(time.Second))
	require.Equal(t, protocol.DraftStateClosed, realSession.InputGuard().DraftState)
	release(priorReady)
	prior := h35ObserveEventually(t, realSession, func(report protocol.StateObservationReport) bool {
		return report.OutputGeneration > reaction.OutputGeneration &&
			report.ScreenState == protocol.StateObservationScreenAvailable &&
			h35Contains(report.ScreenRows, "settled prior output")
	})
	release(modalReady)
	modal := h35ObserveEventually(t, realSession, func(report protocol.StateObservationReport) bool {
		return report.OutputGeneration > prior.OutputGeneration &&
			report.ScreenState == protocol.StateObservationScreenAvailable &&
			h35Contains(report.ScreenRows, proxysession.PermissionDialogMarker)
	})
	writeInput("resolve")
	release(resolvedReactionReady)
	resolvedReaction := h35ObserveEventually(t, realSession, func(report protocol.StateObservationReport) bool {
		return report.OutputGeneration > modal.OutputGeneration
	})
	fixture.clock.set(fixture.clock.Now().Add(time.Second))
	require.Equal(t, protocol.DraftStateClosed, realSession.InputGuard().DraftState)
	release(resolvedReady)
	h35ObserveEventually(t, realSession, func(report protocol.StateObservationReport) bool {
		return report.OutputGeneration > resolvedReaction.OutputGeneration
	})
	fixture.clock.set(fixture.clock.Now().Add(time.Second))
	final, ok := realSession.Observe()
	require.True(t, ok)
	require.Greater(t, final.OutputGeneration, modal.OutputGeneration)
	require.Zero(t, final.Activity.BytesLast500ms)

	var manifestJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT manifest_json FROM sessions WHERE session_id = ?", executorTestSession).Scan(&manifestJSON))
	var manifest protocol.CapabilityManifest
	require.NoError(t, json.Unmarshal([]byte(manifestJSON), &manifest))
	manifest.Harness = protocol.HarnessOpencode
	manifest.HarnessVersion = h35OpenCodeVersion
	manifestJSONBytes, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE agents SET harness = ? WHERE agent_id = ?`,
		protocol.HarnessOpencode, executorTestAgent)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions SET manifest_json = ? WHERE session_id = ?`,
		string(manifestJSONBytes), executorTestSession)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `DELETE FROM input_guards WHERE session_id = ?`, executorTestSession)
	require.NoError(t, err)
	guard := realSession.InputGuard()
	guard.AgentID, guard.SessionID = executorTestAgent, executorTestSession
	guardStore, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixture.clock)
	require.NoError(t, err)
	written, err := guardStore.WriteInputGuard(t.Context(), guard)
	require.NoError(t, err)
	require.True(t, written)

	final.AgentID, final.SessionID = executorTestAgent, executorTestSession
	final.Harness, final.HarnessVersion = protocol.HarnessOpencode, h35OpenCodeVersion
	require.Equal(t, guard.InputGuardVersion, final.InputGuardVersion)
	require.NoError(t, final.Validate())

	stateProfile, recorded := reviewRefreshAnalysis(t, fixture, "call-h35-permission-scrollback")
	recorded.record.CreatedAt = final.ObservedAt
	recorder, err := gateway.NewSQLRecorder(fixture.store.DB())
	require.NoError(t, err)
	inferrer := &h35RecordedInferrer{recorder: recorder, record: recorded.record}
	redactor, err := redact.New(redact.Options{})
	require.NoError(t, err)
	stateAnalyst, err := analyst.New(inferrer, screenproj.Policies{
		Lines: screenproj.MaxLines, Redactor: redactor, Mask: screenproj.NoMask(),
	})
	require.NoError(t, err)
	producer := newTestStateProducer(t, fixture, stateAnalyst, stateProfile)
	require.NoError(t, producer.Submit(final))
	require.NoError(t, producer.Demand(final.AgentID))
	require.NoError(t, producer.produceOne(t.Context(), final.AgentID))

	var stale int
	var route, callID sql.NullString
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT stale,
		analysis_route, glimmer_call_id FROM state_snapshots WHERE observation_id = ?`,
		final.ObservationID).Scan(&stale, &route, &callID))
	t.Logf("final proxy screen=%s gateway_calls=%d stale=%d route=%q call=%q",
		final.ScreenState, inferrer.callCount(), stale, route.String, callID.String)
	require.Equal(t, 1, inferrer.callCount(), "the idle prompt must reach state analysis")
	require.Zero(t, stale)
	require.Equal(t, string(analyst.RoutePTY), route.String)
	require.Equal(t, "call-h35-permission-scrollback", callID.String)
}
