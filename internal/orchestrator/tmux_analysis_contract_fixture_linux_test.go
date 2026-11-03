//go:build linux

package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
	"github.com/smellak/gifford/internal/tmuxinstance"
	"github.com/smellak/gifford/internal/triage"
)

const (
	tmuxContractIdleScreen  = "completed fixture task\n$ "
	tmuxContractError       = "error"
	tmuxContractTimeout     = "timeout"
	tmuxContractUnavailable = "unavailable"
	tmuxContractInvalid     = "invalid"
)

// The real transport, detector, executor, worker, Analyst and StateProducer
// run against synthetic pane bytes. Only the operating-system commands and
// inference endpoint are doubles; no live tmux server or model is contacted.
// Tests using this fixture must not run in parallel: the command double is
// selected through PATH, with t.Setenv restoring the caller's environment.
const tmuxContractCommand = `#!/bin/sh
set -eu
command=
literal=no
last=
for arg do
  case "$arg" in
    capture-pane|display-message|list-clients|send-keys) command="$arg" ;;
    -l) literal=yes ;;
  esac
  last="$arg"
done
case "$command" in
  capture-pane)
    if test -f "$GIFFORD_TMUX_CONTRACT_DIR/capture-error"; then exit 1; fi
    cat "$GIFFORD_TMUX_CONTRACT_DIR/screen"
    ;;
  display-message) printf '%%7\t100\t80\t24\n' ;;
  list-clients) : ;;
  send-keys)
    printf '%s\n' "$literal" >> "$GIFFORD_TMUX_CONTRACT_DIR/writes"
    if test "$literal" = yes; then
      printf '$ %s' "$last" > "$GIFFORD_TMUX_CONTRACT_DIR/screen"
    elif test "$last" = Enter; then
      printf '$ ' > "$GIFFORD_TMUX_CONTRACT_DIR/screen"
    else
      exit 2
    fi
    ;;
  *) exit 2 ;;
esac
`

type tmuxContractLock struct{}

func (tmuxContractLock) Lock(context.Context, tmuxattach.Target) (tmuxattach.Lock, error) {
	return tmuxContractLock{}, nil
}

func (tmuxContractLock) Unlock() error { return nil }

type tmuxContractModel struct {
	fixture      *executorFixture
	recorder     *gateway.SQLRecorder
	requests     []protocol.InferRequest
	failure      string
	beforeChoose func()
}

func (m *tmuxContractModel) Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error) {
	m.requests = append(m.requests, req)
	if req.Contract == protocol.ModelContractChooseOrchestrationAction {
		if m.beforeChoose != nil {
			m.beforeChoose()
		}
		result := chooseAnswer(protocol.OrchestratorActionInjectPTYPrompt)
		result.CallRecord.CallID = protocol.OpaqueID(fmt.Sprintf("call-tmux-choose-%d", len(m.requests)))
		result.CallRecord.AgentID = req.AgentID
		result.CallRecord.CreatedAt = m.fixture.clock.Now()
		if err := m.recorder.RecordCall(ctx, result.CallRecord, "choose-contract-test"); err != nil {
			return protocol.InferResult{}, err
		}
		return result, nil
	}
	if req.Contract != protocol.ModelContractAnalyzeAgentState {
		return protocol.InferResult{}, errors.New("unexpected inference contract")
	}
	switch m.failure {
	case tmuxContractError:
		return protocol.InferResult{}, errors.New("synthetic analyst failure")
	case tmuxContractTimeout:
		return protocol.InferResult{}, context.DeadlineExceeded
	case tmuxContractUnavailable:
		return protocol.InferResult{}, analyst.ErrModelUnavailable
	case tmuxContractInvalid:
		return protocol.InferResult{Label: "not-a-phase"}, nil
	}
	probabilities := map[string]float64{
		string(protocol.AgentPhaseIdleAtPrompt): .95,
		string(protocol.AgentPhaseThinking):     .05,
	}
	record := callRecord(req.Contract,
		protocol.OpaqueID(fmt.Sprintf("call-tmux-analyze-%d", len(m.requests))), probabilities)
	record.AgentID = req.AgentID
	record.CreatedAt = m.fixture.clock.Now()
	record.PromptVersion = analyst.PromptVersion
	if err := m.recorder.RecordCall(ctx, record, analyst.PromptVersion); err != nil {
		return protocol.InferResult{}, err
	}
	return protocol.InferResult{
		Contract: req.Contract, InferenceProfile: protocol.InferenceProfileBase,
		Label: string(protocol.AgentPhaseIdleAtPrompt), Probabilities: probabilities,
		CallRecord: record,
	}, nil
}

func (m *tmuxContractModel) calls(contract protocol.ModelContract) []protocol.InferRequest {
	var found []protocol.InferRequest
	for _, request := range m.requests {
		if request.Contract == contract {
			found = append(found, request)
		}
	}
	return found
}

type tmuxContractFixture struct {
	*executorFixture
	worker       *daemon.Worker
	producer     *StateProducer
	model        *tmuxContractModel
	policies     screenproj.Policies
	profile      StateAnalysisProfile
	dir          string
	initialState protocol.ULID
	states       *tmuxContractStates
}

// Preserve the real producer while recording every report at its entry,
// including reports it later discards or projects to an empty screen.
type tmuxContractStates struct {
	*StateProducer
	reports []protocol.StateObservationReport
}

func (s *tmuxContractStates) Submit(report protocol.StateObservationReport) error {
	s.reports = append(s.reports, cloneObservation(report))
	return s.StateProducer.Submit(report)
}

func newTmuxContractFixture(t *testing.T, analysis bool) *tmuxContractFixture {
	t.Helper()
	base := newExecutorFixture(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sudo"), []byte(tmuxContractCommand), 0o700))
	t.Setenv("GIFFORD_TMUX_CONTRACT_DIR", dir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	f := &tmuxContractFixture{executorFixture: base, dir: dir}
	f.setScreen(t, tmuxContractIdleScreen)
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: executorTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: executorTestHarnessVersion,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderTmuxAttach,
		CaptureMode:        protocol.CaptureModeTmuxAttach, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	require.NoError(t, manifest.Validate())
	raw, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = base.store.DB().ExecContext(t.Context(), `UPDATE sessions SET capture_mode = ?, manifest_json = ?
		WHERE session_id = ?`, protocol.CaptureModeTmuxAttach, string(raw), executorTestSession)
	require.NoError(t, err)
	_, err = base.store.DB().ExecContext(t.Context(), "DELETE FROM input_guards WHERE session_id = ?", executorTestSession)
	require.NoError(t, err)
	require.NoError(t, base.store.DB().QueryRowContext(t.Context(),
		"SELECT state_id FROM state_snapshots ORDER BY state_version DESC LIMIT 1").Scan(&f.initialState))
	// The initial snapshot is real, but cannot authorize this later delivery.
	base.clock.set(executorTestTime.Add(time.Minute))
	transport, err := tmuxattach.New(tmuxattach.Options{Locker: tmuxContractLock{}})
	require.NoError(t, err)
	registry := &tmuxRegistryStub{instance: tmuxinstance.Instance{
		ID: tmuxTestInstance, AgentID: executorTestAgent,
		OwnerUser: "agent_owner", OwnerUID: 1001, Socket: filepath.Join(dir, "synthetic.sock"),
		PaneID: tmuxTestPane, PanePID: 100, Harness: protocol.HarnessCodex,
		HarnessPID: 101, HarnessStart: 202, AgentUID: 303, AgentGID: 404,
		SessionID: executorTestSession, State: tmuxinstance.StateActive,
	}}
	guards, err := agentlink.NewInputGuardStore(base.store.DB(), base.clock)
	require.NoError(t, err)
	redactor, err := redact.New(redact.Options{})
	require.NoError(t, err)
	f.policies = screenproj.Policies{Lines: screenproj.MaxLines, Redactor: redactor, Mask: screenproj.NoMask()}
	tmuxOptions := TmuxAttachOptions{
		Registry: registry, Transport: transport, Sessions: &tmuxSessionStub{},
		InputGuards: guards, ProcessGuard: &procguard.Guard{},
		SampleTree: func(int) ([]procguard.Entry, error) {
			return []procguard.Entry{
				{PID: 100, StartTime: 201, Program: tmuxTestLauncher, Comm: tmuxTestLauncher, Pgrp: 100, State: "S", TPGID: 101},
				{PID: 101, PPID: 100, StartTime: 202, Program: tmuxTestProgram, Comm: tmuxTestProgram, Pgrp: 101, State: "S", TPGID: 101},
			}, nil
		},
		DetectorTable: detect.Table{detect.HarnessCodex: {
			InputExpr: tmuxTestInputExpr, CLIBinaries: []string{tmuxTestProgram}, BusyMarks: []string{"esc to interrupt"},
		}},
		HumanActivityWindow: 15 * time.Second,
	}
	// Keep the behavioral canaries compilable before and after the projection
	// option is added. Its absence is not itself an assertion: the old route
	// must fail on missing analysis, not on a new field or method name.
	if projection := reflect.ValueOf(&tmuxOptions).Elem().FieldByName("Projection"); projection.IsValid() {
		require.True(t, projection.CanSet())
		require.Equal(t, reflect.TypeFor[screenproj.Policies](), projection.Type())
		projection.Set(reflect.ValueOf(f.policies))
	}
	opts := ExecutorOptions{
		DB: base.store.DB(), Channel: base.executor.channel, Approvals: base.controller,
		Links: base.links, Clock: base.clock, TmuxAttach: &tmuxOptions,
	}
	recorder, err := gateway.NewSQLRecorder(base.store.DB())
	require.NoError(t, err)
	f.model = &tmuxContractModel{fixture: base, recorder: recorder}
	reader, err := analyst.New(f.model, f.policies)
	require.NoError(t, err)
	f.profile = choiceProfile
	f.profile.Temperature = 1
	f.producer = newTestStateProducer(t, base, reader, f.profile)
	f.states = &tmuxContractStates{StateProducer: f.producer}
	if analysis {
		opts.Model, opts.States, opts.StateProfile = f.model, f.states, &f.profile
		opts.Triage = &choiceTriager{
			db: base.store.DB(), label: protocol.TriageLabelNormal, outcome: triage.OutcomeClassified,
		}
	}
	base.executor, err = NewExecutor(opts)
	require.NoError(t, err)
	f.worker, err = daemon.NewWorker(daemon.WorkerOptions{
		DB: base.store.DB(), Processor: base.executor, Approvals: base.executor,
		Prepared: base.executor, Proxies: base.links, Clock: base.clock, Concurrency: 1,
	})
	require.NoError(t, err)
	return f
}

func (f *tmuxContractFixture) setScreen(t *testing.T, screen string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(f.dir, "screen"), []byte(screen), 0o600))
}

// Step the actual daemon once, then its sole state producer. No test submits
// an observation or demands an analysis on behalf of the tmux executor.
func (f *tmuxContractFixture) cycle(t *testing.T) error {
	t.Helper()
	_, err := f.worker.RunOnce(t.Context())
	for _, agent := range f.producer.dueAgents() {
		require.NoError(t, f.producer.produceOne(t.Context(), agent))
	}
	f.clock.set(f.clock.Now().Add(time.Second))
	return err
}

func (f *tmuxContractFixture) cycles(t *testing.T, count int) {
	t.Helper()
	for range count {
		require.NoError(t, f.cycle(t))
	}
	t.Logf("cycles=%d ANALYZE=%d CHOOSE=%d attempts=%d COMMIT_SENT=%d writes=%d", count,
		len(f.model.calls(protocol.ModelContractAnalyzeAgentState)),
		len(f.model.calls(protocol.ModelContractChooseOrchestrationAction)),
		countRows(t, f.store.DB(), "SELECT count(*) FROM delivery_attempts"),
		countRows(t, f.store.DB(), "SELECT count(*) FROM delivery_attempts WHERE commit_sent_at IS NOT NULL"),
		f.writes(t))
}

func (f *tmuxContractFixture) writes(t *testing.T) int {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.dir, "writes"))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	require.NoError(t, err)
	return strings.Count(string(raw), "\n")
}

func (f *tmuxContractFixture) requireNoAttempt(t *testing.T) {
	t.Helper()
	require.Zero(t, countRows(t, f.store.DB(), "SELECT count(*) FROM delivery_attempts"), "no attempt without current analysis")
	require.Zero(t, countRows(t, f.store.DB(), "SELECT count(*) FROM delivery_attempts WHERE commit_sent_at IS NOT NULL"), "COMMIT_SENT is durable even after the attempt changes state")
	require.Zero(t, f.writes(t), "neither a notice nor Enter may be sent")
}
