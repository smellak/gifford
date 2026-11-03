package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
)

const tmuxCodexTestExe = "/opt/example/codex/bin/codex"

const (
	tmuxCodexTestTool          = "sleep"
	tmuxCodexTestHostPath      = "/opt/example/codex/bin/codex-code-mode-host"
	tmuxCodexTestHostProgram   = "codex-code-mode-host"
	tmuxCodexTestOtherHostPath = "/tmp/other/codex-code-mode-host"
)

func tmuxCodexHostEntry() procguard.Entry {
	return procguard.Entry{
		PID: 103, PPID: 101, StartTime: 204,
		Comm: "codex-code-mode", Program: tmuxCodexTestHostProgram,
		Argv0: tmuxCodexTestHostPath, Exe: tmuxCodexTestHostPath, Pgrp: 101, TPGID: 101, State: "S",
	}
}

func TestTmuxCodexHostBornAfterBaselineDelivers(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	transport.processes[1].Exe = tmuxCodexTestExe
	transport.processes[1].Argv0 = tmuxCodexTestExe
	registry.instance.HarnessArgv0 = tmuxTestProgram
	candidate := fixture.seedDelivery(t, 101)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	transport.processes = append(transport.processes, tmuxCodexHostEntry())
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes)
	var state daemon.AttemptState
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		`SELECT state FROM delivery_attempts WHERE message_id=?`, candidate.MessageID).Scan(&state))
	require.Equal(t, daemon.AttemptCommitted, state)
	var queueState string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		`SELECT state FROM delivery_queue WHERE message_id=?`, candidate.MessageID).Scan(&queueState))
	require.Equal(t, queueStateDelivered, queueState)
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(
		latestGuards(t, fixture.store.DB(), candidate.MessageID), protocol.GuardNameNoNewChildOrTool))
}

func TestTmuxCodexHostAtBaselineMayDisappear(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	transport.processes[1].Exe = tmuxCodexTestExe
	transport.processes[1].Argv0 = tmuxCodexTestExe
	registry.instance.HarnessArgv0 = tmuxTestProgram
	transport.processes = append(transport.processes, tmuxCodexHostEntry())
	candidate := fixture.seedDelivery(t, 102)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	transport.processes = transport.processes[:2]
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes, "the same helper projection must be used at baseline and at check")
}

func TestTmuxCodexHostExceptionNeverHidesNewWork(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*tmuxTransportStub){
		"same comm and argv but other executable": func(transport *tmuxTransportStub) {
			transport.processes[2].Exe = tmuxCodexTestOtherHostPath
		},
		"helper exe unreadable but argv in another directory": func(transport *tmuxTransportStub) {
			transport.processes[2].Exe = ""
			transport.processes[2].Argv0 = tmuxCodexTestOtherHostPath
		},
		"same comm but another argv": func(transport *tmuxTransportStub) {
			transport.processes[2].Argv0 = tmuxCodexTestOtherHostPath
		},
		"helper argv is relative": func(transport *tmuxTransportStub) {
			transport.processes[2].Argv0 = tmuxCodexTestHostProgram
			transport.processes[2].Exe = ""
		},
		"helper comm differs": func(transport *tmuxTransportStub) {
			transport.processes[2].Comm = "other"
		},
		"harness launched by relative argv": func(transport *tmuxTransportStub) {
			transport.processes[1].Argv0 = tmuxTestProgram
		},
		"helper under another parent": func(transport *tmuxTransportStub) {
			transport.processes[2].PPID = 100
		},
		"new child of helper": func(transport *tmuxTransportStub) {
			transport.processes = append(transport.processes, procguard.Entry{
				PID: 104, PPID: 103, StartTime: 205, Comm: tmuxCodexTestTool, Program: tmuxCodexTestTool, Exe: "/usr/bin/sleep",
			})
		},
		"other new child of harness": func(transport *tmuxTransportStub) {
			transport.processes = append(transport.processes, procguard.Entry{
				PID: 104, PPID: 101, StartTime: 205, Comm: tmuxCodexTestTool, Program: tmuxCodexTestTool, Exe: "/usr/bin/sleep",
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, transport, registry := newTmuxExecutorFixture(t)
			transport.processes[1].Exe = tmuxCodexTestExe
			transport.processes[1].Argv0 = tmuxCodexTestExe
			registry.instance.HarnessArgv0 = tmuxTestProgram
			candidate := fixture.seedDelivery(t, 103)
			require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
			transport.processes = append(transport.processes, tmuxCodexHostEntry())
			change(transport)
			require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
			require.Zero(t, transport.writes)
			var attempts int
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT count(*) FROM delivery_attempts WHERE message_id=?`, candidate.MessageID).Scan(&attempts))
			require.Zero(t, attempts)
			var queueState string
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT state FROM delivery_queue WHERE message_id=?`, candidate.MessageID).Scan(&queueState))
			require.Equal(t, queueStateQueued, queueState)
			require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(
				latestGuards(t, fixture.store.DB(), candidate.MessageID), protocol.GuardNameNoNewChildOrTool))
		})
	}
}

func TestTmuxCodexHostExceptionIsCodexOnly(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	registry.instance.Harness = protocol.HarnessClaudeCode
	fixture.executor.tmux.detectorTable[detect.HarnessClaude] = fixture.executor.tmux.detectorTable[detect.HarnessCodex]
	transport.processes[1].Exe = tmuxCodexTestExe
	transport.processes[1].Argv0 = tmuxCodexTestExe
	registry.instance.HarnessArgv0 = tmuxTestProgram
	candidate := fixture.seedDelivery(t, 104)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	transport.processes = append(transport.processes, tmuxCodexHostEntry())
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(
		latestGuards(t, fixture.store.DB(), candidate.MessageID), protocol.GuardNameNoNewChildOrTool))
}

func TestTmuxCodexHostUnreadableExeDelivers(t *testing.T) {
	t.Parallel()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	registry.instance.HarnessArgv0 = tmuxTestProgram
	transport.processes[1].Argv0 = tmuxCodexTestExe
	transport.processes[1].Exe = ""
	candidate := fixture.seedDelivery(t, 105)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	host := tmuxCodexHostEntry()
	host.Exe = ""
	transport.processes = append(transport.processes, host)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes)
	require.Equal(t, protocol.GuardOutcomePass, guardOutcome(
		latestGuards(t, fixture.store.DB(), candidate.MessageID), protocol.GuardNameNoNewChildOrTool))
}
