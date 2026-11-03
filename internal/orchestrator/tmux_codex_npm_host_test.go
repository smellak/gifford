package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	tmuxCodexNPMProgram      = "node"
	tmuxCodexNPMOtherProgram = "other"
)

func newTmuxCodexNPMFixture(t *testing.T) (*executorFixture, *tmuxTransportStub, *tmuxRegistryStub) {
	t.Helper()
	fixture, transport, registry := newTmuxExecutorFixture(t)
	registry.instance.HarnessArgv0 = tmuxCodexNPMProgram
	registry.instance.HarnessArgv1 = tmuxTestProgram
	transport.processes[1].Comm = tmuxCodexNPMProgram
	transport.processes[1].Program = tmuxCodexNPMProgram
	transport.processes[1].Argv0 = "/usr/bin/node"
	transport.processes[1].Exe = "/usr/bin/node"
	transport.processes = append(transport.processes, procguard.Entry{
		PID: 102, PPID: 101, StartTime: 203,
		Comm: tmuxTestProgram, Program: tmuxTestProgram,
		Argv0: tmuxCodexTestExe, Exe: tmuxCodexTestExe,
		Pgrp: 101, TPGID: 101, State: "S",
	})
	return fixture, transport, registry
}

func tmuxCodexNPMHostEntry() procguard.Entry {
	host := tmuxCodexHostEntry()
	host.PPID = 102
	return host
}

func TestTmuxCodexNPMHostBornAfterBaselineDelivers(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxCodexNPMFixture(t)
	candidate := fixture.seedDelivery(t, 106)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	transport.processes = append(transport.processes, tmuxCodexNPMHostEntry())
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

func TestTmuxCodexNPMHostAtBaselineMayDisappear(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxCodexNPMFixture(t)
	transport.processes = append(transport.processes, tmuxCodexNPMHostEntry())
	candidate := fixture.seedDelivery(t, 107)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes)
	transport.processes = transport.processes[:3]
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes, "the helper must also be excluded when warming the baseline")
}

func TestTmuxCodexNPMHostUnreadableExeDelivers(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxCodexNPMFixture(t)
	for i := range transport.processes {
		transport.processes[i].Exe = ""
	}
	candidate := fixture.seedDelivery(t, 108)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	host := tmuxCodexNPMHostEntry()
	host.Exe = ""
	transport.processes = append(transport.processes, host)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Equal(t, 1, transport.writes)
}

func TestTmuxCodexNPMHostExceptionNeverHidesNewWork(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*tmuxTransportStub){
		"same comm and argv but other executable": func(transport *tmuxTransportStub) { transport.processes[3].Exe = tmuxCodexTestOtherHostPath },
		"unreadable exe but another path": func(transport *tmuxTransportStub) {
			transport.processes[3].Exe = ""
			transport.processes[3].Argv0 = tmuxCodexTestOtherHostPath
		},
		"same comm but another argv": func(transport *tmuxTransportStub) { transport.processes[3].Argv0 = tmuxCodexTestOtherHostPath },
		"relative host argv": func(transport *tmuxTransportStub) {
			transport.processes[3].Argv0 = tmuxCodexTestHostProgram
			transport.processes[3].Exe = ""
		},
		"host comm differs":           func(transport *tmuxTransportStub) { transport.processes[3].Comm = tmuxCodexNPMOtherProgram },
		"host under node":             func(transport *tmuxTransportStub) { transport.processes[3].PPID = 101 },
		"host under unrelated parent": func(transport *tmuxTransportStub) { transport.processes[3].PPID = 100 },
		"new child of host": func(transport *tmuxTransportStub) {
			transport.processes = append(transport.processes, procguard.Entry{PID: 104, PPID: 103, StartTime: 205, Program: tmuxCodexTestTool})
		},
		"new child of native": func(transport *tmuxTransportStub) {
			transport.processes = append(transport.processes, procguard.Entry{PID: 104, PPID: 102, StartTime: 205, Program: tmuxCodexTestTool})
		},
		"new child of node": func(transport *tmuxTransportStub) {
			transport.processes = append(transport.processes, procguard.Entry{PID: 104, PPID: 101, StartTime: 205, Program: tmuxCodexTestTool})
		},
		"two natives": func(transport *tmuxTransportStub) {
			second := transport.processes[2]
			second.PID, second.StartTime = 104, 205
			transport.processes = append(transport.processes, second)
		},
		"no native": func(transport *tmuxTransportStub) {
			transport.processes = append(transport.processes[:2], transport.processes[3:]...)
		},
		"relative native argv":         func(transport *tmuxTransportStub) { transport.processes[2].Argv0 = tmuxTestProgram },
		"native argv basename differs": func(transport *tmuxTransportStub) { transport.processes[2].Argv0 = "/opt/example/codex/bin/other" },
		"native comm differs":          func(transport *tmuxTransportStub) { transport.processes[2].Comm = tmuxCodexNPMOtherProgram },
		"native under another parent":  func(transport *tmuxTransportStub) { transport.processes[2].PPID = 100 },
		"native restarted":             func(transport *tmuxTransportStub) { transport.processes[2].StartTime++ },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, transport, _ := newTmuxCodexNPMFixture(t)
			candidate := fixture.seedDelivery(t, 109)
			require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
			transport.processes = append(transport.processes, tmuxCodexNPMHostEntry())
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

func TestTmuxCodexNPMHostUnprovenNativeCannotWarmBaseline(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*tmuxTransportStub, *tmuxRegistryStub){
		"no native": func(transport *tmuxTransportStub, _ *tmuxRegistryStub) { transport.processes = transport.processes[:2] },
		"two natives": func(transport *tmuxTransportStub, _ *tmuxRegistryStub) {
			second := transport.processes[2]
			second.PID, second.StartTime = 104, 205
			transport.processes = append(transport.processes, second)
		},
		"relative native argv": func(transport *tmuxTransportStub, _ *tmuxRegistryStub) {
			transport.processes[2].Argv0 = tmuxTestProgram
		},
		"wrong registered script": func(_ *tmuxTransportStub, registry *tmuxRegistryStub) {
			registry.instance.HarnessArgv1 = tmuxCodexNPMOtherProgram
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, transport, registry := newTmuxCodexNPMFixture(t)
			change(transport, registry)
			candidate := fixture.seedDelivery(t, 110)
			for range 3 {
				require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
				require.False(t, fixture.executor.tmux.baselineIsReady(string(registry.instance.ID)))
			}
			require.Zero(t, transport.writes)
			var attempts int
			require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
				`SELECT count(*) FROM delivery_attempts WHERE message_id=?`, candidate.MessageID).Scan(&attempts))
			require.Zero(t, attempts)
		})
	}
}

func TestTmuxCodexNPMHostInvalidNativeAfterBaselineBlocksWithoutHelper(t *testing.T) {
	t.Parallel()
	fixture, transport, _ := newTmuxCodexNPMFixture(t)
	candidate := fixture.seedDelivery(t, 111)
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	transport.processes[2].Argv0 = tmuxTestProgram
	require.NoError(t, fixture.executor.EvaluateTmuxAttach(t.Context(), candidate))
	require.Zero(t, transport.writes, "unchanged PID/start census cannot override an unproven native path")
	require.Equal(t, protocol.GuardOutcomeFail, guardOutcome(
		latestGuards(t, fixture.store.DB(), candidate.MessageID), protocol.GuardNameNoNewChildOrTool))
}
