package procguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// restTree is the synthetic census shape: node launcher, tool binary,
// MCP client child, with or without codex-code-mode-host.
const testCodexComm = "codex"

func restTree(host bool) []Entry {
	tree := []Entry{
		{PID: 100, StartTime: 11, Comm: "node", Program: "node", PPID: 50, Pgrp: 100, State: "S", TPGID: -1},
		{PID: 200, StartTime: 12, Comm: testCodexComm, Program: testCodexComm, PPID: 100, Pgrp: 100, State: "S", TPGID: -1},
		{PID: 300, StartTime: 13, Comm: "mcp-shim", Program: "mcp-shim", PPID: 200, Pgrp: 100, State: "S", TPGID: -1},
	}
	if host {
		tree = append(tree, Entry{PID: 400, StartTime: 14, Comm: codexCodeModeHostProgram, PPID: 200, Pgrp: 100, State: "S", TPGID: -1})
	}
	return tree
}

func TestGuardRestAndWork(t *testing.T) {
	t.Parallel()
	for _, host := range []bool{false, true} {
		var g Guard
		g.Rebaseline("pane", restTree(host))
		require.Equal(t, Free, g.Check("pane", restTree(host)))

		working := append(append([]Entry{}, restTree(host)...),
			Entry{PID: 500, StartTime: 15, Comm: "sleep", Program: "sleep"})
		require.Equal(t, Busy, g.Check("pane", working))
	}
}

func TestGuardAuxiliaryAbsorbedAtNextRest(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", restTree(false))

	aux := append(append([]Entry{}, restTree(false)...),
		Entry{PID: 600, StartTime: 16, Comm: "helper", Program: "helper"})
	require.Equal(t, Busy, g.Check("pane", aux))

	g.Rebaseline("pane", aux)
	require.Equal(t, Free, g.Check("pane", aux))
}

func TestGuardPidReuseIsNew(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", restTree(false))

	reused := append([]Entry{}, restTree(false)...)
	reused[1] = Entry{PID: 200, StartTime: 99, Comm: testCodexComm, Program: testCodexComm}
	require.Equal(t, Busy, g.Check("pane", reused))
}

func TestGuardGoneMemberIsUncertain(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", restTree(true))
	require.Equal(t, Uncertain, g.Check("pane", restTree(false)))
}

func TestGuardUnknownPanelIsUncertain(t *testing.T) {
	t.Parallel()
	var g Guard
	require.Equal(t, Uncertain, g.Check("pane", restTree(false)))
}
