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
		tree = append(tree, Entry{PID: 400, StartTime: 14, Comm: "codex-code-mode-host", PPID: 200, Pgrp: 100, State: "S", TPGID: -1})
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

func TestProgramNameKeepsOnlyArgvZeroBasename(t *testing.T) {
	t.Parallel()
	require.Equal(t, testCodexComm, programName([]byte("/usr/bin/codex\x00--secret\x00abc123\x00")))
	require.Equal(t, "node", programName([]byte("node\x00/usr/bin/codex\x00")))
	require.Empty(t, programName(nil))
}

func TestGuardNeverStoresArgv(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", []Entry{{
		PID: 7, StartTime: 1, Comm: testCodexComm,
		Program: programName([]byte("/usr/bin/codex\x00--token\x00abc123\x00")),
	}})
	require.Equal(t, Free, g.Check("pane", []Entry{{PID: 7, StartTime: 1}}))
	// Program never identifies: a different label on the same process stays Free.
	require.Equal(t, Free, g.Check("pane", []Entry{{PID: 7, StartTime: 1, Program: "other"}}))
}

func TestParseStatSkipsParensInComm(t *testing.T) {
	t.Parallel()
	got, err := parseStat(123, "123 (weird (name)) R 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 4242 26")
	require.NoError(t, err)
	require.Equal(t, Entry{PID: 123, StartTime: 4242, State: "R", PPID: 7, Pgrp: 8, TPGID: 11}, got)
}
