package detect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Synthetic captures: hand-built ANSI streams standing in for real bench
// screens, which have not arrived yet. Labeled synthetic, never recorded.

// shellBin is the shell binary every synthetic tree runs.
const shellBin = "bash"

// shellTree returns a pane tree whose pane child runs bash in front.
func shellTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 100, Argv: []string{shellBin}},
	}
}

func TestEmptyBox(t *testing.T) {
	st := Classify(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, 100, shellTree())
	require.True(t, st.Certain)
	require.False(t, st.Busy)
	require.False(t, st.BoxBusy)
	require.True(t, st.IsCLI)
}

func TestTypedBox(t *testing.T) {
	st := Classify(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ hello"), 80, 24, 100, shellTree())
	require.True(t, st.Certain)
	require.False(t, st.Busy)
	require.True(t, st.BoxBusy)
	require.True(t, st.IsCLI)
}

func TestFaintPlaceholderIsEmptyBox(t *testing.T) {
	st := Classify(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ \x1b[2mType here\x1b[0m"), 80, 24, 100, shellTree())
	require.True(t, st.Certain)
	require.False(t, st.BoxBusy)
}

func TestFaintPlaceholderPlusTypedIsBoxBusy(t *testing.T) {
	st := Classify(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ \x1b[2mType here\x1b[0mhi"), 80, 24, 100, shellTree())
	require.True(t, st.Certain)
	require.True(t, st.BoxBusy)
}

func TestBusyMark(t *testing.T) {
	table := DefaultTable()
	shell := table[HarnessShell]
	shell.BusyMarks = []string{"WORKING…"}
	table[HarnessShell] = shell
	st := Classify(HarnessShell, table,
		[]byte("status: WORKING…\nuser@h:~$ "), 80, 24, 100, shellTree())
	require.True(t, st.Certain)
	require.True(t, st.Busy)
	require.False(t, st.BoxBusy)
}

func TestChildCLI(t *testing.T) {
	procs := []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 100, Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 100, Argv: []string{"/usr/bin/codex"}},
	}
	st := Classify(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, 100, procs)
	require.True(t, st.IsCLI)
}

func TestNoCLIInTree(t *testing.T) {
	procs := []Process{
		{PID: 100, PPID: 50, Argv: []string{"tmux: client"}},
		{PID: 200, PPID: 100, Argv: []string{shellBin, "-c", "sleep 60"}},
	}
	table := Table{HarnessShell: {InputExpr: `[$#] ?(.*)$`, CLIBinaries: []string{"codex"}}}
	st := Classify(HarnessShell, table,
		[]byte("user@h:~$ "), 80, 24, 100, procs)
	require.True(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestConsolePanelIsNotCLI(t *testing.T) {
	// A panel sitting at a command console read with the Codex profile:
	// no composer line and no harness binary. Missing composer evidence
	// fails closed: uncertain, and not a CLI. Never type there.
	procs := []Process{
		{PID: 100, PPID: 50, Argv: []string{shellBin}},
	}
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, 100, procs)
	require.False(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestEmptyAgentProfileStaysUncertain(t *testing.T) {
	// No bench markers yet: the detector must not guess.
	st := Classify(HarnessClaude, DefaultTable(),
		[]byte("anything"), 80, 24, 100, shellTree())
	require.False(t, st.Certain)
}

// codexTree returns a pane tree with the Codex CLI in the foreground:
// its group owns the pane tty.
func codexTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{"/usr/bin/codex"}},
	}
}

func TestSuspendedCLIIsNotForeground(t *testing.T) {
	// Ctrl-Z leaves Codex stopped (T) with bash back in front: present
	// in the tree but not the pane's agent, even with a composer showing.
	procs := []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 100, Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'T', TPGID: 100, Argv: []string{"/usr/bin/codex"}},
	}
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› Explain this codebase"), 80, 24, 100, procs)
	require.True(t, st.Certain)
	require.False(t, st.BoxBusy)
	require.False(t, st.IsCLI)
}

func TestTracedCLIIsNotForeground(t *testing.T) {
	procs := []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 't', TPGID: 200, Argv: []string{"/usr/bin/codex"}},
	}
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› Explain this codebase"), 80, 24, 100, procs)
	require.True(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestBackgroundCLIIsNotForeground(t *testing.T) {
	// Codex running but backgrounded, bash in front: not the agent.
	procs := []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 100, Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 100, Argv: []string{"/usr/bin/codex"}},
	}
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› Explain this codebase"), 80, 24, 100, procs)
	require.True(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestUnknownTPGIDIsNotCLI(t *testing.T) {
	// Without the tty foreground group the tree proves nothing.
	procs := []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', Argv: []string{shellBin}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', Argv: []string{"/usr/bin/codex"}},
	}
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› Explain this codebase"), 80, 24, 100, procs)
	require.True(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestCodexKnownPlaceholderIsFree(t *testing.T) {
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› Explain this codebase"), 80, 24, 100, codexTree())
	require.True(t, st.Certain)
	require.False(t, st.Busy)
	require.False(t, st.BoxBusy)
	require.True(t, st.IsCLI)
}

func TestCodexLabelledPlaceholderIsFree(t *testing.T) {
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("mysession   › Summarize recent commits"), 80, 24, 100, codexTree())
	require.True(t, st.Certain)
	require.False(t, st.BoxBusy)
}

func TestCodexFaintUnknownPlaceholderIsFree(t *testing.T) {
	// A placeholder the known list missed still shows faint through -e.
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› \x1b[2mType to chat\x1b[0m"), 80, 24, 100, codexTree())
	require.True(t, st.Certain)
	require.False(t, st.BoxBusy)
}

func TestCodexTypedBoxIsBusy(t *testing.T) {
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("› hello"), 80, 24, 100, codexTree())
	require.True(t, st.Certain)
	require.False(t, st.Busy)
	require.True(t, st.BoxBusy)
	require.Equal(t, "typed", st.Detail)
	require.NotContains(t, st.Detail, "hello")
}

func TestCodexWorkingIndicator(t *testing.T) {
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("thinking…\nesc to interrupt\n› "), 80, 24, 100, codexTree())
	require.True(t, st.Certain)
	require.True(t, st.Busy)
	require.False(t, st.BoxBusy)
}

func TestBusyMarkOutsideTailIsIgnored(t *testing.T) {
	// Stale transcript above the tail must not read as working.
	filler := ""
	for range 12 {
		filler += "old output line\n"
	}
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("esc to interrupt\n"+filler+"› "), 80, 24, 100, codexTree())
	require.True(t, st.Certain)
	require.False(t, st.Busy)
}

func TestUnknownHarnessStaysUncertain(t *testing.T) {
	st := Classify(Harness("other"), DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, 100, shellTree())
	require.False(t, st.Certain)
}
