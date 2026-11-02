package detect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Synthetic captures: hand-built ANSI streams standing in for real bench
// screens, which have not arrived yet. Labeled synthetic, never recorded.

// shellTree returns a pane tree whose pane child runs bash.
func shellTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Argv: []string{"bash"}},
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
		{PID: 100, PPID: 50, Argv: []string{"bash"}},
		{PID: 200, PPID: 100, Argv: []string{"/usr/bin/codex"}},
	}
	st := Classify(HarnessShell, DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, 100, procs)
	require.True(t, st.IsCLI)
}

func TestNoCLIInTree(t *testing.T) {
	procs := []Process{
		{PID: 100, PPID: 50, Argv: []string{"tmux: client"}},
		{PID: 200, PPID: 100, Argv: []string{"bash", "-c", "sleep 60"}},
	}
	table := Table{HarnessShell: {InputExpr: `[$#] ?(.*)$`, CLIBinaries: []string{"codex"}}}
	st := Classify(HarnessShell, table,
		[]byte("user@h:~$ "), 80, 24, 100, procs)
	require.True(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestConsolePanelIsNotCLI(t *testing.T) {
	// A panel sitting at a command console read with the wrong profile:
	// no harness binary anywhere, and no input line to classify, so the
	// verdict stays uncertain instead of guessing an empty box.
	procs := []Process{
		{PID: 100, PPID: 50, Argv: []string{"bash"}},
	}
	table := Table{HarnessCodex: {InputExpr: `› (.*)$`, CLIBinaries: []string{"codex"}}}
	st := Classify(HarnessCodex, table,
		[]byte("user@h:~$ "), 80, 24, 100, procs)
	require.False(t, st.Certain)
	require.False(t, st.IsCLI)
}

func TestEmptyAgentProfileStaysUncertain(t *testing.T) {
	// No bench markers yet: the detector must not guess.
	st := Classify(HarnessCodex, DefaultTable(),
		[]byte("anything"), 80, 24, 100, shellTree())
	require.False(t, st.Certain)
}

func TestUnknownHarnessStaysUncertain(t *testing.T) {
	st := Classify(Harness("other"), DefaultTable(),
		[]byte("user@h:~$ "), 80, 24, 100, shellTree())
	require.False(t, st.Certain)
}
