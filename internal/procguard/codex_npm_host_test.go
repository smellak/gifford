package procguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	codexNPMProgram      = "node"
	codexNPMOtherProgram = "other"
)

func npmCodexCensus(entries []Entry) ([]Entry, bool) {
	return CodexWorkCensus(entries, 100, 11, codexNPMProgram, testCodexComm)
}

func TestCodexNPMHostBornAfterBaselineIsNotNewWork(t *testing.T) {
	t.Parallel()
	base, ready := npmCodexCensus(codexHostTree(false))
	require.True(t, ready)
	var g Guard
	g.Rebaseline("pane", base)
	fresh := codexHostTree(true)
	original := append([]Entry(nil), fresh...)
	work, ready := npmCodexCensus(fresh)
	require.True(t, ready)
	require.Equal(t, Free, g.Check("pane", work))
	require.Equal(t, original, fresh, "the raw detector census must stay unchanged")
	require.Contains(t, work, fresh[1], "the native CLI is not excluded with its helper")
}

func TestCodexNPMHostAtBaselineMayDisappear(t *testing.T) {
	t.Parallel()
	base, ready := npmCodexCensus(codexHostTree(true))
	require.True(t, ready)
	var g Guard
	g.Rebaseline("pane", base)
	work, ready := npmCodexCensus(codexHostTree(false))
	require.True(t, ready)
	require.Equal(t, Free, g.Check("pane", work))
}

func TestCodexNPMHostNeedsOneBoundNative(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func([]Entry) []Entry{
		"no native": func(entries []Entry) []Entry { return append(entries[:1], entries[2:]...) },
		"two natives": func(entries []Entry) []Entry {
			second := entries[1]
			second.PID, second.StartTime = 500, 15
			return append(entries, second)
		},
		"second native has invalid argv": func(entries []Entry) []Entry {
			second := entries[1]
			second.PID, second.StartTime, second.Argv0 = 500, 15, "codex"
			return append(entries, second)
		},
		"native is a grandchild":       func(entries []Entry) []Entry { entries[1].PPID = 300; return entries },
		"native comm differs":          func(entries []Entry) []Entry { entries[1].Comm = codexNPMOtherProgram; return entries },
		"native argv is relative":      func(entries []Entry) []Entry { entries[1].Argv0 = testCodexComm; return entries },
		"native argv basename differs": func(entries []Entry) []Entry { entries[1].Argv0 = "/opt/example/codex/bin/other"; return entries },
		"native argv absent":           func(entries []Entry) []Entry { entries[1].Argv0 = ""; return entries },
		"native start unknown":         func(entries []Entry) []Entry { entries[1].StartTime = 0; return entries },
		"native PID invalid":           func(entries []Entry) []Entry { entries[1].PID = 0; return entries },
		"wrapper PID reused":           func(entries []Entry) []Entry { entries[0].StartTime++; return entries },
		"wrapper absent":               func(entries []Entry) []Entry { return entries[1:] },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fresh := change(codexHostTree(true))
			work, ready := npmCodexCensus(fresh)
			require.False(t, ready, "an unproven wrapper tree must vote Busy, even before baseline")
			require.Equal(t, fresh, work, "no row may be hidden when native selection is unproven")
		})
	}
}

func TestCodexNPMHostNeedsRegisteredLauncher(t *testing.T) {
	t.Parallel()
	fresh := codexHostTree(true)
	for _, identity := range []struct {
		pid    int
		start  uint64
		script string
	}{
		{0, 11, testCodexComm},
		{100, 0, testCodexComm},
		{999, 11, testCodexComm},
		{100, 999, testCodexComm},
		{100, 11, ""},
		{100, 11, codexNPMOtherProgram},
	} {
		work, ready := CodexWorkCensus(fresh, identity.pid, identity.start, codexNPMProgram, identity.script)
		require.False(t, ready)
		require.Equal(t, fresh, work)
	}
	work, _ := CodexWorkCensus(fresh, 100, 11, codexNPMOtherProgram, testCodexComm)
	require.Equal(t, fresh, work, "another registered launcher does not gain the npm exception")
}

func TestCodexNPMHostUnprovenAuxiliaryRemainsNewWork(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func([]Entry){
		"exe mismatch":                  func(entries []Entry) { entries[3].Exe = otherHostTestExe },
		"unreadable exe and other path": func(entries []Entry) { entries[3].Exe = ""; entries[3].Argv0 = otherHostTestExe },
		"argv mismatch":                 func(entries []Entry) { entries[3].Argv0 = otherHostTestExe },
		"relative argv":                 func(entries []Entry) { entries[3].Argv0 = codexCodeModeHostProgram; entries[3].Exe = "" },
		"comm mismatch":                 func(entries []Entry) { entries[3].Comm = codexNPMOtherProgram },
		"host under node":               func(entries []Entry) { entries[3].PPID = 100 },
		"host under another process":    func(entries []Entry) { entries[3].PPID = 300 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			base, ready := npmCodexCensus(codexHostTree(false))
			require.True(t, ready)
			var g Guard
			g.Rebaseline("pane", base)
			fresh := codexHostTree(true)
			change(fresh)
			work, ready := npmCodexCensus(fresh)
			require.True(t, ready, "native identity remains proved, but the new auxiliary is not excluded")
			require.Equal(t, fresh, work)
			require.Equal(t, Busy, g.Check("pane", work))
		})
	}
}

func TestCodexNPMHostChildrenAndOtherToolsRemainBusy(t *testing.T) {
	t.Parallel()
	for name, child := range map[string]Entry{
		"host tool":    {PID: 500, StartTime: 15, PPID: 400, Exe: sleepTestExe},
		"native tool":  {PID: 500, StartTime: 15, PPID: 200, Exe: sleepTestExe},
		"wrapper tool": {PID: 500, StartTime: 15, PPID: 100, Exe: sleepTestExe},
		"helper-like grandchild": {
			PID: 500, StartTime: 15, PPID: 400,
			Comm: codexCodeModeHostComm, Program: codexCodeModeHostProgram, Argv0: hostTestExe, Exe: hostTestExe,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			base, ready := npmCodexCensus(codexHostTree(false))
			require.True(t, ready)
			var g Guard
			g.Rebaseline("pane", base)
			work, ready := npmCodexCensus(append(codexHostTree(true), child))
			require.True(t, ready)
			require.Contains(t, work, child)
			require.Equal(t, Busy, g.Check("pane", work))
		})
	}
}

func TestCodexNPMHostUnreadableExeUsesBoundMetadata(t *testing.T) {
	t.Parallel()
	fresh := codexHostTree(true)
	for i := range fresh {
		fresh[i].Exe = ""
	}
	work, ready := npmCodexCensus(fresh)
	require.True(t, ready)
	require.Len(t, work, len(fresh)-1)
}

func TestCodexNPMHostNativeRestartRemainsNewWork(t *testing.T) {
	t.Parallel()
	base, ready := npmCodexCensus(codexHostTree(false))
	require.True(t, ready)
	var g Guard
	g.Rebaseline("pane", base)
	fresh := codexHostTree(true)
	fresh[1].StartTime++
	work, ready := npmCodexCensus(fresh)
	require.True(t, ready)
	require.Equal(t, Busy, g.Check("pane", work), "the native remains in the process guard")
}

func TestCodexNPMHostPreservesNativeRegistration(t *testing.T) {
	t.Parallel()
	fresh := codexHostTree(true)
	work, ready := CodexWorkCensus(fresh, 200, 12, testCodexComm, "")
	require.True(t, ready)
	require.Equal(t, WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm), work)
}
