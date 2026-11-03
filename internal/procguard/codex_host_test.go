package procguard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	codexTestExe     = "/opt/example/codex/bin/codex"
	hostTestExe      = "/opt/example/codex/bin/codex-code-mode-host"
	sleepTestExe     = "/usr/bin/sleep"
	otherHostTestExe = "/tmp/other/codex-code-mode-host"
)

func codexHostTree(host bool) []Entry {
	entries := restTree(false)
	entries[1].Exe = codexTestExe
	entries[1].Argv0 = codexTestExe
	if host {
		entries = append(entries, Entry{
			PID: 400, StartTime: 14, PPID: 200,
			Comm: codexCodeModeHostComm, Program: codexCodeModeHostProgram, Argv0: hostTestExe, Exe: hostTestExe,
		})
	}
	return entries
}

func TestCodexHostBornAfterBaselineIsNotNewWork(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", WithoutCodexCodeModeHost(codexHostTree(false), 200, 12, testCodexComm))
	fresh := codexHostTree(true)
	before := append([]Entry(nil), fresh...)
	require.Equal(t, Free, g.Check("pane", WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm)))
	require.Equal(t, before, fresh, "the screen detector still receives the full original tree")
}

func TestCodexHostKnownAtBaselineCanExitWithoutHidingWork(t *testing.T) {
	t.Parallel()
	var g Guard
	g.Rebaseline("pane", WithoutCodexCodeModeHost(codexHostTree(true), 200, 12, testCodexComm))
	require.Equal(t, Free, g.Check("pane", WithoutCodexCodeModeHost(codexHostTree(false), 200, 12, testCodexComm)))
	fresh := append(codexHostTree(true), Entry{PID: 500, StartTime: 15, PPID: 400, Exe: sleepTestExe})
	require.Equal(t, Busy, g.Check("pane", WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm)))
}

func TestCodexHostExceptionFailsClosed(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func([]Entry){
		"same comm and argv with another exe":            func(entries []Entry) { entries[3].Exe = otherHostTestExe },
		"unreadable exe and argv from another directory": func(entries []Entry) { entries[3].Exe = ""; entries[3].Argv0 = otherHostTestExe },
		"same comm but another argv":                     func(entries []Entry) { entries[3].Argv0 = otherHostTestExe },
		"host argv is relative":                          func(entries []Entry) { entries[3].Argv0 = codexCodeModeHostProgram; entries[3].Exe = "" },
		"host comm is different":                         func(entries []Entry) { entries[3].Comm = "wrong-comm" },
		"host is not a direct child":                     func(entries []Entry) { entries[3].PPID = 300 },
		"registered parent pid reused":                   func(entries []Entry) { entries[1].StartTime++ },
		"registered parent argv missing":                 func(entries []Entry) { entries[1].Argv0 = "" },
		"registered parent argv basename mismatch":       func(entries []Entry) { entries[1].Argv0 = "/opt/example/codex/bin/other" },
		"relative parent argv is not proof": func(entries []Entry) {
			entries[1].Argv0 = testCodexComm
			entries[3].Argv0 = codexCodeModeHostProgram
			entries[3].Exe = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var g Guard
			g.Rebaseline("pane", codexHostTree(false))
			fresh := codexHostTree(true)
			change(fresh)
			require.Equal(t, fresh, WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm), "unproven helpers stay in the census")
			require.Equal(t, Busy, g.Check("pane", WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm)))
		})
	}
}

func TestCodexHostChildrenAndOtherNewToolsRemainBusy(t *testing.T) {
	t.Parallel()
	for name, child := range map[string]Entry{
		"child of host": {PID: 500, StartTime: 15, PPID: 400, Exe: sleepTestExe},
		"host exe beneath host is still work": {
			PID: 500, StartTime: 15, PPID: 400,
			Comm: codexCodeModeHostComm, Program: codexCodeModeHostProgram, Argv0: hostTestExe, Exe: hostTestExe,
		},
		"other direct child of harness": {PID: 500, StartTime: 15, PPID: 200, Exe: sleepTestExe},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var g Guard
			g.Rebaseline("pane", WithoutCodexCodeModeHost(codexHostTree(false), 200, 12, testCodexComm))
			fresh := append(codexHostTree(true), child)
			work := WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm)
			require.Contains(t, work, child)
			require.Equal(t, Busy, g.Check("pane", work))
		})
	}
}

func TestCodexHostExceptionNeedsRegisteredIdentity(t *testing.T) {
	t.Parallel()
	fresh := codexHostTree(true)
	for _, identity := range []struct {
		pid   int
		start uint64
	}{
		{0, 12}, {200, 0}, {999, 12}, {200, 999},
	} {
		require.Equal(t, fresh, WithoutCodexCodeModeHost(fresh, identity.pid, identity.start, testCodexComm))
	}
}

func TestCodexHostUnreadableExeUsesBoundMetadata(t *testing.T) {
	t.Parallel()
	base, fresh := codexHostTree(false), codexHostTree(true)
	base[1].Exe, fresh[1].Exe, fresh[3].Exe = "", "", ""
	var g Guard
	g.Rebaseline("pane", WithoutCodexCodeModeHost(base, 200, 12, testCodexComm))
	require.Equal(t, Free, g.Check("pane", WithoutCodexCodeModeHost(fresh, 200, 12, testCodexComm)))
}

func TestCodexHostRegisteredBasenameMustMatch(t *testing.T) {
	t.Parallel()
	fresh := codexHostTree(true)
	for _, program := range []string{"", "launcher", "wrong-basename"} {
		require.Equal(t, fresh, WithoutCodexCodeModeHost(fresh, 200, 12, program))
	}
}

func TestArgvZeroDoesNotRetainArguments(t *testing.T) {
	t.Parallel()
	require.Equal(t, codexTestExe, argvZero([]byte(codexTestExe+"\x00--token\x00private-canary\x00")))
	require.Empty(t, argvZero(nil))
}
