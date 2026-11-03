package procguard

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSampleTreeExecutableIsNotSpoofedArgvZero(t *testing.T) {
	t.Parallel()
	sleep, err := exec.LookPath("sleep")
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), sleep, "30")
	command.Args[0] = codexCodeModeHostProgram
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	// Start returns once the child is inside execve, but the kernel
	// publishes the new argv in /proc/<pid>/cmdline only after the exe
	// link: a loaded machine can sample in between and read no argv.
	cmdline := filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "cmdline")
	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(cmdline)
		return err == nil && len(raw) > 0
	}, 5*time.Second, 5*time.Millisecond)
	entries, err := SampleTree(command.Process.Pid)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	want, err := os.Readlink(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "exe"))
	require.NoError(t, err)
	require.Equal(t, want, entries[0].Exe)
	require.Equal(t, codexCodeModeHostProgram, entries[0].Program)
	require.Equal(t, codexCodeModeHostProgram, entries[0].Argv0)
	require.Equal(t, "sleep", filepath.Base(entries[0].Exe))
	require.Equal(t, os.Getpid(), entries[0].PPID)
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

func TestParseStatRejectsMissingCommClose(t *testing.T) {
	t.Parallel()
	_, err := parseStat(123, "123 no-parens-here R 1 2 3")
	require.Error(t, err)
}

func TestSampleTreeDeadRootIsErrNoRoot(t *testing.T) {
	t.Parallel()
	_, err := SampleTree(1 << 30)
	require.ErrorIs(t, err, ErrNoRoot)
}

func TestParseStatSkipsParensInComm(t *testing.T) {
	t.Parallel()
	got, err := parseStat(123, "123 (weird (name)) R 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 4242 26")
	require.NoError(t, err)
	require.Equal(t, Entry{PID: 123, StartTime: 4242, State: "R", PPID: 7, Pgrp: 8, TPGID: 11}, got)
}

func TestArgvZeroDoesNotRetainArguments(t *testing.T) {
	t.Parallel()
	require.Equal(t, codexTestExe, argvZero([]byte(codexTestExe+"\x00--token\x00private-canary\x00")))
	require.Empty(t, argvZero(nil))
}
