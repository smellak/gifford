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
