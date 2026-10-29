//go:build unix

package run

import (
	"flag"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/proxy"
)

// TestHelperProcess is the fake harness for the run tests. It runs as
// os.Args[0] -test.run=^TestHelperProcess$ -- <status|kill>: a status exits
// with it, kill SIGKILLs itself. With no argument it is a normal test run
// and skips.
func TestHelperProcess(t *testing.T) {
	if flag.NArg() == 0 {
		t.Skip("not a helper process")
	}
	// The kill mode needs SIGKILL, so it lives in the unix test file.
	if flag.Arg(0) == "kill" {
		proc, err := os.FindProcess(os.Getpid())
		require.NoError(t, err)
		require.NoError(t, proc.Kill())
		os.Exit(127)
	}
	code, err := strconv.Atoi(flag.Arg(0))
	require.NoError(t, err)
	os.Exit(code)
}

// cliHelperArgv re-runs this test binary as a helper with the given mode.
func cliHelperArgv(mode string) []string {
	return []string{os.Args[0], "-test.run=^TestHelperProcess$", "--", mode}
}

// TestRunPassesExitStatusUnix runs the command on a test PTY through NewCmd:
// status 0 passes nil, 7 passes ExitError{7}, SIGKILL passes 137, stderr
// carries exactly the daemon notice, and a non-file stdin refuses with
// ErrNotTerminal. It is serial: it sets process environment.
func TestRunPassesExitStatusUnix(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	outer, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = outer.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(outer, &pty.Winsize{Rows: 24, Cols: 80}))
	runOnPTY := func(t *testing.T, stdin io.Reader, args []string) (error, string) {
		t.Helper()
		cmd := NewCmd(nil)
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetIn(stdin)
		cmd.SetOut(slave)
		var stderr strings.Builder
		cmd.SetErr(&stderr)
		cmd.SetArgs(args)
		return cmd.Execute(), stderr.String()
	}
	t.Run("status 0", func(t *testing.T) {
		err, stderr := runOnPTY(t, slave, append([]string{"--"}, cliHelperArgv("0")...))
		require.NoError(t, err)
		require.Equal(t, proxy.DaemonNotice+"\n", stderr)
	})
	t.Run("status 7", func(t *testing.T) {
		err, stderr := runOnPTY(t, slave, append([]string{"--"}, cliHelperArgv("7")...))
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, 7, ec.Code)
		require.Equal(t, proxy.DaemonNotice+"\n", stderr)
	})
	t.Run("killed", func(t *testing.T) {
		err, stderr := runOnPTY(t, slave, append([]string{"--"}, cliHelperArgv("kill")...))
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, 137, ec.Code)
		require.Equal(t, proxy.DaemonNotice+"\n", stderr)
	})
	t.Run("non-terminal stdin", func(t *testing.T) {
		err, stderr := runOnPTY(t, strings.NewReader(""), append([]string{"--"}, cliHelperArgv("0")...))
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec)
		require.Equal(t, exitcode.Error, ec.Code)
		require.ErrorIs(t, ec.Err, proxy.ErrNotTerminal)
		require.Empty(t, stderr)
	})
}
