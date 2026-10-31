//go:build unix

package nodeproc

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/runtimelock"
	"github.com/smellak/gifford/internal/xdg"
)

// fakeHubEnv makes the test binary act as the node. Its value is how:
// "ready" takes the runtime lock, prints the ready line and serves until
// SIGTERM; "exit" fails before it is ready; "silent" holds the lock and
// never gets ready; "stubborn" gets ready and ignores SIGTERM.
const (
	fakeHubEnv     = "GIFFORD_NODEPROC_FAKE_HUB"
	fakeRuntimeEnv = "GIFFORD_NODEPROC_FAKE_RUNTIME"
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(fakeHubEnv); mode != "" {
		os.Exit(fakeHub(mode))
	}
	os.Exit(m.Run())
}

func fakeHub(mode string) int {
	lock, err := runtimelock.Acquire(os.Getenv(fakeRuntimeEnv))
	if err != nil {
		fmt.Fprintln(os.Stderr, "gifford hub:", err)
		return 1
	}
	defer func() { _ = lock.Close() }()
	switch mode {
	case "exit":
		fmt.Fprintln(os.Stderr, "gifford hub: load config: invented failure")
		return 1
	case "silent":
		time.Sleep(time.Hour)
		return 1
	case "stubborn":
		signal.Ignore(syscall.SIGTERM)
		fmt.Fprintln(os.Stderr, "gifford hub: ready: serving the fake sockets")
		time.Sleep(time.Hour)
		return 1
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)
	fmt.Fprintln(os.Stderr, "gifford hub: ready: serving the fake sockets")
	<-stop
	return 0
}

// fakeNode returns the directories and the node's command line for a fake
// hub that behaves as mode, and kills any fake hub left at the end.
func fakeNode(t *testing.T, mode string) (xdg.Dirs, []string) {
	t.Helper()
	root := t.TempDir()
	dirs := xdg.Dirs{
		Config: filepath.Join(root, "config"), Data: filepath.Join(root, "data"), Runtime: filepath.Join(root, "run"),
	}
	_, err := xdg.EnsureAll(dirs)
	require.NoError(t, err)
	t.Setenv(fakeHubEnv, mode)
	t.Setenv(fakeRuntimeEnv, dirs.Runtime)
	t.Cleanup(func() {
		if pid, err := ReadPID(dirs.Runtime); err == nil {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	return dirs, []string{os.Args[0], "hub"}
}

func pidFileExists(dirs xdg.Dirs) bool {
	_, err := os.Stat(filepath.Join(dirs.Runtime, PIDFile))
	return err == nil
}

func TestStartRunsTheNodeUntilStop(t *testing.T) {
	dirs, command := fakeNode(t, "ready")
	pid, err := Start(dirs, command, 10*time.Second)
	require.NoError(t, err)
	require.Positive(t, pid)
	recorded, err := ReadPID(dirs.Runtime)
	require.NoError(t, err)
	require.Equal(t, pid, recorded)
	running, err := Running(dirs.Runtime)
	require.NoError(t, err)
	require.True(t, running, "the node holds the runtime lock")
	log, err := os.ReadFile(filepath.Join(dirs.Data, LogFile))
	require.NoError(t, err)
	require.Contains(t, string(log), readyPrefix, "the node's stderr goes to the log")
	info, err := os.Stat(filepath.Join(dirs.Data, LogFile))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	_, err = Start(dirs, command, 10*time.Second)
	require.ErrorIs(t, err, ErrRunning, "a second start refuses")
	recorded, err = ReadPID(dirs.Runtime)
	require.NoError(t, err)
	require.Equal(t, pid, recorded, "and leaves the PID file alone")

	stopped, err := Stop(dirs, command, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, pid, stopped)
	running, err = Running(dirs.Runtime)
	require.NoError(t, err)
	require.False(t, running, "the lock is free once Stop returns")
	require.False(t, pidFileExists(dirs))

	_, err = Stop(dirs, command, 10*time.Second)
	require.ErrorIs(t, err, ErrNotRunning)
}

func TestStartFailsWhenTheNodeExitsBeforeReady(t *testing.T) {
	dirs, command := fakeNode(t, "exit")
	_, err := Start(dirs, command, 10*time.Second)
	require.ErrorContains(t, err, "stopped before it was ready")
	require.ErrorContains(t, err, filepath.Join(dirs.Data, LogFile), "the error points at the log")
	require.False(t, pidFileExists(dirs))
	log, err := os.ReadFile(filepath.Join(dirs.Data, LogFile))
	require.NoError(t, err)
	require.Contains(t, string(log), "invented failure")
}

func TestStartStopsANodeThatIsNeverReady(t *testing.T) {
	dirs, command := fakeNode(t, "silent")
	_, err := Start(dirs, command, 300*time.Millisecond)
	require.ErrorContains(t, err, "not ready within")
	require.False(t, pidFileExists(dirs))
	running, err := Running(dirs.Runtime)
	require.NoError(t, err)
	require.False(t, running, "the node was stopped")
}

func TestStopSignalsOnlyTheNodeUpStarted(t *testing.T) {
	dirs, command := fakeNode(t, "ready")
	// A node gifford up did not start: it holds the lock, with no PID file.
	other := exec.CommandContext(t.Context(), command[0], command[1:]...)
	stderr, err := other.StderrPipe()
	require.NoError(t, err)
	require.NoError(t, other.Start())
	t.Cleanup(func() {
		_ = other.Process.Kill()
		_ = other.Wait()
	})
	buf := make([]byte, 256)
	_, err = stderr.Read(buf)
	require.NoError(t, err, "the node got ready")

	_, err = Stop(dirs, command, time.Second)
	require.ErrorIs(t, err, ErrNotStartedByUp)

	// A PID file that names another process: this test. Only Linux checks
	// the command line; elsewhere Stop would signal it.
	if runtime.GOOS == "linux" {
		require.NoError(t, writePID(dirs.Runtime, os.Getpid()))
		_, err = Stop(dirs, command, time.Second)
		require.ErrorIs(t, err, ErrNotStartedByUp)
		require.NoError(t, removePID(dirs.Runtime))
	}
	require.NoError(t, syscall.Kill(other.Process.Pid, 0), "the node was not signalled")
}

func TestStopRemovesAStalePIDFile(t *testing.T) {
	dirs, command := fakeNode(t, "ready")
	require.NoError(t, writePID(dirs.Runtime, 1<<22))
	_, err := Stop(dirs, command, time.Second)
	require.ErrorIs(t, err, ErrNotRunning)
	require.False(t, pidFileExists(dirs))
}

func TestStopFailsWhenTheNodeKeepsTheLock(t *testing.T) {
	dirs, command := fakeNode(t, "stubborn")
	pid, err := Start(dirs, command, 10*time.Second)
	require.NoError(t, err)
	_, err = Stop(dirs, command, 300*time.Millisecond)
	require.ErrorContains(t, err, "still holds the runtime lock")
	require.True(t, pidFileExists(dirs), "the PID file stays for another try")
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	require.Eventually(t, func() bool {
		running, err := Running(dirs.Runtime)
		return err == nil && !running
	}, 10*time.Second, 20*time.Millisecond)
}

func TestReadPIDRefusesAFileWithoutAPID(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadPID(dir)
	require.ErrorIs(t, err, ErrNotStartedByUp)
	for _, content := range []string{"", "x\n", "0\n", "-4\n"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, PIDFile), []byte(content), 0o600))
		_, err := ReadPID(dir)
		require.Error(t, err, "content %q", content)
		require.NotErrorIs(t, err, ErrNotStartedByUp)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, PIDFile), []byte(strconv.Itoa(42)+"\n"), 0o600))
	pid, err := ReadPID(dir)
	require.NoError(t, err)
	require.Equal(t, 42, pid)
}
