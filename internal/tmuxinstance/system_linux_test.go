//go:build linux

package tmuxinstance

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcSystemReadsItsOwnProcess(t *testing.T) {
	t.Parallel()
	sys := newSystem()
	info, err := sys.process(int64(os.Getpid()))
	require.NoError(t, err)
	require.Equal(t, uint32(os.Getuid()), info.uid)
	require.Equal(t, uint32(os.Getgid()), info.gid)
	require.Equal(t, int64(os.Getppid()), info.ppid)
	require.Positive(t, info.start)
	comm, err := os.ReadFile("/proc/self/comm")
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(string(comm)), info.comm)
	require.Equal(t, filepath.Base(os.Args[0]), info.argv0)
	require.Empty(t, info.argv1, "the test binary is no interpreter")

	_, err = sys.process(1 << 23)
	require.ErrorIs(t, err, errProcessGone, "a PID above any pid_max")
}

func TestProcSystemListsChildren(t *testing.T) {
	t.Parallel()
	cmd := exec.CommandContext(t.Context(), "cat")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})
	kids, err := newSystem().children(int64(os.Getpid()))
	require.NoError(t, err)
	require.Contains(t, kids, int64(cmd.Process.Pid))
	child, err := newSystem().process(int64(cmd.Process.Pid))
	require.NoError(t, err)
	require.Equal(t, int64(os.Getpid()), child.ppid)
	require.Equal(t, "cat", child.comm)
}

func TestProcSystemSocketDirectory(t *testing.T) {
	t.Parallel()
	sys := newSystem()
	dir := filepath.Join(t.TempDir(), "tmux")
	require.NoError(t, os.Mkdir(dir, 0o700))
	info, err := sys.socketDir(filepath.Join(dir, "default"))
	require.NoError(t, err)
	require.Equal(t, dirInfo{uid: uint32(os.Getuid()), perm: 0o700, dir: true}, info)

	link := filepath.Join(t.TempDir(), "link")
	require.NoError(t, os.Symlink(dir, link))
	info, err = sys.socketDir(filepath.Join(link, "default"))
	require.NoError(t, err)
	require.True(t, info.symlink, "lstat, never stat: a symlinked directory is refused")

	_, err = sys.socketDir(filepath.Join(t.TempDir(), "missing", "default"))
	require.True(t, isNotExist(err))
}

func TestProcSystemIdentifiers(t *testing.T) {
	t.Parallel()
	sys := newSystem()
	boot, err := sys.bootID()
	require.NoError(t, err)
	require.Regexp(t, bootIDPattern, boot)
	if _, err := os.Stat("/etc/machine-id"); err == nil {
		machine, err := sys.machineID()
		require.NoError(t, err)
		require.Regexp(t, machineIDPattern, machine)
	}
	me, err := user.Current()
	require.NoError(t, err)
	uid, err := sys.lookupUser(me.Username)
	require.NoError(t, err)
	require.Equal(t, uint32(os.Getuid()), uid)
}
