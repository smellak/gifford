package down

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/cli/runtimelock"
	"github.com/smellak/gifford/internal/xdg"
)

func tempDirs(t *testing.T) xdg.Dirs {
	t.Helper()
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "run"))
	dirs, err := xdg.Resolve()
	require.NoError(t, err)
	return dirs
}

func execute() (string, error) {
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(nil)
	err := cmd.Execute()
	return out.String(), err
}

func TestDownWithoutANodeIsNotAnError(t *testing.T) {
	dirs := tempDirs(t)
	out, err := execute()
	require.NoError(t, err)
	require.Equal(t, "gifford down: the node is not running\n", out)

	_, err = xdg.EnsureAll(dirs)
	require.NoError(t, err)
	out, err = execute()
	require.NoError(t, err)
	require.Contains(t, out, "not running")
}

func TestDownLeavesANodeUpDidNotStart(t *testing.T) {
	dirs := tempDirs(t)
	_, err := xdg.EnsureAll(dirs)
	require.NoError(t, err)
	// A node that holds the runtime lock, with no PID file from gifford up.
	lock, err := runtimelock.Acquire(dirs.Runtime)
	require.NoError(t, err)
	defer func() { require.NoError(t, lock.Close()) }()

	_, err = execute()
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Refused, ec.Code)
	require.ErrorContains(t, err, "not started by gifford up")
}
