package initcmd

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/xdg"
)

// cleanHome points HOME at an empty directory, with no XDG variable, as
// for a user who never ran gifford.
func cleanHome(t *testing.T) xdg.Dirs {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	for _, k := range []string{"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_RUNTIME_DIR"} {
		t.Setenv(k, "")
	}
	dirs, err := xdg.Resolve()
	require.NoError(t, err)
	return dirs
}

func execute(args ...string) (string, error) {
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestInitWritesConfigAndDirectories(t *testing.T) {
	dirs := cleanHome(t)
	out, err := execute("--node-id", "node-test")
	require.NoError(t, err)
	path := config.DefaultPath(dirs.Config)
	require.Contains(t, out, path)
	require.Contains(t, out, "next: gifford up")

	cfg, err := config.Load(path)
	require.NoError(t, err)
	require.Equal(t, protocol.NodeID("node-test"), cfg.NodeID)
	require.Equal(t, config.SupportedSchemaVersion, cfg.SchemaVersion)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, fs.FileMode(0o600), info.Mode().Perm())
	for _, dir := range []string{dirs.Config, dirs.Data, dirs.Runtime} {
		info, err := os.Stat(dir)
		require.NoError(t, err, dir)
		require.True(t, info.IsDir(), dir)
		require.Equal(t, fs.FileMode(0o700), info.Mode().Perm(), dir)
	}
}

func TestInitGeneratesANodeID(t *testing.T) {
	dirs := cleanHome(t)
	_, err := execute()
	require.NoError(t, err)
	cfg, err := config.Load(config.DefaultPath(dirs.Config))
	require.NoError(t, err)
	require.True(t, cfg.NodeID.Valid())
	require.Regexp(t, `^node-[0-9a-f]{8}$`, string(cfg.NodeID))
}

func TestInitNeverOverwritesConfig(t *testing.T) {
	dirs := cleanHome(t)
	_, err := execute("--node-id", "node-first")
	require.NoError(t, err)
	path := config.DefaultPath(dirs.Config)
	written, err := os.ReadFile(path)
	require.NoError(t, err)

	_, err = execute("--node-id", "node-second")
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Refused, ec.Code)
	require.ErrorContains(t, err, "never overwrites")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, written, after)

	// A config.yaml written by hand is kept too.
	own := []byte("schema_version: 1\nnode_id: node-own\norganization_id: org-own\n")
	require.NoError(t, os.WriteFile(path, own, 0o600))
	_, err = execute()
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Refused, ec.Code)
	after, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, own, after)
}

func TestInitRefusesAnInvalidNodeID(t *testing.T) {
	dirs := cleanHome(t)
	for _, id := range []string{"Node-Upper", "-dash-first", "has space", strings.Repeat("a", 65)} {
		_, err := execute("--node-id", id)
		var ec *deps.ExitError
		require.ErrorAs(t, err, &ec, id)
		require.Equal(t, exitcode.Usage, ec.Code, id)
	}
	_, err := os.Stat(config.DefaultPath(dirs.Config))
	require.ErrorIs(t, err, fs.ErrNotExist, "nothing is written")
}
