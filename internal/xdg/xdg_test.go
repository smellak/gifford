package xdg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_DATA_HOME", "")
	t.Setenv("XDG_RUNTIME_DIR", "")

	// Unset (not just empty) so os.UserHomeDir falls back to $HOME.
	// t.Setenv cannot unset; empty XDG_* already triggers the defaults.
	dirs, err := Resolve()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(home, ".config", "gifford"), dirs.Config)
	require.Equal(t, filepath.Join(home, ".local", "share", "gifford"), dirs.Data)
	require.Equal(t, filepath.Join(dirs.Data, "run"), dirs.Runtime)
}

func TestResolveExplicitDirs(t *testing.T) {
	home := t.TempDir()
	cfg := t.TempDir()
	data := t.TempDir()
	run := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", cfg)
	t.Setenv("XDG_DATA_HOME", data)
	t.Setenv("XDG_RUNTIME_DIR", run)

	dirs, err := Resolve()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(cfg, "gifford"), dirs.Config)
	require.Equal(t, filepath.Join(data, "gifford"), dirs.Data)
	require.Equal(t, filepath.Join(run, "gifford"), dirs.Runtime)
}

func TestEnsureAllCreatesMissing(t *testing.T) {
	base := t.TempDir()
	dirs := Dirs{
		Config:  filepath.Join(base, "cfg", "gifford"),
		Data:    filepath.Join(base, "data", "gifford"),
		Runtime: filepath.Join(base, "data", "gifford", "run"),
	}

	checks, err := EnsureAll(dirs)
	require.NoError(t, err)
	require.Len(t, checks, 3)
	for _, c := range checks {
		require.True(t, c.Created, c.Name)
		require.True(t, c.OK, c.Name)
	}

	for _, p := range []string{dirs.Config, dirs.Data, dirs.Runtime} {
		info, statErr := os.Stat(p)
		require.NoError(t, statErr)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), p)
	}
}

func TestEnsureAllReportsWrongMode(t *testing.T) {
	base := t.TempDir()
	odd := filepath.Join(base, "run")
	require.NoError(t, os.MkdirAll(odd, 0o755))
	dirs := Dirs{
		Config:  filepath.Join(base, "cfg", "gifford"),
		Data:    filepath.Join(base, "data", "gifford"),
		Runtime: odd,
	}

	checks, err := EnsureAll(dirs)
	require.NoError(t, err)
	require.Len(t, checks, 3)

	var runtime DirCheck
	for _, c := range checks {
		if c.Name == "runtime" {
			runtime = c
		}
	}
	require.False(t, runtime.Created)
	require.False(t, runtime.OK)
	require.Equal(t, os.FileMode(0o755), runtime.Mode)

	info, statErr := os.Stat(odd)
	require.NoError(t, statErr)
	require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
}

func TestEnsureAllRejectsRegularFiles(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	paths := []string{"config", "data", "runtime"}
	dirs := Dirs{}
	for _, name := range paths {
		p := filepath.Join(base, name)
		require.NoError(t, os.WriteFile(p, nil, 0o700))
		switch name {
		case "config":
			dirs.Config = p
		case "data":
			dirs.Data = p
		case "runtime":
			dirs.Runtime = p
		}
	}

	checks, err := EnsureAll(dirs)
	require.Error(t, err)
	require.Len(t, checks, 3)
	for _, c := range checks {
		require.False(t, c.Created, c.Name)
		require.False(t, c.OK, c.Name)
		require.Equal(t, os.FileMode(0o700), c.Mode, c.Name)
	}
	require.Equal(t, 3, strings.Count(err.Error(), "not a directory"))

	for _, p := range []string{dirs.Config, dirs.Data, dirs.Runtime} {
		info, statErr := os.Stat(p)
		require.NoError(t, statErr)
		require.True(t, info.Mode().IsRegular(), p)
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), p)
	}
}
