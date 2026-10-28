package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadMissingFile(t *testing.T) {
	t.Parallel()

	cfg, err := Load(filepath.Join(t.TempDir(), "config.yaml"))
	require.NoError(t, err)
	require.Equal(t, DefaultConfig(), cfg)
}

func TestLoadKnownFields(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: 1\n"), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, 1, cfg.SchemaVersion)
}

func TestLoadUnknownKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: 1\nfoo: bar\n"), 0o600))

	_, err := Load(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found in type")
}

func TestLoadUnsupportedSchemaVersion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: 2\n"), 0o600))

	_, err := Load(path)
	require.ErrorIs(t, err, ErrUnsupportedSchemaVersion)
}

func TestLoadEmptyFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte{}, 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, DefaultConfig(), cfg)
}

func TestDefaultPath(t *testing.T) {
	t.Parallel()

	require.Equal(t, filepath.Join("/x", "config.yaml"), DefaultPath("/x"))
}
