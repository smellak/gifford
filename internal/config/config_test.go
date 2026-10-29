package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
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

func TestLoadAbsentSchemaVersionDefaults(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		"# just a comment\n",
		"{}\n",
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

			cfg, err := Load(path)
			require.NoError(t, err)
			require.Equal(t, DefaultConfig(), cfg)
		})
	}
}

func TestLoadTrailingComment(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: 1\n# trailing comment\n"), 0o600))

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

	for _, body := range []string{
		"schema_version: 2\n",
		"schema_version: 0\n",
		"schema_version: -1\n",
		"schema_version: ~\n",
		"schema_version:\n",
		"schema_version: \"1\"\n",
		"schema_version: 1.0\n",
		"schema_version: [1]\n",
		"schema_version: {v: 1}\n",
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

			_, err := Load(path)
			require.ErrorIs(t, err, ErrUnsupportedSchemaVersion)
		})
	}
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

func TestLoadNodeID(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: 1\nnode_id: node-a\n"), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "node-a", string(cfg.NodeID))
}

func TestLoadMissingNodeIDDefaults(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: 1\n"), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Empty(t, string(cfg.NodeID))
}

func TestLoadInvalidNodeID(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		"schema_version: 1\nnode_id: Node-A\n",
		"schema_version: 1\nnode_id: \"\"\n",
		"schema_version: 1\nnode_id: a b\n",
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o600))

			_, err := Load(path)
			require.ErrorIs(t, err, ErrInvalidNodeID)
		})
	}
}

func TestLoadFederation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(
		"schema_version: 1\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: /etc/gifford/peers\n",
	), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:7443", cfg.Federation.Listen)
	require.Equal(t, "/etc/gifford/peers", cfg.Federation.TrustBundle)
}

func TestLoadInvalidFederation(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"unknown federation field": "federation:\n  tls_skip_verify: true\n",
		"invalid listen":           "federation:\n  listen: https://example.invalid\n  trust_bundle: /tmp/peers\n",
		"missing bundle":           "federation:\n  listen: 127.0.0.1:7443\n",
		"numeric listen":           "federation:\n  listen: 7443\n  trust_bundle: /tmp/peers\n",
		"null bundle":              "federation:\n  listen: 127.0.0.1:7443\n  trust_bundle: null\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
			_, err := Load(path)
			require.Error(t, err)
		})
	}
}

func TestLoadMLReadiness(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	cfg, err := load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.Equal(t, dataset.DefaultReadinessThresholds(), cfg.ML.Readiness, "defaults when absent")

	cfg, err = load(t, "ml:\n  readiness:\n    train_min: 2000\n    synthetic_max_percent: 0\n    rare_label_test_harnesses_min: 2\n")
	require.NoError(t, err)
	want := dataset.DefaultReadinessThresholds()
	want.TrainMin, want.SyntheticMaxPercent, want.RareLabelTestHarnessesMin = 2000, 0, 2
	require.Equal(t, want, cfg.ML.Readiness, "overrides only what is given")

	for name, body := range map[string]string{
		"unknown key":       "ml:\n  readiness:\n    train_minimum: 2000\n",
		"unknown ml key":    "ml:\n  other: 1\n",
		"negative":          "ml:\n  readiness:\n    test_min: -1\n",
		"float":             "ml:\n  readiness:\n    test_min: 1.5\n",
		"string":            "ml:\n  readiness:\n    test_min: \"300\"\n",
		"null":              "ml:\n  readiness:\n    test_min: null\n",
		"list":              "ml:\n  readiness:\n    test_min: [1]\n",
		"percent above 100": "ml:\n  readiness:\n    label_share_min_percent: 101\n",
	} {
		_, err := load(t, body)
		require.Error(t, err, name)
		if name != "unknown key" && name != "unknown ml key" {
			require.ErrorIs(t, err, ErrInvalidReadiness, name)
		}
	}
}
