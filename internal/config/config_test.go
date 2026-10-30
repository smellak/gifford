package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/notifications"
	"github.com/smellak/gifford/internal/screenproj"
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

func TestLoadOrganizationID(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(
		"schema_version: 1\norganization_id: org-a\n",
	), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "org-a", cfg.OrganizationID)
}

func TestLoadInvalidOrganizationID(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"Org-A", "", "org a"} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(
			"schema_version: 1\norganization_id: \""+value+"\"\n",
		), 0o600))
		_, err := Load(path)
		require.ErrorIs(t, err, ErrInvalidOrganizationID)
	}
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
		"schema_version: 1\norganization_id: org-a\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: /etc/gifford/peers\n  advertise: https://node-a.example.invalid:7443\n",
	), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:7443", cfg.Federation.Listen)
	require.Equal(t, "/etc/gifford/peers", cfg.Federation.TrustBundle)
	require.Equal(t, "https://node-a.example.invalid:7443", cfg.Federation.Advertise)
}

func TestLoadUnenrolledFederation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(
		"schema_version: 1\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: /etc/gifford/peers\n  advertise: https://node-a.example.invalid:7443\n",
	), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.Empty(t, cfg.OrganizationID)
}

func TestLoadInvalidFederation(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"unknown federation field": "federation:\n  tls_skip_verify: true\n",
		"invalid listen":           "organization_id: org-a\nfederation:\n  listen: https://example.invalid\n  trust_bundle: /tmp/peers\n  advertise: https://node-a.example.invalid\n",
		"missing bundle":           "organization_id: org-a\nfederation:\n  listen: 127.0.0.1:7443\n  advertise: https://node-a.example.invalid\n",
		"missing advertise":        "organization_id: org-a\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: /tmp/peers\n",
		"invalid advertise":        "organization_id: org-a\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: /tmp/peers\n  advertise: http://node-a.example.invalid\n",
		"advertise with query":     "organization_id: org-a\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: /tmp/peers\n  advertise: https://node-a.example.invalid?token=no\n",
		"numeric listen":           "organization_id: org-a\nfederation:\n  listen: 7443\n  trust_bundle: /tmp/peers\n  advertise: https://node-a.example.invalid\n",
		"null bundle":              "organization_id: org-a\nfederation:\n  listen: 127.0.0.1:7443\n  trust_bundle: null\n  advertise: https://node-a.example.invalid\n",
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

func TestLoadNotificationsTMUX(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	disabled, err := load(t, "notifications: {}\n")
	require.NoError(t, err)
	require.Nil(t, disabled.Notifications.TMUX)

	defaults, err := load(t, "notifications:\n  tmux:\n    socket: /tmp/gifford-test.sock\n    client: /dev/pts/9\n")
	require.NoError(t, err)
	require.Equal(t, notifications.DefaultDuration, defaults.Notifications.TMUX.Duration)
	require.Equal(t, notifications.DefaultTimeout, defaults.Notifications.TMUX.Timeout)
	require.Equal(t, notifications.DefaultTemplate, defaults.Notifications.TMUX.Template)

	cfg, err := load(t, "notifications:\n  tmux:\n    socket: /tmp/gifford-test.sock\n    client: /dev/pts/9\n    duration: 3s\n    timeout: 250ms\n    template: '[GIFFORD] {priority} {class} id {id}'\n")
	require.NoError(t, err)
	require.Equal(t, "/tmp/gifford-test.sock", cfg.Notifications.TMUX.Socket)
	require.Equal(t, "/dev/pts/9", cfg.Notifications.TMUX.Client)
	require.Equal(t, 3*time.Second, cfg.Notifications.TMUX.Duration)
	require.Equal(t, 250*time.Millisecond, cfg.Notifications.TMUX.Timeout)
	require.Equal(t, "[GIFFORD] {priority} {class} id {id}", cfg.Notifications.TMUX.Template)
}

func TestLoadInvalidNotifications(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"unknown notifications key": "notifications:\n  desktop: true\n",
		"unknown tmux key":          "notifications:\n  tmux:\n    socket: /tmp/a\n    client: c\n    popup: true\n",
		"missing socket":            "notifications:\n  tmux:\n    client: c\n",
		"relative socket":           "notifications:\n  tmux:\n    socket: relative.sock\n    client: c\n",
		"missing client":            "notifications:\n  tmux:\n    socket: /tmp/a\n",
		"numeric client":            "notifications:\n  tmux:\n    socket: /tmp/a\n    client: 7\n",
		"zero duration":             "notifications:\n  tmux:\n    socket: /tmp/a\n    client: c\n    duration: 0s\n",
		"numeric timeout":           "notifications:\n  tmux:\n    socket: /tmp/a\n    client: c\n    timeout: 1\n",
		"unsafe template":           "notifications:\n  tmux:\n    socket: /tmp/a\n    client: c\n    template: '[GIFFORD] {request_note}'\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
			_, err := Load(path)
			require.Error(t, err)
			if name != "unknown notifications key" && name != "unknown tmux key" {
				require.ErrorIs(t, err, ErrInvalidNotifications)
			}
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

func TestLoadMLProjection(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}
	list := filepath.Join(t.TempDir(), "mask.txt")

	cfg, err := load(t, "ml:\n  screen_lines: 40\n  mask: none\n")
	require.NoError(t, err)
	require.Equal(t, 40, cfg.ML.ScreenLines)
	require.Equal(t, screenproj.DomainMaskNone, cfg.ML.Mask)
	cfg, err = load(t, "ml:\n  screen_lines: 1\n  mask: "+list+"\n")
	require.NoError(t, err)
	require.Equal(t, 1, cfg.ML.ScreenLines)
	require.Equal(t, list, cfg.ML.Mask)
	cfg, err = load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.Zero(t, cfg.ML.ScreenLines, "no default")
	require.Empty(t, cfg.ML.Mask, "no default")

	for name, body := range map[string]string{
		"zero lines":     "ml:\n  screen_lines: 0\n",
		"41 lines":       "ml:\n  screen_lines: 41\n",
		"negative lines": "ml:\n  screen_lines: -3\n",
		"float lines":    "ml:\n  screen_lines: 40.0\n",
		"string lines":   "ml:\n  screen_lines: \"40\"\n",
		"null lines":     "ml:\n  screen_lines: null\n",
		"list lines":     "ml:\n  screen_lines: [40]\n",
		"empty mask":     "ml:\n  mask: \"\"\n",
		"relative mask":  "ml:\n  mask: mask.txt\n",
		"none in caps":   "ml:\n  mask: NONE\n",
		"numeric mask":   "ml:\n  mask: 7\n",
		"null mask":      "ml:\n  mask: null\n",
		"bool mask":      "ml:\n  mask: false\n",
	} {
		_, err := load(t, body)
		require.ErrorIs(t, err, ErrInvalidProjection, name)
	}
}

func TestProjectionPolicies(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	list := filepath.Join(dir, "mask.txt")
	require.NoError(t, os.WriteFile(list, []byte("# private\nAcmeBank\n"), 0o600))

	p, err := ML{ScreenLines: 12, Mask: screenproj.DomainMaskNone}.ProjectionPolicies()
	require.NoError(t, err)
	require.NoError(t, p.Validate())
	require.Equal(t, 12, p.Lines)
	require.Equal(t, screenproj.DomainMaskNone, p.Mask.Version())

	p, err = ML{ScreenLines: 40, Mask: list}.ProjectionPolicies()
	require.NoError(t, err)
	want, err := screenproj.NewMask([]string{"AcmeBank"})
	require.NoError(t, err)
	require.Equal(t, want.Version(), p.Mask.Version(), "the list ml.mask names")

	empty := filepath.Join(dir, "empty.txt")
	require.NoError(t, os.WriteFile(empty, []byte("# no name\n"), 0o600))
	for name, m := range map[string]ML{
		"both absent":      {},
		"lines absent":     {Mask: screenproj.DomainMaskNone},
		"mask absent":      {ScreenLines: 40},
		"too many lines":   {ScreenLines: 41, Mask: screenproj.DomainMaskNone},
		"a relative mask":  {ScreenLines: 40, Mask: "mask.txt"},
		"a list with none": {ScreenLines: 40, Mask: empty},
	} {
		_, err := m.ProjectionPolicies()
		require.ErrorIs(t, err, ErrInvalidProjection, name)
	}
	_, err = ML{ScreenLines: 40, Mask: filepath.Join(dir, "missing.txt")}.ProjectionPolicies()
	require.ErrorIs(t, err, os.ErrNotExist, "an unreadable list fails")
	_, err = DefaultConfig().ML.ProjectionPolicies()
	require.ErrorIs(t, err, ErrInvalidProjection, "a config without the keys has no projection")
}
