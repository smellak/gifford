package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/notifications"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/screenproj"
)

const (
	configTestInteger      = "integer"
	configTestList         = "list"
	configTestNull         = "null"
	configTestUnknownField = "unknown field"
	configTestUnknownKey   = "unknown key"
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

func TestLoadChannelBudgetProfile(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	for name, body := range map[string]string{
		"absent":       "schema_version: 1\n",
		"empty object": "channel: {}\n",
	} {
		cfg, err := load(t, body)
		require.NoError(t, err, name)
		require.Equal(t, protocol.BudgetProfileDefault, cfg.Channel.BudgetProfile, name)
	}
	for _, profile := range []protocol.BudgetProfile{
		protocol.BudgetProfileDemo,
		protocol.BudgetProfileDefault,
		protocol.BudgetProfileStrict,
	} {
		cfg, err := load(t, "channel:\n  budget_profile: "+string(profile)+"\n")
		require.NoError(t, err)
		require.Equal(t, profile, cfg.Channel.BudgetProfile)
	}

	for name, body := range map[string]string{
		"empty":                "channel:\n  budget_profile: \"\"\n",
		"unknown":              "channel:\n  budget_profile: generous\n",
		"custom":               "channel:\n  budget_profile: custom\n",
		configTestInteger:      "channel:\n  budget_profile: 1\n",
		configTestNull:         "channel:\n  budget_profile: null\n",
		configTestList:         "channel:\n  budget_profile: [default]\n",
		configTestUnknownField: "channel:\n  profile: default\n",
		"null channel":         "channel: null\n",
		"scalar channel":       "channel: default\n",
		"list channel":         "channel: []\n",
	} {
		_, err := load(t, body)
		require.Error(t, err, name)
		require.ErrorIs(t, err, ErrInvalidChannel, name)
	}
}

func TestLoadBridgeNoticeLine(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	const validLine = "[GIFFORD] 2 pending messages: check your inbox"

	// wantErr marks validation failures; strict marks decoder-level
	// failures, which carry no sentinel.
	cases := []struct {
		name    string
		body    string
		want    string
		wantErr bool
		strict  bool
	}{
		{name: "absent"},
		{name: "empty object", body: "bridge: {}\n"},
		{name: "null bridge", body: "bridge: null\n"},
		{name: "custom", body: "bridge:\n  notice_line: \"" + validLine + "\"\n", want: validLine},
		{name: "empty value", body: "bridge:\n  notice_line: \"\"\n", wantErr: true},
		{name: "semicolon", body: "bridge:\n  notice_line: \"[GIFFORD] pending; check\"\n", wantErr: true},
		{name: "newline", body: "bridge:\n  notice_line: |\n    [GIFFORD] pending\n    check\n", wantErr: true},
		{name: configTestInteger, body: "bridge:\n  notice_line: 1\n", wantErr: true},
		{name: "null value", body: "bridge:\n  notice_line: null\n", wantErr: true},
		{name: "list value", body: "bridge:\n  notice_line: [inbox]\n", wantErr: true},
		{name: "unknown bridge field", body: "bridge:\n  template: inbox\n", wantErr: true, strict: true},
		{name: "scalar bridge", body: "bridge: inbox\n", wantErr: true, strict: true},
		{name: "slice bridge", body: "bridge: []\n", wantErr: true, strict: true},
	}
	for _, tc := range cases {
		body := tc.body
		if body == "" {
			body = "schema_version: 1\n"
		}
		cfg, err := load(t, body)
		if !tc.wantErr {
			require.NoError(t, err, tc.name)
			if tc.want == "" {
				require.Empty(t, cfg.Bridge.NoticeLine, tc.name)
			} else {
				require.Equal(t, tc.want, cfg.Bridge.NoticeLine, tc.name)
			}
			continue
		}
		require.Error(t, err, tc.name)
		if !tc.strict {
			require.ErrorIs(t, err, ErrInvalidBridge, tc.name)
		}
	}
}

func TestLoadTmuxAttachHumanActivityWindow(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	cfg, err := load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, cfg.TmuxAttach.HumanActivityWindow)

	cfg, err = load(t, "tmux_attach:\n  human_activity_window: 30s\n")
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, cfg.TmuxAttach.HumanActivityWindow)

	for name, body := range map[string]string{
		"zero":                 "tmux_attach:\n  human_activity_window: 0s\n",
		"negative":             "tmux_attach:\n  human_activity_window: -1s\n",
		"invalid":              "tmux_attach:\n  human_activity_window: soon\n",
		configTestInteger:      "tmux_attach:\n  human_activity_window: 15\n",
		configTestUnknownField: "tmux_attach:\n  activity_window: 15s\n",
	} {
		_, err := load(t, body)
		require.Error(t, err, name)
		if name != configTestUnknownField {
			require.ErrorIs(t, err, ErrInvalidTmuxAttach, name)
		}
	}
}

func TestLoadTmuxAttachHelperRevision(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	cfg, err := load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.Empty(t, cfg.TmuxAttach.HelperManifestSHA256, "unset keeps the raw tmux path")

	revision := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	cfg, err = load(t, "tmux_attach:\n  helper_manifest_sha256: "+revision+"\n")
	require.NoError(t, err)
	require.Equal(t, revision, cfg.TmuxAttach.HelperManifestSHA256)

	for name, body := range map[string]string{
		"short":     "tmux_attach:\n  helper_manifest_sha256: abc123\n",
		"uppercase": "tmux_attach:\n  helper_manifest_sha256: 0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef\n",
		"non-hex":   "tmux_attach:\n  helper_manifest_sha256: z123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd\n",
	} {
		_, err := load(t, body)
		require.ErrorIs(t, err, ErrInvalidTmuxAttach, name)
	}
}

func TestLoadTmuxLocksRequiresClosedConfiguration(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	cfg, err := load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.False(t, cfg.TmuxLocks.Enabled)

	const valid = `tmux_locks:
  peer_owner: peer_writer
  peer_writers:
    - peer_dispatch
    - peer_writer
  host: host-7
  mode_path: /etc/example/pane-mode
  admin_uids_path: /etc/example/pane-admin-uids
  operation_schema: example-pane-op/1
  initial_take_drain: 30s
`
	cfg, err = load(t, valid)
	require.NoError(t, err)
	require.Equal(t, TmuxLocks{
		Enabled: true, PeerOwner: "peer_writer",
		PeerWriters: []string{"peer_dispatch", "peer_writer"}, Host: "host-7",
		ModePath: "/etc/example/pane-mode", AdminUIDsPath: "/etc/example/pane-admin-uids",
		OperationSchema: "example-pane-op/1", InitialTakeDrain: 30 * time.Second,
	}, cfg.TmuxLocks)

	for name, body := range map[string]string{
		"node owner":    "tmux_locks:\n  peer_owner: gifford\n",
		"blank owner":   "tmux_locks:\n  peer_owner: \"\"\n",
		"uppercase":     strings.Replace(valid, "peer_writer", "PeerWriter", 1),
		"integer":       "tmux_locks:\n  peer_owner: 1\n",
		"unknown field": valid + "  owner: peer_writer\n",
		"missing host":  strings.Replace(valid, "  host: host-7\n", "", 1),
		"missing writers": strings.Replace(valid,
			"  peer_writers:\n    - peer_dispatch\n    - peer_writer\n", "", 1),
		"unordered writers": strings.Replace(valid,
			"    - peer_dispatch\n    - peer_writer\n", "    - peer_writer\n    - peer_dispatch\n", 1),
		"node writer":   strings.Replace(valid, "    - peer_dispatch\n", "    - provision\n", 1),
		"relative mode": strings.Replace(valid, "/etc/example/pane-mode", "pane-mode", 1),
		"root mode":     strings.Replace(valid, "/etc/example/pane-mode", "/", 1),
		"root admins":   strings.Replace(valid, "/etc/example/pane-admin-uids", "/", 1),
		"empty admins":  strings.Replace(valid, "/etc/example/pane-admin-uids", "", 1),
		"bad schema":    strings.Replace(valid, "example-pane-op/1", "Example Pane", 1),
		"zero drain":    strings.Replace(valid, "30s", "0s", 1),
	} {
		_, err := load(t, body)
		require.Error(t, err, name)
		if name != "unknown field" {
			require.ErrorIs(t, err, ErrInvalidTmuxLocks, name)
		}
	}
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
		configTestUnknownKey: "ml:\n  readiness:\n    train_minimum: 2000\n",
		"unknown ml key":     "ml:\n  other: 1\n",
		"negative":           "ml:\n  readiness:\n    test_min: -1\n",
		"float":              "ml:\n  readiness:\n    test_min: 1.5\n",
		"string":             "ml:\n  readiness:\n    test_min: \"300\"\n",
		configTestNull:       "ml:\n  readiness:\n    test_min: null\n",
		configTestList:       "ml:\n  readiness:\n    test_min: [1]\n",
		"percent above 100":  "ml:\n  readiness:\n    label_share_min_percent: 101\n",
	} {
		_, err := load(t, body)
		require.Error(t, err, name)
		if name != configTestUnknownKey && name != "unknown ml key" {
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

func TestLoadPermissionDialogSources(t *testing.T) {
	t.Parallel()
	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}
	configured := "ml:\n  permission_dialog_sources:\n" +
		"    - harness: opencode\n" +
		"      harness_version: opencode 1.18.29\n"
	cfg, err := load(t, configured)
	require.NoError(t, err)
	require.Equal(t, []session.PermissionDialogSource{{
		Harness: protocol.HarnessOpencode, HarnessVersion: "opencode 1.18.29",
	}}, cfg.ML.PermissionDialogSources)

	cfg, err = load(t, "ml:\n  permission_dialog_sources: []\n")
	require.NoError(t, err)
	require.Empty(t, cfg.ML.PermissionDialogSources, "the feature is off by default and for an empty list")

	for name, body := range map[string]string{
		"not a list":           "ml:\n  permission_dialog_sources: opencode\n",
		"missing version":      "ml:\n  permission_dialog_sources:\n    - harness: opencode\n",
		configTestUnknownField: "ml:\n  permission_dialog_sources:\n    - harness: opencode\n      version: 1.18.29\n",
		"unmeasured version":   "ml:\n  permission_dialog_sources:\n    - harness: opencode\n      harness_version: 1.18.30\n",
		"unsupported harness":  "ml:\n  permission_dialog_sources:\n    - harness: muse-code\n      harness_version: 1.4.13-R6996.1\n",
		"duplicate":            configured + "    - harness: opencode\n      harness_version: opencode 1.18.29\n",
	} {
		_, err := load(t, body)
		require.ErrorIs(t, err, ErrInvalidPermissionDialogSources, name)
	}
}

func TestLoadStateAnalysis(t *testing.T) {
	t.Parallel()

	digest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	body := "state_analysis:\n" +
		"  gateway_config: " + path + "\n" +
		"  profile_revision: analyst-profile-v1\n" +
		"  profile_sha256: " + digest + "\n" +
		"  calibration_revision: calibration-v1\n" +
		"  calibration_sha256: " + digest + "\n" +
		"  temperature: 1.25\n" +
		"  idle_threshold: 0.9\n"
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(body), 0o600))
	cfg, err := Load(configPath)
	require.NoError(t, err)
	require.Equal(t, &StateAnalysis{
		GatewayConfig: path, ProfileRevision: "analyst-profile-v1",
		ProfileSHA256: protocol.SHA256Hex(digest), CalibrationRevision: "calibration-v1",
		CalibrationSHA256: protocol.SHA256Hex(digest), Temperature: 1.25, IdleThreshold: 0.9,
	}, cfg.StateAnalysis)

	without, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	require.NoError(t, err)
	require.Nil(t, without.StateAnalysis)
}

func TestLoadInvalidStateAnalysis(t *testing.T) {
	t.Parallel()

	digest := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	valid := "  gateway_config: /etc/gifford/gateway.yaml\n" +
		"  profile_revision: profile-v1\n" +
		"  profile_sha256: " + digest + "\n" +
		"  calibration_revision: calibration-v1\n" +
		"  calibration_sha256: " + digest + "\n" +
		"  temperature: 1\n" +
		"  idle_threshold: 0.9\n"
	for name, fields := range map[string]string{
		"missing field":       strings.Replace(valid, "  profile_revision: profile-v1\n", "", 1),
		"relative gateway":    strings.Replace(valid, "/etc/gifford/gateway.yaml", "gateway.yaml", 1),
		"bad profile digest":  strings.Replace(valid, digest, "bad", 1),
		"zero temperature":    strings.Replace(valid, "temperature: 1", "temperature: 0", 1),
		"threshold above one": strings.Replace(valid, "idle_threshold: 0.9", "idle_threshold: 1.1", 1),
		configTestUnknownKey:  valid + "  endpoint: secret\n",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte("state_analysis:\n"+fields), 0o600))
		_, err := Load(path)
		require.Error(t, err, name)
		if name != configTestUnknownKey {
			require.ErrorIs(t, err, ErrInvalidStateAnalysis, name)
		}
	}
}

func TestLoadNodeSocket(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	cfg, err := load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.Nil(t, cfg.NodeSocket, "absent keeps the node private")

	cfg, err = load(t, "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 998\n")
	require.NoError(t, err)
	require.Equal(t, &NodeSocket{Path: "/run/gifford/node.sock", BridgeUID: 998}, cfg.NodeSocket)

	for name, body := range map[string]string{
		"no bridge uid":      "node_socket:\n  path: /run/gifford/node.sock\n",
		"no path":            "node_socket:\n  bridge_uid: 998\n",
		"root bridge":        "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 0\n",
		"negative uid":       "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: -1\n",
		"uid out of range":   "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 4294967296\n",
		"hexadecimal uid":    "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 0x3e6\n",
		"zero-padded uid":    "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 0998\n",
		"user name":          "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: bridge\n",
		"quoted uid":         "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: \"998\"\n",
		"relative path":      "node_socket:\n  path: run/node.sock\n  bridge_uid: 998\n",
		"unclean path":       "node_socket:\n  path: /run/gifford/../node.sock\n  bridge_uid: 998\n",
		"path too long":      "node_socket:\n  path: /" + strings.Repeat("a", 107) + "\n  bridge_uid: 998\n",
		"integer path":       "node_socket:\n  path: 1\n  bridge_uid: 998\n",
		"empty node socket":  "node_socket: {}\n",
		"null node socket":   "node_socket:\n  path: null\n  bridge_uid: 998\n",
		"uid list":           "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: [998]\n",
		"fractional uid":     "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 998.0\n",
		"uid with plus sign": "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: +998\n",
	} {
		_, err := load(t, body)
		require.ErrorIs(t, err, ErrInvalidNodeSocket, name)
	}

	_, err = load(t, "node_socket:\n  path: /run/gifford/node.sock\n  bridge_uid: 998\n  group: gifford\n")
	require.Error(t, err, "an unknown field fails at the strict decoder")
}

func TestLoadNodeSocketReservesNativeEntryPath(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, socketPath string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		body := "node_socket:\n  path: " + socketPath + "\n  bridge_uid: 998\n"
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	fitDir := "/" + strings.Repeat("a", 94)
	fitNodePath := filepath.Join(fitDir, "n")
	fitNativePath := filepath.Join(fitDir, nativeevents.NodeSocketName)
	require.Len(t, fitNativePath, maxSocketPath)
	cfg, err := load(t, fitNodePath)
	require.NoError(t, err, "a derived native socket at the exact sun_path limit must fit")
	require.Equal(t, fitNodePath, cfg.NodeSocket.Path)

	overflowDir := fitDir + "a"
	overflowNodePath := filepath.Join(overflowDir, "n")
	overflowNativePath := filepath.Join(overflowDir, nativeevents.NodeSocketName)
	require.LessOrEqual(t, len(overflowNodePath), maxSocketPath, "the configured node socket itself still fits")
	require.Len(t, overflowNativePath, maxSocketPath+1)
	_, err = load(t, overflowNodePath)
	require.ErrorIs(t, err, ErrInvalidNodeSocket)
	require.ErrorContains(t, err, "native.sock path of 108 bytes; must be at most 107 bytes")
}

func TestLoadTmuxInstances(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		return Load(path)
	}

	cfg, err := load(t, "schema_version: 1\n")
	require.NoError(t, err)
	require.Equal(t, "/etc/gifford/instances.yaml", cfg.TmuxInstances.StatementsPath("/etc/gifford"),
		"absent reads instances.yaml in the config dir")

	cfg, err = load(t, "tmux_instances:\n  statements: /srv/gifford/instances.yaml\n")
	require.NoError(t, err)
	require.Equal(t, "/srv/gifford/instances.yaml", cfg.TmuxInstances.StatementsPath("/etc/gifford"))

	for name, body := range map[string]string{
		"relative path":  "tmux_instances:\n  statements: gifford/instances.yaml\n",
		"unclean path":   "tmux_instances:\n  statements: /etc/gifford/../instances.yaml\n",
		"empty path":     "tmux_instances:\n  statements: \"\"\n",
		"integer path":   "tmux_instances:\n  statements: 1\n",
		"null path":      "tmux_instances:\n  statements: null\n",
		"list of paths":  "tmux_instances:\n  statements: [/etc/gifford/instances.yaml]\n",
		"trailing slash": "tmux_instances:\n  statements: /etc/gifford/\n",
		"double slash":   "tmux_instances:\n  statements: /etc//gifford/instances.yaml\n",
	} {
		_, err := load(t, body)
		require.ErrorIs(t, err, ErrInvalidTmuxInstances, name)
	}

	_, err = load(t, "tmux_instances:\n  statements: /etc/gifford/instances.yaml\n  excluded: [a]\n")
	require.Error(t, err, "an unknown field fails at the strict decoder")
}
