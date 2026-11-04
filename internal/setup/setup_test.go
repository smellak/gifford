package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/adapters"
	"github.com/smellak/gifford/internal/profiles"
)

const (
	testGifford = "/opt/gifford/bin/gifford"
	claudeBin   = "claude"
	codexBin    = "codex"
	opencodeBin = "opencode"
)

// fakeHarnesses answers like the four harness CLIs: their versions, and the
// MCP and plugin registrations setup makes through them.
type fakeHarnesses struct {
	versions   map[string]string
	claudeMCP  bool
	musePlugin bool
	codexBroke bool
	calls      []string
}

func (f *fakeHarnesses) run(_ context.Context, name string, args ...string) ([]byte, error) {
	call := filepath.Base(name) + " " + strings.Join(args, " ")
	f.calls = append(f.calls, call)
	switch {
	case len(args) == 1 && args[0] == "--version":
		return []byte(f.versions[filepath.Base(name)] + "\n"), nil
	case call == "claude mcp get gifford":
		if f.claudeMCP {
			return nil, nil
		}
		return nil, errors.New("not found")
	case strings.HasPrefix(call, "claude mcp add "):
		f.claudeMCP = true
	case call == "claude mcp remove --scope user gifford":
		f.claudeMCP = false
	case call == "codex mcp list":
		if f.codexBroke {
			return []byte("unknown variant"), errors.New("exit 1")
		}
	case call == "muse plugins inspect "+musePluginID:
		if !f.musePlugin {
			return nil, errors.New("not installed")
		}
	case strings.HasPrefix(call, "muse plugins install "):
		f.musePlugin = true
	case call == "muse plugins remove "+musePluginID:
		f.musePlugin = false
	}
	return nil, nil
}

func testEnv(t *testing.T, fake *fakeHarnesses) Env {
	t.Helper()
	home := t.TempDir()
	loaded, err := profiles.Load()
	require.NoError(t, err)
	return Env{
		Home: home, ConfigHome: filepath.Join(home, ".config"), DataHome: filepath.Join(home, ".local", "share"),
		Gifford: testGifford,
		LookPath: func(name string) (string, error) {
			if _, ok := fake.versions[name]; ok {
				return "/usr/bin/" + name, nil
			}
			return "", errors.New("not found")
		},
		Run:      fake.run,
		Now:      func() time.Time { return time.Date(2026, time.November, 4, 16, 0, 0, 0, time.UTC) },
		Profiles: loaded,
	}
}

func measuredVersions() map[string]string {
	return map[string]string{
		claudeBin:   "2.1.280 (Claude Code)",
		codexBin:    "codex-cli 0.154.0",
		opencodeBin: "1.18.29",
		"muse":      "warning: update check failed\nMuse Code 1.4.13 (1.4.13-R6996.1)",
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	obj, err := readJSONObject(path)
	require.NoError(t, err)
	return obj
}

func TestSetupMergesKeepsUserSettingsAndUndoes(t *testing.T) {
	fake := &fakeHarnesses{versions: measuredVersions()}
	env := testEnv(t, fake)
	claudeSettings := filepath.Join(env.Home, ".claude", "settings.json")
	userSettings := `{"model":"opus","hooks":{"Stop":[{"hooks":[{"type":"command","command":"notify-me"}]}]},` +
		`"permissions":{"allow":["Bash(ls:*)"]}}`
	writeFile(t, claudeSettings, userSettings)
	codexConfig := filepath.Join(env.Home, ".codex", "config.toml")
	userToml := "model = \"gpt-5\"\n[profiles.fast]\nmodel = \"mini\""
	writeFile(t, codexConfig, userToml)

	plan, err := NewPlan(t.Context(), env)
	require.NoError(t, err)
	require.Len(t, plan.Harnesses, 4)
	for _, h := range plan.Harnesses {
		require.True(t, h.Measured, h.Name)
		require.True(t, h.Observer, h.Name)
		require.True(t, h.AutomaticDelivery(), h.Name)
	}
	var report bytes.Buffer
	WriteReport(&report, plan, false)
	require.Contains(t, report.String(), "muse-code    1.4.13-R6996.1     MEASURED    true")

	var consented []string
	_, err = Apply(t.Context(), env, plan, func(step Step) bool {
		consented = append(consented, step.Harness)
		return true
	}, &bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, []string{"claude-code", codexBin}, consented, "only the tool pre-approvals ask")

	settings := readJSON(t, claudeSettings)
	require.Equal(t, "opus", settings["model"])
	allow := settings["permissions"].(map[string]any)["allow"].([]any)
	require.Equal(t, []any{
		"Bash(ls:*)", "mcp__gifford__send_message", "mcp__gifford__list_pending_messages",
		"mcp__gifford__read_message", "mcp__gifford__ack_message",
	}, allow)
	stop := settings["hooks"].(map[string]any)["Stop"].([]any)
	require.Len(t, stop, 2, "the user's own Stop hook stays")
	require.Len(t, settings["hooks"].(map[string]any), 12)
	require.True(t, fake.claudeMCP)
	require.Equal(t, map[string]any{"DISABLE_AUTOUPDATER": "1"}, settings["env"], "the measured version stays")

	toml, err := os.ReadFile(codexConfig)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(toml), "check_for_update_on_startup = false\n"+userToml+"\n"))
	require.Contains(t, string(toml), "[mcp_servers.gifford]\ncommand = \""+testGifford+"\"\nargs = [\"mcp\"]\n")
	for _, tool := range MessageTools {
		require.Contains(t, string(toml), "[mcp_servers.gifford.tools."+tool+"]\napproval_mode = \"approve\"\n")
	}
	require.Len(t, readJSON(t, filepath.Join(env.Home, ".codex", "hooks.json"))["hooks"].(map[string]any), 9)
	agents, err := os.ReadFile(filepath.Join(env.Home, ".codex", "AGENTS.md"))
	require.NoError(t, err)
	require.Contains(t, string(agents), "list_pending_messages")

	opencode := readJSON(t, filepath.Join(env.ConfigHome, "opencode", "opencode.json"))
	require.Equal(t, map[string]any{"type": "local", "command": []any{testGifford, "mcp"}, "enabled": true},
		opencode["mcp"].(map[string]any)["gifford"])
	require.Equal(t, false, opencode["autoupdate"])
	plugin, err := os.ReadFile(filepath.Join(env.ConfigHome, "opencode", "plugins", "gifford-native-events.js"))
	require.NoError(t, err)
	require.NotEmpty(t, plugin)
	muse := readJSON(t, filepath.Join(env.ConfigHome, "muse", "settings.json"))
	require.Equal(t, map[string]any{"type": "stdio", "command": testGifford, "args": []any{"mcp"}},
		muse["mcpServers"].(map[string]any)["gifford"])
	require.True(t, fake.musePlugin)

	again, err := NewPlan(t.Context(), env)
	require.NoError(t, err)
	for _, step := range again.Steps {
		require.True(t, step.Done(), "%s: %s", step.Harness, step.Description)
	}

	_, err = Undo(t.Context(), env, &bytes.Buffer{})
	require.NoError(t, err)
	restored, err := os.ReadFile(claudeSettings)
	require.NoError(t, err)
	require.Equal(t, userSettings, string(restored))
	restoredToml, err := os.ReadFile(codexConfig)
	require.NoError(t, err)
	require.Equal(t, userToml, string(restoredToml))
	for _, created := range []string{
		filepath.Join(env.Home, ".codex", "hooks.json"),
		filepath.Join(env.Home, ".codex", "AGENTS.md"),
		filepath.Join(env.ConfigHome, "opencode", "opencode.json"),
		filepath.Join(env.ConfigHome, "muse", "settings.json"),
	} {
		_, err := os.Stat(created)
		require.ErrorIs(t, err, os.ErrNotExist, created)
	}
	require.False(t, fake.claudeMCP)
	require.False(t, fake.musePlugin)
	_, err = Undo(t.Context(), env, &bytes.Buffer{})
	require.ErrorIs(t, err, ErrNothingToUndo)
}

func TestSetupWithoutConsentPreApprovesNothing(t *testing.T) {
	fake := &fakeHarnesses{versions: measuredVersions()}
	env := testEnv(t, fake)
	plan, err := NewPlan(t.Context(), env)
	require.NoError(t, err)
	_, err = Apply(t.Context(), env, plan, func(Step) bool { return false }, &bytes.Buffer{})
	require.NoError(t, err)
	settings := readJSON(t, filepath.Join(env.Home, ".claude", "settings.json"))
	require.NotContains(t, settings, "permissions")
	toml, err := os.ReadFile(filepath.Join(env.Home, ".codex", "config.toml"))
	require.NoError(t, err)
	require.NotContains(t, string(toml), "approval_mode")
}

func TestSetupLeavesUnmeasuredBuildsOnApprovalWithoutObservers(t *testing.T) {
	fake := &fakeHarnesses{versions: map[string]string{
		claudeBin: "2.1.300 (Claude Code)", "muse": "Muse Code 1.4.15 (1.4.15-R8000.1)",
	}}
	env := testEnv(t, fake)
	plan, err := NewPlan(t.Context(), env)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{codexBin, "opencode"}, plan.Missing)
	for _, h := range plan.Harnesses {
		require.False(t, h.Measured, h.Name)
		require.False(t, h.AutomaticDelivery(), h.Name)
		require.False(t, h.Observer, h.Name)
	}
	for _, step := range plan.Steps {
		require.NotContains(t, step.Description, "hooks", "no observer for an unsupported version")
		require.NotContains(t, step.Description, "plugin")
	}
	var report bytes.Buffer
	WriteReport(&report, plan, false)
	require.Contains(t, report.String(), "claude-code  2.1.300            UNMEASURED  false")
}

func TestSetupRevertsATomlCodexCannotLoad(t *testing.T) {
	fake := &fakeHarnesses{versions: map[string]string{codexBin: "codex-cli 0.154.0"}, codexBroke: true}
	env := testEnv(t, fake)
	config := filepath.Join(env.Home, ".codex", "config.toml")
	writeFile(t, config, "model = \"gpt-5\"\n")
	plan, err := NewPlan(t.Context(), env)
	require.NoError(t, err)
	_, err = Apply(t.Context(), env, plan, func(Step) bool { return true }, &bytes.Buffer{})
	require.ErrorContains(t, err, "change reverted")
	raw, err := os.ReadFile(config)
	require.NoError(t, err)
	require.Equal(t, "model = \"gpt-5\"\n", string(raw))
}

func TestSetupRefusesAFileItCannotMergeExactly(t *testing.T) {
	fake := &fakeHarnesses{versions: map[string]string{claudeBin: "2.1.280 (Claude Code)"}}
	env := testEnv(t, fake)
	settings := filepath.Join(env.Home, ".claude", "settings.json")
	writeFile(t, settings, "// my notes\n{\"model\":\"opus\"}\n")
	plan, err := NewPlan(t.Context(), env)
	require.NoError(t, err)
	_, err = Apply(t.Context(), env, plan, func(Step) bool { return true }, &bytes.Buffer{})
	require.ErrorContains(t, err, "merge it by hand")
	raw, err := os.ReadFile(settings)
	require.NoError(t, err)
	require.Equal(t, "// my notes\n{\"model\":\"opus\"}\n", string(raw))
}

func TestEmbeddedHooksMatchTheAdapterReadmes(t *testing.T) {
	for name, hooks := range map[string][]byte{
		"claude-code": adapters.ClaudeCodeHooks, "codex": adapters.CodexHooks,
	} {
		readme, err := os.ReadFile(filepath.Join("..", "..", "adapters", name, "README.md"))
		require.NoError(t, err)
		block := strings.SplitN(strings.SplitN(string(readme), "## Install", 2)[1], "```json\n", 2)[1]
		block = strings.SplitN(block, "```", 2)[0]
		var fromReadme, embedded any
		require.NoError(t, json.Unmarshal([]byte(block), &fromReadme))
		require.NoError(t, json.Unmarshal(hooks, &embedded))
		require.Equal(t, fromReadme, embedded, name)
	}
}

// OpenCode reads opencode.json and opencode.jsonc together, the jsonc
// winning. Setup writes the plain file, leaves the jsonc byte for byte,
// and refuses a key the jsonc would override.
func TestSetupOpenCodeBesideAJSONC(t *testing.T) {
	t.Parallel()
	setup := func(t *testing.T, jsonc string) (Env, string, string) {
		env := testEnv(t, &fakeHarnesses{versions: map[string]string{opencodeBin: "1.18.29"}})
		dir := filepath.Join(env.ConfigHome, "opencode")
		writeFile(t, filepath.Join(dir, "opencode.jsonc"), jsonc)
		return env, filepath.Join(dir, "opencode.json"), filepath.Join(dir, "opencode.jsonc")
	}
	t.Run("installer file", func(t *testing.T) {
		t.Parallel()
		jsonc := "{\n  // written by the installer\n  \"$schema\": \"https://opencode.ai/config.json\", /* keep */\n}\n"
		env, config, path := setup(t, jsonc)
		plan, err := NewPlan(t.Context(), env)
		require.NoError(t, err)
		require.Empty(t, plan.Notes)
		_, err = Apply(t.Context(), env, plan, nil, &bytes.Buffer{})
		require.NoError(t, err)
		obj := readJSON(t, config)
		require.Equal(t, false, obj["autoupdate"])
		require.Contains(t, obj["mcp"].(map[string]any), serverName)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.True(t, bytes.Equal([]byte(jsonc), raw), "the jsonc stays byte for byte")
	})
	t.Run("another gifford server", func(t *testing.T) {
		t.Parallel()
		env, config, _ := setup(t, `{"mcp": {"gifford": {"type": "local", "command": ["/old/gifford", "mcp"]}}}`)
		plan, err := NewPlan(t.Context(), env)
		require.NoError(t, err)
		require.Empty(t, plan.Steps)
		require.Len(t, plan.Notes, 1)
		require.Contains(t, plan.Notes[0], "overrides "+config)
	})
	t.Run("updates on", func(t *testing.T) {
		t.Parallel()
		env, config, _ := setup(t, "{\"autoupdate\": true,}")
		plan, err := NewPlan(t.Context(), env)
		require.NoError(t, err)
		require.Len(t, plan.Notes, 1)
		require.Contains(t, plan.Notes[0], `sets "autoupdate"`)
		_, err = Apply(t.Context(), env, plan, nil, &bytes.Buffer{})
		require.NoError(t, err)
		obj := readJSON(t, config)
		require.Contains(t, obj["mcp"].(map[string]any), serverName, "the server still goes in")
		require.NotContains(t, obj, "autoupdate")
	})
	t.Run("unreadable", func(t *testing.T) {
		t.Parallel()
		env, config, _ := setup(t, "{ not json")
		plan, err := NewPlan(t.Context(), env)
		require.NoError(t, err)
		require.Empty(t, plan.Steps)
		require.Contains(t, plan.Notes[0], "leaves OpenCode as it is")
		require.NoFileExists(t, config)
	})
}

func TestStripJSONCKeepsStrings(t *testing.T) {
	t.Parallel()
	raw := "{\"url\": \"http://x/*y*/\", // c\n \"list\": [1, 2,], \"s\": \"a,]\\\"//\", /* d */}"
	var obj map[string]any
	require.NoError(t, json.Unmarshal(stripJSONC([]byte(raw)), &obj))
	require.Equal(t, "http://x/*y*/", obj["url"])
	require.Equal(t, "a,]\"//", obj["s"])
	require.Len(t, obj["list"], 2)
}
