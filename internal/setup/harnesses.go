package setup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/smellak/gifford/adapters"
)

const (
	serverName       = "gifford"
	musePluginID     = "gifford-muse-adapter"
	instructionBegin = "<!-- gifford:begin -->"
	instructionEnd   = "<!-- gifford:end -->"
)

// noticeInstructions tells a harness what Gifford's notice line means.
const noticeInstructions = instructionBegin + `
When a line beginning with [GIFFORD] or : [GIFFORD] appears in your input,
messages are waiting for you. Call the gifford MCP tools: list_pending_messages,
then read_message for each message, then ack_message with status ACKNOWLEDGED,
and reply with send_message unless the message says no response is needed.
` + instructionEnd + "\n"

func observerNote(h Harness, supported string) string {
	return fmt.Sprintf("observer skipped: it supports exactly %s, found %q", supported, h.Version)
}

// claudePlan registers the MCP server through Claude Code's own CLI, merges
// the observer hooks into ~/.claude/settings.json and, with consent, allows
// the four message tools there.
func claudePlan(env Env, h Harness) ([]Step, []string, error) {
	settings := filepath.Join(env.Home, ".claude", "settings.json")
	var steps []Step
	var notes []string
	_, getErr := env.Run(context.Background(), h.Binary, "mcp", "get", serverName)
	steps = append(steps, Step{
		Description: "register the gifford MCP server (claude mcp add --scope user)",
		done:        getErr == nil,
		apply: func(ctx context.Context, s *applyState) error {
			out, err := s.env.Run(ctx, h.Binary, "mcp", "add", "--scope", "user", serverName, "--", s.env.Gifford, "mcp")
			if err != nil {
				return fmt.Errorf("claude mcp add: %w: %s", err, firstLine(out))
			}
			return s.addUndo(h.Binary, "mcp", "remove", "--scope", "user", serverName)
		},
	})
	if h.Observer {
		steps = append(steps, Step{
			Description: "merge the observe-only hooks into " + settings,
			Files:       []string{settings},
			done:        hooksPresent(settings, adapters.ClaudeCodeHooks),
			apply: func(_ context.Context, _ *applyState) error {
				obj, err := readJSONObject(settings)
				if err != nil {
					return err
				}
				if _, err := mergeHooks(obj, adapters.ClaudeCodeHooks, settings); err != nil {
					return err
				}
				return writeJSONObject(settings, obj)
			},
		})
	} else {
		notes = append(notes, observerNote(h, "2.1.280"))
	}
	allow := make([]string, len(MessageTools))
	for i, tool := range MessageTools {
		allow[i] = "mcp__" + serverName + "__" + tool
	}
	steps = append(steps, Step{
		Consent:     true,
		Description: "allow exactly Gifford's four message tools in " + settings,
		Files:       []string{settings},
		done:        claudeAllowed(settings, allow),
		apply: func(_ context.Context, _ *applyState) error {
			obj, err := readJSONObject(settings)
			if err != nil {
				return err
			}
			permissions, err := object(obj, "permissions", settings)
			if err != nil {
				return err
			}
			if _, err := appendMissing(permissions, "allow", settings+": permissions", allow); err != nil {
				return err
			}
			return writeJSONObject(settings, obj)
		},
	})
	return steps, notes, nil
}

func claudeAllowed(settings string, allow []string) bool {
	obj, err := readJSONObject(settings)
	if err != nil {
		return false
	}
	permissions, ok := obj["permissions"].(map[string]any)
	if !ok {
		return false
	}
	changed, err := appendMissing(permissions, "allow", settings, allow)
	return err == nil && !changed
}

func codexHome(env Env) string {
	if env.CodexHome != "" {
		return env.CodexHome
	}
	return filepath.Join(env.Home, ".codex")
}

func tomlString(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}

func tomlHasTable(raw []byte, table string) bool {
	pattern := regexp.MustCompile(`(?m)^\s*\[\s*` + regexp.QuoteMeta(table) + `\s*\]\s*(#.*)?$`)
	return pattern.Match(raw)
}

// appendTOML appends block to config.toml, then checks that Codex still
// loads its configuration; if it does not, the file is put back as it was.
func appendTOML(ctx context.Context, env Env, binary, path, block string) error {
	before, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read %s: %w", path, err)
	}
	next := append([]byte{}, before...)
	if len(next) > 0 && !bytes.HasSuffix(next, []byte("\n")) {
		next = append(next, '\n')
	}
	next = append(next, '\n')
	next = append(next, block...)
	if err := writeFileAtomic(path, next, fileMode(path)); err != nil {
		return err
	}
	if out, err := env.Run(ctx, binary, "mcp", "get", serverName); err != nil {
		if before == nil {
			_ = os.Remove(path)
		} else {
			_ = writeFileAtomic(path, before, fileMode(path))
		}
		return fmt.Errorf("codex no longer loads %s, change reverted: %w: %s", path, err, firstLine(out))
	}
	return nil
}

// codexPlan adds the MCP server and, with consent, per-tool approvals to
// config.toml, merges the observer hooks into hooks.json, and adds the
// notice instructions to the global AGENTS.md.
func codexPlan(env Env, h Harness) ([]Step, []string, error) {
	home := codexHome(env)
	config := filepath.Join(home, "config.toml")
	hooks := filepath.Join(home, "hooks.json")
	agents := filepath.Join(home, "AGENTS.md")
	raw, err := os.ReadFile(config)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("read %s: %w", config, err)
	}
	var steps []Step
	var notes []string
	serverTable := "mcp_servers." + serverName
	steps = append(steps, Step{
		Description: "add the gifford MCP server to " + config,
		Files:       []string{config},
		done:        tomlHasTable(raw, serverTable),
		apply: func(ctx context.Context, s *applyState) error {
			block := "[" + serverTable + "]\ncommand = " + tomlString(s.env.Gifford) + "\nargs = [\"mcp\"]\n"
			return appendTOML(ctx, s.env, h.Binary, config, block)
		},
	})
	var missing []string
	for _, tool := range MessageTools {
		if !tomlHasTable(raw, serverTable+".tools."+tool) {
			missing = append(missing, tool)
		}
	}
	steps = append(steps, Step{
		Consent:     true,
		Description: "pre-approve exactly Gifford's four message tools in " + config,
		Files:       []string{config},
		done:        len(missing) == 0,
		apply: func(ctx context.Context, s *applyState) error {
			current, err := os.ReadFile(config)
			if err != nil {
				return fmt.Errorf("read %s: %w", config, err)
			}
			var block strings.Builder
			for _, tool := range MessageTools {
				table := serverTable + ".tools." + tool
				if !tomlHasTable(current, table) {
					fmt.Fprintf(&block, "[%s]\napproval_mode = \"approve\"\n\n", table)
				}
			}
			if block.Len() == 0 {
				return nil
			}
			return appendTOML(ctx, s.env, h.Binary, config, strings.TrimSuffix(block.String(), "\n"))
		},
	})
	if h.Observer {
		steps = append(steps, Step{
			Description: "merge the observe-only hooks into " + hooks,
			Files:       []string{hooks},
			done:        hooksPresent(hooks, adapters.CodexHooks),
			apply: func(_ context.Context, _ *applyState) error {
				obj, err := readJSONObject(hooks)
				if err != nil {
					return err
				}
				if _, err := mergeHooks(obj, adapters.CodexHooks, hooks); err != nil {
					return err
				}
				return writeJSONObject(hooks, obj)
			},
		})
		notes = append(notes, "review the new hooks once with /hooks inside Codex")
	} else {
		notes = append(notes, observerNote(h, "0.154.0"))
	}
	steps = append(steps, instructionStep(agents))
	return steps, notes, nil
}

// instructionStep appends the notice instructions to an instructions file.
func instructionStep(path string) Step {
	raw, _ := os.ReadFile(path)
	return Step{
		Description: "tell the harness what the notice line means in " + path,
		Files:       []string{path},
		done:        bytes.Contains(raw, []byte(instructionBegin)),
		apply: func(_ context.Context, _ *applyState) error {
			current, err := os.ReadFile(path)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("read %s: %w", path, err)
			}
			if bytes.Contains(current, []byte(instructionBegin)) {
				return nil
			}
			next := append([]byte{}, current...)
			if len(next) > 0 {
				if !bytes.HasSuffix(next, []byte("\n")) {
					next = append(next, '\n')
				}
				next = append(next, '\n')
			}
			next = append(next, noticeInstructions...)
			return writeFileAtomic(path, next, fileMode(path))
		},
	}
}

// opencodePlan adds the MCP server to opencode.json and installs the
// native-events plugin.
func opencodePlan(env Env, h Harness) ([]Step, []string, error) {
	dir := filepath.Join(env.ConfigHome, "opencode")
	config := filepath.Join(dir, "opencode.json")
	plugin := filepath.Join(dir, "plugins", "gifford-native-events.js")
	var notes []string
	if _, err := os.Stat(filepath.Join(dir, "opencode.jsonc")); err == nil {
		notes = append(notes, "opencode.jsonc found: add the gifford MCP server to it by hand (see README)")
		return nil, notes, nil
	}
	obj, readErr := readJSONObject(config)
	present := false
	if readErr == nil {
		if mcp, ok := obj["mcp"].(map[string]any); ok {
			_, present = mcp[serverName]
		}
	}
	steps := []Step{{
		Description: "add the gifford MCP server to " + config,
		Files:       []string{config},
		done:        present,
		apply: func(_ context.Context, s *applyState) error {
			obj, err := readJSONObject(config)
			if err != nil {
				return err
			}
			mcp, err := object(obj, "mcp", config)
			if err != nil {
				return err
			}
			mcp[serverName] = map[string]any{
				"type": "local", "command": []any{s.env.Gifford, "mcp"}, "enabled": true,
			}
			return writeJSONObject(config, obj)
		},
	}}
	if h.Observer {
		have, _ := os.ReadFile(plugin)
		steps = append(steps, Step{
			Description: "install the native-events plugin as " + plugin,
			Files:       []string{plugin},
			done:        bytes.Equal(have, adapters.OpenCodePlugin),
			apply: func(_ context.Context, _ *applyState) error {
				return writeFileAtomic(plugin, adapters.OpenCodePlugin, 0o600)
			},
		})
	} else {
		notes = append(notes, observerNote(h, "1.18.29"))
	}
	return steps, notes, nil
}

// musePlan adds the MCP server to Muse's settings and installs, approves and
// enables the observe-only plugin through Muse's own CLI.
func musePlan(env Env, h Harness) ([]Step, []string, error) {
	settings := filepath.Join(env.ConfigHome, "muse", "settings.json")
	bundle := filepath.Join(env.DataHome, "gifford", "adapters", "muse")
	obj, readErr := readJSONObject(settings)
	present := false
	if readErr == nil {
		if servers, ok := obj["mcpServers"].(map[string]any); ok {
			_, present = servers[serverName]
		}
	}
	steps := make([]Step, 0, 2)
	steps = append(steps, Step{
		Description: "add the gifford MCP server to " + settings,
		Files:       []string{settings},
		done:        present,
		apply: func(ctx context.Context, s *applyState) error {
			obj, err := readJSONObject(settings)
			if err != nil {
				return err
			}
			servers, err := object(obj, "mcpServers", settings)
			if err != nil {
				return err
			}
			servers[serverName] = map[string]any{
				"type": "stdio", "command": s.env.Gifford, "args": []any{"mcp"},
			}
			// Muse refuses a settings file without its schema version.
			if _, ok := obj["schema_version"]; !ok {
				obj["schema_version"] = json.Number("1")
			}
			before, readErr := os.ReadFile(settings)
			if err := writeJSONObject(settings, obj); err != nil {
				return err
			}
			if out, err := s.env.Run(ctx, h.Binary, "plugins", "list"); err != nil {
				if errors.Is(readErr, os.ErrNotExist) {
					_ = os.Remove(settings)
				} else {
					_ = writeFileAtomic(settings, before, fileMode(settings))
				}
				return fmt.Errorf("muse no longer loads %s, change reverted: %w: %s", settings, err, firstLine(out))
			}
			return nil
		},
	})
	notes := []string{"Muse has no per-tool approval setting: answer \"Always allow\" for the four gifford tools on first use"}
	if !h.Observer {
		notes = append(notes, observerNote(h, "1.4.13-R6996.1 or 1.4.14-R7309.1"))
		return steps, notes, nil
	}
	_, inspectErr := env.Run(context.Background(), h.Binary, "plugins", "inspect", musePluginID)
	manifest := filepath.Join(bundle, ".muse-plugin", "plugin.json")
	steps = append(steps, Step{
		Description: "install, approve and enable the " + musePluginID + " plugin",
		Files:       []string{manifest},
		done:        inspectErr == nil,
		apply: func(ctx context.Context, s *applyState) error {
			if err := writeFileAtomic(manifest, adapters.MusePluginManifest, 0o600); err != nil {
				return err
			}
			if out, err := s.env.Run(ctx, h.Binary, "plugins", "install", bundle, "--scope", "user"); err != nil {
				return fmt.Errorf("muse plugins install: %w: %s", err, firstLine(out))
			}
			if err := s.addUndo(h.Binary, "plugins", "remove", musePluginID); err != nil {
				return err
			}
			for _, verb := range []string{"approve", "enable"} {
				if out, err := s.env.Run(ctx, h.Binary, "plugins", verb, musePluginID); err != nil {
					return fmt.Errorf("muse plugins %s: %w: %s", verb, err, firstLine(out))
				}
			}
			return nil
		},
	})
	return steps, notes, nil
}
