// Package adapters embeds the observe-only harness integrations that
// `gifford setup` installs, so a release binary carries them without a
// checkout. The README next to each file documents the same content.
package adapters

import _ "embed"

// ClaudeCodeHooks is the Claude Code settings object that registers the
// observer hooks.
//
//go:embed claude-code/hooks.json
var ClaudeCodeHooks []byte

// CodexHooks is the Codex hooks.json object that registers the observer
// hooks.
//
//go:embed codex/hooks.json
var CodexHooks []byte

// OpenCodePlugin is the OpenCode native-events plugin.
//
//go:embed opencode/gifford-native-events.js
var OpenCodePlugin []byte

// MusePluginManifest is the Muse Code plugin manifest.
//
//go:embed muse/.muse-plugin/plugin.json
var MusePluginManifest []byte
