package cli

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func TestVersionCommand(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&deps.Deps{})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"version"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "gifford ")
}

func TestManCommand(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&deps.Deps{})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"man"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), ".TH \"GIFFORD\"")
}

func TestHelpListsAllFirstLevelCommands(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&deps.Deps{})
	want := []string{
		"init", "doctor", "up", "down", "status", "install-systemd", "daemon",
		"run", "adapter", "mcp", "hub", "relay", "panel", "ctl", "node", "artifact",
		"capture", "keycap", "fuzz", "label", "dataset", "eval", "db",
		"version", "man",
	}
	got := make([]string, 0, len(root.Commands()))
	for _, c := range root.Commands() {
		got = append(got, c.Name())
	}
	require.ElementsMatch(t, want, got)
}

func TestRootWiringSmoke(t *testing.T) {
	t.Parallel()
	// Each command is reached through the root: a leaf without behaviour
	// yet says so, and an implemented command rejects invalid input locally.
	for args, want := range map[string]int{
		"ctl model status":           exitcode.Error,
		"ctl model rollback":         exitcode.NotImplemented,
		"capture ingest":             exitcode.NotImplemented,
		"dataset redact":             exitcode.NotImplemented,
		"node enroll-token --ttl 0s": exitcode.Error,
		"dataset build":              exitcode.Usage,
		"label weak import":          exitcode.Usage,
		"label bridge opencode":      exitcode.Usage,
		"db backup":                  exitcode.Usage,
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()
			root := NewRootCmd(&deps.Deps{})
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(strings.Fields(args))
			var ec *deps.ExitError
			require.ErrorAs(t, root.Execute(), &ec)
			require.Equal(t, want, ec.Code)
		})
	}
}

func TestAdapterEmitAlwaysSucceedsThroughRoot(t *testing.T) {
	t.Setenv("GIFFORD_ADAPTER_SOCKET", "")
	for name, extra := range map[string][]string{
		"unknown flag":    {"--bogus-flag", "1"},
		"bad sequence":    {"--seq", "abc"},
		"positional":      {"unexpected"},
		"emitter version": {"--emitter-version", "1.2.3"},
	} {
		t.Run(name, func(t *testing.T) {
			root := NewRootCmd(&deps.Deps{})
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(append([]string{"adapter", "emit"}, extra...))
			require.NoError(t, root.Execute())
		})
	}
}

// notImplemented is the inventory of commands with no behaviour in
// v0.1-hackathon: each is built by deps.Leaf.
var notImplemented = []string{
	"gifford capture attach",
	"gifford capture export",
	"gifford capture ingest",
	"gifford capture reconcile",
	"gifford capture status",
	"gifford capture stop",
	"gifford capture supervise",
	"gifford ctl approve-message",
	"gifford ctl deliver-next",
	"gifford ctl hold-message",
	"gifford ctl inspect",
	"gifford ctl loop complete",
	"gifford ctl loop create",
	"gifford ctl loop next",
	"gifford ctl model activate",
	"gifford ctl model rollback",
	"gifford ctl reject-message",
	"gifford ctl release-hold",
	"gifford ctl replay",
	"gifford ctl retry-artifact",
	"gifford ctl revoke-node",
	"gifford ctl unlock",
	"gifford dataset redact",
	"gifford install-systemd",
	"gifford label transcript",
	"gifford node keys",
	"gifford relay",
}

func TestStubInventory(t *testing.T) {
	t.Parallel()
	var got []string
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if c.Annotations[deps.NotImplementedAnnotation] == "true" {
			require.True(t, c.Hidden, "%s has no behaviour and must stay out of help", c.CommandPath())
			got = append(got, c.CommandPath())
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(NewRootCmd(&deps.Deps{}))
	require.ElementsMatch(t, notImplemented, got)
}

func TestEveryVisibleCommandHelpRenders(t *testing.T) {
	root := NewRootCmd(&deps.Deps{})
	var walk func(*cobra.Command)
	walk = func(cmd *cobra.Command) {
		if cmd.Hidden {
			return
		}
		t.Run(cmd.CommandPath(), func(t *testing.T) {
			var help bytes.Buffer
			cmd.SetOut(&help)
			require.NoError(t, cmd.Help())
			require.Contains(t, help.String(), "Usage:")
		})
		for _, child := range cmd.Commands() {
			walk(child)
		}
	}
	walk(root)
}

func TestHelpDescribesImplementedFeatures(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&deps.Deps{})
	for path, want := range map[string]string{
		"capture":        "Import captures recorded without the binary",
		"capture import": "Import an asciicast v2 capture recorded without the binary",
		"ctl approve":    "Approve a pending request for immediate delivery",
		"ctl loop stop":  "Stop a loop",
		"ctl model":      "Inspect the active model adapter",
		"dataset":        "Build and inspect the Analyst's dataset",
	} {
		cmd, _, err := root.Find(strings.Fields(path))
		require.NoError(t, err)
		require.Equal(t, want, cmd.Short, path)
	}

	for path, flags := range map[string][]string{
		"ctl approve":   {"add-turns", "mode", "once", "steps", "thread"},
		"ctl loop stop": {"cancel-turn"},
	} {
		cmd, _, err := root.Find(strings.Fields(path))
		require.NoError(t, err)
		for _, name := range flags {
			flag := cmd.Flags().Lookup(name)
			require.NotNil(t, flag, "%s --%s", path, name)
			require.Contains(t, flag.Usage, "(not implemented yet)", "%s --%s", path, name)
		}
	}
}

func TestStubsAreOutOfHelpAndSayNotImplemented(t *testing.T) {
	t.Parallel()
	for _, path := range notImplemented {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			args := strings.Fields(strings.TrimPrefix(path, "gifford "))
			root := NewRootCmd(&deps.Deps{})
			root.SetOut(io.Discard)
			root.SetErr(io.Discard)
			root.SetArgs(args)
			var ec *deps.ExitError
			require.ErrorAs(t, root.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
			require.Equal(t, path+": not implemented in v0.1-hackathon, see README#roadmap", ec.Error())

			parent := NewRootCmd(&deps.Deps{})
			var help bytes.Buffer
			parent.SetOut(&help)
			parent.SetArgs(append(args[:len(args)-1:len(args)-1], "--help"))
			require.NoError(t, parent.Execute())
			require.NotContains(t, help.String(), "\n  "+args[len(args)-1]+" ", "the parent's help does not list it")
		})
	}
}

func TestHelpListsTheInstallCommands(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&deps.Deps{})
	var help bytes.Buffer
	root.SetOut(&help)
	root.SetArgs([]string{"--help"})
	require.NoError(t, root.Execute())
	for _, name := range []string{"init", "doctor", "up", "status", "down", "run", "artifact"} {
		require.Contains(t, help.String(), "\n  "+name+" ")
	}
	for _, name := range []string{"relay", "install-systemd"} {
		require.NotContains(t, help.String(), "\n  "+name+" ", "a stub, or a group of stubs only")
	}
}
