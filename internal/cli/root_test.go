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
		"run", "mcp", "hub", "relay", "panel", "ctl", "node", "artifact",
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

// notImplemented is the inventory of commands with no behaviour in
// v0.1-hackathon: each is built by deps.Leaf.
var notImplemented = []string{
	"gifford artifact send",
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
	"gifford ctl loop budget",
	"gifford ctl loop complete",
	"gifford ctl loop create",
	"gifford ctl loop list",
	"gifford ctl loop next",
	"gifford ctl loop set-mode",
	"gifford ctl loop status",
	"gifford ctl loop stop",
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
			got = append(got, c.CommandPath())
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(NewRootCmd(&deps.Deps{}))
	require.ElementsMatch(t, notImplemented, got)
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
	for _, name := range []string{"init", "doctor", "up", "status", "down", "run"} {
		require.Contains(t, help.String(), "\n  "+name+" ")
	}
	for _, name := range []string{"relay", "install-systemd", "artifact"} {
		require.NotContains(t, help.String(), "\n  "+name+" ", "a stub, or a group of stubs only")
	}
}
