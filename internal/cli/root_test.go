package cli

import (
	"bytes"
	"io"
	"strings"
	"testing"

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
		"capture", "keycap", "fuzz", "label", "dataset", "eval",
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
	for _, args := range []string{"ctl model status", "capture ingest", "dataset build", "node enroll-token"} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()
			root := NewRootCmd(&deps.Deps{})
			root.SetOut(io.Discard)
			root.SetArgs(strings.Fields(args))
			var ec *deps.ExitError
			require.ErrorAs(t, root.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}
