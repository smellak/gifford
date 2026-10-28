package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
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

// TestFoundationCommandsPresent is the narrow presence test for the
// foundation PR: the full first-level set only lands with the command-tree
// PR, which replaces this test with TestHelpListsAllFirstLevelCommands.
func TestFoundationCommandsPresent(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&deps.Deps{})
	got := make([]string, 0, len(root.Commands()))
	for _, c := range root.Commands() {
		got = append(got, c.Name())
	}
	require.ElementsMatch(t, []string{"init", "doctor", "version", "man"}, got)
}
