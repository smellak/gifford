package cli

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionCommand(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&Deps{})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"version"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), "gifford ")
}

func TestManCommand(t *testing.T) {
	t.Parallel()
	root := NewRootCmd(&Deps{})
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"man"})
	require.NoError(t, root.Execute())
	require.Contains(t, out.String(), ".TH \"GIFFORD\"")
}
