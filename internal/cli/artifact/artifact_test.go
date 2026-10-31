package artifact

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/protocol"
)

func TestArtifactCommands(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	names := make([]string, 0, len(cmd.Commands()))
	for _, child := range cmd.Commands() {
		names = append(names, child.Name())
	}
	require.ElementsMatch(t, []string{"send", "list", "receive", "status"}, names)

	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{"send", "notes.txt"})
	require.EqualError(t, cmd.Execute(), "--to is required")
}

func TestArtifactBareGroupShowsHelp(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetArgs([]string{})
	require.NoError(t, cmd.Execute())
}

func TestLinkWithoutOverwriteUsesSuffix(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	temporary := filepath.Join(directory, ".artifact.part")
	require.NoError(t, os.WriteFile(temporary, []byte("received"), 0o600))
	existing := filepath.Join(directory, "report.txt")
	require.NoError(t, os.WriteFile(existing, []byte("existing"), 0o600))

	published, err := linkWithoutOverwrite(temporary, directory, "report.txt")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(directory, "report.txt (1)"), published)
	require.FileExists(t, temporary)
	require.Equal(t, []byte("existing"), mustReadArtifactFile(t, existing))
	require.Equal(t, []byte("received"), mustReadArtifactFile(t, published))
}

func TestFinalizeContentKeyAfterIdempotentComplete(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	id := protocol.OpaqueID("art-0123456789abcdef0123456789abcdef")
	require.NoError(t, ensureOutgoingDirectory(dataDir))
	created, err := writePrivateOnce(outgoingKeyPath(dataDir, id), make([]byte, 32))
	require.NoError(t, err)
	require.True(t, created)

	require.NoError(t, finalizeContentKey(
		dataDir, id, protocol.ArtifactStateLocalAvailable, true,
	))
	_, err = os.Lstat(outgoingKeyPath(dataDir, id))
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func mustReadArtifactFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}
