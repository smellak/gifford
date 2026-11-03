//go:build unix

package fuzz

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

func TestEditorEmptyAreaMatchesProfile(t *testing.T) {
	t.Parallel()
	profile, err := harnessProfile(protocol.InjectModeBracketedPaste)
	require.NoError(t, err)
	emulator, err := vt.New(80, 24)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, emulator.Close()) })
	var e editor
	_, err = emulator.Write(e.start())
	require.NoError(t, err)
	assertEmpty := func() {
		t.Helper()
		snapshot := emulator.Snapshot()
		text, ok := draft.CursorInput(profile, snapshot)
		require.Truef(t, ok, "cursor=(%d,%d) screen=%q", snapshot.CursorCol, snapshot.CursorRow, snapshot.Text())
		require.Empty(t, text)
	}
	assertEmpty()
	out, _ := e.feed([]byte("human\r"))
	_, err = emulator.Write(out)
	require.NoError(t, err)
	assertEmpty()
}
