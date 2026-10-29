package dataset

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitV1(t *testing.T) {
	t.Parallel()
	seen := map[Split]int{}
	for i := range 2000 {
		g := testDigest(fmt.Sprintf("session %d", i))
		s, err := SplitV1{}.Split(g)
		require.NoError(t, err)
		again, err := SplitV1{}.Split(g)
		require.NoError(t, err)
		require.Equal(t, s, again, "the same group, the same split")
		seen[s]++
	}
	// 70/15/15, within a loose band.
	require.InDelta(t, 1400, seen[SplitTrain], 120)
	require.InDelta(t, 300, seen[SplitValidation], 80)
	require.InDelta(t, 300, seen[SplitTest], 80)
	// A fixed vector pins split-v1.
	s, err := SplitV1{}.Split(testDigest("synthetic group"))
	require.NoError(t, err)
	require.Equal(t, splitOfVector, s)
	_, err = SplitV1{}.Split("not a digest")
	require.Error(t, err)
}

// splitOfVector is split-v1 of testDigest("synthetic group").
const splitOfVector = SplitTrain

func TestWriteDir(t *testing.T) {
	t.Parallel()
	ws := testWindows()
	m, err := BuildManifest(ws)
	require.NoError(t, err)
	parent := t.TempDir()
	dir := filepath.Join(parent, "ds")
	require.NoError(t, WriteDir(dir, Inventory{Windows: ws, Manifest: m}))

	raw, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	require.NoError(t, err)
	got, err := ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	require.Equal(t, m, got)
	for _, e := range m.Entries {
		f, err := os.Open(filepath.Join(dir, WindowsDir, WindowFile(string(e.WindowID))))
		require.NoError(t, err)
		w, err := ParseWindow(f)
		require.NoError(t, f.Close())
		require.NoError(t, err)
		d, err := WindowDigest(w)
		require.NoError(t, err)
		require.Equal(t, e.WindowDigest, d)
	}
	if runtime.GOOS != "windows" {
		for path, want := range map[string]os.FileMode{
			dir: 0o700, filepath.Join(dir, WindowsDir): 0o700, filepath.Join(dir, ManifestFile): 0o600,
			filepath.Join(dir, WindowsDir, WindowFile(string(ws[0].WindowID))): 0o600,
		} {
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, want, info.Mode().Perm(), path)
		}
	}

	// The same inventory, the same bytes.
	dir2 := filepath.Join(parent, "ds2")
	require.NoError(t, WriteDir(dir2, Inventory{Windows: ws, Manifest: m}))
	require.Equal(t, treeOf(t, dir), treeOf(t, dir2))

	// An existing directory is never written into or replaced.
	err = WriteDir(dir, Inventory{Windows: ws, Manifest: m})
	require.ErrorIs(t, err, ErrDatasetDir)
	require.Equal(t, treeOf(t, dir2), treeOf(t, dir))

	// On failure nothing appears, not even the temporary directory.
	bad := m
	bad.DatasetDigest = testDigest("other")
	for name, inv := range map[string]Inventory{
		"invalid manifest": {Windows: ws, Manifest: bad},
		"missing window":   {Windows: ws[:1], Manifest: m},
		// Fails half-way, once the temporary directory holds a file.
		"repeated window": {Windows: []Window{ws[0], ws[0], ws[1], ws[2]}, Manifest: m},
	} {
		fresh := filepath.Join(parent, "fresh")
		require.ErrorIs(t, WriteDir(fresh, inv), ErrDatasetDir, name)
		require.NoDirExists(t, fresh, name)
	}
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Len(t, entries, 2, "no temporary directory left behind")
}

// treeOf maps each file under dir to its bytes.
func treeOf(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		rel, _ := filepath.Rel(dir, path)
		out[rel] = string(b)
		return err
	}))
	return out
}
