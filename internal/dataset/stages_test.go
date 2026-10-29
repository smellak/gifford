package dataset

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/redact"
)

func TestHumanInputV1(t *testing.T) {
	t.Parallel()
	screen := redact.Screen{Cols: 40, Rows: []string{
		"compiling step 3",
		"",
		"tests passed",
		"> first line",
		"  second line", // the cursor's row
		"  hint below",
		"",
	}}
	ex := HumanInputV1{}
	shown := Cursor{Row: 4, Known: true, Visible: true}

	got, version, err := ex.ExcludeHumanInput(screen, DraftClosed, shown)
	require.NoError(t, err)
	require.Equal(t, HumanInputPolicyV1, version)
	require.Equal(t, screen, got, "a closed draft keeps the prompt row")

	// With the draft not closed the whole screen goes: nothing says
	// where the input block starts.
	blank := make([]string, len(screen.Rows))
	for _, draft := range []DraftState{DraftOpen, DraftUnknown, ""} {
		got, version, err := ex.ExcludeHumanInput(screen, draft, shown)
		require.NoError(t, err)
		require.Equal(t, HumanInputPolicyV1, version)
		require.Equal(t, blank, got.Rows, "draft %q", draft)
		require.Equal(t, screen.Cols, got.Cols)
	}
	require.Equal(t, "> first line", screen.Rows[3], "the input is not changed")

	// A paste longer than any guessed block: no row of it is left, the
	// first ones included.
	const canary = "HUMAN-CANARY-5t1w"
	tall := redact.Screen{Cols: 80}
	for i := range 24 {
		tall.Rows = append(tall.Rows, fmt.Sprintf("  %s pasted row %d", canary, i))
	}
	got, _, err = ex.ExcludeHumanInput(tall, DraftOpen, Cursor{Row: 23, Known: true, Visible: true})
	require.NoError(t, err)
	require.Len(t, got.Rows, 24)
	require.NotContains(t, strings.Join(got.Rows, "\n"), canary)

	// Unknown, hidden or off the screen: the window cannot be cleared
	// on trust.
	for _, c := range []Cursor{{Row: 4, Visible: true}, {Row: 4, Known: true}, {Row: -1, Known: true, Visible: true}, {Row: 7, Known: true, Visible: true}} {
		_, _, err := ex.ExcludeHumanInput(screen, DraftOpen, c)
		require.ErrorIs(t, err, ErrNoCursor, "%+v", c)
	}
	_, _, err = ex.ExcludeHumanInput(screen, DraftClosed, Cursor{})
	require.NoError(t, err, "a closed draft needs no cursor")
}

func TestDomainMask(t *testing.T) {
	t.Parallel()
	const acme = "AcmeBank"
	names, err := ParseMaskList(strings.NewReader("# private list\n\n  AcmeBank  \nacme\nNorth Wind\n"))
	require.NoError(t, err)
	require.Equal(t, []string{acme, "acme", "North Wind"}, names)
	m, err := NewDomainMask(names)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(m.Version(), "domain-mask-v1:"))
	require.Len(t, m.Version(), len("domain-mask-v1:")+12)

	got, version, err := m.MaskDomain(redact.Screen{Cols: 80, Rows: []string{
		"login to ACMEBANK portal", "acme-tools and acmebank_x and acmex", "north wind ltd, North Wind.", "",
	}})
	require.NoError(t, err)
	require.Equal(t, m.Version(), version)
	require.Equal(t, []string{
		"login to [domain] portal", "[domain]-tools and acmebank_x and acmex", "[domain] ltd, [domain].", "",
	}, got.Rows, "case-insensitive, whole words only")

	// The version names the list, not its order or case.
	same, err := NewDomainMask([]string{"north wind", "ACME", "acmebank", "Acme"})
	require.NoError(t, err)
	require.Equal(t, m.Version(), same.Version())
	other, err := NewDomainMask([]string{"acme"})
	require.NoError(t, err)
	require.NotEqual(t, m.Version(), other.Version())
	require.NoError(t, Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: HumanInputPolicyV1, DomainMaskPolicyVersion: m.Version()}.validate())

	got, version, err = NoMask{}.MaskDomain(redact.Screen{Cols: 80, Rows: []string{acme}})
	require.NoError(t, err)
	require.Equal(t, DomainMaskNone, version)
	require.Equal(t, []string{acme}, got.Rows)

	for name, in := range map[string]string{
		"empty":     "",
		"comments":  "# only\n\n",
		"control":   "acme\nbad\x07name\n",
		"too long":  strings.Repeat("n", 129) + "\n",
		"too large": strings.Repeat("name\n", (1<<20)/5+1),
	} {
		_, err := ParseMaskList(strings.NewReader(in))
		require.ErrorIs(t, err, ErrMaskList, name)
		require.NotContains(t, err.Error(), "bad", name)
	}
	_, err = NewDomainMask(nil)
	require.ErrorIs(t, err, ErrMaskList)
}

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
