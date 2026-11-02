package dataset

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// sessionSet is a SessionMembership for the tests.
type sessionSet map[protocol.OpaqueID]struct{}

func (s sessionSet) Len() int { return len(s) }

func (s sessionSet) Contains(id protocol.OpaqueID) bool {
	_, ok := s[id]
	return ok
}

// Every session id below starts with sess-, so an error that names one is
// easy to catch.
const (
	splitTraining = `{"session":"sess-train-1","split":"train"}` + "\n" +
		`{"session":"sess-val-1","split":"validation"}` + "\n" +
		`{"session":"sess-test-1","split":"test"}` + "\n"
	splitBlind1 = `{"session":"sess-blind-1","split":"test"}` + "\n"
	splitBlind2 = `{"session":"sess-blind-2","split":"test"}` + "\n"
)

func blindSessions() sessionSet {
	return sessionSet{"sess-blind-1": {}, "sess-blind-2": {}}
}

func TestParseSplitFile(t *testing.T) {
	t.Parallel()
	sessions, err := ParseSplitFile([]byte("\n" + splitTraining + "  \n"))
	require.NoError(t, err)
	require.Equal(t, map[string]Split{
		"sess-train-1": SplitTrain, "sess-val-1": SplitValidation, "sess-test-1": SplitTest,
	}, sessions)

	_, err = ParseSplitFile([]byte(splitTraining + `{"session":"sess-val-1","split":"test"}`))
	require.EqualError(t, err, "split file line 4: session listed twice")
	_, err = ParseSplitFile(make([]byte, MaxSplitFileBytes+1))
	require.EqualError(t, err, "split file: larger than 1 MiB")
	_, err = ParseSplitFile([]byte(" \n\n"))
	require.EqualError(t, err, "split file: no session")
}

func TestSplitFileDigest(t *testing.T) {
	t.Parallel()
	require.Equal(t,
		protocol.Digest("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
		SplitFileDigest(nil))
}

func TestCheckSplitFileExtensionAccepts(t *testing.T) {
	t.Parallel()
	unterminated := strings.TrimSuffix(splitTraining, "\n")
	for name, files := range map[string][2]string{
		"the blind sessions appended": {splitTraining, splitTraining + splitBlind1 + splitBlind2},
		// A training file without a final newline keeps its exact bytes:
		// a newline comes first, then the new lines.
		"a newline after an unterminated training file": {unterminated, unterminated + "\n" + splitBlind1 + splitBlind2},
		"blank lines between":                           {splitTraining, splitTraining + "\n" + splitBlind1 + "\n\n" + splitBlind2},
	} {
		require.NoError(t, CheckSplitFileExtension([]byte(files[0]), []byte(files[1]), blindSessions()), name)
	}
}

func TestCheckSplitFileExtensionRejects(t *testing.T) {
	t.Parallel()
	unterminated := strings.TrimSuffix(splitTraining, "\n")
	firstLine, rest, _ := strings.Cut(splitTraining, "\n")
	reordered := rest + firstLine + "\n"
	blind := splitBlind1 + splitBlind2
	oversized := splitTraining + blind + strings.Repeat("\n", MaxSplitFileBytes)
	const notAPrefix = "does not start with the training split file"
	tests := []struct {
		name, training, final, want string
	}{
		{"the same file", splitTraining, splitTraining, "does not start with the training split file and add to it"},
		{"blanks only", splitTraining, splitTraining + "\n \n", "adds no session"},
		{
			"a training byte edited", splitTraining, strings.Replace(splitTraining, `"validation"`, `"test"`, 1) + blind,
			notAPrefix,
		},
		{"a training line dropped", splitTraining, rest + blind, notAPrefix},
		{"training lines reordered", splitTraining, reordered + blind, notAPrefix},
		{
			"a training session again", splitTraining, splitTraining + splitBlind1 + `{"session":"sess-train-1","split":"test"}` + "\n" + splitBlind2,
			"final split file line 5: session listed twice",
		},
		{
			"an object glued without a newline", unterminated, unterminated + blind,
			"final split file line 3: not one object with session and split",
		},
		{
			"a malformed training file", `{"session":"sess-train-1"` + "\n", `{"session":"sess-train-1"` + "\n" + blind,
			"training split file line 1: not one object with session and split",
		},
		{
			"a new train session", splitTraining, splitTraining + strings.Replace(splitBlind1, `"test"`, `"train"`, 1) + splitBlind2,
			"adds a session that is not test",
		},
		{
			"a new validation session", splitTraining, splitTraining + splitBlind1 + strings.Replace(splitBlind2, `"test"`, `"validation"`, 1),
			"adds a session that is not test",
		},
		{
			"an extra test session", splitTraining, splitTraining + blind + `{"session":"sess-extra","split":"test"}` + "\n",
			"adds a session that is not blind-test",
		},
		{"a blind session missing", splitTraining, splitTraining + splitBlind1, "misses a blind-test session"},
		{
			"a blind session already in training", splitTraining + splitBlind1, splitTraining + splitBlind1 + splitBlind2,
			"the training split file already lists a blind-test session",
		},
		{"a final file over 1 MiB", splitTraining, oversized, "final split file: larger than 1 MiB"},
	}
	for _, tt := range tests {
		err := CheckSplitFileExtension([]byte(tt.training), []byte(tt.final), blindSessions())
		require.ErrorIs(t, err, ErrSplitFileExtension, tt.name)
		require.ErrorContains(t, err, tt.want, tt.name)
		require.NotContains(t, err.Error(), "sess-", "%s: the error names no session", tt.name)
	}
}

func TestWriteDirSplitFileSidecar(t *testing.T) {
	t.Parallel()
	ws := testWindows()
	m, err := BuildManifest(ws)
	require.NoError(t, err)
	raw := []byte(splitTraining)
	digest := SplitFileDigest(raw)
	m.SplitFile = &digest
	m.DatasetDigest = ManifestDigest(m)
	require.NoError(t, m.Validate())
	parent := t.TempDir()

	dir := filepath.Join(parent, "ds")
	require.NoError(t, WriteDir(dir, Inventory{Windows: ws, Manifest: m, SplitFileBytes: raw}))
	sidecar := filepath.Join(dir, SplitFileSidecar)
	kept, err := os.ReadFile(sidecar)
	require.NoError(t, err)
	require.Equal(t, raw, kept, "the exact bytes the build read")
	if runtime.GOOS != "windows" {
		info, err := os.Stat(sidecar)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}

	// Without the bytes there is no sidecar, as for a dataset built
	// before it existed.
	bare := filepath.Join(parent, "bare")
	require.NoError(t, WriteDir(bare, Inventory{Windows: ws, Manifest: m}))
	require.NoFileExists(t, filepath.Join(bare, SplitFileSidecar))

	noSplitFile, err := BuildManifest(ws)
	require.NoError(t, err)
	for name, inv := range map[string]Inventory{
		"bytes that do not hash to split_file": {Windows: ws, Manifest: m, SplitFileBytes: append([]byte(splitTraining), '\n')},
		"bytes without a split_file":           {Windows: ws, Manifest: noSplitFile, SplitFileBytes: raw},
	} {
		fresh := filepath.Join(parent, "fresh")
		require.ErrorIs(t, WriteDir(fresh, inv), ErrDatasetDir, name)
		require.NoDirExists(t, fresh, name)
	}
	entries, err := os.ReadDir(parent)
	require.NoError(t, err)
	require.Len(t, entries, 2, "no temporary directory left behind")
}
