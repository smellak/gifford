//go:build unix

package dataset

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
)

// Synthetic asciicast v2 casts. lateCast writes its first output after
// the first window's anchor, emptyCast records an output of no bytes
// between two real ones, and controlCast is lateCast with its first
// output before the first anchor.
const (
	castHeader  = `{"version": 2, "width": 80, "height": 24, "timestamp": 1700000000}` + "\n"
	lateCast    = castHeader + `[1.5, "o", "hello\r\n"]` + "\n" + `[2.5, "o", "world\r\n"]` + "\n" + `[6.0, "o", "$ "]` + "\n"
	emptyCast   = castHeader + `[0.1, "o", "hello\r\n"]` + "\n" + `[3.5, "o", ""]` + "\n" + `[6.0, "o", "$ "]` + "\n"
	controlCast = castHeader + `[0.1, "o", "hello\r\n"]` + "\n" + `[2.5, "o", "world\r\n"]` + "\n" + `[6.0, "o", "$ "]` + "\n"
)

// castEnv imports cast as a codex session in a fresh data dir, labels its
// windows numbered in labels (1 is the first window), and writes the
// config and the align config a build reads. It sets XDG_DATA_HOME and
// XDG_CONFIG_HOME, so its tests do not run in parallel.
func castEnv(t *testing.T, cast string, labels map[int]dataset.Label) (e env, ids []protocol.ULID) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "data"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "config"))
	e = env{
		dataDir: filepath.Join(root, "data", "gifford"), configDir: filepath.Join(root, "config", "gifford"),
		work: filepath.Join(root, "work"),
	}
	require.NoError(t, os.MkdirAll(e.work, 0o700))
	source := filepath.Join(e.work, "synthetic.cast")
	require.NoError(t, os.WriteFile(source, []byte(cast), 0o600))
	res, err := capture.Import(context.Background(), capture.Options{
		DataDir: e.dataDir, Source: source, Format: capture.FormatAsciicastV2,
		AgentID: "agent-1", Harness: protocol.HarnessCodex, EnvironmentDomain: dataset.EnvironmentDomainBankGeneric,
	})
	require.NoError(t, err)

	src, err := replay.Open(replay.Options{DataDir: e.dataDir, SessionID: res.SessionID})
	require.NoError(t, err)
	var anns []label.Annotation
	for {
		rec, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		ids = append(ids, rec.WindowID)
		if l, ok := labels[len(ids)]; ok {
			anns = append(anns, label.Annotation{WindowID: rec.WindowID, Label: l, Annotator: "tester", LabeledAt: t0})
		}
	}
	require.Len(t, anns, len(labels), "every label names a window")
	require.NoError(t, label.SaveAnnotations(label.Dir(e.dataDir), anns))
	e.alignCfg = filepath.Join(e.work, "align.yaml")
	require.NoError(t, os.WriteFile(e.alignCfg, []byte("margin_ms: 0\nhook_harnesses: []\nweak:\n  silence_seconds: 3600\n"), 0o600))
	writeConfig(t, e, "screen_lines: 10", maskNone)
	return e, ids
}

// buildCast builds e's dataset as the import's own windows are cut.
func buildCast(t *testing.T, e env) (string, string, error) {
	t.Helper()
	out := filepath.Join(e.work, "ds")
	stdout, err := buildCmd(t, flagOut, out, flagAlign, e.alignCfg, "--every", "1s", flagNoMask, flagScreenLines, "10")
	return out, stdout, err
}

// TestBuildLeavesOutWindowsBeforeTheFirstOutput builds imported casts
// whose labelled windows come before any output, or after an output of
// no bytes. A window before the first output is left out and counted
// once, the two passes of the build notwithstanding, and the window
// after it keeps its id and its label. A build left with no window
// fails as an empty dataset. An output of no bytes ends no quiet time:
// the window after it is built, timed from the output before it.
func TestBuildLeavesOutWindowsBeforeTheFirstOutput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cast   string
		labels map[int]dataset.Label
		// built is the window number of the one window built, 0 for none,
		// and quiet its seconds since the last output.
		built       int
		quiet       float64
		noOutputYet int
	}{
		{name: "the label before the first output", cast: lateCast, labels: map[int]dataset.Label{1: dataset.LabelA}, noOutputYet: 1},
		{name: "the label after the first output", cast: lateCast, labels: map[int]dataset.Label{2: dataset.LabelB}, built: 2, quiet: 0.5},
		{name: "labels on both sides of the first output", cast: lateCast, labels: map[int]dataset.Label{1: dataset.LabelA, 2: dataset.LabelB}, built: 2, quiet: 0.5, noOutputYet: 1},
		{name: "the label after an output of no bytes", cast: emptyCast, labels: map[int]dataset.Label{4: dataset.LabelC}, built: 4, quiet: 3.9},
		{name: "the label before an output of no bytes", cast: emptyCast, labels: map[int]dataset.Label{1: dataset.LabelC}, built: 1, quiet: 0.9},
		{name: "the first output before the first anchor", cast: controlCast, labels: map[int]dataset.Label{1: dataset.LabelA}, built: 1, quiet: 0.9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, ids := castEnv(t, tc.cast, tc.labels)
			out, stdout, err := buildCast(t, e)
			if tc.built == 0 {
				require.ErrorContains(t, err, "no windows", "an empty dataset, never a trainable one")
				require.NotContains(t, err.Error(), "invalid window")
				_, statErr := os.Lstat(out)
				require.ErrorIs(t, statErr, os.ErrNotExist, "nothing written")
				return
			}
			require.NoError(t, err)
			require.Contains(t, stdout, "no_output_yet "+strconv.Itoa(tc.noOutputYet)+", ")
			ws := readWindows(t, out)
			require.Len(t, ws, 1)
			require.Equal(t, ids[tc.built-1], ws[0].WindowID, "the window keeps its id")
			require.Equal(t, tc.labels[tc.built], ws[0].Target.Label, "and its label")
			require.InDelta(t, tc.quiet, ws[0].Input.SecondsSinceLastOutput, 1e-9)
			require.NoError(t, ws[0].Validate())
		})
	}
}
