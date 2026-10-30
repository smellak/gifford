//go:build unix

package capture

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	datasetcmd "github.com/smellak/gifford/internal/cli/dataset"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/label"
	"github.com/smellak/gifford/internal/protocol"
)

// The canaries mark what an import never writes nor prints: typed input,
// a marker, the header's title and env, and the source's path.
const (
	typedCanary  = "typed-canary-2v9n"
	markerCanary = "marker-canary-8c4j"
	titleCanary  = "title-canary-1x6h"
	envCanary    = "env-canary-7b3s"
	pathCanary   = "path-canary-4f5l"
)

// castFile writes a synthetic asciicast v2 file with output, typed input,
// a resize and a marker.
func castFile(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), pathCanary)
	require.NoError(t, os.Mkdir(dir, 0o700))
	body := strings.Join([]string{
		`{"version": 2, "width": 80, "height": 24, "timestamp": 1761825600, "title": "` + titleCanary + `", "env": {"TERM": "` + envCanary + `"}}`,
		`[0.25, "o", "build ok\r\n$ "]`,
		`[0.5, "i", "` + typedCanary + `"]`,
		`[1.5, "r", "100x30"]`,
		`[1.75, "m", "` + markerCanary + `"]`,
		`[2.5, "o", "\u001b[1mdone\u001b[0m\r\n$ "]`,
		`[3.25, "i", "` + typedCanary + `"]`,
	}, "\n") + "\n"
	path := filepath.Join(dir, "session.cast")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// execute runs one gifford capture command line.
func execute(args ...string) (stdout, stderr string, err error) {
	cmd := NewCmd(&deps.Deps{})
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func TestImportUsage(t *testing.T) {
	t.Parallel()
	const flags = "--format asciicast-v2 --agent agent-1 --harness codex --environment-domain bank_generic"
	for name, args := range map[string]string{
		"no file":            "import " + flags,
		"two files":          "import a.cast b.cast " + flags,
		"no format":          "import a.cast --agent agent-1 --harness codex --environment-domain bank_generic",
		"no agent":           "import a.cast --format asciicast-v2 --harness codex --environment-domain bank_generic",
		"no harness":         "import a.cast --format asciicast-v2 --agent agent-1 --environment-domain bank_generic",
		"no domain":          "import a.cast --format asciicast-v2 --agent agent-1 --harness codex",
		"an unknown format":  "import a.cast --format asciinema --agent agent-1 --harness codex --environment-domain bank_generic",
		"a bad agent id":     "import a.cast --format asciicast-v2 --agent Agent.One! --harness codex --environment-domain bank_generic",
		"an unknown harness": "import a.cast --format asciicast-v2 --agent agent-1 --harness vim --environment-domain bank_generic",
		"an unknown domain":  "import a.cast --format asciicast-v2 --agent agent-1 --harness codex --environment-domain home",
		"an unknown flag":    "import a.cast --title x " + flags,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, _, err := execute(strings.Fields(args)...)
			var ec *deps.ExitError
			require.ErrorAs(t, err, &ec)
			require.Equal(t, exitcode.Usage, ec.Code, "%v", err)
		})
	}
}

// filesUnder maps every file under dir to its bytes.
func filesUnder(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		out[path] = string(b)
		return err
	}))
	return out
}

// TestImportThenBuild imports a synthetic cast through the command, imports
// it again, then builds a dataset from the data dir with one window
// labeled: the session is not skipped, and its window's screen is blank,
// since the draft is unknown. It sets XDG_DATA_HOME and XDG_CONFIG_HOME,
// so it does not run in parallel.
func TestImportThenBuild(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "data"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "config"))
	dataDir := filepath.Join(root, "data", "gifford")
	source := castFile(t)
	args := []string{"import", source, "--format", "asciicast-v2", "--agent", "agent-1", "--harness", "codex", "--environment-domain", "fleet_managed"}

	stdout, stderr, err := execute(args...)
	require.NoError(t, err)
	require.Regexp(t, `^imported asciicast2-[0-9a-f]{32}: 2 output, 1 resize; dropped 2 input, 1 marker\n$`, stdout)
	id := strings.TrimSuffix(strings.Fields(stdout)[1], ":")
	again, stderr2, err := execute(args...)
	require.NoError(t, err, "the same file again exits 0")
	require.Equal(t, "already imported "+id+"\n", again)
	for _, never := range []string{typedCanary, markerCanary, titleCanary, envCanary, pathCanary, "session.cast"} {
		require.NotContains(t, stdout+stderr+again+stderr2, never)
		for path, body := range filesUnder(t, dataDir) {
			require.NotContains(t, body, never, path)
		}
	}

	// One window labeled, the second: replay gives the ids.
	src, err := replay.OpenAll(dataDir, 0, nil)
	require.NoError(t, err)
	var ids []protocol.ULID
	var screens []string
	for {
		rec, err := src.Next(context.Background())
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		ids = append(ids, rec.WindowID)
		screens = append(screens, strings.Join(rec.Screen.Rows, "\n"))
	}
	require.Len(t, ids, 3)
	require.Contains(t, screens[1], "build ok", "the replayed screen, before the projection")
	require.NoError(t, label.SaveAnnotations(label.Dir(dataDir), []label.Annotation{{
		WindowID: ids[1], Label: dataset.LabelA, Annotator: "tester", LabeledAt: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC),
	}}))
	configDir := filepath.Join(root, "config", "gifford")
	require.NoError(t, os.MkdirAll(configDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte("schema_version: 1\nml:\n  screen_lines: 24\n  mask: none\n"), 0o600))
	align := filepath.Join(root, "align.yaml")
	require.NoError(t, os.WriteFile(align, []byte("margin_ms: 0\nhook_harnesses: []\nweak:\n  silence_seconds: 3600\n"), 0o600))

	out := filepath.Join(root, "ds")
	build := datasetcmd.NewCmd(&deps.Deps{})
	var buildOut, buildErr bytes.Buffer
	build.SetOut(&buildOut)
	build.SetErr(&buildErr)
	build.SetArgs([]string{"build", "--out", out, "--no-mask", "--align-config", align})
	require.NoError(t, build.Execute(), buildErr.String())
	require.NotContains(t, buildErr.String(), "skipped", "the imported session replays")
	require.Contains(t, buildOut.String(), "sessions skipped 0")

	ents, err := os.ReadDir(filepath.Join(out, dataset.WindowsDir))
	require.NoError(t, err)
	require.Len(t, ents, 1, "the labeled window")
	f, err := os.Open(filepath.Join(out, dataset.WindowsDir, ents[0].Name()))
	require.NoError(t, err)
	w, err := dataset.ParseWindow(f)
	require.NoError(t, f.Close())
	require.NoError(t, err)
	require.Equal(t, ids[1], w.WindowID)
	require.Equal(t, dataset.LabelA, w.Target.Label)
	require.Equal(t, protocol.SourceQualityBootstrapImport, w.Provenance.SourceQuality)
	require.Equal(t, dataset.EnvironmentDomainFleetManaged, w.EnvironmentDomain)
	// The draft is unknown, so the projection blanks the whole screen: a
	// measure, not a failure.
	require.Empty(t, strings.Join(w.Input.ScreenLines, ""))
	for path, body := range filesUnder(t, out) {
		for _, never := range []string{typedCanary, markerCanary, titleCanary, envCanary, pathCanary} {
			require.NotContains(t, body, never, path)
		}
	}
}
