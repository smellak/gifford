package eval

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
)

func digestOf(s string) protocol.Digest {
	sum := sha256.Sum256([]byte(s))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// cliWindow is a minimal valid window. Every value is invented.
func cliWindow(n int, split dataset.Split, label dataset.Label) dataset.Window {
	anchor := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"step 3 of 7: compiling", "tests: 12 passed, 0 failed"}
	return dataset.Window{
		SchemaVersion: dataset.WindowSchemaVersion,
		WindowID:      protocol.ULID(fmt.Sprintf("01JB%022d", n)),
		Contract:      protocol.ModelContractAnalyzeAgentState,
		Input: dataset.AnalyzeInput{
			Harness:                protocol.HarnessCodex,
			SecondsSinceLastOutput: 0.2,
			OutputBytes:            dataset.OutputBytes{Last500ms: 120, Last2s: 480, Last10s: 900},
			NewChildProcesses:      []string{},
			ScreenLines:            lines,
		},
		Screen: dataset.ScreenEvidence{
			Cols: 80, Rows: 24,
			Redaction:           dataset.AppliedPolicy{Applied: true, PolicyVersion: "1"},
			HumanInputExclusion: dataset.AppliedPolicy{Applied: true, PolicyVersion: "1"},
			Digest:              dataset.ScreenDigest(80, 24, lines),
		},
		Target: dataset.Target{Label: label, Source: dataset.LabelSourceHook},
		Provenance: dataset.Provenance{
			SessionID:        "cap-1",
			CaptureSessionID: "cap-1",
			SourceQuality:    protocol.SourceQualityPTYScreen,
			SequenceFrom:     2,
			SequenceTo:       40,
			WindowStart:      anchor.Add(-30 * time.Second),
			WindowEnd:        anchor,
			AnchorAt:         anchor,
			SourceRefs:       []protocol.BlobRef{"blob://capture/cap-01ARZ3NDEKTSV4RRFFQ69G5FAV/seg-01ARZ3NDEKTSV4RRFFQ69G5FAW/0"},
			SourceDigest:     digestOf(fmt.Sprintf("source %d", n)),
		},
		EnvironmentDomain: dataset.EnvironmentDomainBankGeneric,
		Policies:          dataset.Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
		Split:             dataset.SplitAssignment{Split: split, GroupDigest: digestOf("group " + string(split))},
	}
}

// fixture writes a manifest and its windows directory, one train, two
// validation and two test windows.
func fixture(t *testing.T, dir string) (manifest, windows string) {
	t.Helper()
	ws := []dataset.Window{
		cliWindow(1, dataset.SplitTrain, dataset.LabelA),
		cliWindow(2, dataset.SplitValidation, dataset.LabelC),
		cliWindow(3, dataset.SplitValidation, dataset.LabelA),
		cliWindow(4, dataset.SplitTest, dataset.LabelC),
		cliWindow(5, dataset.SplitTest, dataset.LabelD),
	}
	m, err := dataset.BuildManifest(ws)
	require.NoError(t, err)
	windows = filepath.Join(dir, "windows")
	require.NoError(t, os.Mkdir(windows, 0o700))
	for _, w := range ws {
		b, err := json.Marshal(w)
		require.NoError(t, err)
		write(t, windows, string(w.WindowID)+".json", string(b))
	}
	b, err := json.Marshal(m)
	require.NoError(t, err)
	return write(t, dir, "manifest.json", string(b)), windows
}

// gatewayConfig serves every contract with the base profile.
func gatewayConfig(t *testing.T, dir string) string {
	t.Helper()
	return write(t, dir, "gateway.yaml", `
endpoint:
  url_env: GIFFORD_EVAL_TEST_URL
  token_env: GIFFORD_EVAL_TEST_TOKEN
base_model:
  repo: example.invalid/glimmer
  revision: v1
runtime: vllm
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: base
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`)
}

// glimmer is a fake endpoint on loopback; fail makes it answer 500.
func glimmer(t *testing.T, fail *atomic.Bool) *atomic.Int64 {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"label":"RUNNING_TOOL","probabilities":{"RUNNING_TOOL":0.75,"IDLE_AT_PROMPT":0.25},"evidence":["sentinel_q7"]}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("GIFFORD_EVAL_TEST_URL", srv.URL)
	t.Setenv("GIFFORD_EVAL_TEST_TOKEN", "test-token")
	return &calls
}

const (
	flagManifest = "--manifest"
	flagWindows  = "--windows"
	flagGateway  = "--gateway"
	flagOut      = "--out"
)

func execPredict(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var stderr bytes.Buffer
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&stderr)
	cmd.SetArgs(append([]string{"predict"}, args...))
	err := cmd.Execute()
	return stderr.String(), err
}

func TestPredictWritesScorablePredictions(t *testing.T) {
	var fail atomic.Bool
	calls := glimmer(t, &fail)
	dir := t.TempDir()
	manifest, windows := fixture(t, dir)
	gw := gatewayConfig(t, dir)
	out := filepath.Join(dir, "base.jsonl")
	args := []string{flagProfile, "base", flagManifest, manifest, flagWindows, windows, flagGateway, gw, flagOut, out}

	stderr, err := execPredict(t, args...)
	require.NoError(t, err)
	require.EqualValues(t, 4, calls.Load(), "validation and test only")
	require.Contains(t, stderr, "windows 4, valid 4, invalid 0")
	require.Contains(t, stderr, "latency p50")
	first, err := os.ReadFile(out)
	require.NoError(t, err)
	require.NotContains(t, string(first), "sentinel")
	require.NotContains(t, string(first), "compiling")
	ps, err := eval.ReadPredictions(bytes.NewReader(first), eval.ProfileBase)
	require.NoError(t, err)
	require.Len(t, ps, 4)
	require.InDelta(t, 0.75, ps[0].Probs[2], 0)

	// The file is what gifford eval scores.
	_, err = exec(t, flagProfile, "base="+out, flagManifest, manifest)
	require.NoError(t, err)

	// A failing endpoint still gives every window its line. Calls
	// one at a time: the gateway's breaker opens after the third
	// failure, and the fourth window never reaches the endpoint.
	fail.Store(true)
	stderr, err = execPredict(t, append(args, "--concurrency", "1")...)
	require.NoError(t, err)
	require.Contains(t, stderr, "windows 4, valid 0, invalid 4")
	require.Contains(t, stderr, "failed circuit_open 1")
	require.Contains(t, stderr, "failed call_failed 3")
	failed, err := os.ReadFile(out)
	require.NoError(t, err)
	ps, err = eval.ReadPredictions(bytes.NewReader(failed), eval.ProfileBase)
	require.NoError(t, err)
	for i, p := range ps {
		require.True(t, p.Invalid)
		want := eval.ClassCallFailed
		if i == 3 {
			want = eval.ClassCircuitOpen
		}
		require.Equal(t, want, p.ErrorClass)
	}
}

func TestPredictFailsClosed(t *testing.T) {
	var fail atomic.Bool
	calls := glimmer(t, &fail)
	dir := t.TempDir()
	manifest, windows := fixture(t, dir)
	base := gatewayConfig(t, dir)
	out := filepath.Join(dir, "out.jsonl")
	full := func(profile string) []string {
		return []string{flagProfile, profile, flagManifest, manifest, flagWindows, windows, flagGateway, base, flagOut, out}
	}

	for name, args := range map[string][]string{
		"no profile":       {flagManifest, manifest, flagWindows, windows, flagGateway, base, flagOut, out},
		"unknown profile":  full("lora"),
		"no out":           {flagProfile, "base", flagManifest, manifest, flagWindows, windows, flagGateway, base},
		"bad concurrency":  append(full("base"), "--concurrency", "0"),
		"profile mismatch": full("analyst-lora"),
	} {
		_, err := execPredict(t, args...)
		require.Error(t, err, name)
		require.True(t, errorsAsUsage(err), "%s: %v", name, err)
	}

	// A window edited after the manifest was built fails the run
	// before any call, and writes nothing.
	id := "01JB0000000000000000000004"
	path := filepath.Join(windows, id+".json")
	orig, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, bytes.Replace(orig, []byte(`"seconds_since_last_output":0.2`), []byte(`"seconds_since_last_output":0.3`), 1), 0o600))
	_, err = execPredict(t, full("base")...)
	require.ErrorIs(t, err, eval.ErrPredict)
	require.Contains(t, err.Error(), id)
	require.Zero(t, calls.Load())
	require.NoFileExists(t, out)

	// A window that escapes the directory is not read.
	outside := filepath.Join(dir, "outside.json")
	require.NoError(t, os.WriteFile(outside, orig, 0o600))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink(outside, path))
	_, err = execPredict(t, full("base")...)
	require.ErrorIs(t, err, eval.ErrPredict)
	require.NotContains(t, err.Error(), dir, "no path in the error")
	require.Zero(t, calls.Load())
	require.NoFileExists(t, out)
}
