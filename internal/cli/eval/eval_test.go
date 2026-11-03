package eval

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/dataset"
	coreeval "github.com/smellak/gifford/internal/eval"
)

func predictions(profile string) string {
	var b strings.Builder
	adapter := ""
	if profile == profileLoRA {
		adapter = `,"adapter_name":"analyst","adapter_revision":"test-rev-1","adapter_sha256":"` +
			strings.Repeat("a", 64) + `","training_run_id":"test-training-1"`
	}
	for i, truth := range []string{"A", "B", "D", "E"} {
		probs := make([]string, 0, 7)
		for _, l := range []string{"A", "B", "C", "D", "E", "F", "G"} {
			v := 0
			if l == truth {
				v = 1
			}
			probs = append(probs, fmt.Sprintf("%q:%d", l, v))
		}
		fmt.Fprintf(&b, `{"window_id":"01JB%022d","split":"test","harness":"codex","harness_version":"1.0","session_id":"sess-1","source_quality":"pty_screen","label":%q,"probabilities":{%s},"valid_mass":1,"invalid":false,"latency_ms":120,"profile":%q%s}`+"\n",
			i+1, truth, strings.Join(probs, ","), profile, adapter)
	}
	return b.String()
}

func predictionsWithRunIdentity(profile string) string {
	run := `,"prompt_version":"analyze_agent_state.v1","base_model_repo":"example.invalid/glimmer","base_model_revision":"revision-1","runtime":"vllm","runtime_version":"1.2.3","quantization":"fp8","gpu":"L4","predict_concurrency":1`
	return strings.ReplaceAll(predictions(profile), "}\n", run+"}\n")
}

const (
	flagProfile           = "--profile"
	flagReportJSON        = "--report-json"
	flagSafetyPredictions = "--safety-predictions"
	profileBase           = "base"
	profileLoRA           = "analyst-lora"
	testModelRevision     = "revision-1"
)

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func exec(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestEvalWritesStableReport(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := write(t, dir, "base.jsonl", predictions(profileBase))
	lora := write(t, dir, "lora.jsonl", predictions("analyst-lora"))
	out := filepath.Join(dir, "RESULTS.md")
	_, err := exec(t, flagProfile, "base="+base, flagProfile, "analyst-lora="+lora, "--out", out)
	require.NoError(t, err)
	first, err := os.ReadFile(out)
	require.NoError(t, err)
	// The same model twice does not improve: criterion 1 fails the gate.
	require.Contains(t, string(first), "## Gate: FAIL")

	stdout, err := exec(t, flagProfile, "base="+base, flagProfile, "analyst-lora="+lora)
	require.NoError(t, err)
	require.Equal(t, string(first), stdout, "same inputs, same bytes")
}

func TestEvalWritesActivationReportJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	baseData := predictionsWithRunIdentity(profileBase)
	loraData := predictionsWithRunIdentity(profileLoRA)
	base := write(t, dir, "base.jsonl", baseData)
	lora := write(t, dir, "lora.jsonl", loraData)
	reportPath := filepath.Join(dir, "evaluation-report.json")
	args := []string{flagProfile, "base=" + base, flagProfile, "analyst-lora=" + lora}

	markdown, err := exec(t, args...)
	require.NoError(t, err)
	require.NoFileExists(t, reportPath, "no JSON report is created without --report-json")

	withReport, err := exec(t, append(args, flagReportJSON, reportPath)...)
	require.NoError(t, err)
	require.Equal(t, markdown, withReport, "the optional JSON output does not change Markdown")
	raw, err := os.ReadFile(reportPath)
	require.NoError(t, err)
	var report coreeval.Report
	require.NoError(t, json.Unmarshal(raw, &report))
	require.Equal(t, coreeval.EvaluationLegacy, report.Mode)

	require.Len(t, report.Profiles, 2)
	profileRoles := map[coreeval.Profile]string{}
	for _, profile := range report.Profiles {
		profileRoles[profile.Profile] = profile.Role
	}
	require.Equal(t, map[coreeval.Profile]string{
		coreeval.ProfileBase: string(coreeval.ProfileBase),
		coreeval.ProfileLoRA: string(coreeval.ProfileLoRA),
	}, profileRoles)

	inputs := map[coreeval.Profile]coreeval.Input{}
	for _, input := range report.Inputs {
		if input.Role == "" {
			inputs[input.Profile] = input
		}
	}
	require.Len(t, inputs, 2)
	baseInput, loraInput := inputs[coreeval.ProfileBase], inputs[coreeval.ProfileLoRA]
	require.True(t, baseInput.RunRecorded)
	require.True(t, loraInput.RunRecorded)
	require.Equal(t, baseInput.Run.BaseModelRepo, loraInput.Run.BaseModelRepo)
	require.Equal(t, baseInput.Run.BaseModelRevision, loraInput.Run.BaseModelRevision)
	require.Empty(t, baseInput.Run.AdapterName)
	require.Empty(t, baseInput.Run.AdapterRevision)
	require.Empty(t, baseInput.Run.AdapterSHA256)
	require.Empty(t, baseInput.Run.TrainingRunID)
	require.Equal(t, "analyst", loraInput.Run.AdapterName)
	require.Equal(t, "test-rev-1", loraInput.Run.AdapterRevision)
	require.Equal(t, strings.Repeat("a", 64), string(loraInput.Run.AdapterSHA256))
	require.Equal(t, "test-training-1", string(loraInput.Run.TrainingRunID))

	secondPath := filepath.Join(dir, "evaluation-report-second.json")
	_, err = exec(t, append(args, flagReportJSON, secondPath)...)
	require.NoError(t, err)
	second, err := os.ReadFile(secondPath)
	require.NoError(t, err)
	require.Equal(t, raw, second, "same inputs produce the same JSON bytes")
}

func TestEvalReportJSONHelpNamesOverrideEvidence(t *testing.T) {
	t.Parallel()

	flag := NewCmd(&deps.Deps{}).Flags().Lookup("report-json")
	require.NotNil(t, flag)
	require.Contains(t, flag.Usage, "evaluation_report")
	require.Contains(t, flag.Usage, "gifford ctl model activate-override")
}

func TestEvalAdapterLoadsSafetyPredictionsByProfile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := write(t, dir, "base.jsonl", predictions(profileBase))
	lora := write(t, dir, "lora.jsonl", predictions("analyst-lora"))
	safetyManifest := filepath.Join("..", "..", "eval", "testdata", "safety", "manifest.json")
	safetyBase := write(t, dir, "safety-base.jsonl", promptPredictions(t, safetyManifest, "analyze_agent_state.v1", 1))
	safetyLoRAData := strings.ReplaceAll(
		promptPredictions(t, safetyManifest, "analyze_agent_state.v1", 1),
		`"profile":"base"`,
		`"profile":"analyst-lora","adapter_name":"analyst","adapter_revision":"test-rev-1","adapter_sha256":"`+strings.Repeat("a", 64)+`","training_run_id":"test-training-1"`,
	)
	safetyLoRA := write(t, dir, "safety-lora.jsonl", safetyLoRAData)

	out, err := exec(t,
		flagProfile, "base="+base,
		flagProfile, "analyst-lora="+lora,
		"--safety-manifest", safetyManifest,
		flagSafetyPredictions, "base="+safetyBase,
		flagSafetyPredictions, "analyst-lora="+safetyLoRA,
	)
	require.NoError(t, err)
	require.Contains(t, out, "| 5 | safety regression suite | PASS | base failures 0/24; analyst-lora failures 0/24;")
	require.Contains(t, out, "### Profile results")
	require.Contains(t, out, "| analyst-lora | 24 | 0.0000 | 0.9700 | 0 | 0 | 0 | 0 | PASS |")
	require.Contains(t, out, "| safety | analyst-lora | analyze\\_agent\\_state\\.v1 |")
	require.Contains(t, out, "| L4 | 1 | 24 |")
}

func TestEvalFailsClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := write(t, dir, "base.jsonl", predictions(profileBase))
	lora := write(t, dir, "lora.jsonl", predictions("analyst-lora"))
	short := write(t, dir, "short.jsonl", strings.SplitAfter(predictions("analyst-lora"), "\n")[0])
	nan := write(t, dir, "nan.jsonl", strings.Replace(predictions(profileBase), `"valid_mass":1`, `"valid_mass":NaN`, 1))

	for name, tc := range map[string]struct {
		args []string
		code int
	}{
		"no base":           {[]string{flagProfile, "analyst-lora=" + lora}, exitcode.Usage},
		"unknown profile":   {[]string{flagProfile, "big=" + base}, exitcode.Usage},
		"profile twice":     {[]string{flagProfile, "base=" + base, flagProfile, "base=" + base}, exitcode.Usage},
		"bad bins":          {[]string{flagProfile, "base=" + base, "--bins", "0"}, exitcode.Usage},
		"different windows": {[]string{flagProfile, "base=" + base, flagProfile, "analyst-lora=" + short}, exitcode.Error},
		"NaN":               {[]string{flagProfile, "base=" + nan}, exitcode.Error},
		"swapped profiles":  {[]string{flagProfile, "base=" + lora}, exitcode.Error},
	} {
		_, err := exec(t, tc.args...)
		require.Error(t, err, name)
		var ec *deps.ExitError
		if tc.code == exitcode.Usage {
			require.ErrorAs(t, err, &ec, name)
			require.Equal(t, exitcode.Usage, ec.Code, name)
		} else {
			require.False(t, errorsAsUsage(err), name)
		}
	}
}

func errorsAsUsage(err error) bool {
	var ec *deps.ExitError
	return errors.As(err, &ec) && ec.Code == exitcode.Usage
}

func promptPredictions(t *testing.T, manifestPath, prompt string, concurrency int) string {
	t.Helper()
	raw, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	var out strings.Builder
	enc := json.NewEncoder(&out)
	for i, e := range m.Entries {
		if e.Split != dataset.SplitValidation && e.Split != dataset.SplitTest {
			continue
		}
		probs := map[string]float64{"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0, "G": 0}
		probs[string(e.Label)] = 1
		require.NoError(t, enc.Encode(struct {
			WindowID       string             `json:"window_id"`
			Split          dataset.Split      `json:"split"`
			Harness        string             `json:"harness"`
			HarnessVersion string             `json:"harness_version"`
			SessionID      string             `json:"session_id"`
			SourceQuality  string             `json:"source_quality"`
			Label          dataset.Label      `json:"label"`
			Probabilities  map[string]float64 `json:"probabilities"`
			ValidMass      float64            `json:"valid_mass"`
			Invalid        bool               `json:"invalid"`
			LatencyMS      int                `json:"latency_ms"`
			Profile        string             `json:"profile"`
			PromptVersion  string             `json:"prompt_version"`
			BaseModelRepo  string             `json:"base_model_repo"`
			BaseModelRev   string             `json:"base_model_revision"`
			Runtime        string             `json:"runtime"`
			RuntimeVersion string             `json:"runtime_version"`
			Quantization   string             `json:"quantization"`
			GPU            string             `json:"gpu"`
			Concurrency    int                `json:"predict_concurrency"`
			SinceOutput    float64            `json:"seconds_since_last_output"`
		}{
			WindowID: string(e.WindowID), Split: e.Split, Harness: string(e.Harness), HarnessVersion: e.HarnessVersion,
			SessionID: fmt.Sprintf("cli-prompt-%d", i+1), SourceQuality: string(e.SourceQuality), Label: e.Label,
			Probabilities: probs, ValidMass: 1, LatencyMS: 100, Profile: profileBase, PromptVersion: prompt,
			BaseModelRepo: "example.invalid/glimmer", BaseModelRev: testModelRevision, Runtime: "vllm",
			RuntimeVersion: "1.2.3", Quantization: "fp8", GPU: "L4", Concurrency: concurrency, SinceOutput: 1,
		}))
	}
	return out.String()
}

func TestEvalPromptCandidateWritesStableReport(t *testing.T) {
	dir := t.TempDir()
	manifest, _ := fixture(t, dir)
	safetyManifest := filepath.Join("..", "..", "eval", "testdata", "safety", "manifest.json")
	base := write(t, dir, "base-v1.jsonl", promptPredictions(t, manifest, "analyze_agent_state.v1", 1))
	candidate := write(t, dir, "base-v2.jsonl", promptPredictions(t, manifest, "analyze_agent_state.v2", 1))
	safetyBase := write(t, dir, "safety-v1.jsonl", promptPredictions(t, safetyManifest, "analyze_agent_state.v1", 1))
	safetyCandidate := write(t, dir, "safety-v2.jsonl", promptPredictions(t, safetyManifest, "analyze_agent_state.v2", 1))
	out := filepath.Join(dir, "RESULTS.md")
	args := []string{
		flagProfile, "base=" + base,
		"--candidate-prompt", "analyze_agent_state.v2=" + candidate,
		flagManifest, manifest,
		"--safety-manifest", safetyManifest,
		flagSafetyPredictions, "baseline=" + safetyBase,
		flagSafetyPredictions, "candidate=" + safetyCandidate,
	}
	_, err := exec(t, append(args, flagOut, out)...)
	require.NoError(t, err)
	first, err := os.ReadFile(out)
	require.NoError(t, err)
	stdout, err := exec(t, args...)
	require.NoError(t, err)
	require.Equal(t, string(first), stdout)
	require.Contains(t, stdout, "| role | profile | prompt version |")
	require.Contains(t, stdout, "| 0 | dataset preparation for training | N/A | N/A | no aplica: sin entrenamiento | N/A |")
	require.Contains(t, stdout, "## Safety regression suite")
}

func TestEvalPromptCandidateRejectsOldAndConcurrentPredictions(t *testing.T) {
	dir := t.TempDir()
	manifest, _ := fixture(t, dir)
	safetyManifest := filepath.Join("..", "..", "eval", "testdata", "safety", "manifest.json")
	candidate := write(t, dir, "candidate.jsonl", promptPredictions(t, manifest, "analyze_agent_state.v2", 1))
	safetyBase := write(t, dir, "safety-v1.jsonl", promptPredictions(t, safetyManifest, "analyze_agent_state.v1", 1))
	safetyCandidate := write(t, dir, "safety-v2.jsonl", promptPredictions(t, safetyManifest, "analyze_agent_state.v2", 1))
	common := []string{
		"--candidate-prompt", "analyze_agent_state.v2=" + candidate,
		flagManifest, manifest, "--safety-manifest", safetyManifest,
		flagSafetyPredictions, "baseline=" + safetyBase,
		flagSafetyPredictions, "candidate=" + safetyCandidate,
	}
	for name, base := range map[string]string{
		"legacy":     write(t, dir, "legacy.jsonl", predictions(profileBase)),
		"concurrent": write(t, dir, "concurrent.jsonl", promptPredictions(t, manifest, "analyze_agent_state.v1", 4)),
	} {
		_, err := exec(t, append([]string{flagProfile, "base=" + base}, common...)...)
		require.Error(t, err, name)
		if name == "legacy" {
			require.Contains(t, err.Error(), "lacks run provenance")
		} else {
			require.Contains(t, err.Error(), "predict_concurrency must be 1, got 4")
		}
	}
}

func TestEvalPromptCandidateRejectsAdapterPartitionFlagsBeforeReadingFiles(t *testing.T) {
	t.Parallel()
	_, err := exec(t,
		flagProfile, "base=missing-base.jsonl",
		"--candidate-prompt", "analyze_agent_state.v2=missing-candidate.jsonl",
		"--val-cal-sessions", "missing-sessions.jsonl",
		"--calibration-out", "calibration.json",
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--candidate-prompt does not accept partition lists or sealed calibrations")
}
