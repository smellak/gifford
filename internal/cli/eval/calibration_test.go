package eval

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	coreeval "github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testCalibrationPath = "cal.json"
	testValListPath     = valCalSessionFile
	testBlindSession    = "blind-session"
	testHistorySession  = "history-session"
)

func testCalibrationRuns() (coreeval.RunProvenance, coreeval.RunProvenance) {
	base := coreeval.RunProvenance{
		PromptVersion: "analyze_agent_state.v2", BaseModelRepo: "example.invalid/glimmer",
		BaseModelRevision: testModelRevision, Runtime: "vllm", RuntimeVersion: "1.0.0",
		Quantization: "fp8", GPU: "L4", PredictConcurrency: 1,
	}
	lora := base
	lora.AdapterName = "analyst"
	lora.AdapterRevision = testModelRevision
	lora.AdapterSHA256 = protocol.SHA256Hex(strings.Repeat("b", 64))
	lora.TrainingRunID = "training-1"
	return base, lora
}

func TestSessionListIsStrictAndContentAddressed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	content := "{\"session_id\":\"session-one\"}\n{\"session_id\":\"session-two\"}\n"
	path := write(t, dir, "val-cal.jsonl", content)
	set, err := loadSessionSet(path)
	require.NoError(t, err)
	require.Equal(t, 2, set.Len())
	sum := sha256.Sum256([]byte(content))
	require.Equal(t, protocol.Digest(fmt.Sprintf("sha256:%x", sum)), set.Digest())

	for name, raw := range map[string]string{
		"unknown member":   "{\"session\":\"session-one\"}\n",
		"extra member":     "{\"session_id\":\"session-one\",\"split\":\"validation\"}\n",
		"repeated member":  "{\"session_id\":\"session-one\",\"session_id\":\"session-two\"}\n",
		"not object":       "[\"session-one\"]\n",
		"invalid id":       "{\"session_id\":\"/private/path\"}\n",
		"trailing data":    "{\"session_id\":\"session-one\"}{}\n",
		"empty":            "\n",
		"duplicate line":   "{\"session_id\":\"session-one\"}\n{\"session_id\":\"session-one\"}\n",
		"unsorted":         "{\"session_id\":\"session-two\"}\n{\"session_id\":\"session-one\"}\n",
		"blank line":       "{\"session_id\":\"session-one\"}\n\n",
		"spaced JSON":      "{ \"session_id\": \"session-one\" }\n",
		"CRLF":             "{\"session_id\":\"session-one\"}\r\n",
		"missing final LF": `{"session_id":"session-one"}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := loadSessionSet(write(t, t.TempDir(), "sessions.jsonl", raw))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "session-one", "errors never render a session id")
		})
	}
}

func TestCalibrationFileRoundTripAndModes(t *testing.T) {
	t.Parallel()
	baseRun, loraRun := testCalibrationRuns()
	set := coreeval.CalibrationSet{
		SchemaVersion:            2,
		ValidationSessionsSHA256: protocol.Digest("sha256:" + strings.Repeat("a", 64)),
		Calibrations: []coreeval.SealedCalibration{
			{
				Profile: coreeval.ProfileBase, Role: "base", Temperature: 1.25,
				TemperatureFitted: true, Theta: .9, ThetaFitted: true,
				CalibrationInputSHA256: protocol.Digest("sha256:" + strings.Repeat("c", 64)),
				CalibrationInputLines:  2, Run: baseRun,
			},
			{
				Profile: coreeval.ProfileLoRA, Role: "analyst-lora", Temperature: 1.5,
				TemperatureFitted: true, Theta: .8, ThetaFitted: true,
				CalibrationInputSHA256: protocol.Digest("sha256:" + strings.Repeat("d", 64)),
				CalibrationInputLines:  2, Run: loraRun,
			},
		},
	}
	raw, err := set.Marshal()
	require.NoError(t, err)
	path := write(t, t.TempDir(), "calibration.json", string(raw))
	got, gotRaw, err := loadCalibrations(path)
	require.NoError(t, err)
	require.Equal(t, set, got)
	require.Equal(t, raw, gotRaw)

	require.NoError(t, (calibrationFlags{}).validate(""))
	require.NoError(t, (calibrationFlags{valCalSessions: testValListPath, calibrationOut: testCalibrationPath}).validate("manifest.json"))
	require.NoError(t, (calibrationFlags{valCalSessions: testValListPath, blindTestSessions: blindSessionFile, sealed: testCalibrationPath}).validate("manifest.json"))
	for _, flags := range []calibrationFlags{
		{valCalSessions: testValListPath},
		{valCalSessions: testValListPath, calibrationOut: testCalibrationPath, sealed: "sealed.json"},
		{blindTestSessions: blindSessionFile, sealed: testCalibrationPath},
	} {
		require.Error(t, flags.validate("manifest.json"))
	}
	require.Error(t, (calibrationFlags{valCalSessions: testValListPath, calibrationOut: testCalibrationPath}).validate(""))
	require.Error(t, (calibrationFlags{valCalSessions: "val-cal.jsonl", calibrationOut: testCalibrationPath}).validate("manifest.json"))
}

func TestCLIProducesAndConsumesSealedCalibrations(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	type placed struct {
		window  dataset.Window
		session string
	}
	placements := []placed{
		{cliWindow(1, dataset.SplitTrain, dataset.LabelA), "train-session"},
		{cliWindow(2, dataset.SplitValidation, dataset.LabelA), "val-cal-session"},
		{cliWindow(3, dataset.SplitValidation, dataset.LabelB), "val-cmp-session"},
		{cliWindow(4, dataset.SplitTest, dataset.LabelA), testBlindSession},
		{cliWindow(5, dataset.SplitTest, dataset.LabelD), testBlindSession},
		{cliWindow(6, dataset.SplitTest, dataset.LabelE), testBlindSession},
		{cliWindow(7, dataset.SplitTest, dataset.LabelA), testHistorySession},
		{cliWindow(8, dataset.SplitTest, dataset.LabelD), testHistorySession},
		{cliWindow(9, dataset.SplitTest, dataset.LabelE), testHistorySession},
	}
	windows := make([]dataset.Window, 0, len(placements))
	for i := range placements {
		p := &placements[i]
		p.window.Provenance.SourceQuality = protocol.SourceQualityPTYScreen
		p.window.Provenance.SessionID = protocol.OpaqueID(p.session)
		p.window.Provenance.CaptureSessionID = protocol.OpaqueID(p.session)
		p.window.Split.GroupDigest = replay.GroupDigest(p.session)
		windows = append(windows, p.window)
	}
	manifest, err := dataset.BuildManifest(windows)
	require.NoError(t, err)
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := write(t, dir, "manifest.json", string(manifestRaw))
	baseRun, loraRun := testCalibrationRuns()
	predictionsFor := func(profile string) string {
		var out strings.Builder
		enc := json.NewEncoder(&out)
		run := baseRun
		if profile == string(coreeval.ProfileLoRA) {
			run = loraRun
		}
		for _, p := range placements {
			if p.window.Split.Split == dataset.SplitTrain {
				continue
			}
			probabilities := map[string]float64{"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0, "G": 0}
			probabilities[string(p.window.Target.Label)] = 1
			line := map[string]any{
				"window_id": p.window.WindowID, "split": p.window.Split.Split,
				"harness": p.window.Input.Harness, "harness_version": p.window.Provenance.HarnessVersion,
				"session_id": p.session, "source_quality": p.window.Provenance.SourceQuality,
				"label": p.window.Target.Label, "probabilities": probabilities,
				"valid_mass": 1, "invalid": false, "latency_ms": 100, "profile": profile,
				"prompt_version": run.PromptVersion, "base_model_repo": run.BaseModelRepo,
				"base_model_revision": run.BaseModelRevision, "runtime": run.Runtime,
				"runtime_version": run.RuntimeVersion, "quantization": run.Quantization,
				"gpu": run.GPU, "predict_concurrency": run.PredictConcurrency,
			}
			if profile == string(coreeval.ProfileLoRA) {
				line["adapter_name"] = run.AdapterName
				line["adapter_revision"] = run.AdapterRevision
				line["adapter_sha256"] = run.AdapterSHA256
				line["training_run_id"] = run.TrainingRunID
			}
			require.NoError(t, enc.Encode(line))
		}
		return out.String()
	}
	base := write(t, dir, "base.jsonl", predictionsFor("base"))
	lora := write(t, dir, "lora.jsonl", predictionsFor("analyst-lora"))
	valCal := write(t, dir, valCalSessionFile, "{\"session_id\":\"val-cal-session\"}\n")
	blind := write(t, dir, blindSessionFile, "{\"session_id\":\"blind-session\"}\n")
	calibration := filepath.Join(dir, "calibration.json")

	checkpoint, err := exec(t,
		flagProfile, "base="+base, flagProfile, "analyst-lora="+lora,
		flagManifest, manifestPath, "--val-cal-sessions", valCal,
		"--calibration-out", calibration,
	)
	require.NoError(t, err)
	require.Contains(t, checkpoint, "Checkpoint comparison (no final gate)")
	calibrationRaw, err := os.ReadFile(calibration)
	require.NoError(t, err)
	_, err = coreeval.ParseCalibrationSet(calibrationRaw)
	require.NoError(t, err)

	final, err := exec(t,
		flagProfile, "base="+base, flagProfile, "analyst-lora="+lora,
		flagManifest, manifestPath, "--val-cal-sessions", valCal,
		"--blind-test-sessions", blind, "--sealed-calibrations", calibration,
	)
	require.NoError(t, err)
	require.Contains(t, final, "blind test (gate denominator)")
	require.Contains(t, final, "historical test regression")
	require.Contains(t, final, "sealed; report values verified exactly")
}
