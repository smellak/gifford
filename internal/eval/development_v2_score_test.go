package eval

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
)

const (
	developmentV2ScoreRoot = "testdata/development-v2"
	// developmentV2FrozenCalibration is checkpoint-selection input only. Its
	// values are fixed here rather than fitted from the development corpus.
	developmentV2FrozenCalibration = `{
  "schema_version": 2,
  "validation_sessions_sha256": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "calibrations": [
    {
      "profile": "base",
      "role": "base",
      "temperature": 1,
      "temperature_fitted": true,
      "theta": 0.97,
      "theta_fitted": true,
      "calibration_input_sha256": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "calibration_input_lines": 1,
      "run": {
        "prompt_version": "development-fixture.v1",
        "base_model_repo": "example.invalid/base",
        "base_model_revision": "fixture-revision",
        "runtime": "fixture",
        "runtime_version": "1.0",
        "quantization": "",
        "gpu": "fixture",
        "predict_concurrency": 1
      }
    },
    {
      "profile": "analyst-lora",
      "role": "analyst-lora",
      "temperature": 1,
      "temperature_fitted": true,
      "theta": 0.97,
      "theta_fitted": true,
      "calibration_input_sha256": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
      "calibration_input_lines": 1,
      "run": {
        "prompt_version": "development-fixture.v1",
        "base_model_repo": "example.invalid/base",
        "base_model_revision": "fixture-revision",
        "runtime": "fixture",
        "runtime_version": "1.0",
        "quantization": "",
        "gpu": "fixture",
        "predict_concurrency": 1,
        "adapter_name": "fixture-adapter",
        "adapter_revision": "fixture-revision",
        "adapter_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        "training_run_id": "development-fixture-training"
      }
    }
  ]
}
`
)

func TestDevelopmentV2CanonicalCorpusScoresEndToEnd(t *testing.T) {
	manifest, windows := loadDevelopmentV2ScoreCorpus(t)
	base := developmentV2ScorePredictions(t, windows, ProfileBase)
	lora := developmentV2ScorePredictions(t, windows, ProfileLoRA)
	calibrations, err := ParseCalibrationSet([]byte(developmentV2FrozenCalibration))
	require.NoError(t, err)

	options := DevelopmentOptions{
		Base: base, LoRA: lora,
		Manifest:        ManifestCheck{Given: true, Manifest: manifest},
		DatasetSHA256:   manifest.DatasetDigest,
		Calibrations:    &calibrations,
		CalibrationsRaw: []byte(developmentV2FrozenCalibration),
	}
	report, err := ScoreDevelopment(options)
	require.NoError(t, err)
	require.Equal(t, manifest.DatasetDigest, report.DatasetSHA256)
	require.Len(t, report.Profiles, 2)
	for _, profile := range report.Profiles {
		require.Equal(t, len(windows), profile.Validation.All.N)
		require.InDelta(t, 1, profile.Validation.All.Accuracy, 0)
		require.InDelta(t, 1, profile.Validation.All.MacroF1, 0)
	}

	foreignBase := append([]Prediction(nil), base...)
	foreignLoRA := append([]Prediction(nil), lora...)
	foreignBase[0].SessionID = "foreign-development-session"
	foreignLoRA[0].SessionID = "foreign-development-session"
	options.Base, options.LoRA = foreignBase, foreignLoRA
	_, err = ScoreDevelopment(options)
	require.ErrorIs(t, err, ErrDevelopment)
	require.Contains(t, err.Error(), "do not exactly match the sealed manifest")
}

func loadDevelopmentV2ScoreCorpus(t *testing.T) (dataset.Manifest, []dataset.Window) {
	t.Helper()
	manifestRaw, err := os.ReadFile(filepath.Join(developmentV2ScoreRoot, dataset.ManifestFile))
	require.NoError(t, err)
	manifest, err := dataset.ParseManifest(bytes.NewReader(manifestRaw))
	require.NoError(t, err)

	windows := make([]dataset.Window, 0, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		raw, err := os.ReadFile(filepath.Join(
			developmentV2ScoreRoot, dataset.WindowsDir, dataset.WindowFile(string(entry.WindowID)),
		))
		require.NoError(t, err)
		window, err := dataset.ParseWindow(bytes.NewReader(raw))
		require.NoError(t, err)
		require.Equal(t, entry.WindowID, window.WindowID)
		digest, err := dataset.WindowDigest(window)
		require.NoError(t, err)
		require.Equal(t, entry.WindowDigest, digest)
		windows = append(windows, window)
	}
	require.Len(t, windows, 120)
	return manifest, windows
}

func developmentV2ScorePredictions(
	t *testing.T,
	windows []dataset.Window,
	profile Profile,
) []Prediction {
	t.Helper()
	predictions := make([]Prediction, 0, len(windows))
	for _, window := range windows {
		truth, ok := labelIndex(window.Target.Label)
		require.True(t, ok)
		prediction := Prediction{
			WindowID: window.WindowID, Split: window.Split.Split,
			Harness: window.Input.Harness, HarnessVersion: window.Provenance.HarnessVersion,
			SessionID: window.Provenance.SessionID, SourceQuality: window.Provenance.SourceQuality,
			Truth: truth, ValidMass: 1, LatencyMS: 1, Profile: profile,
			SecondsSinceLastOutput: window.Input.SecondsSinceLastOutput,
			OutputTimingAvailable:  true,
		}
		prediction.Probs[truth] = 1
		predictions = append(predictions, prediction)
	}
	return predictions
}
