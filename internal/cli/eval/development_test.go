package eval

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	coreeval "github.com/smellak/gifford/internal/eval"
	"github.com/smellak/gifford/internal/protocol"
)

func TestDevelopmentCommandUsesFrozenCalibrationOnSeparateEvidence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	windows := []dataset.Window{
		cliWindow(31, dataset.SplitValidation, dataset.LabelA),
		cliWindow(32, dataset.SplitValidation, dataset.LabelD),
	}
	for i := range windows {
		session := protocol.OpaqueID("development-session")
		windows[i].Target.Source = dataset.LabelSourceSynthetic
		windows[i].Provenance.SessionID = session
		windows[i].Provenance.CaptureSessionID = session
		windows[i].Split.GroupDigest = replay.GroupDigest(string(session))
	}
	manifest, err := dataset.BuildManifest(windows)
	require.NoError(t, err)
	manifest.Readiness = nil
	manifest.DatasetDigest = dataset.ManifestDigest(manifest)
	require.NoError(t, manifest.Validate())
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	manifestPath := write(t, dir, "development-manifest.json", string(manifestRaw))

	predictions := func(profile coreeval.Profile) string {
		var out strings.Builder
		enc := json.NewEncoder(&out)
		for _, window := range windows {
			probabilities := map[string]float64{"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "F": 0, "G": 0}
			probabilities[string(window.Target.Label)] = 1
			require.NoError(t, enc.Encode(map[string]any{
				"window_id": window.WindowID, "split": dataset.SplitValidation,
				"harness": window.Input.Harness, "harness_version": window.Provenance.HarnessVersion,
				"session_id": window.Provenance.SessionID, "source_quality": protocol.SourceQualitySynthetic,
				"label": window.Target.Label, "probabilities": probabilities,
				"valid_mass": 1, "invalid": false, "latency_ms": 100, "profile": profile,
			}))
		}
		return out.String()
	}
	base := write(t, dir, "development-base.jsonl", predictions(coreeval.ProfileBase))
	lora := write(t, dir, "development-lora.jsonl", predictions(coreeval.ProfileLoRA))
	calibrations := coreeval.CalibrationSet{
		SchemaVersion:            1,
		ValidationSessionsSHA256: protocol.Digest("sha256:" + strings.Repeat("a", 64)),
		Calibrations: []coreeval.SealedCalibration{
			{Profile: coreeval.ProfileBase, Role: "base", Temperature: 1.25, TemperatureFitted: true, Theta: .75, ThetaFitted: true},
			{Profile: coreeval.ProfileLoRA, Role: "analyst-lora", Temperature: 1.5, TemperatureFitted: true, Theta: .8, ThetaFitted: true},
		},
	}
	calibrationRaw, err := calibrations.Marshal()
	require.NoError(t, err)
	calibrationPath := write(t, dir, "calibration.json", string(calibrationRaw))

	report, err := exec(t, "development",
		"--profile", "base="+base, "--profile", "analyst-lora="+lora,
		"--manifest", manifestPath, "--dataset-sha256", string(manifest.DatasetDigest),
		"--sealed-calibrations", calibrationPath,
	)
	require.NoError(t, err)
	require.Contains(t, report, "Calibration is frozen")
	require.Contains(t, report, "this report is not val-cmp and has no gate")
	require.Contains(t, report, "| base | 1.25 | 0.75 |")

	_, err = exec(t, "development",
		"--profile", "base="+base, "--profile", "analyst-lora="+lora,
		"--manifest", manifestPath, "--dataset-sha256", "sha256:"+strings.Repeat("f", 64),
		"--sealed-calibrations", calibrationPath,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected seal")
}
