package eval

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

func developmentFixture(t *testing.T) ([]Prediction, []Prediction, ManifestCheck, CalibrationSet, []byte) {
	t.Helper()
	f := newPartitionFixture(t)
	base := make([]Prediction, 0)
	lora := make([]Prediction, 0)
	manifest := f.manifest.Manifest
	manifest.Entries = make([]dataset.ManifestEntry, 0, len(f.manifest.Manifest.Entries))
	for i, p := range f.base {
		if p.Split != dataset.SplitValidation {
			continue
		}
		bp, lp := p, f.lora[i]
		bp.SourceQuality = protocol.SourceQualitySynthetic
		lp.SourceQuality = protocol.SourceQualitySynthetic
		base, lora = append(base, bp), append(lora, lp)
	}
	for _, e := range f.manifest.Manifest.Entries {
		if e.Split != dataset.SplitValidation {
			continue
		}
		e.SourceQuality = protocol.SourceQualitySynthetic
		e.LabelSource = dataset.LabelSourceSynthetic
		manifest.Entries = append(manifest.Entries, e)
	}
	manifest.Counts = dataset.CountEntries(manifest.Entries)
	manifest.Readiness = nil
	manifest.DatasetDigest = dataset.ManifestDigest(manifest)
	require.NoError(t, manifest.Validate())
	calibrations := CalibrationSet{
		SchemaVersion:            calibrationSchemaVersion,
		ValidationSessionsSHA256: f.valCal.Digest(),
		Calibrations: []SealedCalibration{
			{
				Profile: ProfileBase, Role: string(ProfileBase), Temperature: 1.25,
				TemperatureFitted: true, Theta: .75, ThetaFitted: true,
				CalibrationInputSHA256: fixedDigest('e'), CalibrationInputLines: 8,
				Run: f.base[0].Run,
			},
			{
				Profile: ProfileLoRA, Role: string(ProfileLoRA), Temperature: 1.5,
				TemperatureFitted: true, Theta: .8, ThetaFitted: true,
				CalibrationInputSHA256: fixedDigest('f'), CalibrationInputLines: 8,
				Run: f.lora[0].Run,
			},
		},
	}
	raw, err := calibrations.Marshal()
	require.NoError(t, err)
	return base, lora, ManifestCheck{Given: true, Manifest: manifest}, calibrations, raw
}

func TestDevelopmentPredictionsCannotChangeCalibrationOrGateWindows(t *testing.T) {
	t.Parallel()
	production := newPartitionFixture(t)
	checkpoint := func() Report {
		r, err := Evaluate(Options{
			Base: production.base, LoRA: production.lora, Manifest: production.manifest,
			ValidationCalibrationSessions: production.valCal,
		})
		require.NoError(t, err)
		return r
	}
	before := checkpoint()
	beforeCalibration, err := before.CalibrationJSON()
	require.NoError(t, err)

	base, lora, manifest, calibrations, raw := developmentFixture(t)
	first, err := ScoreDevelopment(DevelopmentOptions{
		Base: base, LoRA: lora, Manifest: manifest, DatasetSHA256: manifest.Manifest.DatasetDigest,
		Calibrations: &calibrations, CalibrationsRaw: raw,
	})
	require.NoError(t, err)
	for i := range lora {
		lora[i].Probs = [numLabels]float64{}
		lora[i].Probs[idxG] = 1
	}
	second, err := ScoreDevelopment(DevelopmentOptions{
		Base: base, LoRA: lora, Manifest: manifest, DatasetSHA256: manifest.Manifest.DatasetDigest,
		Calibrations: &calibrations, CalibrationsRaw: raw,
	})
	require.NoError(t, err)
	require.InDelta(t, first.Profiles[1].Temperature, second.Profiles[1].Temperature, 0)
	require.InDelta(t, first.Profiles[1].Theta, second.Profiles[1].Theta, 0)
	require.NotEqual(t, first.Profiles[1].Validation.All.MacroF1, second.Profiles[1].Validation.All.MacroF1)

	after := checkpoint()
	afterCalibration, err := after.CalibrationJSON()
	require.NoError(t, err)
	require.Equal(t, beforeCalibration, afterCalibration)
	require.Equal(t, before.Partition, after.Partition, "development inputs cannot alter val-cal/val-cmp windows")
}

func TestDevelopmentEvidenceIsSealedAndCannotEnterRealCalibration(t *testing.T) {
	t.Parallel()
	base, lora, manifest, calibrations, raw := developmentFixture(t)
	_, err := ScoreDevelopment(DevelopmentOptions{
		Base: base, LoRA: lora, Manifest: manifest, DatasetSHA256: fixedDigest('f'),
		Calibrations: &calibrations, CalibrationsRaw: raw,
	})
	require.ErrorIs(t, err, ErrDevelopment)
	require.Contains(t, err.Error(), "expected seal")

	production := newPartitionFixture(t)
	_, err = Evaluate(Options{
		Base: base, LoRA: lora, Manifest: manifest,
		ValidationCalibrationSessions: production.valCal,
	})
	require.ErrorIs(t, err, ErrCalibration)
	require.Contains(t, err.Error(), "separate development or safety path")
}

func TestReview291OrdinaryEvaluationRejectsDevelopmentCorpus(t *testing.T) {
	t.Parallel()
	base, lora, manifest, _, _ := developmentFixture(t)
	_, err := Evaluate(Options{Base: base, LoRA: lora, Manifest: manifest})
	require.ErrorIs(t, err, ErrCalibration)
	require.Contains(t, err.Error(), "synthetic validation or test predictions")
}
