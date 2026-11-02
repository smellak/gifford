package eval

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/protocol"
)

type partitionFixture struct {
	base, lora []Prediction
	manifest   ManifestCheck
	valCal     SessionSet
	blind      SessionSet
}

func fixedDigest(b byte) protocol.Digest {
	return protocol.Digest("sha256:" + strings.Repeat(string(b), 64))
}

func newPartitionFixture(t *testing.T) partitionFixture {
	t.Helper()
	const (
		calSession        = "val-cal-session"
		cmpSession        = "val-cmp-session"
		blindSession      = "test-blind-session"
		historicalSession = "test-v1-session"
	)
	valCal, err := NewSessionSet([]protocol.OpaqueID{calSession}, fixedDigest('a'))
	require.NoError(t, err)
	blind, err := NewSessionSet([]protocol.OpaqueID{blindSession}, fixedDigest('b'))
	require.NoError(t, err)

	var base, lora []Prediction
	var entries []dataset.ManifestEntry
	baseRun := runFor(promptV1)
	loraRun := baseRun
	loraRun.AdapterName = testAdapterName
	loraRun.AdapterRevision = "partition-rev-1"
	loraRun.AdapterSHA256 = protocol.SHA256Hex(strings.Repeat("a", 64))
	loraRun.TrainingRunID = "partition-training-1"
	n := 0
	add := func(session string, split dataset.Split, truth dataset.Label, bp, lp map[dataset.Label]float64) {
		n++
		id := protocol.ULID(fmt.Sprintf("01JB%022d", n))
		prediction := func(profile Profile, probs map[dataset.Label]float64) Prediction {
			run := baseRun
			adapterRecorded := false
			if profile == ProfileLoRA {
				run, adapterRecorded = loraRun, true
			}
			p := Prediction{
				WindowID: id, Split: split, Harness: protocol.HarnessCodex, HarnessVersion: testHarnessVersion,
				SessionID: protocol.OpaqueID(session), SourceQuality: protocol.SourceQualityPTYScreen, ValidMass: 1,
				Profile: profile, LatencyMS: 100, Run: run, RunRecorded: true,
				AdapterRecorded: adapterRecorded,
			}
			p.Truth, _ = labelIndex(truth)
			for label, probability := range probs {
				i, _ := labelIndex(label)
				p.Probs[i] = probability
			}
			return p
		}
		base = append(base, prediction(ProfileBase, bp))
		lora = append(lora, prediction(ProfileLoRA, lp))
		entries = append(entries, dataset.ManifestEntry{
			WindowID: id, Split: split, Label: truth, EnvironmentDomain: dataset.EnvironmentDomainBankGeneric,
			WindowDigest: fixedDigest('c'), GroupDigest: replay.GroupDigest(session), SourceDigest: fixedDigest('d'),
			Harness: protocol.HarnessCodex, HarnessVersion: testHarnessVersion, SourceQuality: protocol.SourceQualityPTYScreen,
			LabelSource: dataset.LabelSourceManual,
		})
	}
	probs := func(a, b float64) map[dataset.Label]float64 {
		return map[dataset.Label]float64{dataset.LabelA: a, dataset.LabelB: b}
	}
	// val-cal alone has optimum T=2: every row says .9/.1 and 3/4 are A.
	for i := range 8 {
		truth := dataset.LabelA
		if i%4 == 3 {
			truth = dataset.LabelB
		}
		add(calSession, dataset.SplitValidation, truth, probs(.9, .1), probs(.9, .1))
	}
	// These strongly contradictory val-cmp rows would move T if they leaked
	// into fitting. They are only the checkpoint-comparison denominator.
	for range 4 {
		add(cmpSession, dataset.SplitValidation, dataset.LabelB, probs(.99, .01), probs(.99, .01))
	}
	correct := func(label dataset.Label) map[dataset.Label]float64 {
		return map[dataset.Label]float64{label: 1}
	}
	wrongIdle := map[dataset.Label]float64{dataset.LabelA: .9, dataset.LabelD: .1}
	// The candidate loses on blind test.
	for _, truth := range []dataset.Label{dataset.LabelA, dataset.LabelD, dataset.LabelD, dataset.LabelE} {
		lp := wrongIdle
		if truth == dataset.LabelA {
			lp = correct(truth)
		}
		add(blindSession, dataset.SplitTest, truth, correct(truth), lp)
	}
	// It wins on a larger historical set. This must not compensate the blind
	// failure in C1-C4.
	for i := range 12 {
		truth := dataset.LabelD
		if i%3 == 0 {
			truth = dataset.LabelE
		}
		add(historicalSession, dataset.SplitTest, truth, wrongIdle, correct(truth))
	}
	m := dataset.Manifest{
		SchemaVersion: dataset.ManifestSchemaVersion, Contract: protocol.ModelContractAnalyzeAgentState,
		WindowSchemaVersion: dataset.WindowSchemaVersion,
		Policies:            dataset.Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
		Entries:             entries,
	}
	m.Counts = dataset.CountEntries(m.Entries)
	thresholds := dataset.ReadinessThresholds{
		TestMin: 1, RareLabelTestMin: 1, RareLabelTestHarnessesMin: 1,
	}
	readiness := dataset.CheckReadiness(m.Entries, thresholds)
	require.True(t, readiness.Passed)
	m.Readiness = &readiness
	m.DatasetDigest = dataset.ManifestDigest(m)
	require.NoError(t, m.Validate())
	return partitionFixture{base: base, lora: lora, manifest: ManifestCheck{Given: true, Manifest: m}, valCal: valCal, blind: blind}
}

func TestReview291FinalGateCannotBorrowHistoricalCoverage(t *testing.T) {
	t.Parallel()
	f := newPartitionFixture(t)
	checkpoint, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal,
	})
	require.NoError(t, err)
	raw, err := checkpoint.CalibrationJSON()
	require.NoError(t, err)
	sealed, err := ParseCalibrationSet(raw)
	require.NoError(t, err)

	evaluate := func(testMin int64) Report {
		manifest := f.manifest
		thresholds := manifest.Manifest.Readiness.Thresholds
		thresholds.TestMin = testMin
		readiness := dataset.CheckReadiness(manifest.Manifest.Entries, thresholds)
		require.True(t, readiness.Passed, "the complete manifest remains ready")
		manifest.Manifest.Readiness = &readiness
		manifest.Manifest.DatasetDigest = dataset.ManifestDigest(manifest.Manifest)
		r, evalErr := Evaluate(Options{
			Base: f.base, LoRA: f.lora, Manifest: manifest,
			ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
			SealedCalibrations: &sealed, SealedCalibrationsRaw: raw,
		})
		require.NoError(t, evalErr)
		return r
	}

	insufficient := evaluate(5) // four blind windows; historical test supplies twelve more.
	for _, criterion := range insufficient.Gate.Criteria {
		if criterion.ID >= 1 && criterion.ID <= 4 {
			require.Equal(t, VerdictPending, criterion.Verdict)
			require.Contains(t, criterion.Detail, "blind test coverage is insufficient")
		}
	}
	require.Equal(t, VerdictPending, insufficient.Gate.Verdict)

	sufficient := evaluate(4)
	require.NotEqual(t, VerdictPending, sufficient.Gate.Criteria[1].Verdict,
		"once blind coverage passes, C1 is decided from blind metrics")
}

func TestCheckpointFitsOnlyValCalAndScoresOnlyValCmp(t *testing.T) {
	t.Parallel()
	f := newPartitionFixture(t)
	r, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal,
	})
	require.NoError(t, err)
	require.Equal(t, EvaluationCheckpoint, r.Mode)
	require.Empty(t, r.Gate.Criteria, "checkpoint comparison cannot decide the final gate")
	require.InDelta(t, 2, r.Profiles[0].Temperature, 1e-6, "val-cmp must not leak into fitting")
	require.Equal(t, 4, r.Profiles[0].Validation.All.N)
	require.Zero(t, r.Profiles[0].Test.All.N, "checkpoint mode does not score test")
	require.Equal(t, 8, r.Partition.CalibrationWindows)
	require.Equal(t, 4, r.Partition.ComparisonWindows)
	require.Equal(t, 1, r.Partition.ValidationSessions)

	raw, err := r.CalibrationJSON()
	require.NoError(t, err)
	sealed, err := ParseCalibrationSet(raw)
	require.NoError(t, err)
	require.Equal(t, f.valCal.Digest(), sealed.ValidationSessionsSHA256)
	require.Equal(t, protocol.Digest(digest(raw)), r.Partition.CalibrationArtifactSHA256)
	require.Len(t, sealed.Calibrations, 2)
	require.Contains(t, r.Markdown(), "This report cannot pass or fail the final gate.")
}

func TestFinalGateUsesSealedCalibrationAndBlindTestOnly(t *testing.T) {
	t.Parallel()
	f := newPartitionFixture(t)
	checkpoint, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal,
	})
	require.NoError(t, err)
	raw, err := checkpoint.CalibrationJSON()
	require.NoError(t, err)
	sealed, err := ParseCalibrationSet(raw)
	require.NoError(t, err)

	// A distinctive sealed value proves final mode did not call Fit* again.
	for i := range sealed.Calibrations {
		sealed.Calibrations[i].Temperature = 1.25
	}
	raw, err = sealed.Marshal()
	require.NoError(t, err)
	r, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
		SealedCalibrations: &sealed, SealedCalibrationsRaw: raw,
	})
	require.NoError(t, err)
	require.Equal(t, EvaluationFinal, r.Mode)
	for _, p := range r.Profiles {
		require.InDelta(t, 1.25, p.Temperature, 0)
		require.Equal(t, 4, p.Test.All.N)
		require.Equal(t, 12, p.HistoricalTest.All.N)
		require.Equal(t, 4, p.Validation.All.N)
	}
	require.Less(t, r.Profiles[1].Test.All.MacroF1, r.Profiles[0].Test.All.MacroF1)
	require.Greater(t, r.Profiles[1].HistoricalTest.All.MacroF1, r.Profiles[0].HistoricalTest.All.MacroF1)
	criteria := map[int]Criterion{}
	for _, c := range r.Gate.Criteria {
		criteria[c.ID] = c
	}
	require.Equal(t, VerdictFail, criteria[1].Verdict, "historical improvement cannot compensate a blind failure")
	require.Equal(t, protocol.Digest(digest(raw)), r.Partition.CalibrationArtifactSHA256)
	require.Equal(t, 1, r.Partition.BlindTestSessions)
	require.Equal(t, sealed.Calibrations, r.Calibrations.Calibrations, "report T/theta and fitted flags exactly match the seal")
	require.Contains(t, r.Markdown(), "blind test (gate denominator)")
	require.Contains(t, r.Markdown(), "historical test regression")
	require.Contains(t, r.Markdown(), "| base | base | 1.25 | true |", "sealed T is rendered exactly")
}

func TestPartitionedEvaluationFailsClosed(t *testing.T) {
	t.Parallel()
	f := newPartitionFixture(t)
	checkpoint, err := Evaluate(Options{
		Base: f.base, LoRA: f.lora, Manifest: f.manifest,
		ValidationCalibrationSessions: f.valCal,
	})
	require.NoError(t, err)
	raw, err := checkpoint.CalibrationJSON()
	require.NoError(t, err)
	sealed, err := ParseCalibrationSet(raw)
	require.NoError(t, err)

	t.Run("blind prediction missing", func(t *testing.T) {
		t.Parallel()
		base, lora := append([]Prediction(nil), f.base...), append([]Prediction(nil), f.lora...)
		const firstBlind = 12
		base = append(base[:firstBlind], base[firstBlind+1:]...)
		lora = append(lora[:firstBlind], lora[firstBlind+1:]...)
		_, err := Evaluate(Options{
			Base: base, LoRA: lora, Manifest: f.manifest,
			ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
			SealedCalibrations: &sealed, SealedCalibrationsRaw: raw,
		})
		require.ErrorIs(t, err, ErrCalibration)
	})

	t.Run("blind list points at validation", func(t *testing.T) {
		t.Parallel()
		wrong, err := NewSessionSet([]protocol.OpaqueID{"val-cal-session"}, fixedDigest('e'))
		require.NoError(t, err)
		_, err = Evaluate(Options{
			Base: f.base, LoRA: f.lora, Manifest: f.manifest,
			ValidationCalibrationSessions: f.valCal, BlindTestSessions: wrong,
			SealedCalibrations: &sealed, SealedCalibrationsRaw: raw,
		})
		require.ErrorIs(t, err, ErrCalibration)
	})

	t.Run("calibration artifact names another val-cal list", func(t *testing.T) {
		t.Parallel()
		other := sealed
		other.ValidationSessionsSHA256 = fixedDigest('f')
		_, err := Evaluate(Options{
			Base: f.base, LoRA: f.lora, Manifest: f.manifest,
			ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
			SealedCalibrations: &other,
		})
		require.ErrorIs(t, err, ErrCalibration)
	})

	t.Run("calibration bytes disagree with parsed values", func(t *testing.T) {
		t.Parallel()
		changed := sealed
		changed.Calibrations = append([]SealedCalibration(nil), sealed.Calibrations...)
		changed.Calibrations[0].Temperature = 1.75
		changedRaw, marshalErr := changed.Marshal()
		require.NoError(t, marshalErr)
		_, err := Evaluate(Options{
			Base: f.base, LoRA: f.lora, Manifest: f.manifest,
			ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
			SealedCalibrations: &sealed, SealedCalibrationsRaw: changedRaw,
		})
		require.ErrorIs(t, err, ErrCalibration)
	})

	t.Run("calibration belongs to another lora checkpoint", func(t *testing.T) {
		t.Parallel()
		lora := append([]Prediction(nil), f.lora...)
		for i := range lora {
			lora[i].Run.AdapterRevision = "partition-rev-2"
		}
		_, err := Evaluate(Options{
			Base: f.base, LoRA: lora, Manifest: f.manifest,
			ValidationCalibrationSessions: f.valCal, BlindTestSessions: f.blind,
			SealedCalibrations: &sealed, SealedCalibrationsRaw: raw,
		})
		require.ErrorIs(t, err, ErrCalibration)
	})

	t.Run("v2 manifest cannot use legacy refitting", func(t *testing.T) {
		t.Parallel()
		manifest := f.manifest
		manifest.Manifest.ScreenForm = dataset.ScreenFormLiveV2
		_, err := Evaluate(Options{Base: f.base, LoRA: f.lora, Manifest: manifest})
		require.ErrorIs(t, err, ErrCalibration)
	})
}

func TestCalibrationArtifactRequiresEveryMember(t *testing.T) {
	t.Parallel()
	set := CalibrationSet{
		SchemaVersion:            calibrationSchemaVersion,
		ValidationSessionsSHA256: fixedDigest('a'),
		Calibrations: []SealedCalibration{{
			Profile: ProfileBase, Role: "base", Temperature: 1,
			TemperatureFitted: true, Theta: .9, ThetaFitted: true,
			CalibrationInputSHA256: fixedDigest('c'), CalibrationInputLines: 8,
			Run: runFor(promptV1),
		}},
	}
	raw, err := set.Marshal()
	require.NoError(t, err)
	_, err = ParseCalibrationSet(raw)
	require.NoError(t, err)
	for name, edit := range map[string]func(string) string{
		"missing fitted flag": func(s string) string { return strings.Replace(s, "      \"temperature_fitted\": true,\n", "", 1) },
		"unknown member": func(s string) string {
			return strings.Replace(s, "      \"role\": \"base\",", "      \"role\": \"base\",\n      \"extra\": true,", 1)
		},
		"repeated member": func(s string) string {
			return strings.Replace(s, "      \"theta\": 0.9,", "      \"theta\": 0.9,\n      \"theta\": 0.8,", 1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseCalibrationSet([]byte(edit(string(raw))))
			require.ErrorIs(t, err, ErrCalibration)
		})
	}
}
