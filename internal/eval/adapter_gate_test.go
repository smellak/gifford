package eval

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
)

func adapterGateOptions(t *testing.T) Options {
	t.Helper()
	baseLines := gateSet("base", false, 100)
	loraLines := gateSet("analyst-lora", true, 900)
	manifest := manifestFor(baseLines)
	manifest.Manifest.Readiness = &dataset.Readiness{Passed: true, Faults: []dataset.Fault{}}
	safetyManifest := loadSafetyManifest(t)
	baseSafety := safetyPredictions(safetyManifest, runFor(promptV1))
	loraSafety := safetyPredictions(safetyManifest, runFor(promptV1))
	for i := range loraSafety {
		loraSafety[i].Profile = ProfileLoRA
	}
	return Options{
		Base: read(t, file(baseLines...), ProfileBase), LoRA: read(t, file(loraLines...), ProfileLoRA),
		BaseRaw: []byte("adapter base"), LoRARaw: []byte("adapter lora"), Manifest: manifest,
		SafetyBase: baseSafety, SafetyCandidate: loraSafety,
		SafetyBaseRaw: []byte("safety base"), SafetyCandidateRaw: []byte("safety lora"),
		SafetyManifest: ManifestCheck{Given: true, Manifest: safetyManifest},
	}
}

func TestAdapterGatePassesAllCriteria(t *testing.T) {
	t.Parallel()
	r, err := Evaluate(adapterGateOptions(t))
	require.NoError(t, err)
	require.Equal(t, VerdictPass, r.Gate.Verdict)
	for id := range 6 {
		require.Equal(t, VerdictPass, criterion(r, id).Verdict, "criterion %d", id)
	}
	require.Positive(t, r.Profiles[0].Guards.OutputQuietUnavailable)
	require.Positive(t, r.Profiles[1].Guards.OutputQuietUnavailable)
	require.Zero(t, r.Safety.Candidate.Failures)
	require.True(t, r.Safety.Candidate.Passed)
}

func TestAdapterGateFailsWhenResidualFalseSafeRises(t *testing.T) {
	t.Parallel()
	o := adapterGateOptions(t)
	o.Base[1].Probs = [numLabels]float64{}
	o.Base[1].Probs[idxD] = 1
	o.LoRA[1].Probs = [numLabels]float64{}
	o.LoRA[1].Probs[idxA] = 1

	r, err := Evaluate(o)
	require.NoError(t, err)
	require.Equal(t, VerdictFail, criterion(r, 2).Verdict)
	require.Equal(t, VerdictFail, r.Gate.Verdict)
}

func TestAdapterGateFailsSafetyAtOwnThreshold(t *testing.T) {
	t.Parallel()
	o := adapterGateOptions(t)
	o.SafetyCandidate[0].Probs = [numLabels]float64{}
	o.SafetyCandidate[0].Probs[idxA] = 1

	r, err := Evaluate(o)
	require.NoError(t, err)
	require.Equal(t, VerdictFail, criterion(r, 5).Verdict)
	require.Equal(t, 1, r.Safety.Candidate.ThresholdFailure)
	require.Equal(t, VerdictFail, r.Gate.Verdict)
}

func TestAdapterGateSafetyIsPendingWithoutPredictions(t *testing.T) {
	t.Parallel()
	o := adapterGateOptions(t)
	o.SafetyBase, o.SafetyCandidate = nil, nil
	o.SafetyBaseRaw, o.SafetyCandidateRaw = nil, nil

	r, err := Evaluate(o)
	require.NoError(t, err)
	require.Equal(t, VerdictPending, criterion(r, 5).Verdict)
	require.Equal(t, VerdictPending, r.Gate.Verdict)
	require.Nil(t, r.Safety)
}

func TestAdapterGateOnlyReportsBaselineSafety(t *testing.T) {
	t.Parallel()
	o := adapterGateOptions(t)
	o.SafetyBase[0].Invalid = true
	o.SafetyBase[0].ErrorClass = ClassCallFailed

	r, err := Evaluate(o)
	require.NoError(t, err)
	require.Equal(t, VerdictPass, criterion(r, 5).Verdict)
	require.Equal(t, VerdictPass, r.Gate.Verdict)
	require.Equal(t, 1, r.Safety.Baseline.Failures)
	require.Zero(t, r.Safety.Candidate.Failures)
}
