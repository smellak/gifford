package eval

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

const (
	promptV1           = "analyze_agent_state.v1"
	promptV2           = "analyze_agent_state.v2"
	testBaseModelRepo  = "example.invalid/glimmer"
	testBaseModelRev   = "revision-1"
	testRuntime        = "vllm"
	testRuntimeVersion = "0.9.0"
	testQuantization   = "fp8"
	testGPU            = "L4"
)

func runFor(prompt string) RunProvenance {
	return RunProvenance{
		PromptVersion: prompt, BaseModelRepo: testBaseModelRepo, BaseModelRevision: testBaseModelRev,
		Runtime: testRuntime, RuntimeVersion: "1.2.3", Quantization: testQuantization, GPU: testGPU, PredictConcurrency: 1,
	}
}

func promptPrediction(n int, split dataset.Split, truth dataset.Label, probs map[dataset.Label]float64, latency, since float64, run RunProvenance) Prediction {
	i, ok := labelIndex(truth)
	if !ok {
		panic("bad test label")
	}
	p := Prediction{
		WindowID: protocol.ULID(fmt.Sprintf("01JB%022d", n)), Split: split,
		Harness: protocol.HarnessCodex, HarnessVersion: "synthetic-v1",
		SessionID: protocol.OpaqueID(fmt.Sprintf("synthetic-%d", n)), SourceQuality: protocol.SourceQualitySynthetic,
		Truth: i, ValidMass: 1, LatencyMS: latency, Profile: ProfileBase,
		Run: run, RunRecorded: true, SecondsSinceLastOutput: since, OutputTimingAvailable: true,
	}
	for label, v := range probs {
		j, _ := labelIndex(label)
		p.Probs[j] = v
	}
	return p
}

func mainPromptPredictions(prompt string, candidate bool) []Prediction {
	run := runFor(prompt)
	val := promptPrediction(101, dataset.SplitValidation, dataset.LabelC, map[dataset.Label]float64{dataset.LabelG: 1}, 50, 1, run)
	val.Invalid, val.ValidMass = true, 0
	ps := []Prediction{
		val,
		promptPrediction(102, dataset.SplitTest, dataset.LabelA, map[dataset.Label]float64{dataset.LabelA: 1}, 100, 1, run),
		promptPrediction(103, dataset.SplitTest, dataset.LabelD, map[dataset.Label]float64{dataset.LabelA: 1}, 100, 0.1, run),
		promptPrediction(104, dataset.SplitTest, dataset.LabelD, map[dataset.Label]float64{dataset.LabelD: 1}, 100, 1, run),
		promptPrediction(105, dataset.SplitTest, dataset.LabelE, map[dataset.Label]float64{dataset.LabelE: 1}, 100, 1, run),
		promptPrediction(106, dataset.SplitTest, dataset.LabelC, map[dataset.Label]float64{dataset.LabelC: 1}, 100, 1, run),
	}
	if candidate {
		ps[2].Probs = [numLabels]float64{}
		ps[2].Probs[idxD] = 1
		for i := 1; i < len(ps); i++ {
			ps[i].LatencyMS = 900
		}
	}
	return ps
}

func manifestOf(ps []Prediction) dataset.Manifest {
	m := dataset.Manifest{DatasetDigest: protocol.Digest("sha256:" + strings.Repeat("1", 64))}
	for _, p := range ps {
		m.Entries = append(m.Entries, dataset.ManifestEntry{
			WindowID: p.WindowID, Split: p.Split, Label: labels()[p.Truth], Harness: p.Harness,
			HarnessVersion: p.HarnessVersion, SourceQuality: p.SourceQuality,
		})
	}
	return m
}

func loadSafetyManifest(t *testing.T) dataset.Manifest {
	t.Helper()
	raw, err := os.ReadFile("testdata/safety/manifest.json")
	require.NoError(t, err)
	m, err := dataset.ParseManifest(bytes.NewReader(raw))
	require.NoError(t, err)
	return m
}

func safetyPredictions(m dataset.Manifest, run RunProvenance) []Prediction {
	ps := make([]Prediction, 0, len(m.Entries))
	for n, e := range m.Entries {
		p := promptPrediction(1, e.Split, e.Label, map[dataset.Label]float64{e.Label: 1}, 80, 1, run)
		p.WindowID, p.Harness, p.HarnessVersion, p.SourceQuality = e.WindowID, e.Harness, e.HarnessVersion, e.SourceQuality
		p.SessionID = protocol.OpaqueID(fmt.Sprintf("safety-eval-%d", n+1))
		ps = append(ps, p)
	}
	return ps
}

func promptOptions(t *testing.T) Options {
	t.Helper()
	base := mainPromptPredictions(promptV1, false)
	candidate := mainPromptPredictions(promptV2, true)
	safetyManifest := loadSafetyManifest(t)
	return Options{
		Base: base, Candidate: candidate, CandidatePrompt: promptV2,
		BaseRaw: []byte("baseline"), CandidateRaw: []byte("candidate"),
		Manifest:        ManifestCheck{Given: true, Manifest: manifestOf(base)},
		SafetyBase:      safetyPredictions(safetyManifest, runFor(promptV1)),
		SafetyCandidate: safetyPredictions(safetyManifest, runFor(promptV2)),
		SafetyBaseRaw:   []byte("safety baseline"), SafetyCandidateRaw: []byte("safety candidate"),
		SafetyManifest: ManifestCheck{Given: true, Manifest: safetyManifest},
	}
}

func criterion(r Report, id int) Criterion {
	for _, c := range r.Gate.Criteria {
		if c.ID == id {
			return c
		}
	}
	panic("criterion not found")
}

func TestPromptGatePassesAllApplicableCriteria(t *testing.T) {
	t.Parallel()
	r, err := Evaluate(promptOptions(t))
	require.NoError(t, err)
	require.Equal(t, VerdictPass, r.Gate.Verdict)
	require.Equal(t, VerdictNA, criterion(r, 0).Verdict)
	require.Equal(t, "no aplica: sin entrenamiento", criterion(r, 0).Detail)
	for _, id := range []int{1, 2, 3, 4, 5} {
		require.Equal(t, VerdictPass, criterion(r, id).Verdict, "criterion %d", id)
	}
	require.Equal(t, 1, r.Profiles[0].Guards.BlockedByOutputQuiet)
	require.Equal(t, Rate{Den: 4}, r.Profiles[0].Guards.ResidualFalseSafe)
	require.Equal(t, Rate{Den: 4}, r.Profiles[1].Guards.ResidualFalseSafe)
	require.NotNil(t, r.Safety)
	require.Equal(t, 24, r.Safety.Total)
	require.True(t, r.Safety.Candidate.Passed)
}

func TestPromptGateSeededMutants(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]struct {
		criterion int
		mutate    func(*Options)
	}{
		"criterion 2 residual false safe": {2, func(o *Options) {
			o.Candidate[5].Probs = [numLabels]float64{}
			o.Candidate[5].Probs[idxA], o.Candidate[5].Probs[2] = 0.49, 0.51
		}},
		"criterion 4 latency": {4, func(o *Options) {
			for i := 1; i < len(o.Candidate); i++ {
				o.Candidate[i].LatencyMS = 1601
			}
		}},
		"criterion 5 safety": {5, func(o *Options) {
			o.SafetyCandidate[0].Probs = [numLabels]float64{}
			o.SafetyCandidate[0].Probs[idxA] = 1
		}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := promptOptions(t)
			mutate.mutate(&o)
			r, err := Evaluate(o)
			require.NoError(t, err)
			require.Equal(t, VerdictFail, criterion(r, mutate.criterion).Verdict)
			require.Equal(t, VerdictFail, r.Gate.Verdict)
		})
	}
}

func TestSafetyCriterionFailsClosedOnMissingInvalidAndError(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*Options){
		"missing fixture": func(o *Options) {
			o.SafetyBase = o.SafetyBase[1:]
			o.SafetyCandidate = o.SafetyCandidate[1:]
		},
		"invalid output": func(o *Options) {
			p := &o.SafetyCandidate[0]
			p.Probs = [numLabels]float64{}
			p.Probs[idxG], p.ValidMass, p.Invalid = 1, 0, true
		},
		"call error": func(o *Options) {
			p := &o.SafetyCandidate[0]
			p.Probs = [numLabels]float64{}
			p.Probs[idxG], p.ValidMass, p.Invalid, p.ErrorClass = 1, 0, true, ClassCallFailed
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := promptOptions(t)
			mutate(&o)
			r, err := Evaluate(o)
			require.NoError(t, err)
			require.Equal(t, VerdictFail, criterion(r, 5).Verdict)
			require.Positive(t, r.Safety.Candidate.Failures)
		})
	}
}

func TestSafetyCriterionUsesCandidateAndReportsBaseline(t *testing.T) {
	t.Parallel()
	o := promptOptions(t)
	p := &o.SafetyBase[0]
	p.Probs = [numLabels]float64{}
	p.Probs[idxG], p.ValidMass, p.Invalid, p.ErrorClass = 1, 0, true, ClassCallFailed
	r, err := Evaluate(o)
	require.NoError(t, err)
	require.Equal(t, VerdictPass, criterion(r, 5).Verdict)
	require.Equal(t, VerdictPass, r.Gate.Verdict)
	require.Equal(t, 1, r.Safety.Baseline.Failures)
	require.Zero(t, r.Safety.Candidate.Failures)
	require.True(t, r.Safety.Candidate.Passed)
	require.Contains(t, criterion(r, 5).Detail, "baseline failures 1/24; candidate failures 0/24")
}

func TestOutputQuietReplayUsesTheWriterThreshold(t *testing.T) {
	t.Parallel()
	run := runFor(promptV2)
	unsafe := func(seconds float64, available bool) GuardReplay {
		p := promptPrediction(501, dataset.SplitTest, dataset.LabelC, map[dataset.Label]float64{dataset.LabelA: 1}, 1, seconds, run)
		p.OutputTimingAvailable = available
		return replayGuards([]Prediction{p}, 1, 0.5)
	}
	before := unsafe(session.OutputQuietPeriod.Seconds()-0.001, true)
	require.Equal(t, 1, before.BlockedByOutputQuiet)
	require.Zero(t, before.ResidualFalseSafe.Num)
	at := unsafe(session.OutputQuietPeriod.Seconds(), true)
	require.Zero(t, at.BlockedByOutputQuiet)
	require.Equal(t, 1, at.ResidualFalseSafe.Num)
	unavailable := unsafe(0, false)
	require.Equal(t, 1, unavailable.OutputQuietUnavailable)
	require.Equal(t, 1, unavailable.ResidualFalseSafe.Num, "unavailable guard is PASS in the worst case")
}

func TestPromptComparisonRejectsUnreproducibleRuns(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		mutate func(*Options)
		want   string
	}{
		"legacy file": {func(o *Options) {
			for i := range o.Base {
				o.Base[i].RunRecorded = false
			}
		}, "lacks run provenance; predict it again with --concurrency 1"},
		"old concurrency four": {func(o *Options) {
			for i := range o.Base {
				o.Base[i].Run.PredictConcurrency = 4
			}
		}, "predict_concurrency must be 1, got 4"},
		"mixed provenance": {func(o *Options) {
			o.Base[1].Run.RuntimeVersion = "mixed-version"
		}, "mixes run provenance"},
		"different runtime": {func(o *Options) {
			for i := range o.Candidate {
				o.Candidate[i].Run.GPU = "different-gpu"
			}
		}, "different model or runtime identity"},
		"different output timing": {func(o *Options) {
			o.Candidate[1].SecondsSinceLastOutput++
		}, "disagree on a window's facts"},
		"same prompt": {func(o *Options) {
			o.CandidatePrompt = promptV1
			for i := range o.Candidate {
				o.Candidate[i].Run.PromptVersion = promptV1
			}
		}, "same prompt version"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := promptOptions(t)
			tc.mutate(&o)
			_, err := Evaluate(o)
			require.ErrorIs(t, err, ErrPredictions)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestPromptComparisonRequiresTheFixedSafetySuite(t *testing.T) {
	t.Parallel()
	o := promptOptions(t)
	o.SafetyManifest.Manifest.DatasetDigest = protocol.Digest("sha256:" + strings.Repeat("0", 64))
	_, err := Evaluate(o)
	require.ErrorIs(t, err, ErrPredictions)
	require.Contains(t, err.Error(), "safety manifest is not the fixed prompt suite")
}

func TestPredictionRunProvenanceRoundTrips(t *testing.T) {
	t.Parallel()
	base := line{n: 701, truth: "C", probs: map[string]float64{"C": 1}}.json()
	run := runFor(promptV2)
	encoded := strings.TrimSuffix(base, "}") + fmt.Sprintf(
		`,"prompt_version":%q,"base_model_repo":%q,"base_model_revision":%q,"runtime":%q,"runtime_version":%q,"quantization":%q,"gpu":%q,"predict_concurrency":%d,"seconds_since_last_output":0.3}`,
		run.PromptVersion, run.BaseModelRepo, run.BaseModelRevision, run.Runtime, run.RuntimeVersion, run.Quantization, run.GPU, run.PredictConcurrency)
	ps := read(t, encoded+"\n", ProfileBase)
	require.True(t, ps[0].RunRecorded)
	require.Equal(t, run, ps[0].Run)
	require.True(t, ps[0].OutputTimingAvailable)
	require.InDelta(t, 0.3, ps[0].SecondsSinceLastOutput, 0)
}

func TestPromptReportIsStableAndAuditable(t *testing.T) {
	t.Parallel()
	r, err := Evaluate(promptOptions(t))
	require.NoError(t, err)
	first := r.Markdown()
	require.Equal(t, first, r.Markdown())
	for _, text := range []string{
		"| role | profile | prompt version | model repo |",
		"## Calibration (validation only)",
		"Safety fixtures never fit T or theta.",
		"## Guard replay (test, truth not A)",
		"shared 300 ms threshold",
		"| 0 | dataset preparation for training | N/A | N/A | no aplica: sin entrenamiento | N/A |",
		"## Safety regression suite",
		"sha256:3444d1085fd702ac7f61d22e4093ba8cbb31894a17ba046df2c12ade6ed0d805",
		`| candidate | analyze\_agent\_state\.v2 | 24 |`,
	} {
		require.Contains(t, first, text)
	}
	require.NotContains(t, first, "01JB", "no window ids")
	require.NotContains(t, first, "synthetic-101", "no session ids")
}

func TestSafetyCorpusDigestsAndWindowsAreFixed(t *testing.T) {
	t.Parallel()
	f, err := os.Open("testdata/safety/SHA256SUMS")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	sc := bufio.NewScanner(f)
	checked := 0
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		require.Len(t, fields, 2)
		raw, err := os.ReadFile(filepath.Join("..", "..", fields[1]))
		require.NoError(t, err)
		sum := sha256.Sum256(raw)
		require.Equal(t, fields[0], hex.EncodeToString(sum[:]), fields[1])
		checked++
	}
	require.NoError(t, sc.Err())
	require.Equal(t, 25, checked)

	m := loadSafetyManifest(t)
	require.Equal(t, promptSafetyDatasetDigest, m.DatasetDigest)
	require.Equal(t, protocol.Digest("sha256:3444d1085fd702ac7f61d22e4093ba8cbb31894a17ba046df2c12ade6ed0d805"), m.DatasetDigest)
	require.Len(t, m.Entries, 24)
	harnesses := map[protocol.Harness]int{}
	categories := map[string]int{}
	for _, e := range m.Entries {
		raw, err := os.ReadFile(filepath.Join("testdata/safety/windows", string(e.WindowID)+".json"))
		require.NoError(t, err)
		w, err := dataset.ParseWindow(bytes.NewReader(raw))
		require.NoError(t, err)
		d, err := dataset.WindowDigest(w)
		require.NoError(t, err)
		require.Equal(t, e.WindowDigest, d)
		require.Equal(t, protocol.SourceQualitySynthetic, w.Provenance.SourceQuality)
		harnesses[e.Harness]++
		categories[safetyCategory(e.Label)]++
	}
	for _, harness := range []protocol.Harness{
		protocol.HarnessClaudeCode, protocol.HarnessCodex, protocol.HarnessMuseCode,
		protocol.HarnessOpencode, protocol.HarnessAntigravity, protocol.HarnessGeneric,
	} {
		require.Equal(t, 4, harnesses[harness], harness)
	}
	for _, category := range []string{"adversarial-ready-look", "permission-menu", "question", "blocking-error"} {
		require.Equal(t, 6, categories[category], category)
	}
}
