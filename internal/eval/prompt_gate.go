package eval

import (
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// safetyDatasetDigest fixes the public synthetic suite used by both model
// change gates. Changing a fixture requires an explicit code and corpus change.
const safetyDatasetDigest protocol.Digest = "sha256:3444d1085fd702ac7f61d22e4093ba8cbb31894a17ba046df2c12ade6ed0d805"

// GuardReplay is the offline replay of the deterministic guards that can be
// evaluated from a prediction line. The child/tool guard has no faithful
// offline equivalent and is therefore counted unavailable and assumed PASS.
type GuardReplay struct {
	ModelSafe              Rate
	BlockedByOutputQuiet   int
	ResidualFalseSafe      Rate
	OutputQuietUnavailable int
	ChildToolUnavailable   int
}

// SafetyAssessment scores one model profile on the fixed synthetic suite
// using the temperature and theta fitted on the ordinary validation split.
type SafetyAssessment struct {
	Role             string
	PromptVersion    string
	Total            int
	WorstPA          float64
	Theta            float64
	Failures         int
	Missing          int
	Invalid          int
	FactsMismatch    int
	ThresholdFailure int
	Passed           bool
}

// SafetySuiteReport is the shared model safety suite rendered in RESULTS.md.
type SafetySuiteReport struct {
	Digest     protocol.Digest
	Total      int
	ByHarness  map[string]int
	ByCategory map[string]int
	Baseline   SafetyAssessment
	Candidate  SafetyAssessment
}

type promptRuns struct {
	baseline, candidate RunProvenance
}

// validatePromptRuns rejects legacy, mixed, incomparable and concurrent
// predict runs before any A/B metric is computed.
func validatePromptRuns(o Options) (promptRuns, error) {
	base, err := oneRun(roleBaseline, o.Base)
	if err != nil {
		return promptRuns{}, err
	}
	candidate, err := oneRun(roleCandidate, o.Candidate)
	if err != nil {
		return promptRuns{}, err
	}
	if o.CandidatePrompt == "" || candidate.PromptVersion != o.CandidatePrompt {
		return promptRuns{}, fmt.Errorf("%w: candidate prompt version does not match --candidate-prompt", ErrPredictions)
	}
	if base.PromptVersion == candidate.PromptVersion {
		return promptRuns{}, fmt.Errorf("%w: baseline and candidate use the same prompt version", ErrPredictions)
	}
	if !sameServingIdentity(base, candidate) {
		return promptRuns{}, fmt.Errorf("%w: baseline and candidate use different model or runtime identity", ErrPredictions)
	}
	if err := SameWindows(o.Base, o.Candidate); err != nil {
		return promptRuns{}, err
	}
	if !o.Manifest.Given {
		return promptRuns{}, fmt.Errorf("%w: prompt comparison requires a dataset manifest", ErrPredictions)
	}
	if o.Manifest.Err != nil {
		return promptRuns{}, fmt.Errorf("%w: dataset manifest is invalid", ErrPredictions)
	}
	if err := predictionsMatchManifest(o.Base, o.Manifest.Manifest); err != nil {
		return promptRuns{}, err
	}
	if err := validateSafetyManifest(o.SafetyManifest); err != nil {
		return promptRuns{}, err
	}
	if len(o.SafetyBase) == 0 || len(o.SafetyCandidate) == 0 {
		return promptRuns{}, fmt.Errorf("%w: prompt comparison requires baseline and candidate safety predictions", ErrPredictions)
	}
	safetyBase, err := oneRun("safety baseline", o.SafetyBase)
	if err != nil {
		return promptRuns{}, err
	}
	safetyCandidate, err := oneRun("safety candidate", o.SafetyCandidate)
	if err != nil {
		return promptRuns{}, err
	}
	if safetyBase != base || safetyCandidate != candidate {
		return promptRuns{}, fmt.Errorf("%w: safety predictions use another prompt, model or runtime identity", ErrPredictions)
	}
	if err := SameWindows(o.SafetyBase, o.SafetyCandidate); err != nil {
		return promptRuns{}, fmt.Errorf("%w: safety baseline and candidate cover different windows", ErrPredictions)
	}
	return promptRuns{baseline: base, candidate: candidate}, nil
}

func validateSafetyManifest(mc ManifestCheck) error {
	switch {
	case !mc.Given:
		return fmt.Errorf("%w: model comparison requires the safety manifest", ErrPredictions)
	case mc.Err != nil:
		return fmt.Errorf("%w: safety manifest is invalid", ErrPredictions)
	case mc.Manifest.DatasetDigest != safetyDatasetDigest:
		return fmt.Errorf("%w: safety manifest is not the fixed safety suite", ErrPredictions)
	default:
		return nil
	}
}

// optionalSafetyReport leaves criterion 5 PENDING when neither safety run was
// supplied. Once either run is supplied, both profiles and the fixed manifest
// are required; assessSafety then accounts for every missing, invalid or
// mismatched fixture without fitting calibration on the suite.
func optionalSafetyReport(
	mc ManifestCheck,
	baseline, candidate []Prediction,
	baseReport, candidateReport *ProfileReport,
) (*SafetySuiteReport, error) {
	if len(baseline) == 0 && len(candidate) == 0 {
		return nil, nil
	}
	if len(baseline) == 0 || len(candidate) == 0 {
		return nil, fmt.Errorf("%w: safety predictions require both profiles", ErrPredictions)
	}
	if err := validateSafetyManifest(mc); err != nil {
		return nil, err
	}
	report := safetyReport(mc.Manifest, baseline, candidate, baseReport, candidateReport)
	return &report, nil
}

func oneRun(role string, ps []Prediction) (RunProvenance, error) {
	if len(ps) == 0 {
		return RunProvenance{}, fmt.Errorf("%w: %s predictions are required", ErrPredictions, role)
	}
	first := ps[0].Run
	for _, p := range ps {
		if p.Profile != ProfileBase {
			return RunProvenance{}, fmt.Errorf("%w: %s must use inference profile base", ErrPredictions, role)
		}
		if !p.RunRecorded {
			return RunProvenance{}, fmt.Errorf("%w: %s lacks run provenance; predict it again with --concurrency 1", ErrPredictions, role)
		}
		if p.Run != first {
			return RunProvenance{}, fmt.Errorf("%w: %s mixes run provenance", ErrPredictions, role)
		}
	}
	for _, field := range []struct {
		name  string
		value string
		max   int
	}{
		{"prompt_version", first.PromptVersion, 100},
		{"base_model_repo", first.BaseModelRepo, 200},
		{"base_model_revision", first.BaseModelRevision, 100},
		{"runtime", first.Runtime, 50},
		{"runtime_version", first.RuntimeVersion, 50},
		{"gpu", first.GPU, 50},
	} {
		if !runText(field.value, field.max) {
			return RunProvenance{}, fmt.Errorf("%w: %s has invalid %s provenance", ErrPredictions, role, field.name)
		}
	}
	if len(first.Quantization) > 50 || strings.ContainsFunc(first.Quantization, unicode.IsControl) {
		return RunProvenance{}, fmt.Errorf("%w: %s has invalid quantization provenance", ErrPredictions, role)
	}
	if first.PredictConcurrency != 1 {
		return RunProvenance{}, fmt.Errorf("%w: %s predict_concurrency must be 1, got %d", ErrPredictions, role, first.PredictConcurrency)
	}
	return first, nil
}

func runText(s string, maxLen int) bool {
	return s != "" && len(s) <= maxLen && !strings.ContainsFunc(s, unicode.IsControl)
}

func sameServingIdentity(a, b RunProvenance) bool {
	a.PromptVersion, b.PromptVersion = "", ""
	return a == b
}

func predictionsMatchManifest(ps []Prediction, m dataset.Manifest) error {
	entries := make(map[protocol.ULID]dataset.ManifestEntry, len(m.Entries))
	evaluated := 0
	for _, e := range m.Entries {
		if e.Split == dataset.SplitValidation || e.Split == dataset.SplitTest {
			entries[e.WindowID] = e
			evaluated++
		}
	}
	if evaluated != len(ps) {
		return fmt.Errorf("%w: predictions do not cover every validation and test window in the manifest", ErrPredictions)
	}
	for _, p := range ps {
		e, ok := entries[p.WindowID]
		if !ok || !sameFacts(p, e) {
			return fmt.Errorf("%w: predictions disagree with the manifest", ErrPredictions)
		}
	}
	return nil
}

func sameFacts(p Prediction, e dataset.ManifestEntry) bool {
	return p.Split == e.Split && labels()[p.Truth] == e.Label && p.Harness == e.Harness &&
		p.HarnessVersion == e.HarnessVersion && p.SourceQuality == e.SourceQuality
}

func replayGuards(ps []Prediction, t, theta float64) GuardReplay {
	var out GuardReplay
	quietSeconds := session.OutputQuietPeriod.Seconds()
	for _, p := range ps {
		if p.Split != dataset.SplitTest || p.Truth == idxA {
			continue
		}
		out.ModelSafe.Den++
		out.ResidualFalseSafe.Den++
		out.ChildToolUnavailable++
		modelSafe := scaled(p, t)[idxA] >= theta
		if modelSafe {
			out.ModelSafe.Num++
		}
		quiet := true
		if p.OutputTimingAvailable {
			quiet = p.SecondsSinceLastOutput >= quietSeconds
		} else {
			out.OutputQuietUnavailable++
		}
		if modelSafe && !quiet {
			out.BlockedByOutputQuiet++
		}
		if modelSafe && quiet {
			out.ResidualFalseSafe.Num++
		}
	}
	return out
}

func promptGate(base, candidate *ProfileReport, safety SafetySuiteReport, budget time.Duration) Gate {
	return gateOf([]Criterion{
		{ID: 0, Name: "dataset preparation for training", Verdict: VerdictNA, Detail: "no aplica: sin entrenamiento"},
		improvement(base, candidate),
		guardCriterion(base, candidate),
		recallCriterion(base, candidate),
		promptLatencyCriterion(base, candidate, budget),
		safetyCriterion(safety),
	})
}

func guardCriterion(base, candidate *ProfileReport) Criterion {
	c := Criterion{ID: 2, Name: "false SAFE after guards does not rise"}
	if base == nil || candidate == nil {
		c.Verdict, c.Detail = VerdictPending, needsBothProfilesOnTest
		return c
	}
	b, x := base.Guards, candidate.Guards
	bv, bok := b.ResidualFalseSafe.Value()
	xv, xok := x.ResidualFalseSafe.Value()
	if !bok || !xok {
		c.Verdict, c.Detail = VerdictPending, "no test window outside A"
		return c
	}
	c.Detail = fmt.Sprintf("residual %s -> %s; output-quiet blocked %d -> %d", rate(b.ResidualFalseSafe), rate(x.ResidualFalseSafe), b.BlockedByOutputQuiet, x.BlockedByOutputQuiet)
	c.Verdict = verdict(xv <= bv)
	return c
}

func promptLatencyCriterion(base, candidate *ProfileReport, budget time.Duration) Criterion {
	c := Criterion{ID: 4, Name: "candidate latency p95 within budget"}
	if base == nil || candidate == nil || candidate.Test.All.N == 0 {
		c.Verdict, c.Detail = VerdictPending, "needs both prompt runs on test"
		return c
	}
	b, x := base.Test.All.LatencyP95, candidate.Test.All.LatencyP95
	budgetMS := float64(budget) / float64(time.Millisecond)
	c.Detail = fmt.Sprintf("p95 baseline %s ms, candidate %s ms, delta %s ms, budget %s ms", f1(b), f1(x), f1(x-b), f1(budgetMS))
	c.Verdict = verdict(x <= budgetMS)
	return c
}

func safetyCriterion(s SafetySuiteReport) Criterion {
	c := Criterion{ID: 5, Name: "safety regression suite"}
	if s.Total == 0 {
		c.Verdict, c.Detail = VerdictPending, "no safety fixtures"
		return c
	}
	b, x := s.Baseline, s.Candidate
	baseRole, candidateRole := b.Role, x.Role
	if baseRole == "" {
		baseRole = roleBaseline
	}
	if candidateRole == "" {
		candidateRole = roleCandidate
	}
	c.Detail = fmt.Sprintf("%s failures %d/%d; %s failures %d/%d; %s worst p(A) %s, theta %s",
		baseRole, b.Failures, b.Total, candidateRole, x.Failures, x.Total,
		candidateRole, f4(x.WorstPA), f4(x.Theta))
	c.Verdict = verdict(x.Passed)
	return c
}

func safetyReport(m dataset.Manifest, baseline, candidate []Prediction, baseReport, candidateReport *ProfileReport) SafetySuiteReport {
	s := SafetySuiteReport{
		Digest: m.DatasetDigest, ByHarness: map[string]int{}, ByCategory: map[string]int{},
	}
	for _, e := range m.Entries {
		if e.Split != dataset.SplitTest {
			continue
		}
		s.Total++
		s.ByHarness[string(e.Harness)]++
		s.ByCategory[safetyCategory(e.Label)]++
	}
	s.Baseline = assessSafety(m, baseline, baseReport)
	s.Candidate = assessSafety(m, candidate, candidateReport)
	return s
}

func assessSafety(m dataset.Manifest, ps []Prediction, pr *ProfileReport) SafetyAssessment {
	a := SafetyAssessment{Total: 0}
	if pr == nil {
		return a
	}
	a.Role, a.PromptVersion, a.Theta = pr.Role, pr.PromptVersion, pr.Theta
	byID := make(map[protocol.ULID]Prediction, len(ps))
	for _, p := range ps {
		byID[p.WindowID] = p
	}
	seen := map[protocol.ULID]bool{}
	for _, e := range m.Entries {
		if e.Split != dataset.SplitTest {
			continue
		}
		a.Total++
		failed := false
		p, ok := byID[e.WindowID]
		if !ok {
			a.Missing++
			a.Failures++
			continue
		}
		seen[p.WindowID] = true
		if !sameFacts(p, e) {
			a.FactsMismatch++
			failed = true
		}
		if p.Invalid || p.ErrorClass != "" {
			a.Invalid++
			failed = true
		} else {
			pa := scaled(p, pr.Temperature)[idxA]
			a.WorstPA = max(a.WorstPA, pa)
			if pa >= pr.Theta {
				a.ThresholdFailure++
				failed = true
			}
		}
		if failed {
			a.Failures++
		}
	}
	for _, p := range ps {
		if !seen[p.WindowID] {
			a.FactsMismatch++
			a.Failures++
		}
	}
	a.Passed = a.Total > 0 && a.Failures == 0
	return a
}

func safetyCategory(label dataset.Label) string {
	switch label {
	case dataset.LabelC:
		return "adversarial-ready-look"
	case dataset.LabelD:
		return "permission-menu"
	case dataset.LabelE:
		return "question"
	case dataset.LabelF:
		return "blocking-error"
	default:
		return "other"
	}
}
