package eval

import (
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/dataset"
)

// Verdict is a gate criterion's outcome.
type Verdict string

// Verdict values.
const (
	VerdictPass    Verdict = "PASS"
	VerdictFail    Verdict = "FAIL"
	VerdictPending Verdict = "PENDING"
	VerdictNA      Verdict = "N/A"
)

// DefaultLatencyBudget is the p95 budget of criterion 4.
const DefaultLatencyBudget = 1500 * time.Millisecond

const (
	roleBaseline            = "baseline"
	roleCandidate           = "candidate"
	needsBothProfilesOnTest = "needs both profiles on test"
)

// Criterion is one gate line; Detail is a stable, auditable explanation.
type Criterion struct {
	ID      int
	Name    string
	Verdict Verdict
	Detail  string
}

// Gate is one model-change gate. Every applicable criterion, and the overall
// verdict, PASS only when every applicable criterion passes.
type Gate struct {
	Criteria []Criterion
	Verdict  Verdict
}

// ManifestCheck is criterion 0's input: the dataset manifest the
// predictions were drawn from, if any, and whether it parsed.
type ManifestCheck struct {
	Given    bool
	Manifest dataset.Manifest
	// Err is set when the manifest failed its own validation.
	Err error
}

func gate(base, lora *ProfileReport, preds []Prediction, mc ManifestCheck, safety SafetySuiteReport, budget time.Duration, mode EvaluationMode, blindCoverage *dataset.Readiness) Gate {
	criteria := []Criterion{
		manifestCriterion(preds, mc),
		improvementOn(base, lora, mode),
		guardCriterionOn(base, lora, mode),
		recallCriterionOn(base, lora, mode),
		latencyCriterionOn(lora, budget, mode),
		safetyCriterion(safety),
	}
	if mode == EvaluationFinal && blindCoverage != nil && !blindCoverage.Passed {
		detail := fmt.Sprintf("blind test coverage is insufficient: %d readiness faults", len(blindCoverage.Faults))
		if len(blindCoverage.Faults) > 0 {
			detail = "blind test coverage is insufficient: " + blindCoverage.Faults[0].Line()
		}
		for i := range criteria {
			if criteria[i].ID >= 1 && criteria[i].ID <= 4 {
				criteria[i].Verdict, criteria[i].Detail = VerdictPending, detail
			}
		}
	}
	return gateOf(criteria)
}

// gateOf is PASS when every applicable criterion passes: N/A is ignored,
// any FAIL fails it, and otherwise any PENDING leaves it pending.
func gateOf(cs []Criterion) Gate {
	g := Gate{Criteria: cs, Verdict: VerdictPass}
	for _, c := range g.Criteria {
		switch c.Verdict {
		case VerdictFail:
			g.Verdict = VerdictFail
		case VerdictPending:
			if g.Verdict != VerdictFail {
				g.Verdict = VerdictPending
			}
		default:
		}
	}
	return g
}

// manifestCriterion is criterion 0. It fails when the manifest is invalid
// or does not match the predictions (ids, split, label); a matching one
// gives the verdict of its preparation gate, PENDING when it has none.
func manifestCriterion(preds []Prediction, mc ManifestCheck) Criterion {
	c := Criterion{ID: 0, Name: "dataset manifest passed its preparation gate"}
	switch {
	case !mc.Given:
		c.Verdict, c.Detail = VerdictPending, "no manifest given"
		return c
	case mc.Err != nil:
		c.Verdict, c.Detail = VerdictFail, "manifest invalid"
		return c
	}
	entries := map[string]dataset.ManifestEntry{}
	evaluated := 0
	for _, e := range mc.Manifest.Entries {
		entries[string(e.WindowID)] = e
		if e.Split != dataset.SplitTrain {
			evaluated++
		}
	}
	for _, p := range preds {
		e, ok := entries[string(p.WindowID)]
		if !ok || e.Split != p.Split || e.Label != labels()[p.Truth] || e.Split == dataset.SplitTrain ||
			e.Harness != p.Harness || e.HarnessVersion != p.HarnessVersion {
			c.Verdict, c.Detail = VerdictFail, "predictions do not match the manifest"
			return c
		}
	}
	if evaluated != len(preds) {
		c.Verdict, c.Detail = VerdictFail, fmt.Sprintf("%d validation and test windows in the manifest, %d predicted", evaluated, len(preds))
		return c
	}
	// The preparation gate (PRD §12.10.1) is computed when the manifest
	// is built and recomputed when it is parsed: eval only reads it.
	switch r := mc.Manifest.Readiness; {
	case r == nil:
		c.Verdict, c.Detail = VerdictPending, fmt.Sprintf("%d windows match; the manifest carries no preparation gate result", len(preds))
	case r.Passed:
		c.Verdict, c.Detail = VerdictPass, fmt.Sprintf("%d windows match; preparation gate passed", len(preds))
	default:
		c.Verdict, c.Detail = VerdictFail, fmt.Sprintf("%d windows match; preparation gate failed with %d faults", len(preds), len(r.Faults))
	}
	return c
}

// improvement is criterion 1 on test: macro-F1 up, false IDLE down (or both
// zero), calibrated ECE no worse. Prompt comparison keeps this legacy path.
func improvement(base, candidate *ProfileReport) Criterion {
	return improvementOn(base, candidate, EvaluationLegacy)
}

func improvementOn(base, lora *ProfileReport, mode EvaluationMode) Criterion {
	c := Criterion{ID: 1, Name: "macro-F1 and false IDLE improve, calibrated ECE no worse"}
	if base == nil || lora == nil || gateSplit(base, mode).All.N == 0 {
		c.Verdict, c.Detail = VerdictPending, needsBothProfiles(mode)
		return c
	}
	b, l := gateSplit(base, mode).All, gateSplit(lora, mode).All
	bi, bok := b.FalseIdle.Value()
	li, lok := l.FalseIdle.Value()
	if !bok || !lok {
		c.Verdict, c.Detail = VerdictPending, "no "+gateDenominator(mode)+" window outside A"
		return c
	}
	c.Detail = fmt.Sprintf("macro-F1 %s -> %s, false IDLE %s -> %s, ECE %s -> %s",
		f4(b.MacroF1), f4(l.MacroF1), f4(bi), f4(li), f4(b.ECECal), f4(l.ECECal))
	idleBetter := li < bi || li == 0 && bi == 0
	c.Verdict = verdict(l.MacroF1 > b.MacroF1 && idleBetter && l.ECECal <= b.ECECal)
	return c
}

// recallCriterion is criterion 3 on test. Prompt comparison keeps this legacy
// path.
func recallCriterion(base, candidate *ProfileReport) Criterion {
	return recallCriterionOn(base, candidate, EvaluationLegacy)
}

func recallCriterionOn(base, lora *ProfileReport, mode EvaluationMode) Criterion {
	c := Criterion{ID: 3, Name: "recall of D and of E not lower"}
	if base == nil || lora == nil {
		c.Verdict, c.Detail = VerdictPending, needsBothProfiles(mode)
		return c
	}
	b, l := gateSplit(base, mode).All, gateSplit(lora, mode).All
	if b.Support[idxD] == 0 || b.Support[idxE] == 0 {
		c.Verdict, c.Detail = VerdictPending, "no "+gateDenominator(mode)+" window of D or of E"
		return c
	}
	c.Detail = fmt.Sprintf("D %s -> %s, E %s -> %s", f4(b.Recall[idxD]), f4(l.Recall[idxD]), f4(b.Recall[idxE]), f4(l.Recall[idxE]))
	c.Verdict = verdict(l.Recall[idxD] >= b.Recall[idxD] && l.Recall[idxE] >= b.Recall[idxE])
	return c
}

func latencyCriterionOn(lora *ProfileReport, budget time.Duration, mode EvaluationMode) Criterion {
	c := Criterion{ID: 4, Name: "latency p95 within budget"}
	if lora == nil || gateSplit(lora, mode).All.N == 0 {
		c.Verdict, c.Detail = VerdictPending, "needs analyst-lora on "+gateDenominator(mode)
		return c
	}
	ms := float64(budget) / float64(time.Millisecond)
	p95 := gateSplit(lora, mode).All.LatencyP95
	c.Detail = fmt.Sprintf("p95 %s ms, budget %s ms", f1(p95), f1(ms))
	c.Verdict = verdict(p95 <= ms)
	return c
}

func guardCriterionOn(base, lora *ProfileReport, mode EvaluationMode) Criterion {
	c := guardCriterion(base, lora)
	if mode != EvaluationFinal {
		return c
	}
	switch c.Detail {
	case needsBothProfilesOnTest:
		c.Detail = needsBothProfiles(mode)
	case "no test window outside A":
		c.Detail = "no " + gateDenominator(mode) + " window outside A"
	}
	return c
}

func gateSplit(p *ProfileReport, _ EvaluationMode) SplitReport {
	return p.Test
}

func gateDenominator(mode EvaluationMode) string {
	if mode == EvaluationFinal {
		return "blind test"
	}
	return testName
}

func needsBothProfiles(mode EvaluationMode) string {
	if mode == EvaluationFinal {
		return "needs both profiles on blind test"
	}
	return needsBothProfilesOnTest
}

func verdict(ok bool) Verdict {
	if ok {
		return VerdictPass
	}
	return VerdictFail
}
