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
)

// DefaultLatencyBudget is the p95 budget of criterion 4.
const DefaultLatencyBudget = 1500 * time.Millisecond

// Criterion is one gate line; Detail holds numbers only.
type Criterion struct {
	ID      int
	Name    string
	Verdict Verdict
	Detail  string
}

// Gate is the LoRA gate: every criterion, and the overall verdict, PASS
// only when every criterion passes.
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

func gate(base, lora *ProfileReport, preds []Prediction, mc ManifestCheck, budget time.Duration) Gate {
	return gateOf([]Criterion{
		manifestCriterion(preds, mc),
		improvement(base, lora),
		{ID: 2, Name: "false SAFE after guards does not rise", Verdict: VerdictPending, Detail: "guards land in 5.8"},
		recallCriterion(base, lora),
		latencyCriterion(lora, budget),
		{ID: 5, Name: "safety regression suite", Verdict: VerdictPending, Detail: "suite not wired yet"},
	})
}

// gateOf is PASS only when every criterion passes: any FAIL fails it,
// and otherwise any PENDING leaves it pending.
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
// stays PENDING until manifests record the preparation gate.
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
		if !ok || e.Split != p.Split || e.Label != labels()[p.Truth] || e.Split == dataset.SplitTrain {
			c.Verdict, c.Detail = VerdictFail, "predictions do not match the manifest"
			return c
		}
	}
	if evaluated != len(preds) {
		c.Verdict, c.Detail = VerdictFail, fmt.Sprintf("%d validation and test windows in the manifest, %d predicted", evaluated, len(preds))
		return c
	}
	// The preparation gate (PRD §12.10.1) is dataset build's to compute
	// and write into the manifest (10.6, 10.7); until a manifest carries
	// it as passed, this criterion cannot pass.
	c.Verdict, c.Detail = VerdictPending, fmt.Sprintf("%d windows match; the manifest carries no preparation gate result yet", len(preds))
	return c
}

// improvement is criterion 1 on test: macro-F1 up, false IDLE down (or
// both zero), calibrated ECE no worse.
func improvement(base, lora *ProfileReport) Criterion {
	c := Criterion{ID: 1, Name: "macro-F1 and false IDLE improve, calibrated ECE no worse"}
	if base == nil || lora == nil || base.Test.All.N == 0 {
		c.Verdict, c.Detail = VerdictPending, "needs both profiles on test"
		return c
	}
	b, l := base.Test.All, lora.Test.All
	bi, bok := b.FalseIdle.Value()
	li, lok := l.FalseIdle.Value()
	if !bok || !lok {
		c.Verdict, c.Detail = VerdictPending, "no test window outside A"
		return c
	}
	c.Detail = fmt.Sprintf("macro-F1 %s -> %s, false IDLE %s -> %s, ECE %s -> %s",
		f4(b.MacroF1), f4(l.MacroF1), f4(bi), f4(li), f4(b.ECECal), f4(l.ECECal))
	idleBetter := li < bi || li == 0 && bi == 0
	c.Verdict = verdict(l.MacroF1 > b.MacroF1 && idleBetter && l.ECECal <= b.ECECal)
	return c
}

// recallCriterion is criterion 3 on test: D and E recall not lower.
func recallCriterion(base, lora *ProfileReport) Criterion {
	c := Criterion{ID: 3, Name: "recall of D and of E not lower"}
	if base == nil || lora == nil {
		c.Verdict, c.Detail = VerdictPending, "needs both profiles on test"
		return c
	}
	b, l := base.Test.All, lora.Test.All
	if b.Support[idxD] == 0 || b.Support[idxE] == 0 {
		c.Verdict, c.Detail = VerdictPending, "no test window of D or of E"
		return c
	}
	c.Detail = fmt.Sprintf("D %s -> %s, E %s -> %s", f4(b.Recall[idxD]), f4(l.Recall[idxD]), f4(b.Recall[idxE]), f4(l.Recall[idxE]))
	c.Verdict = verdict(l.Recall[idxD] >= b.Recall[idxD] && l.Recall[idxE] >= b.Recall[idxE])
	return c
}

// latencyCriterion is criterion 4: the LoRA's test p95 within budget.
func latencyCriterion(lora *ProfileReport, budget time.Duration) Criterion {
	c := Criterion{ID: 4, Name: "latency p95 within budget"}
	if lora == nil || lora.Test.All.N == 0 {
		c.Verdict, c.Detail = VerdictPending, "needs analyst-lora on test"
		return c
	}
	ms := float64(budget) / float64(time.Millisecond)
	c.Detail = fmt.Sprintf("p95 %s ms, budget %s ms", f1(lora.Test.All.LatencyP95), f1(ms))
	c.Verdict = verdict(lora.Test.All.LatencyP95 <= ms)
	return c
}

func verdict(ok bool) Verdict {
	if ok {
		return VerdictPass
	}
	return VerdictFail
}
