package dataset

import (
	"errors"
	"fmt"
	"slices"

	"github.com/smellak/gifford/internal/protocol"
)

// ErrBadThresholds means readiness thresholds are out of range.
var ErrBadThresholds = errors.New("dataset: invalid readiness thresholds")

// ReadinessThresholds are the preparation gate's limits (PRD §12.10.1).
// config.yaml can override each under ml.readiness.
type ReadinessThresholds struct {
	TrainMin      int64 `json:"train_min"`
	ValidationMin int64 `json:"validation_min"`
	TestMin       int64 `json:"test_min"`
	// LabelTrainMin and LabelTestMin hold for A, B, C and F.
	LabelTrainMin int64 `json:"label_train_min"`
	LabelTestMin  int64 `json:"label_test_min"`
	// RareLabel* hold for D and E, which must also come from at least
	// RareLabelHarnessesMin harnesses in train.
	RareLabelTrainMin     int64 `json:"rare_label_train_min"`
	RareLabelTestMin      int64 `json:"rare_label_test_min"`
	RareLabelHarnessesMin int64 `json:"rare_label_harnesses_min"`
	// UnknownTrainMin holds for G.
	UnknownTrainMin int64 `json:"unknown_train_min"`
	// LabelShareMinPercent: no label below this share of train.
	LabelShareMinPercent int64 `json:"label_share_min_percent"`
	// HarnessesMin harnesses need HarnessTrainMin train windows each.
	HarnessesMin    int64 `json:"harnesses_min"`
	HarnessTrainMin int64 `json:"harness_train_min"`
	// SyntheticMaxPercent bounds the synthetic share of each label in
	// train.
	SyntheticMaxPercent int64 `json:"synthetic_max_percent"`
}

// DefaultReadinessThresholds are the PRD values.
func DefaultReadinessThresholds() ReadinessThresholds {
	return ReadinessThresholds{
		TrainMin: 1500, ValidationMin: 300, TestMin: 300,
		LabelTrainMin: 100, LabelTestMin: 20,
		RareLabelTrainMin: 150, RareLabelTestMin: 30, RareLabelHarnessesMin: 3,
		UnknownTrainMin:      50,
		LabelShareMinPercent: 3,
		HarnessesMin:         3, HarnessTrainMin: 200,
		SyntheticMaxPercent: 30,
	}
}

// Validate refuses negative counts and percentages above 100.
func (t ReadinessThresholds) Validate() error {
	for _, v := range []int64{
		t.TrainMin, t.ValidationMin, t.TestMin, t.LabelTrainMin, t.LabelTestMin,
		t.RareLabelTrainMin, t.RareLabelTestMin, t.RareLabelHarnessesMin, t.UnknownTrainMin,
		t.HarnessesMin, t.HarnessTrainMin,
	} {
		if v < 0 {
			return fmt.Errorf("%w: a count is negative", ErrBadThresholds)
		}
	}
	for _, v := range []int64{t.LabelShareMinPercent, t.SyntheticMaxPercent} {
		if v < 0 || v > 100 {
			return fmt.Errorf("%w: a percentage is outside 0-100", ErrBadThresholds)
		}
	}
	return nil
}

// Readiness rules, as Fault.Rule names them.
const (
	RuleSplitSize      = "split_size"
	RuleLabelTrain     = "label_train"
	RuleLabelTest      = "label_test"
	RuleLabelHarnesses = "label_harnesses"
	RuleLabelShare     = "label_share"
	RuleHarnesses      = "harnesses"
	RuleHarnessInTest  = "harness_in_test"
	RuleSynthetic      = "synthetic_label"
	RuleSyntheticShare = "synthetic_share"
	RuleSyntheticTest  = "synthetic_in_test"
	RuleTestSource     = "test_label_source"
	RuleGroupLeak      = "group_leak"
)

// Fault is one rule a dataset misses: what it has and what the rule asks.
// Subject names the label, harness or split concerned; numbers only.
type Fault struct {
	Rule    string `json:"rule"`
	Subject string `json:"subject"`
	Split   Split  `json:"split"`
	Have    int64  `json:"have"`
	Want    int64  `json:"want"`
}

// Line renders the fault for gifford dataset inspect: numbers and names
// only.
func (f Fault) Line() string {
	switch f.Rule {
	case RuleSplitSize:
		return fmt.Sprintf("%s: %d/%d windows", f.Split, f.Have, f.Want)
	case RuleLabelTrain, RuleLabelTest:
		return fmt.Sprintf("%s: %d/%d in %s", f.Subject, f.Have, f.Want, f.Split)
	case RuleLabelHarnesses:
		return fmt.Sprintf("%s: %d harnesses of %d in %s", f.Subject, f.Have, f.Want, f.Split)
	case RuleLabelShare:
		return fmt.Sprintf("%s: %d%% of %s, at least %d%%", f.Subject, f.Have, f.Split, f.Want)
	case RuleHarnesses:
		return fmt.Sprintf("harnesses: %d with enough %s windows, %d needed", f.Have, f.Split, f.Want)
	case RuleHarnessInTest:
		return fmt.Sprintf("%s: %d in test, at least %d", f.Subject, f.Have, f.Want)
	case RuleSynthetic:
		return fmt.Sprintf("%s: %d synthetic in %s, only D, E and F may have them", f.Subject, f.Have, f.Split)
	case RuleSyntheticShare:
		return fmt.Sprintf("%s: %d%% synthetic in %s, at most %d%%", f.Subject, f.Have, f.Split, f.Want)
	case RuleSyntheticTest:
		return fmt.Sprintf("test: %d synthetic, %d allowed", f.Have, f.Want)
	case RuleTestSource:
		return fmt.Sprintf("test: %d %s-labeled, %d allowed", f.Have, f.Subject, f.Want)
	case RuleGroupLeak:
		return fmt.Sprintf("groups: %d in two splits, %d allowed", f.Have, f.Want)
	default:
		return fmt.Sprintf("%s: %d/%d", f.Rule, f.Have, f.Want)
	}
}

// Readiness is the preparation gate's result, kept in the manifest with
// the thresholds it ran under; Validate recomputes it, so it cannot
// claim a pass the entries do not earn.
type Readiness struct {
	Thresholds ReadinessThresholds `json:"thresholds"`
	Passed     bool                `json:"passed"`
	Faults     []Fault             `json:"faults"`
}

// synthetic reports an entry made up rather than captured.
func (e ManifestEntry) synthetic() bool {
	return e.LabelSource == LabelSourceSynthetic || e.SourceQuality == protocol.SourceQualitySynthetic
}

// CheckReadiness runs the preparation gate over a manifest's entries and
// lists every fault, in a fixed order.
func CheckReadiness(entries []ManifestEntry, t ReadinessThresholds) Readiness {
	r := Readiness{Thresholds: t, Faults: []Fault{}}
	add := func(f Fault) { r.Faults = append(r.Faults, f) }
	type key struct {
		split Split
		label Label
	}
	splits := map[Split]int64{}
	byLabel := map[key]int64{}
	synth := map[key]int64{}
	harnessTrain := map[protocol.Harness]int64{}
	harnessTest := map[protocol.Harness]int64{}
	labelHarnesses := map[Label]map[protocol.Harness]bool{}
	testSources := map[LabelSource]int64{}
	groups := map[protocol.Digest]Split{}
	leaked := map[protocol.Digest]bool{}
	var syntheticTest int64
	for _, e := range entries {
		splits[e.Split]++
		byLabel[key{e.Split, e.Label}]++
		if e.synthetic() {
			synth[key{e.Split, e.Label}]++
			if e.Split == SplitTest {
				syntheticTest++
			}
		}
		switch e.Split {
		case SplitTrain:
			harnessTrain[e.Harness]++
			if labelHarnesses[e.Label] == nil {
				labelHarnesses[e.Label] = map[protocol.Harness]bool{}
			}
			labelHarnesses[e.Label][e.Harness] = true
		case SplitTest:
			harnessTest[e.Harness]++
			if !e.LabelSource.Real() {
				testSources[e.LabelSource]++
			}
		default:
		}
		if prior, seen := groups[e.GroupDigest]; seen && prior != e.Split {
			leaked[e.GroupDigest] = true
		}
		groups[e.GroupDigest] = e.Split
	}

	for _, s := range []struct {
		split Split
		min   int64
	}{{SplitTrain, t.TrainMin}, {SplitValidation, t.ValidationMin}, {SplitTest, t.TestMin}} {
		if splits[s.split] < s.min {
			add(Fault{Rule: RuleSplitSize, Subject: string(s.split), Split: s.split, Have: splits[s.split], Want: s.min})
		}
	}
	train := splits[SplitTrain]
	for _, l := range []Label{LabelA, LabelB, LabelC, LabelD, LabelE, LabelF, LabelG} {
		trainMin, testMin := t.LabelTrainMin, t.LabelTestMin
		switch l {
		case LabelD, LabelE:
			trainMin, testMin = t.RareLabelTrainMin, t.RareLabelTestMin
		case LabelG:
			trainMin, testMin = t.UnknownTrainMin, 0
		default:
		}
		if n := byLabel[key{SplitTrain, l}]; n < trainMin {
			add(Fault{Rule: RuleLabelTrain, Subject: string(l), Split: SplitTrain, Have: n, Want: trainMin})
		}
		if n := byLabel[key{SplitTest, l}]; n < testMin {
			add(Fault{Rule: RuleLabelTest, Subject: string(l), Split: SplitTest, Have: n, Want: testMin})
		}
		if l == LabelD || l == LabelE {
			if n := int64(len(labelHarnesses[l])); n < t.RareLabelHarnessesMin {
				add(Fault{Rule: RuleLabelHarnesses, Subject: string(l), Split: SplitTrain, Have: n, Want: t.RareLabelHarnessesMin})
			}
		}
		if n := byLabel[key{SplitTrain, l}]; n*100 < t.LabelShareMinPercent*train {
			add(Fault{Rule: RuleLabelShare, Subject: string(l), Split: SplitTrain, Have: percentOf(n, train), Want: t.LabelShareMinPercent})
		}
	}

	var enough int64
	for _, n := range harnessTrain {
		if n >= t.HarnessTrainMin {
			enough++
		}
	}
	if enough < t.HarnessesMin {
		add(Fault{Rule: RuleHarnesses, Subject: "harnesses", Split: SplitTrain, Have: enough, Want: t.HarnessesMin})
	}
	for _, h := range sortedHarnesses(harnessTrain) {
		if harnessTest[h] == 0 {
			add(Fault{Rule: RuleHarnessInTest, Subject: string(h), Split: SplitTest, Have: 0, Want: 1})
		}
	}

	for _, s := range []Split{SplitTrain, SplitValidation, SplitTest} {
		for _, l := range []Label{LabelA, LabelB, LabelC, LabelG} {
			if n := synth[key{s, l}]; n > 0 {
				add(Fault{Rule: RuleSynthetic, Subject: string(l), Split: s, Have: n, Want: 0})
			}
		}
	}
	for _, l := range []Label{LabelD, LabelE, LabelF} {
		n, total := synth[key{SplitTrain, l}], byLabel[key{SplitTrain, l}]
		if n*100 > t.SyntheticMaxPercent*total {
			add(Fault{Rule: RuleSyntheticShare, Subject: string(l), Split: SplitTrain, Have: percentOf(n, total), Want: t.SyntheticMaxPercent})
		}
	}
	if syntheticTest > 0 {
		add(Fault{Rule: RuleSyntheticTest, Subject: "synthetic", Split: SplitTest, Have: syntheticTest, Want: 0})
	}
	for _, src := range []LabelSource{LabelSourceWeak, LabelSourceSynthetic} {
		if n := testSources[src]; n > 0 {
			add(Fault{Rule: RuleTestSource, Subject: string(src), Split: SplitTest, Have: n, Want: 0})
		}
	}
	if len(leaked) > 0 {
		add(Fault{Rule: RuleGroupLeak, Subject: "groups", Split: SplitTest, Have: int64(len(leaked)), Want: 0})
	}
	r.Passed = len(r.Faults) == 0
	return r
}

// percentOf is n as a whole percentage of total, rounded down.
func percentOf(n, total int64) int64 {
	if total == 0 {
		return 0
	}
	return n * 100 / total
}

func sortedHarnesses(m map[protocol.Harness]int64) []protocol.Harness {
	out := make([]protocol.Harness, 0, len(m))
	for h := range m {
		out = append(out, h)
	}
	slices.Sort(out)
	return out
}
