package dataset

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

var readyHarnesses = []protocol.Harness{protocol.HarnessClaudeCode, protocol.HarnessCodex, protocol.HarnessOpencode}

// readyEntries meets testThresholds exactly: every count below sits on
// its threshold.
func readyEntries() []ManifestEntry {
	var out []ManifestEntry
	n := 0
	add := func(split Split, label Label, count int) {
		for range count {
			out = append(out, ManifestEntry{
				WindowID: protocol.ULID(fmt.Sprintf("01JB%022d", n)), Split: split, Label: label,
				GroupDigest: testDigest(fmt.Sprintf("group-%d", n)), Harness: readyHarnesses[n%3],
				SourceQuality: protocol.SourceQualityPTYScreen, LabelSource: LabelSourceHook,
			})
			n++
		}
	}
	order := []Label{LabelA, LabelB, LabelC, LabelD, LabelE, LabelF, LabelG}
	train := map[Label]int{LabelA: 4, LabelB: 5, LabelC: 5, LabelF: 5, LabelD: 6, LabelE: 7, LabelG: 3}
	test := map[Label]int{LabelA: 1, LabelB: 2, LabelC: 2, LabelF: 2, LabelD: 2, LabelE: 3, LabelG: 2}
	for _, l := range order {
		add(SplitTrain, l, train[l])
	}
	add(SplitValidation, LabelB, 5)
	for _, l := range order {
		add(SplitTest, l, test[l])
	}
	return out
}

// testThresholds are scaled down so the fixture stays small.
func testThresholds() ReadinessThresholds {
	return ReadinessThresholds{
		TrainMin: 35, ValidationMin: 5, TestMin: 14,
		LabelTrainMin: 4, LabelTestMin: 1,
		RareLabelTrainMin: 6, RareLabelTestMin: 2, RareLabelHarnessesMin: 3, RareLabelTestHarnessesMin: 2,
		UnknownTrainMin: 3, LabelShareMinPercent: 8,
		HarnessesMin: 3, HarnessTrainMin: minHarnessTrain(readyEntries()),
		SyntheticMaxPercent: 30,
	}
}

func minHarnessTrain(es []ManifestEntry) int64 {
	n := map[protocol.Harness]int64{}
	for _, e := range es {
		if e.Split == SplitTrain {
			n[e.Harness]++
		}
	}
	m := int64(-1)
	for _, v := range n {
		if m < 0 || v < m {
			m = v
		}
	}
	return m
}

func rules(r Readiness) []string {
	out := make([]string, 0, len(r.Faults))
	for _, f := range r.Faults {
		out = append(out, f.Rule+":"+f.Subject)
	}
	return out
}

func TestReadinessAtTheThresholdsPasses(t *testing.T) {
	t.Parallel()
	r := CheckReadiness(readyEntries(), testThresholds())
	require.True(t, r.Passed, "%v", rules(r))
	require.Empty(t, r.Faults)
	require.NotNil(t, r.Faults, "an empty list, never null")
}

// TestReadinessOneAboveFails raises each threshold one past the fixture:
// exactly that rule fails.
func TestReadinessOneAboveFails(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		bump func(*ReadinessThresholds)
		want []string
	}{
		"train size":          {func(t *ReadinessThresholds) { t.TrainMin++ }, []string{"split_size:train"}},
		"validation size":     {func(t *ReadinessThresholds) { t.ValidationMin++ }, []string{"split_size:validation"}},
		"test size":           {func(t *ReadinessThresholds) { t.TestMin++ }, []string{"split_size:test"}},
		"label in train":      {func(t *ReadinessThresholds) { t.LabelTrainMin++ }, []string{"label_train:A"}},
		"label in test":       {func(t *ReadinessThresholds) { t.LabelTestMin++ }, []string{"label_test:A"}},
		"rare in train":       {func(t *ReadinessThresholds) { t.RareLabelTrainMin++ }, []string{"label_train:D"}},
		"rare in test":        {func(t *ReadinessThresholds) { t.RareLabelTestMin++ }, []string{"label_test:D"}},
		"rare harnesses":      {func(t *ReadinessThresholds) { t.RareLabelHarnessesMin++ }, []string{"label_harnesses:D", "label_harnesses:E"}},
		"rare test harnesses": {func(t *ReadinessThresholds) { t.RareLabelTestHarnessesMin++ }, []string{"label_harnesses:D"}},
		"unknown in train":    {func(t *ReadinessThresholds) { t.UnknownTrainMin++ }, []string{"label_train:G"}},
		"label share":         {func(t *ReadinessThresholds) { t.LabelShareMinPercent++ }, []string{"label_share:G"}},
		"harness count":       {func(t *ReadinessThresholds) { t.HarnessesMin++ }, []string{"harnesses:harnesses"}},
		"harness size":        {func(t *ReadinessThresholds) { t.HarnessTrainMin++ }, []string{"harnesses:harnesses"}},
	} {
		th := testThresholds()
		tc.bump(&th)
		require.Equal(t, tc.want, rules(CheckReadiness(readyEntries(), th)), name)
	}
}

func TestReadinessEntryRules(t *testing.T) {
	t.Parallel()
	first := func(es []ManifestEntry, split Split, label Label) *ManifestEntry {
		for i := range es {
			if es[i].Split == split && es[i].Label == label {
				return &es[i]
			}
		}
		panic("no such entry")
	}
	for name, tc := range map[string]struct {
		edit func([]ManifestEntry) []ManifestEntry
		want []string
	}{
		"harness missing from test": {func(es []ManifestEntry) []ManifestEntry {
			e := es[0]
			e.WindowID, e.Harness, e.GroupDigest = "01JB9999999999999999999999", protocol.HarnessMuseCode, testDigest("new")
			return append(es, e)
		}, []string{"harness_in_test:muse-code"}},
		"synthetic outside D, E, F": {func(es []ManifestEntry) []ManifestEntry {
			first(es, SplitTrain, LabelA).LabelSource = LabelSourceSynthetic
			return es
		}, []string{"synthetic_label:A"}},
		"synthetic share at 30 %": {func(es []ManifestEntry) []ManifestEntry {
			first(es, SplitTrain, LabelD).SourceQuality = protocol.SourceQualitySynthetic // 1 of 6
			return es
		}, nil},
		"synthetic share above 30 %": {func(es []ManifestEntry) []ManifestEntry {
			n := 0
			for i := range es {
				if es[i].Split == SplitTrain && es[i].Label == LabelD && n < 2 {
					es[i].LabelSource = LabelSourceSynthetic // 2 of 6
					n++
				}
			}
			return es
		}, []string{"synthetic_share:D"}},
		"synthetic in test": {func(es []ManifestEntry) []ManifestEntry {
			first(es, SplitTest, LabelD).SourceQuality = protocol.SourceQualitySynthetic
			return es
		}, []string{"synthetic_in_test:synthetic"}},
		"weak in test": {func(es []ManifestEntry) []ManifestEntry {
			first(es, SplitTest, LabelB).LabelSource = LabelSourceWeak
			return es
		}, []string{"test_label_source:weak"}},
		"weak in train is fine": {func(es []ManifestEntry) []ManifestEntry {
			first(es, SplitTrain, LabelB).LabelSource = LabelSourceWeak
			return es
		}, nil},
		"session in two splits": {func(es []ManifestEntry) []ManifestEntry {
			first(es, SplitTest, LabelB).GroupDigest = first(es, SplitTrain, LabelB).GroupDigest
			return es
		}, []string{"group_leak:groups"}},
	} {
		got := rules(CheckReadiness(tc.edit(readyEntries()), testThresholds()))
		if tc.want == nil {
			require.Empty(t, got, name)
			continue
		}
		require.Equal(t, tc.want, got, name)
	}
}

func TestReadinessInTheManifest(t *testing.T) {
	t.Parallel()
	m, err := BuildManifest(testWindows())
	require.NoError(t, err)
	require.NotNil(t, m.Readiness)
	require.False(t, m.Readiness.Passed, "four windows are far from ready")
	require.Equal(t, DefaultReadinessThresholds(), m.Readiness.Thresholds)
	require.NoError(t, m.Validate())

	// The digest covers it, and Validate recomputes it.
	forged := m
	r := *m.Readiness
	r.Passed, r.Faults = true, []Fault{}
	forged.Readiness = &r
	require.NotEqual(t, m.DatasetDigest, ManifestDigest(forged))
	forged.DatasetDigest = ManifestDigest(forged)
	require.Error(t, forged.Validate(), "a pass the entries do not earn")

	// A session in two splits never makes a manifest.
	ws := testWindows()
	ws[0].Split.GroupDigest = ws[1].Split.GroupDigest
	_, err = BuildManifest(ws)
	require.ErrorIs(t, err, ErrInvalidManifest)

	bad := DefaultReadinessThresholds()
	bad.TrainMin = -1
	_, err = BuildManifestWith(testWindows(), bad)
	require.ErrorIs(t, err, ErrBadThresholds)
	bad = DefaultReadinessThresholds()
	bad.SyntheticMaxPercent = 101
	require.ErrorIs(t, bad.Validate(), ErrBadThresholds)
}

func TestFaultLines(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		f    Fault
		want string
	}{
		{Fault{Rule: RuleLabelTrain, Subject: "D", Split: SplitTrain, Have: 61, Want: 150}, "D: 61/150 in train"},
		{Fault{Rule: RuleLabelHarnesses, Subject: "D", Split: SplitTrain, Have: 2, Want: 3}, "D: 2 harnesses of 3 in train"},
		{Fault{Rule: RuleSplitSize, Subject: "test", Split: SplitTest, Have: 120, Want: 300}, "test: 120/300 windows"},
		{Fault{Rule: RuleTestSource, Subject: "weak", Split: SplitTest, Have: 4, Want: 0}, "test: 4 weak-labeled, 0 allowed"},
	} {
		require.Equal(t, tc.want, tc.f.Line())
	}
}

// TestRareLabelTestHarnesses: under the defaults, 30 D windows in test
// from two harnesses are one fault, so D's recall never rests on one TUI.
func TestRareLabelTestHarnesses(t *testing.T) {
	t.Parallel()
	es := make([]ManifestEntry, 0, 30)
	for i := range 30 {
		es = append(es, ManifestEntry{
			WindowID: protocol.ULID(fmt.Sprintf("01JB%022d", i)), Split: SplitTest, Label: LabelD,
			GroupDigest: testDigest(fmt.Sprintf("g-%d", i)), Harness: readyHarnesses[i%2],
			SourceQuality: protocol.SourceQualityPTYScreen, LabelSource: LabelSourceHook,
		})
	}
	var test []Fault
	for _, f := range CheckReadiness(es, DefaultReadinessThresholds()).Faults {
		if f.Rule == RuleLabelHarnesses && f.Subject == "D" && f.Split == SplitTest {
			test = append(test, f)
		}
	}
	require.Equal(t, []Fault{{Rule: RuleLabelHarnesses, Subject: "D", Split: SplitTest, Have: 2, Want: 3}}, test)
	require.Equal(t, "D: 2 harnesses of 3 in test", test[0].Line())
}
