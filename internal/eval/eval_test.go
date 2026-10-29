package eval

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// line is one synthetic predictions line.
type line struct {
	n       int
	split   string
	truth   string
	probs   map[string]float64
	mass    float64
	latency float64
	harness string
	version string
	profile string
}

const splitVal = "validation"

func (l line) id() string { return fmt.Sprintf("01JB%022d", l.n) }

func (l line) json() string {
	probs := make([]string, 0, numLabels)
	for _, k := range labels() {
		probs = append(probs, fmt.Sprintf("%q:%v", k, l.probs[string(k)]))
	}
	split, harness, version, profile, mass := l.split, l.harness, l.version, l.profile, l.mass
	if split == "" {
		split = "test"
	}
	if harness == "" {
		harness = "codex"
	}
	if version == "" {
		version = "1.0"
	}
	if profile == "" {
		profile = "base"
	}
	if mass == 0 {
		mass = 1
	}
	return fmt.Sprintf(`{"window_id":%q,"split":%q,"harness":%q,"harness_version":%q,"session_id":"sess-%d","source_quality":"pty_screen","label":%q,"probabilities":{%s},"valid_mass":%v,"invalid":%v,"latency_ms":%v,"profile":%q}`,
		l.id(), split, harness, version, l.n%3, l.truth, strings.Join(probs, ","), mass, mass < 0.5, l.latency, profile)
}

func file(ls ...line) string {
	var b strings.Builder
	for _, l := range ls {
		b.WriteString(l.json() + "\n")
	}
	return b.String()
}

func read(t *testing.T, s string, p Profile) []Prediction {
	t.Helper()
	ps, err := ReadPredictions(strings.NewReader(s), p)
	require.NoError(t, err)
	return ps
}

// handSet: four test windows, scored by hand below.
func handSet() []line {
	return []line{
		{n: 1, truth: "A", probs: map[string]float64{"A": 1}, latency: 100},
		{n: 2, truth: "B", probs: map[string]float64{"A": 0.38, "B": 0.62}, latency: 200},
		{n: 3, truth: "D", probs: map[string]float64{"A": 0.7, "D": 0.3}, latency: 300},
		{n: 4, truth: "E", probs: map[string]float64{"E": 1}, mass: 0.3, latency: 400}, // invalid: UNKNOWN
	}
}

func TestScoreByHand(t *testing.T) {
	t.Parallel()

	ps := read(t, file(handSet()...), ProfileBase)
	tt, fitted := FitTemperature(ps)
	require.False(t, fitted)
	require.InDelta(t, 1.0, tt, 0, "no validation: T = 1")
	theta, fitted := FitTheta(ps, 1)
	require.False(t, fitted)
	require.InDelta(t, DefaultTheta, theta, 0)

	m := Score(ps, 1, theta, DefaultBins)
	require.Equal(t, 4, m.N)
	require.InDelta(t, 0.5, m.Accuracy, 1e-12)
	// F1: A 2·0.5·1/1.5, B 1, D 0, E 0, over the four labels with support.
	require.InDelta(t, (2.0/3+1)/4, m.MacroF1, 1e-12)
	require.Equal(t, []int{idxA, 1, idxD, idxE}, m.MacroLabels)
	require.Equal(t, [numLabels]int{1, 1, 0, 1, 1, 0, 0}, m.Support)
	require.InDelta(t, 1, m.Recall[idxA], 1e-12)
	require.InDelta(t, 0, m.Recall[idxD], 1e-12)
	require.Equal(t, 1, m.Confusion[idxD][idxA])
	require.Equal(t, 1, m.Confusion[idxE][idxG])
	require.Equal(t, Rate{Num: 1, Den: 3}, m.FalseIdle)
	require.Equal(t, Rate{Num: 0, Den: 3}, m.FalseSafe)
	// ECE: bin 14 holds 1.0 twice, one right (0.5·2/4); bin 9 holds 0.62
	// right (0.38/4); bin 10 holds 0.7 wrong (0.7/4).
	require.InDelta(t, 0.25+0.095+0.175, m.ECERaw, 1e-12)
	require.InDelta(t, m.ECERaw, m.ECECal, 1e-12, "T = 1 changes nothing")
	require.Equal(t, 2, m.Reliability[14].N)
	require.Equal(t, 1, m.Reliability[9].N)
	// Brier: 0, 0.38²·2, 0.7²+0.7², and 2 for the invalid line.
	require.InDelta(t, (0+2*0.38*0.38+0.98+2)/4, m.BrierRaw, 1e-12)
	require.Equal(t, Rate{Num: 1, Den: 4}, m.Unknown)
	require.InDelta(t, 200, m.LatencyP50, 0)
	require.InDelta(t, 400, m.LatencyP95, 0)
}

func TestFitTemperatureKnown(t *testing.T) {
	t.Parallel()

	// All say A 0.9 / B 0.1, and 3 in 4 are A: the best calibrated p(A)
	// is 0.75, so 9^(1/T) = 3 and T = 2.
	ls := make([]line, 0, 8)
	for i := range 8 {
		truth := "A"
		if i%4 == 3 {
			truth = "B"
		}
		ls = append(ls, line{n: i + 1, split: splitVal, truth: truth, probs: map[string]float64{"A": 0.9, "B": 0.1}})
	}
	tt, fitted := FitTemperature(read(t, file(ls...), ProfileBase))
	require.True(t, fitted)
	require.InDelta(t, 2, tt, 1e-6)
	require.InDelta(t, 0.75, scaled(read(t, file(ls[0]), ProfileBase)[0], tt)[idxA], 1e-6)
}

func TestFitThetaKnown(t *testing.T) {
	t.Parallel()

	// 400 validation windows outside A with p(A) = i/400: 0.5 % allows 2,
	// so θ sits just above the third largest, 398/400.
	var ls []line
	for i := 1; i <= 400; i++ {
		pa := float64(i) / 400
		ls = append(ls, line{n: i, split: splitVal, truth: "C", probs: map[string]float64{"A": pa, "C": 1 - pa}})
	}
	ls = append(ls, line{n: 401, split: splitVal, truth: "A", probs: map[string]float64{"A": 0.2, "C": 0.8}})
	ps := read(t, file(ls...), ProfileBase)
	theta, fitted := FitTheta(ps, 1)
	require.True(t, fitted)
	require.InDelta(t, 398.0/400, theta, 1e-12)
	require.Greater(t, theta, 398.0/400)
	m := Score(ps, 1, theta, DefaultBins)
	require.Equal(t, Rate{Num: 2, Den: 400}, m.FalseSafe)

	// Fewer than 200 windows outside A allow none: θ just above the max.
	theta, _ = FitTheta(ps[390:], 1)
	require.Greater(t, theta, 1.0-1e-12)
	require.LessOrEqual(t, theta, math.Nextafter(1, 2))
}

func TestReadPredictionsFailsClosed(t *testing.T) {
	t.Parallel()

	good := line{n: 1, truth: "A", probs: map[string]float64{"A": 1}}.json()
	two := good + "\n" + line{n: 2, truth: "B", probs: map[string]float64{"B": 1}}.json()
	for name, in := range map[string]string{
		"empty":            "",
		"NaN":              strings.Replace(good, `"A":1`, `"A":NaN`, 1),
		"Infinity":         strings.Replace(good, `"A":1`, `"A":Infinity`, 1),
		"out of range":     strings.Replace(good, `"A":1`, `"A":1e400`, 1),
		"negative":         strings.Replace(good, `"A":1,"B":0`, `"A":1.5,"B":-0.5`, 1),
		"above one":        strings.Replace(good, `"A":1,"B":0`, `"A":1.1,"B":-0.1`, 1),
		"bad sum":          strings.Replace(good, `"A":1`, `"A":0.9`, 1),
		"string number":    strings.Replace(good, `"A":1`, `"A":"1"`, 1),
		"null number":      strings.Replace(good, `"latency_ms":0`, `"latency_ms":null`, 1),
		"missing latency":  strings.Replace(good, `,"latency_ms":0`, ``, 1),
		"missing label":    strings.Replace(good, `,"G":0`, ``, 1),
		"extra label":      strings.Replace(good, `"G":0`, `"G":0,"H":0`, 1),
		"unknown label":    strings.Replace(good, `"label":"A"`, `"label":"Z"`, 1),
		"repeated id":      good + "\n" + good,
		"unknown field":    strings.Replace(good, `"profile"`, `"screen":"x","profile"`, 1),
		"invalid flag":     strings.Replace(good, `"invalid":false`, `"invalid":true`, 1),
		"low mass valid":   strings.Replace(good, `"valid_mass":1`, `"valid_mass":0.4`, 1),
		"wrong profile":    strings.Replace(good, `"profile":"base"`, `"profile":"analyst-lora"`, 1),
		"unknown profile":  strings.Replace(good, `"profile":"base"`, `"profile":"big"`, 1),
		"bad harness":      strings.Replace(good, `"harness":"codex"`, `"harness":"vim"`, 1),
		"bad version":      strings.Replace(good, `"harness_version":"1.0"`, `"harness_version":"1 0"`, 1),
		"bad split":        strings.Replace(good, `"split":"test"`, `"split":"dev"`, 1),
		"train split":      strings.Replace(good, `"split":"test"`, `"split":"train"`, 1),
		"negative latency": strings.Replace(good, `"latency_ms":0`, `"latency_ms":-1`, 1),
		"trailing data":    good + ` {}`,
		"second bad line":  strings.Replace(two, `"B":1`, `"B":2`, 1),
		"repeated prob":    strings.Replace(good, `"G":0`, `"G":0,"A":1`, 1),
		"repeated field":   strings.Replace(good, `"label":"A"`, `"label":"B","label":"A"`, 1),
	} {
		_, err := ReadPredictions(strings.NewReader(in), ProfileBase)
		require.ErrorIs(t, err, ErrPredictions, name)
	}
}

func TestProfilesMustCoverTheSameWindows(t *testing.T) {
	t.Parallel()

	base := read(t, file(handSet()...), ProfileBase)
	lora := func(ls []line) []Prediction {
		for i := range ls {
			ls[i].profile = "analyst-lora"
		}
		return read(t, file(ls...), ProfileLoRA)
	}
	require.NoError(t, SameWindows(base, lora(handSet())))

	other := handSet()
	other[0].n = 99
	_, err := Evaluate(Options{Base: base, LoRA: lora(other)})
	require.ErrorIs(t, err, ErrPredictions)
	_, err = Evaluate(Options{Base: base, LoRA: lora(handSet()[:3])})
	require.ErrorIs(t, err, ErrPredictions)
	relabeled := handSet()
	relabeled[1].truth = "C"
	_, err = Evaluate(Options{Base: base, LoRA: lora(relabeled)})
	require.ErrorIs(t, err, ErrPredictions)
}

// gateSet: test windows where the LoRA beats the base on every count.
func gateSet(profile string, better bool, latency float64) []line {
	wrong := map[string]float64{"A": 0.6, "D": 0.4}
	if better {
		wrong = map[string]float64{"A": 0.3, "D": 0.7}
	}
	return []line{
		{n: 1, truth: "A", probs: map[string]float64{"A": 0.9, "B": 0.1}, latency: latency, profile: profile},
		{n: 2, truth: "D", probs: wrong, latency: latency, profile: profile},
		{n: 3, truth: "D", probs: map[string]float64{"D": 0.8, "A": 0.2}, latency: latency, profile: profile},
		{n: 4, truth: "E", probs: map[string]float64{"E": 0.7, "B": 0.3}, latency: latency, profile: profile},
	}
}

func manifestFor(ls []line) ManifestCheck {
	m := dataset.Manifest{}
	for _, l := range ls {
		m.Entries = append(m.Entries, dataset.ManifestEntry{WindowID: protocol.ULID(l.id()), Split: dataset.SplitTest, Label: dataset.Label(l.truth)})
	}
	return ManifestCheck{Given: true, Manifest: m}
}

func TestGate(t *testing.T) {
	t.Parallel()

	base := read(t, file(gateSet("base", false, 100)...), ProfileBase)
	lora := read(t, file(gateSet("analyst-lora", true, 900)...), ProfileLoRA)
	r, err := Evaluate(Options{Base: base, LoRA: lora, Manifest: manifestFor(gateSet("base", false, 0))})
	require.NoError(t, err)
	verdicts := map[int]Verdict{}
	for _, c := range r.Gate.Criteria {
		verdicts[c.ID] = c.Verdict
	}
	require.Equal(t, map[int]Verdict{0: VerdictPending, 1: VerdictPass, 2: VerdictPending, 3: VerdictPass, 4: VerdictPass, 5: VerdictPending}, verdicts,
		"a matching manifest without a preparation gate result stays PENDING")
	require.Equal(t, VerdictPending, r.Gate.Verdict, "a PENDING criterion blocks PASS")

	// Every criterion passing would pass; one failing fails.
	g := Gate{Criteria: []Criterion{{Verdict: VerdictPass}, {Verdict: VerdictPass}}}
	require.Equal(t, VerdictPass, gateVerdict(g))
	slow := read(t, file(gateSet("analyst-lora", true, 1600)...), ProfileLoRA)
	r, err = Evaluate(Options{Base: base, LoRA: slow, Manifest: manifestFor(gateSet("base", false, 0))})
	require.NoError(t, err)
	require.Equal(t, VerdictFail, r.Gate.Criteria[4].Verdict, "p95 1600 ms over the 1.5 s budget")
	require.Equal(t, VerdictFail, r.Gate.Verdict)

	// No LoRA and no manifest: the criteria that need them wait.
	r, err = Evaluate(Options{Base: base})
	require.NoError(t, err)
	for _, c := range r.Gate.Criteria {
		require.Equal(t, VerdictPending, c.Verdict, "criterion %d", c.ID)
	}
	// A manifest that does not match fails criterion 0.
	mismatch := manifestFor(gateSet("base", false, 0)[:3])
	r, err = Evaluate(Options{Base: base, LoRA: lora, Manifest: mismatch})
	require.NoError(t, err)
	require.Equal(t, VerdictFail, r.Gate.Criteria[0].Verdict)
	// The manifest's preparation gate decides a matching manifest.
	for _, tc := range []struct {
		r    *dataset.Readiness
		want Verdict
	}{
		{nil, VerdictPending},
		{&dataset.Readiness{Passed: true, Faults: []dataset.Fault{}}, VerdictPass},
		{&dataset.Readiness{Faults: []dataset.Fault{{Rule: dataset.RuleSplitSize, Subject: "test", Split: dataset.SplitTest, Have: 4, Want: 300}}}, VerdictFail},
	} {
		mc := manifestFor(gateSet("base", false, 0))
		mc.Manifest.Readiness = tc.r
		r, err = Evaluate(Options{Base: base, LoRA: lora, Manifest: mc})
		require.NoError(t, err)
		require.Equal(t, tc.want, r.Gate.Criteria[0].Verdict)
	}
	// A passed gate does not save a manifest that does not match.
	mismatch.Manifest.Readiness = &dataset.Readiness{Passed: true, Faults: []dataset.Fault{}}
	r, err = Evaluate(Options{Base: base, LoRA: lora, Manifest: mismatch})
	require.NoError(t, err)
	require.Equal(t, VerdictFail, r.Gate.Criteria[0].Verdict)

	invalid := manifestFor(gateSet("base", false, 0))
	invalid.Err = ErrPredictions
	r, err = Evaluate(Options{Base: base, LoRA: lora, Manifest: invalid})
	require.NoError(t, err)
	require.Equal(t, VerdictFail, r.Gate.Criteria[0].Verdict, "an invalid manifest fails")
}

// gateVerdict recomputes the overall verdict of g.
func gateVerdict(g Gate) Verdict {
	return gateOf(g.Criteria).Verdict
}

func TestResultsAreStable(t *testing.T) {
	t.Parallel()

	render := func() string {
		bs, ls := file(gateSet("base", false, 100)...), file(gateSet("analyst-lora", true, 900)...)
		r, err := Evaluate(Options{
			Base: read(t, bs, ProfileBase), LoRA: read(t, ls, ProfileLoRA),
			BaseRaw: []byte(bs), LoRARaw: []byte(ls), Manifest: manifestFor(gateSet("base", false, 0)),
		})
		require.NoError(t, err)
		return r.Markdown()
	}
	first := render()
	for range 5 {
		require.Equal(t, first, render(), "same inputs, same bytes")
	}
	require.Contains(t, first, "## Gate: PENDING")
	require.Contains(t, first, "| macro-F1 labels (with support) | A, D, E |")
	require.Contains(t, first, "| codex |")
	for _, l := range gateSet("base", false, 0) {
		require.NotContains(t, first, l.id(), "no window ids")
	}
	require.NotContains(t, first, "sess-", "no session ids")
}

// TestResultsSayWhereTCameFrom: T fitted on validation and the default
// T = 1 render differently, each byte-stable.
func TestResultsSayWhereTCameFrom(t *testing.T) {
	t.Parallel()

	render := func(ls []line) string {
		r, err := Evaluate(Options{Base: read(t, file(ls...), ProfileBase)})
		require.NoError(t, err)
		return r.Markdown()
	}
	noVal := handSet()
	fitted := append(handSet(), line{n: 9, split: splitVal, truth: "A", probs: map[string]float64{"A": 0.8, "B": 0.2}})
	// A validation line that is invalid does not fit T.
	invalidVal := append(handSet(), line{n: 9, split: splitVal, truth: "A", probs: map[string]float64{"A": 1}, mass: 0.2})

	for _, tc := range []struct {
		ls   []line
		want string
	}{
		{noVal, "Temperature T = 1.000000 (default; no valid validation windows)."},
		{invalidVal, "Temperature T = 1.000000 (default; no valid validation windows)."},
		{fitted, "(fitted on validation)."},
	} {
		out := render(tc.ls)
		require.Contains(t, out, tc.want)
		require.Equal(t, out, render(tc.ls), "byte-stable")
	}
	require.NotContains(t, render(noVal), "fitted on validation")
}
