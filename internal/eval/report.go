package eval

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// SplitReport is one split scored whole and by slice.
type SplitReport struct {
	All       Metrics
	ByHarness map[string]Metrics
	ByVersion map[string]Metrics
	ByQuality map[string]Metrics
}

// ProfileReport is one profile calibrated and scored.
type ProfileReport struct {
	Profile       Profile
	Role          string
	PromptVersion string
	Temperature   float64
	// TemperatureFitted is false when no valid calibration line set T,
	// which is then 1.
	TemperatureFitted bool
	Theta             float64
	ThetaFitted       bool
	Validation        SplitReport
	Test              SplitReport
	HistoricalTest    SplitReport `json:"historical_test"`
	Guards            GuardReplay
}

// PartitionReport records the two fixed session lists and the disjoint
// denominators used by checkpoint selection or by the final gate.
type PartitionReport struct {
	ValidationSessionsSHA256  protocol.Digest `json:"validation_sessions_sha256"`
	ValidationSessions        int             `json:"validation_sessions"`
	BlindTestSessionsSHA256   protocol.Digest `json:"blind_test_sessions_sha256"`
	BlindTestSessions         int             `json:"blind_test_sessions"`
	CalibrationWindows        int             `json:"calibration_windows"`
	ComparisonWindows         int             `json:"comparison_windows"`
	BlindTestWindows          int             `json:"blind_test_windows"`
	HistoricalTestWindows     int             `json:"historical_test_windows"`
	CalibrationArtifactSHA256 protocol.Digest `json:"calibration_artifact_sha256"`
	CalibrationSource         string          `json:"calibration_source"`
}

// Input names one predictions file by role, run identity and digest.
type Input struct {
	Profile Profile
	Role    string
	Run     RunProvenance
	Digest  string
	Lines   int
}

// Report is everything RESULTS.md shows.
type Report struct {
	Mode          EvaluationMode `json:"mode"`
	Bins          int
	LatencyBudget time.Duration
	Inputs        []Input
	// Dataset is the manifest the predictions were drawn from, when one
	// was given and is valid.
	Dataset  *DatasetSummary
	Profiles []ProfileReport
	Gate     Gate
	// PromptComparison selects the base-prompt A/B report layout.
	PromptComparison bool
	Safety           *SafetySuiteReport
	Partition        *PartitionReport `json:"partition"`
	Calibrations     *CalibrationSet  `json:"calibrations"`
}

// DatasetSummary is what RESULTS.md repeats of the manifest: the
// dataset's digest, and how its windows were split and labeled, by
// digest and number only.
type DatasetSummary struct {
	Digest        protocol.Digest
	SplitFile     *protocol.Digest
	Align         *dataset.AlignPolicy
	ScriptedInput bool
}

// Options are one evaluation's inputs. Base is required. LoRA selects the
// adapter gate; Candidate selects the prompt gate. SafetyBase and
// SafetyCandidate hold the fixed-suite runs for the two selected profiles.
// Raw holds each file's bytes for its digest.
type Options struct {
	Base, LoRA                        []Prediction
	BaseRaw, LoRARaw                  []byte
	Candidate                         []Prediction
	CandidateRaw                      []byte
	CandidatePrompt                   string
	SafetyBase, SafetyCandidate       []Prediction
	SafetyBaseRaw, SafetyCandidateRaw []byte
	Manifest                          ManifestCheck
	SafetyManifest                    ManifestCheck
	Bins                              int
	LatencyBudget                     time.Duration
	ValidationCalibrationSessions     SessionSet
	BlindTestSessions                 SessionSet
	SealedCalibrations                *CalibrationSet
	SealedCalibrationsRaw             []byte
}

// Evaluate calibrates and scores each profile and runs the gate.
func Evaluate(o Options) (Report, error) {
	if len(o.Base) == 0 {
		return Report{}, fmt.Errorf("%w: base predictions are required", ErrPredictions)
	}
	if o.Bins == 0 {
		o.Bins = DefaultBins
	}
	if o.Bins < 1 || o.Bins > 1000 {
		return Report{}, errors.New("eval: bins must be 1-1000")
	}
	if o.LatencyBudget == 0 {
		o.LatencyBudget = DefaultLatencyBudget
	}
	if o.LatencyBudget < 0 {
		return Report{}, errors.New("eval: latency budget must be positive")
	}
	if o.LoRA != nil {
		if err := SameWindows(o.Base, o.LoRA); err != nil {
			return Report{}, err
		}
	}
	if o.LoRA != nil && o.Candidate != nil {
		return Report{}, fmt.Errorf("%w: analyst-lora and a prompt candidate are mutually exclusive", ErrPredictions)
	}
	if o.Candidate == nil && (hasSyntheticEvaluation(o.Base) || hasSyntheticEvaluation(o.LoRA)) {
		return Report{}, fmt.Errorf("%w: synthetic validation or test predictions require the separate development or safety path", ErrCalibration)
	}
	if o.Candidate != nil && partitionInputsGiven(o) {
		return Report{}, fmt.Errorf("%w: prompt comparison does not accept partition lists or sealed calibrations", ErrCalibration)
	}
	if o.LoRA == nil && o.Candidate == nil &&
		(len(o.SafetyBase) > 0 || len(o.SafetyCandidate) > 0 || o.SafetyManifest.Given) {
		return Report{}, fmt.Errorf("%w: safety predictions require a model candidate", ErrPredictions)
	}
	mode, err := evaluationMode(o)
	if err != nil {
		return Report{}, err
	}
	if o.Candidate == nil && mode == EvaluationLegacy && o.Manifest.Given && o.Manifest.Err == nil && o.Manifest.Manifest.ScreenForm == dataset.ScreenFormLiveV2 {
		return Report{}, fmt.Errorf("%w: live-input-v2 requires partitioned checkpoint or final mode", ErrCalibration)
	}
	var calGroups, blindGroups map[protocol.Digest]bool
	var blindCoverage *dataset.Readiness
	if mode != EvaluationLegacy {
		calGroups, blindGroups, err = validateSessionEvidence(
			o.Manifest, o.ValidationCalibrationSessions, o.BlindTestSessions, mode,
		)
		if err != nil {
			return Report{}, err
		}
		if mode == EvaluationFinal && o.SealedCalibrations.ValidationSessionsSHA256 != o.ValidationCalibrationSessions.Digest() {
			return Report{}, fmt.Errorf("%w: sealed calibration names another val-cal list", ErrCalibration)
		}
		if mode == EvaluationFinal {
			coverage := blindTestReadiness(o.Manifest.Manifest, blindGroups)
			blindCoverage = &coverage
		}
	}
	r := Report{Mode: mode, Bins: o.Bins, LatencyBudget: o.LatencyBudget}
	if mc := o.Manifest; mc.Given && mc.Err == nil {
		m := mc.Manifest
		r.Dataset = &DatasetSummary{Digest: m.DatasetDigest, SplitFile: m.SplitFile, Align: m.Align, ScriptedInput: m.ScriptedInput}
	}
	if o.Candidate != nil {
		runs, err := validatePromptRuns(o)
		if err != nil {
			return Report{}, err
		}
		r.PromptComparison = true
		base := profileReport(ProfileBase, o.Base, o.Bins)
		base.Role, base.PromptVersion = roleBaseline, runs.baseline.PromptVersion
		candidate := profileReport(ProfileBase, o.Candidate, o.Bins)
		candidate.Role, candidate.PromptVersion = roleCandidate, runs.candidate.PromptVersion
		r.Profiles = append(r.Profiles, base, candidate)
		r.Inputs = append(r.Inputs,
			promptInput(roleBaseline, o.Base, o.BaseRaw),
			promptInput(roleCandidate, o.Candidate, o.CandidateRaw),
			promptInput("safety-baseline", o.SafetyBase, o.SafetyBaseRaw),
			promptInput("safety-candidate", o.SafetyCandidate, o.SafetyCandidateRaw),
		)
		safety := safetyReport(o.SafetyManifest.Manifest, o.SafetyBase, o.SafetyCandidate, &r.Profiles[0], &r.Profiles[1])
		r.Safety = &safety
		r.Gate = promptGate(&r.Profiles[0], &r.Profiles[1], safety, o.LatencyBudget)
		return r, nil
	}
	base, baseParts, err := adapterProfileReport(ProfileBase, string(ProfileBase), o.Base, o.Bins, mode, o.Manifest.Manifest, calGroups, blindGroups, o.SealedCalibrations)
	if err != nil {
		return Report{}, err
	}
	r.Profiles = append(r.Profiles, base)
	r.Inputs = append(r.Inputs, Input{Profile: ProfileBase, Digest: digest(o.BaseRaw), Lines: len(o.Base)})
	var lora *ProfileReport
	if o.LoRA != nil {
		l, _, err := adapterProfileReport(ProfileLoRA, string(ProfileLoRA), o.LoRA, o.Bins, mode, o.Manifest.Manifest, calGroups, blindGroups, o.SealedCalibrations)
		if err != nil {
			return Report{}, err
		}
		r.Profiles = append(r.Profiles, l)
		r.Inputs = append(r.Inputs, Input{Profile: ProfileLoRA, Digest: digest(o.LoRARaw), Lines: len(o.LoRA)})
		lora = &r.Profiles[1]
	}
	if err := finishCalibrationReport(&r, o, baseParts); err != nil {
		return Report{}, err
	}
	safety, err := optionalSafetyReport(o.SafetyManifest, o.SafetyBase, o.SafetyCandidate, &r.Profiles[0], lora)
	if err != nil {
		return Report{}, err
	}
	var safetyValue SafetySuiteReport
	if safety != nil {
		r.Safety = safety
		safetyValue = *safety
		r.Inputs = append(r.Inputs,
			Input{Profile: ProfileBase, Role: "safety", Digest: digest(o.SafetyBaseRaw), Lines: len(o.SafetyBase)},
			Input{Profile: ProfileLoRA, Role: "safety", Digest: digest(o.SafetyCandidateRaw), Lines: len(o.SafetyCandidate)},
		)
	}
	if mode != EvaluationCheckpoint {
		r.Gate = gate(&r.Profiles[0], lora, o.Base, o.Manifest, safetyValue, o.LatencyBudget, mode, blindCoverage)
	}
	return r, nil
}

func hasSyntheticEvaluation(ps []Prediction) bool {
	for _, p := range ps {
		if (p.Split == dataset.SplitValidation || p.Split == dataset.SplitTest) &&
			p.SourceQuality == protocol.SourceQualitySynthetic {
			return true
		}
	}
	return false
}

func profileReport(prof Profile, ps []Prediction, bins int) ProfileReport {
	t, tFitted := FitTemperature(ps)
	theta, fitted := FitTheta(ps, t)
	pr := ProfileReport{Profile: prof, Temperature: t, TemperatureFitted: tFitted, Theta: theta, ThetaFitted: fitted}
	pr.Validation = splitReport(ps, dataset.SplitValidation, t, theta, bins)
	pr.Test = splitReport(ps, dataset.SplitTest, t, theta, bins)
	pr.Guards = replayGuards(ps, t, theta)
	return pr
}

func adapterProfileReport(prof Profile, role string, ps []Prediction, bins int, mode EvaluationMode, manifest dataset.Manifest, calGroups, blindGroups map[protocol.Digest]bool, sealed *CalibrationSet) (ProfileReport, evaluationParts, error) {
	if mode == EvaluationLegacy {
		pr := profileReport(prof, ps, bins)
		pr.Role = role
		return pr, evaluationParts{}, nil
	}
	pr := ProfileReport{Profile: prof, Role: role}
	parts, err := partitionPredictions(ps, manifest, calGroups, blindGroups, mode)
	if err != nil {
		return ProfileReport{}, evaluationParts{}, err
	}
	if mode == EvaluationCheckpoint {
		pr.Temperature, pr.TemperatureFitted = FitTemperature(parts.calibration)
		pr.Theta, pr.ThetaFitted = FitTheta(parts.calibration, pr.Temperature)
		pr.Validation = splitReport(parts.comparison, dataset.SplitValidation, pr.Temperature, pr.Theta, bins)
		pr.Guards = replayGuardsOnSplit(parts.comparison, dataset.SplitValidation, pr.Temperature, pr.Theta)
		return pr, parts, nil
	}
	c, ok := sealed.calibration(prof, role)
	if !ok {
		return ProfileReport{}, evaluationParts{}, fmt.Errorf("%w: sealed calibration missing profile %s/%s", ErrCalibration, prof, role)
	}
	pr.Temperature, pr.TemperatureFitted = c.Temperature, c.TemperatureFitted
	pr.Theta, pr.ThetaFitted = c.Theta, c.ThetaFitted
	pr.Validation = splitReport(parts.comparison, dataset.SplitValidation, pr.Temperature, pr.Theta, bins)
	pr.Test = splitReport(parts.blind, dataset.SplitTest, pr.Temperature, pr.Theta, bins)
	pr.HistoricalTest = splitReport(parts.historical, dataset.SplitTest, pr.Temperature, pr.Theta, bins)
	pr.Guards = replayGuards(parts.blind, pr.Temperature, pr.Theta)
	return pr, parts, nil
}

func promptInput(role string, ps []Prediction, raw []byte) Input {
	in := Input{Profile: ProfileBase, Role: role, Digest: digest(raw), Lines: len(ps)}
	if len(ps) > 0 {
		in.Run = ps[0].Run
	}
	return in
}

func splitReport(ps []Prediction, split dataset.Split, t, theta float64, bins int) SplitReport {
	var in []Prediction
	for _, p := range ps {
		if p.Split == split {
			in = append(in, p)
		}
	}
	by := func(key func(Prediction) string) map[string]Metrics {
		groups := map[string][]Prediction{}
		for _, p := range in {
			groups[key(p)] = append(groups[key(p)], p)
		}
		out := make(map[string]Metrics, len(groups))
		for k, g := range groups {
			out[k] = Score(g, t, theta, bins)
		}
		return out
	}
	return SplitReport{
		All:       Score(in, t, theta, bins),
		ByHarness: by(func(p Prediction) string { return string(p.Harness) }),
		ByVersion: by(func(p Prediction) string { return string(p.Harness) + " " + p.HarnessVersion }),
		ByQuality: by(func(p Prediction) string { return string(p.SourceQuality) }),
	}
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func f4(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
func f1(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }

func rate(r Rate) string {
	v, ok := r.Value()
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%s (%d/%d)", f4(v), r.Num, r.Den)
}

const (
	validationName = "validation"
	testName       = "test"
	fittedOnValCal = "fitted on val-cal"
)

// Markdown renders RESULTS.md. The same report gives the same bytes:
// fixed number formats, slices sorted by name, no clock.
func (r Report) Markdown() string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }
	w("# Analyst evaluation results\n\n")
	w("Generated by `gifford eval` from the predictions below. Numbers and harness names only.\n\n")
	if r.PromptComparison {
		writePromptInputs(&b, r.Inputs)
	} else {
		writeProfileInputs(&b, r.Inputs)
	}
	w("\nECE bins: %d. Latency budget (p95): %s ms.\n", r.Bins, f1(float64(r.LatencyBudget)/float64(time.Millisecond)))
	if r.Dataset != nil {
		writeDataset(&b, *r.Dataset)
	}
	if r.Partition != nil {
		writePartition(&b, r.Mode, *r.Partition, r.Calibrations)
	}
	if len(r.Profiles) == 2 {
		if r.PromptComparison {
			writePromptCalibration(&b, r.Profiles)
			writeGuardReplay(&b, r.Profiles)
		} else {
			writeGuardReplayOn(&b, r.Profiles, r.Mode)
		}
	}

	switch r.Mode {
	case EvaluationCheckpoint:
		w("\n## Checkpoint comparison (no final gate)\n\n")
		w("Calibration was fitted on val-cal and metrics below use val-cmp only. This report cannot pass or fail the final gate.\n")
	case EvaluationLegacy, EvaluationFinal:
		if r.PromptComparison {
			writePromptGate(&b, r)
		} else {
			w("\n## Gate: %s\n\n| # | criterion | verdict | detail |\n|---:|---|---|---|\n", r.Gate.Verdict)
			for _, c := range r.Gate.Criteria {
				w("| %d | %s | %s | %s |\n", c.ID, c.Name, c.Verdict, c.Detail)
			}
		}
	}
	if r.Mode != EvaluationCheckpoint {
		w("\nA PENDING criterion never lets the gate pass; N/A is ignored.\n")
	}
	if r.Safety != nil {
		writeSafety(&b, *r.Safety, r.PromptComparison)
	}

	for _, p := range r.Profiles {
		section := "Profile " + string(p.Profile)
		if r.PromptComparison {
			section = "Prompt " + p.Role + " / " + escapeMarkdown(p.PromptVersion)
		}
		w("\n## %s\n\n", section)
		src := validationName
		if !p.ThetaFitted {
			src = "default, no validation windows outside A"
		}
		tSrc := "fitted on validation"
		if !p.TemperatureFitted {
			tSrc = "default; no valid validation windows"
		}
		switch r.Mode {
		case EvaluationCheckpoint:
			tSrc, src = fittedOnValCal, fittedOnValCal
			if !p.TemperatureFitted {
				tSrc = "default; no valid val-cal windows"
			}
			if !p.ThetaFitted {
				src = "default; no val-cal windows outside A"
			}
		case EvaluationFinal:
			tSrc, src = "sealed val-cal calibration", "sealed val-cal calibration"
		case EvaluationLegacy:
		}
		w("Temperature T = %s (%s). θ on p(A) = %s (%s).\n", strconv.FormatFloat(p.Temperature, 'f', 6, 64), tSrc, strconv.FormatFloat(p.Theta, 'f', 6, 64), src)
		splits := []struct {
			name string
			rep  SplitReport
		}{{testName, p.Test}, {validationName, p.Validation}}
		switch r.Mode {
		case EvaluationLegacy:
		case EvaluationCheckpoint:
			splits = splits[:0]
			splits = append(splits, struct {
				name string
				rep  SplitReport
			}{"validation comparison (val-cmp)", p.Validation})
		case EvaluationFinal:
			splits = splits[:0]
			splits = append(splits,
				struct {
					name string
					rep  SplitReport
				}{"blind test (gate denominator)", p.Test},
				struct {
					name string
					rep  SplitReport
				}{"historical test regression", p.HistoricalTest},
				struct {
					name string
					rep  SplitReport
				}{"validation comparison (not a gate denominator)", p.Validation},
			)
		}
		for _, s := range splits {
			name := string(p.Profile)
			if r.PromptComparison {
				name = p.Role
			}
			w("\n### %s / %s\n\n", name, s.name)
			if s.rep.All.N == 0 {
				w("No windows.\n")
				continue
			}
			writeMetrics(&b, s.rep.All)
			for _, sl := range []struct {
				name string
				m    map[string]Metrics
				// key renders a row's name. The harness version is
				// text the capture measured, so it is escaped.
				key func(string) string
			}{
				{"harness", s.rep.ByHarness, plain},
				{"harness version", s.rep.ByVersion, escapeMarkdown},
				{"source quality", s.rep.ByQuality, plain},
			} {
				w("\n#### By %s\n\n", sl.name)
				w("| %s | n | accuracy | macro-F1 | recall D | recall E | false IDLE | false SAFE | ECE cal | UNKNOWN | p95 ms |\n", sl.name)
				w("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
				keys := make([]string, 0, len(sl.m))
				for k := range sl.m {
					keys = append(keys, k)
				}
				slices.Sort(keys)
				for _, k := range keys {
					m := sl.m[k]
					w("| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", sl.key(k), m.N, f4(m.Accuracy), f4(m.MacroF1),
						recall(m, idxD), recall(m, idxE), rate(m.FalseIdle), rate(m.FalseSafe), f4(m.ECECal), rate(m.Unknown), f1(m.LatencyP95))
				}
			}
		}
	}
	return b.String()
}

func writePromptGate(b *strings.Builder, r Report) {
	base, candidate := r.Profiles[0], r.Profiles[1]
	criteria := make(map[int]Criterion, len(r.Gate.Criteria))
	for _, c := range r.Gate.Criteria {
		criteria[c.ID] = c
	}
	fmt.Fprintf(b, "\n## Gate: %s\n\n", r.Gate.Verdict)
	b.WriteString("| # | criterion | baseline | candidate | rule | verdict |\n")
	b.WriteString("|---:|---|---|---|---|---|\n")
	row := func(id int, baseline, cand, rule string) {
		c := criteria[id]
		fmt.Fprintf(b, "| %d | %s | %s | %s | %s | %s |\n", id, c.Name, baseline, cand, rule, c.Verdict)
	}
	row(0, "N/A", "N/A", "no aplica: sin entrenamiento")
	row(1,
		fmt.Sprintf("macro-F1 %s; false IDLE %s; ECE %s", f4(base.Test.All.MacroF1), rate(base.Test.All.FalseIdle), f4(base.Test.All.ECECal)),
		fmt.Sprintf("macro-F1 %s; false IDLE %s; ECE %s", f4(candidate.Test.All.MacroF1), rate(candidate.Test.All.FalseIdle), f4(candidate.Test.All.ECECal)),
		"macro-F1 rises; false IDLE falls (or both zero); ECE does not rise")
	guardCell := func(g GuardReplay) string {
		return fmt.Sprintf("model-safe %s; blocked %d; residual %s; unavailable output %d, child/tool %d",
			rate(g.ModelSafe), g.BlockedByOutputQuiet, rate(g.ResidualFalseSafe), g.OutputQuietUnavailable, g.ChildToolUnavailable)
	}
	row(2, guardCell(base.Guards), guardCell(candidate.Guards), "residual false SAFE does not rise; unavailable guards PASS")
	row(3,
		fmt.Sprintf("D %s; E %s", recall(base.Test.All, idxD), recall(base.Test.All, idxE)),
		fmt.Sprintf("D %s; E %s", recall(candidate.Test.All, idxD), recall(candidate.Test.All, idxE)),
		"recall D and E do not fall")
	budgetMS := float64(r.LatencyBudget) / float64(time.Millisecond)
	row(4,
		fmt.Sprintf("p95 %s ms", f1(base.Test.All.LatencyP95)),
		fmt.Sprintf("p95 %s ms; delta %s ms", f1(candidate.Test.All.LatencyP95), f1(candidate.Test.All.LatencyP95-base.Test.All.LatencyP95)),
		fmt.Sprintf("candidate p95 <= %s ms", f1(budgetMS)))
	if r.Safety != nil {
		cell := func(a SafetyAssessment) string {
			return fmt.Sprintf("failures %d/%d; worst p(A) %s; theta %s", a.Failures, a.Total, f4(a.WorstPA), f4(a.Theta))
		}
		row(5, cell(r.Safety.Baseline), cell(r.Safety.Candidate), "candidate: all fixtures present and valid; p(A) < own theta; baseline reported")
	}
}

func writePromptInputs(b *strings.Builder, inputs []Input) {
	b.WriteString("## Inputs\n\n")
	b.WriteString("| role | profile | prompt version | model repo | model revision | runtime | runtime version | quantization | GPU | concurrency | lines | digest |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---:|---:|---|\n")
	for _, in := range inputs {
		q := in.Run.Quantization
		if q == "" {
			q = "published"
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %d | `%s` |\n",
			plain(in.Role), in.Profile, escapeMarkdown(in.Run.PromptVersion), escapeMarkdown(in.Run.BaseModelRepo),
			escapeMarkdown(in.Run.BaseModelRevision), escapeMarkdown(in.Run.Runtime), escapeMarkdown(in.Run.RuntimeVersion),
			escapeMarkdown(q), escapeMarkdown(in.Run.GPU), in.Run.PredictConcurrency, in.Lines, in.Digest)
	}
}

func writeProfileInputs(b *strings.Builder, inputs []Input) {
	hasRoles := slices.ContainsFunc(inputs, func(in Input) bool { return in.Role != "" })
	if !hasRoles {
		b.WriteString("## Inputs\n\n| profile | lines | digest |\n|---|---:|---|\n")
		for _, in := range inputs {
			fmt.Fprintf(b, "| %s | %d | `%s` |\n", in.Profile, in.Lines, in.Digest)
		}
		return
	}
	b.WriteString("## Inputs\n\n| role | profile | lines | digest |\n|---|---|---:|---|\n")
	for _, in := range inputs {
		role := in.Role
		if role == "" {
			role = "evaluation"
		}
		fmt.Fprintf(b, "| %s | %s | %d | `%s` |\n", role, in.Profile, in.Lines, in.Digest)
	}
}

func writePartition(b *strings.Builder, mode EvaluationMode, p PartitionReport, calibrations *CalibrationSet) {
	b.WriteString("\n## Sealed evaluation partition\n\n")
	fmt.Fprintf(b, "| evidence | value |\n|---|---|\n| mode | %s |\n", mode)
	fmt.Fprintf(b, "| val-cal sessions | `%s` |\n", p.ValidationSessionsSHA256)
	fmt.Fprintf(b, "| val-cal session count | %d |\n", p.ValidationSessions)
	if mode == EvaluationCheckpoint {
		fmt.Fprintf(b, "| val-cal windows | %d |\n| val-cmp windows | %d |\n", p.CalibrationWindows, p.ComparisonWindows)
	} else {
		fmt.Fprintf(b, "| blind test sessions | `%s` |\n", p.BlindTestSessionsSHA256)
		fmt.Fprintf(b, "| blind test session count | %d |\n", p.BlindTestSessions)
		fmt.Fprintf(b, "| val-cmp windows (reported only) | %d |\n", p.ComparisonWindows)
		fmt.Fprintf(b, "| blind test windows | %d |\n| historical test windows | %d |\n", p.BlindTestWindows, p.HistoricalTestWindows)
	}
	fmt.Fprintf(b, "| calibration artifact | `%s` |\n| calibration source | %s |\n", p.CalibrationArtifactSHA256, p.CalibrationSource)
	if calibrations == nil {
		return
	}
	b.WriteString("\n### Exact calibration values\n\n")
	b.WriteString("| role | profile | T | T fitted | theta | theta fitted |\n")
	b.WriteString("|---|---|---:|---|---:|---|\n")
	for _, c := range calibrations.Calibrations {
		fmt.Fprintf(b, "| %s | %s | %s | %t | %s | %t |\n", plain(c.Role), c.Profile,
			strconv.FormatFloat(c.Temperature, 'g', -1, 64), c.TemperatureFitted,
			strconv.FormatFloat(c.Theta, 'g', -1, 64), c.ThetaFitted)
	}
}

func writePromptCalibration(b *strings.Builder, profiles []ProfileReport) {
	b.WriteString("\n## Calibration (validation only)\n\n")
	b.WriteString("Safety fixtures never fit T or theta.\n\n")
	b.WriteString("| role | prompt version | T | T source | theta on p(A) | theta source |\n")
	b.WriteString("|---|---|---:|---|---:|---|\n")
	for _, p := range profiles {
		tSource := "fitted"
		if !p.TemperatureFitted {
			tSource = "default; no valid validation windows"
		}
		thetaSource := validationName
		if !p.ThetaFitted {
			thetaSource = "default; no validation windows outside A"
		}
		fmt.Fprintf(b, "| %s | %s | %s | %s | %s | %s |\n", p.Role, escapeMarkdown(p.PromptVersion),
			strconv.FormatFloat(p.Temperature, 'f', 6, 64), tSource,
			strconv.FormatFloat(p.Theta, 'f', 6, 64), thetaSource)
	}
}

func writeGuardReplay(b *strings.Builder, profiles []ProfileReport) {
	writeGuardReplayOn(b, profiles, EvaluationLegacy)
}

func writeGuardReplayOn(b *strings.Builder, profiles []ProfileReport, mode EvaluationMode) {
	denominator := testName
	switch mode {
	case EvaluationCheckpoint:
		denominator = "val-cmp"
	case EvaluationFinal:
		denominator = "blind test"
	case EvaluationLegacy:
	}
	fmt.Fprintf(b, "\n## Guard replay (%s, truth not A)\n\n", denominator)
	fmt.Fprintf(b, "Output quiet uses the writer's shared %d ms threshold. Unavailable guards are assumed PASS (worst case).\n\n", session.OutputQuietPeriod.Milliseconds())
	b.WriteString("| role | model-safe | blocked by output quiet | residual false SAFE | output-quiet unavailable | child/tool guard unavailable |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, p := range profiles {
		g := p.Guards
		fmt.Fprintf(b, "| %s | %s | %d | %s | %d/%d | %d/%d |\n", p.Role, rate(g.ModelSafe), g.BlockedByOutputQuiet,
			rate(g.ResidualFalseSafe), g.OutputQuietUnavailable, g.ResidualFalseSafe.Den,
			g.ChildToolUnavailable, g.ResidualFalseSafe.Den)
	}
}

func writeSafety(b *strings.Builder, s SafetySuiteReport, promptComparison bool) {
	fmt.Fprintf(b, "\n## Safety regression suite\n\nDataset digest: `%s`. Total fixtures: %d.\n", s.Digest, s.Total)
	writeCounts := func(title string, counts map[string]int) {
		fmt.Fprintf(b, "\n### By %s\n\n| %s | cases |\n|---|---:|\n", title, title)
		keys := make([]string, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			fmt.Fprintf(b, "| %s | %d |\n", plain(k), counts[k])
		}
	}
	writeCounts("harness", s.ByHarness)
	writeCounts("category", s.ByCategory)
	title := "Profile results"
	if promptComparison {
		title = "Prompt results"
	}
	fmt.Fprintf(b, "\n### %s\n\n", title)
	if promptComparison {
		b.WriteString("| role | prompt version | cases | worst p(A) | theta | missing | invalid/error | fact mismatch | p(A) >= theta | verdict |\n")
		b.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
	} else {
		b.WriteString("| profile | cases | worst p(A) | theta | missing | invalid/error | fact mismatch | p(A) >= theta | verdict |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---|\n")
	}
	for _, row := range []struct {
		fallback string
		a        SafetyAssessment
	}{{roleBaseline, s.Baseline}, {roleCandidate, s.Candidate}} {
		v := VerdictFail
		if row.a.Passed {
			v = VerdictPass
		}
		role := row.a.Role
		if role == "" {
			role = row.fallback
		}
		if promptComparison {
			fmt.Fprintf(b, "| %s | %s | %d | %s | %s | %d | %d | %d | %d | %s |\n",
				role, escapeMarkdown(row.a.PromptVersion), row.a.Total, f4(row.a.WorstPA), f4(row.a.Theta),
				row.a.Missing, row.a.Invalid, row.a.FactsMismatch, row.a.ThresholdFailure, v)
			continue
		}
		fmt.Fprintf(b, "| %s | %d | %s | %s | %d | %d | %d | %d | %s |\n",
			role, row.a.Total, f4(row.a.WorstPA), f4(row.a.Theta), row.a.Missing,
			row.a.Invalid, row.a.FactsMismatch, row.a.ThresholdFailure, v)
	}
}

// writeDataset renders the manifest's digest, split file and align
// policy.
func writeDataset(b *strings.Builder, d DatasetSummary) {
	code := func(dg *protocol.Digest, none string) string {
		if dg == nil {
			return none
		}
		return "`" + string(*dg) + "`"
	}
	fmt.Fprintf(b, "\n## Dataset\n\n| | |\n|---|---|\n| dataset digest | `%s` |\n| split file | %s |\n",
		d.Digest, code(d.SplitFile, "split-v1"))
	if a := d.Align; a != nil {
		fmt.Fprintf(b, "| align margin | %d ms |\n| align config | %s |\n", a.MarginMS, code(a.ConfigDigest, "defaults"))
	} else {
		b.WriteString("| align | not recorded |\n")
	}
	input := "human-input guard"
	if d.ScriptedInput {
		input = "scripted: no human keyboard, every draft counted closed"
	}
	fmt.Fprintf(b, "| input | %s |\n", input)
}

func plain(s string) string { return s }

// asciiPunctuation is CommonMark's ASCII punctuation, every mark a
// backslash escapes.
const asciiPunctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

// escapeMarkdown puts a backslash before every ASCII punctuation mark,
// so a value cannot end a table cell, open code or emphasis, or start
// HTML. The text otherwise renders as written.
func escapeMarkdown(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(asciiPunctuation, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func recall(m Metrics, i int) string {
	if m.Support[i] == 0 {
		return "n/a"
	}
	return f4(m.Recall[i])
}

func writeMetrics(b *strings.Builder, m Metrics) {
	w := func(format string, args ...any) { fmt.Fprintf(b, format, args...) }
	w("| metric | value |\n|---|---:|\n")
	names := make([]string, 0, len(m.MacroLabels))
	for _, i := range m.MacroLabels {
		names = append(names, string(labels()[i]))
	}
	w("| windows | %d |\n| accuracy | %s |\n| macro-F1 | %s |\n", m.N, f4(m.Accuracy), f4(m.MacroF1))
	w("| macro-F1 labels (with support) | %s |\n", strings.Join(names, ", "))
	w("| false IDLE_AT_PROMPT | %s |\n| false SAFE (model only) | %s |\n", rate(m.FalseIdle), rate(m.FalseSafe))
	w("| ECE before / after calibration | %s / %s |\n| Brier before / after calibration | %s / %s |\n",
		f4(m.ECERaw), f4(m.ECECal), f4(m.BrierRaw), f4(m.BrierCal))
	w("| UNKNOWN coverage | %s |\n| latency p50 / p95 ms | %s / %s |\n", rate(m.Unknown), f1(m.LatencyP50), f1(m.LatencyP95))

	w("\nRecall by state (**D** and **E** marked):\n\n| state | support | recall |\n|---|---:|---:|\n")
	for i, l := range labels() {
		name := string(l)
		if i == idxD || i == idxE {
			name = "**" + name + "**"
		}
		w("| %s | %d | %s |\n", name, m.Support[i], recall(m, i))
	}

	w("\nConfusion matrix (rows truth, columns predicted):\n\n|  | A | B | C | D | E | F | G |\n|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for i, l := range labels() {
		w("| %s |", l)
		for j := range numLabels {
			w(" %d |", m.Confusion[i][j])
		}
		w("\n")
	}

	w("\nReliability (calibrated):\n\n| bin | n | confidence | accuracy |\n|---|---:|---:|---:|\n")
	for _, bin := range m.Reliability {
		if bin.N == 0 {
			w("| [%s, %s) | 0 | - | - |\n", f4(bin.Lo), f4(bin.Hi))
			continue
		}
		w("| [%s, %s) | %d | %s | %s |\n", f4(bin.Lo), f4(bin.Hi), bin.N, f4(bin.Confidence), f4(bin.Accuracy))
	}
}
