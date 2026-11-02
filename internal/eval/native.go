package eval

import (
	"slices"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// NativeConcordanceReport is the model-independent comparison between sealed
// native-screen truth and offline Glimmer predictions. It is deliberately not
// a model gate input.
type NativeConcordanceReport struct {
	EvidenceDigest protocol.Digest
	Profiles       []NativeProfileConcordance
}

// NativeProfileConcordance reports one prediction profile without fitting on
// native truth. Validation and test remain separate; train is never inferred.
type NativeProfileConcordance struct {
	Profile    Profile
	Role       string
	Validation NativeSplitConcordance
	Test       NativeSplitConcordance
}

// NativeSplitConcordance is raw argmax agreement for one held-out split.
type NativeSplitConcordance struct {
	Coverage  Rate
	Metrics   Metrics
	ByHarness map[string]NativeGroupConcordance
	ByVersion map[string]NativeGroupConcordance
}

// NativeGroupConcordance is the compact coverage and agreement breakdown of
// one harness or exact harness version.
type NativeGroupConcordance struct {
	Coverage Rate
	Metrics  Metrics
}

type nativeProfileInput struct {
	profile Profile
	role    string
	preds   []Prediction
}

func nativeConcordance(
	m dataset.Manifest,
	inputs ...nativeProfileInput,
) *NativeConcordanceReport {
	if m.NativeLabels == nil {
		return nil
	}
	report := &NativeConcordanceReport{EvidenceDigest: m.NativeLabels.Digest}
	for _, input := range inputs {
		report.Profiles = append(report.Profiles, NativeProfileConcordance{
			Profile: input.profile,
			Role:    input.role,
			Validation: nativeSplitConcordance(
				nativePredictions(m, input.preds, dataset.SplitValidation),
			),
			Test: nativeSplitConcordance(
				nativePredictions(m, input.preds, dataset.SplitTest),
			),
		})
	}
	return report
}

func nativePredictions(
	m dataset.Manifest,
	preds []Prediction,
	split dataset.Split,
) []Prediction {
	byID := make(map[protocol.ULID]Prediction, len(preds))
	for _, prediction := range preds {
		byID[prediction.WindowID] = prediction
	}
	var out []Prediction
	for _, entry := range m.Entries {
		if entry.Split != split || entry.LabelSource != dataset.LabelSourceNative {
			continue
		}
		truth, ok := labelIndex(entry.Label)
		if !ok {
			continue
		}
		prediction, found := byID[entry.WindowID]
		factsMatch := found && prediction.Split == entry.Split && prediction.Truth == truth &&
			prediction.Harness == entry.Harness && prediction.HarnessVersion == entry.HarnessVersion &&
			prediction.SourceQuality == entry.SourceQuality
		if !factsMatch {
			prediction = Prediction{Invalid: true}
		}
		// Truth and grouping facts always come from the sealed manifest. The
		// prediction file supplies only the model result and its validity.
		prediction.WindowID = entry.WindowID
		prediction.Split = entry.Split
		prediction.Harness = entry.Harness
		prediction.HarnessVersion = entry.HarnessVersion
		prediction.SourceQuality = entry.SourceQuality
		prediction.Truth = truth
		out = append(out, prediction)
	}
	return out
}

func nativeSplitConcordance(preds []Prediction) NativeSplitConcordance {
	group := func(key func(Prediction) string) map[string]NativeGroupConcordance {
		grouped := map[string][]Prediction{}
		for _, prediction := range preds {
			name := key(prediction)
			grouped[name] = append(grouped[name], prediction)
		}
		out := make(map[string]NativeGroupConcordance, len(grouped))
		for name, values := range grouped {
			out[name] = NativeGroupConcordance{
				Coverage: predictionCoverage(values),
				Metrics:  Score(values, 1, 0.5, DefaultBins),
			}
		}
		return out
	}
	return NativeSplitConcordance{
		Coverage:  predictionCoverage(preds),
		Metrics:   Score(preds, 1, 0.5, DefaultBins),
		ByHarness: group(func(p Prediction) string { return string(p.Harness) }),
		ByVersion: group(func(p Prediction) string { return string(p.Harness) + " " + p.HarnessVersion }),
	}
}

func predictionCoverage(preds []Prediction) Rate {
	coverage := Rate{Den: len(preds)}
	for _, prediction := range preds {
		if !prediction.Invalid {
			coverage.Num++
		}
	}
	return coverage
}

// withoutNativeTruth leaves the ordinary calibration and model gate untouched
// by optional native-screen targets. With no native policy it returns the
// original slice, preserving the existing path exactly.
func withoutNativeTruth(preds []Prediction, mc ManifestCheck) []Prediction {
	if !mc.Given || mc.Err != nil || mc.Manifest.NativeLabels == nil {
		return preds
	}
	ids := make(map[protocol.ULID]bool, len(mc.Manifest.Entries))
	for _, entry := range mc.Manifest.Entries {
		if entry.LabelSource == dataset.LabelSourceNative {
			ids[entry.WindowID] = true
		}
	}
	out := slices.DeleteFunc(slices.Clone(preds), func(prediction Prediction) bool {
		return ids[prediction.WindowID]
	})
	return out
}
