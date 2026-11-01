package eval

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/protocol"
)

// ErrDevelopment means a synthetic checkpoint-development input is not
// isolated or sealed well enough to score.
var ErrDevelopment = errors.New("eval: invalid development evidence")

// DevelopmentOptions are deliberately separate from Options: development
// predictions can be scored with frozen calibration but can never enter
// calibration, val-cmp, a manifest gate, or the final gate.
type DevelopmentOptions struct {
	Base, LoRA      []Prediction
	Manifest        ManifestCheck
	DatasetSHA256   protocol.Digest
	Calibrations    *CalibrationSet
	CalibrationsRaw []byte
	Bins            int
}

// DevelopmentProfileReport is one profile scored on the sealed synthetic
// development corpus with its already-frozen calibration.
type DevelopmentProfileReport struct {
	Profile           Profile
	Temperature       float64
	TemperatureFitted bool
	Theta             float64
	ThetaFitted       bool
	Validation        SplitReport
}

// DevelopmentReport is checkpoint-selection evidence only. It has no gate.
type DevelopmentReport struct {
	DatasetSHA256             protocol.Digest
	ValidationSessionsSHA256  protocol.Digest
	CalibrationArtifactSHA256 protocol.Digest
	Bins                      int
	Profiles                  []DevelopmentProfileReport
}

// ScoreDevelopment scores a sealed synthetic corpus without calling either
// calibration fitter. The expected dataset digest is supplied independently
// so the predictions are bound to the intended corpus, not merely a manifest.
func ScoreDevelopment(o DevelopmentOptions) (DevelopmentReport, error) {
	if len(o.Base) == 0 || len(o.LoRA) == 0 {
		return DevelopmentReport{}, fmt.Errorf("%w: base and analyst-lora predictions are required", ErrDevelopment)
	}
	if err := SameWindows(o.Base, o.LoRA); err != nil {
		return DevelopmentReport{}, fmt.Errorf("%w: profiles cover different windows", ErrDevelopment)
	}
	if !o.DatasetSHA256.Valid() {
		return DevelopmentReport{}, fmt.Errorf("%w: expected dataset digest is required", ErrDevelopment)
	}
	if !o.Manifest.Given || o.Manifest.Err != nil {
		return DevelopmentReport{}, fmt.Errorf("%w: a valid development manifest is required", ErrDevelopment)
	}
	if err := o.Manifest.Manifest.Validate(); err != nil {
		return DevelopmentReport{}, fmt.Errorf("%w: development manifest does not validate", ErrDevelopment)
	}
	if o.Manifest.Manifest.DatasetDigest != o.DatasetSHA256 {
		return DevelopmentReport{}, fmt.Errorf("%w: development manifest does not match its expected seal", ErrDevelopment)
	}
	if err := validateDevelopmentManifest(o.Manifest.Manifest); err != nil {
		return DevelopmentReport{}, err
	}
	if err := validateDevelopmentPredictions(o.Base, ProfileBase, o.Manifest.Manifest); err != nil {
		return DevelopmentReport{}, err
	}
	if err := validateDevelopmentPredictions(o.LoRA, ProfileLoRA, o.Manifest.Manifest); err != nil {
		return DevelopmentReport{}, err
	}
	calibrationRaw, err := calibrationInputBytes(o.Calibrations, o.CalibrationsRaw)
	if err != nil {
		return DevelopmentReport{}, fmt.Errorf("%w: %w", ErrDevelopment, err)
	}
	if len(o.Calibrations.Calibrations) != 2 {
		return DevelopmentReport{}, fmt.Errorf("%w: calibration must contain exactly the two adapter profiles", ErrDevelopment)
	}
	if o.Bins == 0 {
		o.Bins = DefaultBins
	}
	if o.Bins < 1 || o.Bins > 1000 {
		return DevelopmentReport{}, fmt.Errorf("%w: bins must be 1-1000", ErrDevelopment)
	}
	r := DevelopmentReport{
		DatasetSHA256:             o.DatasetSHA256,
		ValidationSessionsSHA256:  o.Calibrations.ValidationSessionsSHA256,
		CalibrationArtifactSHA256: protocol.Digest(digest(calibrationRaw)),
		Bins:                      o.Bins,
	}
	for _, input := range []struct {
		profile Profile
		values  []Prediction
	}{{ProfileBase, o.Base}, {ProfileLoRA, o.LoRA}} {
		cal, ok := o.Calibrations.calibration(input.profile, string(input.profile))
		if !ok {
			return DevelopmentReport{}, fmt.Errorf("%w: calibration missing profile %s", ErrDevelopment, input.profile)
		}
		r.Profiles = append(r.Profiles, DevelopmentProfileReport{
			Profile: input.profile, Temperature: cal.Temperature, TemperatureFitted: cal.TemperatureFitted,
			Theta: cal.Theta, ThetaFitted: cal.ThetaFitted,
			Validation: splitReport(input.values, dataset.SplitValidation, cal.Temperature, cal.Theta, o.Bins),
		})
	}
	return r, nil
}

func validateDevelopmentManifest(m dataset.Manifest) error {
	if m.Readiness != nil || len(m.Entries) == 0 {
		return fmt.Errorf("%w: development manifest must be nonempty and carry no readiness result", ErrDevelopment)
	}
	for _, e := range m.Entries {
		if e.Split != dataset.SplitValidation || e.SourceQuality != protocol.SourceQualitySynthetic ||
			e.LabelSource != dataset.LabelSourceSynthetic {
			return fmt.Errorf("%w: development manifest must contain only synthetic validation windows", ErrDevelopment)
		}
	}
	return nil
}

func validateDevelopmentPredictions(ps []Prediction, profile Profile, m dataset.Manifest) error {
	entries := make(map[protocol.ULID]dataset.ManifestEntry, len(m.Entries))
	for _, e := range m.Entries {
		entries[e.WindowID] = e
	}
	seen := make(map[protocol.ULID]bool, len(ps))
	for _, p := range ps {
		e, ok := entries[p.WindowID]
		if !ok || seen[p.WindowID] || p.Profile != profile || !sameFacts(p, e) ||
			replay.GroupDigest(string(p.SessionID)) != e.GroupDigest {
			return fmt.Errorf("%w: development predictions do not exactly match the sealed manifest", ErrDevelopment)
		}
		seen[p.WindowID] = true
	}
	if len(seen) != len(entries) {
		return fmt.Errorf("%w: development predictions do not cover the sealed manifest", ErrDevelopment)
	}
	return nil
}

// Markdown renders a deliberately gate-free development comparison.
func (r DevelopmentReport) Markdown() string {
	var b strings.Builder
	b.WriteString("# Adapter development comparison\n\n")
	b.WriteString("Synthetic development evidence only. Calibration is frozen; this report is not val-cmp and has no gate.\n\n")
	fmt.Fprintf(&b, "| evidence | digest |\n|---|---|\n| development dataset | `%s` |\n", r.DatasetSHA256)
	fmt.Fprintf(&b, "| val-cal session list | `%s` |\n| calibration artifact | `%s` |\n", r.ValidationSessionsSHA256, r.CalibrationArtifactSHA256)
	b.WriteString("\n| profile | T | theta | windows | accuracy | macro-F1 | false IDLE | ECE cal |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|\n")
	profiles := append([]DevelopmentProfileReport(nil), r.Profiles...)
	slices.SortFunc(profiles, func(a, b DevelopmentProfileReport) int { return strings.Compare(string(a.Profile), string(b.Profile)) })
	for _, p := range profiles {
		m := p.Validation.All
		fmt.Fprintf(&b, "| %s | %s | %s | %d | %s | %s | %s | %s |\n",
			p.Profile, strconv.FormatFloat(p.Temperature, 'g', -1, 64), strconv.FormatFloat(p.Theta, 'g', -1, 64),
			m.N, f4(m.Accuracy), f4(m.MacroF1), rate(m.FalseIdle), f4(m.ECECal))
	}
	return b.String()
}
