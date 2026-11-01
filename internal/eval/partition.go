package eval

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/dataset/replay"
	"github.com/smellak/gifford/internal/protocol"
)

// EvaluationMode says which evidence an evaluation is allowed to use.
type EvaluationMode string

const (
	// EvaluationLegacy preserves the v1 validation/test report.
	EvaluationLegacy EvaluationMode = "legacy"
	// EvaluationCheckpoint fits on val-cal and compares checkpoints on val-cmp.
	EvaluationCheckpoint EvaluationMode = "checkpoint"
	// EvaluationFinal uses sealed calibrations and gates on blind test sessions.
	EvaluationFinal EvaluationMode = "final"

	calibrationSchemaVersion = 1
)

// ErrCalibration means a validation partition or calibration artifact is not
// complete and trustworthy enough to use.
var ErrCalibration = errors.New("eval: invalid calibration evidence")

// SessionSet is a content-addressed list of opaque recording sessions. The
// digest is over the exact list-file bytes; session ids are never rendered.
type SessionSet struct {
	ids    map[protocol.OpaqueID]struct{}
	digest protocol.Digest
}

// NewSessionSet validates a parsed session list and its byte digest.
func NewSessionSet(ids []protocol.OpaqueID, digest protocol.Digest) (SessionSet, error) {
	if !digest.Valid() {
		return SessionSet{}, fmt.Errorf("%w: session list digest is not sha256", ErrCalibration)
	}
	set := SessionSet{ids: make(map[protocol.OpaqueID]struct{}, len(ids)), digest: digest}
	for _, id := range ids {
		if !id.Valid() {
			return SessionSet{}, fmt.Errorf("%w: session list contains an invalid id", ErrCalibration)
		}
		if _, exists := set.ids[id]; exists {
			return SessionSet{}, fmt.Errorf("%w: session list contains a duplicate", ErrCalibration)
		}
		set.ids[id] = struct{}{}
	}
	if len(set.ids) == 0 {
		return SessionSet{}, fmt.Errorf("%w: session list is empty", ErrCalibration)
	}
	return set, nil
}

// Digest returns the digest of the exact list-file bytes.
func (s SessionSet) Digest() protocol.Digest { return s.digest }

// Len returns the number of sessions without exposing their ids.
func (s SessionSet) Len() int { return len(s.ids) }

func (s SessionSet) contains(id protocol.OpaqueID) bool {
	_, ok := s.ids[id]
	return ok
}

func (s SessionSet) given() bool { return len(s.ids) > 0 || s.digest != "" }

// SealedCalibration is one profile's fixed T and theta.
type SealedCalibration struct {
	Profile           Profile `json:"profile"`
	Role              string  `json:"role"`
	Temperature       float64 `json:"temperature"`
	TemperatureFitted bool    `json:"temperature_fitted"`
	Theta             float64 `json:"theta"`
	ThetaFitted       bool    `json:"theta_fitted"`
}

// CalibrationSet is the small artifact sealed after checkpoint selection and
// consumed by the final gate. ValidationSessionsSHA256 binds it to val-cal.
type CalibrationSet struct {
	SchemaVersion            int64               `json:"schema_version"`
	ValidationSessionsSHA256 protocol.Digest     `json:"validation_sessions_sha256"`
	Calibrations             []SealedCalibration `json:"calibrations"`
}

// ParseCalibrationSet strictly parses one sealed calibration artifact.
func ParseCalibrationSet(raw []byte) (CalibrationSet, error) {
	if !utf8.Valid(raw) || noDuplicateKeys(raw) != nil {
		return CalibrationSet{}, fmt.Errorf("%w: calibration is not one strict JSON object", ErrCalibration)
	}
	if !calibrationMembersPresent(raw) {
		return CalibrationSet{}, fmt.Errorf("%w: calibration members are missing or unknown", ErrCalibration)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var set CalibrationSet
	if err := dec.Decode(&set); err != nil {
		return CalibrationSet{}, fmt.Errorf("%w: calibration is not one strict JSON object", ErrCalibration)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return CalibrationSet{}, fmt.Errorf("%w: trailing calibration data", ErrCalibration)
	}
	if err := set.validate(); err != nil {
		return CalibrationSet{}, err
	}
	return set, nil
}

func calibrationMembersPresent(raw []byte) bool {
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil || !exactKeys(top, "schema_version", "validation_sessions_sha256", "calibrations") {
		return false
	}
	var calibrations []json.RawMessage
	if json.Unmarshal(top["calibrations"], &calibrations) != nil || len(calibrations) == 0 {
		return false
	}
	for _, rawCalibration := range calibrations {
		var fields map[string]json.RawMessage
		if json.Unmarshal(rawCalibration, &fields) != nil || !exactKeys(fields,
			"profile", "role", "temperature", "temperature_fitted", "theta", "theta_fitted") {
			return false
		}
	}
	return true
}

func exactKeys(fields map[string]json.RawMessage, want ...string) bool {
	if len(fields) != len(want) {
		return false
	}
	for _, key := range want {
		if _, ok := fields[key]; !ok {
			return false
		}
	}
	return true
}

// Marshal returns the stable bytes whose digest the report records.
func (s CalibrationSet) Marshal() ([]byte, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("%w: encode calibration", ErrCalibration)
	}
	return append(raw, '\n'), nil
}

func (s CalibrationSet) validate() error {
	if s.SchemaVersion != calibrationSchemaVersion {
		return fmt.Errorf("%w: calibration schema version", ErrCalibration)
	}
	if !s.ValidationSessionsSHA256.Valid() {
		return fmt.Errorf("%w: validation session digest", ErrCalibration)
	}
	if len(s.Calibrations) == 0 {
		return fmt.Errorf("%w: no profile calibration", ErrCalibration)
	}
	seen := map[string]bool{}
	for _, c := range s.Calibrations {
		key := string(c.Profile) + "\x00" + c.Role
		switch {
		case !c.Profile.Valid() || c.Role == "" || len(c.Role) > 64 || !utf8.ValidString(c.Role):
			return fmt.Errorf("%w: calibration profile or role", ErrCalibration)
		case seen[key]:
			return fmt.Errorf("%w: duplicate profile calibration", ErrCalibration)
		case math.IsNaN(c.Temperature) || math.IsInf(c.Temperature, 0) || c.Temperature < tMin || c.Temperature > tMax:
			return fmt.Errorf("%w: calibration temperature", ErrCalibration)
		case math.IsNaN(c.Theta) || math.IsInf(c.Theta, 0) || c.Theta < 0 || c.Theta > math.Nextafter(1, math.Inf(1)):
			return fmt.Errorf("%w: calibration theta", ErrCalibration)
		case !c.TemperatureFitted && c.Temperature != 1:
			return fmt.Errorf("%w: default temperature does not equal 1", ErrCalibration)
		case !c.ThetaFitted && c.Theta != DefaultTheta:
			return fmt.Errorf("%w: default theta does not match", ErrCalibration)
		}
		seen[key] = true
	}
	return nil
}

func (s CalibrationSet) calibration(profile Profile, role string) (SealedCalibration, bool) {
	for _, c := range s.Calibrations {
		if c.Profile == profile && c.Role == role {
			return c, true
		}
	}
	return SealedCalibration{}, false
}

type evaluationParts struct {
	calibration []Prediction
	comparison  []Prediction
	blind       []Prediction
	historical  []Prediction
}

// validateSessionEvidence binds both private session lists to public manifest
// group digests and their declared splits. It returns the group sets used to
// partition predictions without rendering a session id.
func validateSessionEvidence(mc ManifestCheck, valCal, blind SessionSet, mode EvaluationMode) (map[protocol.Digest]bool, map[protocol.Digest]bool, error) {
	if !mc.Given || mc.Err != nil {
		return nil, nil, fmt.Errorf("%w: a valid sealed manifest is required", ErrCalibration)
	}
	if err := mc.Manifest.Validate(); err != nil {
		return nil, nil, fmt.Errorf("%w: manifest does not validate", ErrCalibration)
	}
	if !valCal.given() {
		return nil, nil, fmt.Errorf("%w: val-cal session list is required", ErrCalibration)
	}
	if mode == EvaluationFinal && !blind.given() {
		return nil, nil, fmt.Errorf("%w: blind test session list is required", ErrCalibration)
	}
	manifestGroups := map[protocol.Digest]dataset.Split{}
	for _, e := range mc.Manifest.Entries {
		manifestGroups[e.GroupDigest] = e.Split
	}
	groups := func(set SessionSet, want dataset.Split, name string) (map[protocol.Digest]bool, error) {
		out := make(map[protocol.Digest]bool, set.Len())
		for id := range set.ids {
			group := replay.GroupDigest(string(id))
			if split, ok := manifestGroups[group]; !ok || split != want {
				return nil, fmt.Errorf("%w: %s session is absent from its manifest split", ErrCalibration, name)
			}
			if out[group] {
				return nil, fmt.Errorf("%w: %s sessions resolve to one group", ErrCalibration, name)
			}
			out[group] = true
		}
		return out, nil
	}
	calGroups, err := groups(valCal, dataset.SplitValidation, "val-cal")
	if err != nil {
		return nil, nil, err
	}
	blindGroups := map[protocol.Digest]bool{}
	if mode == EvaluationFinal {
		blindGroups, err = groups(blind, dataset.SplitTest, "blind test")
		if err != nil {
			return nil, nil, err
		}
	}
	for id := range valCal.ids {
		if blind.contains(id) || blindGroups[replay.GroupDigest(string(id))] {
			return nil, nil, fmt.Errorf("%w: val-cal and blind test overlap", ErrCalibration)
		}
	}
	return calGroups, blindGroups, nil
}

func partitionPredictions(ps []Prediction, m dataset.Manifest, calGroups, blindGroups map[protocol.Digest]bool, mode EvaluationMode) (evaluationParts, error) {
	entries := make(map[protocol.ULID]dataset.ManifestEntry, len(m.Entries))
	want := map[protocol.ULID]bool{}
	for _, e := range m.Entries {
		entries[e.WindowID] = e
		if (mode == EvaluationCheckpoint && e.Split == dataset.SplitValidation) ||
			(mode == EvaluationFinal && (e.Split == dataset.SplitValidation || e.Split == dataset.SplitTest)) {
			want[e.WindowID] = true
		}
	}
	seen := make(map[protocol.ULID]bool, len(want))
	var out evaluationParts
	for _, p := range ps {
		e, ok := entries[p.WindowID]
		if !ok || !sameFacts(p, e) || replay.GroupDigest(string(p.SessionID)) != e.GroupDigest {
			return evaluationParts{}, fmt.Errorf("%w: predictions disagree with the manifest session evidence", ErrCalibration)
		}
		switch mode {
		case EvaluationLegacy:
			return evaluationParts{}, fmt.Errorf("%w: legacy mode cannot use session partitions", ErrCalibration)
		case EvaluationCheckpoint:
			if p.Split != dataset.SplitValidation {
				continue
			}
			if p.SourceQuality == protocol.SourceQualitySynthetic || e.LabelSource == dataset.LabelSourceSynthetic {
				return evaluationParts{}, fmt.Errorf("%w: synthetic development predictions require the separate development scorer", ErrCalibration)
			}
			seen[p.WindowID] = true
			if calGroups[e.GroupDigest] {
				out.calibration = append(out.calibration, p)
			} else {
				out.comparison = append(out.comparison, p)
			}
		case EvaluationFinal:
			if p.Split == dataset.SplitValidation {
				if p.SourceQuality == protocol.SourceQualitySynthetic || e.LabelSource == dataset.LabelSourceSynthetic {
					return evaluationParts{}, fmt.Errorf("%w: synthetic development predictions require the separate development scorer", ErrCalibration)
				}
				seen[p.WindowID] = true
				if !calGroups[e.GroupDigest] {
					out.comparison = append(out.comparison, p)
				}
				continue
			}
			if p.Split != dataset.SplitTest {
				continue
			}
			seen[p.WindowID] = true
			if blindGroups[e.GroupDigest] {
				out.blind = append(out.blind, p)
			} else {
				out.historical = append(out.historical, p)
			}
		}
	}
	if len(seen) != len(want) {
		return evaluationParts{}, fmt.Errorf("%w: predictions do not cover the required manifest split", ErrCalibration)
	}
	for id := range want {
		if !seen[id] {
			return evaluationParts{}, fmt.Errorf("%w: predictions do not cover the required manifest split", ErrCalibration)
		}
	}
	switch mode {
	case EvaluationLegacy:
		return evaluationParts{}, fmt.Errorf("%w: legacy mode cannot use session partitions", ErrCalibration)
	case EvaluationCheckpoint:
		if len(out.calibration) == 0 || len(out.comparison) == 0 {
			return evaluationParts{}, fmt.Errorf("%w: val-cal and val-cmp must both contain windows", ErrCalibration)
		}
	case EvaluationFinal:
		if len(out.comparison) == 0 || len(out.blind) == 0 || len(out.historical) == 0 {
			return evaluationParts{}, fmt.Errorf("%w: val-cmp, blind, and historical test must all contain windows", ErrCalibration)
		}
	}
	return out, nil
}

// blindTestReadiness reuses the manifest preparation rules and thresholds,
// replacing only the complete test split with the fixed blind subset. This
// keeps the full preparation result separate while preventing historical test
// windows from lending coverage to the final gate.
func blindTestReadiness(m dataset.Manifest, blindGroups map[protocol.Digest]bool) dataset.Readiness {
	entries := make([]dataset.ManifestEntry, 0, len(m.Entries))
	for _, e := range m.Entries {
		if e.Split != dataset.SplitTest || blindGroups[e.GroupDigest] {
			entries = append(entries, e)
		}
	}
	thresholds := dataset.DefaultReadinessThresholds()
	if m.Readiness != nil {
		thresholds = m.Readiness.Thresholds
	}
	return dataset.CheckReadiness(entries, thresholds)
}

func expectedCalibrations(profiles []ProfileReport) []SealedCalibration {
	out := make([]SealedCalibration, 0, len(profiles))
	for _, p := range profiles {
		out = append(out, SealedCalibration{
			Profile: p.Profile, Role: p.Role, Temperature: p.Temperature,
			TemperatureFitted: p.TemperatureFitted, Theta: p.Theta, ThetaFitted: p.ThetaFitted,
		})
	}
	slices.SortFunc(out, func(a, b SealedCalibration) int {
		if a.Role < b.Role {
			return -1
		}
		if a.Role > b.Role {
			return 1
		}
		return 0
	})
	return out
}

func partitionInputsGiven(o Options) bool {
	return o.ValidationCalibrationSessions.given() || o.BlindTestSessions.given() ||
		o.SealedCalibrations != nil || len(o.SealedCalibrationsRaw) > 0
}

func evaluationMode(o Options) (EvaluationMode, error) {
	valCal := o.ValidationCalibrationSessions.given()
	blind := o.BlindTestSessions.given()
	sealed := o.SealedCalibrations != nil
	switch {
	case !valCal && !blind && !sealed && len(o.SealedCalibrationsRaw) == 0:
		return EvaluationLegacy, nil
	case valCal && !blind && !sealed && len(o.SealedCalibrationsRaw) == 0:
		if o.LoRA == nil && o.Candidate == nil {
			return "", fmt.Errorf("%w: checkpoint comparison needs base and candidate profiles", ErrCalibration)
		}
		return EvaluationCheckpoint, nil
	case valCal && blind && sealed:
		if o.LoRA == nil && o.Candidate == nil {
			return "", fmt.Errorf("%w: final gate needs base and candidate profiles", ErrCalibration)
		}
		if _, err := calibrationInputBytes(o.SealedCalibrations, o.SealedCalibrationsRaw); err != nil {
			return "", err
		}
		return EvaluationFinal, nil
	default:
		return "", fmt.Errorf("%w: incomplete checkpoint/final mode inputs", ErrCalibration)
	}
}

func calibrationInputBytes(set *CalibrationSet, raw []byte) ([]byte, error) {
	if set == nil {
		return nil, fmt.Errorf("%w: sealed calibration is required", ErrCalibration)
	}
	if err := set.validate(); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return set.Marshal()
	}
	parsed, err := ParseCalibrationSet(raw)
	if err != nil || parsed.SchemaVersion != set.SchemaVersion ||
		parsed.ValidationSessionsSHA256 != set.ValidationSessionsSHA256 ||
		!slices.Equal(parsed.Calibrations, set.Calibrations) {
		return nil, fmt.Errorf("%w: sealed calibration bytes do not match parsed values", ErrCalibration)
	}
	return raw, nil
}

func finishCalibrationReport(r *Report, o Options, parts evaluationParts) error {
	if r.Mode == EvaluationLegacy {
		return nil
	}
	set := CalibrationSet{
		SchemaVersion:            calibrationSchemaVersion,
		ValidationSessionsSHA256: o.ValidationCalibrationSessions.Digest(),
		Calibrations:             expectedCalibrations(r.Profiles),
	}
	raw, err := set.Marshal()
	if err != nil {
		return err
	}
	source := "generated from val-cal"
	switch r.Mode {
	case EvaluationLegacy:
		return nil
	case EvaluationCheckpoint:
	case EvaluationFinal:
		raw, err = sealedCalibrationBytes(o, set)
		if err != nil {
			return err
		}
		source = "sealed; report values verified exactly"
	}
	r.Calibrations = &set
	r.Partition = &PartitionReport{
		ValidationSessionsSHA256:  o.ValidationCalibrationSessions.Digest(),
		ValidationSessions:        o.ValidationCalibrationSessions.Len(),
		CalibrationArtifactSHA256: protocol.Digest(digest(raw)),
		CalibrationSource:         source,
	}
	if r.Mode == EvaluationCheckpoint {
		r.Partition.CalibrationWindows = len(parts.calibration)
		r.Partition.ComparisonWindows = len(parts.comparison)
	} else {
		r.Partition.ComparisonWindows = len(parts.comparison)
		r.Partition.BlindTestSessionsSHA256 = o.BlindTestSessions.Digest()
		r.Partition.BlindTestSessions = o.BlindTestSessions.Len()
		r.Partition.BlindTestWindows = len(parts.blind)
		r.Partition.HistoricalTestWindows = len(parts.historical)
	}
	return nil
}

func sealedCalibrationBytes(o Options, report CalibrationSet) ([]byte, error) {
	sealed := append([]SealedCalibration(nil), o.SealedCalibrations.Calibrations...)
	slices.SortFunc(sealed, func(a, b SealedCalibration) int {
		if a.Role < b.Role {
			return -1
		}
		if a.Role > b.Role {
			return 1
		}
		return 0
	})
	if !slices.Equal(sealed, report.Calibrations) {
		return nil, fmt.Errorf("%w: report calibration does not exactly match sealed values", ErrCalibration)
	}
	if len(o.SealedCalibrationsRaw) > 0 {
		return o.SealedCalibrationsRaw, nil
	}
	raw, err := o.SealedCalibrations.Marshal()
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// CalibrationJSON returns the stable calibration artifact produced by a
// checkpoint comparison. A final or legacy report never manufactures one.
func (r Report) CalibrationJSON() ([]byte, error) {
	if r.Mode != EvaluationCheckpoint || r.Calibrations == nil {
		return nil, fmt.Errorf("%w: report did not fit a checkpoint calibration", ErrCalibration)
	}
	return r.Calibrations.Marshal()
}
