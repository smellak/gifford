package eval

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
)

// ErrPredictions means a predictions file is not valid; nothing is
// computed from it.
var ErrPredictions = errors.New("eval: invalid predictions")

// Profile names the model a predictions file came from.
type Profile string

// Profile values.
const (
	ProfileBase Profile = "base"
	ProfileLoRA Profile = "analyst-lora"
)

// Valid reports a known profile.
func (p Profile) Valid() bool { return p == ProfileBase || p == ProfileLoRA }

const (
	numLabels      = 7
	sumTolerance   = 1e-6
	minValidMass   = 0.5
	maxLineBytes   = 1 << 20
	maxPredictions = 1 << 22
)

// labels are A–G in contract order; index i of a distribution is
// labels[i].
func labels() [numLabels]dataset.Label {
	return [numLabels]dataset.Label{
		dataset.LabelA, dataset.LabelB, dataset.LabelC, dataset.LabelD,
		dataset.LabelE, dataset.LabelF, dataset.LabelG,
	}
}

func labelIndex(l dataset.Label) (int, bool) {
	for i, x := range labels() {
		if x == l {
			return i, true
		}
	}
	return 0, false
}

const (
	idxA = 0
	idxD = 3
	idxE = 4
	idxG = 6
)

// Prediction is one validated line.
type Prediction struct {
	WindowID       protocol.ULID
	Split          dataset.Split
	Harness        protocol.Harness
	HarnessVersion string
	SessionID      protocol.OpaqueID
	SourceQuality  protocol.SourceQuality
	// Truth is the index of the true label in labels().
	Truth int
	// Probs are the uncalibrated probabilities over A–G.
	Probs [numLabels]float64
	// ValidMass is the share of the model's output that named a label;
	// below 0.5 the line is Invalid and counts as UNKNOWN.
	ValidMass float64
	Invalid   bool
	LatencyMS float64
	Profile   Profile
	// ErrorClass names why an invalid line has no prediction; empty
	// when the model answered.
	ErrorClass ErrorClass
}

type rawPrediction struct {
	WindowID       string                     `json:"window_id"`
	Split          string                     `json:"split"`
	Harness        string                     `json:"harness"`
	HarnessVersion string                     `json:"harness_version"`
	SessionID      string                     `json:"session_id"`
	SourceQuality  string                     `json:"source_quality"`
	Label          string                     `json:"label"`
	Probabilities  map[string]json.RawMessage `json:"probabilities"`
	ValidMass      json.RawMessage            `json:"valid_mass"`
	Invalid        *bool                      `json:"invalid"`
	LatencyMS      json.RawMessage            `json:"latency_ms"`
	Profile        string                     `json:"profile"`
	ErrorClass     json.RawMessage            `json:"error_class"`
}

// ReadPredictions reads and validates a whole predictions file of one
// profile. Any bad line fails the whole file; the error names the line
// number and the field, never a value.
func ReadPredictions(r io.Reader, want Profile) ([]Prediction, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxLineBytes)
	seen := map[protocol.ULID]bool{}
	var out []Prediction
	line := 0
	for sc.Scan() {
		line++
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		if len(out) == maxPredictions {
			return nil, fmt.Errorf("%w: too many lines", ErrPredictions)
		}
		p, err := parseLine(b)
		if err != nil {
			return nil, fmt.Errorf("%w: line %d: %s", ErrPredictions, line, err.Error())
		}
		if p.Profile != want {
			return nil, fmt.Errorf("%w: line %d: profile is not %s", ErrPredictions, line, want)
		}
		if seen[p.WindowID] {
			return nil, fmt.Errorf("%w: line %d: window_id repeated", ErrPredictions, line)
		}
		seen[p.WindowID] = true
		out = append(out, p)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPredictions, err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: no predictions", ErrPredictions)
	}
	return out, nil
}

// fieldErr names a field; it never quotes the value.
type fieldErr string

func (e fieldErr) Error() string { return string(e) }

func parseLine(b []byte) (Prediction, error) {
	// A repeated key would let the last value win unseen.
	if err := noDuplicateKeys(b); err != nil {
		return Prediction{}, fieldErr("repeated key")
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var raw rawPrediction
	if err := dec.Decode(&raw); err != nil {
		return Prediction{}, fieldErr("not one well-formed object")
	}
	if dec.More() {
		return Prediction{}, fieldErr("trailing data")
	}
	p := Prediction{
		WindowID:       protocol.ULID(raw.WindowID),
		Split:          dataset.Split(raw.Split),
		Harness:        protocol.Harness(raw.Harness),
		HarnessVersion: raw.HarnessVersion,
		SessionID:      protocol.OpaqueID(raw.SessionID),
		SourceQuality:  protocol.SourceQuality(raw.SourceQuality),
		Profile:        Profile(raw.Profile),
	}
	switch {
	case !p.WindowID.Valid():
		return Prediction{}, fieldErr("window_id")
	case p.Split != dataset.SplitValidation && p.Split != dataset.SplitTest:
		// Train lines would be counted in the inputs but scored nowhere.
		return Prediction{}, fieldErr("split must be validation or test")
	case !inEnum(string(p.Harness), protocol.Harness("").EnumValues()):
		return Prediction{}, fieldErr("harness")
	case !session.ValidHarnessVersion(p.HarnessVersion):
		return Prediction{}, fieldErr("harness_version")
	case !p.SessionID.Valid():
		return Prediction{}, fieldErr("session_id")
	case !inEnum(string(p.SourceQuality), protocol.SourceQuality("").EnumValues()):
		return Prediction{}, fieldErr("source_quality")
	case !p.Profile.Valid():
		return Prediction{}, fieldErr("profile")
	}
	truth, ok := labelIndex(dataset.Label(raw.Label))
	if !ok {
		return Prediction{}, fieldErr("label")
	}
	p.Truth = truth
	if len(raw.Probabilities) != numLabels {
		return Prediction{}, fieldErr("probabilities must name A-G exactly")
	}
	sum := 0.0
	for i, l := range labels() {
		n, ok := raw.Probabilities[string(l)]
		if !ok {
			return Prediction{}, fieldErr("probabilities must name A-G exactly")
		}
		v, err := unit(n)
		if err != nil {
			return Prediction{}, fieldErr("probabilities." + string(l))
		}
		p.Probs[i] = v
		sum += v
	}
	if math.Abs(sum-1) > sumTolerance {
		return Prediction{}, fieldErr("probabilities do not sum to 1")
	}
	mass, err := unit(raw.ValidMass)
	if err != nil {
		return Prediction{}, fieldErr("valid_mass")
	}
	p.ValidMass = mass
	if raw.Invalid == nil || *raw.Invalid != (mass < minValidMass) {
		return Prediction{}, fieldErr("invalid must be true exactly when valid_mass < 0.5")
	}
	p.Invalid = *raw.Invalid
	lat, err := finite(raw.LatencyMS)
	if err != nil || lat < 0 {
		return Prediction{}, fieldErr("latency_ms")
	}
	p.LatencyMS = lat
	if raw.ErrorClass != nil {
		// Optional, but never null: a line without a class omits it.
		var c string
		if raw.ErrorClass[0] != '"' || json.Unmarshal(raw.ErrorClass, &c) != nil || !ErrorClass(c).Valid() {
			return Prediction{}, fieldErr("error_class")
		}
		if !p.Invalid || p.ValidMass != 0 {
			return Prediction{}, fieldErr("error_class is only for an invalid line with valid_mass 0")
		}
		p.ErrorClass = ErrorClass(c)
	}
	return p, nil
}

// finite parses a JSON number that must be a finite float64: a JSON
// number literal, not a string or null, and not NaN, Inf or out of range,
// which ParseFloat alone would let through.
func finite(n json.RawMessage) (float64, error) {
	var num json.Number
	if len(n) == 0 || n[0] == '"' || json.Unmarshal(n, &num) != nil {
		return 0, errors.New("not a number")
	}
	v, err := strconv.ParseFloat(string(num), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, errors.New("not a finite number")
	}
	return v, nil
}

func unit(n json.RawMessage) (float64, error) {
	v, err := finite(n)
	if err != nil || v < 0 || v > 1 {
		return 0, errors.New("not in [0, 1]")
	}
	return v, nil
}

func inEnum(v string, values []string) bool {
	for _, x := range values {
		if x == v {
			return true
		}
	}
	return false
}

// SameWindows fails closed unless both profiles cover exactly the same
// windows, each with the same split, label, harness, version, session and
// source quality.
func SameWindows(base, lora []Prediction) error {
	if len(base) != len(lora) {
		return fmt.Errorf("%w: the two profiles cover different windows", ErrPredictions)
	}
	byID := make(map[protocol.ULID]Prediction, len(base))
	for _, p := range base {
		byID[p.WindowID] = p
	}
	for _, q := range lora {
		p, ok := byID[q.WindowID]
		if !ok {
			return fmt.Errorf("%w: the two profiles cover different windows", ErrPredictions)
		}
		if p.Split != q.Split || p.Truth != q.Truth || p.Harness != q.Harness || p.HarnessVersion != q.HarnessVersion ||
			p.SessionID != q.SessionID || p.SourceQuality != q.SourceQuality {
			return fmt.Errorf("%w: the two profiles disagree on a window's facts", ErrPredictions)
		}
	}
	return nil
}

// noDuplicateKeys walks one JSON value and refuses an object that names a
// key twice, at any depth.
func noDuplicateKeys(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	var walk func() error
	walk = func() error {
		tok, err := dec.Token()
		if err != nil {
			return err //nolint:wrapcheck // only the fact matters
		}
		d, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch d {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return err //nolint:wrapcheck // only the fact matters
				}
				key, _ := k.(string)
				if seen[key] {
					return errors.New("repeated key")
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for dec.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		default:
			return errors.New("unexpected delimiter")
		}
		_, err = dec.Token() // the closing delimiter
		return err           //nolint:wrapcheck // only the fact matters
	}
	return walk()
}
