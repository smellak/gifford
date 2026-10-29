package eval

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// ErrPredict means a predict run was refused or aborted; it writes no
// predictions.
var ErrPredict = errors.New("eval: predict run failed")

// Inferrer runs one model contract. *gateway.Gateway satisfies it.
type Inferrer interface {
	Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error)
}

// WindowLoader returns the window a manifest entry names, read with
// dataset.ParseWindow.
type WindowLoader func(id protocol.ULID) (dataset.Window, error)

// ErrorClass names why a window got no prediction. It is a fixed word,
// never model or window text.
type ErrorClass string

// ErrorClass values.
const (
	ClassInvalidRequest   ErrorClass = "invalid_request"
	ClassRedactionFailed  ErrorClass = "redaction_failed"
	ClassTimeout          ErrorClass = "timeout"
	ClassInvalidOutput    ErrorClass = "invalid_output"
	ClassCircuitOpen      ErrorClass = "circuit_open"
	ClassAgentBusy        ErrorClass = "agent_busy"
	ClassRecordFailed     ErrorClass = "record_failed"
	ClassCallFailed       ErrorClass = "call_failed"
	ClassStatusNotOK      ErrorClass = "status_not_ok"
	ClassBadProbabilities ErrorClass = "bad_probabilities"
)

// ErrorClasses lists every class, in a fixed order.
func ErrorClasses() []ErrorClass {
	return []ErrorClass{
		ClassInvalidRequest, ClassRedactionFailed, ClassTimeout, ClassInvalidOutput, ClassCircuitOpen,
		ClassAgentBusy, ClassRecordFailed, ClassCallFailed, ClassStatusNotOK, ClassBadProbabilities,
	}
}

// Valid reports a known class.
func (c ErrorClass) Valid() bool { return slices.Contains(ErrorClasses(), c) }

// DefaultConcurrency bounds the calls in flight of a predict run.
const DefaultConcurrency = 4

// maxConcurrency bounds the parallel-call limit.
const maxConcurrency = 64

// probTolerance is how far from 1 the gateway lets a distribution sum
// (protocol.InferResult.Validate): within sumTolerance it passes as is,
// beyond it and within this it is rescaled to sum to 1.
const probTolerance = 1e-3

// PredictOptions bound a predict run.
type PredictOptions struct {
	Profile Profile
	// Concurrency is the most calls in flight, 1–64; zero means
	// DefaultConcurrency.
	Concurrency int
}

// PredictSummary counts a run, numbers only.
type PredictSummary struct {
	Windows int
	Valid   int
	// Failed counts the invalid lines by class.
	Failed map[ErrorClass]int
	// LatencyP50 and LatencyP95 are over the calls that answered, in ms.
	LatencyP50, LatencyP95 float64
}

// job is one window to predict: its line's facts and its request.
type job struct {
	line    predictionLine
	payload []byte
}

// predictionLine is one line of a predictions file, in its field order.
type predictionLine struct {
	WindowID       protocol.ULID          `json:"window_id"`
	Split          dataset.Split          `json:"split"`
	Harness        protocol.Harness       `json:"harness"`
	HarnessVersion string                 `json:"harness_version"`
	SessionID      protocol.OpaqueID      `json:"session_id"`
	SourceQuality  protocol.SourceQuality `json:"source_quality"`
	Label          dataset.Label          `json:"label"`
	Probabilities  map[string]float64     `json:"probabilities"`
	ValidMass      float64                `json:"valid_mass"`
	Invalid        bool                   `json:"invalid"`
	LatencyMS      int64                  `json:"latency_ms"`
	Profile        Profile                `json:"profile"`
	ErrorClass     ErrorClass             `json:"error_class,omitempty"`
}

// analyzePayload is the ANALYZE_AGENT_STATE payload the Analyst sends
// (internal/analyst): dataset.AnalyzeInput member for member, under the
// gateway's wire names. Only output_bytes.last_500ms is named apart from
// the window's last_0_5s.
type analyzePayload struct {
	Harness                 protocol.Harness    `json:"harness"`
	SecondsSinceLastOutput  float64             `json:"seconds_since_last_output"`
	OutputBytes             payloadBytes        `json:"output_bytes"`
	NewChildProcesses       []string            `json:"new_child_processes"`
	PreviousState           protocol.AgentPhase `json:"previous_state"`
	PreviousStateAgeSeconds float64             `json:"previous_state_age_seconds"`
	ScreenLines             []string            `json:"screen_lines"`
}

type payloadBytes struct {
	Last500ms int64 `json:"last_500ms"`
	Last2s    int64 `json:"last_2s"`
	Last10s   int64 `json:"last_10s"`
}

// payloadOf encodes a window's model input exactly as in training.
func payloadOf(in dataset.AnalyzeInput) ([]byte, error) {
	b, err := json.Marshal(analyzePayload{
		Harness:                in.Harness,
		SecondsSinceLastOutput: in.SecondsSinceLastOutput,
		OutputBytes: payloadBytes{
			Last500ms: in.OutputBytes.Last500ms,
			Last2s:    in.OutputBytes.Last2s,
			Last10s:   in.OutputBytes.Last10s,
		},
		NewChildProcesses:       in.NewChildProcesses,
		PreviousState:           in.PreviousState,
		PreviousStateAgeSeconds: in.PreviousStateAgeSeconds,
		ScreenLines:             in.ScreenLines,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: payload not encoded", ErrPredict)
	}
	return b, nil
}

func predictErr(id protocol.ULID, reason string) error {
	return fmt.Errorf("%w: window %s: %s", ErrPredict, id, reason)
}

// Predict runs the model over the validation and test windows of a
// manifest and returns the predictions file, one line per window sorted
// by window_id. Every window is read and checked against its entry
// (window digest, split, label, harness, version and source quality) before any
// call: one that does not match fails the run. A call that fails, ends
// in a status other than ok, or answers without a valid distribution
// still gets its line, invalid, with its ErrorClass. An answer from
// another profile than the one asked for, or a cancelled context, aborts
// the run. Equal answers give equal bytes, whatever the concurrency.
func Predict(ctx context.Context, inf Inferrer, m dataset.Manifest, load WindowLoader, opts PredictOptions) ([]byte, PredictSummary, error) {
	if inf == nil || load == nil {
		return nil, PredictSummary{}, fmt.Errorf("%w: no inferrer or window loader", ErrPredict)
	}
	if !opts.Profile.Valid() {
		return nil, PredictSummary{}, fmt.Errorf("%w: unknown profile", ErrPredict)
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = DefaultConcurrency
	}
	if opts.Concurrency < 1 || opts.Concurrency > maxConcurrency {
		return nil, PredictSummary{}, fmt.Errorf("%w: concurrency must be 1-%d", ErrPredict, maxConcurrency)
	}
	if err := m.Validate(); err != nil {
		return nil, PredictSummary{}, fmt.Errorf("%w: %w", ErrPredict, err)
	}
	jobs, err := prepare(m, load, opts.Profile)
	if err != nil {
		return nil, PredictSummary{}, err
	}
	lines, err := runJobs(ctx, inf, jobs, opts)
	if err != nil {
		return nil, PredictSummary{}, err
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, l := range lines {
		if err := enc.Encode(l); err != nil {
			return nil, PredictSummary{}, predictErr(l.WindowID, "line not encoded")
		}
	}
	return buf.Bytes(), summarise(lines), nil
}

// prepare reads and checks every scored window and builds its request.
// The manifest's entries are sorted by window id, so the jobs are too.
func prepare(m dataset.Manifest, load WindowLoader, profile Profile) ([]job, error) {
	var jobs []job
	for _, e := range m.Entries {
		if e.Split != dataset.SplitValidation && e.Split != dataset.SplitTest {
			continue
		}
		w, err := load(e.WindowID)
		if err != nil {
			// The loader's error may quote a path; only the fact
			// matters.
			return nil, predictErr(e.WindowID, "not readable as a window")
		}
		digest, err := dataset.WindowDigest(w)
		switch {
		case err != nil || digest != e.WindowDigest:
			return nil, predictErr(e.WindowID, "window digest does not match the manifest")
		case w.WindowID != e.WindowID:
			return nil, predictErr(e.WindowID, "window id does not match the manifest")
		case w.Split.Split != e.Split:
			return nil, predictErr(e.WindowID, "split does not match the manifest")
		case w.Target.Label != e.Label:
			return nil, predictErr(e.WindowID, "label does not match the manifest")
		case w.Input.Harness != e.Harness || w.Provenance.HarnessVersion != e.HarnessVersion || w.Provenance.SourceQuality != e.SourceQuality:
			return nil, predictErr(e.WindowID, "harness, version or source quality does not match the manifest")
		}
		payload, err := payloadOf(w.Input)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job{
			line: predictionLine{
				WindowID:       e.WindowID,
				Split:          e.Split,
				Harness:        e.Harness,
				HarnessVersion: w.Provenance.HarnessVersion,
				SessionID:      w.Provenance.SessionID,
				SourceQuality:  e.SourceQuality,
				Label:          e.Label,
				Profile:        profile,
			},
			payload: payload,
		})
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("%w: the manifest has no validation or test window", ErrPredict)
	}
	return jobs, nil
}

// runJobs calls the model for every job, at most opts.Concurrency at a
// time, and returns the lines in job order. The first abort stops the
// rest.
func runJobs(ctx context.Context, inf Inferrer, jobs []job, opts PredictOptions) ([]predictionLine, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	lines := make([]predictionLine, len(jobs))
	var (
		wg       sync.WaitGroup
		once     sync.Once
		abortErr error
	)
	abort := func(err error) {
		once.Do(func() { abortErr = err; cancel() })
	}
	next := make(chan int)
	for range opts.Concurrency {
		wg.Go(func() {
			for i := range next {
				l, err := predictOne(ctx, inf, jobs[i], opts.Profile)
				if err != nil {
					abort(err)
					continue
				}
				lines[i] = l
			}
		})
	}
	for i := range jobs {
		if ctx.Err() != nil {
			break
		}
		select {
		case next <- i:
		case <-ctx.Done():
		}
	}
	close(next)
	wg.Wait()
	if abortErr != nil {
		return nil, abortErr
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPredict, err)
	}
	return lines, nil
}

// agentOf gives each window its own agent id, so concurrent calls never
// meet the gateway's one-call-per-agent rule.
func agentOf(id protocol.ULID) protocol.AgentID {
	return protocol.AgentID("eval-" + strings.ToLower(string(id)))
}

// predictOne runs one window. Its error aborts the run; a failed call is
// a line.
func predictOne(ctx context.Context, inf Inferrer, j job, profile Profile) (predictionLine, error) {
	if err := ctx.Err(); err != nil {
		return predictionLine{}, fmt.Errorf("%w: %w", ErrPredict, err)
	}
	res, err := inf.Infer(ctx, protocol.InferRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID:  agentOf(j.line.WindowID),
		Payload:  protocol.Payload(j.payload),
	})
	if cerr := ctx.Err(); cerr != nil {
		return predictionLine{}, fmt.Errorf("%w: %w", ErrPredict, cerr)
	}
	want := protocol.InferenceProfile(profile)
	rec := res.CallRecord
	// A failed call may carry no result; whatever profile it names
	// must be the one asked for, and a success must name it.
	if (res.InferenceProfile != "" || err == nil) && res.InferenceProfile != want ||
		rec.InferenceProfile != "" && rec.InferenceProfile != want {
		return predictionLine{}, predictErr(j.line.WindowID, "answered by another inference profile than "+string(profile))
	}
	l := j.line
	l.LatencyMS = max(0, rec.LatencyMS)
	switch {
	case err != nil:
		return invalidLine(l, classOfErr(err)), nil
	case rec.Status != protocol.GlimmerCallStatusOK:
		return invalidLine(l, classOfStatus(rec.Status)), nil
	}
	probs, ok := distribution(res.Probabilities)
	if !ok {
		return invalidLine(l, ClassBadProbabilities), nil
	}
	l.Probabilities, l.ValidMass = probs, 1
	return l, nil
}

// invalidLine is a line with no prediction: it counts as UNKNOWN, with
// all its mass on G and none valid.
func invalidLine(l predictionLine, c ErrorClass) predictionLine {
	l.Probabilities = map[string]float64{}
	for _, x := range labels() {
		l.Probabilities[string(x)] = 0
	}
	l.Probabilities[string(dataset.LabelG)] = 1
	l.ValidMass, l.Invalid, l.ErrorClass = 0, true, c
	return l
}

// distribution maps the model's phase distribution onto A–G. Every key
// must be one of the seven phases and every value in [0, 1]; a sum off 1
// by more than the gateway allows is refused, a smaller drift rescaled.
func distribution(in map[string]float64) (map[string]float64, bool) {
	if len(in) == 0 {
		return nil, false
	}
	out := make(map[string]float64, numLabels)
	for _, x := range labels() {
		out[string(x)] = 0
	}
	sum := 0.0
	for k, v := range in {
		l, ok := dataset.LabelOf(protocol.AgentPhase(k))
		if !ok || math.IsNaN(v) || v < 0 || v > 1 {
			return nil, false
		}
		out[string(l)] = v
		sum += v
	}
	if math.Abs(sum-1) > probTolerance {
		return nil, false
	}
	if math.Abs(sum-1) > sumTolerance {
		for k, v := range out {
			out[k] = v / sum
		}
	}
	return out, true
}

func classOfErr(err error) ErrorClass {
	for _, c := range []struct {
		target error
		class  ErrorClass
	}{
		{gateway.ErrInvalidRequest, ClassInvalidRequest},
		{gateway.ErrRedactionFailed, ClassRedactionFailed},
		{gateway.ErrTimeout, ClassTimeout},
		{gateway.ErrInvalidOutput, ClassInvalidOutput},
		{gateway.ErrCircuitOpen, ClassCircuitOpen},
		{gateway.ErrAgentBusy, ClassAgentBusy},
		{gateway.ErrRecordFailed, ClassRecordFailed},
	} {
		if errors.Is(err, c.target) {
			return c.class
		}
	}
	return ClassCallFailed
}

func classOfStatus(s protocol.GlimmerCallStatus) ErrorClass {
	switch s {
	case protocol.GlimmerCallStatusTimeout:
		return ClassTimeout
	case protocol.GlimmerCallStatusInvalid:
		return ClassInvalidOutput
	case protocol.GlimmerCallStatusRedactionFailed:
		return ClassRedactionFailed
	case protocol.GlimmerCallStatusError:
		return ClassCallFailed
	default:
		return ClassStatusNotOK
	}
}

func summarise(lines []predictionLine) PredictSummary {
	s := PredictSummary{Windows: len(lines), Failed: map[ErrorClass]int{}}
	lat := make([]float64, 0, len(lines))
	for _, l := range lines {
		if l.ErrorClass != "" {
			s.Failed[l.ErrorClass]++
			continue
		}
		s.Valid++
		lat = append(lat, float64(l.LatencyMS))
	}
	slices.Sort(lat)
	s.LatencyP50, s.LatencyP95 = percentile(lat, 0.50), percentile(lat, 0.95)
	return s
}

// Lines renders the summary for stderr: numbers and class names only.
func (s PredictSummary) Lines() []string {
	out := []string{fmt.Sprintf("windows %d, valid %d, invalid %d", s.Windows, s.Valid, s.Windows-s.Valid)}
	for _, c := range ErrorClasses() {
		if n := s.Failed[c]; n > 0 {
			out = append(out, fmt.Sprintf("failed %s %d", c, n))
		}
	}
	return append(out, fmt.Sprintf("latency p50 %s ms, p95 %s ms", f1(s.LatencyP50), f1(s.LatencyP95)))
}
