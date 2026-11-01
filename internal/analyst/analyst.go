package analyst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/screenproj"
)

// ErrModelUnavailable means the gateway gave no usable answer, so the
// phase is UNKNOWN and the previous state is stale. It wraps the
// gateway's own sentinel.
var ErrModelUnavailable = errors.New("analyst model unavailable")

// ErrScreenWithheld means the PTY route sent nothing to the gateway: the
// input guard snapshot was missing, invalid, of another agent or stale,
// or the screen projection refused the screen. The phase is UNKNOWN and
// the previous state stale; the error names a fixed reason, never
// content.
var ErrScreenWithheld = errors.New("analyst screen withheld")

// Route says how a phase was reached.
type Route string

// Routes.
const (
	// RouteNative is a deterministic veto from reduced native events, or the
	// process exit. Native events never provide positive idle evidence.
	RouteNative Route = "native"
	// RoutePTY asks the model through the gateway.
	RoutePTY Route = "pty"
)

// Analysis is the Analyst's advisory reading of one observation.
type Analysis struct {
	AgentID protocol.AgentID
	Route   Route
	Phase   protocol.AgentPhase
	// Confidence is the probability of Phase: 1 on the native route, the
	// model's on the PTY route, 0 when the phase is unknown by failure.
	Confidence float64
	Evidence   []string
	// PreviousStale marks the previous state as no longer current: the
	// model could not be read, so nothing replaces it but it is not
	// trusted either.
	PreviousStale bool
	PromptVersion string
	// NativeEvidence is the durable event proving a native veto or uncertainty.
	NativeEvidence []protocol.ULID
	// NativeExpiresAt is the receipt-time deadline of an authoritative native
	// veto. It is zero for PTY fallback diagnostics.
	NativeExpiresAt time.Time
	// CallRecord is the gateway's record of the PTY call, if one ran and its
	// result was not superseded by a native veto.
	CallRecord *protocol.GlimmerCallRecord
	// ScreenLines is N, the cut the screen projection ran with, Policies
	// the versions it ran under, as a dataset window records them, and
	// Redactions its substitutions. They are zero unless the PTY route
	// projected the screen.
	ScreenLines int
	Policies    screenproj.Versions
	Redactions  int64
}

// Inferrer runs a model contract. *gateway.Gateway satisfies it.
type Inferrer interface {
	Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error)
}

// Analyst reads agent state. It is advisory: it never writes to a
// terminal, never decides a delivery and never touches the human draft
// or the input guard, of which it only reads a snapshot.
type Analyst struct {
	gw   Inferrer
	proj screenproj.Policies
	// noPrevious leaves previous_state empty in every payload: see
	// Served.
	noPrevious bool
}

// Option adjusts an Analyst at New.
type Option func(*Analyst)

// Served names the inference profile that serves ANALYZE_AGENT_STATE.
// Under analyst-lora every payload's previous_state stays empty, with age
// 0, as in every dataset window the adapter learns from; under base the
// Analyst fills it from the last state stored for the agent.
func Served(p protocol.InferenceProfile) Option {
	return func(a *Analyst) { a.noPrevious = p == protocol.InferenceProfileAnalystLoRA }
}

// New returns an Analyst over a gateway, projecting screens under proj,
// the policies the dataset build runs under.
func New(gw Inferrer, proj screenproj.Policies, opts ...Option) (*Analyst, error) {
	if gw == nil {
		return nil, fmt.Errorf("%w: no gateway", ErrModelUnavailable)
	}
	if err := proj.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScreenWithheld, err)
	}
	a := &Analyst{gw: gw, proj: proj}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

// unknown is the reading when nothing can be trusted.
func unknown(agent protocol.AgentID, route Route) Analysis {
	return Analysis{AgentID: agent, Route: route, Phase: protocol.AgentPhaseUnknown, PreviousStale: true}
}

// Analyze reads one observation. A process exit settles without a model.
// Otherwise Glimmer sees the same PTY input with or without native events;
// the native reducer can replace that result only with a current non-idle
// veto. Native uncertainty stays attached as a diagnostic while PTY evidence
// becomes authoritative. A failed model call gives
// UNKNOWN with the previous state stale and an error wrapping
// ErrModelUnavailable, and a screen that cannot be projected gives the
// same with ErrScreenWithheld: the Analyst never guesses.
func (a *Analyst) Analyze(ctx context.Context, obs Observation) (Analysis, error) {
	if err := obs.Validate(); err != nil {
		return unknown(obs.AgentID, RouteNative), err
	}
	if obs.ExitCode != nil {
		return Analysis{
			AgentID: obs.AgentID, Route: RouteNative,
			Phase: protocol.AgentPhaseErrorOrExited, Confidence: 1,
			Evidence: []string{"process exited"},
		}, nil
	}
	pty, err := a.ptyPhase(ctx, obs)
	reduced := ReduceNative(obs.NativeEvents, obs.ObservedAt)
	if res, ok := nativePhase(obs.AgentID, reduced); ok {
		return res, nil
	}
	if reduced.Phase == protocol.AgentPhaseUnknown {
		pty.NativeEvidence = append([]protocol.ULID(nil), reduced.Evidence...)
	}
	return pty, err
}

// nativePhase applies only restrictive deterministic rules. It never reports
// idle: positive delivery evidence always comes from Glimmer.
func nativePhase(agent protocol.AgentID, reduced NativeState) (Analysis, bool) {
	if !reduced.Blocks {
		return Analysis{}, false
	}
	why := "native reducer veto"
	if reduced.Phase == protocol.AgentPhaseUnknown {
		why = "native reducer continuity unknown"
	}
	return Analysis{
		AgentID: agent, Route: RouteNative, Phase: reduced.Phase,
		Confidence: reduced.Confidence, Evidence: []string{why},
		PreviousStale: reduced.Stale, NativeEvidence: reduced.Evidence,
		NativeExpiresAt: reduced.ExpiresAt,
	}, true
}

// analyzeInput is the ANALYZE_AGENT_STATE payload: dataset.AnalyzeInput
// field for field, the model input of training, and nothing else.
type analyzeInput struct {
	Harness                 protocol.Harness    `json:"harness"`
	SecondsSinceLastOutput  float64             `json:"seconds_since_last_output"`
	OutputBytes             outputBytes         `json:"output_bytes"`
	NewChildProcesses       []string            `json:"new_child_processes"`
	PreviousState           protocol.AgentPhase `json:"previous_state"`
	PreviousStateAgeSeconds float64             `json:"previous_state_age_seconds"`
	ScreenLines             []string            `json:"screen_lines"`
}

type outputBytes struct {
	Last500ms int64 `json:"last_500ms"`
	Last2s    int64 `json:"last_2s"`
	Last10s   int64 `json:"last_10s"`
}

func (a *Analyst) ptyPhase(ctx context.Context, obs Observation) (Analysis, error) {
	out := unknown(obs.AgentID, RoutePTY)
	draft, err := guardDraft(obs)
	if err != nil {
		return out, err
	}
	proj, err := screenproj.Project(screenproj.Input{Rows: obs.Screen, Cols: obs.Cols, Cursor: obs.Cursor, Draft: draft}, a.proj)
	if err != nil {
		// Project's errors name a stage, never content.
		return out, fmt.Errorf("%w: %w", ErrScreenWithheld, err)
	}
	p := analyzeInput{
		Harness:                obs.Harness,
		SecondsSinceLastOutput: obs.Activity.SecondsSinceLastOutput,
		OutputBytes: outputBytes{
			Last500ms: obs.Activity.BytesLast500ms,
			Last2s:    obs.Activity.BytesLast2s,
			Last10s:   obs.Activity.BytesLast10s,
		},
		NewChildProcesses: screenproj.ModelChildProcesses(obs.ChildProcesses),
		ScreenLines:       proj.Lines,
	}
	if obs.Previous != nil && !a.noPrevious {
		p.PreviousState = obs.Previous.Phase
		p.PreviousStateAgeSeconds = obs.ObservedAt.Sub(obs.Previous.Since).Seconds()
	}
	body, err := json.Marshal(p)
	if err != nil {
		return out, fmt.Errorf("%w: payload not encoded", ErrModelUnavailable)
	}
	out.PromptVersion = PromptVersion
	out.ScreenLines, out.Policies, out.Redactions = proj.N, proj.Versions, proj.Redactions
	res, err := a.gw.Infer(ctx, protocol.InferRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID:  obs.AgentID,
		Payload:  protocol.Payload(body),
	})
	if res.CallRecord.CallID != "" {
		rec := res.CallRecord
		out.CallRecord = &rec
	}
	if err != nil {
		return out, fmt.Errorf("%w: %w", ErrModelUnavailable, err)
	}
	phase := protocol.AgentPhase(res.Label)
	conf, ok := res.Probabilities[res.Label]
	if res.CallRecord.Status != protocol.GlimmerCallStatusOK || !slices.Contains(modelLabels(), phase) || !ok {
		return out, fmt.Errorf("%w: answer outside the contract", ErrModelUnavailable)
	}
	out.Phase, out.Confidence, out.Evidence, out.PreviousStale = phase, conf, res.Evidence, false
	return out, nil
}

// guardDraft is the draft state of the input guard snapshot the screen
// was captured with. A snapshot that is missing, invalid, of another
// agent or of another input_guard_version than the capture's withholds
// the screen: nothing then says the human draft is off it. The Analyst
// keeps no snapshot: each observation brings its own, and one that does
// not match is discarded, never reused.
func guardDraft(obs Observation) (protocol.DraftState, error) {
	if obs.Guard == nil {
		return "", fmt.Errorf("%w: no input guard snapshot", ErrScreenWithheld)
	}
	// One copy, so every check reads the same report.
	g := *obs.Guard
	switch {
	case g.Validate() != nil:
		return "", fmt.Errorf("%w: input guard snapshot invalid", ErrScreenWithheld)
	case g.AgentID != obs.AgentID:
		return "", fmt.Errorf("%w: input guard snapshot of another agent", ErrScreenWithheld)
	case g.InputGuardVersion != obs.GuardVersion:
		return "", fmt.Errorf("%w: input guard snapshot stale", ErrScreenWithheld)
	}
	return g.DraftState, nil
}
