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

// idleConfirm is how long the pane must stay quiet after turn.completed
// before it settles IDLE_AT_PROMPT (PRD §12.3, default of §12.8). Sooner,
// the screen decides.
const idleConfirm = 400 * time.Millisecond

// Route says how a phase was reached.
type Route string

// Routes.
const (
	// RouteNative is a deterministic rule on a native event or the exit
	// code; no model is called.
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
	// CallRecord is the gateway's record of the PTY call, if one ran.
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
}

// New returns an Analyst over a gateway, projecting screens under proj,
// the policies the dataset build runs under.
func New(gw Inferrer, proj screenproj.Policies) (*Analyst, error) {
	if gw == nil {
		return nil, fmt.Errorf("%w: no gateway", ErrModelUnavailable)
	}
	if err := proj.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrScreenWithheld, err)
	}
	return &Analyst{gw: gw, proj: proj}, nil
}

// unknown is the reading when nothing can be trusted.
func unknown(agent protocol.AgentID, route Route) Analysis {
	return Analysis{AgentID: agent, Route: route, Phase: protocol.AgentPhaseUnknown, PreviousStale: true}
}

// Analyze reads one observation. The native route answers first and
// without a model; anything it cannot settle goes to the model, with the
// screen projected by screenproj.Project. A failed model call gives
// UNKNOWN with the previous state stale and an error wrapping
// ErrModelUnavailable, and a screen that cannot be projected gives the
// same with ErrScreenWithheld: the Analyst never guesses.
func (a *Analyst) Analyze(ctx context.Context, obs Observation) (Analysis, error) {
	if err := obs.Validate(); err != nil {
		return unknown(obs.AgentID, RouteNative), err
	}
	if res, ok := nativePhase(obs); ok {
		return res, nil
	}
	return a.ptyPhase(ctx, obs)
}

// nativePhase applies the deterministic rules: an exited process, or the
// latest native event when it settles the phase. turn.completed settles
// IDLE_AT_PROMPT only once the pane has stayed quiet for idleConfirm
// after it, with no output since.
func nativePhase(obs Observation) (Analysis, bool) {
	native := func(p protocol.AgentPhase, why string) (Analysis, bool) {
		return Analysis{AgentID: obs.AgentID, Route: RouteNative, Phase: p, Confidence: 1, Evidence: []string{why}}, true
	}
	if obs.ExitCode != nil {
		return native(protocol.AgentPhaseErrorOrExited, "process exited")
	}
	if len(obs.NativeEvents) == 0 {
		return Analysis{}, false
	}
	// The latest event wins; on a tie, the later one in the list.
	last := obs.NativeEvents[0]
	for _, e := range obs.NativeEvents[1:] {
		if !e.At.Before(last.At) {
			last = e
		}
	}
	why := "native event " + string(last.Type)
	switch last.Type {
	case protocol.EventTypeToolStarted:
		return native(protocol.AgentPhaseRunningTool, why)
	case protocol.EventTypeApprovalRequested:
		return native(protocol.AgentPhaseWaitingApproval, why)
	case protocol.EventTypeQuestionRequested:
		return native(protocol.AgentPhaseAskingQuestion, why)
	case protocol.EventTypeBackgroundTaskStarted, protocol.EventTypeBackgroundTaskCompleted:
		return native(protocol.AgentPhaseBackgroundWork, why)
	case protocol.EventTypeTurnCompleted:
		lastOutput := obs.ObservedAt.Add(-time.Duration(obs.Activity.SecondsSinceLastOutput * float64(time.Second)))
		if !lastOutput.After(last.At) && obs.ObservedAt.Sub(last.At) >= idleConfirm {
			return native(protocol.AgentPhaseIdleAtPrompt, why)
		}
	default:
		// No other event settles the phase: the screen decides.
	}
	return Analysis{}, false
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
	if obs.Previous != nil {
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
// the screen: nothing then says the human draft is off it.
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
