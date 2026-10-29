package analyst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

// ErrModelUnavailable means the gateway gave no usable answer, so the
// phase is UNKNOWN and the previous state is stale. It wraps the
// gateway's own sentinel.
var ErrModelUnavailable = errors.New("analyst model unavailable")

// Preprocessing bounds of the PTY route.
const (
	// screenLines is n, the last rows of the pane the model sees.
	screenLines = 40
	// maxRowRunes bounds one row sent to the model.
	maxRowRunes = 512
	// maxModelChildren bounds the child processes sent to the model.
	maxModelChildren = 8
	// idleConfirm is how long the pane must stay quiet after
	// turn.completed before it settles IDLE_AT_PROMPT (PRD §12.3, default
	// of §12.8). Sooner, the screen decides.
	idleConfirm = 400 * time.Millisecond
)

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
}

// ScreenRedactor masks the secrets of a screen. *redact.Redactor
// satisfies it.
type ScreenRedactor interface {
	RedactScreen(s redact.Screen) (redact.ScreenResult, error)
}

// Inferrer runs a model contract. *gateway.Gateway satisfies it.
type Inferrer interface {
	Infer(ctx context.Context, req protocol.InferRequest) (protocol.InferResult, error)
}

// Analyst reads agent state. It is advisory: it never writes to a
// terminal, never decides a delivery and never touches the human draft.
type Analyst struct {
	gw  Inferrer
	red ScreenRedactor
}

// New returns an Analyst over a gateway. The redactor sees the whole
// screen before the PTY route cuts it, so no cut can split a secret
// past the redactor.
func New(gw Inferrer, red ScreenRedactor) (*Analyst, error) {
	if gw == nil || red == nil {
		return nil, fmt.Errorf("%w: no gateway or redactor", ErrModelUnavailable)
	}
	return &Analyst{gw: gw, red: red}, nil
}

// unknown is the reading when nothing can be trusted.
func unknown(agent protocol.AgentID, route Route) Analysis {
	return Analysis{AgentID: agent, Route: route, Phase: protocol.AgentPhaseUnknown, PreviousStale: true}
}

// Analyze reads one observation. The native route answers first and
// without a model; anything it cannot settle goes to the model. A
// failed model call gives UNKNOWN with the previous state stale and an
// error wrapping ErrModelUnavailable: the Analyst never guesses.
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
	screen, err := a.modelScreen(obs.Screen)
	if err != nil {
		// Nothing is sent: a screen the redactor cannot read is not cut
		// and forwarded.
		return unknown(obs.AgentID, RoutePTY), fmt.Errorf("%w: %w", ErrModelUnavailable, gateway.ErrRedactionFailed)
	}
	p := analyzeInput{
		Harness:                obs.Harness,
		SecondsSinceLastOutput: obs.Activity.SecondsSinceLastOutput,
		OutputBytes: outputBytes{
			Last500ms: obs.Activity.BytesLast500ms,
			Last2s:    obs.Activity.BytesLast2s,
			Last10s:   obs.Activity.BytesLast10s,
		},
		NewChildProcesses: modelChildren(obs.ChildProcesses),
		ScreenLines:       screen,
	}
	if obs.Previous != nil {
		p.PreviousState = obs.Previous.Phase
		p.PreviousStateAgeSeconds = obs.ObservedAt.Sub(obs.Previous.Since).Seconds()
	}
	body, err := json.Marshal(p)
	if err != nil {
		return unknown(obs.AgentID, RoutePTY), fmt.Errorf("%w: payload not encoded", ErrModelUnavailable)
	}
	res, err := a.gw.Infer(ctx, protocol.InferRequest{
		Contract: protocol.ModelContractAnalyzeAgentState,
		AgentID:  obs.AgentID,
		Payload:  protocol.Payload(body),
	})
	out := unknown(obs.AgentID, RoutePTY)
	out.PromptVersion = PromptVersion
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

// modelScreen prepares the rows the model sees. Control runes are
// dropped and trailing blanks trimmed; then the whole screen is redacted,
// so a secret is masked before any cut could split it; only then are the
// trailing empty rows dropped and the last screenLines rows kept, each
// bounded to maxRowRunes. The gateway redacts again before sending.
func (a *Analyst) modelScreen(rows []string) (out []string, err error) {
	defer func() {
		if recover() != nil {
			out, err = nil, gateway.ErrRedactionFailed
		}
	}()
	clean := make([]string, 0, len(rows))
	for _, r := range rows {
		r = strings.Map(func(c rune) rune {
			if unicode.IsControl(c) {
				return -1
			}
			return c
		}, r)
		clean = append(clean, strings.TrimRightFunc(r, unicode.IsSpace))
	}
	res, err := a.red.RedactScreen(redact.Screen{Rows: clean})
	if err != nil || len(res.Rows) != len(clean) {
		return nil, gateway.ErrRedactionFailed
	}
	masked := res.Rows
	for len(masked) > 0 && masked[len(masked)-1] == "" {
		masked = masked[:len(masked)-1]
	}
	masked = masked[max(0, len(masked)-screenLines):]
	out = make([]string, len(masked))
	for i, r := range masked {
		if rs := []rune(r); len(rs) > maxRowRunes {
			r = string(rs[:maxRowRunes])
		}
		out[i] = r
	}
	return out, nil
}

// modelChildren sorts and dedups the child processes, keeping the
// tri-state: nil stays unknown. An entry that is not a basename with an
// optional verb, or more than maxModelChildren, makes the list unknown
// rather than sending argv.
func modelChildren(children []string) []string {
	if children == nil {
		return nil
	}
	out := slices.Clone(children)
	slices.Sort(out)
	out = slices.Compact(out)
	if len(out) > maxModelChildren {
		return nil
	}
	for _, c := range out {
		if !isChild(c) {
			return nil
		}
	}
	return out
}

// isChild reports whether c is an executable basename of 1–64 of
// [A-Za-z0-9._+-], optionally followed by one space and a verb of 1–16
// lowercase letters.
func isChild(c string) bool {
	name, verb, hasVerb := strings.Cut(c, " ")
	if len(name) < 1 || len(name) > 64 || strings.Trim(name, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._+-") != "" {
		return false
	}
	if !hasVerb {
		return true
	}
	return len(verb) >= 1 && len(verb) <= 16 && strings.Trim(verb, "abcdefghijklmnopqrstuvwxyz") == ""
}
