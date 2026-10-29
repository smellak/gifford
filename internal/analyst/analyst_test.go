package analyst

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
)

// All screens and values below are invented.
const childGoTest = "go test"

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func baseObservation() Observation {
	return Observation{
		AgentID:        "agent-1",
		Harness:        protocol.HarnessClaudeCode,
		HarnessVersion: "1.2.3",
		ObservedAt:     t0,
		Screen:         []string{"step 3 of 7: compiling", "tests: 12 passed, 0 failed"},
		Activity:       Activity{BytesLast500ms: 10, BytesLast2s: 40, BytesLast10s: 90, SecondsSinceLastOutput: 0.4},
		ChildProcesses: []string{childGoTest},
		Previous:       &PreviousState{Phase: protocol.AgentPhaseThinking, Since: t0.Add(-4 * time.Second)},
	}
}

// fakeGateway answers every call with a fixed result and keeps the
// requests.
type fakeGateway struct {
	mu   sync.Mutex
	reqs []protocol.InferRequest
	res  protocol.InferResult
	err  error
}

func (f *fakeGateway) Infer(_ context.Context, req protocol.InferRequest) (protocol.InferResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs = append(f.reqs, req)
	return f.res, f.err
}

func (f *fakeGateway) calls() []protocol.InferRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protocol.InferRequest(nil), f.reqs...)
}

func okResult(label protocol.AgentPhase, p float64) protocol.InferResult {
	probs := map[string]float64{string(label): p}
	if p < 1 {
		probs[string(protocol.AgentPhaseUnknown)] = 1 - p
	}
	return protocol.InferResult{
		Contract:         protocol.ModelContractAnalyzeAgentState,
		InferenceProfile: protocol.InferenceProfileBase,
		Label:            string(label),
		Probabilities:    probs,
		Evidence:         []string{"tests: 12 passed"},
		CallRecord:       protocol.GlimmerCallRecord{CallID: "call-1", Status: protocol.GlimmerCallStatusOK},
	}
}

func newAnalyst(t *testing.T, gw Inferrer) *Analyst {
	t.Helper()
	a, err := New(gw)
	require.NoError(t, err)
	return a
}

func TestNativeRoute(t *testing.T) {
	t.Parallel()
	ev := func(typ protocol.EventType, ago time.Duration) NativeEvent {
		return NativeEvent{Type: typ, At: t0.Add(-ago)}
	}
	exited := 1
	cases := []struct {
		name   string
		events []NativeEvent
		exit   *int
		phase  protocol.AgentPhase // empty: the PTY route is taken
	}{
		{"tool started", []NativeEvent{ev(protocol.EventTypeToolStarted, time.Second)}, nil, protocol.AgentPhaseRunningTool},
		{"approval requested", []NativeEvent{ev(protocol.EventTypeApprovalRequested, time.Second)}, nil, protocol.AgentPhaseWaitingApproval},
		{"question requested", []NativeEvent{ev(protocol.EventTypeQuestionRequested, time.Second)}, nil, protocol.AgentPhaseAskingQuestion},
		{"background task started", []NativeEvent{ev(protocol.EventTypeBackgroundTaskStarted, time.Second)}, nil, protocol.AgentPhaseBackgroundWork},
		{"background task completed", []NativeEvent{ev(protocol.EventTypeBackgroundTaskCompleted, time.Second)}, nil, protocol.AgentPhaseBackgroundWork},
		{"turn completed, quiet since", []NativeEvent{ev(protocol.EventTypeTurnCompleted, 100*time.Millisecond)}, nil, protocol.AgentPhaseIdleAtPrompt},
		{"turn completed, output since", []NativeEvent{ev(protocol.EventTypeTurnCompleted, 2*time.Second)}, nil, ""},
		{"an unsettling event", []NativeEvent{ev(protocol.EventTypeToolCompleted, time.Second)}, nil, ""},
		{"no events", nil, nil, ""},
		{"latest event wins", []NativeEvent{
			ev(protocol.EventTypeApprovalRequested, time.Second),
			ev(protocol.EventTypeToolStarted, 3*time.Second),
		}, nil, protocol.AgentPhaseWaitingApproval},
		{"a later unsettling event sends to the model", []NativeEvent{
			ev(protocol.EventTypeToolStarted, 3*time.Second),
			ev(protocol.EventTypeApprovalResolved, time.Second),
		}, nil, ""},
		{"same instant: the later in the list", []NativeEvent{
			ev(protocol.EventTypeToolStarted, time.Second),
			ev(protocol.EventTypeQuestionRequested, time.Second),
		}, nil, protocol.AgentPhaseAskingQuestion},
		{"an exited process", []NativeEvent{ev(protocol.EventTypeToolStarted, time.Second)}, &exited, protocol.AgentPhaseErrorOrExited},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gw := &fakeGateway{res: okResult(protocol.AgentPhaseThinking, 0.9)}
			obs := baseObservation()
			obs.NativeEvents, obs.ExitCode = tc.events, tc.exit
			got, err := newAnalyst(t, gw).Analyze(context.Background(), obs)
			require.NoError(t, err)
			if tc.phase == "" {
				require.Equal(t, RoutePTY, got.Route)
				require.Len(t, gw.calls(), 1, "the model is asked")
				return
			}
			require.Equal(t, RouteNative, got.Route)
			require.Equal(t, tc.phase, got.Phase)
			require.InDelta(t, 1.0, got.Confidence, 0)
			require.False(t, got.PreviousStale)
			require.Nil(t, got.CallRecord)
			require.Empty(t, gw.calls(), "the native route never calls the model")
		})
	}
}

// decodePayload decodes strictly: a field beyond AnalyzeInput fails.
func decodePayload(t *testing.T, req protocol.InferRequest) analyzeInput {
	t.Helper()
	var p analyzeInput
	dec := json.NewDecoder(strings.NewReader(string(req.Payload)))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&p))
	return p
}

func TestPTYRoute(t *testing.T) {
	t.Parallel()
	gw := &fakeGateway{res: okResult(protocol.AgentPhaseRunningTool, 0.8)}
	obs := baseObservation()
	rows := make([]string, 0, 60)
	for i := range 50 {
		rows = append(rows, fmt.Sprintf("row %02d  \t", i))
	}
	rows = append(rows, "bell\a here", strings.Repeat("x", maxRowRunes+10), "", "   ")
	obs.Screen = rows
	obs.ChildProcesses = []string{"npm install", childGoTest, childGoTest}

	got, err := newAnalyst(t, gw).Analyze(context.Background(), obs)
	require.NoError(t, err)
	require.Equal(t, RoutePTY, got.Route)
	require.Equal(t, protocol.AgentPhaseRunningTool, got.Phase)
	require.InDelta(t, 0.8, got.Confidence, 1e-9, "the probability of the chosen label")
	require.Equal(t, []string{"tests: 12 passed"}, got.Evidence)
	require.False(t, got.PreviousStale)
	require.Equal(t, PromptVersion, got.PromptVersion)
	require.NotNil(t, got.CallRecord)

	calls := gw.calls()
	require.Len(t, calls, 1)
	require.Equal(t, protocol.ModelContractAnalyzeAgentState, calls[0].Contract)
	require.Equal(t, protocol.AgentID("agent-1"), calls[0].AgentID)
	p := decodePayload(t, calls[0])
	require.Equal(t, protocol.HarnessClaudeCode, p.Harness)
	require.Len(t, p.ScreenLines, screenLines, "the last n rows")
	require.Equal(t, "row 12", p.ScreenLines[0], "trailing blanks trimmed")
	require.Equal(t, "bell here", p.ScreenLines[screenLines-2], "control runes dropped")
	require.Len(t, []rune(p.ScreenLines[screenLines-1]), maxRowRunes, "rows bounded; trailing empty rows dropped")
	require.Equal(t, []string{childGoTest, "npm install"}, p.NewChildProcesses)
	require.Equal(t, protocol.AgentPhaseThinking, p.PreviousState)
	require.InDelta(t, 4.0, p.PreviousStateAgeSeconds, 1e-9)
	require.Equal(t, outputBytes{Last500ms: 10, Last2s: 40, Last10s: 90}, p.OutputBytes)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(calls[0].Payload, &fields))
	require.Len(t, fields, 7, "exactly the AnalyzeInput fields, no prompt")
}

func TestPTYFirstWindowAndChildren(t *testing.T) {
	t.Parallel()
	gw := &fakeGateway{res: okResult(protocol.AgentPhaseIdleAtPrompt, 1)}
	obs := baseObservation()
	obs.Previous, obs.Screen, obs.ChildProcesses = nil, nil, nil
	_, err := newAnalyst(t, gw).Analyze(context.Background(), obs)
	require.NoError(t, err)
	raw := string(gw.calls()[0].Payload)
	require.Contains(t, raw, `"new_child_processes":null`, "unknown stays unknown")
	require.Contains(t, raw, `"screen_lines":[]`)
	require.Contains(t, raw, `"previous_state":""`)

	for name, children := range map[string][]string{
		"argv":      {"python secret_script.py --token abc"},
		"a path":    {"/usr/bin/go test"},
		"too many":  {"a", "b", "c", "d", "e", "f", "g", "h", "i"},
		"bad verb":  {"go Test"},
		"empty one": {""},
	} {
		require.Nil(t, modelChildren(children), name)
	}
	require.Equal(t, []string{}, modelChildren([]string{}), "none stays none")
}

func TestPTYAnswerOutsideTheContractIsUnknown(t *testing.T) {
	t.Parallel()
	for name, res := range map[string]protocol.InferResult{
		"background work from the model": okResult(protocol.AgentPhaseBackgroundWork, 1),
		"label without probability": func() protocol.InferResult {
			r := okResult(protocol.AgentPhaseRunningTool, 1)
			r.Probabilities = map[string]float64{string(protocol.AgentPhaseUnknown): 1}
			return r
		}(),
		"not ok": func() protocol.InferResult {
			r := okResult(protocol.AgentPhaseRunningTool, 1)
			r.CallRecord.Status = protocol.GlimmerCallStatusInvalid
			return r
		}(),
		"unknown label": okResult("SLEEPING", 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := newAnalyst(t, &fakeGateway{res: res}).Analyze(context.Background(), baseObservation())
			require.ErrorIs(t, err, ErrModelUnavailable)
			require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
			require.True(t, got.PreviousStale)
			require.Zero(t, got.Confidence)
			require.Empty(t, got.Evidence)
		})
	}
}

func TestGatewayFailureIsUnknown(t *testing.T) {
	t.Parallel()
	for _, gwErr := range []error{
		gateway.ErrTimeout, gateway.ErrInvalidOutput, gateway.ErrCallFailed,
		gateway.ErrRedactionFailed, gateway.ErrCircuitOpen, gateway.ErrAgentBusy,
	} {
		t.Run(gwErr.Error(), func(t *testing.T) {
			t.Parallel()
			res := protocol.InferResult{CallRecord: protocol.GlimmerCallRecord{CallID: "call-9", Status: protocol.GlimmerCallStatusError}}
			got, err := newAnalyst(t, &fakeGateway{res: res, err: gwErr}).Analyze(context.Background(), baseObservation())
			require.ErrorIs(t, err, ErrModelUnavailable)
			require.ErrorIs(t, err, gwErr)
			require.Equal(t, protocol.AgentPhaseUnknown, got.Phase, "never a guess")
			require.True(t, got.PreviousStale)
			require.Zero(t, got.Confidence)
			require.NotNil(t, got.CallRecord, "the failed call's record is kept")
		})
	}
}

func TestObservationValidate(t *testing.T) {
	t.Parallel()
	require.NoError(t, baseObservation().Validate())
	cases := map[string]func(*Observation){
		"agent id":         func(o *Observation) { o.AgentID = "Agent One" },
		"harness":          func(o *Observation) { o.Harness = "vim" },
		"harness version":  func(o *Observation) { o.HarnessVersion = strings.Repeat("1", 65) },
		"observed at":      func(o *Observation) { o.ObservedAt = time.Time{} },
		"negative bytes":   func(o *Observation) { o.Activity.BytesLast500ms = -1 },
		"windows unnested": func(o *Observation) { o.Activity.BytesLast2s = 5 },
		"negative idle":    func(o *Observation) { o.Activity.SecondsSinceLastOutput = -1 },
		"huge idle":        func(o *Observation) { o.Activity.SecondsSinceLastOutput = 1e300 },
		"event type": func(o *Observation) {
			o.NativeEvents = []NativeEvent{{Type: "weather.rain", At: t0}}
		},
		"event in the future": func(o *Observation) {
			o.NativeEvents = []NativeEvent{{Type: protocol.EventTypeToolStarted, At: t0.Add(time.Second)}}
		},
		"previous phase":  func(o *Observation) { o.Previous.Phase = "NAPPING" },
		"previous future": func(o *Observation) { o.Previous.Since = t0.Add(time.Second) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			obs := baseObservation()
			mutate(&obs)
			gw := &fakeGateway{}
			got, err := newAnalyst(t, gw).Analyze(context.Background(), obs)
			require.ErrorIs(t, err, ErrInvalidObservation)
			require.Equal(t, protocol.AgentPhaseUnknown, got.Phase)
			require.Empty(t, gw.calls(), "an invalid observation is never sent")
		})
	}
	_, err := New(nil)
	require.Error(t, err)
}

func TestPromptsRender(t *testing.T) {
	t.Parallel()
	in := `{"harness":"codex","seconds_since_last_output":0.4,"output_bytes":{"last_500ms":1,"last_2s":2,"last_10s":3},` +
		`"new_child_processes":null,"previous_state":"THINKING_STREAMING","previous_state_age_seconds":4,` +
		`"screen_lines":["$ go test ./...","token ••••"]}`
	p, err := Prompts{}.RenderPrompt(protocol.ModelContractAnalyzeAgentState, []byte(in))
	require.NoError(t, err)
	require.Equal(t, PromptVersion, p.Version)
	require.Contains(t, p.Text, "(codex)")
	for _, l := range modelLabels() {
		require.Contains(t, p.Text, "- "+string(l)+"\n")
	}
	require.NotContains(t, p.Text, string(protocol.AgentPhaseBackgroundWork), "never a model label")
	require.Contains(t, p.Text, "| $ go test ./...\n| token ••••\n", "the sanitised rows")
	require.Contains(t, p.Text, "new_child_processes: unknown")
	require.Contains(t, p.Text, "previous_state: THINKING_STREAMING for 4s")
	_, err = promptFS.Open("prompts/" + PromptVersion + ".tmpl")
	require.NoError(t, err, "the version names its template file")

	none, err := Prompts{}.RenderPrompt(protocol.ModelContractTriageMessage, []byte(`{"text":"hi"}`))
	require.NoError(t, err)
	require.Equal(t, gateway.Prompt{}, none, "no template for other contracts yet")

	_, err = Prompts{}.RenderPrompt(protocol.ModelContractAnalyzeAgentState, []byte(`{"sentinel_field":1}`))
	require.Error(t, err)
	require.NotContains(t, err.Error(), "sentinel", "the decoder's text is not wrapped")
}
