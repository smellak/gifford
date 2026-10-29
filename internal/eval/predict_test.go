package eval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/gateway"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
)

func digestOf(s string) protocol.Digest {
	sum := sha256.Sum256([]byte(s))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// evalWindow is a minimal valid window. Every value is invented.
func evalWindow(n int, split dataset.Split, label dataset.Label) dataset.Window {
	anchor := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"step 3 of 7: compiling", fmt.Sprintf("tests: %d passed, 0 failed", n)}
	id := protocol.ULID(fmt.Sprintf("01JB%022d", n))
	return dataset.Window{
		SchemaVersion: dataset.WindowSchemaVersion,
		WindowID:      id,
		Contract:      protocol.ModelContractAnalyzeAgentState,
		Input: dataset.AnalyzeInput{
			Harness:                 protocol.HarnessClaudeCode,
			SecondsSinceLastOutput:  0.2,
			OutputBytes:             dataset.OutputBytes{Last500ms: 120, Last2s: 480, Last10s: 900},
			NewChildProcesses:       []string{"go test"},
			PreviousState:           protocol.AgentPhaseRunningTool,
			PreviousStateAgeSeconds: 4.5,
			ScreenLines:             lines,
		},
		Screen: dataset.ScreenEvidence{
			Cols: 80, Rows: 24,
			Redaction:           dataset.AppliedPolicy{Applied: true, PolicyVersion: "1"},
			HumanInputExclusion: dataset.AppliedPolicy{Applied: true, PolicyVersion: "1"},
			Digest:              dataset.ScreenDigest(80, 24, lines),
		},
		Target: dataset.Target{Label: label, Source: dataset.LabelSourceHook},
		Provenance: dataset.Provenance{
			SessionID:        protocol.OpaqueID(fmt.Sprintf("cap-%d", n)),
			CaptureSessionID: protocol.OpaqueID(fmt.Sprintf("cap-%d", n)),
			SourceQuality:    protocol.SourceQualityPTYScreen,
			SequenceFrom:     2,
			SequenceTo:       40,
			WindowStart:      anchor.Add(-30 * time.Second),
			WindowEnd:        anchor,
			AnchorAt:         anchor,
			SourceRefs:       []protocol.BlobRef{"blob://capture/cap-01ARZ3NDEKTSV4RRFFQ69G5FAV/seg-01ARZ3NDEKTSV4RRFFQ69G5FAW/0"},
			SourceDigest:     digestOf(fmt.Sprintf("source %d", n)),
		},
		EnvironmentDomain: dataset.EnvironmentDomainBankGeneric,
		Policies:          dataset.Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
		Split:             dataset.SplitAssignment{Split: split, GroupDigest: digestOf("group " + string(split))},
	}
}

// corpus is n scored windows, alternately validation and test, after one
// train window that is never predicted. It is built out of order.
func corpus(t *testing.T, n int) (dataset.Manifest, map[protocol.ULID]dataset.Window) {
	t.Helper()
	ws := []dataset.Window{evalWindow(0, dataset.SplitTrain, dataset.LabelA)}
	for i := n; i >= 1; i-- {
		split := dataset.SplitValidation
		if i%2 == 0 {
			split = dataset.SplitTest
		}
		ws = append(ws, evalWindow(i, split, labels()[i%numLabels]))
	}
	m, err := dataset.BuildManifest(ws)
	require.NoError(t, err)
	byID := map[protocol.ULID]dataset.Window{}
	for _, w := range ws {
		byID[w.WindowID] = w
	}
	return m, byID
}

func loaderOf(ws map[protocol.ULID]dataset.Window) WindowLoader {
	return func(id protocol.ULID) (dataset.Window, error) {
		w, ok := ws[id]
		if !ok {
			return dataset.Window{}, errors.New("no such window /secret/path")
		}
		return w, nil
	}
}

// answer is what the fake gives one call.
type answer struct {
	res protocol.InferResult
	err error
}

// fakeInferrer answers by agent id, and records the requests and the
// most calls ever in flight.
type fakeInferrer struct {
	mu       sync.Mutex
	reqs     []protocol.InferRequest
	answer   func(req protocol.InferRequest) answer
	delay    func(req protocol.InferRequest) time.Duration
	inFlight atomic.Int64
	peak     atomic.Int64
}

func (f *fakeInferrer) Infer(_ context.Context, req protocol.InferRequest) (protocol.InferResult, error) {
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		p := f.peak.Load()
		if n <= p || f.peak.CompareAndSwap(p, n) {
			break
		}
	}
	f.mu.Lock()
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	if f.delay != nil {
		time.Sleep(f.delay(req))
	}
	a := f.answer(req)
	return a.res, a.err
}

func (f *fakeInferrer) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func okAnswer(profile protocol.InferenceProfile, probs map[string]float64, latency int64) answer {
	rec := protocol.GlimmerCallRecord{
		CallID: "call-1", Contract: protocol.ModelContractAnalyzeAgentState,
		InferenceProfile: profile, Status: protocol.GlimmerCallStatusOK, LatencyMS: latency,
	}
	return answer{res: protocol.InferResult{
		Contract: protocol.ModelContractAnalyzeAgentState, InferenceProfile: profile,
		Label: string(protocol.AgentPhaseRunningTool), Probabilities: probs,
		Evidence: []string{"model text sentinel_q7"}, CallRecord: rec,
	}}
}

func failAnswer(profile protocol.InferenceProfile, status protocol.GlimmerCallStatus, err error) answer {
	rec := protocol.GlimmerCallRecord{
		CallID: "call-1", Contract: protocol.ModelContractAnalyzeAgentState,
		InferenceProfile: profile, Status: status, LatencyMS: 7,
	}
	return answer{res: protocol.InferResult{Contract: rec.Contract, InferenceProfile: profile, CallRecord: rec}, err: err}
}

var runningProbs = map[string]float64{
	string(protocol.AgentPhaseRunningTool): 0.625,
	string(protocol.AgentPhaseThinking):    0.25,
	string(protocol.AgentPhaseUnknown):     0.125,
}

func TestPredictPassesProbabilitiesThrough(t *testing.T) {
	t.Parallel()
	m, ws := corpus(t, 4)
	f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
		return okAnswer(protocol.InferenceProfileBase, runningProbs, 42)
	}}
	out, sum, err := Predict(context.Background(), f, m, loaderOf(ws), PredictOptions{Profile: ProfileBase})
	require.NoError(t, err)
	require.Equal(t, 4, f.calls(), "the train window is not predicted")
	require.NotContains(t, string(out), "sentinel", "no model text in the file")
	require.NotContains(t, string(out), "compiling", "no screen in the file")

	ps, err := ReadPredictions(bytes.NewReader(out), ProfileBase)
	require.NoError(t, err, "the file passes the part-1 validator")
	require.Len(t, ps, 4)
	for _, p := range ps {
		require.Equal(t, [numLabels]float64{0, 0.25, 0.625, 0, 0, 0, 0.125}, p.Probs, "unchanged, onto A-G")
		require.InDelta(t, 1.0, p.ValidMass, 0)
		require.False(t, p.Invalid)
		require.Empty(t, p.ErrorClass)
		require.InDelta(t, 42.0, p.LatencyMS, 0)
		require.Equal(t, UnknownHarnessVersion, p.HarnessVersion)
		w := ws[p.WindowID]
		require.Equal(t, w.Split.Split, p.Split)
		require.Equal(t, w.Provenance.SessionID, p.SessionID)
		truth, _ := labelIndex(w.Target.Label)
		require.Equal(t, truth, p.Truth)
	}
	require.Equal(t, PredictSummary{Windows: 4, Valid: 4, Failed: map[ErrorClass]int{}, LatencyP50: 42, LatencyP95: 42}, sum)

	// A small drift the gateway admits is rescaled to sum to 1.
	f = &fakeInferrer{answer: func(protocol.InferRequest) answer {
		return okAnswer(protocol.InferenceProfileBase, map[string]float64{string(protocol.AgentPhaseIdleAtPrompt): 0.9995}, 1)
	}}
	out, _, err = Predict(context.Background(), f, m, loaderOf(ws), PredictOptions{Profile: ProfileBase})
	require.NoError(t, err)
	ps, err = ReadPredictions(bytes.NewReader(out), ProfileBase)
	require.NoError(t, err)
	require.InDelta(t, 1.0, ps[0].Probs[idxA], 1e-12)
}

func TestPredictPayloadIsTheTrainingInput(t *testing.T) {
	t.Parallel()
	m, ws := corpus(t, 2)
	f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
		return okAnswer(protocol.InferenceProfileBase, runningProbs, 1)
	}}
	_, _, err := Predict(context.Background(), f, m, loaderOf(ws), PredictOptions{Profile: ProfileBase})
	require.NoError(t, err)
	agents := map[protocol.AgentID]bool{}
	for _, req := range f.reqs {
		require.Equal(t, protocol.ModelContractAnalyzeAgentState, req.Contract)
		require.True(t, req.AgentID.Valid())
		agents[req.AgentID] = true
		var got analyzePayload
		dec := json.NewDecoder(bytes.NewReader([]byte(req.Payload)))
		dec.DisallowUnknownFields()
		require.NoError(t, dec.Decode(&got))
		in := ws[protocol.ULID(strings.ToUpper(strings.TrimPrefix(string(req.AgentID), "eval-")))].Input
		require.Equal(t, in.ScreenLines, got.ScreenLines)
		require.Equal(t, in.NewChildProcesses, got.NewChildProcesses)
		require.Equal(t, in.OutputBytes.Last500ms, got.OutputBytes.Last500ms)
		require.Contains(t, string(req.Payload), `"last_500ms":120`, "the gateway's wire name")
	}
	require.Len(t, agents, 2, "one agent per window: concurrent calls never meet ErrAgentBusy")
}

func TestPredictFailuresAreInvalidLines(t *testing.T) {
	t.Parallel()
	base := protocol.InferenceProfileBase
	cases := []struct {
		name  string
		a     answer
		class ErrorClass
	}{
		{"timeout", failAnswer(base, protocol.GlimmerCallStatusTimeout, gateway.ErrTimeout), ClassTimeout},
		{"circuit open", failAnswer(base, protocol.GlimmerCallStatusError, gateway.ErrCircuitOpen), ClassCircuitOpen},
		{"invalid output", failAnswer(base, protocol.GlimmerCallStatusInvalid, gateway.ErrInvalidOutput), ClassInvalidOutput},
		{"redaction", failAnswer(base, protocol.GlimmerCallStatusRedactionFailed, gateway.ErrRedactionFailed), ClassRedactionFailed},
		{"other error", failAnswer(base, protocol.GlimmerCallStatusError, errors.New("model said sentinel_q7")), ClassCallFailed},
		{"invalid request, no result", answer{err: fmt.Errorf("%w: sentinel_q7", gateway.ErrInvalidRequest)}, ClassInvalidRequest},
		{"status without error", failAnswer(base, protocol.GlimmerCallStatusInvalid, nil), ClassInvalidOutput},
		{"no probabilities", okAnswer(base, nil, 3), ClassBadProbabilities},
		{"outside A-G", okAnswer(base, map[string]float64{string(protocol.AgentPhaseBackgroundWork): 1}, 3), ClassBadProbabilities},
		{"invalid mass", okAnswer(base, map[string]float64{string(protocol.AgentPhaseThinking): 0.8}, 3), ClassBadProbabilities},
		{"out of range", okAnswer(base, map[string]float64{string(protocol.AgentPhaseThinking): 1.5, string(protocol.AgentPhaseUnknown): -0.5}, 3), ClassBadProbabilities},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, ws := corpus(t, 3)
			f := &fakeInferrer{answer: func(protocol.InferRequest) answer { return tc.a }}
			out, sum, err := Predict(context.Background(), f, m, loaderOf(ws), PredictOptions{Profile: ProfileBase})
			require.NoError(t, err, "a failed call is a line, not an abort")
			require.NotContains(t, string(out), "sentinel")
			ps, err := ReadPredictions(bytes.NewReader(out), ProfileBase)
			require.NoError(t, err)
			require.Len(t, ps, 3, "no window is dropped")
			for _, p := range ps {
				require.True(t, p.Invalid)
				require.InDelta(t, 0.0, p.ValidMass, 0)
				require.Equal(t, tc.class, p.ErrorClass)
				require.InDelta(t, 1.0, p.Probs[idxG], 0, "it counts as UNKNOWN")
			}
			require.Equal(t, map[ErrorClass]int{tc.class: 3}, sum.Failed)
			require.Zero(t, sum.Valid)
			require.Contains(t, strings.Join(sum.Lines(), "\n"), fmt.Sprintf("failed %s 3", tc.class))
		})
	}
}

func TestPredictAbortsOnAnotherProfile(t *testing.T) {
	t.Parallel()
	lora := protocol.InferenceProfileAnalystLoRA
	for name, a := range map[string]answer{
		"answer":        okAnswer(lora, runningProbs, 1),
		"failed record": failAnswer(lora, protocol.GlimmerCallStatusError, gateway.ErrCallFailed),
		"no profile":    okAnswer("", runningProbs, 1),
	} {
		m, ws := corpus(t, 3)
		f := &fakeInferrer{answer: func(protocol.InferRequest) answer { return a }}
		out, _, err := Predict(context.Background(), f, m, loaderOf(ws), PredictOptions{Profile: ProfileBase})
		require.ErrorIs(t, err, ErrPredict, name)
		require.Contains(t, err.Error(), "another inference profile", name)
		require.Nil(t, out, name)
	}
}

func TestPredictIsStableUnderConcurrency(t *testing.T) {
	t.Parallel()
	m, ws := corpus(t, 24)
	mk := func() *fakeInferrer {
		return &fakeInferrer{
			answer: func(req protocol.InferRequest) answer {
				if strings.HasSuffix(string(req.AgentID), "3") {
					return failAnswer(protocol.InferenceProfileAnalystLoRA, protocol.GlimmerCallStatusTimeout, gateway.ErrTimeout)
				}
				return okAnswer(protocol.InferenceProfileAnalystLoRA, runningProbs, int64(len(req.AgentID)))
			},
			// Later windows answer first.
			delay: func(req protocol.InferRequest) time.Duration {
				return time.Duration(100-int(req.AgentID[len(req.AgentID)-1])%100) * 50 * time.Microsecond
			},
		}
	}
	serial := mk()
	one, _, err := Predict(context.Background(), serial, m, loaderOf(ws), PredictOptions{Profile: ProfileLoRA, Concurrency: 1})
	require.NoError(t, err)
	require.EqualValues(t, 1, serial.peak.Load())
	parallel := mk()
	many, _, err := Predict(context.Background(), parallel, m, loaderOf(ws), PredictOptions{Profile: ProfileLoRA, Concurrency: 8})
	require.NoError(t, err)
	require.Equal(t, one, many, "same answers, same bytes")
	require.LessOrEqual(t, parallel.peak.Load(), int64(8))
	again, _, err := Predict(context.Background(), mk(), m, loaderOf(ws), PredictOptions{Profile: ProfileLoRA})
	require.NoError(t, err)
	require.Equal(t, one, again)

	ps, err := ReadPredictions(bytes.NewReader(many), ProfileLoRA)
	require.NoError(t, err)
	require.Len(t, ps, 24)
	for i := 1; i < len(ps); i++ {
		require.Less(t, ps[i-1].WindowID, ps[i].WindowID, "sorted by window_id")
	}
}

func TestPredictFailsClosedOnTheManifest(t *testing.T) {
	t.Parallel()
	m, ws := corpus(t, 4)
	var victim protocol.ULID
	for _, e := range m.Entries {
		if e.Split == dataset.SplitTest {
			victim = e.WindowID
			break
		}
	}
	cases := map[string]func(map[protocol.ULID]dataset.Window){
		"edited window": func(ws map[protocol.ULID]dataset.Window) {
			w := ws[victim]
			w.Input.SecondsSinceLastOutput = 9
			ws[victim] = w
		},
		"relabelled": func(ws map[protocol.ULID]dataset.Window) {
			w := ws[victim]
			w.Target.Label = dataset.LabelF
			ws[victim] = w
		},
		"other split": func(ws map[protocol.ULID]dataset.Window) {
			w := ws[victim]
			w.Split.Split = dataset.SplitValidation
			ws[victim] = w
		},
		"other window": func(ws map[protocol.ULID]dataset.Window) {
			for id, w := range ws {
				if id != victim {
					ws[victim] = w
					return
				}
			}
		},
		"missing window": func(ws map[protocol.ULID]dataset.Window) { delete(ws, victim) },
	}
	for name, mutate := range cases {
		local := map[protocol.ULID]dataset.Window{}
		for k, v := range ws {
			local[k] = v
		}
		mutate(local)
		f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
			return okAnswer(protocol.InferenceProfileBase, runningProbs, 1)
		}}
		out, _, err := Predict(context.Background(), f, m, loaderOf(local), PredictOptions{Profile: ProfileBase})
		require.ErrorIs(t, err, ErrPredict, name)
		require.Contains(t, err.Error(), string(victim), name)
		require.NotContains(t, err.Error(), "secret", name)
		require.Nil(t, out, name)
		require.Zero(t, f.calls(), "%s: checked before any call", name)
	}

	forged := m
	forged.Entries = append([]dataset.ManifestEntry(nil), m.Entries...)
	forged.Entries[1].Label = dataset.LabelF
	_, _, err := Predict(context.Background(), &fakeInferrer{}, forged, loaderOf(ws), PredictOptions{Profile: ProfileBase})
	require.ErrorIs(t, err, ErrPredict, "a manifest that does not validate")

	for _, opts := range []PredictOptions{{Profile: "other"}, {Profile: ProfileBase, Concurrency: -1}, {Profile: ProfileBase, Concurrency: 65}} {
		_, _, err := Predict(context.Background(), &fakeInferrer{}, m, loaderOf(ws), opts)
		require.ErrorIs(t, err, ErrPredict)
	}
}

func TestPredictStopsWhenCancelled(t *testing.T) {
	t.Parallel()
	m, ws := corpus(t, 12)
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
		cancel()
		return failAnswer(protocol.InferenceProfileBase, protocol.GlimmerCallStatusError, context.Canceled)
	}}
	out, _, err := Predict(ctx, f, m, loaderOf(ws), PredictOptions{Profile: ProfileBase, Concurrency: 2})
	require.ErrorIs(t, err, ErrPredict)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, out)
	require.Less(t, f.calls(), 12)
}

func TestErrorClassOnlyOnFailedLines(t *testing.T) {
	t.Parallel()
	const timeout = `,"error_class":"timeout"}`
	ok := line{n: 1, split: splitVal, truth: "A", probs: map[string]float64{"A": 1}, mass: 1, latency: 1, harness: "codex", version: "1", profile: string(ProfileBase)}
	bad := ok
	bad.probs, bad.mass = map[string]float64{"G": 1}, 0
	for name, tc := range map[string]struct {
		l    line
		tail string
		ok   bool
	}{
		"failed line":      {bad, timeout, true},
		"no class":         {bad, `}`, true},
		"unknown class":    {bad, `,"error_class":"sentinel_q7"}`, false},
		"null class":       {bad, `,"error_class":null}`, false},
		"number class":     {bad, `,"error_class":1}`, false},
		"class on a valid": {ok, timeout, false},
		"class with mass":  {line{n: 1, split: splitVal, truth: "A", probs: map[string]float64{"G": 1}, mass: 0.25, latency: 1, profile: string(ProfileBase)}, timeout, false},
	} {
		js := tc.l.json()
		if tc.l.mass == 0 {
			// line writes a zero mass as 1.
			js = strings.Replace(js, `"valid_mass":1,"invalid":false`, `"valid_mass":0,"invalid":true`, 1)
		}
		js = strings.TrimSuffix(js, "}") + tc.tail
		_, err := ReadPredictions(strings.NewReader(js+"\n"), ProfileBase)
		if tc.ok {
			require.NoError(t, err, name)
			continue
		}
		require.ErrorIs(t, err, ErrPredictions, name)
		require.NotContains(t, err.Error(), "sentinel", name)
	}
}

// Through the real gateway, with a fake endpoint: the payload passes the
// gateway's field schema and the Analyst's prompt, unchanged.
type stubEndpoint struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (e *stubEndpoint) Send(_ context.Context, body []byte) ([]byte, error) {
	e.mu.Lock()
	e.bodies = append(e.bodies, body)
	e.mu.Unlock()
	return []byte(`{"label":"RUNNING_TOOL","probabilities":{"RUNNING_TOOL":0.75,"UNKNOWN":0.25},"evidence":["go test"]}`), nil
}

type nopRecorder struct{}

func (nopRecorder) RecordCall(context.Context, protocol.GlimmerCallRecord) error { return nil }

type seqIDs struct{ n atomic.Int64 }

func (s *seqIDs) NewCallID() protocol.OpaqueID {
	return protocol.OpaqueID(fmt.Sprintf("call-%d", s.n.Add(1)))
}

const predictGatewayYAML = `
endpoint:
  url_env: U
  token_env: T
base_model:
  repo: example.invalid/glimmer
  revision: v1
runtime: vllm
profiles:
  - contract: ANALYZE_AGENT_STATE
    inference_profile: base
  - contract: TRIAGE_MESSAGE
    inference_profile: base
  - contract: CHOOSE_ORCHESTRATION_ACTION
    inference_profile: base
`

func TestPredictThroughTheGateway(t *testing.T) {
	t.Parallel()
	cfg, err := gateway.ParseConfig(strings.NewReader(predictGatewayYAML), func(k string) string {
		return map[string]string{"U": "http://127.0.0.1:1", "T": "test-token"}[k]
	})
	require.NoError(t, err)
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	ep := &stubEndpoint{}
	gw, err := gateway.New(cfg, gateway.Deps{
		Transport: ep, Redactor: red, Recorder: nopRecorder{}, Clock: gateway.SystemClock{},
		IDs: &seqIDs{}, Prompts: analyst.Prompts{},
	})
	require.NoError(t, err)
	m, ws := corpus(t, 6)
	out, sum, err := Predict(context.Background(), gw, m, loaderOf(ws), PredictOptions{Profile: ProfileBase})
	require.NoError(t, err)
	require.Equal(t, 6, sum.Valid, "every payload passes the gateway: %v", sum.Failed)
	ps, err := ReadPredictions(bytes.NewReader(out), ProfileBase)
	require.NoError(t, err)
	require.InDelta(t, 0.75, ps[0].Probs[2], 0)
	require.Len(t, ep.bodies, 6)
	require.Contains(t, string(ep.bodies[0]), `"prompt_version":"`+analyst.PromptVersion+`"`, "the Analyst's prompt, none of its own")

	// Asking for the LoRA of a gateway that serves base aborts.
	_, _, err = Predict(context.Background(), gw, m, loaderOf(ws), PredictOptions{Profile: ProfileLoRA})
	require.ErrorIs(t, err, ErrPredict)
}
