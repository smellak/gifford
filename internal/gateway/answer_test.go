package gateway

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// triageRequest is a TRIAGE_MESSAGE request with no secret in it.
var triageRequest = protocol.InferRequest{
	Contract: protocol.ModelContractTriageMessage,
	Payload:  protocol.Payload(`{"text":"please review the parser change"}`),
}

// chooseRequest is a CHOOSE_ORCHESTRATION_ACTION request that allows
// exactly the given actions.
func chooseRequest(t *testing.T, allowed ...protocol.OrchestratorAction) protocol.InferRequest {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(orchestrationPayload(t, "tests: 9 passed"), &doc))
	actions, err := json.Marshal(allowed)
	require.NoError(t, err)
	doc["allowed_actions"] = actions
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return protocol.InferRequest{Contract: protocol.ModelContractChooseOrchestrationAction, Payload: protocol.Payload(b)}
}

// inferAnswer runs req against an endpoint that gives answer.
func inferAnswer(t *testing.T, req protocol.InferRequest, answer string) (fixture, protocol.InferResult, error) {
	t.Helper()
	f := newFixture(t, "http://127.0.0.1:1", &captureTransport{answer: []byte(answer)}, nil)
	res, err := f.gw.Infer(context.Background(), req)
	return f, res, err
}

// requireInvalid checks a call whose answer could not be read.
func requireInvalid(t *testing.T, f fixture, res protocol.InferResult, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrInvalidOutput)
	require.Equal(t, protocol.GlimmerCallStatusInvalid, res.CallRecord.Status)
	require.Empty(t, res.Label, "an unread answer gives no label")
	require.Empty(t, res.Probabilities)
	require.Empty(t, res.Evidence)
	require.Empty(t, res.CallRecord.Probabilities, "nor a recorded distribution")
	require.Len(t, f.recorder.all(), 1, "it still leaves its record")
	require.NoError(t, res.CallRecord.Validate())
}

func TestLetterAnswers(t *testing.T) {
	t.Parallel()
	reason80 := strings.Repeat("é", maxReasonRunes)
	normal := string(protocol.TriageLabelNormal)
	queue := string(protocol.OrchestratorActionQueue)
	deferAction := string(protocol.OrchestratorActionDefer)
	choose := chooseRequest(t, protocol.OrchestratorActionQueue, protocol.OrchestratorActionDefer)
	cases := []struct {
		name   string
		req    protocol.InferRequest
		answer string
		label  string // empty: the answer cannot be read
	}{
		{"triage", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":0.7,"LOW_PRIORITY":0.3},"letter_mass":0.9}`, normal},
		{"triage at the mass bar", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":0.5}`, normal},
		{"triage with all the mass", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":1}`, normal},
		{"triage without a mass", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1}}`, ""},
		{"triage with a null mass", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":null}`, ""},
		{"triage below the mass bar", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":0.49}`, ""},
		{"triage with a mass above one", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":1.2}`, ""},
		{"triage with a negative mass", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":-0.1}`, ""},
		{"triage with a huge mass", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":1e999}`, ""},
		{"triage tied at the top", triageRequest, `{"label":"NORMAL","probabilities":{"NORMAL":0.5,"LOW_PRIORITY":0.5},"letter_mass":0.9}`, ""},
		{"triage label not the most probable", triageRequest, `{"label":"LOW_PRIORITY","probabilities":{"NORMAL":0.7,"LOW_PRIORITY":0.3},"letter_mass":0.9}`, ""},
		{"triage label without a probability", triageRequest, `{"label":"REQUIRES_HUMAN","probabilities":{"NORMAL":1},"letter_mass":0.9}`, ""},
		{"choose", choose, `{"label":"QUEUE","probabilities":{"QUEUE":0.8,"DEFER":0.2},"letter_mass":0.95,"evidence":["the agent is at its prompt"]}`, queue},
		{"choose with an 80-character reason", choose, `{"label":"DEFER","probabilities":{"DEFER":1},"letter_mass":0.6,"evidence":["` + reason80 + `"]}`, deferAction},
		{"choose an action not allowed", choose, `{"label":"EXPIRE","probabilities":{"EXPIRE":0.9,"DEFER":0.1},"letter_mass":0.95,"evidence":["the message is old"]}`, ""},
		{"choose weighing an action not allowed", choose, `{"label":"QUEUE","probabilities":{"QUEUE":0.8,"DEFER":0.1,"EXPIRE":0.1},"letter_mass":0.95,"evidence":["the agent is at its prompt"]}`, ""},
		{"choose tied at the top", choose, `{"label":"QUEUE","probabilities":{"QUEUE":0.5,"DEFER":0.5},"letter_mass":0.95,"evidence":["either would do"]}`, ""},
		{"choose label not the most probable", choose, `{"label":"DEFER","probabilities":{"QUEUE":0.8,"DEFER":0.2},"letter_mass":0.95,"evidence":["wait"]}`, ""},
		{"choose without a mass", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"evidence":["the agent is at its prompt"]}`, ""},
		{"choose below the mass bar", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"letter_mass":0.3,"evidence":["the agent is at its prompt"]}`, ""},
		{"choose without a reason", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"letter_mass":0.95}`, ""},
		{"choose with no reasons", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"letter_mass":0.95,"evidence":[]}`, ""},
		{"choose with a blank reason", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"letter_mass":0.95,"evidence":["   "]}`, ""},
		{"choose with two reasons", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"letter_mass":0.95,"evidence":["idle","quiet"]}`, ""},
		{"choose with an 81-character reason", choose, `{"label":"QUEUE","probabilities":{"QUEUE":1},"letter_mass":0.95,"evidence":["` + reason80 + `x"]}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, res, err := inferAnswer(t, tc.req, tc.answer)
			if tc.label == "" {
				requireInvalid(t, f, res, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, protocol.GlimmerCallStatusOK, res.CallRecord.Status)
			require.Equal(t, tc.label, res.Label)
			require.Equal(t, res.Probabilities, res.CallRecord.Probabilities)
			require.NoError(t, res.Validate())
		})
	}
}

// The allowed set is the one of the request being answered, not the
// contract's twelve actions.
func TestChooseReadsItsOwnRequest(t *testing.T) {
	t.Parallel()
	answer := `{"label":"DEFER","probabilities":{"DEFER":0.9,"QUEUE":0.1},"letter_mass":0.95,"evidence":["the human is typing"]}`
	_, res, err := inferAnswer(t, chooseRequest(t, protocol.OrchestratorActionQueue, protocol.OrchestratorActionDefer), answer)
	require.NoError(t, err)
	require.Equal(t, string(protocol.OrchestratorActionDefer), res.Label)

	f, res, err := inferAnswer(t, chooseRequest(t, protocol.OrchestratorActionQueue, protocol.OrchestratorActionRetry), answer)
	requireInvalid(t, f, res, err)
}

// The reason is read and dropped: no result, record or error carries it,
// while the output digest still covers the answer that held it.
func TestChooseReasonNeverLeaves(t *testing.T) {
	t.Parallel()
	const canary = "reason-canary-5e2"
	answer := `{"label":"QUEUE","probabilities":{"QUEUE":0.8,"DEFER":0.2},"letter_mass":0.95,"evidence":["` + canary + `"]}`
	f, res, err := inferAnswer(t, chooseRequest(t, protocol.OrchestratorActionQueue, protocol.OrchestratorActionDefer), answer)
	require.NoError(t, err)
	require.Equal(t, string(protocol.OrchestratorActionQueue), res.Label, "the answer was read")
	require.Empty(t, res.Evidence)
	enc, mErr := json.Marshal(res)
	require.NoError(t, mErr)
	require.NotContains(t, string(enc), canary)
	recs := f.recorder.all()
	require.Len(t, recs, 1)
	stored, mErr := json.Marshal(recs[0])
	require.NoError(t, mErr)
	require.NotContains(t, string(stored), canary)
	require.Equal(t, sha256Hex([]byte(answer)), recs[0].OutputSHA256)
}

// A TRIAGE_MESSAGE result keeps its evidence as before.
func TestTriageKeepsItsEvidence(t *testing.T) {
	t.Parallel()
	_, res, err := inferAnswer(t, triageRequest,
		`{"label":"NORMAL","probabilities":{"NORMAL":1},"letter_mass":0.9,"evidence":["asks for a review"]}`)
	require.NoError(t, err)
	require.Equal(t, []string{"asks for a review"}, res.Evidence)
}

// ANALYZE_AGENT_STATE answers keep their own rules: no mass is needed,
// and neither the mass bar nor the top of the distribution is read here.
// Only a mass outside [0, 1] is refused, as on any answer.
func TestAnalyzeAnswersKeepTheirRules(t *testing.T) {
	t.Parallel()
	running, unknown := string(protocol.AgentPhaseRunningTool), string(protocol.AgentPhaseUnknown)
	for name, tc := range map[string]struct {
		answer string
		label  string
	}{
		"no mass":           {`{"label":"RUNNING_TOOL","probabilities":{"RUNNING_TOOL":0.8,"UNKNOWN":0.2}}`, running},
		"low mass, unknown": {`{"label":"UNKNOWN","probabilities":{"IDLE_AT_PROMPT":0.9,"UNKNOWN":0.1},"letter_mass":0.3}`, unknown},
		"tied at the top":   {`{"label":"RUNNING_TOOL","probabilities":{"RUNNING_TOOL":0.5,"UNKNOWN":0.5}}`, running},
		"mass above one":    {`{"label":"RUNNING_TOOL","probabilities":{"RUNNING_TOOL":1},"letter_mass":2}`, ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f, res, err := inferAnswer(t, analyzeRequest("x"), tc.answer)
			if tc.label == "" {
				requireInvalid(t, f, res, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.label, res.Label)
		})
	}
}
