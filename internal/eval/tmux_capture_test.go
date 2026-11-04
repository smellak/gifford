package eval

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

func TestPredictTmuxCapturePreservesWireInput(t *testing.T) {
	w := evalWindow(1, dataset.SplitValidation, dataset.LabelA)
	w.Input.SecondsSinceLastOutput = 0
	w.Input.OutputBytes = dataset.OutputBytes{}
	w.Input.NewChildProcesses = nil
	w.Input.PreviousState = ""
	w.Input.PreviousStateAgeSeconds = 0
	w.Screen.ObservationForm = dataset.ObservationFormTmuxCaptureV1
	w.Provenance.SourceQuality = protocol.SourceQualityBootstrapImport
	w.Provenance.ActivityVersion = dataset.TmuxCaptureActivityVersion
	w.Provenance.SequenceFrom, w.Provenance.SequenceTo = 0, 0
	w.Provenance.WindowStart = w.Provenance.AnchorAt
	w.Provenance.WindowEnd = w.Provenance.AnchorAt
	m, err := dataset.BuildTmuxCaptureManifest([]dataset.Window{w})
	require.NoError(t, err)
	payload, err := payloadOf(w.Input)
	require.NoError(t, err)
	require.JSONEq(t, `{"harness":"claude-code","seconds_since_last_output":0,"output_bytes":{"last_500ms":0,"last_2s":0,"last_10s":0},"new_child_processes":null,"previous_state":"","previous_state_age_seconds":0,"screen_lines":["step 3 of 7: compiling","tests: 1 passed, 0 failed"]}`, string(payload))
	// Training rows render these messages before tokenization. Predict must
	// render byte-identical text from the same window, including zero/null.
	trainingPrompt, err := (analyst.Prompts{}).RenderPrompt(w.Contract, payload)
	require.NoError(t, err)
	f := &fakeInferrer{answer: func(req protocol.InferRequest) answer {
		evalPrompt, err := (analyst.Prompts{}).RenderPrompt(req.Contract, []byte(req.Payload))
		require.NoError(t, err)
		require.Equal(t, trainingPrompt, evalPrompt)
		return okAnswer(protocol.InferenceProfileBase, runningProbs, 1)
	}}
	_, sum, err := Predict(context.Background(), f, m, loaderOf(map[protocol.ULID]dataset.Window{w.WindowID: w}), PredictOptions{Profile: ProfileBase})
	require.NoError(t, err)
	require.Equal(t, 1, f.calls(), "offline evaluator reads the tmux form without changing PTY validation")
	require.Equal(t, 1, sum.Valid)

	// The live Analyst also renders the same defaults. Supplying a previous
	// state here proves that the served LoRA profile drops it as tmux does.
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	live, err := analyst.New(f, screenproj.Policies{Lines: 24, Redactor: red, Mask: screenproj.NoMask()}, analyst.Served(protocol.InferenceProfileAnalystLoRA))
	require.NoError(t, err)
	_, err = live.Analyze(context.Background(), analyst.Observation{
		AgentID: "agent-fixture", Harness: w.Input.Harness, HarnessVersion: w.Provenance.HarnessVersion,
		ObservedAt: w.Provenance.AnchorAt, Screen: w.Input.ScreenLines, Cols: int(w.Screen.Cols),
		Cursor: screenproj.Cursor{Known: false, Visible: true}, GuardVersion: 1,
		Previous: &analyst.PreviousState{Phase: protocol.AgentPhaseThinking, Since: w.Provenance.AnchorAt},
		Guard: &protocol.InputGuardReport{
			AgentID: "agent-fixture", SessionID: "session-fixture", DraftState: protocol.DraftStateClosed,
			ActivityState: protocol.ActivityStateIdle, InputGuardVersion: 1,
			DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, f.calls(), "live Analyst and offline Predict rendered the same training prompt")
}
