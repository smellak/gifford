package eval

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/analyst"
	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
	"github.com/smellak/gifford/internal/screenproj"
)

// oneRecording yields one recording, then io.EOF.
type oneRecording struct {
	rec  dataset.Recording
	done bool
}

func (o *oneRecording) Next(context.Context) (dataset.Recording, error) {
	if o.done {
		return dataset.Recording{}, io.EOF
	}
	o.done = true
	return o.rec, nil
}

// TestTrainingAndServingPayloadsMatch pins the one screen projection:
// for the same screen, input guard reading and policies, the payload of
// the window the dataset build writes, as eval sends it, and the payload
// the Analyst sends in serving are the same bytes, under the same N and
// policy versions.
func TestTrainingAndServingPayloadsMatch(t *testing.T) {
	t.Parallel()
	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	mask, err := screenproj.NewMask([]string{"SentinelCorp"})
	require.NoError(t, err)
	policies := screenproj.Policies{Lines: 12, Redactor: red, Mask: mask}
	const canary = "HUMAN-CANARY-p4Xe"

	cases := map[string]struct {
		rows  []string
		draft dataset.DraftState
		guard protocol.DraftState
	}{
		"a secret": {
			rows:  []string{"$ go test ./...", "export GITHUB_TOKEN=" + secret, "ok  example.invalid/pkg 0.2s", ""},
			draft: dataset.DraftClosed, guard: protocol.DraftStateClosed,
		},
		"a listed name": {
			rows:  []string{"deploy to SentinelCorp staging", "sentinelcorp: 3 hosts", "done"},
			draft: dataset.DraftClosed, guard: protocol.DraftStateClosed,
		},
		"an open draft, a blank screen": {
			rows:  []string{"build ok", "", "> " + canary},
			draft: dataset.DraftOpen, guard: protocol.DraftStateOpen,
		},
		"inert controls inside a secret": {
			rows:  []string{"$ env", "GITHUB_TOKEN=" + secret[:5] + "\a" + secret[5:10] + "\x00" + secret[10:15] + "\x7f" + secret[15:]},
			draft: dataset.DraftClosed, guard: protocol.DraftStateClosed,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			window, err := trainingWindow(policies, tc.rows, tc.draft)
			require.NoError(t, err)
			training, err := payloadOf(window.Input)
			require.NoError(t, err)
			f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
				return okAnswer(protocol.InferenceProfileBase, runningProbs, 1)
			}}
			got, err := serve(t, f, policies, tc.rows, tc.guard)
			require.NoError(t, err)
			require.Equal(t, 1, f.calls())
			serving := []byte(f.reqs[0].Payload)

			require.Equal(t, string(training), string(serving), "the same bytes in training and serving")
			for _, part := range []string{secret[:5], secret[5:10], secret[10:15], secret[15:]} {
				require.NotContains(t, string(serving), part)
			}
			require.NotContains(t, strings.ToLower(string(serving)), "sentinelcorp")
			require.NotContains(t, string(serving), canary)
			require.Equal(t, policies.Lines, got.ScreenLines, "the same N")
			require.Equal(t, dataset.Policies{
				RedactionPolicyVersion:  got.Policies.Redaction,
				HumanInputPolicyVersion: got.Policies.HumanInput,
				DomainMaskPolicyVersion: got.Policies.DomainMask,
			}, window.Policies, "the same policy versions")
			require.Equal(t, mask.Version(), window.Policies.DomainMaskPolicyVersion)
		})
	}

	// A control with an effect on the screen, or a byte that is not
	// UTF-8, inside a secret: both paths refuse the screen, and nothing
	// comes out of either.
	for name, mark := range map[string]string{
		"an escape sequence": "\x1b[0m", "a backspace": "\b", "a carriage return": "\r",
		"a tab": "\t", "a C1 control": "\u009b", "a byte not UTF-8": "\xff",
	} {
		rows := []string{"$ env", "GITHUB_TOKEN=" + secret[:12] + mark + secret[12:]}
		_, err := trainingWindow(policies, rows, dataset.DraftClosed)
		require.ErrorIs(t, err, screenproj.ErrInvalidInput, "%s: no window", name)
		f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
			t.Errorf("%s: the gateway was called", name)
			return answer{}
		}}
		_, err = serve(t, f, policies, rows, protocol.DraftStateClosed)
		require.ErrorIs(t, err, analyst.ErrScreenWithheld, name)
		require.ErrorIs(t, err, screenproj.ErrInvalidInput, name)
		require.Zero(t, f.calls(), "%s: no body", name)
	}
}

// trainingWindow is the window the dataset pipeline builds from one
// screen, as gifford dataset build would.
func trainingWindow(policies screenproj.Policies, rows []string, draft dataset.DraftState) (dataset.Window, error) {
	w := evalWindow(1, dataset.SplitTest, dataset.LabelC)
	features := w.Input
	features.ScreenLines = nil
	inv, err := dataset.RunPipeline(context.Background(), dataset.PipelineDeps{
		Source: &oneRecording{rec: dataset.Recording{
			WindowID: w.WindowID, Contract: w.Contract, Features: features,
			Screen: redact.Screen{Rows: rows, Cols: 80},
			Label:  w.Target.Label, LabelSource: w.Target.Source, Provenance: w.Provenance,
			EnvironmentDomain: w.EnvironmentDomain, GroupDigest: w.Split.GroupDigest,
			Draft: draft, Cursor: lastRow(rows),
		}},
		Projection: policies,
		Splitter:   dataset.SplitV1{},
	}, dataset.PipelineOptions{MaxWindows: 1})
	if err != nil {
		return dataset.Window{}, err
	}
	return inv.Windows[0], nil
}

// serve has the Analyst observe the same screen, with the features of
// evalWindow and a guard report of the given draft state.
func serve(t *testing.T, gw analyst.Inferrer, policies screenproj.Policies, rows []string, draft protocol.DraftState) (analyst.Analysis, error) {
	t.Helper()
	w := evalWindow(1, dataset.SplitTest, dataset.LabelC)
	in, observedAt := w.Input, w.Provenance.AnchorAt
	a, err := analyst.New(gw, policies)
	require.NoError(t, err)
	return a.Analyze(context.Background(), analyst.Observation{
		AgentID: "agent-1", Harness: in.Harness, HarnessVersion: w.Provenance.HarnessVersion, ObservedAt: observedAt,
		Screen: rows,
		Activity: analyst.Activity{
			BytesLast500ms: in.OutputBytes.Last500ms, BytesLast2s: in.OutputBytes.Last2s, BytesLast10s: in.OutputBytes.Last10s,
			SecondsSinceLastOutput: in.SecondsSinceLastOutput,
		},
		ChildProcesses: in.NewChildProcesses,
		Previous: &analyst.PreviousState{
			Phase: in.PreviousState,
			Since: observedAt.Add(-time.Duration(in.PreviousStateAgeSeconds * float64(time.Second))),
		},
		Cols: 80, Cursor: lastRow(rows),
		Guard: &protocol.InputGuardReport{
			AgentID: "agent-1", SessionID: "sess-1", DraftState: draft, ActivityState: protocol.ActivityStateIdle,
			InputGuardVersion: 7, DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
		},
		GuardVersion: 7,
	})
}

// lastRow is a cursor known and shown on the last row.
func lastRow(rows []string) screenproj.Cursor {
	return screenproj.Cursor{Row: len(rows) - 1, Known: true, Visible: true}
}
