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
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w := evalWindow(1, dataset.SplitTest, dataset.LabelC)
			cursor := screenproj.Cursor{Row: len(tc.rows) - 1, Known: true, Visible: true}

			// Training: the dataset pipeline builds the window.
			features := w.Input
			features.ScreenLines = nil
			inv, err := dataset.RunPipeline(context.Background(), dataset.PipelineDeps{
				Source: &oneRecording{rec: dataset.Recording{
					WindowID: w.WindowID, Contract: w.Contract, Features: features,
					Screen: redact.Screen{Rows: tc.rows, Cols: 80},
					Label:  w.Target.Label, LabelSource: w.Target.Source, Provenance: w.Provenance,
					EnvironmentDomain: w.EnvironmentDomain, GroupDigest: w.Split.GroupDigest,
					Draft: tc.draft, Cursor: cursor,
				}},
				Projection: policies,
				Splitter:   dataset.SplitV1{},
			}, dataset.PipelineOptions{MaxWindows: 1})
			require.NoError(t, err)
			require.Len(t, inv.Windows, 1)
			window := inv.Windows[0]
			training, err := payloadOf(window.Input)
			require.NoError(t, err)

			// Serving: the Analyst observes the same screen.
			in := window.Input
			observedAt := w.Provenance.AnchorAt
			f := &fakeInferrer{answer: func(protocol.InferRequest) answer {
				return okAnswer(protocol.InferenceProfileBase, runningProbs, 1)
			}}
			a, err := analyst.New(f, policies)
			require.NoError(t, err)
			got, err := a.Analyze(context.Background(), analyst.Observation{
				AgentID: "agent-1", Harness: in.Harness, HarnessVersion: w.Provenance.HarnessVersion, ObservedAt: observedAt,
				Screen: tc.rows,
				Activity: analyst.Activity{
					BytesLast500ms: in.OutputBytes.Last500ms, BytesLast2s: in.OutputBytes.Last2s, BytesLast10s: in.OutputBytes.Last10s,
					SecondsSinceLastOutput: in.SecondsSinceLastOutput,
				},
				ChildProcesses: in.NewChildProcesses,
				Previous: &analyst.PreviousState{
					Phase: in.PreviousState,
					Since: observedAt.Add(-time.Duration(in.PreviousStateAgeSeconds * float64(time.Second))),
				},
				Cols: 80, Cursor: cursor,
				Guard: &protocol.InputGuardReport{
					AgentID: "agent-1", SessionID: "sess-1", DraftState: tc.guard, ActivityState: protocol.ActivityStateIdle,
					InputGuardVersion: 7, DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
				},
				GuardVersion: 7,
			})
			require.NoError(t, err)
			require.Equal(t, 1, f.calls())
			serving := []byte(f.reqs[0].Payload)

			require.Equal(t, string(training), string(serving), "the same bytes in training and serving")
			require.NotContains(t, string(serving), secret)
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
}
