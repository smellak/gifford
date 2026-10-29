package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// testDigest is an invented sha256 digest over a fixture label: the
// fixtures reference bytes that never existed.
func testDigest(label string) protocol.Digest {
	sum := sha256.Sum256([]byte(label))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// validWindow is the minimal valid window. Every value is invented.
func validWindow() Window {
	anchor := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"step 3 of 7: compiling", "tests: 12 passed, 0 failed"}
	return Window{
		SchemaVersion: WindowSchemaVersion,
		WindowID:      "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Input: AnalyzeInput{
			Contract:                protocol.ModelContractAnalyzeAgentState,
			Harness:                 protocol.HarnessClaudeCode,
			SecondsSinceLastOutput:  0.2,
			OutputBytes:             OutputBytes{Last500ms: 120, Last2s: 480, Last10s: 900},
			NewChildProcesses:       1,
			PreviousState:           protocol.AgentPhaseRunningTool,
			PreviousStateAgeSeconds: 4.5,
			Screen: RedactedScreen{
				Redacted: true, Cols: 80, Rows: 24,
				Lines: lines, Digest: ScreenDigest(lines),
			},
		},
		Target: Target{Label: LabelC},
		Provenance: Provenance{
			SessionID:        "cap-01ARZ3NDEKTSV4RRFFQ69G5FAV",
			CaptureSessionID: "cap-01ARZ3NDEKTSV4RRFFQ69G5FAV",
			SourceQuality:    protocol.SourceQualitySynthetic,
			SequenceFrom:     2,
			SequenceTo:       40,
			WindowStart:      anchor.Add(-30 * time.Second),
			WindowEnd:        anchor,
			AnchorAt:         anchor,
			SourceRefs:       []protocol.BlobRef{"blob://capture/cap-01ARZ3NDEKTSV4RRFFQ69G5FAV/seg-01ARZ3NDEKTSV4RRFFQ69G5FAW/0"},
			SourceDigest:     testDigest("synthetic source"),
		},
		EnvironmentDomain: EnvironmentDomainBankGeneric,
		Policies:          Policies{RedactionPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
		Split:             SplitAssignment{Split: SplitTrain, GroupDigest: testDigest("synthetic group")},
	}
}

func TestWindowValidMinimal(t *testing.T) {
	t.Parallel()
	require.NoError(t, validWindow().Validate())

	first := validWindow()
	first.Input.PreviousState = ""
	first.Input.PreviousStateAgeSeconds = 0
	require.NoError(t, first.Validate(), "a session's first window has no previous state")

	idle := validWindow()
	idle.Input.OutputBytes = OutputBytes{}
	idle.Input.SecondsSinceLastOutput = 12
	idle.Input.Screen.Lines = nil
	idle.Input.Screen.Digest = ScreenDigest(nil)
	require.NoError(t, idle.Validate(), "an idle pane with an empty screen is valid")
}

func TestWindowInvalid(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*Window)
		field  string
	}{
		// Domain, label, split and source.
		{"unknown domain", func(w *Window) { w.EnvironmentDomain = "bank" }, fieldDomain},
		{"near-miss domain", func(w *Window) { w.EnvironmentDomain = "fleet" }, fieldDomain},
		{"empty domain", func(w *Window) { w.EnvironmentDomain = "" }, fieldDomain},
		{"label past G", func(w *Window) { w.Target.Label = "H" }, fieldLabel},
		{"label lowercase", func(w *Window) { w.Target.Label = "a" }, fieldLabel},
		{"label as phase name", func(w *Window) { w.Target.Label = Label(protocol.AgentPhaseRunningTool) }, fieldLabel},
		{"unknown split", func(w *Window) { w.Split.Split = "dev" }, "split.split"},
		{"split without group", func(w *Window) { w.Split.GroupDigest = "" }, "split.group_digest"},
		{"human annotation source", func(w *Window) { w.Provenance.SourceQuality = protocol.SourceQualityHumanAnnotation }, fieldSourceQuality},
		{"transcript source", func(w *Window) { w.Provenance.SourceQuality = protocol.SourceQualityTranscript }, fieldSourceQuality},
		{"unknown source", func(w *Window) { w.Provenance.SourceQuality = "camera" }, fieldSourceQuality},
		// Contract and envelope.
		{"other contract", func(w *Window) { w.Input.Contract = protocol.ModelContractTriageMessage }, "input.contract"},
		{"unknown harness", func(w *Window) { w.Input.Harness = "editor" }, "input.harness"},
		{"schema version", func(w *Window) { w.SchemaVersion = 2 }, "schema_version"},
		{"window id", func(w *Window) { w.WindowID = "w-1" }, "window_id"},
		{"unknown previous state", func(w *Window) { w.Input.PreviousState = "SLEEPING" }, "input.previous_state"},
		{"negative children", func(w *Window) { w.Input.NewChildProcesses = -1 }, "input.new_child_processes"},
		// Incoherent times.
		{"window end before start", func(w *Window) {
			w.Provenance.WindowStart, w.Provenance.WindowEnd = w.Provenance.WindowEnd, w.Provenance.WindowStart
		}, fieldWindowEnd},
		{"empty window", func(w *Window) { w.Provenance.WindowStart = w.Provenance.WindowEnd }, fieldWindowEnd},
		{"anchor after window", func(w *Window) { w.Provenance.AnchorAt = w.Provenance.WindowEnd.Add(time.Second) }, fieldAnchorAt},
		{"anchor before window", func(w *Window) { w.Provenance.AnchorAt = w.Provenance.WindowStart.Add(-time.Second) }, fieldAnchorAt},
		{"missing anchor", func(w *Window) { w.Provenance.AnchorAt = time.Time{} }, "provenance"},
		{"reversed sequence", func(w *Window) { w.Provenance.SequenceFrom, w.Provenance.SequenceTo = 40, 2 }, "provenance.sequence"},
		{"negative sequence", func(w *Window) { w.Provenance.SequenceFrom = -1 }, "provenance.sequence"},
		{"negative since last output", func(w *Window) { w.Input.SecondsSinceLastOutput = -0.1 }, fieldSinceLastOutput},
		{"negative previous age", func(w *Window) { w.Input.PreviousStateAgeSeconds = -1 }, fieldPreviousAge},
		{"age without previous state", func(w *Window) { w.Input.PreviousState = "" }, fieldPreviousAge},
		{"nested windows decrease", func(w *Window) { w.Input.OutputBytes.Last2s = 100 }, fieldOutputBytes},
		{"negative bytes", func(w *Window) { w.Input.OutputBytes.Last500ms = -1 }, fieldOutputBytes},
		{"recent bytes but old output", func(w *Window) { w.Input.SecondsSinceLastOutput = 3 }, fieldSinceLastOutput},
		{"no bytes but recent output", func(w *Window) {
			w.Input.OutputBytes = OutputBytes{}
			w.Input.SecondsSinceLastOutput = 1
		}, fieldSinceLastOutput},
		// The screen.
		{"screen not redacted", func(w *Window) { w.Input.Screen.Redacted = false }, "input.screen.redacted"},
		{"raw escape in screen", func(w *Window) {
			w.Input.Screen.Lines = []string{"\x1b[32mok\x1b[0m"}
			w.Input.Screen.Digest = ScreenDigest(w.Input.Screen.Lines)
		}, fieldScreenLines},
		{"carriage return in screen", func(w *Window) {
			w.Input.Screen.Lines = []string{"ok\r"}
			w.Input.Screen.Digest = ScreenDigest(w.Input.Screen.Lines)
		}, fieldScreenLines},
		{"screen digest mismatch", func(w *Window) { w.Input.Screen.Lines = []string{"edited"} }, "input.screen.digest"},
		{"more lines than rows", func(w *Window) { w.Input.Screen.Rows = 1 }, fieldScreenLines},
		{"zero size screen", func(w *Window) { w.Input.Screen.Cols = 0 }, "input.screen"},
		// Provenance and hashes.
		{"missing session", func(w *Window) { w.Provenance.SessionID = "" }, "provenance.session_id"},
		{"missing capture session", func(w *Window) { w.Provenance.CaptureSessionID = "" }, "provenance.capture_session_id"},
		{"missing source digest", func(w *Window) { w.Provenance.SourceDigest = "" }, fieldSourceDigest},
		{"bare hex source digest", func(w *Window) {
			w.Provenance.SourceDigest = protocol.Digest(strings.TrimPrefix(string(testDigest("x")), "sha256:"))
		}, fieldSourceDigest},
		{"no source refs", func(w *Window) { w.Provenance.SourceRefs = nil }, fieldSourceRefs},
		{"malformed source ref", func(w *Window) { w.Provenance.SourceRefs = []protocol.BlobRef{"file:///tmp/x"} }, fieldSourceRefs},
		// Policies.
		{"missing redaction policy", func(w *Window) { w.Policies.RedactionPolicyVersion = "" }, fieldRedactionPolicy},
		{"missing domain mask policy", func(w *Window) { w.Policies.DomainMaskPolicyVersion = "" }, fieldDomainMaskPolicy},
		{"policy with space", func(w *Window) { w.Policies.DomainMaskPolicyVersion = "v 1" }, fieldDomainMaskPolicy},
		{"policy too long", func(w *Window) { w.Policies.RedactionPolicyVersion = strings.Repeat("1", 33) }, fieldRedactionPolicy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := validWindow()
			tc.mutate(&w)
			err := w.Validate()
			require.ErrorIs(t, err, ErrInvalidWindow)
			var fe *FieldError
			require.ErrorAs(t, err, &fe)
			require.Equal(t, tc.field, fe.Field, "reason: %s", fe.Reason)
		})
	}
}

func TestLabelTable(t *testing.T) {
	t.Parallel()
	// The closed A-G table, spelled out: never derived by position.
	want := []struct {
		label Label
		phase protocol.AgentPhase
	}{
		{LabelA, protocol.AgentPhaseIdleAtPrompt},
		{LabelB, protocol.AgentPhaseThinking},
		{LabelC, protocol.AgentPhaseRunningTool},
		{LabelD, protocol.AgentPhaseWaitingApproval},
		{LabelE, protocol.AgentPhaseAskingQuestion},
		{LabelF, protocol.AgentPhaseErrorOrExited},
		{LabelG, protocol.AgentPhaseUnknown},
	}
	for _, tc := range want {
		got, ok := tc.label.Phase()
		require.True(t, ok, "label %s", tc.label)
		require.Equal(t, tc.phase, got, "label %s", tc.label)
		back, ok := LabelOf(tc.phase)
		require.True(t, ok, "phase %s", tc.phase)
		require.Equal(t, tc.label, back, "phase %s", tc.phase)
	}
	// Every phase but BACKGROUND_WORK is a target exactly once.
	for _, p := range protocol.AgentPhase("").EnumValues() {
		_, ok := LabelOf(protocol.AgentPhase(p))
		require.Equal(t, protocol.AgentPhase(p) != protocol.AgentPhaseBackgroundWork, ok, "phase %s", p)
	}
	for _, bad := range []protocol.AgentPhase{protocol.AgentPhaseBackgroundWork, "SLEEPING", ""} {
		_, ok := LabelOf(bad)
		require.False(t, ok, "phase %q has no label", bad)
	}
	for _, bad := range []Label{"", "H", "a", "AB"} {
		_, ok := bad.Phase()
		require.False(t, ok, "label %q", bad)
	}
}

func TestPreviousStateAcceptsEveryPhase(t *testing.T) {
	t.Parallel()
	for _, p := range protocol.AgentPhase("").EnumValues() {
		w := validWindow()
		w.Input.PreviousState = protocol.AgentPhase(p)
		require.NoError(t, w.Validate(), "previous state %s", p)
	}
}

// windowFields lists every JSON field path of a Window, so a new field
// is a reviewed change to this contract, never an accident.
func windowFields(t reflect.Type, prefix string, out *[]string) {
	for i := range t.NumField() {
		f := t.Field(i)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		path := prefix + name
		*out = append(*out, path)
		ft := f.Type
		if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(time.Time{}) {
			windowFields(ft, path+".", out)
		}
	}
}

func TestWindowHasNoContentFields(t *testing.T) {
	t.Parallel()
	var got []string
	windowFields(reflect.TypeOf(Window{}), "", &got)
	sort.Strings(got)
	want := []string{
		"environment_domain",
		"input", "input.contract", "input.harness", "input.new_child_processes",
		"input.output_bytes", "input.output_bytes.last_0_5s", "input.output_bytes.last_10s", "input.output_bytes.last_2s",
		"input.previous_state", "input.previous_state_age_seconds",
		"input.screen", "input.screen.cols", "input.screen.digest", "input.screen.lines", "input.screen.redacted", "input.screen.rows",
		"input.seconds_since_last_output",
		"policies", "policies.domain_mask_policy_version", "policies.redaction_policy_version",
		"provenance", "provenance.anchor_at", "provenance.capture_session_id", "provenance.sequence_from", "provenance.sequence_to",
		"provenance.session_id", "provenance.source_digest", "provenance.source_quality", "provenance.source_refs",
		"provenance.window_end", "provenance.window_start",
		"schema_version",
		"split", "split.group_digest", "split.split",
		"target", "target.label",
		"window_id",
	}
	require.Equal(t, want, got, "a window field was added or renamed: review it against the no-content rule")
	// The only text the window carries is the redacted screen: no
	// raw stream, draft, keystrokes, prompt or message content.
	for _, path := range got {
		for _, banned := range []string{"raw", "draft", "keystroke", "typed", "human", "prompt", "message", "body", "transcript", "clipboard"} {
			require.NotContains(t, path, banned, "field %s", path)
		}
	}
}

func TestParseWindowStrict(t *testing.T) {
	t.Parallel()
	enc, err := json.Marshal(validWindow())
	require.NoError(t, err)
	got, err := ParseWindow(bytes.NewReader(enc))
	require.NoError(t, err)
	require.Equal(t, validWindow().Input, got.Input)
	require.True(t, validWindow().Provenance.AnchorAt.Equal(got.Provenance.AnchorAt))

	var doc map[string]any
	require.NoError(t, json.Unmarshal(enc, &doc))
	doc["raw_bytes"] = "AAAA"
	extra, err := json.Marshal(doc)
	require.NoError(t, err)
	_, err = ParseWindow(bytes.NewReader(extra))
	require.ErrorIs(t, err, ErrInvalidWindow, "an unknown field is refused, not dropped")

	_, err = ParseWindow(bytes.NewReader(append(append([]byte{}, enc...), []byte(`{}`)...)))
	require.ErrorIs(t, err, ErrInvalidWindow, "trailing data is refused")

	bad := validWindow()
	bad.Input.Screen.Redacted = false
	enc, err = json.Marshal(bad)
	require.NoError(t, err)
	_, err = ParseWindow(bytes.NewReader(enc))
	require.ErrorIs(t, err, ErrInvalidWindow, "parsing validates")

	_, err = ParseWindow(bytes.NewReader(bytes.Repeat([]byte(" "), maxWindowBytes+1)))
	require.ErrorIs(t, err, ErrInvalidWindow, "oversized input is refused before decoding")
}

func TestFieldErrorCarriesNoContent(t *testing.T) {
	t.Parallel()
	w := validWindow()
	w.Input.Screen.Lines = []string{"marker-text-\x1b"}
	w.Input.Screen.Digest = ScreenDigest(w.Input.Screen.Lines)
	err := w.Validate()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "marker-text")
}
