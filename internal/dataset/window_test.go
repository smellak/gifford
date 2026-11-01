package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// Invented child process entries for the fixtures.
const (
	childGoTest     = "go test"
	childNpmInstall = "npm install"
	childCargo      = "cargo"
)

// testDigest is an invented sha256 digest over a fixture label: the
// fixtures reference bytes that never existed.
func testDigest(label string) protocol.Digest {
	sum := sha256.Sum256([]byte(label))
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// setLines replaces the screen lines and recomputes their digest.
func setLines(w *Window, lines []string) {
	w.Input.ScreenLines = lines
	w.Screen.Digest = ScreenDigest(w.Screen.Cols, w.Screen.Rows, lines)
}

// validWindow is the minimal valid window. Every value is invented.
func validWindow() Window {
	anchor := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	lines := []string{"step 3 of 7: compiling", "tests: 12 passed, 0 failed"}
	return Window{
		SchemaVersion: WindowSchemaVersion,
		WindowID:      "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Contract:      protocol.ModelContractAnalyzeAgentState,
		Input: AnalyzeInput{
			Harness:                 protocol.HarnessClaudeCode,
			SecondsSinceLastOutput:  0.2,
			OutputBytes:             OutputBytes{Last500ms: 120, Last2s: 480, Last10s: 900},
			NewChildProcesses:       []string{childGoTest},
			PreviousState:           protocol.AgentPhaseRunningTool,
			PreviousStateAgeSeconds: 4.5,
			ScreenLines:             lines,
		},
		Screen: ScreenEvidence{
			Cols: 80, Rows: 24,
			Redaction:           AppliedPolicy{Applied: true, PolicyVersion: "1"},
			HumanInputExclusion: AppliedPolicy{Applied: true, PolicyVersion: "1"},
			Digest:              ScreenDigest(80, 24, lines),
		},
		Target: Target{Label: LabelC, Source: LabelSourceHook},
		Provenance: Provenance{
			SessionID:        "cap-01ARZ3NDEKTSV4RRFFQ69G5FAV",
			CaptureSessionID: "cap-01ARZ3NDEKTSV4RRFFQ69G5FAV",
			SourceQuality:    protocol.SourceQualitySynthetic,
			HarnessVersion:   "2.0.14 (Claude Code)",
			SequenceFrom:     2,
			SequenceTo:       40,
			WindowStart:      anchor.Add(-30 * time.Second),
			WindowEnd:        anchor,
			AnchorAt:         anchor,
			SourceRefs:       []protocol.BlobRef{"blob://capture/cap-01ARZ3NDEKTSV4RRFFQ69G5FAV/seg-01ARZ3NDEKTSV4RRFFQ69G5FAW/0"},
			SourceDigest:     testDigest("synthetic source"),
		},
		EnvironmentDomain: EnvironmentDomainBankGeneric,
		Policies:          Policies{RedactionPolicyVersion: "1", HumanInputPolicyVersion: "1", DomainMaskPolicyVersion: "1"},
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
	setLines(&idle, []string{})
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
		{"other contract", func(w *Window) { w.Contract = protocol.ModelContractTriageMessage }, wantContract},
		{"unknown harness", func(w *Window) { w.Input.Harness = "editor" }, "input.harness"},
		{"schema version", func(w *Window) { w.SchemaVersion = 2 }, wantSchemaVersion},
		{"window id", func(w *Window) { w.WindowID = "w-1" }, "window_id"},
		{"unknown previous state", func(w *Window) { w.Input.PreviousState = "SLEEPING" }, "input.previous_state"},
		{"nine children", func(w *Window) {
			w.Input.NewChildProcesses = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
		}, fieldChildren},
		{"duplicate child", func(w *Window) { w.Input.NewChildProcesses = []string{"go", "go"} }, fieldChildren},
		{"unsorted children", func(w *Window) { w.Input.NewChildProcesses = []string{childNpmInstall, childGoTest} }, fieldChildren},
		{"child with a path", func(w *Window) { w.Input.NewChildProcesses = []string{"bin/go"} }, fieldChildren},
		{"child with an absolute path", func(w *Window) { w.Input.NewChildProcesses = []string{"/usr/bin/go"} }, fieldChildren},
		{"child with a backslash", func(w *Window) { w.Input.NewChildProcesses = []string{`tools\\go`} }, fieldChildren},
		{"child argument with a path", func(w *Window) { w.Input.NewChildProcesses = []string{"go ./pkg"} }, fieldChildren},
		{"child argument is a URL", func(w *Window) { w.Input.NewChildProcesses = []string{"curl https://example.invalid"} }, fieldChildren},
		{"child argument host and port", func(w *Window) { w.Input.NewChildProcesses = []string{"nc example.invalid:80"} }, fieldChildren},
		{"child argument is a flag", func(w *Window) { w.Input.NewChildProcesses = []string{"go -race"} }, fieldChildren},
		{"child argument is a long flag", func(w *Window) { w.Input.NewChildProcesses = []string{"npm --global"} }, fieldChildren},
		{"child with two arguments", func(w *Window) { w.Input.NewChildProcesses = []string{"go test pkg"} }, fieldChildren},
		{"child argument is a token", func(w *Window) { w.Input.NewChildProcesses = []string{"python secretToken123"} }, fieldChildren},
		{"child argument is a key", func(w *Window) { w.Input.NewChildProcesses = []string{"cli APIKEY123"} }, fieldChildren},
		{"child argument is a name", func(w *Window) { w.Input.NewChildProcesses = []string{"bash Alice"} }, fieldChildren},
		{"child argument is a file", func(w *Window) { w.Input.NewChildProcesses = []string{"python main.py"} }, fieldChildren},
		{"child verb in capitals", func(w *Window) { w.Input.NewChildProcesses = []string{"go TEST"} }, fieldChildren},
		{"child verb with a suffix", func(w *Window) { w.Input.NewChildProcesses = []string{"go tests"} }, fieldChildren},
		{"child with an environment assignment", func(w *Window) { w.Input.NewChildProcesses = []string{"GOFLAGS=x go"} }, fieldChildren},
		{"child with a control byte", func(w *Window) { w.Input.NewChildProcesses = []string{"go\ttest"} }, fieldChildren},
		{"child not UTF-8", func(w *Window) { w.Input.NewChildProcesses = []string{string([]byte{'g', 0xff})} }, fieldChildren},
		{"child not ASCII", func(w *Window) { w.Input.NewChildProcesses = []string{"gö"} }, fieldChildren},
		{"empty child", func(w *Window) { w.Input.NewChildProcesses = []string{""} }, fieldChildren},
		{"child leading space", func(w *Window) { w.Input.NewChildProcesses = []string{" go"} }, fieldChildren},
		{"child trailing space", func(w *Window) { w.Input.NewChildProcesses = []string{"go "} }, fieldChildren},
		{"child hidden relative name", func(w *Window) { w.Input.NewChildProcesses = []string{"..go"} }, fieldChildren},
		{"child name too long", func(w *Window) { w.Input.NewChildProcesses = []string{strings.Repeat("g", 33)} }, fieldChildren},
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
		{"screen not redacted", func(w *Window) { w.Screen.Redaction.Applied = false }, fieldScreenRedaction},
		{"redaction under another version", func(w *Window) { w.Screen.Redaction.PolicyVersion = "2" }, fieldScreenRedaction},
		{"redaction without version", func(w *Window) { w.Screen.Redaction.PolicyVersion = "" }, fieldScreenRedaction},
		{"human input not excluded", func(w *Window) { w.Screen.HumanInputExclusion.Applied = false }, fieldScreenHumanInput},
		{"human input under another version", func(w *Window) { w.Screen.HumanInputExclusion.PolicyVersion = "2" }, fieldScreenHumanInput},
		{"human input without version", func(w *Window) { w.Screen.HumanInputExclusion.PolicyVersion = "" }, fieldScreenHumanInput},
		{"raw escape in screen", func(w *Window) { setLines(w, []string{"\x1b[32mok\x1b[0m"}) }, fieldScreenLines},
		{"carriage return in screen", func(w *Window) { setLines(w, []string{"ok\r"}) }, fieldScreenLines},
		{"newline inside a line", func(w *Window) { setLines(w, []string{"a\nb"}) }, fieldScreenLines},
		{"invalid UTF-8 line", func(w *Window) { setLines(w, []string{string([]byte{0xff})}) }, fieldScreenLines},
		{"truncated UTF-8 line", func(w *Window) { setLines(w, []string{"ok " + string([]byte{0xe2, 0x82})}) }, fieldScreenLines},
		{"null screen lines", func(w *Window) { setLines(w, nil) }, fieldScreenLines},
		{"screen digest mismatch", func(w *Window) { w.Input.ScreenLines = []string{"edited"} }, fieldScreenDigest},
		{"size changed under the digest", func(w *Window) { w.Screen.Cols = 81 }, fieldScreenDigest},
		{"more lines than rows", func(w *Window) { w.Screen.Rows = 1 }, fieldScreenLines},
		{"zero size screen", func(w *Window) { w.Screen.Cols = 0 }, "screen"},
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
		{"missing human input policy", func(w *Window) { w.Policies.HumanInputPolicyVersion = "" }, wantHumanPolicy},
		{"missing domain mask policy", func(w *Window) { w.Policies.DomainMaskPolicyVersion = "" }, fieldDomainMaskPolicy},
		{"policy with space", func(w *Window) { w.Policies.DomainMaskPolicyVersion = "v 1" }, fieldDomainMaskPolicy},
		{"policy too long", func(w *Window) { w.Policies.RedactionPolicyVersion = strings.Repeat("1", 33) }, fieldRedactionPolicy},
		{"invalid UTF-8 policy", func(w *Window) { w.Policies.RedactionPolicyVersion = string([]byte{'v', 0xff}) }, fieldRedactionPolicy},
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
		"contract",
		"environment_domain",
		"input", "input.harness", "input.new_child_processes",
		"input.output_bytes", "input.output_bytes.last_0_5s", "input.output_bytes.last_10s", "input.output_bytes.last_2s",
		"input.previous_state", "input.previous_state_age_seconds",
		"input.screen_lines", "input.seconds_since_last_output",
		"policies", "policies.domain_mask_policy_version", "policies.human_input_policy_version", "policies.redaction_policy_version",
		"provenance", "provenance.activity_version", "provenance.anchor_at", "provenance.capture_session_id", "provenance.harness_version", "provenance.sequence_from", "provenance.sequence_to",
		"provenance.session_id", "provenance.source_digest", "provenance.source_quality", "provenance.source_refs",
		"provenance.window_end", "provenance.window_start",
		wantSchemaVersion,
		"screen", "screen.cols", "screen.digest",
		"screen.human_input_exclusion", "screen.human_input_exclusion.applied", "screen.human_input_exclusion.policy_version",
		"screen.redaction", "screen.redaction.applied", "screen.redaction.policy_version",
		"screen.rows",
		"split", "split.group_digest", "split.split",
		"target", "target.label", "target.source",
		"window_id",
	}
	require.Equal(t, want, got, "a window field was added or renamed: review it against the no-content rule")
	// The model boundary: input is exactly the contract's features.
	// The contract, screen evidence, provenance, domain, policies and
	// split are siblings of input, never inside it.
	var input []string
	windowFields(reflect.TypeOf(AnalyzeInput{}), "", &input)
	sort.Strings(input)
	require.Equal(t, []string{
		"harness", "new_child_processes",
		"output_bytes", "output_bytes.last_0_5s", "output_bytes.last_10s", "output_bytes.last_2s",
		"previous_state", "previous_state_age_seconds",
		"screen_lines", "seconds_since_last_output",
	}, input, "input must hold only the model features")
	// The only text the window carries is the screen lines: no raw
	// stream, draft, keystrokes, prompt or message content.
	for _, path := range got {
		for _, banned := range []string{"raw", "draft", "keystroke", "typed", "human_text", "user_input", "prompt", "message", "body", "transcript", "clipboard"} {
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
	bad.Screen.Redaction.Applied = false
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
	setLines(&w, []string{"marker-text-\x1b"})
	err := w.Validate()
	require.Error(t, err)
	require.NotContains(t, err.Error(), "marker-text")
}

func TestScreenDigestCanonical(t *testing.T) {
	t.Parallel()
	// A fixed vector pins the v1 encoding: changing it must change
	// the domain name, not silently move every stored digest.
	require.Equal(t, protocol.Digest("sha256:7025e3dfc66848b261d4c522fa4b5120c49f03f9a740acbc4d49836dfc3e9b57"),
		ScreenDigest(80, 24, []string{"step 3 of 7: compiling", "tests: 12 passed, 0 failed"}))
	// Structurally different screens never share a preimage.
	distinct := []struct {
		cols, rows int64
		lines      []string
	}{
		{80, 24, []string{"a", "b"}},
		{80, 24, []string{"a\nb"}},
		{80, 24, []string{"ab"}},
		{80, 24, []string{"a", "", "b"}},
		{80, 24, []string{""}},
		{80, 24, nil},
		{24, 80, []string{"a", "b"}},
		{80, 25, []string{"a", "b"}},
	}
	seen := map[protocol.Digest]int{}
	for i, d := range distinct {
		got := ScreenDigest(d.cols, d.rows, d.lines)
		require.True(t, got.Valid())
		if j, ok := seen[got]; ok {
			t.Fatalf("screens %d and %d share a digest", j, i)
		}
		seen[got] = i
	}
}

func TestParseWindowErrorsCarryNoContent(t *testing.T) {
	t.Parallel()
	enc, err := json.Marshal(validWindow())
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(enc, &doc))
	doc["sentinel_field_q7"] = "sentinel-value-q7"
	unknown, err := json.Marshal(doc)
	require.NoError(t, err)
	inputs := map[string][]byte{
		"unknown field":   unknown,
		"malformed token": []byte(`{"schema_version": sentinel_token_q7}`),
		"bad string":      []byte(`{"window_id": "sentinel-id-q7`),
		"wrong type":      []byte(`{"input": {"harness": ["sentinel-elem-q7"]}}`),
	}
	for name, in := range inputs {
		_, err := ParseWindow(bytes.NewReader(in))
		require.ErrorIs(t, err, ErrInvalidWindow, name)
		require.NotContains(t, err.Error(), "sentinel", name)
		require.NotContains(t, err.Error(), "q7", name)
	}
}

// idleFirstWindow is the zero-heavy valid window: a session's first,
// on a pane silent for 12 s with an empty screen. Every zero in it is a
// real value that a missing member must not imitate.
func idleFirstWindow() Window {
	w := validWindow()
	w.Input.OutputBytes = OutputBytes{}
	w.Input.SecondsSinceLastOutput = 12
	w.Input.NewChildProcesses = []string{}
	w.Input.PreviousState = ""
	w.Input.PreviousStateAgeSeconds = 0
	setLines(&w, []string{})
	return w
}

// deletePath removes one dotted member from a decoded document, or sets
// it to null, and reports whether it was there.
func deletePath(doc map[string]any, path string, null bool) bool {
	parts := strings.Split(path, ".")
	for _, part := range parts[:len(parts)-1] {
		next, ok := doc[part].(map[string]any)
		if !ok {
			return false
		}
		doc = next
	}
	last := parts[len(parts)-1]
	if _, ok := doc[last]; !ok {
		return false
	}
	if null {
		doc[last] = nil
	} else {
		delete(doc, last)
	}
	return true
}

func TestParseWindowRequiresEveryMember(t *testing.T) {
	t.Parallel()
	var paths []string
	windowFields(reflect.TypeOf(Window{}), "", &paths)
	for _, base := range []struct {
		name string
		w    Window
	}{{"valid", validWindow()}, {"idle first", idleFirstWindow()}} {
		require.NoError(t, base.w.Validate(), base.name)
		enc, err := json.Marshal(base.w)
		require.NoError(t, err)
		_, err = ParseWindow(bytes.NewReader(enc))
		require.NoError(t, err, "%s: the complete document parses", base.name)
		for _, path := range paths {
			if path == fieldActivityVersion {
				continue // absent means the legacy activity input
			}
			for _, null := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/null=%v", base.name, path, null), func(t *testing.T) {
					t.Parallel()
					var doc map[string]any
					require.NoError(t, json.Unmarshal(enc, &doc))
					require.True(t, deletePath(doc, path, null), "the encoding holds %s", path)
					in, err := json.Marshal(doc)
					require.NoError(t, err)
					_, err = ParseWindow(bytes.NewReader(in))
					if null && path == fieldChildren {
						require.NoError(t, err, "null is the deliberate unknown state")
						return
					}
					require.ErrorIs(t, err, ErrInvalidWindow)
					var fe *FieldError
					require.ErrorAs(t, err, &fe)
					require.Equal(t, path, fe.Field)
					require.Equal(t, "required member missing or null", fe.Reason)
				})
			}
		}
	}
}

func TestEmptyValuesEncodeExplicitly(t *testing.T) {
	t.Parallel()
	enc, err := json.Marshal(idleFirstWindow())
	require.NoError(t, err)
	require.Contains(t, string(enc), `"previous_state":""`, "a first window states its empty previous state")
	require.Contains(t, string(enc), `"screen_lines":[]`, "an empty screen is [], not null")
	got, err := ParseWindow(bytes.NewReader(enc))
	require.NoError(t, err)
	require.Equal(t, []string{}, got.Input.ScreenLines)
}

func TestChildProcessesTriState(t *testing.T) {
	t.Parallel()
	states := []struct {
		name string
		list []string
		json string
	}{
		{"unknown", nil, `"new_child_processes":null`},
		{"none", []string{}, `"new_child_processes":[]`},
		{"listed", []string{childGoTest, childNpmInstall}, `"new_child_processes":["go test","npm install"]`},
	}
	seen := map[string]bool{}
	for _, st := range states {
		w := validWindow()
		w.Input.NewChildProcesses = st.list
		require.NoError(t, w.Validate(), st.name)
		enc, err := json.Marshal(w)
		require.NoError(t, err)
		require.Contains(t, string(enc), st.json, st.name)
		require.False(t, seen[string(enc)], "%s encodes like another state", st.name)
		seen[string(enc)] = true
		got, err := ParseWindow(bytes.NewReader(enc))
		require.NoError(t, err, st.name)
		require.Equal(t, st.list == nil, got.Input.NewChildProcesses == nil, "%s: null stays distinct from []", st.name)
		require.Equal(t, st.list, got.Input.NewChildProcesses, st.name)
	}
}

func TestCanonicalChildProcesses(t *testing.T) {
	t.Parallel()
	require.Nil(t, CanonicalChildProcesses(nil), "not observed stays unknown")
	require.Equal(t, []string{}, CanonicalChildProcesses([]string{}), "observed none stays none")
	in := []string{childNpmInstall, childGoTest, childNpmInstall, childCargo}
	got := CanonicalChildProcesses(in)
	require.Equal(t, []string{childCargo, childGoTest, childNpmInstall}, got)
	require.Equal(t, []string{childNpmInstall, childGoTest, childNpmInstall, childCargo}, in, "the input is not modified")
	w := validWindow()
	w.Input.NewChildProcesses = got
	require.NoError(t, w.Validate())
}

func TestParseWindowRejectsInvalidUTF8Bytes(t *testing.T) {
	t.Parallel()
	// U+FFFD is valid text; its encoded bytes are swapped afterwards
	// for an invalid byte or escape, which decoding would hide.
	const replacement = "�"
	fffd := []byte(replacement)
	line := validWindow()
	setLines(&line, []string{"before " + replacement + " after"})
	policy := validWindow()
	policy.Policies.DomainMaskPolicyVersion = "v" + replacement
	policy.Screen.Redaction.PolicyVersion = policy.Policies.RedactionPolicyVersion
	for _, base := range []struct {
		name string
		w    Window
	}{{"screen line", line}, {"policy version", policy}} {
		require.NoError(t, base.w.Validate(), base.name)
		enc, err := json.Marshal(base.w)
		require.NoError(t, err)
		require.Equal(t, 1, bytes.Count(enc, fffd), base.name)
		_, err = ParseWindow(bytes.NewReader(enc))
		require.NoError(t, err, "%s: a real U+FFFD is valid text", base.name)
		for _, bad := range []string{"\xff", "\xed\xa0\x80", `\udcff`, `\ud800`, `\ud800x`, `\ud800\ud800`, `\uDBFFA`, "\\uDBFF\\u0041"} {
			doc := bytes.Replace(enc, fffd, []byte(bad), 1)
			_, err := ParseWindow(bytes.NewReader(doc))
			require.ErrorIs(t, err, ErrInvalidWindow, "%s with %q", base.name, bad)
			var fe *FieldError
			require.ErrorAs(t, err, &fe)
			require.Equal(t, "window", fe.Field, "%s with %q", base.name, bad)
			require.Equal(t, "not valid UTF-8", fe.Reason, "%s with %q", base.name, bad)
		}
	}
	// A surrogate pair escape is valid text and decodes to the same
	// line as its raw UTF-8.
	emoji := validWindow()
	setLines(&emoji, []string{"ok \U0001F600"})
	enc, err := json.Marshal(emoji)
	require.NoError(t, err)
	doc := bytes.Replace(enc, []byte("\U0001F600"), []byte("\\ud83d\\ude00"), 1)
	require.NotEqual(t, enc, doc)
	got, err := ParseWindow(bytes.NewReader(doc))
	require.NoError(t, err)
	require.Equal(t, []string{"ok \U0001F600"}, got.Input.ScreenLines)
	// A lone surrogate at the very end of a string, and an escaped
	// backslash before a u, are both handled.
	for name, in := range map[string]string{
		"high at end":      `{"window_id":"\ud83d"}`,
		"truncated escape": `{"window_id":"\ud8"}`,
	} {
		_, err := ParseWindow(strings.NewReader(in))
		require.ErrorIs(t, err, ErrInvalidWindow, name)
	}
	require.True(t, surrogatesPaired([]byte(`{"a":"\\udcff"}`)), "an escaped backslash then u is text, not an escape")
}

func TestChildProcessVerbs(t *testing.T) {
	t.Parallel()
	verbs := []string{
		"build", "check", "compile", "exec", "fmt", "format", "generate", "install",
		"lint", "list", "run", "status", "test", "update", "vet",
	}
	for _, verb := range verbs {
		w := validWindow()
		w.Input.NewChildProcesses = []string{"tool " + verb}
		require.NoError(t, w.Validate(), "verb %s", verb)
	}
	w := validWindow()
	w.Input.NewChildProcesses = []string{"cargo", childGoTest, childNpmInstall}
	require.NoError(t, w.Validate(), "a bare basename needs no verb")
}

func TestParseWindowHarnessVersion(t *testing.T) {
	t.Parallel()
	parse := func(v string) error {
		w := validWindow()
		w.Provenance.HarnessVersion = v
		b, err := json.Marshal(w)
		require.NoError(t, err)
		_, err = ParseWindow(bytes.NewReader(b))
		return err
	}
	for _, v := range []string{"2.0.14 (Claude Code)", "unknown", strings.Repeat("9", 64), "版本 1.2"} {
		require.NoError(t, parse(v), v)
	}
	for name, v := range map[string]string{
		"empty":    "",
		"control":  "2.0\x07",
		"newline":  "2.0\n1",
		"65 runes": strings.Repeat("9", 65),
	} {
		err := parse(v)
		var fe *FieldError
		require.ErrorAs(t, err, &fe, name)
		require.Equal(t, fieldHarnessVersion, fe.Field, name)
	}

	// The member is required: a window from before it is refused.
	b, err := json.Marshal(validWindow())
	require.NoError(t, err)
	old := bytes.Replace(b, []byte(`"harness_version":"2.0.14 (Claude Code)",`), nil, 1)
	require.NotEqual(t, b, old)
	_, err = ParseWindow(bytes.NewReader(old))
	var fe *FieldError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, fieldHarnessVersion, fe.Field)

	// It is metadata, not a feature: the model's input has no version.
	in, err := json.Marshal(validWindow().Input)
	require.NoError(t, err)
	require.NotContains(t, string(in), "harness_version")
}
