package analyst

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// The screens below are invented, in each harness's layout as its profile
// measures it and as screenproj projects it: rendered rows, trailing empty
// rows dropped.

const museRule = "────────────────────────"

// museScreen is a Muse Code screen: the transcript, the input box or a
// menu in its place between two rules, and the status line.
func museScreen(transcript, box []string, status string) []string {
	lines := append(slices.Clone(transcript), museRule)
	lines = append(lines, box...)
	return append(lines, museRule, status)
}

// opencodeBox is OpenCode's empty input box, from its blank row above to
// its bottom edge, and opencodeLiveBox the same box as a live screen
// shows it, its measured rows covered.
var (
	opencodeBox     = []string{"", "  ┃", "  ┃  Ask anything...", "  ┃", "  ┃  Build · model", "  ╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀"}
	opencodeLiveBox = []string{"", inputMarker, inputMarker, inputMarker, inputMarker, "  ╹▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀"}
)

// opencodeScreen is an OpenCode screen: the transcript, the input box and
// the hints below it.
func opencodeScreen(transcript, box []string, hints string) []string {
	lines := append(slices.Clone(transcript), box...)
	return append(lines, hints)
}

// Status lines and hints.
const (
	museStatus      = "  Build · model"
	museWorking     = "  ✻ Thinking (4s · esc to interrupt)"
	opencodeHints   = "  ctrl+p commands"
	opencodeWorking = "  ■■■⬝⬝⬝ esc interrupt"
)

// Output counts.
var (
	streaming = outputBytes{Last500ms: 40, Last2s: 300, Last10s: 2000}
	// lastSecond wrote in the last two seconds, not the last half second.
	lastSecond = outputBytes{Last2s: 120, Last10s: 900}
	silent     = outputBytes{}
)

func TestFeatures(t *testing.T) {
	t.Parallel()
	museAnswered := []string{"● Added a test for the parser; go test passes.", "", "◆ Worked for 12s"}
	museAsked := []string{"● Which file should I change, the parser or the lexer?", "", "◆ Worked for 3s"}
	opencodeAnswered := []string{"┃  add a test for the parser", "", "   Added the test; go test passes.", "", "   ▣ Build · model · 3.2s"}
	opencodeAsked := []string{"┃  fix the failing test", "", "   Which file should I change?", "", "   ▣ Build · model · 2.1s"}
	idle := analyzeFeatures{ApprovalMenu: featureNo, QuestionPending: featureNo, ErrorVisible: featureNo, Busy: noneValue, Output: outputQuiet}
	with := func(f func(*analyzeFeatures)) analyzeFeatures {
		out := idle
		f(&out)
		return out
	}
	for _, tc := range []struct {
		name    string
		harness protocol.Harness
		lines   []string
		bytes   outputBytes
		want    analyzeFeatures
	}{
		// Muse Code.
		{"muse idle", protocol.HarnessMuseCode, museScreen(museAnswered, []string{"❯ "}, museStatus), lastSecond, idle},
		{
			"muse question", protocol.HarnessMuseCode, museScreen(museAsked, []string{"❯ "}, museStatus), silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = featureYes }),
		},
		{
			"muse old question above a later answer", protocol.HarnessMuseCode,
			museScreen([]string{
				"● Which file should I change?", "", "❯ the parser", "",
				"● Changed the parser; the tests pass.", "", "◆ Worked for 9s",
			}, []string{"❯ "}, museStatus),
			silent, idle,
		},
		{
			"muse answer with a question mark inside", protocol.HarnessMuseCode,
			museScreen([]string{"● Why did it fail? A nil map; it is fixed now."}, []string{"❯ "}, museStatus), silent, idle,
		},
		{
			"muse menu", protocol.HarnessMuseCode,
			museScreen([]string{"● I will run the tests."}, []string{
				"Would you like to run the following command?", "go test ./...", "",
				"⟩ 1. Yes, proceed (y)", "  2. No, cancel (n)",
			}, museStatus),
			streaming,
			with(func(f *analyzeFeatures) { f.ApprovalMenu, f.Output = featureYes, outputStreaming }),
		},
		{
			"muse thinking", protocol.HarnessMuseCode,
			museScreen([]string{"● Reading the parser."}, []string{"❯ "}, museWorking), streaming,
			with(func(f *analyzeFeatures) { f.Busy, f.Output = busyThinking, outputStreaming }),
		},
		{
			"muse running a command", protocol.HarnessMuseCode,
			museScreen([]string{"● Running command go test ./... · running (3s)"}, []string{"❯ "}, museWorking), streaming,
			with(func(f *analyzeFeatures) { f.Busy, f.Output = busyRunningTool, outputStreaming }),
		},
		{
			"muse command line without the working indicator", protocol.HarnessMuseCode,
			museScreen([]string{"● Running command go test ./... · running (3s)"}, []string{"❯ "}, museStatus), silent, idle,
		},
		{
			"muse error", protocol.HarnessMuseCode,
			museScreen([]string{"● model failed: connection refused by example.invalid"}, []string{"❯ "}, museStatus), silent,
			with(func(f *analyzeFeatures) { f.ErrorVisible = featureYes }),
		},
		{
			"muse live input box", protocol.HarnessMuseCode, museScreen(museAsked, []string{inputMarker}, museStatus), silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = featureYes }),
		},
		{
			"muse masked and redacted question", protocol.HarnessMuseCode,
			museScreen([]string{"● The key •••••••• works on [domain]; shall I push?"}, []string{"❯ "}, museStatus), silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = featureYes }),
		},
		{
			"muse masked and redacted answer", protocol.HarnessMuseCode,
			museScreen([]string{"● Pushed to [domain] with the key ••••••••."}, []string{"❯ "}, museStatus), silent, idle,
		},
		{
			"muse with one rule", protocol.HarnessMuseCode,
			[]string{"● Which file should I change?", museRule, "❯ "},
			silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = unknownValue }),
		},
		{
			"muse empty screen", protocol.HarnessMuseCode, nil, silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = unknownValue }),
		},

		// OpenCode.
		{"opencode idle", protocol.HarnessOpencode, opencodeScreen(opencodeAnswered, opencodeBox, opencodeHints), lastSecond, idle},
		{
			"opencode question", protocol.HarnessOpencode, opencodeScreen(opencodeAsked, opencodeBox, opencodeHints), silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = featureYes }),
		},
		{
			// The last finished message still asks; busy says the agent
			// moved past it.
			"opencode question under a turn at work", protocol.HarnessOpencode,
			opencodeScreen(append(slices.Clone(opencodeAsked), "", "┃  the parser", "", "   ⠙ bash go test ./..."), opencodeBox, opencodeWorking),
			streaming,
			with(func(f *analyzeFeatures) {
				f.QuestionPending, f.Busy, f.Output = featureYes, busyRunningTool, outputStreaming
			}),
		},
		{
			"opencode old question above a later answer", protocol.HarnessOpencode,
			opencodeScreen(append(slices.Clone(opencodeAsked), "", "┃  the parser", "", "   Changed the parser.", "", "   ▣ Build · model · 5.0s"),
				opencodeBox, opencodeHints),
			silent, idle,
		},
		{
			"opencode menu", protocol.HarnessOpencode,
			opencodeScreen(append(slices.Clone(opencodeAnswered), "", "   Permission required", "   $ go test ./...", "   Allow once   Allow always   Reject"),
				opencodeBox, opencodeHints),
			silent,
			with(func(f *analyzeFeatures) { f.ApprovalMenu = featureYes }),
		},
		{
			"opencode thinking", protocol.HarnessOpencode,
			opencodeScreen(append(slices.Clone(opencodeAnswered), "", "┃  now the lexer"), opencodeBox, opencodeWorking), streaming,
			with(func(f *analyzeFeatures) { f.Busy, f.Output = busyThinking, outputStreaming }),
		},
		{
			"opencode spinner without the working indicator", protocol.HarnessOpencode,
			opencodeScreen(append(slices.Clone(opencodeAnswered), "", "   ⠙ bash go test ./..."), opencodeBox, opencodeHints), silent, idle,
		},
		{
			"opencode error", protocol.HarnessOpencode,
			opencodeScreen(append(slices.Clone(opencodeAnswered), "", "   Cannot connect to API: https://example.invalid/v1"), opencodeBox, opencodeHints),
			silent,
			with(func(f *analyzeFeatures) { f.ErrorVisible = featureYes }),
		},
		{
			"opencode live input box", protocol.HarnessOpencode, opencodeScreen(opencodeAsked, opencodeLiveBox, opencodeHints), silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = featureYes }),
		},
		{
			"opencode with no footer", protocol.HarnessOpencode, opencodeScreen([]string{"┃  fix the failing test"}, opencodeBox, opencodeHints), silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = unknownValue }),
		},
		{
			"opencode empty screen", protocol.HarnessOpencode, nil, silent,
			with(func(f *analyzeFeatures) { f.QuestionPending = unknownValue }),
		},

		// A harness without patterns of its own.
		{
			"generic confirmation", protocol.HarnessCodex,
			[]string{"Overwrite parser.go? [y/N]"},
			silent,
			analyzeFeatures{ApprovalMenu: featureYes, QuestionPending: unknownValue, ErrorVisible: unknownValue, Busy: unknownValue, Output: outputQuiet},
		},
		{
			"generic error", protocol.HarnessClaudeCode,
			[]string{"Traceback (most recent call last):", `  File "parse.py", line 3`},
			silent,
			analyzeFeatures{ApprovalMenu: unknownValue, QuestionPending: unknownValue, ErrorVisible: featureYes, Busy: unknownValue, Output: outputQuiet},
		},
		{
			"generic error line", protocol.HarnessGeneric,
			[]string{"$ go build", "Error: no Go files in example.invalid/parser"},
			streaming,
			analyzeFeatures{ApprovalMenu: unknownValue, QuestionPending: unknownValue, ErrorVisible: featureYes, Busy: unknownValue, Output: outputStreaming},
		},
		{
			"generic plain screen", protocol.HarnessAntigravity,
			[]string{"$ go test ./...", "ok  example.invalid/parser 0.2s"},
			silent,
			analyzeFeatures{ApprovalMenu: unknownValue, QuestionPending: unknownValue, ErrorVisible: unknownValue, Busy: unknownValue, Output: outputQuiet},
		},
		{
			"no harness", "",
			[]string{"Which file should I change?", "Proceed? (y/n)"},
			silent,
			analyzeFeatures{ApprovalMenu: featureYes, QuestionPending: unknownValue, ErrorVisible: unknownValue, Busy: unknownValue, Output: outputQuiet},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := features(analyzeInput{Harness: tc.harness, OutputBytes: tc.bytes, ScreenLines: tc.lines})
			require.Equal(t, tc.want, got)
		})
	}
}
