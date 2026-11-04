package detect

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Synthetic public frames with the 1.4.14-R7309.1 layout measured on bank
// screens at 80x24, 120x50 and 156x58: the 1.4.13 composer between rules,
// the model footer, the "◈/◇ Thinking (… · esc to interrupt)" row and the
// task widget above the composer. The private captures never enter the
// repository.
func muse1414Frame(above ...string) []byte {
	lines := make([]string, 0, len(above)+8)
	lines = append(lines, "  Muse Code 1.4.14", "")
	lines = append(lines, above...)
	lines = append(lines,
		"\x1b[2m"+museHintGray+strings.Repeat("─", 94),
		"\x1b[0m\x1b[38;5;75m❯ \x1b[39m",
		"\x1b[2m"+museHintGray+strings.Repeat("─", 94),
		museHintGray+"  \x1b[38;5;75mmuse-model\x1b[38;5;246m · xhigh · ~ · \x1b[38;5;211mYOLO\x1b[39m",
	)
	return []byte(strings.Join(lines, "\n"))
}

func muse1414Classify(frame []byte) State {
	return Classify(HarnessMuse, DefaultTable(), frame, 120, 50, 100,
		museBenchTreeForBinary(museBin1414))
}

func TestMuse1414MeasuredScreens(t *testing.T) {
	t.Parallel()
	widget := []string{
		museHintGray + "  \x1b[38;5;151m✓ \x1b[38;5;246mPublic widget item\x1b[39m",
		museHintGray + "  \x1b[38;5;109m◆ \x1b[38;5;252mPublic widget item\x1b[39m",
	}
	for _, tc := range []struct {
		name          string
		frame         []byte
		busy, boxBusy bool
	}{
		{name: "rest", frame: muse1414Frame()},
		{name: "thinking", busy: true, frame: muse1414Frame(
			"❯ Public prompt",
			museHintGray+"◈ Thinking (0s · "+interruptMark+")\x1b[39m", "")},
		{name: "thinking above the task widget", busy: true, frame: muse1414Frame(append([]string{
			"Public transcript line",
			museHintGray + "◇ Thinking (42s · " + interruptMark + ")\x1b[39m", "",
		}, widget...)...)},
		// The provider refused the turn: the agent is idle and the composer
		// takes a message again.
		{name: "usage limit", frame: muse1414Frame(
			"❯ Public prompt",
			"\x1b[38;5;203m◆ Usage limit reached · wait for usage to reset\x1b[39m",
			"  This turn stopped because your usage limit was reached.",
			"  Send a message to continue when usage is available.", "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := muse1414Classify(tc.frame)
			require.True(t, state.Certain, state.Detail)
			require.True(t, state.IsCLI)
			require.Equal(t, tc.busy, state.Busy, state.Detail)
			require.Equal(t, tc.boxBusy, state.BoxBusy, state.Detail)
		})
	}
}

// For about 200 ms while Muse 1.4.14 prints a transcript line, the
// Thinking row is gone and the screen reads idle. The detector alone cannot
// tell; the delivery paths cover it by requiring the visible text to stay
// unchanged for the quiet window, which these neighbouring frames never do.
func TestMuse1414MarkGapReadsIdleToTheDetectorAlone(t *testing.T) {
	t.Parallel()
	frames := muse1414GapFrames()
	require.True(t, muse1414Classify(frames[0]).Busy)
	gap := muse1414Classify(frames[1])
	require.True(t, gap.Certain)
	require.False(t, gap.Busy, "the measured gap shows no busy mark")
	require.True(t, muse1414Classify(frames[2]).Busy)
	require.NotEqual(t, string(frames[0]), string(frames[1]))
	require.NotEqual(t, string(frames[1]), string(frames[2]))
}

// muse1414GapFrames returns three synthetic public frames shaped like the
// measured Muse Code 1.4.14 sequence at 156x58: working with the Thinking
// row, then one frame without it while a transcript line is printed, then
// working again. Tests of the delivery paths use them to show that the
// quiet window keeps the gap from reading as rest.
func muse1414GapFrames() [3][]byte {
	before := muse1414GapFrame("    14. Public summary line",
		"\x1b[38;5;242m◇ Thinking (42s · "+interruptMark+")\x1b[39m")
	gap := muse1414GapFrame("    14. Public summary line"+"\n"+
		"    15. Public summary line", "")
	after := muse1414GapFrame("    14. Public summary line"+"\n"+
		"    15. Public summary line",
		"\x1b[38;5;242m◇ Thinking (42s · "+interruptMark+")\x1b[39m")
	return [3][]byte{before, gap, after}
}

func muse1414GapFrame(transcript, thinking string) []byte {
	rule := "\x1b[2m\x1b[38;5;242m" + repeatRune('─', 94)
	return []byte("  Muse Code 1.4.14\n\n" + transcript + "\n" + thinking + "\n\n" +
		"\x1b[38;5;242m  \x1b[38;5;151m✓ \x1b[38;5;246mPublic widget item\x1b[39m\n" +
		rule + "\n\x1b[0m\x1b[38;5;75m❯ \x1b[39m\n" + rule + "\n" +
		"\x1b[38;5;242m  \x1b[38;5;75mmuse-model\x1b[38;5;246m · xhigh · ~ · \x1b[38;5;211mYOLO\x1b[39m")
}

func repeatRune(r rune, n int) string {
	out := make([]rune, n)
	for i := range out {
		out[i] = r
	}
	return string(out)
}
