package detect

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var sgr = regexp.MustCompile(`\x1b\[[0-9;:?]*[a-zA-Z]`)

// withRowAbove writes text on the blank row nearest above the composer row
// that holds lead, past the box's own rows, in a measured idle frame.
func withRowAbove(t *testing.T, raw []byte, lead, text string) []byte {
	t.Helper()
	lines := strings.Split(string(raw), "\n")
	composer := -1
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(sgr.ReplaceAllString(lines[i], "")), lead) {
			composer = i
			break
		}
	}
	require.GreaterOrEqual(t, composer, 0, "composer row %q", lead)
	for i := composer - 1; i >= 0; i-- {
		visible := strings.TrimSpace(sgr.ReplaceAllString(lines[i], ""))
		if visible == "" {
			lines[i] = text
			return []byte(strings.Join(lines, "\n"))
		}
	}
	require.FailNow(t, "no blank row above the composer")
	return nil
}

// withRowsAbove writes rows on the content rows right above the composer
// row that holds lead, keeping the blank and rule rows between them, so a
// short transcript ends exactly in rows.
func withRowsAbove(t *testing.T, raw []byte, lead string, rows ...string) []byte {
	t.Helper()
	lines := strings.Split(string(raw), "\n")
	composer := -1
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(sgr.ReplaceAllString(lines[i], "")), lead) {
			composer = i
			break
		}
	}
	require.GreaterOrEqual(t, composer, 0, "composer row %q", lead)
	end := composer
	for end > 0 {
		visible := strings.TrimSpace(sgr.ReplaceAllString(lines[end-1], ""))
		if visible != "" && !isRuleRow(visible) {
			break
		}
		end--
	}
	require.GreaterOrEqual(t, end-len(rows), 0, "room above the composer")
	copy(lines[end-len(rows):end], rows)
	return []byte(strings.Join(lines, "\n"))
}

const (
	vetoQuestion = "● What is your favourite film?"
	vetoAnswer   = "● Thank you, a fine choice."
	vetoReply    = "Casablanca"
)

func museQuestionFrame(rows ...string) []byte {
	lines := append([]string{"Ordinary public transcript output"}, rows...)
	lines = append(lines,
		"\x1b[2m"+museHintGray+strings.Repeat("─", 94),
		"\x1b[0m\x1b[38;5;75m❯ \x1b[39m",
		"\x1b[2m"+museHintGray+strings.Repeat("─", 94),
		museHintGray+"  \x1b[38;5;75mmuse-model\x1b[38;5;246m · high · ~/project · \x1b[38;5;211mYOLO\x1b[39m",
	)
	return []byte(strings.Join(lines, "\n"))
}

// An agent that asks the human a question and waits is not idle, though its
// composer is empty: the verdict turns busy with the question-veto detail on
// every measured harness, and the same frame without the question stays
// writable.
func TestQuestionVetoHoldsAnIdleScreen(t *testing.T) {
	t.Parallel()
	type frame struct {
		classify func(row string) State
	}
	fixture := func(t *testing.T, h Harness, file, lead string, tree []Process) func(string) State {
		raw, err := os.ReadFile("testdata/" + file)
		require.NoError(t, err)
		return func(row string) State {
			capture := raw
			if row != "" {
				capture = withRowAbove(t, raw, lead, row)
			}
			return Classify(h, DefaultTable(), capture, 120, 40, 100, tree)
		}
	}
	frames := map[string]frame{
		"claude-code": {fixture(t, HarnessClaude, "claude-canon-idle-01.ansi", "❯", claudeTree())},
		"codex":       {fixture(t, HarnessCodex, "codex/idle-01.ansi", "›", codexTree())},
		"opencode":    {fixture(t, HarnessOpenCode, "opencode-canon-idle-01.ansi", "┃", opencodeTree())},
		"muse-code": {func(row string) State {
			if row == "" {
				return classifyMeasuredMuse(museQuestionFrame(), 94)
			}
			return classifyMeasuredMuse(museQuestionFrame(row), 94)
		}},
	}
	for name, f := range frames {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			idle := f.classify("")
			require.True(t, idle.Certain && !idle.BoxBusy && !idle.Busy, "detail %s", idle.Detail)
			for _, row := range []string{
				"● ¿Qué día de la semana es hoy?",
				"  What should I name the file?",
				"  Overwrite it? (y/n)",
				"  Continue [Y/n]",
				"  ¿Seguimos？",
			} {
				state := f.classify(row)
				require.True(t, state.Busy, "%q", row)
				require.Equal(t, DetailQuestionVeto, state.Detail, "%q", row)
			}
			for _, row := range []string{"● Done: the file is written.", "  Is it? No, it is not."} {
				state := f.classify(row)
				require.False(t, state.Busy, "%q", row)
				require.NotEqual(t, DetailQuestionVeto, state.Detail, "%q", row)
			}
		})
	}
}

// Only the last few content rows above the composer count: a question the
// agent asked earlier, followed by more output, no longer holds the screen.
func TestQuestionVetoReadsOnlyTheLastRows(t *testing.T) {
	t.Parallel()
	asked := classifyMeasuredMuse(museQuestionFrame("Which file?", "one", "two"), 94)
	require.True(t, asked.Busy, "detail %s", asked.Detail)
	later := classifyMeasuredMuse(museQuestionFrame("Which file?", "one", "two", "three"), 94)
	require.False(t, later.Busy, "detail %s", later.Detail)
}

// A question the human has answered no longer holds the screen: the
// harness echoes the submitted reply with its composer lead, and the scan
// ends there, whatever stands above it. The same frame with the reply still
// unsent, the question being the last agent row, holds.
func TestQuestionVetoEndsAtTheHumanReply(t *testing.T) {
	t.Parallel()
	for name, f := range map[Harness]func(rows ...string) State{
		HarnessMuse: func(rows ...string) State { return classifyMeasuredMuse(museQuestionFrame(rows...), 94) },
		HarnessClaude: func(rows ...string) State {
			raw, err := os.ReadFile("testdata/claude-canon-idle-01.ansi")
			require.NoError(t, err)
			return Classify(HarnessClaude, DefaultTable(), withRowsAbove(t, raw, "❯", rows...), 120, 40, 100, claudeTree())
		},
		HarnessCodex: func(rows ...string) State {
			raw, err := os.ReadFile("testdata/codex/idle-01.ansi")
			require.NoError(t, err)
			return Classify(HarnessCodex, DefaultTable(), withRowsAbove(t, raw, "›", rows...), 120, 40, 100, codexTree())
		},
	} {
		t.Run(string(name), func(t *testing.T) {
			t.Parallel()
			lead := map[Harness]string{HarnessMuse: "❯ ", HarnessClaude: "❯ ", HarnessCodex: "› "}[name]
			asked := f(vetoQuestion)
			require.True(t, asked.Busy, "detail %s", asked.Detail)
			answered := f(vetoQuestion, lead+vetoReply, vetoAnswer)
			require.False(t, answered.Busy, "detail %s", answered.Detail)
			require.Equal(t, DetailQuestionAnswered, answered.Detail)
			typedNotSent := f(vetoQuestion, lead+vetoReply)
			require.False(t, typedNotSent.Busy, "a submitted reply ends the question even before the agent answers")
			require.Equal(t, DetailQuestionAnswered, typedNotSent.Detail)
			noQuestion := f(lead+"hello", vetoAnswer)
			require.False(t, noQuestion.Busy)
			require.NotEqual(t, DetailQuestionAnswered, noQuestion.Detail, "a turn without a question is plain idle")
		})
	}
	later := classifyMeasuredMuse(museQuestionFrame(vetoQuestion, "❯ "+vetoReply, vetoAnswer, "one", "two", "three", "four"), 94)
	require.Equal(t, DetailQuestionAnswered, later.Detail, "the human turn is found past the veto window")
	newQuestion := classifyMeasuredMuse(museQuestionFrame(vetoQuestion, "❯ "+vetoReply, vetoAnswer, "And the next one?"), 94)
	require.True(t, newQuestion.Busy, "a new question after the reply still vetoes")
	oldQuestion := classifyMeasuredMuse(museQuestionFrame("❯ hello", "Which one?", "one", "two", "three", "four"), 94)
	require.False(t, oldQuestion.Busy)
	require.Equal(t, detailEmpty, oldQuestion.Detail, "a question after the last human turn is not answered")
	earlier := classifyMeasuredMuse(museQuestionFrame("Which one?", "❯ that one", "fine", "❯ thanks", "welcome"), 94)
	require.Equal(t, detailEmpty, earlier.Detail, "only the previous agent turn counts")
	// OpenCode echoes the reply inside its box lead and adds a metadata row
	// under each answer.
	lines := openCodeIdleLines(t)
	hints := openCodeLineWith(t, lines, "┃")
	rows := []string{
		"     What is your favourite film?",
		"     ▣  Build · model · 1.4s",
		"  ┃  Casablanca",
		"     Thank you, a fine choice.",
		"     ▣  Build · model · 1.5s",
	}
	for i, row := range rows {
		lines[hints-len(rows)-1+i] = row
	}
	state := Classify(HarnessOpenCode, DefaultTable(), []byte(strings.Join(lines, "\n")), 120, 40, 100, opencodeTree())
	require.False(t, state.Busy, "detail %s", state.Detail)
	require.Equal(t, DetailQuestionAnswered, state.Detail)
	asked := append([]string{}, lines...)
	for i := range rows {
		asked[hints-len(rows)-1+i] = ""
	}
	asked[hints-3] = rows[0]
	asked[hints-2] = rows[1]
	state = Classify(HarnessOpenCode, DefaultTable(), []byte(strings.Join(asked, "\n")), 120, 40, 100, opencodeTree())
	require.True(t, state.Busy, "detail %s", state.Detail)
}
