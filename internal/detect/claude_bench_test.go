package detect

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bench over the 4 sanitized Claude bench frames from the bank
// (testdata/claude-*.ansi, 120x40, users/paths/hosts neutralized)
// plus the 12 canonical bank frames (testdata/claude-canon-*.ansi,
// idle/working/text/dialog x3, same geometry and sanitizer).
// Truth comes with the captures; the acceptance criteria: idle
// Certain and free, pending box occupied with the exact notice,
// working Busy, permission dialog never writable.
func TestClaudeBenchCaptures(t *testing.T) {
	cases := []struct {
		file          string
		certain       bool
		busy, boxBusy bool
	}{
		{"claude-idle-empty.ansi", true, false, false},
		{"claude-idle-pending-unsent.ansi", true, false, true},
		{"claude-working.ansi", true, true, false},
		{"claude-permission-dialog.ansi", false, false, false},
		{"claude-canon-idle-01.ansi", true, false, false},
		{"claude-canon-idle-02.ansi", true, false, false},
		{"claude-canon-idle-03.ansi", true, false, false},
		{"claude-canon-working-01.ansi", true, true, false},
		{"claude-canon-working-02.ansi", true, true, false},
		{"claude-canon-working-03.ansi", true, true, false},
		{"claude-canon-text-01.ansi", true, false, true},
		{"claude-canon-text-02.ansi", true, false, true},
		{"claude-canon-text-03.ansi", true, false, true},
		{"claude-canon-dialog-01.ansi", false, false, false},
		{"claude-canon-dialog-02.ansi", false, false, false},
		{"claude-canon-dialog-03.ansi", false, false, false},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile("testdata/" + tc.file)
		require.NoError(t, err)
		st := Classify(HarnessClaude, DefaultTable(), raw, 120, 40, 100, claudeTree())
		require.Equal(t, tc.certain, st.Certain, "%s certain (detail %s)", tc.file, st.Detail)
		require.Equal(t, tc.busy, st.Busy, "%s busy", tc.file)
		require.Equal(t, tc.boxBusy, st.BoxBusy, "%s box (detail %s)", tc.file, st.Detail)
		require.True(t, st.IsCLI, "%s runs the CLI", tc.file)
		// Only idle screens may allow a write: certain, free box,
		// and not working.
		writable := st.Certain && !st.BoxBusy && !st.Busy
		wantWritable := tc.file == "claude-idle-empty.ansi" ||
			strings.HasPrefix(tc.file, "claude-canon-idle-")
		require.Equal(t, wantWritable, writable,
			"%s write gate (detail %s)", tc.file, st.Detail)
	}
}

// The pending frame must hold exactly the allowlisted notice: the
// writer's Enter gate depends on it.
func TestClaudePendingNoticeVerifiesExact(t *testing.T) {
	raw, err := os.ReadFile("testdata/claude-idle-pending-unsent.ansi")
	require.NoError(t, err)
	certain, exact := VerifyNotice(HarnessClaude, DefaultTable(),
		raw, 120, 40, "[GIFFORD] 1 pending message")
	require.True(t, certain)
	require.True(t, exact)
}

// Live 2.1.280 screens print rows below Claude's status row: a warning
// (U+2718 and a space), the open artifacts (U+29C9, two spaces and the
// names) and right-aligned token or context hints, alone or together and
// with blank rows between them. The private texts are neutral here; the
// shapes and columns are the measured ones. Below the status anchor each
// is footer, so the box stays certain and free and the exact-notice
// verifier still finds it.
func TestClaudeFooterBelowStatusIsChrome(t *testing.T) {
	cases := []struct {
		name     string
		controls []string
	}{
		{"warning", []string{claudeFooterNoticeAt(37)}},
		{"artifact tabs", []string{claudeFooterTabsAt(37, "  ")}},
		{"tabs after one space", []string{claudeFooterTabsAt(37, " ")}},
		{"blank row before tabs", []string{claudeFooterTabsAt(38, "  ")}},
		{"token hint", []string{vtTextAt(37, 54, "new task? /clear to save 437.5k tokens")}},
		{"context hint and tabs", []string{
			vtTextAt(37, 96, "2% until auto-compact"), claudeFooterTabsAt(38, "  "),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := claudeFooterFixture(t, tc.controls...)
			state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
			require.True(t, state.Certain, "footer keeps the composer certain (detail %s)", state.Detail)
			require.False(t, state.Busy)
			require.False(t, state.BoxBusy)
			require.Equal(t, detailEmpty, state.Detail)

			typed := claudeFooterFixture(t, append([]string{
				vtEraseRow(34), vtTextAt(34, 0, "❯ [GIFFORD] 1 pending message"),
			}, tc.controls...)...)
			certain, exact := VerifyNotice(HarnessClaude, DefaultTable(), typed, 120, 40,
				"[GIFFORD] 1 pending message")
			require.True(t, certain)
			require.True(t, exact)
		})
	}
}

// Live 2.1.280 screens out of usage print warning rows (U+26A0) between the
// box's bottom rule and the status row, alone or after a blank row. The box
// is what lies between its two rules, so every row from the bottom rule down
// is footer, and the exact-notice verifier still finds the box. The warning
// that the session continues automatically marks the pane busy: typing
// would cancel that continuation.
func TestClaudeWarningRowsAboveStatusAreFooter(t *testing.T) {
	status := vtTextAt(38, 2, "⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents")
	cases := []struct {
		name     string
		controls []string
		busy     bool
	}{
		{"two warnings", []string{
			vtEraseRow(36), claudeWarningAt(36, claudeUsageLimitWarning),
			claudeWarningAt(37, "/usage-credits to continue now"), status,
		}, true},
		{"blank row then warning", []string{
			vtEraseRow(36), claudeWarningAt(37, "/usage-credits to continue now"), status,
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := claudeFooterFixture(t, tc.controls...)
			state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
			require.True(t, state.Certain, "rows above the status are footer (detail %s)", state.Detail)
			require.Equal(t, tc.busy, state.Busy)
			require.False(t, state.BoxBusy)
			require.Equal(t, detailEmpty, state.Detail)

			typed := claudeFooterFixture(t, append([]string{
				vtEraseRow(34), vtTextAt(34, 0, "❯ [GIFFORD] 1 pending message"),
			}, tc.controls...)...)
			certain, exact := VerifyNotice(HarnessClaude, DefaultTable(), typed, 120, 40,
				"[GIFFORD] 1 pending message")
			require.True(t, certain)
			require.True(t, exact)
		})
	}
}

// claudeUsageLimitWarning is the 2.1.280 footer warning of a session that
// resumes by itself when its usage limit resets.
const claudeUsageLimitWarning = "Usage limit reached · continuing automatically at 9pm · esc to cancel"

// The usage-limit warning marks the pane busy only from the footer. The same
// words in the transcript above the box are content: an idle pane that
// shows them there, or a reply quoting them, stays free.
func TestClaudeUsageLimitBusyOnlyFromFooter(t *testing.T) {
	status := vtTextAt(38, 2, "⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents")
	transcript := vtEraseRow(30) + vtTextAt(30, 0, "● "+claudeUsageLimitWarning)

	quoted := claudeFooterFixture(t, transcript)
	state := Classify(HarnessClaude, DefaultTable(), quoted, 120, 40, 100, claudeTree())
	require.True(t, state.Certain, "detail %s", state.Detail)
	require.False(t, state.Busy, "a transcript row never marks the pane busy")
	require.False(t, state.BoxBusy)

	waiting := claudeFooterFixture(t, transcript, vtEraseRow(36),
		claudeWarningAt(36, claudeUsageLimitWarning), status)
	state = Classify(HarnessClaude, DefaultTable(), waiting, 120, 40, 100, claudeTree())
	require.True(t, state.Certain, "detail %s", state.Detail)
	require.True(t, state.Busy, "the footer warning holds the delivery")
	require.False(t, state.BoxBusy)
}

// The footer starts only below the box's bottom rule over a status row.
// Above the frame or between its rules the same rows stay decisive unknown
// content and stop before an older composer-shaped line. Without a status
// row, or without the top rule that closes the box, there is no footer at
// all.
func TestClaudeFooterRowsOutsideFooterStopScan(t *testing.T) {
	cases := []struct {
		name     string
		controls []string
	}{
		{"warning above rule", []string{vtEraseRow(34), vtTextAt(30, 0, "❯"), claudeFooterNoticeAt(32)}},
		{"warning inside box", []string{vtEraseRow(34), vtTextAt(32, 0, "❯"), claudeFooterNoticeAt(34)}},
		{"tabs above rule", []string{vtEraseRow(34), vtTextAt(30, 0, "❯"), claudeFooterTabsAt(32, "  ")}},
		{"tabs inside box", []string{vtEraseRow(34), vtTextAt(32, 0, "❯"), claudeFooterTabsAt(34, "  ")}},
		{"no status row", []string{vtEraseRow(36), claudeFooterTabsAt(37, "  ")}},
		{"warning under a box without top rule", []string{
			vtEraseRow(33), vtEraseRow(36), claudeWarningAt(36, "/usage-credits to continue now"),
			vtTextAt(37, 2, "⏵⏵ bypass permissions on (shift+tab to cycle)"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture := claudeFooterFixture(t, tc.controls...)
			state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
			require.False(t, state.Certain)
			require.False(t, state.BoxBusy)
			require.Equal(t, "no-composer-line", state.Detail)
		})
	}
}

// A live 2.1.280 status row in auto mode carries no other measured status
// fragment.
func TestClaudeAutoModeStatusRow(t *testing.T) {
	capture := claudeFooterFixture(t, vtEraseRow(36),
		vtTextAt(36, 2, "⏵⏵ auto mode on (shift+tab to cycle)"),
		vtTextAt(37, 54, "new task? /clear to save 437.5k tokens"))
	state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
	require.True(t, state.Certain, "auto mode status anchors the footer (detail %s)", state.Detail)
	require.False(t, state.BoxBusy)
	require.Equal(t, detailEmpty, state.Detail)
}

func claudeFooterTabsAt(row int, lead string) string {
	return vtTextAt(row, 2, "\x1b[38;5;174m⧉\x1b[39m"+lead+
		"\x1b[38;5;246mnotes\x1b[39m \x1b[38;5;246m·\x1b[39m \x1b[38;5;174mplan\x1b[39m")
}

func claudeWarningAt(row int, text string) string {
	return vtTextAt(row, 2, "\x1b[38;5;220m⚠\x1b[39m "+text)
}

func claudeFooterNoticeAt(row int) string {
	return vtTextAt(row, 43, "\x1b[1m✘\x1b[0m ") +
		vtTextAt(row, 116, "\x1b[1mor\x1b[0m")
}

// claudeFooterFixture draws controls over the 120x40 idle frame, whose
// box spans rows 33-35 under the status row 36, and parks the cursor on
// the last row like a capture does.
func claudeFooterFixture(t *testing.T, controls ...string) []byte {
	t.Helper()
	raw, err := os.ReadFile("testdata/claude-idle-empty.ansi")
	require.NoError(t, err)
	capture := append([]byte(nil), raw...)
	for _, control := range controls {
		capture = append(capture, control...)
	}
	return append(capture, vtTextAt(39, 0, "")...)
}

func vtEraseRow(row int) string {
	return fmt.Sprintf("\x1b[%d;1H\x1b[2K", row+1)
}

func vtTextAt(row, column int, text string) string {
	return fmt.Sprintf("\x1b[%d;%dH%s", row+1, column+1, text)
}

// The bank's Claude Code paints a dim prompt suggestion in the empty
// composer after every response; the pixels below are public-safe
// synthetics in that measured shape (faint suggestion between rules,
// idle status line), never the bank capture. The suggestion must read
// as a free box, while the same sentence typed bright stays occupied.
func TestClaudeDimSuggestionGhost(t *testing.T) {
	const suggestion = "Summarize the changes in plain language"
	ghost := claudeGhostFrame("\x1b[2m" + suggestion + "\x1b[0m")
	typed := claudeGhostFrame(suggestion)
	free := Classify(HarnessClaude, DefaultTable(), ghost, 120, 40, 100, claudeTree())
	require.True(t, free.Certain, "ghost certain (detail %s)", free.Detail)
	require.False(t, free.BoxBusy, "ghost free (detail %s)", free.Detail)
	require.False(t, free.Busy, "ghost idle")
	require.True(t, free.IsCLI, "ghost runs the CLI")
	occupied := Classify(HarnessClaude, DefaultTable(), typed, 120, 40, 100, claudeTree())
	require.True(t, occupied.Certain, "typed certain (detail %s)", occupied.Detail)
	require.True(t, occupied.BoxBusy, "typed occupied (detail %s)", occupied.Detail)
}

// claudeGhostFrame builds one synthetic idle screen holding composer
// text between rules under the idle status line.
func claudeGhostFrame(composer string) []byte {
	lines := make([]string, 40)
	lines[1] = "  Claude Code"
	lines[3] = strings.Repeat("\u2500", 120)
	lines[4] = "❯ " + composer
	lines[5] = strings.Repeat("\u2500", 120)
	lines[6] = "  ⏸ manual mode on · ? for shortcuts"
	return []byte(strings.Join(lines, "\n"))
}

// claudeTree returns a pane tree with the Claude CLI in the foreground:
// its group owns the pane tty. Trees do not come in the frames, so the
// harness half is synthetic while the screen half is measured.
func claudeTree() []Process {
	return []Process{
		{PID: 100, PPID: 50, Pgrp: 100, State: 'S', TPGID: 200, Argv: []string{"/bin/bash"}},
		{PID: 200, PPID: 100, Pgrp: 200, State: 'S', TPGID: 200, Argv: []string{"claude"}},
	}
}
