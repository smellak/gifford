package detect

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/vt"
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

// A fresh live screen placed a footer warning below Claude's status row. The
// warning text is private, so this VT fixture adds only the measured cells:
// U+2718 plus a space at columns 43-44 and U+006F,U+0072 at 116-117.
func TestClaudeFooterNoticeBelowStatus(t *testing.T) {
	capture, snap := claudeFooterNoticeFixture(t,
		claudeFooterNoticeAt(37))
	requireClaudeFooterNoticeShape(t, snap, 37)

	prof := DefaultTable()[HarnessClaude]
	require.True(t, footerNoticeBelowStatus(snap, prof, 37))
	state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
	require.True(t, state.Certain, "footer notice keeps composer certain (detail %s)", state.Detail)
	require.False(t, state.Busy)
	require.False(t, state.BoxBusy)
	require.Equal(t, detailEmpty, state.Detail)
}

// The post-write gate uses the same walk. A footer notice may not make
// classification succeed while leaving the exact-notice verifier unable to
// find the composer.
func TestClaudeFooterNoticeAllowsExactVerification(t *testing.T) {
	capture, snap := claudeFooterNoticeFixture(t,
		vtEraseComposerRow(),
		vtTextAt(34, 0, "❯ [GIFFORD] 1 pending message"),
		claudeFooterNoticeAt(37))
	requireClaudeFooterNoticeShape(t, snap, 37)

	certain, exact := VerifyNotice(HarnessClaude, DefaultTable(), capture, 120, 40,
		"[GIFFORD] 1 pending message")
	require.True(t, certain)
	require.True(t, exact)
}

// The glyph is not a global skip marker. Above the frame or between its
// rules it remains the first unclassified row and stops before an older
// composer-shaped line.
func TestClaudeFooterNoticeShapeOutsideFooterStopsScan(t *testing.T) {
	cases := []struct {
		name       string
		warningRow int
		controls   []string
	}{
		{
			name:       "above rule",
			warningRow: 32,
			controls: []string{
				vtEraseComposerRow(),
				vtTextAt(30, 0, "❯"),
				claudeFooterNoticeAt(32),
			},
		},
		{
			name:       "inside box",
			warningRow: 34,
			controls: []string{
				vtEraseComposerRow(),
				vtTextAt(32, 0, "❯"),
				claudeFooterNoticeAt(34),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, snap := claudeFooterNoticeFixture(t, tc.controls...)
			requireClaudeFooterNoticeCells(t, snap, tc.warningRow)

			prof := DefaultTable()[HarnessClaude]
			require.False(t, footerNoticeBelowStatus(snap, prof, tc.warningRow))
			state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
			require.False(t, state.Certain)
			require.False(t, state.BoxBusy)
			require.Equal(t, "no-composer-line", state.Detail)
		})
	}
}

// A live screen listed the session's open artifacts below Claude's status
// row: U+29C9, two spaces and the names. The names are private, so this VT
// fixture keeps only that measured lead and the measured colors.
func TestClaudeFooterTabsBelowStatus(t *testing.T) {
	capture, snap := claudeFooterNoticeFixture(t, claudeFooterTabsAt(37, "  "))
	require.True(t, isStatusLine(strings.TrimSpace(snap.Lines[36])))
	require.True(t, isRuleRow(strings.TrimSpace(snap.Lines[35])))

	require.True(t, footerTabsBelowStatus(snap, DefaultTable()[HarnessClaude], 37))
	state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
	require.True(t, state.Certain, "footer tabs keep composer certain (detail %s)", state.Detail)
	require.False(t, state.Busy)
	require.False(t, state.BoxBusy)
	require.Equal(t, detailEmpty, state.Detail)

	typed, _ := claudeFooterNoticeFixture(t,
		vtEraseComposerRow(),
		vtTextAt(34, 0, "❯ [GIFFORD] 1 pending message"),
		claudeFooterTabsAt(37, "  "))
	certain, exact := VerifyNotice(HarnessClaude, DefaultTable(), typed, 120, 40,
		"[GIFFORD] 1 pending message")
	require.True(t, certain)
	require.True(t, exact)
}

// The tabs glyph is not a global skip marker either: above the frame,
// inside the box or without its two-space lead it stops the scan.
func TestClaudeFooterTabsOutsideFooterStopsScan(t *testing.T) {
	cases := []struct {
		name     string
		tabsRow  int
		controls []string
	}{
		{name: "above rule", tabsRow: 32, controls: []string{
			vtEraseComposerRow(), vtTextAt(30, 0, "❯"), claudeFooterTabsAt(32, "  "),
		}},
		{name: "inside box", tabsRow: 34, controls: []string{
			vtEraseComposerRow(), vtTextAt(32, 0, "❯"), claudeFooterTabsAt(34, "  "),
		}},
		{name: "one space lead", tabsRow: 37, controls: []string{claudeFooterTabsAt(37, " ")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			capture, snap := claudeFooterNoticeFixture(t, tc.controls...)
			require.False(t, footerTabsBelowStatus(snap, DefaultTable()[HarnessClaude], tc.tabsRow))
			state := Classify(HarnessClaude, DefaultTable(), capture, 120, 40, 100, claudeTree())
			require.False(t, state.Certain)
			require.False(t, state.BoxBusy)
			require.Equal(t, "no-composer-line", state.Detail)
		})
	}
}

func claudeFooterTabsAt(row int, lead string) string {
	return vtTextAt(row, 2, "\x1b[38;5;174m⧉\x1b[39m"+lead+
		"\x1b[38;5;246mnotes\x1b[39m \x1b[38;5;246m·\x1b[39m \x1b[38;5;174mplan\x1b[39m")
}

func claudeFooterNoticeFixture(t *testing.T, controls ...string) ([]byte, vt.Snapshot) {
	t.Helper()
	raw, err := os.ReadFile("testdata/claude-idle-empty.ansi")
	require.NoError(t, err)
	capture := append([]byte(nil), raw...)
	for _, control := range controls {
		capture = append(capture, control...)
	}
	capture = append(capture, vtTextAt(39, 0, "")...)

	emu, err := vt.New(120, 40)
	require.NoError(t, err)
	_, err = emu.Write(NormalizeCapture(capture))
	require.NoError(t, err)
	require.NoError(t, emu.Flush())
	return capture, emu.Snapshot()
}

func claudeFooterNoticeAt(row int) string {
	return vtTextAt(row, 43, "\x1b[1m✘\x1b[0m ") +
		vtTextAt(row, 116, "\x1b[1mor\x1b[0m")
}

// vtEraseComposerRow clears the composer row of the 120x40 idle frame.
func vtEraseComposerRow() string {
	return fmt.Sprintf("\x1b[%d;1H\x1b[2K", 34+1)
}

func vtTextAt(row, column int, text string) string {
	return fmt.Sprintf("\x1b[%d;%dH%s", row+1, column+1, text)
}

func requireClaudeFooterNoticeShape(t *testing.T, snap vt.Snapshot, row int) {
	t.Helper()
	require.Equal(t, 120, snap.Cols)
	require.Equal(t, 40, snap.Rows)
	require.Equal(t, 39, snap.CursorRow)
	require.Equal(t, 0, snap.CursorCol)
	status := strings.TrimSpace(snap.Lines[row-1])
	require.True(t, isStatusLine(status))
	require.True(t, isChromeRow(DefaultTable()[HarnessClaude], status))
	require.True(t, isRuleRow(strings.TrimSpace(snap.Lines[row-2])))
	requireClaudeFooterNoticeCells(t, snap, row)
}

func requireClaudeFooterNoticeCells(t *testing.T, snap vt.Snapshot, row int) {
	t.Helper()
	cells, ok := snap.CellsForRow(row)
	require.True(t, ok)
	require.Len(t, cells, 120)
	require.Equal(t, vt.CursorCell{Text: "✘", Col: 43, Width: 1, OtherStyle: true},
		publicCell(cells[43]))
	require.Equal(t, vt.CursorCell{Text: " ", Col: 44, Width: 1}, publicCell(cells[44]))
	require.Equal(t, vt.CursorCell{Text: "o", Col: 116, Width: 1, OtherStyle: true},
		publicCell(cells[116]))
	require.Equal(t, vt.CursorCell{Text: "r", Col: 117, Width: 1, OtherStyle: true},
		publicCell(cells[117]))
}

func publicCell(cell vt.CursorCell) vt.CursorCell {
	return vt.CursorCell{
		Text:       cell.Text,
		Col:        cell.Col,
		Width:      cell.Width,
		Faint:      cell.Faint,
		OtherStyle: cell.OtherStyle,
	}
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
