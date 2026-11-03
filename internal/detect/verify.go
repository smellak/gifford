package detect

import (
	"strings"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"

	"github.com/smellak/gifford/internal/vt"
)

// VerifyNotice checks the post-write gate for the tmux attach path: the
// box must hold exactly the allowlisted notice line and nothing else
// before Enter is pressed. It reuses the same composer and shell
// projections as Classify, so the writer never duplicates the parser.
//
// exact is true only on full equality; a composer box wrapped across
// rows is read whole, as judgeComposerBox explains. certain is false
// whenever the screen cannot support a verdict: no composer or input
// line, or rows that spell the notice at breaks no wrap explains. A
// non-exact certain screen and an uncertain screen both forbid Enter;
// they differ only for the record.
func VerifyNotice(h Harness, table Table, capture []byte, cols, rows int, notice string) (certain, exact bool) {
	if notice == "" {
		return false, false
	}
	prof, ok := table[h]
	if !ok {
		return false, false
	}
	emu, err := vt.New(cols, rows)
	if err != nil {
		return false, false
	}
	// capture-pane ends rows with a bare LF. Fed raw, the emulator keeps
	// the column across rows and draws a staircase in which the composer
	// is never found; Classify normalizes for the same reason.
	if _, err := emu.Write(NormalizeCapture(capture)); err != nil {
		return false, false
	}
	if err := emu.Flush(); err != nil {
		return false, false
	}
	snap := emu.Snapshot()

	if prof.Composer {
		return verifyComposerBox(snap, prof, notice)
	}
	return verifyExprBox(snap, prof.InputExpr, notice)
}

// verifyComposerBox finds the composer line and compares it whole.
func verifyComposerBox(snap vt.Snapshot, prof Profile, notice string) (certain, exact bool) {
	row, rest, found := findComposer(snap, prof)
	if !found {
		return false, false
	}
	if prof.MultiRowBox {
		_, content := multiRowBoxContent(snap, prof, row)
		return true, content == notice
	}
	return judgeComposerBox(snap, prof, row, rest, notice)
}

// judgeComposerBox compares a single-row composer's text with the notice,
// read from the composer row down through the rows a narrow pane wrapped
// it onto. A wrap drops the space it breaks at, or breaks inside a word,
// after a hyphen or at the box edge, so the rows hold the notice only
// when they compose it whole and in order, with at most the dropped
// space at each break. Rows that compose the notice at a break the pane
// width did not force stay uncertain: they hold a line break, or are not
// the box's rows. Every other mismatch is certain non-exact.
func judgeComposerBox(snap vt.Snapshot, prof Profile, row int, rest, notice string) (certain, exact bool) {
	if rest == "" {
		return true, false
	}
	box := boxRows(snap, prof, row, rest)
	at, forced := 0, true
	for i, part := range box {
		if i > 0 {
			gap := 0
			if strings.HasPrefix(notice[at:], " ") {
				gap = 1
			}
			forced = forced && wrapForced(snap, box, i, gap)
			at += gap
		}
		if !strings.HasPrefix(notice[at:], part.text) {
			return true, false
		}
		at += len(part.text)
	}
	if at != len(notice) {
		return true, false
	}
	return forced, forced
}

// wrapSlack is the margin, in cells, that wrapForced allows between the
// right edge of a composer's text and the pane edge, where TUIs keep a
// few cells free.
const wrapSlack = 8

// wrapForced reports whether the pane width can explain the break before
// box row i: the shortest piece of that row a wrap could have kept on
// the row above, after the gap it dropped, must not have fit there. No
// row is wider than the box, so the box edge lies at or past the widest
// row, and within wrapSlack cells of the pane edge.
func wrapForced(snap vt.Snapshot, box []rowText, i, gap int) bool {
	edge := snap.Cols - wrapSlack
	for _, r := range box {
		edge = max(edge, ansi.StringWidth(snap.Lines[r.row]))
	}
	end := ansi.StringWidth(snap.Lines[box[i-1].row])
	return end+gap+ansi.StringWidth(wrapPiece(box[i-1].text, box[i].text, gap)) > edge
}

// wrapPiece returns the shortest leading piece of next that a wrap could
// have kept on the row above: after a dropped space or a hyphen, the
// first word up to its first space or hyphen; inside a word, one
// character, since such a break fills the row to the box edge.
func wrapPiece(prev, next string, gap int) string {
	if gap == 0 && !strings.HasSuffix(prev, "-") {
		_, size := utf8.DecodeRuneInString(next)
		return next[:size]
	}
	if at := strings.IndexAny(next, " -"); at >= 0 {
		if next[at] == '-' {
			return next[:at+1]
		}
		return next[:at]
	}
	return next
}

// verifyExprBox extracts the shell input line and compares it whole.
func verifyExprBox(snap vt.Snapshot, expr, notice string) (certain, exact bool) {
	pattern, err := vt.NewInputPattern(expr)
	if err != nil {
		return false, false
	}
	text, found := vt.InputLine(snap, pattern)
	if !found {
		return false, false
	}
	row := boxRowFor(snap, text)
	return judgeBox(snap.Lines, row, text, notice)
}

// findComposer returns the composer row and its rest, scanning
// cursor-row first then bottom-up like composerBox, above the same
// footer anchor.
func findComposer(snap vt.Snapshot, prof Profile) (row int, rest string, found bool) {
	rows := []int{}
	if snap.CursorRow >= 0 && snap.CursorRow < len(snap.Lines) {
		rows = append(rows, snap.CursorRow)
	}
	for r := len(snap.Lines) - 1; r >= 0; r-- {
		if r != snap.CursorRow {
			rows = append(rows, r)
		}
	}
	anchor := footerAnchor(snap, prof)
	var skippedChrome []int
	for _, r := range rows {
		if anchor >= 0 && r > anchor {
			continue
		}
		s := strings.TrimSpace(snap.Lines[r])
		if s == "" {
			continue
		}
		rest, ok := composerRest(s, prof.ComposerLead)
		if !ok {
			// Rules frame the box like status does, but only where the
			// profile measures them; anything else ends the scan,
			// mirroring composerBox.
			if isStatusLine(s) || (prof.SkipRules && isRuleRow(s)) {
				continue
			}
			if chromeSkipRow(prof, snap, &skippedChrome, r, s) {
				continue
			}
			return wrappedComposer(snap, prof, skippedChrome, r)
		}
		if !chromeSkippedValid(prof, snap, skippedChrome, r) {
			return 0, "", false
		}
		return r, rest, true
	}
	return 0, "", false
}

// wrappedComposer returns the single-row composer that a content row
// continues as a narrow-pane wrap, as composerBox reads it. Any other
// content row ends the scan with no composer.
func wrappedComposer(snap vt.Snapshot, prof Profile, skipped []int, row int) (int, string, bool) {
	if prof.MultiRowBox {
		return 0, "", false
	}
	top, rest, ok := wrappedAbove(snap, prof, row)
	if !ok || !chromeSkippedValid(prof, snap, skipped, top) {
		return 0, "", false
	}
	return top, rest, true
}

// boxRowFor locates the cursor-row input text for the wrap check. It
// fails closed with -1 when the text is not on the cursor row.
func boxRowFor(snap vt.Snapshot, text string) int {
	row := snap.CursorRow
	if row < 0 || row >= len(snap.Lines) || text == "" {
		return -1
	}
	if strings.LastIndex(snap.Lines[row], text) < 0 {
		return -1
	}
	return row
}

// judgeBox compares one box content against the notice. Equality is
// exact. A proper-prefix relation in either direction may be a notice
// wrapped across rows: only a continuation that completes the notice
// proves the wrap, and any wrap stays uncertain. Every other mismatch
// is certain non-exact.
func judgeBox(lines []string, row int, content, notice string) (certain, exact bool) {
	if row < 0 {
		return false, false
	}
	if content == notice {
		return true, true
	}
	if content != "" && (strings.HasPrefix(notice, content) || strings.HasPrefix(content, notice)) {
		if row+1 < len(lines) {
			if content+strings.TrimSpace(lines[row+1]) == notice {
				return false, false
			}
		}
	}
	return true, false
}
