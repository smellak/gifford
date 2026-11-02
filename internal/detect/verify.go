package detect

import (
	"strings"

	"github.com/smellak/gifford/internal/vt"
)

// VerifyNotice checks the post-write gate for the tmux attach path: the
// box must hold exactly the allowlisted notice line and nothing else
// before Enter is pressed. It reuses the same composer and shell
// projections as Classify, so the writer never duplicates the parser.
//
// exact is true only on full equality. certain is false whenever the
// screen cannot support a verdict: no composer or input line, or a
// wrapped notice split across rows. A non-exact certain screen and an
// uncertain screen both forbid Enter; they differ only for the record.
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
	if _, err := emu.Write(capture); err != nil {
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
	return judgeBox(snap.Lines, row, rest, notice)
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
// cursor-row first then bottom-up like composerBox.
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
	for _, r := range rows {
		s := strings.TrimSpace(snap.Lines[r])
		if s == "" {
			continue
		}
		rest, ok := composerRest(s, prof.ComposerLead)
		if !ok {
			if isStatusLine(s) || isChromeRow(prof, s) {
				continue
			}
			return 0, "", false
		}
		return r, rest, true
	}
	return 0, "", false
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
