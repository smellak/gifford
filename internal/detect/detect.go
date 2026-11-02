package detect

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/smellak/gifford/internal/vt"
)

// Harness names the agent harness whose screen markers apply.
type Harness string

// Known harnesses. Only profiles with bench-measured markers classify;
// the rest stay uncertain rather than guessing.
const (
	HarnessShell    Harness = "shell"
	HarnessCodex    Harness = "codex"
	HarnessClaude   Harness = "claude"
	HarnessMuse     Harness = "muse"
	HarnessOpenCode Harness = "opencode"
)

// Profile holds one harness's screen markers. BusyMarks are substrings
// whose presence in the last lines means the agent is working.
// CLIBinaries are executable basenames proving the pane runs the harness
// CLI. TUI agents set Composer and are read through composerRest plus
// Placeholders; plain shells use InputExpr with exactly one capture
// group, as vt.NewInputPattern requires.
type Profile struct {
	BusyMarks    []string
	CLIBinaries  []string
	Composer     bool
	Placeholders []string
	InputExpr    string
	// MultiRow is for bordered boxes spanning several composer rows
	// (OpenCode): content is the concatenation of their non-empty rests.
	// The trailing model line is dropped when more rows sit above it.
	MultiRow bool
}

// Table maps every known harness to its profile.
type Table map[Harness]Profile

// placeholderSet holds texts TUIs paint inside an empty composer as a
// suggestion, measured on real screens. Reading one as typed input would
// wedge the agent busy forever, so they count as an empty box.
var placeholderSet = []string{
	"Explain this codebase",
	"Summarize recent commits",
	"Implement {feature}",
	"Find and fix a bug in @filename",
	"Fix a bug",
	"Improve documentation in @filename",
	"Write tests for @filename",
	"Refactor @filename",
	"Run /review on my current changes",
	"Use /skills to list available skills",
	"Check recently modified functions for compatibility",
	"How many files have been modified?",
	"Will this algorithm scale well?",
	`Try "how do I log an error?"`,
	`Try "edit <filepath> to..."`,
}

// interruptMark is the working indicator shared by the Codex and Claude
// TUIs, measured on real screens of both.
const interruptMark = "esc to interrupt"

// detailStyleMissing is the content-free code for box text whose style
// evidence is absent or incomplete.
const detailStyleMissing = "style-missing"

// detailEmpty and detailTyped are the content-free codes for an empty
// box and for typed box content.
const (
	detailEmpty = "empty"
	detailTyped = "typed"
)

// statusFragments are TUI status lines, never composer content,
// measured on real screens. Short variants count too: the pending
// frame shows a bare "manual mode on", and the permission dialog splits
// "Esc to cancel · Tab to amend" across two rows.
var statusFragments = []string{
	"bypass permissions",
	"for agents",
	interruptMark,
	"manual mode on",
	"Esc to cancel",
	"Tab to amend",
	"· ~",
}

// labelledComposer finds the Codex labelled form
// "SESSION-NAME   › rest", where two spaces separate the label.
var labelledComposer = regexp.MustCompile(`\s{2,}›\s*(.*)$`)

// DefaultTable ships the shell, Codex and Claude profiles, with the
// working indicator, composer forms and placeholder set measured on real
// screens. Muse and OpenCode stay empty until bench-verified markers
// land.
func DefaultTable() Table {
	return Table{
		HarnessShell: {
			// The trailing space of a fresh prompt does not survive
			// the snapshot, so the separator space is optional.
			InputExpr:   `[$#] ?(.*)$`,
			CLIBinaries: []string{"sh", "bash", "zsh", "fish"},
		},
		HarnessCodex: {
			BusyMarks:    []string{interruptMark},
			CLIBinaries:  []string{"codex"},
			Composer:     true,
			Placeholders: placeholderSet,
		},
		// Claude composes after ❯ plus a wide space (U+00A0 in bench
		// frames); the idle status line carries no interrupt mark, the
		// working one reads "esc to interrupt". A permission dialog has
		// no verifiable composer — its option lines are transcript, not
		// the box — so it reads uncertain and nothing is written.
		HarnessClaude: {
			BusyMarks:    []string{interruptMark},
			CLIBinaries:  []string{"claude"},
			Composer:     true,
			Placeholders: placeholderSet,
		},
		HarnessMuse: {},
		// OpenCode draws its composer as a bordered box: content rows
		// start with ┃, the model line sits on its own row below, and
		// the working state shows "esc interrupt" with spinner dots in
		// the status bar. "Ask anything" is the known empty-box text.
		HarnessOpenCode: {
			BusyMarks:    []string{"esc interrupt"},
			CLIBinaries:  []string{"opencode"},
			Composer:     true,
			MultiRow:     true,
			Placeholders: []string{"Ask anything"},
		},
	}
}

// Process is one process in the pane's tree. Pgrp, State and TPGID come
// from fields 5, 3 and 8 of /proc/<pid>/stat, readable across users:
// Pgrp is the process group, State its single-letter state, and TPGID the
// foreground group of the pane's tty. Presence in the tree is not enough:
// only a CLI in the foreground counts, so a suspended (T) or traced (t)
// CLI with the shell back in front is not the pane's agent.
type Process struct {
	PID       int
	PPID      int
	Pgrp      int
	State     byte
	TPGID     int
	StartTime uint64
	Argv      []string
}

// State is one classification. Certain is false whenever the input does
// not support a verdict; the caller must not write then. Detail is a
// content-free code (empty, placeholder, typed, unmapped, style-missing,
// …): it never carries pane text, so it is safe for logs and the record.
type State struct {
	Certain bool
	Busy    bool
	BoxBusy bool
	IsCLI   bool
	Detail  string
}

// Classify answers busy, box-busy and is-CLI for one pane. capture is a
// `capture-pane -p -e` dump of a cols-by-rows pane, panePID the pane's
// child PID, and procs the process list the tree is walked on.
func Classify(h Harness, table Table, capture []byte, cols, rows int, panePID int, procs []Process) State {
	prof, ok := table[h]
	if !ok {
		return State{Detail: "unknown-harness"}
	}
	emu, err := vt.New(cols, rows)
	if err != nil {
		return State{Detail: fmt.Sprintf("emulator: %v", err)}
	}
	if _, err := emu.Write(capture); err != nil {
		return State{Detail: fmt.Sprintf("emulator write: %v", err)}
	}
	if err := emu.Flush(); err != nil {
		return State{Detail: fmt.Sprintf("emulator flush: %v", err)}
	}
	snap := emu.Snapshot()

	busy := false
	tail := tailNonBlank(snap.Text(), 8)
	for _, mark := range prof.BusyMarks {
		if mark != "" && strings.Contains(tail, mark) {
			busy = true
			break
		}
	}

	var boxBusy, certain bool
	var detail string
	switch {
	case prof.MultiRow:
		boxBusy, certain, detail = multiRowBox(snap, prof.Placeholders)
	case prof.Composer:
		boxBusy, certain, detail = composerBox(snap, prof.Placeholders)
	default:
		boxBusy, certain, detail = exprBox(snap, prof.InputExpr)
	}
	return State{
		Certain: certain,
		Busy:    busy,
		BoxBusy: boxBusy,
		IsCLI:   isCLI(prof.CLIBinaries, panePID, procs),
		Detail:  detail,
	}
}

// tailNonBlank joins the last n non-blank lines. tmux pads the pane
// below the shell footer, so the tail is what the agent shows.
func tailNonBlank(text string, n int) string {
	var kept []string
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) != "" {
			kept = append(kept, line)
		}
	}
	if len(kept) > n {
		kept = kept[len(kept)-n:]
	}
	return strings.Join(kept, "\n")
}

// composerRest extracts the composer text from one stripped screen line:
// a leading ❯ (claude), > or › (codex) opens the composer, as does the
// labelled "NAME   › rest" form. OpenCode draws its box with a ┃ border
// instead: the rest is the text after the two-space gutter. Anything else
// is transcript, not the box.
func composerRest(s string) (string, bool) {
	if strings.HasPrefix(s, "❯") || strings.HasPrefix(s, ">") || strings.HasPrefix(s, "›") {
		return strings.TrimSpace(strings.TrimLeft(s, "❯>› ")), true
	}
	if rest, ok := strings.CutPrefix(s, "┃"); ok {
		return strings.TrimSpace(strings.TrimPrefix(rest, "  ")), true
	}
	if m := labelledComposer.FindStringSubmatch(s); m != nil {
		return strings.TrimSpace(m[1]), true
	}
	return "", false
}

// composerBox classifies the TUI input box. The scan runs cursor-row
// first, then bottom-up, skipping status lines; the first composer line
// decides. Known placeholders and empty composers are a free box.
// Unknown text counts only with non-faint style evidence; without it the
// verdict stays uncertain instead of guessing.
func composerBox(snap vt.Snapshot, placeholders []string) (busy, certain bool, detail string) {
	known := map[string]bool{}
	for _, p := range placeholders {
		known[p] = true
	}
	rows := scanRows(snap)
	for _, row := range rows {
		rest, hit := scanRow(snap.Lines[row])
		if hit == hitBlank || hit == hitChrome {
			// Rules and status chrome frame the box; neither is box
			// content, so the scan passes through both.
			continue
		}
		if hit == hitOther {
			// The box was not found below this transcript.
			break
		}
		if rest == "" || known[rest] {
			return false, true, detailEmpty
		}
		return styledText(snap, row, snap.Lines[row], rest)
	}
	// No composer line at all: uncertain, never a free box. A live agent
	// on its alternate screen shows no composer while working.
	return false, false, "no-composer-line"
}

// scanHit classifies one screen line for the composer scan.
type scanHit int

const (
	hitBlank scanHit = iota
	hitComposer
	hitChrome
	hitOther
)

// scanRow classifies one raw screen line. Status chrome and horizontal
// rules frame the box and are passed through; anything else that is not
// the composer ends the scan.
func scanRow(line string) (string, scanHit) {
	s := strings.TrimSpace(line)
	if s == "" {
		return "", hitBlank
	}
	if rest, ok := composerRest(s); ok {
		return rest, hitComposer
	}
	if isStatusLine(s) || isRuleLine(s) {
		return "", hitChrome
	}
	return "", hitOther
}

// scanRows orders the rows cursor-first, then bottom-up, so the live box
// wins over older transcript echoes.
func scanRows(snap vt.Snapshot) []int {
	rows := []int{}
	if snap.CursorRow >= 0 && snap.CursorRow < len(snap.Lines) {
		rows = append(rows, snap.CursorRow)
	}
	for row := len(snap.Lines) - 1; row >= 0; row-- {
		if row != snap.CursorRow {
			rows = append(rows, row)
		}
	}
	return rows
}

// isStatusLine reports TUI status chrome that is never box content.
func isStatusLine(s string) bool {
	for _, frag := range statusFragments {
		if strings.Contains(s, frag) {
			return true
		}
	}
	return false
}

// isRuleLine reports a horizontal rule: made only of ─ cells (the box
// frame Claude paints above and below its composer) or of ▀ cells, with
// an optional leading ╹ (the foot rule closing the OpenCode box).
func isRuleLine(s string) bool {
	t := strings.TrimSpace(s)
	return t != "" && (strings.Trim(t, "─▀") == "" || strings.HasPrefix(t, "╹"))
}

// isBottomRule reports the foot rule closing a bordered box: it starts
// with ╹ or is made only of ─/▀ cells (OpenCode).
func isBottomRule(s string) bool {
	t := strings.TrimSpace(s)
	return t != "" && (strings.HasPrefix(t, "╹") || strings.Trim(t, "─▀") == "")
}

// modelPrefix marks the model line OpenCode prints as the last content
// row of its box, e.g. "Build auto · <model>". It is chrome, not input,
// but only the trailing such row is dropped: user text elsewhere never is.
const modelPrefix = "Build "

// collectRows gathers composer hits bottom-up, skipping the footer below
// the bottom rule, blanks, rules and status chrome, and stopping at the
// first transcript line. The model line and its wrapped remnant (rows
// between it and the bottom rule that are not composer rows) are chrome,
// never content. The lowest hit is the trailing model line when more hits
// sit above it, and is dropped.
func collectRows(snap vt.Snapshot) (rests []string, found bool) {
	bottom := -1
	for r := len(snap.Lines) - 1; r >= 0; r-- {
		if isBottomRule(strings.TrimSpace(snap.Lines[r])) {
			bottom = r
			break
		}
	}
	model := modelRow(snap, bottom)
	var hits []composerHit
	for r := len(snap.Lines) - 1; r >= 0; r-- {
		if bottom >= 0 && r > bottom {
			continue
		}
		if model >= 0 && bottom >= 0 && r > model && r < bottom {
			continue
		}
		rest, hit := scanRow(snap.Lines[r])
		switch hit {
		case hitComposer:
			hits = append(hits, composerHit{row: r, rest: rest})
		case hitBlank, hitChrome:
			continue
		default:
			return dropModel(hits), len(hits) > 0
		}
	}
	return dropModel(hits), len(hits) > 0
}

// modelRow locates the model line: the lowest composer row carrying the
// model prefix, e.g. the "Build auto · <model>" row OpenCode prints last
// in its box. Rows between it and the bottom rule are its wrapped
// remnant when they are not composer rows, so the main loop skips the
// whole span. It returns -1 when no such row exists.
func modelRow(snap vt.Snapshot, bottom int) int {
	for r := len(snap.Lines) - 1; r >= 0; r-- {
		if bottom >= 0 && r > bottom {
			continue
		}
		rest, hit := scanRow(snap.Lines[r])
		if hit != hitComposer {
			continue
		}
		if strings.HasPrefix(rest, modelPrefix) {
			return r
		}
		return -1
	}
	return -1
}

// composerHit is one composer row found by the multi-row scan.
type composerHit struct {
	row  int
	rest string
}

// dropModel removes the trailing model line: the lowest hit, but only
// when it carries the model prefix and more hits sit above it. Anything
// else stays, so user text is never mistaken for chrome.
func dropModel(hits []composerHit) []string {
	out := []string{}
	for i := len(hits) - 1; i >= 0; i-- {
		out = append(out, hits[i].rest)
	}
	if len(out) > 1 && strings.HasPrefix(out[len(out)-1], modelPrefix) {
		out = out[:len(out)-1]
	}
	return out
}

// multiRowBox classifies a bordered multi-row box: busy when any row
// past the model line holds non-placeholder text, free when all are
// empty or known placeholders, uncertain when no composer row exists.
func multiRowBox(snap vt.Snapshot, placeholders []string) (busy, certain bool, detail string) {
	known := map[string]bool{}
	for _, p := range placeholders {
		known[p] = true
	}
	rests, found := collectRows(snap)
	if !found {
		return false, false, "no-composer-line"
	}
	for _, rest := range rests {
		if rest == "" || known[rest] {
			continue
		}
		return true, true, detailTyped
	}
	return false, true, detailEmpty
}

// styledText decides unknown composer text by -e style evidence: all
// faint means a placeholder the known list missed (for example one
// truncated by pane width); anything else is typed text. Missing style
// evidence fails closed.
func styledText(snap vt.Snapshot, row int, line, rest string) (busy, certain bool, detail string) {
	start := strings.LastIndex(line, rest)
	if start < 0 {
		return true, false, "unmapped"
	}
	cells, ok := snap.CellsForRow(row)
	if !ok {
		return true, false, detailStyleMissing
	}
	styled, ok := cellsForByteRange(cells, start, start+len(rest))
	if !ok || len(styled) == 0 {
		return true, false, detailStyleMissing
	}
	for _, c := range styled {
		if !c.Faint || c.OtherStyle {
			return true, true, detailTyped
		}
	}
	return false, true, "placeholder-unlisted"
}

// exprBox classifies a shell-style input box through one regex.
func exprBox(snap vt.Snapshot, expr string) (busy, certain bool, detail string) {
	pattern, err := vt.NewInputPattern(expr)
	if err != nil {
		return false, false, "input-pattern"
	}
	text, found := vt.InputLine(snap, pattern)
	if !found {
		return false, false, "no-input-line"
	}
	if text == "" {
		return false, true, detailEmpty
	}
	row := snap.CursorRow
	if row < 0 || row >= len(snap.Lines) {
		return true, false, "offscreen"
	}
	line := snap.Lines[row]
	start := strings.LastIndex(line, text)
	if start < 0 {
		return true, false, "unmapped"
	}
	cells, _, ok := snap.CursorCellsForBytes(start, start+len(text))
	if !ok || len(cells) == 0 {
		return true, false, detailStyleMissing
	}
	for _, c := range cells {
		if !c.Faint || c.OtherStyle {
			return true, true, detailTyped
		}
	}
	return false, true, "placeholder"
}

// cellsForByteRange returns the cells covering [start, end) in a row's
// text, failing closed when the range splits a grapheme cell.
func cellsForByteRange(cells []vt.CursorCell, start, end int) ([]vt.CursorCell, bool) {
	var out []vt.CursorCell
	offset := 0
	for _, c := range cells {
		next := offset + len(c.Text)
		switch {
		case next <= start || offset >= end:
			// Outside the range.
		case offset < start || next > end:
			return nil, false
		default:
			out = append(out, c)
		}
		offset = next
	}
	if len(out) == 0 {
		return nil, false
	}
	covered := start
	for _, c := range out {
		covered += len(c.Text)
	}
	if covered != end {
		return nil, false
	}
	return out, true
}

// isCLI reports whether the tree under panePID runs one of the binaries.
func isCLI(binaries []string, panePID int, procs []Process) bool {
	if len(binaries) == 0 || panePID <= 0 {
		return false
	}
	want := map[string]bool{}
	for _, b := range binaries {
		want[b] = true
	}
	children := map[int][]Process{}
	for _, p := range procs {
		if p.PID == panePID && cliMatch(want, p) {
			return true
		}
		children[p.PPID] = append(children[p.PPID], p)
	}
	seen := map[int]bool{panePID: true}
	queue := []int{panePID}
	for len(queue) > 0 {
		pid := queue[0]
		queue = queue[1:]
		for _, p := range children[pid] {
			if cliMatch(want, p) {
				return true
			}
			if !seen[p.PID] {
				seen[p.PID] = true
				queue = append(queue, p.PID)
			}
		}
	}
	return false
}

// cliMatch reports whether one process is a foreground CLI: the binary
// matches, its group owns the pane's tty, and it is neither stopped (T)
// nor tracing-stopped (t). An unknown TPGID (0) never matches, since real
// process groups are positive.
func cliMatch(want map[string]bool, p Process) bool {
	return len(p.Argv) > 0 && want[path.Base(p.Argv[0])] &&
		p.Pgrp == p.TPGID && p.State != 'T' && p.State != 't'
}
