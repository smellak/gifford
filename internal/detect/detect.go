package detect

import (
	"bytes"
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/charmbracelet/x/ansi"

	"github.com/smellak/gifford/internal/vt"
)

// detailEmpty names a free input box: empty or a verified placeholder.
// detailTyped names an occupied box holding user input. Both are
// constants so the write gate cannot drift between spellings.
const detailEmpty = "empty"

const detailTyped = "typed"

// interruptMark is the working indicator shared by the Codex, Claude
// and Muse TUIs, measured on real screens of each.
const interruptMark = "esc to interrupt"

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
// group, as vt.NewInputPattern requires. The four extra fields stay
// empty on the single-row harnesses (shell, Codex, Claude), whose path
// is then byte-identical to before. OpenCode sets them: ComposerLead
// opens its box, Chrome lists box and status chrome skipped by the
// scans, MultiRowBox reads the box as one run of composer rows, and
// GrayPlaceholder verifies hints by their fixed gray instead of faint.
type Profile struct {
	BusyMarks       []string
	CLIBinaries     []string
	Composer        bool
	Placeholders    []string
	InputExpr       string
	ComposerLead    string
	Chrome          []string
	MultiRowBox     bool
	GrayPlaceholder bool
	// SkipRules lets the notice walk step over box-drawing rules the
	// way the classify scan does. Only profiles whose box is framed
	// by rules set it; the others keep their exact historic walk.
	SkipRules bool
}

// Table maps every known harness to its profile.
type Table map[Harness]Profile

// placeholderSet holds texts TUIs paint inside an empty composer as a
// suggestion, measured on real screens. Reading one as typed input would
// wedge the agent busy forever, so they count as an empty box.
var placeholderSet = []string{
	// Measured Codex 0.154.0 composer suggestion, kept as a safe literal
	// by the bench sanitizer.
	"Ask Codex to do anything",
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

// statusFragments are TUI status lines, never composer content,
// measured on real screens.
var statusFragments = []string{
	"bypass permissions",
	"for agents",
	interruptMark,
	"· ~",
}

// labelledComposer finds the Codex labelled form
// "SESSION-NAME   › rest", where two spaces separate the label.
var labelledComposer = regexp.MustCompile(`\s{2,}›\s*(.*)$`)

// grayHintSeq is the fixed gray OpenCode 1.18.29 paints composer hints
// with, measured on real screens. The emulator drops truecolor, so the
// raw capture bytes carry this evidence instead of the snapshot cells.
const grayHintSeq = "\x1b[38;2;128;128;128m"

// DefaultTable ships only profiles whose markers were measured on real
// screens. The shell, Codex, Claude, Muse and OpenCode profiles classify;
// shell input runs through a regex while the TUIs read their composer,
// and every profile above stays uncertain rather than guessing where its
// markers do not reach.
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
		// no verifiable composer and reads uncertain, so nothing is
		// written; the pending human approval already holds the
		// delivery above the detector.
		HarnessClaude: {
			BusyMarks:    []string{interruptMark},
			CLIBinaries:  []string{"claude"},
			Composer:     true,
			Placeholders: placeholderSet,
			// The bench terminal prints a focus-events hint row under
			// the status line on working and text frames, and the
			// pending box shows a bare "manual mode on".
			Chrome: []string{
				"manual mode on",
				"focus-events",
			},
			SkipRules: true,
		},
		HarnessMuse: {
			// The measured 1.4.13 screens use a bare ❯ composer between
			// rules. Prompt hints are disabled, so no placeholder is
			// asserted. Working screens expose this stable status suffix.
			BusyMarks: []string{interruptMark},
			// The measured 1.4.13-R6996.1 build runs as muse-bin-<build>.
			CLIBinaries: []string{"muse", "muse-bin-1.4.13-R6996.1"},
			Composer:    true,
			// Measured 1.4.13 footer modes.
			Chrome: []string{
				"· YOLO",
				"· Auto-review",
			},
			SkipRules: true,
		},
		HarnessOpenCode: {
			BusyMarks: []string{"esc interrupt"},
			CLIBinaries: []string{
				"opencode",
			},
			Composer:     true,
			Placeholders: []string{`Ask anything… "Fix a TODO in the codebase"`},
			ComposerLead: "┃",
			Chrome: []string{
				"Build ·",
				// Measured live 1.18.29 model and footer forms.
				"Build auto ·",
				"ctrl+p commands",
				"tab agents",
				"⊙",
				"esc interrupt",
			},
			MultiRowBox:     true,
			GrayPlaceholder: true,
			SkipRules:       true,
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
	// A tmux pane dump is not a terminal flow: bare LFs leave each row
	// starting where the previous one ended. Normalize first so the
	// emulator sees row boundaries as the pane shows them.
	flow := NormalizeCapture(capture)
	if _, err := emu.Write(flow); err != nil {
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
	if prof.Composer {
		// The gray hint check reads style from the raw bytes, so only
		// profiles that need it pay for the split.
		var rawLines []string
		if prof.GrayPlaceholder {
			rawLines = strings.Split(string(flow), "\r\n")
		}
		boxBusy, certain, detail = composerBox(snap, prof, rawLines)
	} else {
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

// NormalizeCapture turns a tmux pane dump into a terminal flow: every
// LF not already part of a CRLF pair starts a new row at column zero.
// The live PTY path already carries cursor motion and must not pass
// through here; capture-only consumers share this helper instead of a
// second implementation that could diverge.
func NormalizeCapture(capture []byte) []byte {
	capture = bytes.ReplaceAll(capture, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(capture, []byte("\n"), []byte("\r\n"))
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
// a leading ❯ (claude), > or ›
// (codex), or the profile's extra lead, opens the composer, as does the
// labelled "NAME   › rest" form. Anything else is transcript, not the
// box. An empty lead keeps the historic set exactly.
func composerRest(s, lead string) (string, bool) {
	if strings.HasPrefix(s, "❯") || strings.HasPrefix(s, ">") || strings.HasPrefix(s, "›") ||
		(lead != "" && strings.HasPrefix(s, lead)) {
		return strings.TrimSpace(strings.TrimLeft(s, "❯>› "+lead)), true
	}
	if m := labelledComposer.FindStringSubmatch(s); m != nil {
		return strings.TrimSpace(m[1]), true
	}
	return "", false
}

// composerBox classifies the TUI input box. The scan runs cursor-row
// first, then bottom-up, skipping blanks, status chrome and bare rules;
// the first content row decides and the scan stops there, so a history
// line above the box can never stand in for it. The verdict reads the
// style evidence on the composer row itself, never on the cursor row,
// because a capture parks the cursor below the box. A known placeholder
// counts as empty only when every cell in its span is faint; the same
// text written bright is typed. A narrow pane wraps the box text onto
// the next row; the wrapped row still belongs to the box above it.
func composerBox(snap vt.Snapshot, prof Profile, rawLines []string) (busy, certain bool, detail string) {
	known := map[string]bool{}
	for _, p := range prof.Placeholders {
		known[p] = true
	}
	rows := []int{}
	if snap.CursorRow >= 0 && snap.CursorRow < len(snap.Lines) {
		rows = append(rows, snap.CursorRow)
	}
	for row := len(snap.Lines) - 1; row >= 0; row-- {
		if row != snap.CursorRow {
			rows = append(rows, row)
		}
	}
	for _, row := range rows {
		s := strings.TrimSpace(snap.Lines[row])
		if s == "" || isStatusLine(s) || isRuleRow(s) {
			continue
		}
		if rest, ok := composerRest(s, prof.ComposerLead); ok {
			if prof.MultiRowBox {
				return verifyMultiRow(snap, prof, known, rawLines, row)
			}
			return verifyBox(snap, known, []int{row}, rest, prof.ComposerLead)
		}
		if isChromeRow(prof, s) {
			continue
		}
		// Content that is not a composer: it still belongs to the box
		// above when that row opens a composer; otherwise the frame
		// holds no box at all.
		if row-1 >= 0 {
			if above, ok := composerRest(strings.TrimSpace(snap.Lines[row-1]), prof.ComposerLead); ok && above != "" {
				if prof.MultiRowBox {
					return verifyMultiRow(snap, prof, known, rawLines, row-1)
				}
				return verifyBox(snap, known, []int{row - 1, row}, above, prof.ComposerLead)
			}
		}
		return false, false, "no-composer-line"
	}
	// No composer line at all: uncertain, never a free box. A live agent
	// on its alternate screen shows no composer while working.
	return false, false, "no-composer-line"
}

// isChromeRow reports profile chrome rows skipped by the scans: box and
// status chrome that is never decisive content. The list is empty on
// the single-row harnesses, so their scans behave exactly as before.
func isChromeRow(prof Profile, s string) bool {
	for _, c := range prof.Chrome {
		if c != "" && strings.Contains(s, c) {
			return true
		}
	}
	return false
}

// isChromeRest reports composer-form rows excluded from multi-row box
// content: the lead opens the row but the text is box chrome, like a
// model line, never user input.
func isChromeRest(prof Profile, rest string) bool {
	for _, c := range prof.Chrome {
		if c != "" && strings.HasPrefix(rest, c) {
			return true
		}
	}
	return false
}

// rowText pairs one box row with the content text read on it.
type rowText struct {
	row  int
	text string
}

// verifyMultiRow judges one box read as a maximal run of composer-form
// rows around the hit row: pads above and below plus one content row,
// with box chrome rows excluded from the content. An empty run is free.
// Content counts as a hint only when every row renders it in the
// profile's fixed gray; bright content is typed. Anything without
// complete gray evidence stays uncertain instead of guessing.
func verifyMultiRow(snap vt.Snapshot, prof Profile, known map[string]bool, rawLines []string, hit int) (busy, certain bool, detail string) {
	pairs, content := multiRowBoxContent(snap, prof, hit)
	if content == "" {
		return false, true, detailEmpty
	}
	gray, complete := allPairsGray(snap, rawLines, pairs)
	switch {
	case !prof.GrayPlaceholder || !complete:
		if !complete {
			return true, false, "typed-unverified"
		}
		return true, true, detailTyped
	case gray && known[content]:
		return false, true, detailEmpty
	case gray:
		return false, true, "placeholder-unlisted"
	default:
		return true, true, detailTyped
	}
}

// multiRowBoxContent reads the maximal composer-form run containing hit.
// Empty padding and measured box chrome do not contribute content; every
// other row does, in screen order. Both classification and the pre-Enter
// verifier use this projection so they agree on the exact box boundary.
func multiRowBoxContent(snap vt.Snapshot, prof Profile, hit int) ([]rowText, string) {
	top := hit
	for top-1 >= 0 {
		if _, ok := composerRest(strings.TrimSpace(snap.Lines[top-1]), prof.ComposerLead); !ok {
			break
		}
		top--
	}
	bottom := hit
	for bottom+1 < len(snap.Lines) {
		if _, ok := composerRest(strings.TrimSpace(snap.Lines[bottom+1]), prof.ComposerLead); !ok {
			break
		}
		bottom++
	}
	var pairs []rowText
	var parts []string
	for r := top; r <= bottom; r++ {
		rest, ok := composerRest(strings.TrimSpace(snap.Lines[r]), prof.ComposerLead)
		if !ok || rest == "" || isChromeRest(prof, rest) {
			continue
		}
		pairs = append(pairs, rowText{row: r, text: rest})
		parts = append(parts, rest)
	}
	return pairs, strings.Join(parts, " ")
}

// allPairsGray reports whether every content row renders its text as one
// fixed-gray run in the raw capture bytes. The snapshot cells drop
// truecolor, so the check runs on the normalized raw lines, aligned row
// by row through rawRow: a box row only ever holds its own box, so a
// row-local match is the box's own rendering. Anything else fails
// closed.
func allPairsGray(snap vt.Snapshot, rawLines []string, pairs []rowText) (gray, complete bool) {
	for _, p := range pairs {
		if p.text == "" {
			return false, false
		}
		line, ok := rawRow(rawLines, snap, p.row)
		if !ok {
			return false, false
		}
		if !strings.Contains(line, grayHintSeq+p.text) {
			return false, true
		}
	}
	if len(pairs) == 0 {
		return false, false
	}
	return true, true
}

// rawRow aligns a snapshot row with its raw capture line. A full-pane
// capture scrolls once on the trailing newline, so its snapshot rows sit
// one line above their raw lines; short captures do not scroll. The
// bottom anchor fixes the shift and the per-row plain-text match proves
// it; anything else fails closed.
func rawRow(rawLines []string, snap vt.Snapshot, row int) (string, bool) {
	if row < 0 || row >= len(snap.Lines) {
		return "", false
	}
	snapLast := -1
	for i := len(snap.Lines) - 1; i >= 0; i-- {
		if strings.TrimSpace(snap.Lines[i]) != "" {
			snapLast = i
			break
		}
	}
	rawLast := -1
	for i := len(rawLines) - 1; i >= 0; i-- {
		if strings.TrimSpace(ansi.Strip(rawLines[i])) != "" {
			rawLast = i
			break
		}
	}
	if snapLast < 0 || rawLast < 0 {
		return "", false
	}
	j := row + rawLast - snapLast
	if j < 0 || j >= len(rawLines) {
		return "", false
	}
	line := rawLines[j]
	if strings.TrimSpace(ansi.Strip(line)) != strings.TrimSpace(snap.Lines[row]) {
		return "", false
	}
	return line, true
}

// verifyBox judges one box opening at rows[0] with rest, continuing onto
// rows[1] when the pane wrapped it. An empty composer is free. A known
// placeholder counts as empty only when every content cell in its span
// is faint; the same text written bright is typed. Unknown text with
// complete faint evidence is an unlisted placeholder; anything else
// occupied stays uncertain instead of guessing.
func verifyBox(snap vt.Snapshot, known map[string]bool, rows []int, rest, lead string) (busy, certain bool, detail string) {
	text := rest
	if text != "" && len(rows) == 1 && rows[0]+1 < len(snap.Lines) {
		if next := strings.TrimSpace(snap.Lines[rows[0]+1]); next != "" && !isStatusLine(next) && !isRuleRow(next) {
			if _, ok := composerRest(next, lead); !ok {
				text = text + " " + next
				rows = append(rows, rows[0]+1)
			}
		}
	}
	if text == "" {
		return false, true, detailEmpty
	}
	allFaint, complete := spanStyleContentCells(snap, rows, rest, text)
	switch {
	case !complete:
		return true, false, "typed-unverified"
	case allFaint && known[text]:
		return false, true, detailEmpty
	case allFaint:
		return false, true, "placeholder-unlisted"
	default:
		return true, true, detailTyped
	}
}

// isRuleRow reports bare box-drawing rules, which are frame chrome and
// never composer content. Some harnesses draw one under the input box.
func isRuleRow(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r != '─' && r != '│' && r != '╹' && r != '▀' && r != ' ' {
			return false
		}
	}
	return true
}

// SpanStyle reports the style evidence for text on one snapshot row:
// whether the covering cells exist, and whether every one is faint with
// no other style. Anything incomplete or mixed fails closed towards
// occupied. It reads the row's own cells, never the cursor row, because
// captures park the cursor below the box. The live-screen path
// shares this strict rule; the composer path below relaxes it for
// whitespace.
func SpanStyle(snap vt.Snapshot, row int, text string) (allFaint, complete bool) {
	cells, ok := rowSpanCells(snap, row, text)
	if !ok {
		return false, false
	}
	for _, c := range cells {
		if !c.Faint || c.OtherStyle {
			return false, true
		}
	}
	return true, true
}

// spanStyleContentCells reads the style evidence of text across its rows
// like spanStyle, but only content cells count: whitespace carries no
// style, and dim placeholders reset between words, leaving plain gaps.
// A span with no content cell at all fails closed. The composer path
// uses this rule; the live-screen path keeps the strict SpanStyle.
func spanStyleContentCells(snap vt.Snapshot, rows []int, rest, text string) (allFaint, complete bool) {
	cells, ok := rowSpanCells(snap, rows[0], rest)
	if !ok {
		return false, false
	}
	span := cells
	if len(rows) > 1 {
		cont := strings.TrimPrefix(text, rest+" ")
		extra, ok := rowSpanCells(snap, rows[1], cont)
		if !ok {
			return false, false
		}
		span = append(span, extra...)
	}
	if len(span) == 0 {
		return false, false
	}
	seen := 0
	for _, c := range span {
		if strings.TrimSpace(c.Text) == "" {
			continue
		}
		seen++
		if !c.Faint || c.OtherStyle {
			return false, true
		}
	}
	if seen == 0 {
		return false, false
	}
	return true, true
}

// rowSpanCells selects the cells covering text in one row, failing closed
// when the span would split a grapheme cell or the row has no evidence.
func rowSpanCells(snap vt.Snapshot, row int, text string) ([]vt.CursorCell, bool) {
	line := snap.Lines[row]
	start := strings.LastIndex(line, text)
	if start < 0 || text == "" {
		return nil, false
	}
	cells, ok := snap.CellsForRow(row)
	if !ok {
		return nil, false
	}
	end := start + len(text)
	var out []vt.CursorCell
	pos := 0
	for _, c := range cells {
		w := len(c.Text)
		if pos+w <= start || pos >= end {
			pos += w
			continue
		}
		if pos < start || pos+w > end {
			return nil, false
		}
		out = append(out, c)
		pos += w
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
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
		return true, false, "style-missing"
	}
	for _, c := range cells {
		if !c.Faint || c.OtherStyle {
			return true, true, detailTyped
		}
	}
	return false, true, "placeholder"
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
