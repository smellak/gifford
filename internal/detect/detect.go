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

const (
	detailTyped               = "typed"
	detailTypedUnverified     = "typed-unverified"
	detailPlaceholderUnlisted = "placeholder-unlisted"
)

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
// group, as vt.NewInputPattern requires. The extra box fields read
// harness-specific composer chrome. BusyChrome records measured,
// text-independent widget rows that do not consume the shared busy-marker
// tail. GrayHint records measured, text-independent evidence for harnesses
// that paint rotating suggestions.
type Profile struct {
	BusyMarks []string
	// FooterBusyMarks mark the agent busy only from a footer row, below
	// the footer anchor. The same words higher up are transcript content
	// and never mark it busy.
	FooterBusyMarks []string
	BusyChrome      BusyChromeStyle
	CLIBinaries     []string
	Composer        bool
	Placeholders    []string
	InputExpr       string
	ComposerLead    string
	Chrome          []string
	// Footer anchors the scans: every row below the anchor is footer
	// chrome, never the composer and never a reason for doubt. Position
	// decides, not text, so a new footer row needs no new marker.
	Footer FooterAnchor
	// ChromeFooterOnly restricts chrome rows to the measured footer
	// zone: the box footer row in multi-row boxes, the rows below the
	// box bottom rule in rule-framed boxes. Anywhere else the same
	// text is content, never skipped chrome.
	ChromeFooterOnly bool
	MultiRowBox      bool
	GrayHint         GrayHintStyle
	// SkipRules lets the notice walk step over box-drawing rules the
	// way the classify scan does. Only profiles whose box is framed
	// by rules set it; the others keep their exact historic walk.
	SkipRules bool
}

// FooterAnchor names the row a profile's footer starts below.
type FooterAnchor int

const (
	// FooterNone keeps the historic walk.
	FooterNone FooterAnchor = iota
	// FooterBelowStatus anchors on the status row right under the box's
	// bottom rule.
	FooterBelowStatus
	// FooterBelowBox anchors on the box's bottom rule right under a box row.
	FooterBelowBox
)

// GrayHintStyle describes one harness's rotating placeholder style. Sequence
// is the exact measured SGR prefix. IndexedCells selects the VT-cell path when
// that colour survives emulation; truecolor hints stay on raw capture bytes.
type GrayHintStyle struct {
	Sequence          string
	IndexedForeground uint8
	IndexedCells      bool
	// MutedHint is a footer key-hint word the harness paints in its theme's
	// muted color, which is also its placeholders' color. Read on the same
	// screen, it proves a hint under a theme that Sequence does not cover.
	MutedHint string
}

// BusyChromeStyle identifies one harness's live widget rows without matching
// their changing text. Rows must be a contiguous block immediately above the
// current composer frame. Indent cells and the following glyph must have the
// exact indexed foregrounds measured on the bench; incomplete or decorated
// evidence is not skipped.
type BusyChromeStyle struct {
	IndentCells      int
	IndentForeground uint8
	GlyphForegrounds map[string]uint8
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
	"auto mode on",
	"for agents",
	interruptMark,
	"· ~",
}

// labelledComposer finds the Codex labelled form
// "SESSION-NAME   › rest", where two spaces separate the label.
var labelledComposer = regexp.MustCompile(`\s{2,}›\s*(.*)$`)

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
			BusyMarks: []string{interruptMark},
			// A 2.1.280 pane out of usage prints "Usage limit reached ·
			// continuing automatically at 9pm · esc to cancel" under the
			// box until the limit resets. Typing there cancels that
			// continuation, so the pane is busy while the row stands.
			FooterBusyMarks: []string{"continuing automatically"},
			CLIBinaries:     []string{"claude"},
			Composer:        true,
			Placeholders:    placeholderSet,
			// Live 2.1.280 screens print warnings, token hints and the
			// open artifacts below the status row, alone or together,
			// with blank rows between them.
			Footer: FooterBelowStatus,
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
			// rules. Rotating prompt hints use indexed gray 242 with the
			// exact measured style; their text is deliberately not asserted.
			// Working screens expose this stable status suffix.
			BusyMarks: []string{interruptMark},
			// Narrow working screens can place a variable-length task widget
			// between the live marker and composer. Its entries have two gray
			// margin cells followed by one of these measured colored glyphs.
			BusyChrome: BusyChromeStyle{
				IndentCells:      2,
				IndentForeground: 242,
				GlyphForegrounds: map[string]uint8{
					"✓": 151,
					"◆": 109,
					"◇": 246,
				},
			},
			// The measured 1.4.13-R6996.1 build runs as muse-bin-<build>.
			CLIBinaries: []string{"muse", "muse-bin-1.4.13-R6996.1"},
			Composer:    true,
			// Measured 1.4.13 footer modes.
			Chrome: []string{
				"· YOLO",
				"· Auto-review",
				// Optional background-task panel measured between the
				// lower composer rule and footer.
				"· ↓ to select",
				" running",
				" watching ·",
			},
			GrayHint: GrayHintStyle{
				Sequence:          "\x1b[38;5;242m",
				IndexedForeground: 242,
				IndexedCells:      true,
			},
			ChromeFooterOnly: true,
			SkipRules:        true,
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
			ChromeFooterOnly: true,
			MultiRowBox:      true,
			GrayHint: GrayHintStyle{
				Sequence: "\x1b[38;2;128;128;128m",
				// The bench theme paints "agents" in that gray; a live
				// 256-color theme painted it and the placeholder alike.
				MutedHint: "agents",
			},
			SkipRules: true,
			// The live 1.18.29 start screen centers the box and prints
			// the key hints, a rotating tip and the status bar below it.
			Footer: FooterBelowBox,
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
	// An open emulator keeps its reply reader and its screen alive; the
	// node classifies every pane on every pass.
	defer func() { _ = emu.Close() }()
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
	tail := tailNonBlankScreen(snap, prof, 8)
	for _, mark := range prof.BusyMarks {
		if mark != "" && strings.Contains(tail, mark) {
			busy = true
			break
		}
	}
	if !busy {
		busy = footerBusy(snap, prof)
	}

	var boxBusy, certain bool
	var detail string
	if prof.Composer {
		// Truecolor hint evidence is absent from VT cells, so only profiles
		// that need raw evidence pay for the split.
		var rawLines []string
		if prof.GrayHint.Sequence != "" && !prof.GrayHint.IndexedCells {
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

// tailNonBlankScreen joins the last n meaningful rows. A profile may remove a
// measured, live widget block immediately above the current composer frame;
// those variable-length rows are status chrome rather than conversation. The
// shared n remains fixed, so an old marker followed by ordinary output still
// ages out.
func tailNonBlankScreen(snap vt.Snapshot, prof Profile, n int) string {
	skipped := liveBusyChromeRows(snap, prof)
	if len(skipped) == 0 {
		return tailNonBlank(snap.Text(), n)
	}
	kept := make([]string, 0, len(snap.Lines))
	for row, line := range snap.Lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if skipped[row] {
			continue
		}
		kept = append(kept, line)
	}
	if len(kept) > n {
		kept = kept[len(kept)-n:]
	}
	return strings.Join(kept, "\n")
}

// liveBusyChromeRows finds only the widget block attached to the bottom-most
// framed composer. Blank separation before the block is allowed, but once a
// measured widget row is seen the block ends at the next blank or mismatch.
// This prevents similarly shaped historical output elsewhere in the screen
// from extending the busy-marker window.
func liveBusyChromeRows(snap vt.Snapshot, prof Profile) map[int]bool {
	style := prof.BusyChrome
	if style.IndentCells <= 0 || len(style.GlyphForegrounds) == 0 {
		return nil
	}
	for composer := len(snap.Lines) - 1; composer > 0; composer-- {
		line := strings.TrimSpace(snap.Lines[composer])
		if _, ok := composerRest(line, prof.ComposerLead); !ok {
			continue
		}
		rule := composer - 1
		if !isRuleRow(strings.TrimSpace(snap.Lines[rule])) {
			continue
		}

		rows := make(map[int]bool)
		started := false
		for row := rule - 1; row >= 0; row-- {
			if strings.TrimSpace(snap.Lines[row]) == "" {
				if started {
					break
				}
				continue
			}
			if !isBusyChromeRow(snap, row, style) {
				break
			}
			started = true
			rows[row] = true
		}
		return rows
	}
	return nil
}

func isBusyChromeRow(snap vt.Snapshot, row int, style BusyChromeStyle) bool {
	cells, ok := snap.CellsForRow(row)
	if !ok || len(cells) <= style.IndentCells+1 {
		return false
	}
	for column := range style.IndentCells {
		cell := cells[column]
		if cell.Col != column || cell.Width != 1 || cell.Text != " " ||
			!cell.ExactIndexedForeground(style.IndentForeground) {
			return false
		}
	}
	glyph := cells[style.IndentCells]
	foreground, ok := style.GlyphForegrounds[glyph.Text]
	if !ok || glyph.Col != style.IndentCells || glyph.Width != 1 ||
		!glyph.ExactIndexedForeground(foreground) {
		return false
	}
	separator := cells[style.IndentCells+1]
	return separator.Col == style.IndentCells+1 && separator.Width == 1 && separator.Text == " "
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
// first, then bottom-up, skipping blanks, status chrome, the footer below
// the profile's anchor and bare rules;
// the first content row decides and the scan stops there, so a history
// line above the box can never stand in for it. The verdict reads the
// style evidence on the composer row itself, never on the cursor row,
// because a capture parks the cursor below the box. A known placeholder
// counts as empty only when every cell in its span is faint; the same
// text written bright is typed. A narrow pane wraps the box text onto
// the rows below; the wrapped rows still belong to the box above them.
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
	anchor := footerAnchor(snap, prof)
	var skippedChrome []int
	for _, row := range rows {
		if anchor >= 0 && row > anchor {
			continue
		}
		s := strings.TrimSpace(snap.Lines[row])
		if s == "" || isStatusLine(s) || isRuleRow(s) {
			continue
		}
		if rest, ok := composerRest(s, prof.ComposerLead); ok {
			if !chromeSkippedValid(prof, snap, skippedChrome, row) {
				return false, false, "chrome-above-box"
			}
			if prof.MultiRowBox {
				return verifyMultiRow(snap, prof, known, rawLines, row)
			}
			return verifyBox(snap, prof, known, row, rest)
		}
		if chromeSkipRow(prof, snap, &skippedChrome, row, s) {
			continue
		}
		// Content that is not a composer: it still belongs to the box
		// above when that row opens a composer; otherwise the frame
		// holds no box at all.
		if busy, certain, detail, ok := wrappedBoxBelow(snap, prof, known, rawLines, skippedChrome, row); ok {
			return busy, certain, detail
		}
		return false, false, "no-composer-line"
	}
	// No composer line at all: uncertain, never a free box. A live agent
	// on its alternate screen shows no composer while working.
	return false, false, "no-composer-line"
}

// footerAnchor returns the bottom-most row the profile's footer starts
// below, or -1. A status anchor is the bottom rule of the box above a
// measured status fragment or a profile chrome mark, as a bare "manual mode
// on" row is; whatever lies between that rule and the status row, such as
// warning rows, is footer too. A box anchor is a rule right under a box row.
// A dialog drawn over the box breaks both shapes, so it never yields an
// anchor.
func footerAnchor(snap vt.Snapshot, prof Profile) int {
	for row := len(snap.Lines) - 1; row > 0; row-- {
		s := strings.TrimSpace(snap.Lines[row])
		above := strings.TrimSpace(snap.Lines[row-1])
		switch prof.Footer {
		case FooterNone:
			return -1
		case FooterBelowStatus:
			if !isStatusLine(s) && !isChromeRow(prof, s) {
				continue
			}
			if rule := boxRuleAbove(snap, prof, row); rule >= 0 {
				return rule
			}
		case FooterBelowBox:
			if _, box := composerRest(above, prof.ComposerLead); box && isRuleRow(s) {
				return row
			}
		}
	}
	return -1
}

// footerBusy reports whether a footer row, below the footer anchor, holds
// one of the profile's footer busy marks. Without an anchor there is no
// footer, and the box is not certain either.
func footerBusy(snap vt.Snapshot, prof Profile) bool {
	if len(prof.FooterBusyMarks) == 0 {
		return false
	}
	anchor := footerAnchor(snap, prof)
	if anchor < 0 {
		return false
	}
	for _, line := range snap.Lines[anchor+1:] {
		for _, mark := range prof.FooterBusyMarks {
			if mark != "" && strings.Contains(line, mark) {
				return true
			}
		}
	}
	return false
}

// boxRuleAbove returns the bottom rule of the box drawn above a status row,
// or -1. The rows between them may say anything but may not open a
// composer, and the rule must close a box: the next rule above it is the
// top rule, and the row right under that opens a composer.
func boxRuleAbove(snap vt.Snapshot, prof Profile, status int) int {
	bottom := -1
	for row := status - 1; row >= 0 && bottom < 0; row-- {
		s := strings.TrimSpace(snap.Lines[row])
		if _, box := composerRest(s, prof.ComposerLead); box {
			return -1
		}
		if isRuleRow(s) {
			bottom = row
		}
	}
	for top := bottom - 1; top >= 0; top-- {
		if !isRuleRow(strings.TrimSpace(snap.Lines[top])) {
			continue
		}
		if top+1 == bottom {
			return -1
		}
		if _, box := composerRest(strings.TrimSpace(snap.Lines[top+1]), prof.ComposerLead); box {
			return bottom
		}
		return -1
	}
	return -1
}

// isChromeRow reports profile chrome rows skipped by the scans: box and
// status chrome that is never decisive content. The list is empty on
// the single-row harnesses, so their scans behave exactly as before.
// Whether a skipped row may stand is decided by chromeSkippedValid: the
// match alone never places it.
func isChromeRow(prof Profile, s string) bool {
	for _, c := range prof.Chrome {
		if c != "" && strings.Contains(s, c) {
			return true
		}
	}
	return false
}

// wrappedAbove reports the composer row that row continues as a
// narrow-pane wrap, with the text on that composer row: the wrapped rows
// still belong to that box, so they are content and never chrome,
// wherever the mark list says. A single-row composer wraps its text onto
// as many rows as it needs, so the walk climbs every content row up to
// the composer, and a wrap always leaves text on the composer row. A
// multi-row box draws its lead on every row, so only the row right under
// one can continue it.
func wrappedAbove(snap vt.Snapshot, prof Profile, row int) (top int, rest string, ok bool) {
	for top = row - 1; top >= 0; top-- {
		s := strings.TrimSpace(snap.Lines[top])
		if rest, box := composerRest(s, prof.ComposerLead); box {
			return top, rest, rest != ""
		}
		if prof.MultiRowBox || !continuesBox(s) {
			break
		}
	}
	return 0, "", false
}

// continuesBox reports whether a stripped row can carry wrapped box
// text: any content but the blanks, status lines and bare rules that
// end a box.
func continuesBox(s string) bool {
	return s != "" && !isStatusLine(s) && !isRuleRow(s)
}

// boxRows reads a single-row composer's text from its row down through
// the rows a narrow pane wrapped it onto: every following content row up
// to a blank, status, rule or composer row.
func boxRows(snap vt.Snapshot, prof Profile, top int, rest string) []rowText {
	rows := []rowText{{row: top, text: rest}}
	for r := top + 1; r < len(snap.Lines); r++ {
		s := strings.TrimSpace(snap.Lines[r])
		if !continuesBox(s) {
			break
		}
		if _, box := composerRest(s, prof.ComposerLead); box {
			break
		}
		rows = append(rows, rowText{row: r, text: s})
	}
	return rows
}

// wrappedBoxBelow verifies a content row that continues the box above it
// as a narrow-pane wrap. Footer-zone chrome skipped on the way down still
// has to stand: like the direct hit, the wrapped box reads uncertain
// when a skipped mark falls outside the footer zone.
func wrappedBoxBelow(snap vt.Snapshot, prof Profile, known map[string]bool, rawLines []string, skipped []int, row int) (busy, certain bool, detail string, ok bool) {
	top, rest, good := wrappedAbove(snap, prof, row)
	if !good {
		return false, false, "", false
	}
	if !chromeSkippedValid(prof, snap, skipped, top) {
		return false, false, "chrome-above-box", true
	}
	if prof.MultiRowBox {
		busy, certain, detail := verifyMultiRow(snap, prof, known, rawLines, top)
		return busy, certain, detail, true
	}
	busy, certain, detail = verifyBox(snap, prof, known, top, rest)
	return busy, certain, detail, true
}

// chromeSkipRow reports whether a non-composer, non-status, non-rule row
// is skippable footer chrome, tracking it for the footer-zone validation
// at the composer hit. A wrapped composer row in a rule-framed footer
// box is content instead, even when it carries a chrome mark; status and
// rule handling stays with each caller exactly as before.
func chromeSkipRow(prof Profile, snap vt.Snapshot, skipped *[]int, row int, s string) bool {
	if prof.ChromeFooterOnly && !prof.MultiRowBox {
		if _, _, ok := wrappedAbove(snap, prof, row); ok {
			return false
		}
	}
	if isChromeRow(prof, s) {
		*skipped = append(*skipped, row)
		return true
	}
	return false
}

// chromeSkippedValid reports whether every chrome-skipped row falls in
// the profile's measured footer zone, given the composer hit row. The
// zone is the run's own footer row and below for multi-row boxes, and
// strictly below the box bottom rule for rule-framed boxes: a chrome
// mark in the composer or above it is content, and the frame reads
// uncertain instead of skipping past it. Profiles without a footer zone
// keep skipping anywhere, exactly as before.
func chromeSkippedValid(prof Profile, snap vt.Snapshot, skipped []int, hit int) bool {
	if !prof.ChromeFooterOnly || len(skipped) == 0 {
		return true
	}
	if prof.MultiRowBox {
		_, bottom := multiRowRun(snap, prof, hit)
		for _, r := range skipped {
			if r < bottom {
				return false
			}
		}
		return true
	}
	for r := hit + 1; r < len(snap.Lines); r++ {
		if isRuleRow(strings.TrimSpace(snap.Lines[r])) {
			for _, s := range skipped {
				if s <= r {
					return false
				}
			}
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
	gray, complete := allPairsGray(snap, rawLines, pairs, prof.GrayHint.Sequence)
	if complete && !gray {
		if muted, ok := footerMutedSequence(snap, prof, rawLines); ok {
			gray, complete = allPairsGray(snap, rawLines, pairs, muted)
		}
	}
	switch {
	case prof.GrayHint.Sequence == "" || !complete:
		if !complete {
			return true, false, detailTypedUnverified
		}
		return true, true, detailTyped
	case gray && known[content]:
		return false, true, detailEmpty
	case gray:
		return false, true, detailPlaceholderUnlisted
	default:
		return true, true, detailTyped
	}
}

// multiRowRun returns the maximal composer-form run containing hit: the
// box rows and its footer row, which always carries the composer lead.
func multiRowRun(snap vt.Snapshot, prof Profile, hit int) (top, bottom int) {
	top = hit
	for top-1 >= 0 {
		if _, ok := composerRest(strings.TrimSpace(snap.Lines[top-1]), prof.ComposerLead); !ok {
			break
		}
		top--
	}
	bottom = hit
	for bottom+1 < len(snap.Lines) {
		if _, ok := composerRest(strings.TrimSpace(snap.Lines[bottom+1]), prof.ComposerLead); !ok {
			break
		}
		bottom++
	}
	return top, bottom
}

// multiRowBoxContent reads the maximal composer-form run containing hit.
// Empty padding and the footer row's measured box chrome do not
// contribute content; every other row does, in screen order. Chrome
// exclusion holds only on the footer's own row, the bottom of the run:
// the same text typed in the composer is content. Both classification
// and the pre-Enter verifier use this projection so they agree on the
// exact box boundary.
func multiRowBoxContent(snap vt.Snapshot, prof Profile, hit int) ([]rowText, string) {
	top, bottom := multiRowRun(snap, prof, hit)
	var pairs []rowText
	var parts []string
	for r := top; r <= bottom; r++ {
		rest, ok := composerRest(strings.TrimSpace(snap.Lines[r]), prof.ComposerLead)
		if !ok || rest == "" {
			continue
		}
		if r == bottom && isChromeRest(prof, rest) {
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
func allPairsGray(snap vt.Snapshot, rawLines []string, pairs []rowText, sequence string) (gray, complete bool) {
	if sequence == "" {
		return false, false
	}
	for _, p := range pairs {
		if p.text == "" {
			return false, false
		}
		line, ok := rawRow(rawLines, snap, p.row)
		if !ok {
			return false, false
		}
		if !strings.Contains(line, sequence+p.text) {
			return false, true
		}
	}
	if len(pairs) == 0 {
		return false, false
	}
	return true, true
}

// footerMutedSequence reads the theme's muted color on a footer row: the
// foreground sequence right before the profile's muted hint word. Only a
// bare foreground sequence counts; any other shape, or a screen without
// a footer anchor, fails closed.
func footerMutedSequence(snap vt.Snapshot, prof Profile, rawLines []string) (string, bool) {
	hint := prof.GrayHint.MutedHint
	anchor := footerAnchor(snap, prof)
	if hint == "" || anchor < 0 {
		return "", false
	}
	for row := anchor + 1; row < len(snap.Lines); row++ {
		if !strings.Contains(snap.Lines[row], hint) {
			continue
		}
		line, ok := rawRow(rawLines, snap, row)
		if !ok {
			return "", false
		}
		at := strings.Index(line, "m"+hint)
		if at < 0 {
			continue
		}
		start := strings.LastIndex(line[:at], "\x1b[")
		if start < 0 {
			continue
		}
		if sequence := line[start : at+1]; foregroundSequence.MatchString(sequence) {
			return sequence, true
		}
	}
	return "", false
}

// foregroundSequence matches one bare indexed or truecolor foreground.
var foregroundSequence = regexp.MustCompile(`^\x1b\[38;(5;\d{1,3}|2;\d{1,3};\d{1,3};\d{1,3})m$`)

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

// verifyBox judges one box opening at row top with rest, continuing onto
// every row the pane wrapped it onto. An empty composer is free. A known
// placeholder counts as empty only when every content cell in its span
// is faint; the same text written bright is typed. Unknown text with
// complete faint evidence is an unlisted placeholder; anything else
// occupied stays uncertain instead of guessing.
func verifyBox(
	snap vt.Snapshot,
	prof Profile,
	known map[string]bool,
	top int,
	rest string,
) (busy, certain bool, detail string) {
	if rest == "" {
		return false, true, detailEmpty
	}
	box := boxRows(snap, prof, top, rest)
	rows := make([]int, len(box))
	parts := make([]string, len(box))
	for i, r := range box {
		rows[i], parts[i] = r.row, r.text
	}
	text := strings.Join(parts, " ")
	if prof.GrayHint.IndexedCells {
		return verifyIndexedGrayHint(snap, prof.GrayHint, rows, text)
	}
	allFaint, complete := spanStyleContentCells(snap, box)
	switch {
	case !complete:
		return true, false, detailTypedUnverified
	case allFaint && known[text]:
		return false, true, detailEmpty
	case allFaint:
		return false, true, detailPlaceholderUnlisted
	default:
		return true, true, detailTyped
	}
}

// verifyIndexedGrayHint accepts a single-row rotating hint only when every
// content cell has exactly the profile's indexed foreground. Text is never
// matched: bright, mixed, decorated or multi-row content stays occupied,
// while missing cell evidence stays uncertain.
func verifyIndexedGrayHint(
	snap vt.Snapshot,
	style GrayHintStyle,
	rows []int,
	text string,
) (busy, certain bool, detail string) {
	if len(rows) != 1 {
		return true, true, detailTyped
	}
	cells, ok := rowSpanCells(snap, rows[0], text)
	if !ok || len(cells) == 0 {
		return true, false, detailTypedUnverified
	}
	for _, cell := range cells {
		if !cell.ExactIndexedForeground(style.IndexedForeground) {
			return true, true, detailTyped
		}
	}
	return false, true, detailPlaceholderUnlisted
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
func spanStyleContentCells(snap vt.Snapshot, rows []rowText) (allFaint, complete bool) {
	var span []vt.CursorCell
	for _, r := range rows {
		cells, ok := rowSpanCells(snap, r.row, r.text)
		if !ok {
			return false, false
		}
		span = append(span, cells...)
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
