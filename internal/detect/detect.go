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

// statusFragments are TUI status lines, never composer content,
// measured on real screens.
var statusFragments = []string{
	"bypass permissions",
	"for agents",
	"esc to interrupt",
	"· ~",
}

// labelledComposer finds the Codex labelled form
// "SESSION-NAME   › rest", where two spaces separate the label.
var labelledComposer = regexp.MustCompile(`\s{2,}›\s*(.*)$`)

// DefaultTable ships the shell profile and the Codex profile, with the
// working indicator, composer forms and placeholder set measured on real
// screens. Claude, Muse and OpenCode stay empty until bench-verified
// markers land.
func DefaultTable() Table {
	return Table{
		HarnessShell: {
			// The trailing space of a fresh prompt does not survive
			// the snapshot, so the separator space is optional.
			InputExpr:   `[$#] ?(.*)$`,
			CLIBinaries: []string{"sh", "bash", "zsh", "fish"},
		},
		HarnessCodex: {
			BusyMarks:    []string{"esc to interrupt"},
			CLIBinaries:  []string{"codex"},
			Composer:     true,
			Placeholders: placeholderSet,
		},
		HarnessClaude:   {},
		HarnessMuse:     {},
		HarnessOpenCode: {},
	}
}

// Process is one process in the pane's tree.
type Process struct {
	PID       int
	PPID      int
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
	if prof.Composer {
		boxBusy, certain, detail = composerBox(snap, prof.Placeholders)
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
// labelled "NAME   › rest" form. Anything else is transcript, not the box.
func composerRest(s string) (string, bool) {
	if strings.HasPrefix(s, "❯") || strings.HasPrefix(s, ">") || strings.HasPrefix(s, "›") {
		return strings.TrimSpace(strings.TrimLeft(s, "❯>› ")), true
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
		line := snap.Lines[row]
		s := strings.TrimSpace(line)
		if s == "" {
			continue
		}
		rest, ok := composerRest(s)
		if !ok {
			if isStatusLine(s) {
				continue
			}
			break
		}
		if rest == "" || known[rest] {
			return false, true, "empty"
		}
		return styledText(snap, row, line, rest)
	}
	// No composer line at all: uncertain, never a free box. A live agent
	// on its alternate screen shows no composer while working.
	return false, false, "no-composer-line"
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

// styledText decides unknown composer text by -e style evidence: all
// faint means a placeholder the known list missed (for example one
// truncated by pane width); anything else is typed text. Missing style
// evidence fails closed.
func styledText(snap vt.Snapshot, row int, line, rest string) (busy, certain bool, detail string) {
	start := strings.LastIndex(line, rest)
	if start < 0 {
		return true, false, "unmapped"
	}
	cells, _, ok := snap.CursorCellsForBytes(start, start+len(rest))
	if row != snap.CursorRow || !ok || len(cells) == 0 {
		return true, false, "style-missing"
	}
	for _, c := range cells {
		if !c.Faint || c.OtherStyle {
			return true, true, "typed"
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
		return false, true, "empty"
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
			return true, true, "typed"
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
		if p.PID == panePID && len(p.Argv) > 0 && want[path.Base(p.Argv[0])] {
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
			if len(p.Argv) > 0 && want[path.Base(p.Argv[0])] {
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
