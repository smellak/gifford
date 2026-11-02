package detect

import (
	"fmt"
	"path"
	"strings"

	"github.com/smellak/gifford/internal/vt"
)

// Harness names the agent harness whose screen markers apply.
type Harness string

// Known harnesses. Agent profiles stay empty until the bench markers land;
// see DefaultTable.
const (
	HarnessShell    Harness = "shell"
	HarnessCodex    Harness = "codex"
	HarnessClaude   Harness = "claude"
	HarnessMuse     Harness = "muse"
	HarnessOpenCode Harness = "opencode"
)

// Profile holds one harness's screen markers, ported from the CapyCom
// notifier. BusyMarks are substrings whose presence means the agent is
// working. CLIBinaries are executable basenames proving the pane runs the
// harness CLI. InputExpr extracts the input-box text with exactly one
// capture group, as vt.NewInputPattern requires.
type Profile struct {
	BusyMarks   []string
	CLIBinaries []string
	InputExpr   string
}

// Table maps every known harness to its profile.
type Table map[Harness]Profile

// DefaultTable ships the shell profile only. Agent profiles are present
// but empty on purpose: without bench-verified markers the detector must
// stay uncertain, never guess.
func DefaultTable() Table {
	return Table{
		HarnessShell: {
			// The trailing space of a fresh prompt does not survive
			// the snapshot, so the separator space is optional.
			InputExpr:   `[$#] ?(.*)$`,
			CLIBinaries: []string{"sh", "bash", "zsh", "fish"},
		},
		HarnessCodex:    {},
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
// not support a verdict; the caller must not write then.
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
		return State{Detail: fmt.Sprintf("unknown harness %q", h)}
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
	for _, mark := range prof.BusyMarks {
		if mark != "" && strings.Contains(snap.Text(), mark) {
			busy = true
			break
		}
	}

	pattern, err := vt.NewInputPattern(prof.InputExpr)
	if err != nil {
		return State{
			Busy: busy, IsCLI: isCLI(prof.CLIBinaries, panePID, procs),
			Detail: fmt.Sprintf("input pattern: %v", err),
		}
	}
	text, found := vt.InputLine(snap, pattern)
	if !found {
		return State{
			Busy: busy, IsCLI: isCLI(prof.CLIBinaries, panePID, procs),
			Detail: "no input line",
		}
	}
	boxBusy, certain := boxState(snap, text)
	return State{
		Certain: certain,
		Busy:    busy,
		BoxBusy: boxBusy,
		IsCLI:   isCLI(prof.CLIBinaries, panePID, procs),
		Detail:  fmt.Sprintf("input %q", text),
	}
}

// boxState reports whether extracted input text counts as box content.
// Empty text is an empty box. All-faint text is the placeholder, visible
// through -e styling: also an empty box. Anything else, or text whose
// style evidence is missing, fails closed.
func boxState(snap vt.Snapshot, text string) (busy, certain bool) {
	if text == "" {
		return false, true
	}
	row := snap.CursorRow
	if row < 0 || row >= len(snap.Lines) {
		return true, false
	}
	line := snap.Lines[row]
	start := strings.LastIndex(line, text)
	if start < 0 {
		return true, false
	}
	cells, _, ok := snap.CursorCellsForBytes(start, start+len(text))
	if !ok || len(cells) == 0 {
		return true, false
	}
	for _, c := range cells {
		if !c.Faint || c.OtherStyle {
			return true, true
		}
	}
	return false, true
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
