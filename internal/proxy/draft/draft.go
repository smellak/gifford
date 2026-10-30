// Package draft guards the human draft: a deterministic state machine over
// input events and screen snapshots. It never persists typed text — only
// states, counters and types — and it never closes a draft on a timeout.
// On a measured profile a draft closes only on screen evidence: the
// harness's own output after the human's last key, quiet since, showing
// an empty input line under the cursor. Anything unconfirmed fails
// closed toward UNKNOWN or a still-open draft.
//
// That trades liveness for safety. A harness that draws nothing after a
// submit, a clear or a cancel leaves the draft open, and injection
// blocked, until a later one of those keys that it does redraw for. A
// measured profile with no input line pattern cannot show what such a
// key did, so the guard goes UNKNOWN until an explicit Unlock.
//
// Nor is CLOSED kept on a key that may have left text on the input line,
// or brought some back. While no draft is open, any key that is not
// text, a paste or a submit, a clear or a cancel, and any sequence the
// parser does not recognise, open a draft at once: a lone ESC the parser
// joins to the next key, which a harness may read apart and so type the
// letter; an arrow; a Tab. So does part of a key the parser holds back,
// a lone ESC above all, as soon as it arrives: the harness has it, and
// may answer it before the key is whole. Nothing closes while any byte
// is held back. The draft closes only on the screen evidence above,
// drawn after the key, so the state the session records and the screen
// projection reads is never CLOSED while such a key may have left text
// there. A profile that cannot show the input line never closes it: the
// guard goes UNKNOWN.
package draft

import (
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/input"
	"github.com/smellak/gifford/internal/vt"
)

// State is a draft guard state.
type State string

// Guard states.
const (
	// StateClosed means no draft is open.
	StateClosed State = "closed"
	// StateOpen means the human is editing a draft.
	StateOpen State = "open"
	// StatePasting means a bracketed paste is landing inside a draft.
	StatePasting State = "pasting"
	// StateSubmitting means submit was pressed and waits for confirmation.
	StateSubmitting State = "submitting"
	// StateUnknown means the guard lost track; injection stays blocked
	// until an explicit Unlock.
	StateUnknown State = "unknown"
)

// OutcomeKind names a draft resolution.
type OutcomeKind string

// Draft outcomes.
const (
	// OutcomeSubmitted closes a confirmed submit.
	OutcomeSubmitted OutcomeKind = "submitted"
	// OutcomeCancelled closes a human cancel.
	OutcomeCancelled OutcomeKind = "cancelled"
	// OutcomeCleared closes a confirmed emptying.
	OutcomeCleared OutcomeKind = "cleared"
)

// Outcome is one draft resolution, carrying the generation of the draft
// it closes so the two-phase commit can compare it.
type Outcome struct {
	Kind       OutcomeKind
	Generation uint64
}

// EventType maps the outcome onto the protocol event stream.
func (o Outcome) EventType() protocol.EventType {
	switch o.Kind {
	case OutcomeSubmitted:
		return protocol.EventTypeInputDraftSubmitted
	case OutcomeCancelled:
		return protocol.EventTypeInputDraftCancelled
	default:
		return protocol.EventTypeInputDraftCleared
	}
}

// Activity is informative only; it never replaces State.
type Activity string

// Activity levels.
const (
	// ActivityActive means a human key landed less than a second ago.
	ActivityActive Activity = "active"
	// ActivityPaused means a draft is open but quiet for over a second.
	ActivityPaused Activity = "paused"
	// ActivityIdle means no draft is open and no key is recent.
	ActivityIdle Activity = "idle"
)

const (
	// reactionQuiet is how long the harness's output must stay quiet,
	// once it reacted to a submit, a clear or a cancel, before the
	// screen may confirm the key: a redraw that comes in bursts settles
	// first. It is a stabilization, not a guarantee every TUI keeps: a
	// harness that draws an empty input line, and only later the draft
	// it restores, can still be read as empty in between.
	reactionQuiet = 300 * time.Millisecond
	// activityWindow bounds a recent key for ActivityActive.
	activityWindow = time.Second
)

// Guard tracks one human draft. The zero value is not usable; build one
// with NewGuard. A Guard is not safe for concurrent use.
type Guard struct {
	clock      clock.Clock
	profile    profiles.Profile
	state      State
	version    int64
	generation uint64
	count      int
	clean      bool
	lastKey    time.Time
	subCount   int
	cols       int
	rows       int
	hasScreen  bool
	// screen is the latest screen the guard took.
	screen vt.Snapshot
	// outputs counts the harness outputs the guard was given, and
	// lastOutput is when the latest came.
	outputs    uint64
	lastOutput time.Time
	// since is outputs when the guard last started to wait, at the last
	// human key or resize: only output counted after it is a reaction.
	since uint64
	// awaiting is the outcome a submit, a clear or a cancel waits for
	// the screen to confirm; "" waits for none.
	awaiting OutcomeKind
	// held says the parser holds part of a human key back: nothing
	// closes until the next event it gives.
	held bool
	// doubted says the draft opened on a key the guard cannot read,
	// with nothing typed in it since, see doubt.
	doubted bool
}

// NewGuard binds a guard to a harness profile and a clock. The screen
// only counts when the profile is measured with an input_line, read
// through CursorInput; anything else confirms nothing and fails closed.
// A nil clock reads the system clock.
func NewGuard(clk clock.Clock, p profiles.Profile) *Guard {
	if clk == nil {
		clk = clock.System{}
	}
	return &Guard{
		clock:   clk,
		profile: p,
		state:   StateClosed,
		clean:   true,
	}
}

// CursorInput reads the input line under the cursor: the one piece of
// screen evidence the guard, the deliveries and the socket's input
// check share. It needs a measured profile with an input line pattern,
// a whole frame, with nothing held back and no synchronized update
// open, a visible cursor in range, and the cursor row an input line;
// text is what the pattern captures there, exactly as drawn. No other
// row counts, whatever it shows. ok is false without all of that
// evidence. A frame inside a synchronized update stays no evidence
// however long it lasts: a redraw that never ends never settles.
//
// A profile with an input placeholder, which only a measured profile
// for one exact version declares, draws its empty input line as that
// placeholder, dimmed, with the cursor where it starts. The capture
// reads empty only drawn exactly so: the whole placeholder, every cell
// faint and in no other style, the cursor on its first column. A
// capture with no faint cell is the human's draft, even one that is the
// placeholder's very text. Anything else proves nothing: the prompt
// with nothing after it, mixed styles, other faint text, a cursor
// elsewhere, a cell the screen does not account for. A profile with no
// placeholder reads the capture as the draft whatever its style.
func CursorInput(p profiles.Profile, snap vt.Snapshot) (text string, ok bool) {
	if !p.Measured || p.InputLine == nil || p.InputLine.NumSubexp() != 1 ||
		snap.Pending || snap.SyncOpen || !snap.CursorVisible {
		return "", false
	}
	if snap.CursorRow < 0 || snap.CursorRow >= snap.Rows || snap.CursorRow >= len(snap.Lines) ||
		snap.CursorCol < 0 || snap.CursorCol >= snap.Cols {
		return "", false
	}
	line := snap.Lines[snap.CursorRow]
	if p.InputPlaceholder == "" {
		return p.Draft(line)
	}
	return placeholderInput(p, snap, line)
}

// placeholderInput reads line, the cursor row, on a profile with an
// input placeholder: see CursorInput.
func placeholderInput(p profiles.Profile, snap vt.Snapshot, line string) (string, bool) {
	m := p.InputLine.FindStringSubmatchIndex(line)
	if m == nil || m[2] < 0 || m[2] == m[3] {
		return "", false
	}
	cells, column, ok := snap.CursorCellsForBytes(m[2], m[3])
	if !ok {
		return "", false
	}
	faint, other := 0, false
	for _, c := range cells {
		if c.Faint {
			faint++
		}
		other = other || c.OtherStyle
	}
	text := line[m[2]:m[3]]
	switch {
	case faint == 0:
		return text, true
	case faint == len(cells) && !other && text == p.InputPlaceholder && column == snap.CursorCol:
		return "", true
	default:
		return "", false
	}
}

// State reports the current state.
func (g *Guard) State() State { return g.state }

// Generation counts draft openings; the two-phase commit compares it.
func (g *Guard) Generation() uint64 { return g.generation }

// GuardVersion counts real guard transitions for the two-phase commit; it
// starts at zero and never moves on no-ops. Every state change goes
// through transition so none is missed, and a key that waits on the
// screen counts even when the state stays: it ends any permit at once.
func (g *Guard) GuardVersion() int64 { return g.version }

// transition moves to s, counting the move. Same-state calls are no-ops.
func (g *Guard) transition(s State) {
	if g.state == s {
		return
	}
	g.state = s
	g.version++
}

// rearm moves to s and counts the move even when the state stays.
func (g *Guard) rearm(s State) {
	if g.state == s {
		g.version++
		return
	}
	g.transition(s)
}

// InjectionAllowed reports whether automatic injection may run: only with
// no draft open. UNKNOWN blocks it too. Human input is a separate path
// and always keeps reaching the harness, even in UNKNOWN; this signal
// must never drop keys.
func (g *Guard) InjectionAllowed() bool { return g.state == StateClosed }

// Activity reports recent human activity; it never replaces State.
func (g *Guard) Activity() Activity {
	if g.clock.Now().Sub(g.lastKey) < activityWindow {
		return ActivityActive
	}
	if g.state == StateOpen || g.state == StatePasting || g.state == StateSubmitting {
		return ActivityPaused
	}
	return ActivityIdle
}

// Output takes the screen after new harness output: the only reaction
// that can confirm a submit, a clear or a cancel. Each output restarts
// the quiet period, so it settles nothing by itself.
func (g *Guard) Output(snap vt.Snapshot) {
	g.resize(snap)
	g.outputs++
	g.lastOutput = g.clock.Now()
	g.screen = snap
}

// Observe takes the current screen with no new harness output: a
// re-read, or the screen after a resize. It is no reaction to a key,
// but it settles one the harness already reacted to, see settle.
func (g *Guard) Observe(snap vt.Snapshot) []Outcome {
	g.screen = snap
	if g.resize(snap) {
		return nil
	}
	return g.settle()
}

// Poll settles a key once the harness's reaction has been quiet long
// enough. Time is no evidence by itself: with no output since the key
// it confirms nothing, however long it waits.
func (g *Guard) Poll() []Outcome { return g.settle() }

// resize follows the screen's size; the first screen sets the
// baseline. A resize mid-paste loses sync and reports true. Any other
// restarts the wait for a reaction, since the harness redraws for its
// new size.
func (g *Guard) resize(snap vt.Snapshot) (lost bool) {
	if !g.hasScreen {
		g.cols, g.rows, g.hasScreen = snap.Cols, snap.Rows, true
		return false
	}
	if snap.Cols == g.cols && snap.Rows == g.rows {
		return false
	}
	g.cols, g.rows = snap.Cols, snap.Rows
	if g.state == StatePasting {
		g.lose()
		return true
	}
	g.since = g.outputs
	return false
}

// settle confirms what the screen now shows: the submit, clear or
// cancel the guard waits for, or a draft emptied by plain backspaces.
// It closes nothing while the parser holds part of a human key back.
// It needs the harness to have reacted, with output counted after the
// last key or resize and quiet for reactionQuiet since, and the current
// screen read through CursorInput. An empty input line closes the
// draft with the awaited outcome. Text left keeps it open and blocking:
// a submit that went in as a newline, a partial clear, a cancel that
// brought the draft back. Short of all that it waits: neither the clock
// nor a screen from before the key closes anything, and neither does a
// profile CursorInput cannot read.
func (g *Guard) settle() []Outcome {
	if g.held || g.outputs <= g.since || g.clock.Now().Sub(g.lastOutput) < reactionQuiet {
		return nil
	}
	text, ok := CursorInput(g.profile, g.screen)
	if !ok {
		return nil
	}
	kind := g.awaiting
	switch {
	case kind != "" && text != "":
		g.awaiting = ""
		if kind == OutcomeSubmitted {
			g.transition(StateOpen)
			g.count = g.subCount + 1
		}
		return nil
	case kind != "":
	case g.state == StateOpen && g.count == 0 && g.clean && text == "":
		kind = OutcomeCleared
	default:
		return nil
	}
	gen := g.generation
	g.close()
	return []Outcome{{Kind: kind, Generation: gen}}
}

// NotifyReset reports a harness restart: with the session alive the guard
// lost track and goes UNKNOWN; with it gone everything is over and the
// guard returns to CLOSED. Neither emits an outcome.
func (g *Guard) NotifyReset(sessionAlive bool) {
	if sessionAlive {
		g.lose()
		return
	}
	g.close()
}

// NotifyDesync reports a parser sync loss: a malformed sequence cut in
// half, or any bytes the input stage could not place. The guard goes
// UNKNOWN and blocks injection.
func (g *Guard) NotifyDesync() { g.lose() }

// Unlock manually leaves UNKNOWN for CLOSED, the only way out of it. The
// owner calls it after resynchronising the input stage from scratch.
// It proves nothing about the screen: a delivery still needs every
// check its prepare and commit make, an empty input line included.
func (g *Guard) Unlock() {
	if g.state == StateUnknown {
		g.close()
	}
}

// lose goes UNKNOWN: the guard lost track, and nothing it waited for
// can confirm anymore.
func (g *Guard) lose() {
	g.transition(StateUnknown)
	g.count = 0
	g.clean = false
	g.awaiting = ""
	g.doubted = false
}

// open starts a draft: generation rises, the buffer estimate restarts.
func (g *Guard) open() {
	g.transition(StateOpen)
	g.generation++
	g.count = 0
	g.clean = true
	g.doubted = false
}

// close shuts the draft without an outcome (manual or sync paths).
func (g *Guard) close() {
	g.transition(StateClosed)
	g.count = 0
	g.clean = true
	g.awaiting = ""
	g.held = false
	g.doubted = false
}

// awaits reports the keys a measured profile confirms on the screen.
func awaits(k input.Kind) bool {
	return k == input.KindSubmit || k == input.KindClearLine || k == input.KindCancel
}

// await takes a submit, a clear or a cancel on a measured profile. A
// profile with no input line can never see what the key did, so the
// guard goes UNKNOWN until an explicit Unlock. Otherwise the draft
// stays open, or submitting, under a fresh generation unless one was
// open, and the guard waits for the screen, see settle.
func (g *Guard) await(k input.Kind) {
	if g.profile.InputLine == nil {
		g.lose()
		return
	}
	g.doubted = false
	if g.state != StateOpen {
		g.generation++
		g.count = 0
		g.clean = false
	}
	switch k {
	case input.KindSubmit:
		g.awaiting = OutcomeSubmitted
		g.subCount = g.count
		g.rearm(StateSubmitting)
	case input.KindClearLine:
		g.awaiting = OutcomeCleared
		g.clean = false
		g.rearm(StateOpen)
	default:
		g.awaiting = OutcomeCancelled
		g.clean = false
		g.rearm(StateOpen)
	}
}

// doubt takes, while no draft is open, a key that may have left text on
// the input line or brought some back. Nothing shows the line still
// empty, so a draft opens at once under a fresh generation, its content
// unknown. On a measured profile with an input line it waits for the
// screen as a clear with no draft does: only the harness's output after
// the key, quiet since, showing the line empty closes it, see settle.
// Text there keeps it open, and so does no output at all, however long.
// Another such key, with nothing typed since, waits anew under the same
// generation, as a part of a key held back does once the key is whole.
// A profile that cannot show the line could never close it, so the guard
// goes UNKNOWN until an explicit Unlock.
func (g *Guard) doubt() {
	if !g.profile.Measured || g.profile.InputLine == nil {
		g.lose()
		return
	}
	if g.state == StateOpen {
		g.rearm(StateOpen)
	} else {
		g.open()
	}
	g.clean = false
	g.doubted = true
	g.awaiting = OutcomeCleared
}

// Hold takes part of a human key the parser holds back: a lone ESC, a
// cut escape sequence or paste marker, a cut rune. Those bytes already
// reached the harness, which may answer them before the key is whole, a
// lone ESC above all. So the wait for a reaction to an earlier key ends,
// while no draft is open one opens at once, see doubt, and nothing
// closes until the next event the parser gives, which ends the hold. On
// a draft already open a hold moves nothing else.
func (g *Guard) Hold() {
	g.lastKey = g.clock.Now()
	g.since = g.outputs
	g.held = true
	if g.state == StateClosed {
		g.doubt()
	}
}

// Feed consumes one human input event and returns at most one outcome.
// Every key ends the wait for a reaction to the one before. On a
// measured profile a submit, a clear or a cancel closes nothing by
// itself, see await; on an unmeasured one a cancel closes at once. While
// no draft is open any other key, and any sequence the parser does not
// recognise, opens one, see doubt. Every event ends a hold, see Hold. No
// key leaves UNKNOWN.
func (g *Guard) Feed(ev input.Event) []Outcome {
	g.lastKey = g.clock.Now()
	g.since = g.outputs
	g.awaiting = ""
	g.held = false
	if g.profile.Measured && g.state != StatePasting && g.state != StateUnknown && awaits(ev.Kind) {
		g.await(ev.Kind)
		return nil
	}
	switch g.state {
	case StateClosed:
		switch ev.Kind {
		case input.KindText, input.KindInsertNewline:
			g.open()
			g.count++
		case input.KindPasteStart:
			g.transition(StatePasting)
			g.generation++
			g.count = 0
			g.clean = true
		case input.KindKey, input.KindUnknown:
			g.doubt()
		default:
			// A lone paste end marker, and on an unmeasured profile a
			// submit, a clear or a cancel: none leaves text on an empty
			// input line. A real sync loss arrives via NotifyDesync.
		}
	case StateOpen:
		switch ev.Kind {
		case input.KindText, input.KindInsertNewline:
			g.count++
			g.doubted = false
		case input.KindKey:
			switch {
			case g.doubted:
				g.doubt()
			case ev.Key.Code == input.KeyBackspace && ev.Key.Mod == 0:
				if g.count > 0 {
					g.count--
				} else {
					g.clean = false
				}
			default:
				g.clean = false
			}
		case input.KindClearLine:
			g.clean = false
		case input.KindUnknown:
			if g.doubted {
				g.doubt()
			} else {
				g.clean = false
			}
		case input.KindSubmit:
			g.transition(StateSubmitting)
			g.subCount = g.count
		case input.KindCancel:
			g.close()
			return []Outcome{{Kind: OutcomeCancelled, Generation: g.generation}}
		case input.KindPasteStart:
			g.transition(StatePasting)
			g.doubted = false
		default:
			// PasteEnd without a start, or a lone marker: no draft change.
		}
	case StatePasting:
		switch ev.Kind {
		case input.KindText, input.KindInsertNewline:
			g.count++
		case input.KindPasteEnd:
			g.transition(StateOpen)
		case input.KindUnknown:
			g.clean = false
		default:
			// Inside a paste only text, its end, or taint matter.
		}
	case StateSubmitting:
		// A new human key reopens with a fresh generation. A paste
		// start keeps its paste semantics so a resize inside it still
		// loses sync.
		if ev.Kind == input.KindPasteStart {
			g.transition(StatePasting)
			g.generation++
			g.count = 0
			g.clean = true
			break
		}
		g.transition(StateOpen)
		g.generation++
		g.count = 0
		g.clean = false
	case StateUnknown:
		// Still lost, whatever the key: only an explicit Unlock leaves.
	}
	return nil
}
