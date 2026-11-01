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
// blocked, until a later one of those keys that it does redraw for. One
// that first redraws the text, and empties its input line only later,
// still closes it: a key whose quiet reaction leaves text goes on
// waiting until a human key, a paste or a desync ends the wait. A
// measured profile with no input line pattern cannot show what such a
// key did, so the guard goes UNKNOWN until an explicit Unlock.
//
// Nor is CLOSED kept on a key that may have left text on the input line,
// or brought some back. While no draft is open, any key that is not
// text, a paste or a submit, a clear or a cancel, and any sequence the
// parser does not recognise, open a draft at once once classified: an
// Alt+letter; an arrow; a Tab. Input transport that has not established a
// human event suspends settlement through SuspendInput without changing the
// draft, activity, or human timestamp. Feed supplies human evidence and ends
// that suspension; ResumeInput ends it for a proven passive terminal reply.
// A profile that cannot show the input line never closes an opened draft: the
// guard goes UNKNOWN.
package draft

import (
	"strings"
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
	// activity is the last activity level an operation observed. Poll uses
	// it to turn a clock-only activity change into a real guard version;
	// human input merely refreshes the baseline because its draft transition,
	// when any, already has the established version semantics.
	activity  Activity
	subCount  int
	cols      int
	rows      int
	hasScreen bool
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
	// held says input transport is unresolved: nothing closes until the
	// parser classifies it as human input or a passive terminal response.
	held bool
	// doubted says the draft opened on a key the guard cannot read,
	// with nothing typed in it since, see doubt.
	doubted bool
}

// NewGuard binds a guard to a harness profile and a clock. The screen
// only counts when the profile is measured with an input_line and canonical
// empty-area bounds, read through CursorInput; anything else confirms nothing
// and fails closed.
// A nil clock reads the system clock.
func NewGuard(clk clock.Clock, p profiles.Profile) *Guard {
	if clk == nil {
		clk = clock.System{}
	}
	return &Guard{
		clock:    clk,
		profile:  p,
		state:    StateClosed,
		clean:    true,
		activity: ActivityIdle,
	}
}

// CursorInput is the one screen reader shared by the guard, PREPARE,
// COMMIT, the agent socket and conformance. It needs a measured profile
// with an input line pattern, a whole frame, with nothing held back and
// no synchronized update open, a visible cursor in range, and the cursor
// row an input line. A nonempty capture is returned exactly as drawn, as
// before. An empty capture is evidence only when every input_bounds row
// exists and matches around it, including any explicit final row outside the
// box; a crop, scroll, partial redraw or text on another editable row therefore
// cannot look like an empty box. A frame inside a synchronized update stays no
// evidence however long it lasts.
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
	if !SupportsInputLayout(p, snap) {
		return "", false
	}
	if !p.InputBounds.Box.Configured() {
		return cursorInputFromLines(p, snap, snap.Lines)
	}
	aligned, known := inputBoxAligned(p, snap)
	if !known {
		return "", false
	}
	if !aligned {
		return cursorInputFromLines(p, snap, snap.Lines)
	}
	if lines, cropped := CropInputBox(p, snap); cropped {
		return cursorInputFromLines(p, snap, lines)
	}

	// Reading a nonempty draft remains safe without box bounds: it cannot
	// prove the input empty. An empty or unrecognized line fails closed.
	text, ok = cursorLineInput(p, snap, snap.Lines[snap.CursorRow])
	if !ok || text != "" {
		return text, ok
	}
	return "", false
}

func cursorInputFromLines(p profiles.Profile, snap vt.Snapshot, lines []string) (text string, ok bool) {
	text, ok = cursorLineInput(p, snap, lines[snap.CursorRow])
	if !ok || text != "" {
		return text, ok
	}
	return "", inputBoundsMatch(p.InputBounds, snap, lines)
}

func cursorLineInput(p profiles.Profile, snap vt.Snapshot, line string) (text string, ok bool) {
	if p.InputPlaceholder == "" {
		return p.Draft(line)
	}
	return placeholderInput(p, snap, line)
}

func inputBoundsMatch(bounds profiles.InputBounds, snap vt.Snapshot, lines []string) bool {
	if !bounds.Complete() {
		return false
	}
	for offset, boundary := range bounds.Above {
		row := snap.CursorRow - offset - 1
		if row < 0 || row >= snap.Rows || row >= len(lines) || !boundary.Matches(lines[row]) {
			return false
		}
	}
	for offset, boundary := range bounds.Below {
		row := snap.CursorRow + offset + 1
		if row < 0 || row >= snap.Rows || row >= len(lines) || !boundary.Matches(lines[row]) {
			return false
		}
	}
	return true
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
	text := line[m[2]:m[3]]
	if p.InputPlaceholderStyle.Configured() {
		for _, cell := range cells {
			if !cell.ExactIndexedStyle(
				p.InputPlaceholderStyle.ForegroundIndex,
				p.InputPlaceholderStyle.BackgroundIndex,
			) {
				return text, true
			}
		}
		if strings.HasPrefix(text, p.InputPlaceholder) {
			if column != snap.CursorCol {
				return "", false
			}
			return "", true
		}
		return text, true
	}
	faint, other := 0, false
	for _, c := range cells {
		if c.Faint {
			faint++
		}
		other = other || c.OtherStyle
	}
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
// through transition so none is missed, a key that waits on the screen
// counts even when the state stays, and Poll counts a clock-only activity
// change: each ends any permit at once.
func (g *Guard) GuardVersion() int64 { return g.version }

// transition moves to s, counting the move. Same-state calls are no-ops.
func (g *Guard) transition(s State) {
	if g.state == s {
		return
	}
	g.state = s
	g.version++
	g.activity = g.currentActivity()
}

// rearm moves to s and counts the move even when the state stays.
func (g *Guard) rearm(s State) {
	if g.state == s {
		g.version++
		g.activity = g.currentActivity()
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
	return g.currentActivity()
}

// currentActivity derives activity from the human-key clock and draft state.
func (g *Guard) currentActivity() Activity {
	if g.clock.Now().Sub(g.lastKey) < activityWindow {
		return ActivityActive
	}
	if g.state == StateOpen || g.state == StatePasting || g.state == StateSubmitting {
		return ActivityPaused
	}
	return ActivityIdle
}

// observeActivity records the current activity. If no draft transition has
// already advanced the guard since version, a clock-only activity change is
// itself a real guard transition. It changes neither draft state nor
// generation.
func (g *Guard) observeActivity(version int64) {
	activity := g.currentActivity()
	if activity == g.activity {
		return
	}
	g.activity = activity
	if g.version == version {
		g.version++
	}
}

// recordHumanActivity refreshes the activity baseline without adding a
// separate version to the existing human-input semantics.
func (g *Guard) recordHumanActivity() { g.activity = g.currentActivity() }

// Output takes the screen after new harness output: the only reaction
// that can confirm a submit, a clear or a cancel. Each output restarts
// the quiet period, so it settles nothing by itself.
func (g *Guard) Output(snap vt.Snapshot) {
	g.resize(snap)
	g.outputs++
	g.lastOutput = g.clock.Now()
	g.screen = snap
}

// Observe takes the current screen with no new reaction to the last
// key: a re-read, the screen after a resize, or output the harness wrote
// before it had that key. It is no reaction to a key, but it settles
// one the harness already reacted to, see settle.
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
func (g *Guard) Poll() []Outcome {
	version := g.version
	defer g.observeActivity(version)
	return g.settle()
}

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
// It closes nothing while input transport is unresolved.
// It needs the harness to have reacted, with output counted after the
// last key or resize and quiet for reactionQuiet since, and the current
// screen read through CursorInput. A completely bounded empty input area
// closes the draft with the awaited outcome. Text left keeps it open and blocking:
// a submit that went in as a newline, a partial clear, a cancel that
// brought the draft back, or a harness that empties its input line only
// on a later redraw. The key goes on waiting, but past that screen: only
// output drawn after it, quiet for reactionQuiet since, can still show
// the line empty and close the draft with the key's outcome. Short of
// all that it waits: neither the clock nor a screen from before the key
// or from before the text closes anything, and neither does an incomplete
// editable area CursorInput cannot read.
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
		// The screen with text is spent: the next reaction to read is
		// output drawn after it. The first one after a submit reopens
		// the draft, with the newline the submit may have put in it.
		g.since = g.outputs
		if g.state == StateSubmitting {
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
func (g *Guard) NotifyDesync() {
	g.lose()
}

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
// Text there keeps it open until output drawn after the text shows the
// line empty, and no output at all keeps it open however long.
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

// Hold records bytes already established as human but not yet expressible as
// a complete input event. It ends the wait for an earlier reaction, records
// human activity, opens a conservative draft if needed, and suspends
// settlement until Feed. Callers that do not yet know whether transport bytes
// are human use SuspendInput instead.
func (g *Guard) Hold() {
	defer g.recordHumanActivity()
	g.lastKey = g.clock.Now()
	g.since = g.outputs
	g.SuspendInput()
	if g.state == StateClosed {
		g.doubt()
	}
}

// SuspendInput freezes screen-based settlement while input bytes have reached
// the harness but are not classified yet. Unlike Hold it is transport state,
// not human evidence: it changes no draft state, version, generation, activity
// timestamp, or reaction boundary.
func (g *Guard) SuspendInput() { g.held = true }

// ResumeInput ends a transport-only suspension. A human event also ends it in
// Feed; a proven passive terminal response uses this explicit path.
func (g *Guard) ResumeInput() { g.held = false }

// Feed consumes one input event and returns at most one outcome. A terminal
// event is outside human semantics and changes nothing.
// Every key ends the wait for a reaction to the one before. On a
// measured profile a submit, a clear or a cancel closes nothing by
// itself, see await; on an unmeasured one a cancel closes at once. While
// no draft is open any other key, and any sequence the parser does not
// recognise, opens one, see doubt. Every human event ends input suspension.
// No key leaves UNKNOWN.
func (g *Guard) Feed(ev input.Event) []Outcome {
	if ev.Kind == input.KindTerminal {
		return nil
	}
	defer g.recordHumanActivity()
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
