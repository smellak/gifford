// Package draft guards the human draft: a deterministic state machine over
// input events and screen snapshots. It never persists typed text — only
// states, counters and types — and it never closes a draft on a timeout:
// closing needs a key or confirmed screen evidence. Anything unconfirmed
// fails closed toward UNKNOWN or a still-open draft.
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
	// until a human submit/cancel from scratch or a manual unlock.
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
	// submitConfirmWindow is the post-submit quiet window that confirms a
	// measured profile without an input_line. It enables the close when
	// observed; it never closes anything on its own.
	submitConfirmWindow = 300 * time.Millisecond
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
	submitted  time.Time
	subCount   int
	cols       int
	rows       int
	hasScreen  bool
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
// nothing held back, a visible cursor in range, and the cursor row an
// input line; text is what the pattern captures there, exactly as
// drawn. No other row counts, whatever it shows. ok is false without
// all of that evidence.
func CursorInput(p profiles.Profile, snap vt.Snapshot) (text string, ok bool) {
	if !p.Measured || p.InputLine == nil || p.InputLine.NumSubexp() != 1 || snap.Pending || !snap.CursorVisible {
		return "", false
	}
	if snap.CursorRow < 0 || snap.CursorRow >= snap.Rows || snap.CursorRow >= len(snap.Lines) ||
		snap.CursorCol < 0 || snap.CursorCol >= snap.Cols {
		return "", false
	}
	return p.Draft(snap.Lines[snap.CursorRow])
}

// State reports the current state.
func (g *Guard) State() State { return g.state }

// Generation counts draft openings; the two-phase commit compares it.
func (g *Guard) Generation() uint64 { return g.generation }

// GuardVersion counts real guard transitions for the two-phase commit; it
// starts at zero and never moves on no-ops. Every state change goes
// through transition so none is missed.
func (g *Guard) GuardVersion() int64 { return g.version }

// transition moves to s, counting the move. Same-state calls are no-ops.
func (g *Guard) transition(s State) {
	if g.state == s {
		return
	}
	g.state = s
	g.version++
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

// Observe consumes a reconstructed screen and returns at most one
// outcome: submit confirmation, newline-instead-of-submit, or a confirmed
// emptying. A resize mid-paste loses sync; any other resize just moves
// the baseline. The first screen sets the baseline.
func (g *Guard) Observe(snap vt.Snapshot) []Outcome {
	if !g.hasScreen {
		g.cols, g.rows, g.hasScreen = snap.Cols, snap.Rows, true
	} else if snap.Cols != g.cols || snap.Rows != g.rows {
		g.cols, g.rows = snap.Cols, snap.Rows
		if g.state == StatePasting {
			g.transition(StateUnknown)
			g.count = 0
			g.clean = false
			return nil
		}
	}
	switch g.state {
	case StateSubmitting:
		return g.confirmSubmit(snap)
	case StateOpen:
		return g.confirmCleared(snap)
	default:
		return nil
	}
}

// Poll evaluates the time-based submit window without a new screen. A
// pending screen match suppresses everything, so only the pattern-less
// path can resolve here.
func (g *Guard) Poll() []Outcome {
	if g.state != StateSubmitting {
		return nil
	}
	return g.confirmSubmit(vt.Snapshot{Pending: true})
}

// confirmSubmit resolves a pending submit against the screen, or against
// the quiet window when the measured profile carries no input_line. An
// unmeasured profile never confirms: without a measurement there is no
// evidence either way, so the draft stays open.
func (g *Guard) confirmSubmit(snap vt.Snapshot) []Outcome {
	if !g.profile.Measured {
		return nil
	}
	if g.profile.InputLine == nil {
		return g.confirmWindow()
	}
	text, ok := CursorInput(g.profile, snap)
	if !ok {
		return nil
	}
	if text != "" {
		g.transition(StateOpen)
		g.count = g.subCount + 1
		return nil
	}
	gen := g.generation
	g.close()
	return []Outcome{{Kind: OutcomeSubmitted, Generation: gen}}
}

// confirmWindow closes a submit after a quiet window with no human keys,
// for measured profiles without an input_line.
func (g *Guard) confirmWindow() []Outcome {
	if g.clock.Now().Sub(g.submitted) < submitConfirmWindow {
		return nil
	}
	if g.clock.Now().Sub(g.lastKey) < submitConfirmWindow {
		return nil
	}
	gen := g.generation
	g.close()
	return []Outcome{{Kind: OutcomeSubmitted, Generation: gen}}
}

// confirmCleared closes an open draft on double evidence only: the buffer
// estimate reached zero through plain backspaces, and the profile input
// line shows empty. Either missing, still open.
func (g *Guard) confirmCleared(snap vt.Snapshot) []Outcome {
	if g.count != 0 || !g.clean {
		return nil
	}
	text, ok := CursorInput(g.profile, snap)
	if !ok || text != "" {
		return nil
	}
	gen := g.generation
	g.close()
	return []Outcome{{Kind: OutcomeCleared, Generation: gen}}
}

// NotifyReset reports a harness restart: with the session alive the guard
// lost track and goes UNKNOWN; with it gone everything is over and the
// guard returns to CLOSED. Neither emits an outcome.
func (g *Guard) NotifyReset(sessionAlive bool) {
	if sessionAlive {
		g.transition(StateUnknown)
		g.count = 0
		g.clean = false
		return
	}
	g.close()
}

// NotifyDesync reports a parser sync loss: a malformed sequence cut in
// half, or any bytes the input stage could not place. The guard goes
// UNKNOWN and blocks injection.
func (g *Guard) NotifyDesync() {
	g.transition(StateUnknown)
	g.count = 0
	g.clean = false
}

// Unlock manually leaves UNKNOWN for CLOSED. The owner calls it after
// resynchronising the input stage from scratch.
func (g *Guard) Unlock() {
	if g.state == StateUnknown {
		g.close()
	}
}

// open starts a draft: generation rises, the buffer estimate restarts.
func (g *Guard) open() {
	g.transition(StateOpen)
	g.generation++
	g.count = 0
	g.clean = true
}

// close shuts the draft without an outcome (manual or sync paths).
func (g *Guard) close() {
	g.transition(StateClosed)
	g.count = 0
	g.clean = true
}

// Feed consumes one human input event and returns at most one outcome.
func (g *Guard) Feed(ev input.Event) []Outcome {
	g.lastKey = g.clock.Now()
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
		default:
			// Anything else needs no draft: keys, lone markers and
			// unrecognised sequences pass through without opening one.
			// A real sync loss arrives via NotifyDesync.
		}
	case StateOpen:
		switch ev.Kind {
		case input.KindText, input.KindInsertNewline:
			g.count++
		case input.KindKey:
			if ev.Key.Code == input.KeyBackspace && ev.Key.Mod == 0 {
				if g.count > 0 {
					g.count--
				} else {
					g.clean = false
				}
			} else {
				g.clean = false
			}
		case input.KindClearLine:
			g.clean = false
		case input.KindUnknown:
			g.clean = false
		case input.KindSubmit:
			g.transition(StateSubmitting)
			g.submitted = g.lastKey
			g.subCount = g.count
		case input.KindCancel:
			g.close()
			return []Outcome{{Kind: OutcomeCancelled, Generation: g.generation}}
		case input.KindPasteStart:
			g.transition(StatePasting)
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
		switch ev.Kind {
		case input.KindSubmit, input.KindCancel:
			g.close()
		default:
			// Still lost: only a human submit/cancel or Unlock closes.
		}
	}
	return nil
}
