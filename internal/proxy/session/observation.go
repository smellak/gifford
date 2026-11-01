//go:build unix

package session

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

const (
	// activitySpan is the widest byte window an observation reports.
	activitySpan = 10 * time.Second
	// activityGrain merges harness reads this close to the last mark, so a
	// window holds a bounded number of marks however small the reads.
	activityGrain = 10 * time.Millisecond
)

// outputMark is the harness output read within activityGrain from at.
type outputMark struct {
	at time.Time
	n  int64
}

// observations is what the session's state observations count. Guarded
// by s.mu.
type observations struct {
	// root starts every observation id of the session.
	root string
	// generation is the last cut's observation generation.
	generation int64
	// output counts harness reads the activity detector could not prove
	// inert.
	output int64
	// marks are those effective reads in the last activitySpan, oldest first.
	marks []outputMark
	// exit is the harness's exit status, once it exited.
	exit   int64
	exited bool
	// changed signals that a new cut may differ, without waiting: one
	// pending signal is enough.
	changed chan struct{}
}

// newObservations starts a session's observations. Their ids start with a
// root of the session's own, "o-" and 32 hex digits of 128 random bits, so
// an id takes at most 54 characters and is never reused. The session id
// would not do: it may take an id's whole length, and a resumed session
// keeps it while its generations start over.
func newObservations() (observations, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return observations{}, fmt.Errorf("session: observation id: %w", err)
	}
	return observations{root: "o-" + hex.EncodeToString(raw[:]), changed: make(chan struct{}, 1)}, nil
}

// Observe cuts a new observation of the session as it is now, in one
// critical section, with the guard settled first. Each cut has a
// generation one above the last and its own id, the session's root with
// that generation. It carries the screen only when the cut proves it holds
// no human draft, and never typed text. The agent socket fills in the agent
// and session ids and the harness; nothing returned shares memory with the
// session.
func (s *Session) Observe() (protocol.StateObservationReport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleLocked()
	now := s.clk.Now()
	s.obs.generation++
	r := protocol.StateObservationReport{
		ObservationID:         protocol.OpaqueID(s.obs.root + "." + strconv.FormatInt(s.obs.generation, 10)),
		ObservedAt:            now,
		ObservationGeneration: s.obs.generation,
		OutputGeneration:      s.obs.output,
		InputGuardVersion:     s.snap.Version,
		ScreenState:           protocol.StateObservationScreenWithheld,
		ScreenRows:            []string{},
		Activity:              s.activityLocked(now),
		NativeEvents:          []protocol.StateObservationEvent{},
	}
	if s.obs.exited {
		exit := s.obs.exit
		r.ExitCode = &exit
	}
	if screen, ok := s.screenRowsLocked(); ok {
		r.ScreenState = protocol.StateObservationScreenAvailable
		r.ScreenRows = screen.rows
		r.Cols = int64(screen.cols)
		r.Cursor = &protocol.StateObservationCursor{Row: int64(screen.cursorRow), Col: int64(screen.cursorCol)}
	}
	return r, true
}

// ObservationChanged signals that a new cut may differ from the last: new
// effective harness output, a new guard version, a resize or the harness's
// exit. A proved output no-op still follows the raw, guard and recording
// paths, but does not wake ANALYZE. The session never waits on this channel:
// a signal nobody took yet stands for every later change.
func (s *Session) ObservationChanged() <-chan struct{} { return s.obs.changed }

// screenRowsLocked returns the last screen with its measured editable area
// replaced by a fixed marker. Every human chunk that arrived must be written
// and folded, no key sequence may be held in part, the guard must be closed
// and the harness running; LiveScreen then checks the frame itself. Callers
// hold s.mu and have settled the guard.
type observationScreen struct {
	rows                 []string
	cols                 int
	cursorRow, cursorCol int
}

func (s *Session) screenRowsLocked() (observationScreen, bool) {
	written := s.humanWritten.Load()
	aligned := s.snap.Chunks == written && s.arrived == written && !s.snap.Pending && !s.inputUnresolvedLocked()
	closed := s.snap.State == draft.StateClosed
	if !s.hasSnap || !s.running {
		return observationScreen{}, false
	}
	if s.permissionDialog != nil {
		screen, reason := s.permissionDialog.Screen(s.permissionGuardClosedLocked(), aligned, s.permissionKeyGeneration)
		if reason == "" {
			return observationScreen{rows: screen.Rows, cols: screen.Cols, cursorRow: screen.CursorRow, cursorCol: screen.CursorCol}, true
		}
		if s.permissionDialog.Active() {
			return observationScreen{}, false
		}
	}
	if closed && aligned && s.opts.Profile.Measured && s.opts.Profile.InputLine != nil {
		if rows, reason := LiveScreen(s.opts.Profile, s.lastSnap); reason == "" {
			return observationScreen{rows: rows, cols: s.lastSnap.Cols, cursorRow: s.lastSnap.CursorRow, cursorCol: s.lastSnap.CursorCol}, true
		}
	}
	return observationScreen{}, false
}

// activityLocked is the harness's effective output activity ending at now.
// The three windows count the same marks by the same rule, so they nest.
// Before any effective output, time since the last output runs from the
// session's start.
// Callers hold s.mu.
func (s *Session) activityLocked(now time.Time) protocol.StateObservationActivity {
	s.dropOldMarksLocked(now)
	var a protocol.StateObservationActivity
	for _, m := range s.obs.marks {
		age := now.Sub(m.at)
		if age < 500*time.Millisecond {
			a.BytesLast500ms += m.n
		}
		if age < 2*time.Second {
			a.BytesLast2s += m.n
		}
		a.BytesLast10s += m.n
	}
	last := s.lastEffectiveOutput
	if last.IsZero() {
		last = s.started
	}
	a.SecondsSinceLastOutput = now.Sub(last).Seconds()
	return a
}

// observeOutputLocked counts one effective harness read of n bytes at at and
// signals the change. Raw reads that are proved inert never enter this
// ANALYZE path. Callers hold s.mu.
func (s *Session) observeOutputLocked(n int, at time.Time) {
	s.obs.output++
	s.lastEffectiveOutput = at
	if k := len(s.obs.marks); k > 0 && at.Sub(s.obs.marks[k-1].at) < activityGrain {
		s.obs.marks[k-1].n += int64(n)
	} else {
		s.obs.marks = append(s.obs.marks, outputMark{at: at, n: int64(n)})
	}
	s.dropOldMarksLocked(at)
	s.observationChangedLocked()
}

// dropOldMarksLocked forgets the marks no window ending at now counts.
// Callers hold s.mu.
func (s *Session) dropOldMarksLocked(now time.Time) {
	i := 0
	for i < len(s.obs.marks) && now.Sub(s.obs.marks[i].at) >= activitySpan {
		i++
	}
	s.obs.marks = s.obs.marks[i:]
}

// observeExit records how the harness ended, as a shell reports it, and
// signals the change. A core that failed may not have run the harness at
// all, so its result gives no exit status.
func (s *Session) observeExit(res coreResult) {
	if res.err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.obs.exit, s.obs.exited = int64(res.exit.Status()), true
	s.observationChangedLocked()
}

// observationChangedLocked signals that a new cut may differ, never
// waiting. Callers hold s.mu.
func (s *Session) observationChangedLocked() {
	select {
	case s.obs.changed <- struct{}{}:
	default:
	}
}
