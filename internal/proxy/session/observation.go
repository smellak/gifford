//go:build unix

package session

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

const (
	// activitySpan is the widest byte window an observation reports.
	activitySpan = 10 * time.Second
	// activityGrain merges harness reads this close to the last mark, so a
	// window holds a bounded number of marks however small the reads.
	activityGrain = 10 * time.Millisecond
	// The report's bounds. A screen past them is withheld, never cut.
	maxObservedRows  = 10000
	maxObservedBytes = 512 << 10
	maxObservedCols  = 10000
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
	// output counts the harness reads that carried bytes.
	output int64
	// marks are the harness reads of the last activitySpan, oldest first.
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
	if s.screenProvenLocked() {
		snap := s.lastSnap
		r.ScreenState = protocol.StateObservationScreenAvailable
		r.ScreenRows = slices.Clone(snap.Lines)
		r.Cols = int64(snap.Cols)
		r.Cursor = &protocol.StateObservationCursor{Row: int64(snap.CursorRow), Col: int64(snap.CursorCol)}
	}
	return r, true
}

// ObservationChanged signals that a new cut may differ from the last: new
// harness output, a new guard version, a resize or the harness's exit. The
// session never waits on it: a signal nobody took yet stands for every
// later change.
func (s *Session) ObservationChanged() <-chan struct{} { return s.obs.changed }

// screenProvenLocked reports whether the last screen may go in a cut. The
// profile is measured, has an input line pattern and declares a single-line
// input area; every human chunk that arrived was written and folded; no key
// sequence is held in part; the guard is closed; the canonical editable area
// reads empty on this same screen; the cursor falls on a captured row and
// column; and the rows fit the report whole.
//
// The bounds are enough to decide whether delivery may write, but do not
// relax R2c: screen contents go out only when the profile separately declares
// the editable area single-line. In every other case the screen is withheld.
// No profile declares single-line today. Callers hold s.mu and have settled
// the guard.
func (s *Session) screenProvenLocked() bool {
	written := s.humanWritten.Load()
	if !s.opts.Profile.Measured || s.opts.Profile.InputLine == nil ||
		s.opts.Profile.InputArea != profiles.InputAreaSingleLine ||
		s.snap.Chunks != written || s.arrived != written || s.snap.Pending ||
		s.snap.State != draft.StateClosed || s.inputEvidenceLocked() != "" {
		return false
	}
	snap := s.lastSnap
	if snap.Cols < 1 || snap.Cols > maxObservedCols || len(snap.Lines) > maxObservedRows ||
		snap.CursorRow < 0 || snap.CursorRow >= len(snap.Lines) ||
		snap.CursorCol < 0 || snap.CursorCol >= snap.Cols {
		return false
	}
	total := 0
	for _, line := range snap.Lines {
		total += len(line)
		if total > maxObservedBytes || !utf8.ValidString(line) {
			return false
		}
	}
	return true
}

// activityLocked is the harness's output activity ending at now. The three
// windows count the same marks by the same rule, so they nest. Before any
// output, the time since the last output runs from the session's start.
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
	last := s.lastOutput
	if last.IsZero() {
		last = s.started
	}
	a.SecondsSinceLastOutput = now.Sub(last).Seconds()
	return a
}

// observeOutputLocked counts one harness read of n bytes, made at
// s.lastOutput, and signals the change. Callers hold s.mu.
func (s *Session) observeOutputLocked(n int) {
	s.obs.output++
	at := s.lastOutput
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
