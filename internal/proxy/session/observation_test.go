//go:build unix

package session

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/vt"
)

// observationID is an observation id's form: the session's root, "o-" and
// 32 hex digits, then a dot and the generation.
var observationID = regexp.MustCompile(`^(o-[0-9a-f]{32})\.([1-9][0-9]*)$`)

// singleLineArea declares the measured profile's input area a single line,
// with no key to break it, as only such a profile may declare it.
func singleLineArea(o *Options) {
	o.Profile.InsertNewline = ""
	o.Profile.InputArea = profiles.InputAreaSingleLine
}

// observe cuts an observation, checks it validates once the socket fills in
// the ids and the harness, and that its id is the session's root with its
// generation, and returns it as the session gave it.
func observe(t *testing.T, s *Session) protocol.StateObservationReport {
	t.Helper()
	r, ok := s.Observe()
	require.True(t, ok)
	id := observationID.FindStringSubmatch(string(r.ObservationID))
	require.NotNil(t, id, "o-, 32 hex digits, a dot and the generation: %q", r.ObservationID)
	require.Equal(t, strconv.FormatInt(r.ObservationGeneration, 10), id[2])
	filled := r
	filled.AgentID, filled.SessionID = sockAgent, "sess-1"
	filled.Harness, filled.HarnessVersion = protocol.HarnessCodex, "unknown"
	require.NoError(t, filled.Validate())
	require.NotNil(t, r.ScreenRows, "an empty list, never null")
	require.NotNil(t, r.NativeEvents, "an empty list, never null")
	require.Empty(t, r.NativeEvents, "no durable native event yet")
	require.Zero(t, r.EventGeneration)
	require.Empty(t, r.AgentID, "the socket fills in the ids")
	return r
}

// idRoot is the root of an observation's id.
func idRoot(r protocol.StateObservationReport) string {
	return observationID.FindStringSubmatch(string(r.ObservationID))[1]
}

// requireWithheld checks that r carries no screen at all, and none of words.
func requireWithheld(t *testing.T, r protocol.StateObservationReport, words ...string) {
	t.Helper()
	require.Equal(t, protocol.StateObservationScreenWithheld, r.ScreenState)
	require.Empty(t, r.ScreenRows)
	require.Zero(t, r.Cols)
	require.Nil(t, r.Cursor)
	raw, err := json.Marshal(r)
	require.NoError(t, err)
	for _, w := range words {
		require.NotContains(t, string(raw), w)
	}
}

// screenOf is the session's last screen.
func screenOf(s *Session) vt.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSnap
}

// requireNothingInFlight checks the guard is closed with every human chunk
// written and folded and no key sequence held: all a delivery asks of the
// guard.
func requireNothingInFlight(t *testing.T, s *Session) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleLocked()
	written := s.humanWritten.Load()
	require.Equal(t, written, s.snap.Chunks)
	require.Equal(t, written, s.arrived)
	require.False(t, s.snap.Pending)
	require.Equal(t, protocol.DraftStateClosed, draftState(s.snap.State))
}

// On a profile not yet measured, the screen never goes out, though the
// guard is closed and nothing is in flight: the cancel key closes such a
// guard at once, with no screen to confirm it, and the human's text is
// still on the screen. Declaring the area a single line changes nothing.
func TestObservationOfAnUnmeasuredProfileWithholdsTheScreen(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-unmeasured", unmeasuredInput, singleLineArea)
	o.s.onOutput([]byte(clearScreen + emptyBox))
	o.typed(t, "secret")
	o.s.onOutput([]byte(clearScreen + "> secret\x1b[0m"))
	o.typed(t, "\x03")
	requireNothingInFlight(t, o.s)
	require.Contains(t, screenOf(o.s).Text(), "secret", "the text the cancel left behind")

	requireWithheld(t, observe(t, o.s), "secret")
}

// A guard that a screen-confirmed submit closed, with nothing in flight and
// the input line empty under the cursor, lets the screen go out as it is,
// on a profile that declares its input area a single line.
func TestObservationCarriesAProvenScreen(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-proven", singleLineArea)
	o.closed(t)
	r := observe(t, o.s)
	snap := screenOf(o.s)
	require.Equal(t, protocol.StateObservationScreenAvailable, r.ScreenState)
	require.Equal(t, snap.Lines, r.ScreenRows)
	require.Equal(t, int64(snap.Cols), r.Cols)
	require.Equal(t, &protocol.StateObservationCursor{Row: int64(snap.CursorRow), Col: int64(snap.CursorCol)}, r.Cursor)
	require.Equal(t, ">", r.ScreenRows[snap.CursorRow], "the empty input line")
}

// A cut settles the guard before it reads it, as every reader of the guard
// does: a submit whose quiet reaction the screen already shows is closed in
// the cut itself, though nothing else read the guard since the clock moved.
func TestObservationSettlesTheGuardFirst(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-settle", singleLineArea)
	o.s.onOutput([]byte(clearScreen + emptyBox))
	o.typed(t, "warmup\r")
	o.clock.advance(time.Millisecond)
	o.s.onOutput([]byte(clearScreen + emptyBox))
	o.clock.advance(time.Second)
	require.NotEqual(t, protocol.DraftStateClosed, draftState(publishedSnapshot(t, o.s).State), "not settled yet")
	require.Equal(t, protocol.StateObservationScreenAvailable, observe(t, o.s).ScreenState)
}

// Any doubt about the human's input withholds the whole screen: a draft
// open, drawn or not yet, a key read but not written, a key written but not
// folded, a key sequence held in part, a draft drawn back on the input line
// while the guard stays closed, and a profile with no input line to prove
// empty.
func TestObservationWithholdsAScreenItCannotProve(t *testing.T) {
	t.Parallel()
	for name, doubt := range map[string]func(*testing.T, *orderRun){
		"a draft open": func(t *testing.T, o *orderRun) {
			t.Helper()
			o.typed(t, "secret")
			o.s.onOutput([]byte(clearScreen + "> secret\x1b[0m"))
		},
		"a draft open, not drawn yet": func(t *testing.T, o *orderRun) {
			t.Helper()
			o.typed(t, "secret")
			require.Equal(t, ">", screenOf(o.s).Lines[screenOf(o.s).CursorRow], "the line still reads empty")
		},
		"a key read, not written": func(t *testing.T, o *orderRun) {
			t.Helper()
			o.arrived()
		},
		"a key written, not folded": func(t *testing.T, o *orderRun) {
			t.Helper()
			o.answered(t, "s", clearScreen+"> secret\x1b[0m")
		},
		"a key sequence held in part": func(t *testing.T, o *orderRun) {
			t.Helper()
			o.typed(t, "\x1b[1;5")
		},
		"a draft drawn back under the cursor": func(t *testing.T, o *orderRun) {
			t.Helper()
			o.s.onOutput([]byte(clearScreen + "> secret\x1b[0m"))
			requireNothingInFlight(t, o.s)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := newOrderRun(t, "observe-doubt", singleLineArea)
			o.closed(t)
			require.Equal(t, protocol.StateObservationScreenAvailable, observe(t, o.s).ScreenState)
			doubt(t, o)
			requireWithheld(t, observe(t, o.s), "secret")
		})
	}
	t.Run("no input line to prove empty", func(t *testing.T) {
		t.Parallel()
		o := newOrderRun(t, "observe-no-line", singleLineArea, func(opts *Options) {
			opts.Profile.InputLinePattern, opts.Profile.InputLine = "", nil
		})
		o.s.onOutput([]byte(clearScreen + "ready"))
		requireNothingInFlight(t, o.s)
		requireWithheld(t, observe(t, o.s), "ready")
	})
}

// A proven screen goes out whole or not at all: one past the report's
// bounds is withheld, never cut, and one right at them goes out.
func TestObservationWithholdsAScreenPastTheReportBounds(t *testing.T) {
	t.Parallel()
	screen := func(cols int, lines ...string) vt.Snapshot {
		return vt.Snapshot{Cols: cols, Rows: len(lines), Lines: lines, CursorCol: 2, CursorVisible: true}
	}
	rows := func(n int) []string {
		lines := make([]string, n)
		lines[0] = "> "
		return lines
	}
	cursorPast := screen(80, "> ")
	cursorPast.CursorCol = 80
	for name, tc := range map[string]struct {
		snap      vt.Snapshot
		available bool
	}{
		"10000 rows":           {snap: screen(80, rows(10000)...), available: true},
		"10001 rows":           {snap: screen(80, rows(10001)...)},
		"512 KiB of rows":      {snap: screen(80, "> ", strings.Repeat("x", 512<<10-2)), available: true},
		"one byte more":        {snap: screen(80, "> ", strings.Repeat("x", 512<<10-1))},
		"a row not UTF-8":      {snap: screen(80, "> ", "\xff")},
		"10000 columns":        {snap: screen(10000, "> "), available: true},
		"10001 columns":        {snap: screen(10001, "> ")},
		"cursor past the cols": {snap: cursorPast},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			o := newOrderRun(t, "observe-bounds", singleLineArea)
			o.closed(t)
			o.s.mu.Lock()
			o.s.lastSnap = tc.snap
			o.s.mu.Unlock()
			r := observe(t, o.s)
			if !tc.available {
				requireWithheld(t, r)
				return
			}
			require.Equal(t, protocol.StateObservationScreenAvailable, r.ScreenState)
			require.Equal(t, tc.snap.Lines, r.ScreenRows, "the rows whole")
		})
	}
}

// The activity ends at the cut, on one clock read: the three windows count
// the same reads by the same rule, the time since the last output is the cut
// less that read's instant, exactly, and before any output it runs from the
// session's start. Reads within the grain are one mark, still counted one by
// one in the output generation.
func TestObservationActivity(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-activity")
	start := o.clock.Now()
	at := func(d time.Duration) {
		o.clock.mu.Lock()
		o.clock.now = start.Add(d)
		o.clock.mu.Unlock()
	}
	r := observe(t, o.s)
	require.Equal(t, protocol.StateObservationActivity{}, r.Activity, "no output yet, at the start")
	at(3 * time.Second)
	require.InDelta(t, 3.0, observe(t, o.s).Activity.SecondsSinceLastOutput, 0, "from the start")

	at(3500 * time.Millisecond)
	o.s.onOutput([]byte(strings.Repeat("a", 100)))
	at(11500 * time.Millisecond)
	o.s.onOutput([]byte(strings.Repeat("b", 200)))
	at(12700 * time.Millisecond)
	o.s.onOutput([]byte(strings.Repeat("c", 300)))
	at(12705 * time.Millisecond)
	o.s.onOutput([]byte(strings.Repeat("d", 7)))

	at(13 * time.Second)
	r = observe(t, o.s)
	require.Equal(t, protocol.StateObservationActivity{
		BytesLast500ms: 307, BytesLast2s: 507, BytesLast10s: 607,
		SecondsSinceLastOutput: (13*time.Second - 12705*time.Millisecond).Seconds(),
	}, r.Activity)
	require.Equal(t, int64(4), r.OutputGeneration, "every read counts")
	o.s.mu.Lock()
	marks := len(o.s.obs.marks)
	o.s.mu.Unlock()
	require.Equal(t, 3, marks, "the two reads 5 ms apart are one mark")

	// No output since: the same output generation, and the windows move
	// with the cut. Ages of exactly 500 ms, 2 s and 10 s are out of their
	// window.
	at(13200 * time.Millisecond)
	require.Equal(t, protocol.StateObservationActivity{
		BytesLast500ms: 0, BytesLast2s: 507, BytesLast10s: 607,
		SecondsSinceLastOutput: (13200*time.Millisecond - 12705*time.Millisecond).Seconds(),
	}, observe(t, o.s).Activity)
	at(13500 * time.Millisecond)
	refresh := observe(t, o.s)
	require.Equal(t, protocol.StateObservationActivity{
		BytesLast500ms: 0, BytesLast2s: 307, BytesLast10s: 507,
		SecondsSinceLastOutput: (13500*time.Millisecond - 12705*time.Millisecond).Seconds(),
	}, refresh.Activity)
	require.Equal(t, r.OutputGeneration, refresh.OutputGeneration)
	require.Equal(t, start.Add(13500*time.Millisecond), refresh.ObservedAt, "observed at the cut")
}

// Every cut is a new observation: a generation one above the last and an id
// made of the session's root and that generation. The output generation
// counts only harness reads that carried bytes, and the guard version is
// the guard's settled one.
func TestObservationGenerations(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-gens")
	first := observe(t, o.s)
	require.Equal(t, int64(1), first.ObservationGeneration)
	root := idRoot(first)
	require.Equal(t, protocol.OpaqueID(root+".1"), first.ObservationID)
	require.Zero(t, first.OutputGeneration)

	o.s.onOutput(nil)
	empty := observe(t, o.s)
	require.Equal(t, int64(2), empty.ObservationGeneration)
	require.Equal(t, protocol.OpaqueID(root+".2"), empty.ObservationID, "one root for every cut")
	require.Zero(t, empty.OutputGeneration, "a read with no bytes is no output")

	o.s.onOutput([]byte("x"))
	require.Equal(t, int64(1), observe(t, o.s).OutputGeneration)

	o.typed(t, "a")
	typed := observe(t, o.s)
	require.Equal(t, o.s.InputGuard().InputGuardVersion, typed.InputGuardVersion)
	require.Greater(t, typed.InputGuardVersion, first.InputGuardVersion)
	require.Equal(t, protocol.OpaqueID(root+".4"), typed.ObservationID)
}

// An observation id is bounded and never reused, whatever the session id:
// a session id of an id's whole length still gives valid ids, and a session
// that takes the id of another, as a resumed session does, gives ids of its
// own, though its generations start over.
func TestObservationIDsAreTheSessions(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("s", 128)
	require.True(t, protocol.OpaqueID(long).Valid())
	first, resumed := newOrderRun(t, long), newOrderRun(t, long)
	a, b := observe(t, first.s), observe(t, resumed.s)
	require.Equal(t, a.ObservationGeneration, b.ObservationGeneration)
	require.NotEqual(t, idRoot(a), idRoot(b))
}

// The input line reading empty proves nothing of the other rows: an input
// area the profile does not declare a single line may hold the draft on the
// row above while the line under the cursor reads empty. So the screen is
// withheld, though nothing is in flight and the line reads empty, unless
// the profile declares the area a single line.
func TestObservationWithholdsAnAreaNotDeclaredSingleLine(t *testing.T) {
	t.Parallel()
	cut := func(t *testing.T, o *orderRun, above string) protocol.StateObservationReport {
		t.Helper()
		o.closed(t)
		o.s.onOutput([]byte(clearScreen + above + "\r\n" + emptyBox))
		requireNothingInFlight(t, o.s)
		o.s.mu.Lock()
		evidence := o.s.inputEvidenceLocked()
		o.s.mu.Unlock()
		require.Empty(t, evidence, "the input line reads empty under the cursor")
		return observe(t, o.s)
	}

	o := newOrderRun(t, "observe-multiline")
	require.Equal(t, profiles.InputAreaUnknown, o.s.opts.Profile.InputArea, "as every profile declares it today")
	requireWithheld(t, cut(t, o, "> synthetic-private-draft"), "synthetic-private-draft")

	r := cut(t, newOrderRun(t, "observe-single-line", singleLineArea), "> an earlier prompt")
	require.Equal(t, protocol.StateObservationScreenAvailable, r.ScreenState)
	require.Contains(t, r.ScreenRows, "> an earlier prompt")
}

// A report shares no memory with the session or with another report: what
// the agent socket keeps for a replay never changes under it, and changing
// it changes nothing in the session.
func TestObservationIsItsOwnCopy(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-copy", singleLineArea)
	o.closed(t)
	o.s.observeExit(coreResult{exit: proxy.Exit{Code: 3}})
	r := observe(t, o.s)
	rows, cursor := slices.Clone(r.ScreenRows), *r.Cursor
	r.ScreenRows[r.Cursor.Row] = "changed"
	r.Cursor.Row = 7
	*r.ExitCode = 9
	next := observe(t, o.s)
	require.Equal(t, rows, next.ScreenRows)
	require.Equal(t, cursor, *next.Cursor)
	require.Equal(t, int64(3), *next.ExitCode)

	kept, err := json.Marshal(next)
	require.NoError(t, err)
	o.s.onOutput([]byte(clearScreen + "later\r\n" + emptyBox))
	again, err := json.Marshal(next)
	require.NoError(t, err)
	require.JSONEq(t, string(kept), string(again), "new output does not reach a report already cut")
}

// The session signals a change for new output, a new guard version, a
// resize and the harness's exit, never waiting, and not for a settle that
// changes nothing.
func TestObservationChangedSignals(t *testing.T) {
	t.Parallel()
	o := newOrderRun(t, "observe-signal")
	changed := o.s.ObservationChanged()
	drained := func() {
		select {
		case <-changed:
		default:
		}
	}
	signalled := func(what string) {
		t.Helper()
		select {
		case <-changed:
		default:
			require.Fail(t, "no signal for "+what)
		}
	}
	quiet := func(what string) {
		t.Helper()
		select {
		case <-changed:
			require.Fail(t, "a signal for "+what)
		default:
		}
	}

	drained()
	o.s.onOutput([]byte("x"))
	signalled("new output")
	o.typed(t, "a")
	signalled("a new guard version")
	o.s.mu.Lock()
	o.s.settleLocked()
	o.s.mu.Unlock()
	quiet("a settle that changes nothing")
	require.NoError(t, o.s.doResize(100, 30, make(chan os.Signal, 1)))
	signalled("a resize")
	o.s.observeExit(coreResult{err: errors.New("the core failed")})
	quiet("a core that failed")
	require.Nil(t, observe(t, o.s).ExitCode)
	o.s.observeExit(coreResult{exit: proxy.Exit{Code: 3}})
	signalled("the exit")
	for range 3 {
		o.s.onOutput([]byte("y"))
	}
	signalled("output")
	quiet("output already signalled once")
}

// The exit status goes in every cut after the harness ended, as a shell
// reports it, and a run whose harness never started has none.
func TestObservationExitCode(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, id string, cmd ...string) (*Session, error) {
		t.Helper()
		_, slave := testPTY(t)
		s, err := NewSession(testOptions(t.TempDir(), id, slave, slave, &chanSink{}, cmd))
		require.NoError(t, err)
		select {
		case r := <-runAsync(t, s):
			return s, r.err
		case <-time.After(30 * time.Second):
			require.FailNow(t, "the session did not end")
			return nil, nil
		}
	}
	for name, tc := range map[string]struct {
		cmd  []string
		exit int64
	}{
		"exit status": {cmd: []string{"sh", "-c", "exit 3"}, exit: 3},
		"signal":      {cmd: []string{"sh", "-c", "kill -TERM $$"}, exit: 128 + 15},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s, err := run(t, "observe-exit", tc.cmd...)
			require.NoError(t, err)
			r := observe(t, s)
			require.NotNil(t, r.ExitCode)
			require.Equal(t, tc.exit, *r.ExitCode)
		})
	}
	t.Run("never started", func(t *testing.T) {
		t.Parallel()
		s, err := run(t, "observe-no-start", "/nonexistent/harness")
		require.Error(t, err)
		require.Nil(t, observe(t, s).ExitCode)
	})
}
