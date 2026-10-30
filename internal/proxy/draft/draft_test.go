package draft

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/input"
	"github.com/smellak/gifford/internal/vt"
)

var bs = string(rune(92))

type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }
func (c *testClock) advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func measuredYAML() string {
	return strings.Join([]string{
		`harness: synth`,
		`measured: true`,
		`version_range: "*"`,
		`submit: "\r"`,
		`insert_newline: "\n"`,
		`clear_line: "` + bs + `u0015"`,
		`cancel: "` + bs + `u0003"`,
		`bracketed_paste: true`,
		`extended_keyboard: none`,
		`input_line: "^` + bs + bs + `s*>` + bs + bs + `s?(.*)$"`,
		"",
	}, "\n")
}

func mustMeasured() profiles.Profile {
	p, err := profiles.ParseFile("synth", []byte(measuredYAML()))
	if err != nil {
		panic(err)
	}
	return p
}

func mustDefault() profiles.Profile {
	ps, err := profiles.Load()
	if err != nil {
		panic(err)
	}
	p, err := ps.Profile("codex", "1.0")
	if err != nil {
		panic(err)
	}
	return p
}

// screen is a snapshot with a visible cursor on its last line.
func screen(lines ...string) vt.Snapshot {
	return vt.Snapshot{Cols: 80, Rows: 24, Lines: lines, CursorRow: len(lines) - 1, CursorVisible: true}
}

// react gives the guard the harness's output drawing lines and lets it
// go quiet: the reaction a submit, a clear or a cancel waits for.
func react(g *Guard, clk *testClock, lines ...string) []Outcome {
	g.Output(screen(lines...))
	clk.advance(reactionQuiet)
	return g.Poll()
}

func txt(s string) input.Event { return input.Event{Kind: input.KindText, Text: s} }
func key(c input.KeyCode) input.Event {
	return input.Event{Kind: input.KindKey, Key: input.Key{Code: c}}
}
func ev(k input.Kind) input.Event { return input.Event{Kind: k} }

// TestCursorInputReadsOnlyTheCursorRow reads the input line under a
// visible cursor and nowhere else: an input line on another row, a
// draft that wrapped to the next row, a snapshot holding bytes back or
// inside an open synchronized update, a hidden cursor or one out of
// range prove nothing, and the text is exactly what the pattern
// captures.
func TestCursorInputReadsOnlyTheCursorRow(t *testing.T) {
	t.Parallel()
	at := func(row, col int, lines ...string) vt.Snapshot {
		return vt.Snapshot{Cols: 80, Rows: 24, Lines: lines, CursorRow: row, CursorCol: col, CursorVisible: true}
	}
	unmeasured := mustMeasured()
	unmeasured.Measured = false
	noPattern := mustMeasured()
	noPattern.InputLinePattern, noPattern.InputLine = "", nil
	pending := at(0, 2, "> ")
	pending.Pending = true
	hidden := at(0, 2, "> ")
	hidden.CursorVisible = false
	syncOpen := at(0, 2, "> ")
	syncOpen.SyncOpen = true
	for _, tc := range []struct {
		name    string
		profile profiles.Profile
		snap    vt.Snapshot
		text    string
		ok      bool
	}{
		{name: "an empty input line", profile: mustMeasured(), snap: at(0, 2, "> "), ok: true},
		{name: "a draft", profile: mustMeasured(), snap: at(1, 7, "working", "> hello"), text: "hello", ok: true},
		{name: "blanks kept", profile: mustMeasured(), snap: at(0, 4, ">  x"), text: " x", ok: true},
		{name: "the input line on another row", profile: mustMeasured(), snap: at(1, 0, "> ", "working")},
		{name: "a draft wrapped to the next row", profile: mustMeasured(), snap: at(1, 2, "> hello", "world")},
		{name: "bytes held back", profile: mustMeasured(), snap: pending},
		{name: "an open synchronized update", profile: mustMeasured(), snap: syncOpen},
		{name: "a hidden cursor", profile: mustMeasured(), snap: hidden},
		{name: "the cursor below the lines", profile: mustMeasured(), snap: at(1, 0, "> ")},
		{name: "the cursor past the screen", profile: mustMeasured(), snap: at(24, 0, "> ")},
		{name: "a negative cursor row", profile: mustMeasured(), snap: at(-1, 0, "> ")},
		{name: "the cursor past the last column", profile: mustMeasured(), snap: at(0, 80, "> ")},
		{name: "an unmeasured profile", profile: unmeasured, snap: at(0, 2, "> ")},
		{name: "a profile without an input line", profile: noPattern, snap: at(0, 2, "> ")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			text, ok := CursorInput(tc.profile, tc.snap)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.text, text)
		})
	}
}

func TestOpenSubmitFlow(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	require.Equal(t, StateClosed, g.State())
	require.True(t, g.InjectionAllowed())

	require.Empty(t, g.Feed(txt("h")))
	require.Empty(t, g.Feed(txt("i")))
	require.Equal(t, StateOpen, g.State())
	require.Equal(t, uint64(1), g.Generation())

	require.Empty(t, g.Feed(ev(input.KindSubmit)))
	require.Equal(t, StateSubmitting, g.State())

	got := react(g, clk, "> ")
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, got)
	require.Equal(t, StateClosed, g.State())
	require.Equal(t, protocol.EventTypeInputDraftSubmitted, got[0].EventType())
}

func TestSubmitWasNewline(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, StateSubmitting, g.State())

	require.Empty(t, react(g, clk, "> still here"))
	require.Equal(t, StateOpen, g.State())
	require.Equal(t, uint64(1), g.Generation())
}

func TestKeyAfterSubmitReopens(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	require.Empty(t, g.Feed(txt("x")))
	require.Equal(t, StateOpen, g.State())
	require.Equal(t, uint64(2), g.Generation())
}

func TestCancelFlow(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("h"))
	require.Empty(t, g.Feed(ev(input.KindCancel)), "a cancel closes nothing by itself")
	require.Equal(t, StateOpen, g.State())
	got := react(g, clk, "> ")
	require.Equal(t, []Outcome{{Kind: OutcomeCancelled, Generation: 1}}, got)
	require.Equal(t, protocol.EventTypeInputDraftCancelled, got[0].EventType())
	require.Equal(t, StateClosed, g.State())
}

func TestClearedFlow(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("a"))
	g.Feed(txt("b"))
	g.Feed(key(input.KeyBackspace))
	g.Feed(key(input.KeyBackspace))
	require.Equal(t, StateOpen, g.State())

	got := react(g, clk, "> ")
	require.Equal(t, []Outcome{{Kind: OutcomeCleared, Generation: 1}}, got)
	require.Equal(t, protocol.EventTypeInputDraftCleared, got[0].EventType())
	require.Equal(t, StateClosed, g.State())
}

func TestClearedNeedsBothEvidences(t *testing.T) {
	t.Parallel()

	setup := func() (*Guard, *testClock) {
		clk := &testClock{now: time.Now()}
		g := NewGuard(clk, mustMeasured())
		g.Feed(txt("a"))
		g.Feed(txt("b"))
		g.Feed(key(input.KeyBackspace))
		g.Feed(key(input.KeyBackspace))
		return g, clk
	}

	g, clk := setup()
	require.Empty(t, react(g, clk, "> not empty"))
	require.Equal(t, StateOpen, g.State())

	g, clk = setup()
	g.Feed(txt("x"))
	g.Feed(key(input.KeyArrowLeft))
	g.Feed(key(input.KeyBackspace))
	g.Feed(key(input.KeyBackspace))
	require.Empty(t, react(g, clk, "> "))
	require.Equal(t, StateOpen, g.State())
}

func TestBackspaceOverrunTaints(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("a"))
	g.Feed(key(input.KeyBackspace))
	g.Feed(key(input.KeyBackspace))
	g.Feed(txt("b"))
	g.Feed(key(input.KeyBackspace))
	require.Empty(t, react(g, clk, "> "))
	require.Equal(t, StateOpen, g.State())
}

func TestUnmeasuredNeverConfirms(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustDefault())
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, StateSubmitting, g.State())

	require.Empty(t, react(g, clk, ""))
	clk.advance(10 * time.Second)
	require.Empty(t, g.Observe(screen("")))
	require.Empty(t, g.Poll())
	require.Equal(t, StateSubmitting, g.State())

	clk2 := &testClock{now: time.Now()}
	g2 := NewGuard(clk2, mustDefault())
	g2.Feed(txt("a"))
	g2.Feed(key(input.KeyBackspace))
	require.Empty(t, react(g2, clk2, ""))
	require.Equal(t, StateOpen, g2.State())
}

// TestNoInputLineWaitsForUnlock runs a measured profile with no input
// line: no screen can show what a submit, a clear or a cancel did, so
// each goes UNKNOWN, no quiet window or output closes it, and only an
// explicit Unlock does.
func TestNoInputLineWaitsForUnlock(t *testing.T) {
	t.Parallel()
	for _, k := range []input.Kind{input.KindSubmit, input.KindClearLine, input.KindCancel} {
		t.Run(string(k), func(t *testing.T) {
			t.Parallel()
			clk := &testClock{now: time.Now()}
			p := mustMeasured()
			p.InputLinePattern, p.InputLine = "", nil
			g := NewGuard(clk, p)
			g.Feed(txt("h"))
			require.Empty(t, g.Feed(ev(k)))
			require.Equal(t, StateUnknown, g.State())
			require.Empty(t, react(g, clk, "> "))
			clk.advance(10 * time.Second)
			require.Empty(t, g.Poll())
			require.Equal(t, StateUnknown, g.State())
			g.Unlock()
			require.Equal(t, StateClosed, g.State())
		})
	}
}

func TestPasteFlow(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	require.Empty(t, g.Feed(ev(input.KindPasteStart)))
	require.Equal(t, StatePasting, g.State())
	require.Equal(t, uint64(1), g.Generation())
	g.Feed(txt("a"))
	g.Feed(txt("b"))
	require.Empty(t, g.Feed(ev(input.KindPasteEnd)))
	require.Equal(t, StateOpen, g.State())
	require.Empty(t, g.Feed(ev(input.KindSubmit)))
	got := react(g, clk, "> ")
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, got)
}

func TestResizeMidPasteLosesSync(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(ev(input.KindPasteStart))
	snap := screen("> ")
	snap.Cols = 100
	require.Empty(t, g.Observe(screen("> ")))
	require.Empty(t, g.Observe(snap))
	require.Equal(t, StateUnknown, g.State())
	require.False(t, g.InjectionAllowed())
}

func TestUnknownPaths(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("h"))
	g.NotifyDesync()
	require.Equal(t, StateUnknown, g.State())
	require.False(t, g.InjectionAllowed())

	require.Empty(t, g.Feed(txt("x")))
	require.Equal(t, StateUnknown, g.State())

	for _, k := range []input.Kind{input.KindSubmit, input.KindClearLine, input.KindCancel} {
		require.Empty(t, g.Feed(ev(k)))
		require.Empty(t, react(g, clk, "> "))
		require.Equal(t, StateUnknown, g.State(), "no key leaves UNKNOWN")
	}
	g.Unlock()
	require.Equal(t, StateClosed, g.State())
	require.True(t, g.InjectionAllowed())

	g.Feed(txt("h"))
	g.NotifyReset(true)
	require.Equal(t, StateUnknown, g.State())
	g.Unlock()
	require.Equal(t, StateClosed, g.State())

	g.Feed(txt("h"))
	g.NotifyReset(false)
	require.Equal(t, StateClosed, g.State())
}

// TestGuardVersionCountsTransitions walks ops with the expected guard
// version: every real transition moves it, every no-op leaves it still.
func TestGuardVersionCountsTransitions(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	require.Equal(t, int64(0), g.GuardVersion())

	g.Feed(txt("a"))
	require.Equal(t, int64(1), g.GuardVersion())
	g.Feed(txt("b"))
	require.Equal(t, int64(1), g.GuardVersion())
	g.Feed(key(input.KeyArrowLeft))
	require.Equal(t, int64(1), g.GuardVersion())

	g.Feed(ev(input.KindSubmit))
	require.Equal(t, int64(2), g.GuardVersion())
	g.Feed(txt("x"))
	require.Equal(t, int64(3), g.GuardVersion())
	// A key that waits on the screen counts even in the same state.
	g.Feed(ev(input.KindCancel))
	require.Equal(t, int64(4), g.GuardVersion())
	g.Feed(ev(input.KindCancel))
	require.Equal(t, int64(5), g.GuardVersion())

	g.Feed(txt("a"))
	require.Equal(t, int64(5), g.GuardVersion())
	g.NotifyDesync()
	require.Equal(t, int64(6), g.GuardVersion())
	g.NotifyDesync()
	require.Equal(t, int64(6), g.GuardVersion())
	g.Unlock()
	require.Equal(t, int64(7), g.GuardVersion())
	g.Unlock()
	require.Equal(t, int64(7), g.GuardVersion())
	g.NotifyReset(false)
	require.Equal(t, int64(7), g.GuardVersion())

	g.Feed(txt("a"))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, int64(9), g.GuardVersion())
	react(g, clk, "> ")
	require.Equal(t, int64(10), g.GuardVersion())
}

func TestAutomaticInjectionRequiresClosedDraft(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	require.True(t, g.InjectionAllowed())

	g.Feed(txt("a"))
	require.False(t, g.InjectionAllowed())
	g.Feed(ev(input.KindPasteStart))
	require.False(t, g.InjectionAllowed())
	g.Feed(ev(input.KindPasteEnd))
	g.Feed(ev(input.KindSubmit))
	require.False(t, g.InjectionAllowed())
	g.NotifyDesync()
	require.False(t, g.InjectionAllowed())
	g.Unlock()
	require.True(t, g.InjectionAllowed())
}

func TestPasteStartWhileSubmittingKeepsPasteState(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("a"))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, StateSubmitting, g.State())

	g.Feed(ev(input.KindPasteStart))
	require.Equal(t, StatePasting, g.State())
	require.Equal(t, uint64(2), g.Generation())

	snap := screen("> ")
	snap.Cols = 100
	g.Observe(screen("> "))
	g.Observe(snap)
	require.Equal(t, StateUnknown, g.State())
}

func TestUnknownSequenceDoesNotInventDraft(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	require.Empty(t, g.Feed(input.Event{Kind: input.KindUnknown, Raw: "x"}))
	require.Equal(t, StateClosed, g.State())
	require.Equal(t, uint64(0), g.Generation())
	require.Equal(t, int64(0), g.GuardVersion())
}

func TestActivityLevels(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	require.Equal(t, ActivityIdle, g.Activity())

	g.Feed(txt("h"))
	require.Equal(t, ActivityActive, g.Activity())
	clk.advance(2 * time.Second)
	require.Equal(t, ActivityPaused, g.Activity())

	g.Feed(ev(input.KindCancel))
	require.NotEmpty(t, react(g, clk, "> "))
	clk.advance(2 * time.Second)
	require.Equal(t, ActivityIdle, g.Activity())
}

// TestKeysWaitForTheReaction runs a submit, a clear and a cancel on a
// measured profile, with no draft and on one: the key ends the permit
// at once, moving the version, and closes nothing until the harness has
// drawn an empty input line after it and stayed quiet for
// reactionQuiet. The empty line from before the key, the clock alone,
// a re-read and a quiet period one millisecond short close nothing.
func TestKeysWaitForTheReaction(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		key     input.Kind
		waiting State
		want    OutcomeKind
	}{
		{input.KindSubmit, StateSubmitting, OutcomeSubmitted},
		{input.KindClearLine, StateOpen, OutcomeCleared},
		{input.KindCancel, StateOpen, OutcomeCancelled},
	} {
		for _, onDraft := range []bool{false, true} {
			name := string(tc.key) + " with no draft"
			if onDraft {
				name = string(tc.key) + " on a draft"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				clk := &testClock{now: time.Now()}
				g := NewGuard(clk, mustMeasured())
				g.Output(screen("> "))
				clk.advance(time.Second)
				if onDraft {
					g.Feed(txt("h"))
				}
				gen, ver := g.Generation(), g.GuardVersion()
				require.Empty(t, g.Feed(ev(tc.key)))
				require.Equal(t, tc.waiting, g.State())
				require.False(t, g.InjectionAllowed())
				require.Greater(t, g.GuardVersion(), ver, "the key ends any permit at once")
				if !onDraft {
					gen++
				}
				require.Equal(t, gen, g.Generation())

				clk.advance(10 * time.Second)
				require.Empty(t, g.Poll(), "no output since the key: the clock alone closes nothing")
				require.Empty(t, g.Observe(screen("> ")), "a re-read is no reaction")
				g.Output(screen("> "))
				clk.advance(reactionQuiet - time.Millisecond)
				require.Empty(t, g.Poll(), "not quiet long enough")
				clk.advance(time.Millisecond)
				require.Equal(t, []Outcome{{Kind: tc.want, Generation: gen}}, g.Poll())
				require.True(t, g.InjectionAllowed())
			})
		}
	}
}

// TestReactionNeedsAnEmptyInputLine reacts to a key with screens that
// do not show the input line empty under the cursor: text left by a
// partial clear, a cancel that brought the draft back, a submit that
// went in as a newline, an empty repaint followed by the draft within
// the quiet period, text that wrapped to the next row, bytes held
// back. None closes the draft, and the guard stops waiting once text
// is left.
func TestReactionNeedsAnEmptyInputLine(t *testing.T) {
	t.Parallel()
	pending := screen("> ")
	pending.Pending = true
	wrapped := vt.Snapshot{Cols: 80, Rows: 24, Lines: []string{"> hello", "world"}, CursorRow: 1, CursorCol: 5, CursorVisible: true}
	for _, tc := range []struct {
		name  string
		key   input.Kind
		react func(g *Guard, clk *testClock)
	}{
		{"a partial clear", input.KindClearLine, func(g *Guard, _ *testClock) { g.Output(screen("> hel")) }},
		{"the draft brought back", input.KindCancel, func(g *Guard, _ *testClock) { g.Output(screen("> hello")) }},
		{"a submit that went in as a newline", input.KindSubmit, func(g *Guard, _ *testClock) { g.Output(screen("> hello")) }},
		{"an empty repaint, then the draft", input.KindCancel, func(g *Guard, clk *testClock) {
			g.Output(screen("> "))
			clk.advance(reactionQuiet / 3)
			require.Empty(t, g.Poll())
			g.Output(screen("> hello"))
		}},
		{"text wrapped to the next row", input.KindCancel, func(g *Guard, _ *testClock) { g.Output(wrapped) }},
		{"bytes held back", input.KindCancel, func(g *Guard, _ *testClock) { g.Output(pending) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clk := &testClock{now: time.Now()}
			g := NewGuard(clk, mustMeasured())
			g.Feed(txt("hello"))
			require.Empty(t, g.Feed(ev(tc.key)))
			tc.react(g, clk)
			clk.advance(reactionQuiet)
			require.Empty(t, g.Poll())
			clk.advance(10 * time.Second)
			require.Empty(t, g.Poll())
			require.Equal(t, StateOpen, g.State(), "the draft stays open and blocking")
			require.False(t, g.InjectionAllowed())
		})
	}
}

// TestAnotherKeyRestartsTheWait presses a key while the guard waits on
// the screen: any key ends the wait, a second cancel waits anew for a
// reaction of its own, and a resize keeps the cancel but needs output
// drawn after it.
func TestAnotherKeyRestartsTheWait(t *testing.T) {
	t.Parallel()

	t.Run("a typed key", func(t *testing.T) {
		t.Parallel()
		clk := &testClock{now: time.Now()}
		g := NewGuard(clk, mustMeasured())
		g.Feed(ev(input.KindCancel))
		g.Output(screen("> "))
		clk.advance(reactionQuiet / 3)
		g.Feed(txt("x"))
		clk.advance(reactionQuiet)
		require.Empty(t, react(g, clk, "> "), "a key typed after the cancel is a draft")
		require.Equal(t, StateOpen, g.State())
	})

	t.Run("a second cancel", func(t *testing.T) {
		t.Parallel()
		clk := &testClock{now: time.Now()}
		g := NewGuard(clk, mustMeasured())
		g.Feed(ev(input.KindCancel))
		gen := g.Generation()
		g.Output(screen("> "))
		clk.advance(reactionQuiet / 2)
		g.Feed(ev(input.KindCancel))
		clk.advance(reactionQuiet)
		require.Empty(t, g.Poll(), "the reaction came before the second cancel")
		require.Equal(t, []Outcome{{Kind: OutcomeCancelled, Generation: gen}}, react(g, clk, "> "))
	})

	t.Run("a resize", func(t *testing.T) {
		t.Parallel()
		clk := &testClock{now: time.Now()}
		g := NewGuard(clk, mustMeasured())
		g.Observe(screen("> "))
		g.Feed(ev(input.KindCancel))
		gen := g.Generation()
		g.Output(screen("> "))
		clk.advance(reactionQuiet / 2)
		resized := screen("> ")
		resized.Cols = 100
		require.Empty(t, g.Observe(resized))
		clk.advance(reactionQuiet)
		require.Empty(t, g.Poll(), "no output since the resize")
		require.Equal(t, StateOpen, g.State())
		g.Output(resized)
		clk.advance(reactionQuiet)
		require.Equal(t, []Outcome{{Kind: OutcomeCancelled, Generation: gen}}, g.Poll())
	})
}

// TestUnmeasuredCancelClosesAtOnce keeps an unmeasured profile as it
// was: a cancel closes the draft at once, since no screen can confirm
// it, and nothing leaves UNKNOWN but an Unlock.
func TestUnmeasuredCancelClosesAtOnce(t *testing.T) {
	t.Parallel()
	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustDefault())
	g.Feed(txt("h"))
	require.Equal(t, []Outcome{{Kind: OutcomeCancelled, Generation: 1}}, g.Feed(ev(input.KindCancel)))
	require.Equal(t, StateClosed, g.State())

	g.NotifyDesync()
	g.Feed(ev(input.KindCancel))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, StateUnknown, g.State())
	g.Unlock()
	require.Equal(t, StateClosed, g.State())
}
