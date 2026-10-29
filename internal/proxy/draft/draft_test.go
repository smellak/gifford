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

func screen(lines ...string) vt.Snapshot {
	return vt.Snapshot{Cols: 80, Rows: 24, Lines: lines, CursorRow: len(lines) - 1}
}

func txt(s string) input.Event { return input.Event{Kind: input.KindText, Text: s} }
func key(c input.KeyCode) input.Event {
	return input.Event{Kind: input.KindKey, Key: input.Key{Code: c}}
}
func ev(k input.Kind) input.Event { return input.Event{Kind: k} }

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

	got := g.Observe(screen("> "))
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

	require.Empty(t, g.Observe(screen("> still here")))
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
	got := g.Feed(ev(input.KindCancel))
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

	got := g.Observe(screen("> "))
	require.Equal(t, []Outcome{{Kind: OutcomeCleared, Generation: 1}}, got)
	require.Equal(t, protocol.EventTypeInputDraftCleared, got[0].EventType())
	require.Equal(t, StateClosed, g.State())
}

func TestClearedNeedsBothEvidences(t *testing.T) {
	t.Parallel()

	setup := func() *Guard {
		clk := &testClock{now: time.Now()}
		g := NewGuard(clk, mustMeasured())
		g.Feed(txt("a"))
		g.Feed(txt("b"))
		g.Feed(key(input.KeyBackspace))
		g.Feed(key(input.KeyBackspace))
		return g
	}

	g := setup()
	require.Empty(t, g.Observe(screen("> not empty")))
	require.Equal(t, StateOpen, g.State())

	g = setup()
	g.Feed(txt("x"))
	g.Feed(key(input.KeyArrowLeft))
	g.Feed(key(input.KeyBackspace))
	g.Feed(key(input.KeyBackspace))
	require.Empty(t, g.Observe(screen("> ")))
	require.Equal(t, StateOpen, g.State())

	g = setup()
	g.Feed(txt("x"))
	g.Feed(ev(input.KindClearLine))
	require.Empty(t, g.Observe(screen("> ")))
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
	require.Empty(t, g.Observe(screen("> ")))
	require.Equal(t, StateOpen, g.State())
}

func TestUnmeasuredNeverConfirms(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustDefault())
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, StateSubmitting, g.State())

	clk.advance(10 * time.Second)
	require.Empty(t, g.Observe(screen("")))
	require.Empty(t, g.Poll())
	require.Equal(t, StateSubmitting, g.State())

	clk2 := &testClock{now: time.Now()}
	g2 := NewGuard(clk2, mustDefault())
	g2.Feed(txt("a"))
	g2.Feed(key(input.KeyBackspace))
	require.Empty(t, g2.Observe(screen("")))
	require.Equal(t, StateOpen, g2.State())
}

func TestQuietWindowConfirms(t *testing.T) {
	t.Parallel()

	clk := &testClock{now: time.Now()}
	p := mustMeasured()
	p.InputLinePattern = "[a"
	g := NewGuard(clk, p)
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	require.Equal(t, StateSubmitting, g.State())

	clk.advance(100 * time.Millisecond)
	require.Empty(t, g.Poll())
	clk.advance(250 * time.Millisecond)
	got := g.Poll()
	require.Equal(t, []Outcome{{Kind: OutcomeSubmitted, Generation: 1}}, got)
	require.Equal(t, StateClosed, g.State())
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
	got := g.Observe(screen("> "))
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

	require.Empty(t, g.Feed(ev(input.KindSubmit)))
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
	clk.advance(2 * time.Second)
	require.Equal(t, ActivityIdle, g.Activity())
}
