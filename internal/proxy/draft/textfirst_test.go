package draft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/proxy/input"
)

// waitingKeys are the keys a measured profile confirms on the screen,
// each with the outcome it waits for.
var waitingKeys = []struct {
	key  input.Kind
	want OutcomeKind
}{
	{input.KindSubmit, OutcomeSubmitted},
	{input.KindClearLine, OutcomeCleared},
	{input.KindCancel, OutcomeCancelled},
}

// textFirst presses key on a draft, then has the harness answer with
// the draft still on the input line, quiet for reactionQuiet: the draft
// stays open and blocking under its generation.
func textFirst(t *testing.T, key input.Kind) (*Guard, *testClock) {
	t.Helper()
	clk := &testClock{now: time.Now()}
	g := NewGuard(clk, mustMeasured())
	g.Feed(txt("h"))
	require.Empty(t, g.Feed(ev(key)))
	require.Empty(t, react(g, clk, "> hello"))
	require.Equal(t, StateOpen, g.State())
	require.False(t, g.InjectionAllowed())
	require.Equal(t, uint64(1), g.Generation())
	return g, clk
}

// TestQuietPeriodStays pins the quiet period a reaction needs.
func TestQuietPeriodStays(t *testing.T) {
	t.Parallel()
	require.Equal(t, 300*time.Millisecond, reactionQuiet)
}

// TestTextLeftWaitsForALaterReaction answers a submit, a clear or a
// cancel with the text still on the input line, as a harness that
// empties it only on a later redraw does. The key goes on waiting: the
// clock, a re-read and an empty line drawn before the text close
// nothing, and every quiet screen with text moves the wait past it, so
// text that stays never closes the draft. An empty line drawn after the
// text closes it with the key's outcome and the draft's generation,
// once quiet for reactionQuiet from that output, and once only.
func TestTextLeftWaitsForALaterReaction(t *testing.T) {
	t.Parallel()
	for _, tc := range waitingKeys {
		t.Run(string(tc.key), func(t *testing.T) {
			t.Parallel()

			t.Run("text that stays", func(t *testing.T) {
				t.Parallel()
				g, clk := textFirst(t, tc.key)
				for range 5 {
					require.Empty(t, react(g, clk, "> hello"), "text again is no empty line")
					clk.advance(time.Hour)
					require.Empty(t, g.Poll())
					require.Empty(t, g.Observe(screen("> hello")))
				}
				require.Equal(t, StateOpen, g.State(), "the draft stays open and blocking")
				require.False(t, g.InjectionAllowed())
			})

			t.Run("an empty line before the text", func(t *testing.T) {
				t.Parallel()
				clk := &testClock{now: time.Now()}
				g := NewGuard(clk, mustMeasured())
				g.Feed(txt("h"))
				g.Feed(ev(tc.key))
				g.Output(screen("> "))
				clk.advance(reactionQuiet / 3)
				require.Empty(t, g.Poll())
				require.Empty(t, react(g, clk, "> hello"))
				clk.advance(time.Hour)
				require.Empty(t, g.Poll(), "the empty line came before the text")
				require.Empty(t, g.Observe(screen("> ")), "a screen read again is no output")
				require.Equal(t, StateOpen, g.State())
			})

			t.Run("an empty line after the text", func(t *testing.T) {
				t.Parallel()
				g, clk := textFirst(t, tc.key)
				clk.advance(time.Hour)
				require.Empty(t, g.Poll(), "the clock alone closes nothing")
				g.Output(screen("> "))
				clk.advance(reactionQuiet - time.Millisecond)
				require.Empty(t, g.Poll(), "not quiet long enough")
				g.Output(screen("> "))
				clk.advance(reactionQuiet - time.Millisecond)
				require.Empty(t, g.Poll(), "the quiet period runs from the last output")
				clk.advance(time.Millisecond)
				require.Equal(t, []Outcome{{Kind: tc.want, Generation: 1}}, g.Poll())
				require.Equal(t, StateClosed, g.State())
				require.True(t, g.InjectionAllowed())
				clk.advance(time.Hour)
				require.Empty(t, g.Poll(), "one outcome")
				require.Empty(t, react(g, clk, "> "))
			})

			t.Run("an empty line, then text again", func(t *testing.T) {
				t.Parallel()
				g, clk := textFirst(t, tc.key)
				g.Output(screen("> "))
				clk.advance(reactionQuiet / 3)
				require.Empty(t, g.Poll())
				require.Empty(t, react(g, clk, "> hello"))
				clk.advance(time.Hour)
				require.Empty(t, g.Poll())
				require.Equal(t, StateOpen, g.State())
				require.Equal(t, []Outcome{{Kind: tc.want, Generation: 1}}, react(g, clk, "> "),
					"an empty line after the last text still closes")
			})
		})
	}
}

// TestInputEndsTheWaitAfterText answers a key with text left, then has
// the human act before the harness empties the line: a typed key, a
// paste or a desync ends the wait, so the empty line drawn after them
// closes nothing. A second submit, clear or cancel waits on its own: an
// empty line drawn before it does not count, one drawn after it closes
// the draft with its own outcome.
func TestInputEndsTheWaitAfterText(t *testing.T) {
	t.Parallel()
	for _, tc := range waitingKeys {
		t.Run(string(tc.key), func(t *testing.T) {
			t.Parallel()
			for _, ends := range []struct {
				name  string
				input func(g *Guard)
				state State
			}{
				{"a typed key", func(g *Guard) { g.Feed(txt("x")) }, StateOpen},
				{"a paste", func(g *Guard) { g.Feed(ev(input.KindPasteStart)) }, StatePasting},
				{"a desync", func(g *Guard) { g.NotifyDesync() }, StateUnknown},
			} {
				t.Run(ends.name, func(t *testing.T) {
					t.Parallel()
					g, clk := textFirst(t, tc.key)
					ends.input(g)
					require.Empty(t, react(g, clk, "> "))
					clk.advance(time.Hour)
					require.Empty(t, g.Poll())
					require.Equal(t, ends.state, g.State())
					require.False(t, g.InjectionAllowed())
				})
			}

			for _, next := range waitingKeys {
				t.Run("then "+string(next.key), func(t *testing.T) {
					t.Parallel()
					g, clk := textFirst(t, tc.key)
					g.Output(screen("> "))
					clk.advance(reactionQuiet / 3)
					ver := g.GuardVersion()
					require.Empty(t, g.Feed(ev(next.key)))
					require.Greater(t, g.GuardVersion(), ver, "the key ends any permit at once")
					clk.advance(time.Hour)
					require.Empty(t, g.Poll(), "the empty line came before the second key")
					require.Equal(t, []Outcome{{Kind: next.want, Generation: 1}}, react(g, clk, "> "))
				})
			}
		})
	}
}

// TestResizeAfterTextNeedsOutput resizes the screen while a key waits
// past text: the empty line on the resized screen is no reaction until
// the harness draws after the resize, and that output closes the draft
// once quiet.
func TestResizeAfterTextNeedsOutput(t *testing.T) {
	t.Parallel()
	for _, tc := range waitingKeys {
		t.Run(string(tc.key), func(t *testing.T) {
			t.Parallel()
			g, clk := textFirst(t, tc.key)
			resized := screen("> ")
			resized.Cols = 100
			require.Empty(t, g.Observe(resized))
			clk.advance(time.Hour)
			require.Empty(t, g.Poll(), "no output since the resize")
			require.Equal(t, StateOpen, g.State())
			g.Output(resized)
			clk.advance(reactionQuiet)
			require.Equal(t, []Outcome{{Kind: tc.want, Generation: 1}}, g.Poll())
		})
	}
}

// TestAmbiguousKeyWaitsPastText presses, with no draft open, each key
// the guard cannot tell typed nothing, and has the harness answer with
// text on the input line, quiet, as one that brings a prompt back and
// empties the line only on a later redraw. The draft the key opened
// stays open and blocking, however long the text stays and however
// often it is read again. An empty line drawn after the text closes it,
// as cleared, once quiet for reactionQuiet from that output, once, under
// the key's generation. So does a lone ESC held and answered with text,
// then made whole by a letter the harness answers with text again.
func TestAmbiguousKeyWaitsPastText(t *testing.T) {
	t.Parallel()
	emptyAfterText := func(t *testing.T, g *Guard, clk *testClock, gen uint64) {
		t.Helper()
		g.Output(screen("> "))
		clk.advance(reactionQuiet - time.Nanosecond)
		require.Empty(t, g.Poll(), "the empty line closed the draft before it was quiet")
		clk.advance(time.Nanosecond)
		require.Equal(t, []Outcome{{Kind: OutcomeCleared, Generation: gen}}, g.Poll())
		require.Equal(t, StateClosed, g.State())
		clk.advance(time.Hour)
		require.Empty(t, g.Poll(), "once only")
	}
	for _, k := range ambiguousKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			g, clk, gen := pressAmbiguous(t, k)
			for range 3 {
				require.Empty(t, react(g, clk, "> recalled"), "text is no empty line")
				clk.advance(time.Hour)
				require.Empty(t, g.Poll())
				require.Empty(t, g.Observe(screen("> recalled")), "a re-read is no reaction")
			}
			require.Equal(t, StateOpen, g.State(), "the draft stays open and blocking")
			require.False(t, g.InjectionAllowed())
			emptyAfterText(t, g, clk, gen)
		})
	}
	t.Run("a lone ESC held, then a letter", func(t *testing.T) {
		t.Parallel()
		clk := &testClock{now: time.Now()}
		p := mustMeasured()
		g := NewGuard(clk, p)
		g.Output(screen("> "))
		clk.advance(time.Second)
		require.Empty(t, g.Poll())
		parser := input.NewParser(p)
		require.Empty(t, parser.Feed([]byte("\x1b")))
		require.True(t, parser.Pending())
		g.Hold()
		gen := g.Generation()
		require.Equal(t, StateOpen, g.State(), "the hold opened a draft")
		require.Empty(t, react(g, clk, "> restored"), "nothing closes while the ESC is held")
		clk.advance(time.Hour)
		require.Empty(t, g.Poll())

		evs := parser.Feed([]byte("a"))
		require.False(t, parser.Pending())
		require.Len(t, evs, 1)
		require.Empty(t, g.Feed(evs[0]))
		require.Empty(t, react(g, clk, "> a"), "the letter's text is no empty line")
		clk.advance(time.Hour)
		require.Empty(t, g.Poll())
		require.Equal(t, StateOpen, g.State())
		emptyAfterText(t, g, clk, gen)
	})
}
