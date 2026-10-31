package draft

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/input"
	"github.com/smellak/gifford/internal/vt"
)

// ambiguousKey is a human key a guard with no draft open cannot tell
// typed nothing, as a terminal sends it: chunks are the writes, and csiU
// says the profile asked for CSI u.
type ambiguousKey struct {
	name   string
	chunks []string
	csiU   bool
	kind   input.Kind
}

// ambiguousKeys are a letter after a lone ESC, which the parser joins
// into Alt+letter with no timer while a harness may read the two apart,
// a letter in CSI u, the arrows up and down, a Tab, and a sequence the
// parser does not recognise.
var ambiguousKeys = []ambiguousKey{
	{name: "a letter after a lone ESC", chunks: []string{"\x1b", "a"}, kind: input.KindKey},
	{name: "a letter in CSI u", chunks: []string{"\x1b[97u"}, csiU: true, kind: input.KindKey},
	{name: "arrow up", chunks: []string{"\x1b[A"}, kind: input.KindKey},
	{name: "arrow down", chunks: []string{"\x1b[B"}, kind: input.KindKey},
	{name: "a Tab", chunks: []string{"\t"}, kind: input.KindKey},
	{name: "an unknown sequence", chunks: []string{"\x1b[5x"}, kind: input.KindUnknown},
}

// keyProfile is the measured profile, asking for CSI u when the key
// needs it.
func keyProfile(t *testing.T, k ambiguousKey) profiles.Profile {
	t.Helper()
	y := measuredYAML()
	if k.csiU {
		y = strings.Replace(y, "extended_keyboard: none", "extended_keyboard: csi_u", 1)
	}
	p, err := profiles.ParseFile("synth", []byte(y))
	require.NoError(t, err)
	return p
}

// typed runs k's chunks through p's parser and returns the one event
// they make once the parser holds nothing back.
func typed(t *testing.T, p profiles.Profile, k ambiguousKey) input.Event {
	t.Helper()
	parser := input.NewParser(p)
	evs := make([]input.Event, 0, len(k.chunks))
	for _, c := range k.chunks {
		evs = append(evs, parser.Feed([]byte(c))...)
	}
	require.False(t, parser.Pending())
	require.Len(t, evs, 1)
	require.Equal(t, k.kind, evs[0].Kind)
	return evs[0]
}

// pressAmbiguous closes a guard on an empty input line, then presses k:
// the key opens a draft at once under a fresh generation and ends any
// permit, and neither the clock nor a re-read of the empty line from
// before it closes the draft. It returns the draft's generation.
func pressAmbiguous(t *testing.T, k ambiguousKey) (*Guard, *testClock, uint64) {
	t.Helper()
	clk := &testClock{now: time.Now()}
	p := keyProfile(t, k)
	g := NewGuard(clk, p)
	g.Output(screen("> "))
	clk.advance(time.Second)
	require.Empty(t, g.Poll())
	require.Equal(t, StateClosed, g.State())
	gen, ver := g.Generation(), g.GuardVersion()

	require.Empty(t, g.Feed(typed(t, p, k)))
	require.Equal(t, StateOpen, g.State(), "the key may have left text: no longer closed")
	require.False(t, g.InjectionAllowed())
	require.Equal(t, gen+1, g.Generation(), "a fresh draft")
	require.Greater(t, g.GuardVersion(), ver, "the key ends any permit at once")

	clk.advance(time.Hour)
	require.Empty(t, g.Poll(), "no output since the key: the clock alone closes nothing")
	require.Empty(t, g.Observe(screen("> ")), "a re-read is no reaction")
	require.Equal(t, StateOpen, g.State())
	return g, clk, gen + 1
}

// TestAmbiguousKeyOpensTheDraft presses, with no draft open, each key
// the guard cannot tell typed nothing. Text the harness draws on the
// input line after it keeps the draft open, however long it stays and
// however often it is read again, and so do an empty line redrawn with
// text within the quiet period and an empty line with bytes held back.
// Nor does a typed key and a backspace that seems to undo it: the count
// cannot tell what the key itself typed. Only an empty line drawn after
// the key, once quiet for reactionQuiet, closes the draft, as cleared,
// once.
func TestAmbiguousKeyOpensTheDraft(t *testing.T) {
	t.Parallel()
	pending := screen("> ")
	pending.Pending = true
	for _, k := range ambiguousKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			recalled := screen("> recalled")
			for _, tc := range []struct {
				name  string
				react func(g *Guard, clk *testClock)
				// last is the screen the reaction leaves.
				last vt.Snapshot
			}{
				{"text on the input line", func(g *Guard, _ *testClock) { g.Output(recalled) }, recalled},
				{"an empty line, then text", func(g *Guard, clk *testClock) {
					g.Output(screen("> "))
					clk.advance(reactionQuiet / 3)
					require.Empty(t, g.Poll())
					g.Output(recalled)
				}, recalled},
				{"an empty line with bytes held back", func(g *Guard, _ *testClock) { g.Output(pending) }, pending},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					g, clk, _ := pressAmbiguous(t, k)
					tc.react(g, clk)
					clk.advance(reactionQuiet)
					require.Empty(t, g.Poll())
					clk.advance(time.Hour)
					require.Empty(t, g.Poll())
					require.Empty(t, g.Observe(tc.last), "a re-read is no reaction")
					require.Equal(t, StateOpen, g.State(), "the draft stays open and blocking")
					require.False(t, g.InjectionAllowed())
				})
			}

			t.Run("a key and a backspace after it", func(t *testing.T) {
				t.Parallel()
				g, clk, _ := pressAmbiguous(t, k)
				g.Feed(txt("x"))
				g.Feed(key(input.KeyBackspace))
				require.Empty(t, react(g, clk, "> "), "the count cannot tell what the key typed")
				clk.advance(time.Hour)
				require.Empty(t, g.Poll())
				require.Equal(t, StateOpen, g.State())
			})

			t.Run("an empty input line", func(t *testing.T) {
				t.Parallel()
				g, clk, gen := pressAmbiguous(t, k)
				g.Output(screen("> "))
				clk.advance(reactionQuiet - time.Millisecond)
				require.Empty(t, g.Poll(), "not quiet long enough")
				clk.advance(time.Millisecond)
				require.Equal(t, []Outcome{{Kind: OutcomeCleared, Generation: gen}}, g.Poll())
				require.Equal(t, StateClosed, g.State())
				require.True(t, g.InjectionAllowed())
				clk.advance(time.Hour)
				require.Empty(t, g.Poll(), "one outcome")
			})
		})
	}
}

// TestAmbiguousKeyOnAnOpenDraft presses the same keys on an open draft:
// it stays open under its generation, and no longer clean, so
// backspaces that seem to empty it and an empty input line after them
// close nothing. A letter after a lone ESC reaches the guard as one key
// the count misses, which is why the draft must not stay clean.
func TestAmbiguousKeyOnAnOpenDraft(t *testing.T) {
	t.Parallel()
	for _, k := range ambiguousKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			clk := &testClock{now: time.Now()}
			p := keyProfile(t, k)
			g := NewGuard(clk, p)
			g.Feed(txt("h"))
			g.Feed(txt("i"))
			gen := g.Generation()
			require.Empty(t, g.Feed(typed(t, p, k)))
			require.Equal(t, StateOpen, g.State())
			require.Equal(t, gen, g.Generation())
			g.Feed(key(input.KeyBackspace))
			g.Feed(key(input.KeyBackspace))
			require.Empty(t, react(g, clk, "> "))
			clk.advance(time.Hour)
			require.Empty(t, g.Poll())
			require.Equal(t, StateOpen, g.State())
			require.False(t, g.InjectionAllowed())
		})
	}
}

// TestAmbiguousKeyWithoutEvidenceIsUnknown presses the same keys, with
// no draft open, on profiles that cannot show the input line: one not
// measured, and a measured one with no input line. No screen could
// close the draft such a key may have left, so the guard goes UNKNOWN:
// no output or quiet period closes it, only an explicit Unlock.
func TestAmbiguousKeyWithoutEvidenceIsUnknown(t *testing.T) {
	t.Parallel()
	for _, k := range ambiguousKeys {
		for _, tc := range []struct {
			name string
			edit func(*profiles.Profile)
		}{
			{"not measured", func(p *profiles.Profile) { p.Measured = false }},
			{"no input line", func(p *profiles.Profile) { p.InputLinePattern, p.InputLine = "", nil }},
		} {
			t.Run(k.name+", "+tc.name, func(t *testing.T) {
				t.Parallel()
				clk := &testClock{now: time.Now()}
				p := keyProfile(t, k)
				tc.edit(&p)
				g := NewGuard(clk, p)
				ev := typed(t, p, k)
				ver := g.GuardVersion()
				require.Empty(t, g.Feed(ev))
				require.Equal(t, StateUnknown, g.State())
				require.False(t, g.InjectionAllowed())
				require.Greater(t, g.GuardVersion(), ver)
				require.Empty(t, react(g, clk, "> "))
				clk.advance(time.Hour)
				require.Empty(t, g.Poll())
				require.Equal(t, StateUnknown, g.State())
				g.Unlock()
				require.Equal(t, StateClosed, g.State())
			})
		}
	}
}

// holdThen runs chunks through p's parser as the session does: the guard
// holds while the parser holds bytes back, and takes each event it
// gives.
func holdThen(t *testing.T, g *Guard, p profiles.Profile, chunks ...string) {
	t.Helper()
	parser := input.NewParser(p)
	for _, c := range chunks {
		for _, ev := range parser.Feed([]byte(c)) {
			require.Empty(t, g.Feed(ev))
		}
		if parser.Pending() {
			g.Hold()
		}
	}
}

// TestHoldOpensTheDraft holds part of a key back with no draft open, a
// lone ESC: the draft opens at once under a fresh generation, and while
// the part is held nothing closes it, an empty line quiet for an hour
// included, and a further hold moves nothing. The key the next part
// completes waits anew: the empty line from before it closes nothing,
// one drawn after it closes the draft, as cleared, under the generation
// the hold opened.
func TestHoldOpensTheDraft(t *testing.T) {
	t.Parallel()
	clk := &testClock{now: time.Now()}
	p := mustMeasured()
	g := NewGuard(clk, p)
	g.Output(screen("> "))
	clk.advance(time.Second)
	gen, ver := g.Generation(), g.GuardVersion()

	parser := input.NewParser(p)
	require.Empty(t, parser.Feed([]byte("\x1b")))
	require.True(t, parser.Pending())
	g.Hold()
	require.Equal(t, StateOpen, g.State(), "the harness has the ESC: no longer closed")
	require.False(t, g.InjectionAllowed())
	require.Equal(t, gen+1, g.Generation())
	require.Greater(t, g.GuardVersion(), ver)
	require.Empty(t, react(g, clk, "> "))
	clk.advance(time.Hour)
	require.Empty(t, g.Poll(), "nothing closes while a part is held")
	ver = g.GuardVersion()
	g.Hold()
	require.Equal(t, ver, g.GuardVersion(), "a hold on an open draft moves nothing")

	evs := parser.Feed([]byte("a"))
	require.Len(t, evs, 1)
	require.Empty(t, g.Feed(evs[0]))
	require.Greater(t, g.GuardVersion(), ver, "the whole key waits anew")
	clk.advance(time.Hour)
	require.Empty(t, g.Poll(), "the empty line came before the key")
	require.Empty(t, g.Observe(screen("> ")))
	require.Equal(t, []Outcome{{Kind: OutcomeCleared, Generation: gen + 1}}, react(g, clk, "> "))
	require.True(t, g.InjectionAllowed())
}

// TestHoldThenTextIsADraft holds part of a key back with no draft open,
// and the rest makes it text: a rune cut in two, a paste whose start
// marker was cut. The draft holds what was typed, so an empty line
// drawn after it closes nothing, and neither does one drawn after an
// arrow: the draft is no longer a doubt.
func TestHoldThenTextIsADraft(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		chunks []string
	}{
		{"a rune cut in two", []string{"\xc3", "\xa9"}},
		{"a paste start cut in two", []string{"\x1b[20", "0~pasted\x1b[201~"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clk := &testClock{now: time.Now()}
			p := mustMeasured()
			g := NewGuard(clk, p)
			holdThen(t, g, p, tc.chunks...)
			require.Equal(t, StateOpen, g.State())
			require.Empty(t, react(g, clk, "> "))
			clk.advance(time.Hour)
			require.Empty(t, g.Poll())
			require.Equal(t, StateOpen, g.State(), "what was typed is a draft")
			holdThen(t, g, p, "\x1b[A")
			require.Empty(t, react(g, clk, "> "))
			clk.advance(time.Hour)
			require.Empty(t, g.Poll())
			require.Equal(t, StateOpen, g.State())
		})
	}
}

// TestHoldWhileASubmitWaits holds a lone ESC while a submit waits for
// its reaction: an empty line drawn while the ESC is held closes
// nothing, however long it is quiet, and the key the ESC becomes
// reopens the draft, as any key after a submit does.
func TestHoldWhileASubmitWaits(t *testing.T) {
	t.Parallel()
	clk := &testClock{now: time.Now()}
	p := mustMeasured()
	g := NewGuard(clk, p)
	g.Feed(txt("h"))
	g.Feed(ev(input.KindSubmit))
	parser := input.NewParser(p)
	require.Empty(t, parser.Feed([]byte("\x1b")))
	ver := g.GuardVersion()
	g.Hold()
	require.Equal(t, StateSubmitting, g.State(), "a hold on a waiting submit moves nothing")
	require.Equal(t, ver, g.GuardVersion())
	require.Empty(t, react(g, clk, "> "))
	clk.advance(time.Hour)
	require.Empty(t, g.Poll(), "nothing closes while a part is held")
	evs := parser.Feed([]byte("x"))
	require.Len(t, evs, 1)
	require.Empty(t, g.Feed(evs[0]))
	require.Equal(t, StateOpen, g.State())
	require.Equal(t, uint64(2), g.Generation())
}

// TestHoldWithoutEvidenceIsUnknown holds a lone ESC with no draft open
// on profiles that cannot show the input line: the guard goes UNKNOWN,
// and only an explicit Unlock leaves it.
func TestHoldWithoutEvidenceIsUnknown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		edit func(*profiles.Profile)
	}{
		{"not measured", func(p *profiles.Profile) { p.Measured = false }},
		{"no input line", func(p *profiles.Profile) { p.InputLinePattern, p.InputLine = "", nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clk := &testClock{now: time.Now()}
			p := mustMeasured()
			tc.edit(&p)
			g := NewGuard(clk, p)
			g.Hold()
			require.Equal(t, StateUnknown, g.State())
			require.Empty(t, react(g, clk, "> "))
			g.Unlock()
			require.Equal(t, StateClosed, g.State())
		})
	}
}
