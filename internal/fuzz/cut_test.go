//go:build unix

package fuzz

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// Kinds of key sequence a move can fall inside.
const (
	cutLoneEscape = "a lone escape"
	cutEscape     = "an escape sequence"
	cutRune       = "a rune"
	cutMarker     = "a paste start marker"
)

// cutKind names the key sequence step i of sc is the first piece of,
// or "" when it is none.
func cutKind(sc Script, i int) string {
	st := sc.Steps[i]
	firstPaste := st.Kind == StepPaste && (i == 0 || sc.Steps[i-1].Kind != StepPaste)
	switch {
	case st.Kind == StepEscape && st.Keys == "\x1b":
		return cutLoneEscape
	case st.Kind == StepEscape && !slices.Contains(escapes, st.Keys) && strings.HasPrefix(st.Keys, "\x1b"):
		return cutEscape
	case st.Kind == StepType && !utf8.ValidString(st.Keys) && !utf8.FullRuneInString(st.Keys[len(st.Keys)-1:]):
		return cutRune
	case firstPaste && len(st.Keys) < len(pasteStart) && strings.HasPrefix(pasteStart, st.Keys):
		return cutMarker
	}
	return ""
}

// cutRegression is a scenario that failed when the session delivered
// while its parser held part of a human key sequence (#96), and the cut
// a notice joined there.
type cutRegression struct {
	seed  uint64
	index int
	kind  string
}

// cutRegressions were found with that hold taken out of the session.
// Each script still holds its cut, so a change to the generator that
// draws other scripts shows here.
var cutRegressions = []cutRegression{
	{seed: 1031, index: 103, kind: cutEscape},      // an up arrow, cut after "\x1b["
	{seed: 1031, index: 1467, kind: cutRune},       // ü, cut after its first byte
	{seed: 1031, index: 1850, kind: cutLoneEscape}, // Alt+x, cut after the escape
	{seed: 1032, index: 1734, kind: cutMarker},     // a paste start, cut after "\x1b[", a typed notice
	{seed: 1033, index: 845, kind: cutMarker},      // a paste start, cut after "\x1b[2", a pasted notice
}

// cutRepeats runs each regression this many times: whether a delivery
// lands inside the cut depends on the scheduler too.
const cutRepeats = 4

// TestCutKeysHoldEveryNotice runs, through a real session, the
// scenarios of a fixed seed that failed when the session did not hold a
// delivery until a human key sequence cut in two was whole: a notice
// arrived while the human had written only its first piece. None fails
// now: no line is mixed, no key lost.
func TestCutKeysHoldEveryNotice(t *testing.T) {
	t.Parallel()
	scripts := make([]Script, 0, len(cutRegressions)*cutRepeats)
	seeds := make([]uint64, 0, len(cutRegressions)*cutRepeats)
	for _, c := range cutRegressions {
		sc := NewScript(c.seed, c.index)
		kinds := make([]string, 0, len(sc.Steps))
		for i := range sc.Steps {
			kinds = append(kinds, cutKind(sc, i))
		}
		require.Contains(t, kinds, c.kind, "seed %d scenario %d cuts %s", c.seed, c.index, c.kind)
		for range cutRepeats {
			scripts = append(scripts, sc)
			seeds = append(seeds, c.seed)
		}
	}
	cfg := config{dir: t.TempDir(), harness: testHarness(t, harnessArg), timeout: DefaultTimeout}
	results := make([]result, len(scripts))
	var wg sync.WaitGroup
	for i, sc := range scripts {
		wg.Go(func() {
			c := cfg
			c.dir = filepath.Join(cfg.dir, strconv.Itoa(i))
			if err := os.Mkdir(c.dir, 0o700); err != nil {
				results[i] = result{index: sc.Index, err: err}
				return
			}
			results[i] = runScenario(t.Context(), c, sc, false)
		})
	}
	wg.Wait()
	for i, r := range results {
		require.False(t, r.failed(false), "seed %d scenario %d: %v", seeds[i], r.index, r.reasons())
	}
}
