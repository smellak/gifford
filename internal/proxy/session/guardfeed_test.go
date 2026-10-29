//go:build unix

package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/proxy/input"
)

// stepClock is a clock the test moves by hand: the guard's activity
// changes only when the test says so.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// guardRun is one live session on a step clock.
type guardRun struct {
	session *Session
	clock   *stepClock
	master  *os.File
	sink    *chanSink
	done    chan runResult
}

// startGuardSession runs one session on a step clock. Its harness reads
// every byte as plain input, a cancel or a clear line included, and
// ends on __done__.
func startGuardSession(t *testing.T, id string) *guardRun {
	t.Helper()
	master, slave := testPTY(t)
	sink := &chanSink{}
	clk := &stepClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	opts := testOptions(t.TempDir(), id, slave, slave, sink, []string{
		"sh", "-c",
		`stty icrnl -echo -isig; while IFS= read -r line; do case "$line" in *__done__*) exit 0;; esac; done`,
	})
	opts.Clock = clk
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	return &guardRun{session: s, clock: clk, master: master, sink: sink, done: done}
}

// activity reads the guard's activity race-clean. Only Guard.Feed
// records a key, so on a step clock an active guard has been fed since
// the clock last moved past the activity window.
func activity(s *Session) draft.Activity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.guard.Activity()
}

// waitFed waits until the guard has been fed.
func waitFed(t *testing.T, s *Session, what string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return activity(s) == draft.ActivityActive
	}, 10*time.Second, time.Millisecond, what)
}

// quiet moves the step clock past the activity window, so the next key
// that reaches the guard shows.
func (r *guardRun) quiet(t *testing.T) {
	t.Helper()
	r.clock.advance(2 * time.Second)
	require.NotEqual(t, draft.ActivityActive, activity(r.session))
}

// TestPasteTextReachesTheGuard pastes text: every pasted rune reaches the
// guard, which stays PASTING, with no new version, until the paste ends.
// The paste size is still the pasted bytes.
func TestPasteTextReachesTheGuard(t *testing.T) {
	t.Parallel()

	r := startGuardSession(t, "paste-text")
	writeKeys(t, r.master, "\x1b[200~")
	waitSnapshot(t, r.session, "paste never started", func(snap guardSnapshot) bool {
		return snap.State == draft.StatePasting
	})
	started := publishedSnapshot(t, r.session)
	r.quiet(t)

	writeKeys(t, r.master, "abc")
	waitFed(t, r.session, "pasted text never reached the guard")
	snap := publishedSnapshot(t, r.session)
	require.Equal(t, draft.StatePasting, snap.State, "text keeps the paste open")
	require.Equal(t, started.Generation, snap.Generation)
	require.Equal(t, started.Version, snap.Version, "pasted text moves no version")

	writeKeys(t, r.master, "\x1b[201~")
	waitSnapshot(t, r.session, "paste never ended", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen
	})
	require.Equal(t, started.Generation, publishedSnapshot(t, r.session).Generation)
	completed := sinkPayloads(r.sink, protocol.EventTypeInputPasteCompleted)
	require.Len(t, completed, 1)
	require.Equal(t, int64(3), completed[0].(InputPaste).SizeBytes)

	endDeliverySession(t, r.master, r.done)
}

// guardFeedCases are the bytes that make the codex parser emit one
// event of each input kind, after setup bytes when the kind needs a
// state first.
var guardFeedCases = []struct {
	kind         input.Kind
	setup, probe string
}{
	{kind: input.KindText, probe: "a"},
	{kind: input.KindSubmit, probe: "\r"},
	{kind: input.KindInsertNewline, probe: "\n"},
	{kind: input.KindClearLine, probe: "\x15"},
	{kind: input.KindCancel, probe: "\x03"},
	{kind: input.KindPasteStart, probe: "\x1b[200~"},
	{kind: input.KindPasteEnd, setup: "\x1b[200~", probe: "\x1b[201~"},
	{kind: input.KindKey, probe: "\x1b[A"},
	{kind: input.KindUnknown, probe: "\a"},
}

// inputKinds lists the values of every input.Kind constant the input
// package declares, from its source.
func inputKinds(t *testing.T) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join("..", "input", "*.go"))
	require.NoError(t, err)
	var kinds []string
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				if typ, ok := vs.Type.(*ast.Ident); !ok || typ.Name != "Kind" {
					continue
				}
				for _, v := range vs.Values {
					lit, ok := v.(*ast.BasicLit)
					require.True(t, ok, "%s: a Kind that is not a literal", name)
					kind, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					kinds = append(kinds, kind)
				}
			}
		}
	}
	require.NotEmpty(t, kinds)
	return kinds
}

// TestEveryInputKindReachesTheGuard types one event of every input kind
// into a real session: each one reaches Guard.Feed. The table must name
// every kind the input package declares, so a new one cannot be left
// out of the guard.
func TestEveryInputKindReachesTheGuard(t *testing.T) {
	t.Parallel()

	covered := make([]string, 0, len(guardFeedCases))
	for _, c := range guardFeedCases {
		covered = append(covered, string(c.kind))
	}
	require.ElementsMatch(t, inputKinds(t), covered, "every input kind has a case")

	for _, c := range guardFeedCases {
		t.Run(string(c.kind), func(t *testing.T) {
			t.Parallel()

			p := input.NewParser(testProfile())
			p.Feed([]byte(c.setup))
			evs := p.Feed([]byte(c.probe))
			require.Len(t, evs, 1, "the probe is one event")
			require.Equal(t, c.kind, evs[0].Kind, "the probe is the case's kind")

			r := startGuardSession(t, "feed-"+string(c.kind))
			require.NotEqual(t, draft.ActivityActive, activity(r.session), "no key yet")
			if c.setup != "" {
				writeKeys(t, r.master, c.setup)
				waitFed(t, r.session, "the setup never reached the guard")
				r.quiet(t)
			}
			writeKeys(t, r.master, c.probe)
			waitFed(t, r.session, string(c.kind)+" never reached the guard")

			endDeliverySession(t, r.master, r.done)
		})
	}
}
