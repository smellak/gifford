//go:build unix

package keycap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/input"
)

// fakeHarness writes a harness script: with --version anywhere in argv
// it prints "fakecap 1.2.3"; otherwise it negotiates modes unless the
// mode is plain, quits at once on quit, and sinks stdin on cat so the
// tour keys never come back.
const fakeHarness = `#!/bin/sh
for a in "$@"; do
  if [ "$a" = "--version" ]; then echo "fakecap 1.2.3"; exit 0; fi
done
if [ "$1" != "plain" ] && [ "$1" != "quit" ]; then
  printf '\033[?2004h'
  printf '\033[>1u'
fi
if [ "$1" = "quit" ]; then exit 0; fi
cat > /dev/null
`

func writeFakeHarness(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fakecap")
	require.NoError(t, os.WriteFile(path, []byte(fakeHarness), 0o755))
	return path
}

// syntheticHuman types one write per step after a head start, like a
// terminal delivering whole keypresses. After a lone ESC it waits past
// the escape timeout, like a human reaching for the next key: without
// that pause the following bytes would merge into an Alt sequence,
// exactly as on a real terminal.
func syntheticHuman(w *os.File, steps []string, headStart, gap, escGap time.Duration) {
	time.Sleep(headStart)
	for _, s := range steps {
		time.Sleep(gap)
		_, _ = w.Write([]byte(s))
		if s == "\x1b" {
			time.Sleep(escGap)
		}
	}
}

func runTour(t *testing.T, id string, command []string, steps []string, outDir string) (string, string) {
	t.Helper()
	master, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = master.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 80}))

	var guide strings.Builder
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		syntheticHuman(master, steps, 300*time.Millisecond, 40*time.Millisecond, 300*time.Millisecond)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	flowErr := runFlow(ctx, flowParams{
		Harness:        id,
		Command:        command,
		In:             slave,
		Out:            slave,
		ErrW:           &guide,
		OutDir:         outDir,
		EscWait:        100 * time.Millisecond,
		PasteIdle:      150 * time.Millisecond,
		VersionTimeout: 5 * time.Second,
	})
	wg.Wait()
	if flowErr != nil {
		t.Logf("guide so far:\n%s", guide.String())
	}
	require.NoError(t, flowErr)
	raw, err := os.ReadFile(filepath.Join(outDir, id+".yaml"))
	require.NoError(t, err)
	return string(raw), guide.String()
}

var fullTour = []string{
	"\r",                                   // enviar
	"\n",                                   // salto de linea
	"\x03",                                 // cancelar
	"\x15",                                 // borrar la linea
	"\x1b",                                 // escape
	"\x03",                                 // control-c
	"\x7f",                                 // retroceso
	arrowUp, arrowDown, "\x1b[D", "\x1b[C", // flechas arriba, abajo, izquierda, derecha
	"\x1b[H", "\x1b[F", // home, end
	"\x1b[9u",                          // tab en CSI u: teclado extendido
	"\x1b[200~line1\nline2\n\x1b[201~", // pegado envuelto
}

// TestFullTourNegotiating runs the whole tour against a mode-negotiating
// fake and a synthetic human: the YAML loads with production rules,
// carries the exact measured bytes, pins the detected version, and
// replays the captures through the profile parser.
func TestFullTourNegotiating(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	content, guide := runTour(t, "testharness", []string{harness, "negotiate"}, fullTour, outDir)
	require.Contains(t, guide, "measured: true")

	prof, err := profiles.ParseFile("testharness", []byte(content))
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, "1.2.3", prof.VersionRange)
	require.True(t, prof.BracketedPaste)
	require.Equal(t, profiles.ExtendedCSIU, prof.ExtendedKeyboard)

	parser := input.NewParser(prof)
	for _, tc := range []struct {
		bytes string
		kind  input.Kind
	}{
		{"\r", input.KindSubmit},
		{"\n", input.KindInsertNewline},
		{"\x03", input.KindCancel},
		{"\x15", input.KindClearLine},
	} {
		evs := parser.Feed([]byte(tc.bytes))
		require.Len(t, evs, 1, "recorded bytes %q must replay to one event", tc.bytes)
		require.Equal(t, tc.kind, evs[0].Kind)
	}
}

// TestFullTourPlain runs the tour against a harness that negotiates
// nothing and a raw paste: xterm shapes, no bracketed paste, still
// measured with the detected version.
func TestFullTourPlain(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	steps := append([]string{}, fullTour...)
	steps[len(steps)-2] = "\t"
	steps[len(steps)-1] = "pasted\nraw\n"
	content, _ := runTour(t, "testharness", []string{harness, "plain"}, steps, outDir)

	prof, err := profiles.ParseFile("testharness", []byte(content))
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, "1.2.3", prof.VersionRange)
	require.False(t, prof.BracketedPaste)
	require.Equal(t, profiles.ExtendedNone, prof.ExtendedKeyboard)
}

// TestHarnessQuitsEarly writes a partial profile when the harness exits
// mid-tour: the file exists with measured false instead of hanging.
func TestHarnessQuitsEarly(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	master, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = master.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 80}))

	var guide strings.Builder
	require.NoError(t, runFlow(context.Background(), flowParams{
		Harness:        "testharness",
		Command:        []string{harness, "quit"},
		In:             slave,
		Out:            slave,
		ErrW:           &guide,
		OutDir:         outDir,
		EscWait:        100 * time.Millisecond,
		PasteIdle:      150 * time.Millisecond,
		VersionTimeout: 5 * time.Second,
	}))
	raw, err := os.ReadFile(filepath.Join(outDir, "testharness.yaml"))
	require.NoError(t, err)
	require.Contains(t, string(raw), "measured: false")
}
