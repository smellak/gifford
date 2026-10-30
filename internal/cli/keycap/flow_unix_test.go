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
case "$1" in
  plain|quit|burst|messy|tidy) ;;
  *) printf '\033[?2004h'; printf '\033[>1u' ;;
esac
case "$1" in
  quit) exit 0 ;;
  burst)
    dd if=/dev/zero bs=1024 count=256 2>/dev/null | tr '\000' 'x'
    printf '\033[?2004l'
    exit 0
    ;;
  messy)
    printf '\033[?2004h'; printf '\033[>1u'; printf '\033[>4;2m'
    exit 0
    ;;
  tidy)
    printf '\033[?2004h'; printf '\033[>1u'; printf '\033[>4;2m'
    printf '\033[?2004l'; printf '\033[<1u'; printf '\033[>4;0m'
    exit 0
    ;;
esac
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

// runTour drives one tour with a synthetic human and returns the
// written profile, the guide text and the flow result: success tours
// return nil, incomplete ones an error without touching the disk.
const (
	tourHarness   = "testharness"
	negotiateMode = "negotiate"
)

func runTour(t *testing.T, command []string, steps []string, outDir string) (string, string, error) {
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
		Harness:        tourHarness,
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
		return "", guide.String(), flowErr
	}
	raw, err := os.ReadFile(filepath.Join(outDir, tourHarness+".yaml"))
	require.NoError(t, err)
	return string(raw), guide.String(), nil
}

// masterReader drains the outer master in the background: with 256 KiB
// past the PTY buffer the flow only finishes while somebody reads.
type masterReader struct {
	mu  sync.Mutex
	got []byte
}

func drainMaster(master *os.File) *masterReader {
	r := &masterReader{}
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := master.Read(buf)
			if n > 0 {
				r.mu.Lock()
				r.got = append(r.got, buf[:n]...)
				r.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return r
}

func (r *masterReader) bytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]byte(nil), r.got...)
}

func (r *masterReader) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

// waitBytes polls until want bytes arrived: the teardown forwards
// everything before the flow returns, so a shortfall means loss.
func (r *masterReader) waitBytes(t *testing.T, want int) {
	t.Helper()
	for i := 0; i < 150 && r.len() < want; i++ {
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, want, r.len(), "outer master missed bytes")
}

// seedProfile writes a known valid profile to correct later.
func seedProfile(t *testing.T, outDir string) string {
	t.Helper()
	content := `harness: testharness
measured: true
version_range: "*"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^(.*)$"
`
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "testharness.yaml"), []byte(content), 0o644))
	return content
}

func seedProfileWithPlaceholder(t *testing.T, outDir string) string {
	t.Helper()
	return seedProfileWithPlaceholderVersion(t, outDir, "1.2.3")
}

func seedProfileWithPlaceholderVersion(t *testing.T, outDir, versionRange string) string {
	t.Helper()
	content := `harness: testharness
measured: true
version_range: "1.2.3"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^fz>(.*)$"
input_placeholder: "Ask Codex to do anything"
`
	content = strings.Replace(content, `version_range: "1.2.3"`, `version_range: "`+versionRange+`"`, 1)
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "testharness.yaml"), []byte(content), 0o644))
	return content
}

func completeMeasurement(version string) *measurement {
	m := &measurement{version: version}
	m.record(StepSubmit, []byte("\r"))
	m.record(StepInsertNewline, []byte("\n"))
	m.record(StepClearLine, []byte("\x15"))
	m.record(StepCancel, []byte("\x03"))
	pasteWrapped := false
	m.pasteWrapped = &pasteWrapped
	return m
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

	content, guide, flowErr := runTour(t, []string{harness, negotiateMode}, fullTour, outDir)
	require.NoError(t, flowErr)
	require.Contains(t, guide, "measured: true")

	prof, err := profiles.ParseFile("testharness", []byte(content))
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, "1.2.3", prof.VersionRange)
	require.Empty(t, prof.InputPlaceholder)
	require.NotContains(t, content, "input_placeholder:")
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

// TestFullTourAtomicallyReplacesPriorProfile starts with a valid profile and
// completes the tour against the fake harness. The final path contains one
// complete, validated replacement with the production mode, and no temp file
// survives the same-directory rename.
func TestFullTourAtomicallyReplacesPriorProfile(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()
	prior := seedProfile(t, outDir)

	content, _, flowErr := runTour(t, []string{harness, negotiateMode}, fullTour, outDir)
	require.NoError(t, flowErr)
	require.NotEqual(t, prior, content)

	path := filepath.Join(outDir, tourHarness+".yaml")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, content, string(raw))
	prof, err := profiles.ParseFile(tourHarness, raw)
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, "1.2.3", prof.VersionRange)

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, profilePerm, info.Mode().Perm())
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, tourHarness+".yaml", entries[0].Name())
}

func TestFullTourPreservesInputPlaceholder(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()
	seedProfileWithPlaceholder(t, outDir)

	content, _, flowErr := runTour(t, []string{harness, negotiateMode}, fullTour, outDir)
	require.NoError(t, flowErr)
	require.Contains(t, content, `input_placeholder: "Ask Codex to do anything"`)

	prof, err := profiles.ParseFile(tourHarness, []byte(content))
	require.NoError(t, err)
	require.Equal(t, "Ask Codex to do anything", prof.InputPlaceholder)
	require.Equal(t, "^fz>(.*)$", prof.InputLinePattern)
}

func TestFinishKeepsPlaceholderBoundToVersion(t *testing.T) {
	for _, tc := range []struct {
		name         string
		versionRange string
		detected     string
		wantErr      bool
	}{
		{name: "reject different version", versionRange: "1.2.3", detected: "1.2.4", wantErr: true},
		{name: "accept explicit equality", versionRange: "= 1.2.3", detected: "1.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outDir := t.TempDir()
			prior := seedProfileWithPlaceholderVersion(t, outDir, tc.versionRange)
			var warnings strings.Builder
			inputLine, inputPlaceholder, versionRange := readExisting(outDir, tourHarness, &warnings)
			require.Empty(t, warnings.String())

			f := &flow{
				params:           flowParams{Harness: tourHarness, OutDir: outDir},
				measure:          completeMeasurement(tc.detected),
				inputLine:        inputLine,
				inputPlaceholder: inputPlaceholder,
				versionRange:     versionRange,
				say:              func(string, ...any) {},
				observer:         &observer{},
			}
			err := f.finish("")
			path := filepath.Join(outDir, tourHarness+".yaml")
			raw, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			if tc.wantErr {
				require.ErrorContains(t, err, `belongs to version "1.2.3", detected "1.2.4"`)
				require.Equal(t, prior, string(raw), "a version mismatch must preserve the prior profile byte for byte")
				return
			}
			require.NoError(t, err)
			prof, parseErr := profiles.ParseFile(tourHarness, raw)
			require.NoError(t, parseErr)
			require.Equal(t, "Ask Codex to do anything", prof.InputPlaceholder)
			require.Equal(t, tc.detected, prof.VersionRange)
		})
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
	content, _, flowErr := runTour(t, []string{harness, "plain"}, steps, outDir)
	require.NoError(t, flowErr)

	prof, err := profiles.ParseFile("testharness", []byte(content))
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, "1.2.3", prof.VersionRange)
	require.False(t, prof.BracketedPaste)
	require.Equal(t, profiles.ExtendedNone, prof.ExtendedKeyboard)
}

// TestQuitPreservesPriorProfile exits the harness at once with a valid
// profile seeded: the run fails and the seeded bytes stay identical.
func TestQuitPreservesPriorProfile(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()
	prior := seedProfile(t, outDir)

	_, _, flowErr := runTour(t, []string{harness, "quit"}, nil, outDir)
	require.Error(t, flowErr)
	raw, err := os.ReadFile(filepath.Join(outDir, "testharness.yaml"))
	require.NoError(t, err)
	require.Equal(t, prior, string(raw))
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, tourHarness+".yaml", entries[0].Name())
}

// TestQuitWithoutPriorWritesNothing exits the harness at once with no
// profile seeded: the run fails and leaves no file behind, temp or not.
func TestQuitWithoutPriorWritesNothing(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	_, _, flowErr := runTour(t, []string{harness, "quit"}, nil, outDir)
	require.Error(t, flowErr)
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestDuplicateSequencesRejected measures the same bytes for submit and
// cancel: the candidate cannot replay both, so the run fails and the
// seeded profile stays identical.
func TestDuplicateSequencesRejected(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()
	prior := seedProfile(t, outDir)

	steps := append([]string{}, fullTour...)
	steps[2] = "\r"
	_, _, flowErr := runTour(t, []string{harness, negotiateMode}, steps, outDir)
	require.Error(t, flowErr)
	raw, err := os.ReadFile(filepath.Join(outDir, "testharness.yaml"))
	require.NoError(t, err)
	require.Equal(t, prior, string(raw))
}

// TestCoalescedStepsLandRight delivers submit and insert_newline in one
// write: the first unit fills submit, the tour advances first, and the
// rest fills insert_newline on the next step.
func TestCoalescedStepsLandRight(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	steps := append([]string{"\r\n"}, fullTour[2:]...)
	content, _, flowErr := runTour(t, []string{harness, negotiateMode}, steps, outDir)
	require.NoError(t, flowErr)
	prof, err := profiles.ParseFile("testharness", []byte(content))
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, "\r", prof.Submit)
	require.Equal(t, "\n", prof.InsertNewline)
}

// runBurstMode runs the flow with no human against a harness mode,
// draining the outer master in the background, and returns the output,
// the guide and the flow result. want is the exact byte count the
// teardown must have forwarded.
func runBurstMode(t *testing.T, harness, mode, outDir string, want int) (output, guide string, flowErr error) {
	t.Helper()
	master, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = master.Close()
		_ = slave.Close()
	})
	require.NoError(t, pty.Setsize(master, &pty.Winsize{Rows: 24, Cols: 80}))

	reader := drainMaster(master)
	var guideBuf strings.Builder
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	flowDone := make(chan error, 1)
	go func() {
		flowDone <- runFlow(ctx, flowParams{
			Harness:        "testharness",
			Command:        []string{harness, mode},
			In:             slave,
			Out:            slave,
			ErrW:           &guideBuf,
			OutDir:         outDir,
			EscWait:        100 * time.Millisecond,
			PasteIdle:      150 * time.Millisecond,
			VersionTimeout: 5 * time.Second,
		})
	}()
	select {
	case flowErr = <-flowDone:
	case <-time.After(30 * time.Second):
		t.Fatal("flow hung")
	}
	reader.waitBytes(t, want)
	return string(reader.bytes()), guideBuf.String(), flowErr
}

// TestBurstOutputDrained dumps 256 KiB past the PTY buffer from an
// exiting harness with no tour: teardown keeps forwarding until the
// core ends, so every byte arrives and nothing hangs.
func TestBurstOutputDrained(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	want := strings.Repeat("x", 256*1024) + "\x1b[?2004l"
	output, _, flowErr := runBurstMode(t, harness, "burst", outDir, len(want))
	require.Error(t, flowErr, "no tour means no success")
	require.Equal(t, want, output)
}

// TestModesRestoredWhenLeftActive leaves every negotiated mode on at
// exit: teardown switches them back on the outer terminal.
func TestModesRestoredWhenLeftActive(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	want := "\x1b[?2004h" + "\x1b[>1u" + "\x1b[>4;2m" + "\x1b[?2004l" + "\x1b[<1u" + "\x1b[>4;0m"
	output, guide, flowErr := runBurstMode(t, harness, "messy", outDir, len(want))
	require.Error(t, flowErr)
	require.Equal(t, want, output)
	require.Contains(t, guide, "restaurados")
}

// TestNoRestoreWhenHarnessCleansUp disables every mode before exit: the
// teardown adds no second reset, so each marker appears exactly once.
func TestNoRestoreWhenHarnessCleansUp(t *testing.T) {
	harness := writeFakeHarness(t)
	outDir := t.TempDir()

	want := "\x1b[?2004h" + "\x1b[>1u" + "\x1b[>4;2m" + "\x1b[?2004l" + "\x1b[<1u" + "\x1b[>4;0m"
	output, _, flowErr := runBurstMode(t, harness, "tidy", outDir, len(want))
	require.Error(t, flowErr)
	require.Equal(t, want, output)
}
