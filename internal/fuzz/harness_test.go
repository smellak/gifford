//go:build unix

package fuzz

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
)

// startHarness runs the fake harness on a new terminal, recording to
// record, and returns the terminal's other end and the screen on it.
func startHarness(t *testing.T, record string) (*exec.Cmd, *os.File, *screen) {
	t.Helper()
	h := testHarness(t, harnessArg)
	cmd := exec.CommandContext(t.Context(), h[0], h[1], record)
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: cols, Rows: rows})
	require.NoError(t, err)
	t.Cleanup(func() { _ = master.Close() })
	scr, err := newScreen()
	require.NoError(t, err)
	go scr.read(master)
	require.True(t, scr.showsDraft(t.Context(), "", 10*time.Second), "the harness never drew its prompt")
	return cmd, master, scr
}

// TestHarnessRecordsEveryLine types on the fake harness: it draws the
// draft as it goes, records each line it gets, and Ctrl-D ends it
// cleanly.
func TestHarnessRecordsEveryLine(t *testing.T) {
	t.Parallel()
	record := filepath.Join(t.TempDir(), recordFile)
	cmd, master, scr := startHarness(t, record)
	_, err := master.WriteString("hello wörld\x7f\x7fld")
	require.NoError(t, err)
	require.True(t, scr.showsDraft(t.Context(), "hello wörld", 10*time.Second), "the draft shows as it is typed")
	_, err = master.WriteString("\r\x1b[200~pas\rted\x1b[201~\x03gone\x15\r" + theEnd)
	require.NoError(t, err)
	require.NoError(t, cmd.Wait())
	raw, err := os.ReadFile(record)
	require.NoError(t, err)
	got, err := ParseRecords(raw)
	require.NoError(t, err)
	require.Equal(t, []Record{
		{RecordLine, "hello wörld"},
		{RecordCancel, "pas\rted"},
		{RecordClear, "gone"},
		{RecordLine, ""},
	}, got)
}

// TestHarnessNeedsCtrlD fails a harness whose terminal closes before
// Ctrl-D, and one whose record file already exists.
func TestHarnessNeedsCtrlD(t *testing.T) {
	t.Parallel()
	record := filepath.Join(t.TempDir(), recordFile)
	cmd, master, _ := startHarness(t, record)
	_, err := master.WriteString("half a line")
	require.NoError(t, err)
	require.NoError(t, cmd.Process.Signal(os.Interrupt))
	require.Error(t, cmd.Wait(), "an interrupted harness does not exit cleanly")

	h := testHarness(t, harnessArg)
	out, err := exec.CommandContext(t.Context(), h[0], h[1], record).CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "fuzz harness: record")

	out, err = exec.CommandContext(t.Context(), h[0], h[1], filepath.Join(t.TempDir(), recordFile)).CombinedOutput()
	require.Error(t, err, "stdin is not a terminal")
	require.Contains(t, string(out), "fuzz harness: raw terminal")
}
