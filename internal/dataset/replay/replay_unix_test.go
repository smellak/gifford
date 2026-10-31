//go:build unix

package replay

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

const canary = "canary-5q8w2x"

func readUntil(t *testing.T, r *os.File, marker string) {
	t.Helper()
	require.NoError(t, r.SetReadDeadline(time.Now().Add(15*time.Second)))
	var sb strings.Builder
	buf := make([]byte, 4096)
	for !strings.Contains(sb.String(), marker) {
		n, err := r.Read(buf)
		sb.Write(buf[:n])
		if err != nil && !strings.Contains(sb.String(), marker) {
			require.NoError(t, err)
		}
	}
	require.NoError(t, r.SetReadDeadline(time.Time{}))
}

// TestLiveSessionReplays runs a fake harness through the session with
// the events file gifford run writes, then replays the pair: pty.opened
// first, sequences in order, 0600, no typed canary anywhere, the screen
// the live vt left, and a resize mid-run reaching the windows after it.
func TestLiveSessionReplays(t *testing.T) {
	t.Parallel()

	data := t.TempDir()
	const id = "live-1"
	master, slave, err := pty.Open()
	require.NoError(t, err)
	t.Cleanup(func() { _ = master.Close(); _ = slave.Close() })
	require.NoError(t, pty.Setsize(slave, &pty.Winsize{Cols: 80, Rows: 24}))
	ps, err := profiles.Load()
	require.NoError(t, err)
	// Replay exercises a stable unmeasured keyboard fixture, not production
	// version selection. Keep its recorded harness identity explicitly Codex.
	profile, err := ps.Profile("generic", "1.0")
	require.NoError(t, err)
	profile.Harness = "codex"

	log, err := session.CreateEventLog(data, id, session.EventLogOptions{})
	require.NoError(t, err)
	s, err := session.NewSession(session.Options{
		// The harness does not echo what it reads: what is typed reaches
		// the screen only if a harness prints it.
		Command: []string{"sh", "-c", `stty -echo; printf 'ready\n'; read -r a; printf 'len %d\n' "${#a}"; read -r b; sleep 0.3; printf 'after resize\n'; sleep 0.3; exit 0`},
		In:      slave, Out: slave, DataDir: data, SessionID: id, Profile: profile,
		Sink: log, Notice: io.Discard, ChunkMinBytes: 64,
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { _, err := s.Run(t.Context()); done <- err }()

	readUntil(t, master, "ready")
	require.Eventually(t, func() bool {
		tio, err := unix.IoctlGetTermios(int(slave.Fd()), ioctlGetTermios)
		return err == nil && tio.Lflag&unix.ECHO == 0
	}, 10*time.Second, time.Millisecond)
	_, err = master.Write([]byte(canary + "\n"))
	require.NoError(t, err)
	readUntil(t, master, "len ")
	time.Sleep(150 * time.Millisecond)
	require.NoError(t, s.Resize(100, 30))
	_, err = master.Write([]byte("go\n"))
	require.NoError(t, err)
	readUntil(t, master, "after resize")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("session did not end")
	}
	require.NoError(t, log.Close())

	dir := session.RecordingsDir(data)
	for _, name := range []string{id + session.EventsSuffix, id + session.RecordingSuffix} {
		fi, err := os.Stat(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), name)
		raw, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.NotContains(t, string(raw), canary, name)
	}

	f, err := os.Open(filepath.Join(dir, id+session.EventsSuffix))
	require.NoError(t, err)
	evs, err := session.ReadEventLog(f, id) // in order, no gap, pty.opened first
	require.NoError(t, f.Close())
	require.NoError(t, err)
	require.Equal(t, protocol.EventTypePTYOpened, evs[0].Type)
	require.JSONEq(t, `{"cols":80,"rows":24,"harness":"codex"}`, string(evs[0].Payload))
	require.Equal(t, protocol.EventTypePTYClosed, evs[len(evs)-1].Type)

	// The last io.chunk names the hash the live screen had; replaying the
	// recording in event order rebuilds the same screen.
	var lastHash protocol.Digest
	for _, e := range evs {
		if e.Type == protocol.EventTypeIOChunk {
			var p session.IOChunkEvent
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			lastHash = p.ScreenHashAfter
		}
	}
	rawFile, err := os.Open(filepath.Join(dir, id+session.RecordingSuffix))
	require.NoError(t, err)
	defer func() { _ = rawFile.Close() }()
	br := bufio.NewReader(rawFile)
	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	for _, e := range evs {
		switch e.Type {
		case protocol.EventTypeIOChunk:
			c, err := session.ReadChunk(br)
			require.NoError(t, err)
			_, err = emu.Write(c.Data)
			require.NoError(t, err)
		case protocol.EventTypeInputResize:
			var p session.InputResize
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			require.NoError(t, emu.Resize(p.Cols, p.Rows))
		default:
		}
	}
	require.Equal(t, lastHash, vt.Hash(emu.Snapshot()))

	ws := windowsOf(t, data, id, 50*time.Millisecond)
	require.NotEmpty(t, ws)
	sawSmall, sawLarge := false, false
	for _, w := range ws {
		require.Equal(t, protocol.HarnessCodex, w.Features.Harness, "the harness the run chose")
		require.NotContains(t, strings.Join(w.Screen.Rows, "\n"), canary)
		switch w.Screen.Cols {
		case 80:
			require.False(t, sawLarge, "an 80-column window after the resize")
			require.Len(t, w.Screen.Rows, 24)
			sawSmall = true
		case 100:
			require.Len(t, w.Screen.Rows, 30)
			sawLarge = true
		default:
			t.Fatalf("unexpected width %d", w.Screen.Cols)
		}
	}
	require.True(t, sawSmall && sawLarge, "windows on both sides of the resize")
	require.Contains(t, strings.Join(ws[len(ws)-1].Screen.Rows, "\n"), "after resize")
}
