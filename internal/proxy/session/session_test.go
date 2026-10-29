//go:build unix

package session

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

type chanSink struct {
	mu sync.Mutex
	ev []Event
}

func (c *chanSink) Emit(e Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ev = append(c.ev, e)
}

func (c *chanSink) all() []Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Event(nil), c.ev...)
}

func testProfile() profiles.Profile {
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

func testPTY(t *testing.T) (master, slave *os.File) {
	t.Helper()
	master, slave, err := pty.Open()
	require.NoError(t, err)
	require.NoError(t, pty.Setsize(slave, &pty.Winsize{Cols: 80, Rows: 24}))
	t.Cleanup(func() {
		_ = master.Close()
		_ = slave.Close()
	})
	return master, slave
}

func testOptions(dataDir, id string, in, out *os.File, sink EventSink, cmd []string) Options {
	return Options{
		Command:       cmd,
		In:            in,
		Out:           out,
		DataDir:       dataDir,
		SessionID:     id,
		Profile:       testProfile(),
		Sink:          sink,
		Notice:        io.Discard,
		ChunkMinBytes: 64,
	}
}

// runResult is a finished run.
type runResult struct {
	exit proxy.Exit
	err  error
}

func runAsync(t *testing.T, s *Session) chan runResult {
	t.Helper()
	done := make(chan runResult, 1)
	go func() {
		exit, err := s.Run(t.Context())
		done <- runResult{exit, err}
	}()
	return done
}

func waitDone(t *testing.T, done chan runResult) {
	t.Helper()
	select {
	case r := <-done:
		require.NoError(t, r.err)
	case <-time.After(30 * time.Second):
		t.Fatal("session did not end")
	}
}

// readUntil reads until marker shows up, or fails after 15 s.
func readUntil(t *testing.T, r *os.File, marker string) string {
	const timeout = 15 * time.Second
	t.Helper()
	require.NoError(t, r.SetReadDeadline(time.Now().Add(timeout)))
	var sb strings.Builder
	buf := make([]byte, 4096)
	for !strings.Contains(sb.String(), marker) {
		n, err := r.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if err != nil {
			if strings.Contains(sb.String(), marker) {
				break
			}
			require.NoError(t, err)
		}
	}
	require.NoError(t, r.SetReadDeadline(time.Time{}))
	return sb.String()
}

// waitRaw waits until the run put the outer terminal in raw mode: keys
// typed earlier would also be echoed by its line discipline.
func waitRaw(t *testing.T, tty *os.File) {
	t.Helper()
	fd := int(tty.Fd())
	require.Eventually(t, func() bool {
		tio, err := unix.IoctlGetTermios(fd, ioctlGetTermios)
		return err == nil && tio.Lflag&unix.ECHO == 0
	}, 10*time.Second, time.Millisecond, "outer terminal never went raw")
}

func writeKeys(t *testing.T, w *os.File, s string) {
	t.Helper()
	_, err := w.Write([]byte(s))
	require.NoError(t, err)
}

// echoFalse answers "got:<line>" per line and quits on __quit__.
func echoFalse() []string {
	return []string{"sh", "-c", `while IFS= read -r line; do case "$line" in __quit__) exit 0;; *) printf 'got:%s\n' "$line";; esac; done`}
}

func TestPassthroughMatchesDirect(t *testing.T) {
	t.Parallel()

	master, slave := testPTY(t)
	done := make(chan runResult, 1)
	go func() {
		exit, err := proxy.Run(t.Context(), proxy.Options{
			Command: echoFalse(),
			In:      slave,
			Out:     slave,
			Notice:  io.Discard,
		})
		done <- runResult{exit, err}
	}()
	waitRaw(t, slave)
	writeKeys(t, master, "hola\n")
	direct := readUntil(t, master, "got:hola\r\n")
	writeKeys(t, master, "__quit__\n")
	waitDone(t, done)

	master2, slave2 := testPTY(t)
	sink := &chanSink{}
	s, err := NewSession(testOptions(t.TempDir(), "passthrough", slave2, slave2, sink, echoFalse()))
	require.NoError(t, err)
	done2 := runAsync(t, s)
	waitRaw(t, slave2)
	writeKeys(t, master2, "hola\n")
	wrapped := readUntil(t, master2, "got:hola\r\n")
	writeKeys(t, master2, "__quit__\n")
	waitDone(t, done2)
	require.Equal(t, direct, wrapped)
}

func TestRecordingReplaysScreen(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	master, slave := testPTY(t)
	sink := &chanSink{}
	s, err := NewSession(testOptions(dataDir, "replay", slave, slave, sink, []string{
		"sh", "-c", `printf '\033[31mred\033[0m\nplain\n'; read -r line; exit 0`,
	}))
	require.NoError(t, err)
	done := runAsync(t, s)
	readUntil(t, master, "plain")
	writeKeys(t, master, "q\n")
	waitDone(t, done)

	var lastHash protocol.Digest
	for _, e := range sink.all() {
		if e.Type == protocol.EventTypeIOChunk {
			lastHash = e.Payload.(IOChunkEvent).ScreenHashAfter
		}
	}
	require.NotEmpty(t, lastHash)

	f, err := os.Open(filepath.Join(dataDir, recordingsDir, "replay.raw"))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	require.NoError(t, Replay(f, func(c Chunk) error {
		_, err := emu.Write(c.Data)
		return err
	}))
	require.Equal(t, lastHash, vt.Hash(emu.Snapshot()))

	info, err := f.Stat()
	require.NoError(t, err)
	require.Positive(t, info.Size())
}

func TestCanaryAbsentFromEvents(t *testing.T) {
	t.Parallel()

	const canary = "canary-9f3k7z"
	master, slave := testPTY(t)
	sink := &chanSink{}
	s, err := NewSession(testOptions(t.TempDir(), "canary", slave, slave, sink, echoFalse()))
	require.NoError(t, err)
	done := runAsync(t, s)
	writeKeys(t, master, canary+"\n")
	readUntil(t, master, "got:"+canary)
	s.Flush()
	writeKeys(t, master, "__quit__\n")
	waitDone(t, done)

	raw, err := json.Marshal(sink.all())
	require.NoError(t, err)
	require.NotContains(t, string(raw), canary)
	require.NotEmpty(t, sink.all())
}

type gateWriter struct {
	mu   sync.Mutex
	gate chan struct{}
	buf  []byte
}

func (g *gateWriter) Write(p []byte) (int, error) {
	<-g.gate
	g.mu.Lock()
	defer g.mu.Unlock()
	g.buf = append(g.buf, p...)
	return len(p), nil
}

func TestStalledRecordingNeverDelaysKeys(t *testing.T) {
	// No t.Parallel: it needs t.Setenv for the child marker.
	marker := filepath.Join(t.TempDir(), "marker")
	t.Setenv("SESSION_MARKER", marker)
	master, slave := testPTY(t)
	sink := &chanSink{}
	gate := &gateWriter{gate: make(chan struct{})}
	opts := testOptions(t.TempDir(), "stalled", slave, slave, sink, []string{
		"sh", "-c", `read -r line; echo "$line" > "$SESSION_MARKER"; read -r line2; exit 0`,
	})
	opts.RecordTo = gate
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)

	writeKeys(t, master, "ping\n")
	require.Eventually(t, func() bool {
		_, err := os.Stat(marker)
		return err == nil
	}, 15*time.Second, 10*time.Millisecond, "key did not reach the child while recording stalled")
	content, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "ping\n", string(content))

	close(gate.gate)
	writeKeys(t, master, "__quit__\n")
	waitDone(t, done)
}

func TestResizeEventsAndPasteUnknown(t *testing.T) {
	t.Parallel()

	master, slave := testPTY(t)
	sink := &chanSink{}
	s, err := NewSession(testOptions(t.TempDir(), "resize", slave, slave, sink, echoFalse()))
	require.NoError(t, err)
	done := runAsync(t, s)

	require.NoError(t, s.Resize(100, 30))
	found := false
	for _, e := range sink.all() {
		if e.Type == protocol.EventTypeInputResize {
			found = true
			require.Equal(t, 100, e.Payload.(InputResize).Cols)
			require.Equal(t, 30, e.Payload.(InputResize).Rows)
		}
	}
	require.True(t, found)

	writeKeys(t, master, "\x1b[200~abc")
	require.Eventually(t, func() bool {
		return s.State() == draft.StatePasting
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, s.Resize(90, 30))
	require.Eventually(t, func() bool {
		return s.State() == draft.StateUnknown
	}, 10*time.Second, 10*time.Millisecond)

	// The pasted line ends on its own, so the child reads __quit__ apart.
	writeKeys(t, master, "\x1b[201~\n")
	readUntil(t, master, "got:")
	writeKeys(t, master, "__quit__\n")
	waitDone(t, done)
}
