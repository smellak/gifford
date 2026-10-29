//go:build unix

package session

import (
	"bytes"
	"encoding/json"
	"errors"
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

// TestPTYOpenedCarriesHarnessVersion runs one session per version: a
// measured version lands in the first emitted event, and without one
// the payload carries no version.
func TestPTYOpenedCarriesHarnessVersion(t *testing.T) {
	t.Parallel()

	emitted := func(t *testing.T, id, version string) PTYOpened {
		t.Helper()
		_, slave := testPTY(t)
		sink := &chanSink{}
		opts := testOptions(t.TempDir(), id, slave, slave, sink, []string{"true"})
		opts.HarnessVersion = version
		s, err := NewSession(opts)
		require.NoError(t, err)
		waitDone(t, runAsync(t, s))
		evs := sink.all()
		require.NotEmpty(t, evs)
		require.Equal(t, protocol.EventTypePTYOpened, evs[0].Type)
		opened, ok := evs[0].Payload.(PTYOpened)
		require.True(t, ok, "first event carries PTYOpened")
		return opened
	}
	require.Equal(t, "9.9.9", emitted(t, "ver-with", "9.9.9").HarnessVersion)
	require.Empty(t, emitted(t, "ver-without", "").HarnessVersion)
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

// failWriter is a recording that always fails, like a disk that went
// read-only mid-session.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("no space left") }

// TestFailingRecorderDoesNotHang folds output through a broken recording:
// onOutput and Flush must return instead of spinning on the same bytes,
// the failure is kept, and nothing is emitted for bytes never recorded.
func TestFailingRecorderDoesNotHang(t *testing.T) {
	t.Parallel()

	_, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), "failrec", slave, slave, sink, echoFalse())
	opts.RecordTo = failWriter{}
	s, err := NewSession(opts)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.onOutput([]byte(strings.Repeat("x", 256)))
		s.Flush()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("output fold hung on a failing recorder")
	}
	s.mu.Lock()
	fatal := s.fatalErr
	s.mu.Unlock()
	require.Error(t, fatal)
	for _, e := range sink.all() {
		require.NotEqual(t, protocol.EventTypeIOChunk, e.Type)
	}
}

// TestChunkHashAfterOwnBytes feeds more than two ChunkMaxBytes in one
// piece and replays the recording incrementally: every io.chunk event
// must carry the screen hash after its own bytes, not the final screen.
func TestChunkHashAfterOwnBytes(t *testing.T) {
	t.Parallel()

	_, slave := testPTY(t)
	sink := &chanSink{}
	var rec bytes.Buffer
	opts := testOptions(t.TempDir(), "fraghash", slave, slave, sink, echoFalse())
	opts.RecordTo = &rec
	opts.ChunkMinBytes = 64
	opts.ChunkMaxBytes = 1024
	s, err := NewSession(opts)
	require.NoError(t, err)

	var sb strings.Builder
	for i := range 200 {
		sb.WriteString("\x1b[3")
		sb.WriteByte('1' + byte(i%6))
		sb.WriteString("mline-")
		sb.WriteString(strings.Repeat("x", 20))
		sb.WriteString("\x1b[0m\n")
	}
	data := sb.String()
	require.Greater(t, len(data), 3*opts.ChunkMaxBytes)
	s.onOutput([]byte(data))
	s.Flush()

	var chunks []IOChunkEvent
	for _, e := range sink.all() {
		if e.Type == protocol.EventTypeIOChunk {
			chunks = append(chunks, e.Payload.(IOChunkEvent))
		}
	}
	require.Greater(t, len(chunks), 2, "want more than two chunks")
	require.NotEqual(t, chunks[0].ScreenHashAfter, chunks[len(chunks)-1].ScreenHashAfter)

	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	i := 0
	require.NoError(t, Replay(bytes.NewReader(rec.Bytes()), func(c Chunk) error {
		require.Less(t, i, len(chunks), "more records than io.chunk events")
		_, err := emu.Write(c.Data)
		require.NoError(t, err)
		require.Equal(t, chunks[i].ScreenHashAfter, vt.Hash(emu.Snapshot()),
			"chunk %d hash is not the screen after its own bytes", i)
		i++
		return nil
	}))
	require.Equal(t, len(chunks), i, "fewer records than io.chunk events")
}

// TestSmallReadsCoalesce folds forty 100-byte reads below the threshold:
// they must aggregate into as few records as ChunkMaxBytes allows, and
// every record must still carry the screen hash after its own bytes on
// incremental replay.
func TestSmallReadsCoalesce(t *testing.T) {
	t.Parallel()

	_, slave := testPTY(t)
	sink := &chanSink{}
	var rec bytes.Buffer
	opts := testOptions(t.TempDir(), "coalesce", slave, slave, sink, echoFalse())
	opts.RecordTo = &rec
	opts.ChunkMinBytes = 4096
	opts.ChunkMaxBytes = 1024
	s, err := NewSession(opts)
	require.NoError(t, err)

	line := "\x1b[32mgreen\x1b[0m\n"
	for range 40 {
		s.onOutput([]byte(strings.Repeat(line, 100/len(line)+1)[:100]))
	}
	s.Flush()

	var chunks []IOChunkEvent
	for _, e := range sink.all() {
		if e.Type == protocol.EventTypeIOChunk {
			chunks = append(chunks, e.Payload.(IOChunkEvent))
		}
	}
	require.Len(t, chunks, 4, "40 small reads must coalesce into 4 records")
	var total int64
	for _, c := range chunks {
		total += c.ByteCount
		require.LessOrEqual(t, c.ByteCount, int64(opts.ChunkMaxBytes))
	}
	require.Equal(t, int64(4000), total)

	emu, err := vt.New(80, 24)
	require.NoError(t, err)
	defer func() { _ = emu.Close() }()
	i := 0
	require.NoError(t, Replay(bytes.NewReader(rec.Bytes()), func(c Chunk) error {
		require.Less(t, i, len(chunks))
		_, err := emu.Write(c.Data)
		require.NoError(t, err)
		require.Equal(t, chunks[i].ScreenHashAfter, vt.Hash(emu.Snapshot()),
			"coalesced chunk %d hash is not the screen after its own bytes", i)
		i++
		return nil
	}))
	require.Equal(t, len(chunks), i)
}

// TestBadSessionIDRejected refuses ids outside protocol.OpaqueID before
// touching the disk: no recordings directory may appear for them.
func TestBadSessionIDRejected(t *testing.T) {
	t.Parallel()

	_, slave := testPTY(t)
	for _, id := range []string{"", "../evil", "a/b", "a b", "-x", strings.Repeat("a", 129)} {
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			sink := &chanSink{}
			_, err := NewSession(testOptions(dataDir, id, slave, slave, sink, echoFalse()))
			require.Error(t, err)
			_, statErr := os.Stat(filepath.Join(dataDir, recordingsDir))
			require.True(t, os.IsNotExist(statErr), "disk touched for bad id %q", id)
		})
	}
}

// TestRecordingsDirModeEnforced refuses a recordings directory that
// already exists with the wrong mode, or one that is a symlink: MkdirAll
// fixes neither, so the 0700 promise is verified on the live directory.
func TestRecordingsDirModeEnforced(t *testing.T) {
	t.Parallel()

	_, slave := testPTY(t)
	t.Run("wrong mode", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dataDir, recordingsDir), 0o755))
		require.NoError(t, os.Chmod(filepath.Join(dataDir, recordingsDir), 0o755))
		sink := &chanSink{}
		_, err := NewSession(testOptions(dataDir, "badmode", slave, slave, sink, echoFalse()))
		require.ErrorContains(t, err, "mode 0755")
	})
	t.Run("symlink", func(t *testing.T) {
		t.Parallel()
		dataDir := t.TempDir()
		target := t.TempDir()
		require.NoError(t, os.Symlink(target, filepath.Join(dataDir, recordingsDir)))
		sink := &chanSink{}
		_, err := NewSession(testOptions(dataDir, "badlink", slave, slave, sink, echoFalse()))
		require.ErrorContains(t, err, "not a directory")
	})
}

// TestPTYOpenedFirstAndOutputBeforeResize: pty.opened leads with the
// starting size, and output that landed before a resize is recorded and
// reported before input.resize, so a replay applies them in live order.
func TestPTYOpenedFirstAndOutputBeforeResize(t *testing.T) {
	t.Parallel()

	master, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), "opened", slave, slave, sink, []string{
		"sh", "-c", `printf 'before\n'; read -r line; exit 0`,
	})
	// Keep output pending: only the resize may flush it.
	opts.ChunkMinBytes = 1 << 20
	opts.OutFlushEvery = time.Hour
	s, err := NewSession(opts)
	require.NoError(t, err)
	evs := sink.all()
	require.Len(t, evs, 1)
	require.Equal(t, protocol.EventTypePTYOpened, evs[0].Type)
	require.Equal(t, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex}, evs[0].Payload)
	require.Zero(t, evs[0].Sequence)

	done := runAsync(t, s)
	readUntil(t, master, "before")
	// The outer pump shows bytes before the loop takes them in: wait
	// until they are pending on the live screen.
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.buffered > 0
	}, 10*time.Second, time.Millisecond)
	require.NoError(t, s.Resize(100, 30))
	writeKeys(t, master, "q\n")
	waitDone(t, done)

	chunk, resize := -1, -1
	for i, e := range sink.all() {
		require.Equal(t, int64(i), e.Sequence)
		switch e.Type {
		case protocol.EventTypeIOChunk:
			if chunk < 0 {
				chunk = i
			}
		case protocol.EventTypeInputResize:
			resize = i
		default:
		}
	}
	require.Positive(t, chunk)
	require.Greater(t, resize, chunk, "the output before the resize is reported first")
	all := sink.all()
	require.Equal(t, protocol.EventTypePTYClosed, all[len(all)-1].Type, "pty.closed is last")
}

func TestUnknownHarnessRejected(t *testing.T) {
	t.Parallel()

	_, slave := testPTY(t)
	for _, h := range []string{"", "vim"} {
		opts := testOptions(t.TempDir(), "harness", slave, slave, &chanSink{}, echoFalse())
		opts.Profile.Harness = h
		_, err := NewSession(opts)
		require.ErrorIs(t, err, ErrBadHarness, "harness %q", h)
	}
}
