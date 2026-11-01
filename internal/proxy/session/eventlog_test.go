package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/outputactivity"
	"github.com/smellak/gifford/internal/protocol"
)

const logSessionID = "log-1"

var logT0 = time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

func logEvent(seq int64, t protocol.EventType, payload any) Event {
	return Event{
		Session: logSessionID, Type: t, Payload: payload,
		WallTime: logT0.Add(time.Duration(seq) * time.Millisecond), MonotonicNS: seq * 1e6, Sequence: seq,
	}
}

func writeLog(t *testing.T, evs ...Event) []byte {
	t.Helper()
	var buf bytes.Buffer
	l := NewEventLog(&buf, logSessionID, EventLogOptions{})
	for _, e := range evs {
		l.Emit(e)
	}
	require.NoError(t, l.Close())
	return buf.Bytes()
}

func TestEventLogRoundTrip(t *testing.T) {
	t.Parallel()

	raw := writeLog(t,
		logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex}),
		logEvent(1, protocol.EventTypeInputKeyChunk, InputKeyChunk{PrintableCount: 3}),
		logEvent(2, protocol.EventTypeInputResize, InputResize{Cols: 100, Rows: 30}),
		logEvent(3, protocol.EventTypePTYClosed, PTYClosed{}),
	)
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	require.Len(t, lines, 5)
	require.JSONEq(t, `{"session":"log-1","type":"pty.opened","wall_time":"2026-10-29T12:00:00Z","monotonic_ns":0,"sequence":0,"payload":{"cols":80,"rows":24,"harness":"codex"}}`, lines[0])
	require.JSONEq(t, `{"session":"log-1","end":true,"events":4,"dropped":0}`, lines[4])

	evs, err := ReadEventLog(bytes.NewReader(raw), logSessionID)
	require.NoError(t, err)
	require.Len(t, evs, 4)
	require.Equal(t, protocol.EventTypeInputResize, evs[2].Type)
	require.JSONEq(t, `{"cols":100,"rows":30,"draft_generation":0,"input_guard_version":0}`, string(evs[2].Payload))
}

// blockingWriter blocks every write until released.
type blockingWriter struct {
	gate chan struct{}
	mu   sync.Mutex
	buf  bytes.Buffer
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	<-w.gate
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// TestEventLogNeverBlocksEmit: with the writer stuck, Emit returns at
// once and counts what it drops; the end line carries the count.
func TestEventLogNeverBlocksEmit(t *testing.T) {
	t.Parallel()

	w := &blockingWriter{gate: make(chan struct{})}
	l := NewEventLog(w, logSessionID, EventLogOptions{Buffer: 2, FlushEvery: time.Millisecond})
	done := make(chan struct{})
	go func() {
		for i := range int64(1000) {
			l.Emit(logEvent(i, protocol.EventTypeInputKeyChunk, InputKeyChunk{PrintableCount: 1}))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Emit blocked on a stuck writer")
	}
	require.Positive(t, l.Dropped())
	close(w.gate)
	require.NoError(t, l.Close())
	l.Emit(logEvent(1000, protocol.EventTypeInputKeyChunk, InputKeyChunk{}))
	require.NoError(t, l.Close(), "a second close is a no-op")

	out := w.buf.String()
	require.Contains(t, out, `"end":true`)
	require.NotContains(t, out, `"dropped":0`)
	_, err := ReadEventLog(strings.NewReader(out), logSessionID)
	require.ErrorIs(t, err, ErrEventLog, "a log that dropped events does not replay")
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("no space left") }

func TestEventLogWriteFailure(t *testing.T) {
	t.Parallel()

	l := NewEventLog(failingWriter{}, logSessionID, EventLogOptions{FlushEvery: time.Millisecond})
	l.Emit(logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex}))
	require.Error(t, l.Close())
}

func TestReadEventLogFailsClosed(t *testing.T) {
	t.Parallel()

	opened := logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex})
	key := logEvent(1, protocol.EventTypeInputKeyChunk, InputKeyChunk{PrintableCount: 1})
	closed := logEvent(2, protocol.EventTypePTYClosed, PTYClosed{})
	quotaGap := logEvent(1, protocol.EventTypeCaptureGapDetected, protocol.CaptureGapDetected{
		CaptureSessionID: logSessionID, Cause: protocol.CaptureGapCauseQuota,
	})
	ingestGap := logEvent(1, protocol.EventTypeCaptureGapDetected, protocol.CaptureGapDetected{
		CaptureSessionID: logSessionID, Cause: protocol.CaptureGapCauseIngestDied,
	})
	good := writeLog(t, opened, key, closed)
	_, err := ReadEventLog(bytes.NewReader(good), logSessionID)
	require.NoError(t, err)
	lines := strings.SplitAfter(string(good), "\n") // opened, key, closed, end, ""
	back := key
	back.MonotonicNS = -1
	at := func(seq int64, e Event) Event { e.Sequence = seq; e.MonotonicNS = seq * 1e6; return e }

	for name, in := range map[string]string{
		"empty":              "",
		"cut":                lines[0] + lines[1],
		"torn line":          lines[0] + lines[1][:10],
		"disordered":         lines[1] + lines[0] + lines[2] + lines[3],
		"gap":                string(writeLog(t, opened, at(2, key), at(3, closed))),
		"capture quota gap":  string(writeLog(t, opened, quotaGap, closed)),
		"capture ingest gap": string(writeLog(t, opened, ingestGap, closed)),
		"no pty.opened":      string(writeLog(t, at(0, key), at(1, closed))),
		"pty.opened twice":   string(writeLog(t, opened, at(1, opened), at(2, closed))),
		"bad pty.opened":     string(writeLog(t, logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 0, Rows: 24, Harness: protocol.HarnessCodex}), key, closed)),
		"bad harness":        string(writeLog(t, logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: "vim"}), key, closed)),
		"bad version":        string(writeLog(t, logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex, HarnessVersion: "1.0\nx"}), key, closed)),
		"long version":       string(writeLog(t, logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex, HarnessVersion: strings.Repeat("v", 65)}), key, closed)),
		"bad activity version": string(writeLog(t, logEvent(0, protocol.EventTypePTYOpened, PTYOpened{
			Cols: 80, Rows: 24, Harness: protocol.HarnessCodex, ActivityVersion: "future-v9",
		}), key, closed)),
		"bad output read": string(writeLog(t, opened,
			logEvent(1, protocol.EventTypeIOOutputRead, IOOutputRead{ReadDigest: ChunkDigest([]byte("x"))}), closed)),
		"no pty.closed":    string(writeLog(t, opened, key)),
		"pty.closed early": string(writeLog(t, opened, at(1, closed), at(2, key))),
		"pty.closed twice": string(writeLog(t, opened, at(1, closed), at(2, closed))),
		"only pty.opened":  string(writeLog(t, opened)),
		"time goes back":   string(writeLog(t, opened, back, closed)),
		"other session":    strings.ReplaceAll(string(good), logSessionID, "log-2"),
		"wrong count":      lines[0] + lines[1] + lines[2] + strings.Replace(lines[3], `"events":3`, `"events":4`, 1),
		"after end":        string(good) + lines[1],
		"unknown member":   strings.Replace(string(good), `"sequence":0`, `"sequence":0,"text":"x"`, 1),
		"only end":         lines[3],
	} {
		_, err := ReadEventLog(strings.NewReader(in), logSessionID)
		require.ErrorIs(t, err, ErrEventLog, name)
	}
}

// TestPTYOpenedVersion checks the harness version round trip: a
// measured version reads back, a log without the field reads as
// unknown, and the validator accepts 1-64 printable characters.
func TestPTYOpenedVersion(t *testing.T) {
	t.Parallel()

	key := logEvent(1, protocol.EventTypeInputKeyChunk, InputKeyChunk{PrintableCount: 1})
	closed := logEvent(2, protocol.EventTypePTYClosed, PTYClosed{})
	readVersion := func(t *testing.T, raw []byte) string {
		t.Helper()
		evs, err := ReadEventLog(bytes.NewReader(raw), logSessionID)
		require.NoError(t, err)
		var p PTYOpened
		require.NoError(t, json.Unmarshal(evs[0].Payload, &p))
		return p.Version()
	}

	versioned := writeLog(t,
		logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex, HarnessVersion: "9.9.9"}),
		key, closed)
	require.Equal(t, "9.9.9", readVersion(t, versioned))

	plain := writeLog(t,
		logEvent(0, protocol.EventTypePTYOpened, PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex}),
		key, closed)
	require.Equal(t, UnknownHarnessVersion, readVersion(t, plain))

	lines := strings.SplitAfter(string(plain), "\n")
	withEmpty := strings.Replace(lines[0], `"harness":"codex"`, `"harness":"codex","harness_version":""`, 1) +
		lines[1] + lines[2] + lines[3]
	require.Equal(t, UnknownHarnessVersion, readVersion(t, []byte(withEmpty)))

	for _, v := range []string{"1.0", strings.Repeat("v", 64), "9.9.9-rc.1"} {
		require.True(t, ValidHarnessVersion(v), "%q", v)
	}
	for _, v := range []string{"", strings.Repeat("v", 65), "1.0\nx", "a\tb", "bad\xff"} {
		require.False(t, ValidHarnessVersion(v), "%q", v)
	}
}

func TestPTYOpenedActivityVersion(t *testing.T) {
	t.Parallel()
	require.Equal(t, outputactivity.ReconstructedVersion, (PTYOpened{}).OutputActivityVersion())
	require.Equal(t, outputactivity.Version, (PTYOpened{ActivityVersion: outputactivity.Version}).OutputActivityVersion())
}

func TestCreateEventLogPrivate(t *testing.T) {
	t.Parallel()

	data := t.TempDir()
	l, err := CreateEventLog(data, "priv-1", EventLogOptions{})
	require.NoError(t, err)
	require.NoError(t, l.Close())
	fi, err := os.Stat(filepath.Join(RecordingsDir(data), "priv-1"+EventsSuffix))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	_, err = CreateEventLog(data, "priv-1", EventLogOptions{})
	require.Error(t, err, "never replaces a file")
	_, err = CreateEventLog(data, "../x", EventLogOptions{})
	require.ErrorIs(t, err, ErrNoSessionID)

	ids, err := ListSessions(data)
	require.NoError(t, err)
	require.Equal(t, []string{"priv-1"}, ids)
}
