package session

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/outputactivity"
	"github.com/smellak/gifford/internal/protocol"
)

// RecordingSuffix and EventsSuffix end a session's two files in
// recordings/: the raw output and its metadata events.
const (
	RecordingSuffix = ".raw"
	EventsSuffix    = ".events.jsonl"
)

const (
	recordingsDir  = "recordings"
	recordingPerm  = os.FileMode(0o600)
	recordingsPerm = os.FileMode(0o700)

	defaultEventBuffer     = 4096
	defaultEventFlushEvery = 200 * time.Millisecond
	maxEventLineBytes      = 1 << 20
	maxLoggedEvents        = 1 << 24
)

var (
	// ErrNoSessionID means a session id is empty or not an opaque id.
	ErrNoSessionID = errors.New("session: no session id")
	// ErrEventLog means an events file is missing its end, out of order,
	// has gaps or dropped events, or is otherwise not one whole session.
	ErrEventLog = errors.New("session: events file is not one whole session")
	// ErrBadHarness means a harness is not a known protocol.Harness.
	ErrBadHarness = errors.New("session: unknown harness")
	// ErrUnsafeFile means a recordings dir or file is a link, a special
	// file, or open to others.
	ErrUnsafeFile = errors.New("session: recording file is not private")
)

// ValidHarness reports a known harness id, generic included.
func ValidHarness(h protocol.Harness) bool {
	return slices.Contains(protocol.Harness("").EnumValues(), string(h))
}

// ValidHarnessVersion reports a well-formed measured version: 1 to 64
// printable characters, like the manifest's HarnessVersion.
func ValidHarnessVersion(v string) bool {
	if utf8.RuneCountInString(v) < 1 || utf8.RuneCountInString(v) > 64 {
		return false
	}
	if !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// ChunkDigest is the SHA-256 of one recorded piece, as io.chunk carries
// it.
func ChunkDigest(data []byte) protocol.Digest {
	sum := sha256.Sum256(data)
	return protocol.Digest("sha256:" + hex.EncodeToString(sum[:]))
}

// MaxLoggedEvents is the most events one events file holds: ReadEventLog
// refuses a file with more.
const MaxLoggedEvents = maxLoggedEvents

// RecordingsDir is the recordings directory of a data directory.
func RecordingsDir(dataDir string) string { return filepath.Join(dataDir, recordingsDir) }

// eventLine is one line of an events file: the envelope of one event and
// its payload as the session built it.
type eventLine struct {
	Session     string          `json:"session"`
	Type        string          `json:"type"`
	WallTime    string          `json:"wall_time"`
	MonotonicNS int64           `json:"monotonic_ns"`
	Sequence    int64           `json:"sequence"`
	Payload     json.RawMessage `json:"payload"`
}

// endLine is the last line of a whole events file: how many events it
// holds and how many the log had to drop.
type endLine struct {
	Session string `json:"session"`
	End     bool   `json:"end"`
	Events  int64  `json:"events"`
	Dropped int64  `json:"dropped"`
}

// EventLogOptions tune an EventLog; zero values take the defaults.
type EventLogOptions struct {
	// Buffer is how many events wait for the writer; past it they are
	// dropped and counted. Default 4096.
	Buffer int
	// FlushEvery is how often written events reach the file. Default
	// 200 ms.
	FlushEvery time.Duration
}

// EventLog is an EventSink that writes events to a file, one JSON line
// each, ending with a line that counts them and the ones dropped. Emit
// never waits: events queue for a writer goroutine, and a full queue
// drops the event and counts it, so a key never waits on the disk.
type EventLog struct {
	session string
	ch      chan Event
	done    chan struct{}
	stopped chan struct{}

	mu      sync.RWMutex // held for writing only to close
	closed  bool
	dropped atomic.Int64

	w       io.Writer
	closer  io.Closer
	written int64
	err     error
}

// NewEventLog writes the events of session to w, closing it with Close
// when it is an io.Closer.
func NewEventLog(w io.Writer, session string, opts EventLogOptions) *EventLog {
	if opts.Buffer <= 0 {
		opts.Buffer = defaultEventBuffer
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = defaultEventFlushEvery
	}
	l := &EventLog{
		session: session,
		ch:      make(chan Event, opts.Buffer),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
		w:       w,
	}
	if c, ok := w.(io.Closer); ok {
		l.closer = c
	}
	go l.run(opts.FlushEvery)
	return l
}

// Emit queues one event, or drops and counts it when the queue is full or
// the log is closed. It never blocks on the writer.
func (l *EventLog) Emit(e Event) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		l.dropped.Add(1)
		return
	}
	select {
	case l.ch <- e:
	default:
		l.dropped.Add(1)
	}
}

// Dropped counts the events dropped so far.
func (l *EventLog) Dropped() int64 { return l.dropped.Load() }

// Close writes the queued events and the end line, then closes the file.
// Later events are dropped.
func (l *EventLog) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()
	close(l.done)
	<-l.stopped
	return l.err
}

func (l *EventLog) run(every time.Duration) {
	defer close(l.stopped)
	bw := bufio.NewWriter(l.w)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case e := <-l.ch:
			l.write(bw, e)
		case <-tick.C:
			l.flush(bw)
		case <-l.done:
			for {
				select {
				case e := <-l.ch:
					l.write(bw, e)
					continue
				default:
				}
				break
			}
			line, err := json.Marshal(endLine{Session: l.session, End: true, Events: l.written, Dropped: l.dropped.Load()})
			if err == nil && l.err == nil {
				_, err = bw.Write(append(line, '\n'))
			}
			l.fail(err)
			l.flush(bw)
			if l.closer != nil {
				l.fail(l.closer.Close())
			}
			return
		}
	}
}

func (l *EventLog) fail(err error) {
	if err != nil && l.err == nil {
		l.err = fmt.Errorf("session: events file: %w", err)
	}
}

func (l *EventLog) flush(bw *bufio.Writer) {
	if l.err == nil {
		l.fail(bw.Flush())
	}
}

// write encodes one event; after a write failure every event is dropped
// and counted.
func (l *EventLog) write(bw *bufio.Writer, e Event) {
	if l.err != nil {
		l.dropped.Add(1)
		return
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		l.dropped.Add(1)
		return
	}
	line, err := json.Marshal(eventLine{
		Session:     e.Session,
		Type:        string(e.Type),
		WallTime:    e.WallTime.UTC().Format(time.RFC3339Nano),
		MonotonicNS: e.MonotonicNS,
		Sequence:    e.Sequence,
		Payload:     payload,
	})
	if err != nil {
		l.dropped.Add(1)
		return
	}
	if _, err := bw.Write(append(line, '\n')); err != nil {
		l.fail(err)
		l.dropped.Add(1)
		return
	}
	l.written++
}

// CreateEventLog creates recordings/<session>.events.jsonl under dataDir,
// 0600 in a 0700 directory, and returns its log. It never replaces a
// file.
func CreateEventLog(dataDir, sessionID string, opts EventLogOptions) (*EventLog, error) {
	if !protocol.OpaqueID(sessionID).Valid() {
		return nil, ErrNoSessionID
	}
	dir := RecordingsDir(dataDir)
	if err := os.MkdirAll(dir, recordingsPerm); err != nil {
		return nil, fmt.Errorf("session: recordings dir: %w", err)
	}
	if err := checkPrivateDir(dir); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, sessionID+EventsSuffix), os.O_CREATE|os.O_EXCL|os.O_WRONLY|noFollow, recordingPerm)
	if err != nil {
		return nil, fmt.Errorf("session: events file: %w", err)
	}
	return NewEventLog(f, sessionID, opts), nil
}

// checkPrivateDir refuses a dir that is a link, not a directory, or open
// to others.
func checkPrivateDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("session: recordings dir: %w", err)
	}
	if !fi.IsDir() || fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("session: recordings dir: %w", ErrUnsafeFile)
	}
	return nil
}

// LoggedEvent is one event read back from an events file.
type LoggedEvent struct {
	Type        protocol.EventType
	WallTime    time.Time
	MonotonicNS int64
	Sequence    int64
	Payload     json.RawMessage
}

// ReadEventLog reads a whole events file of session and fails closed with
// ErrEventLog unless it is one: every line well formed and of the
// session, sequences 0, 1, 2… with no gap, monotonic time never going
// back, exactly one pty.opened (well formed, a known harness) first,
// exactly one pty.closed last, and the end line after it with the right
// count and nothing dropped.
func ReadEventLog(r io.Reader, session string) ([]LoggedEvent, error) {
	bad := func(what string) error { return fmt.Errorf("%w: %s", ErrEventLog, what) }
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxEventLineBytes)
	var out []LoggedEvent
	ended := false
	for sc.Scan() {
		line := sc.Bytes()
		if ended {
			return nil, bad("data after the end line")
		}
		var probe struct {
			End *bool `json:"end"`
		}
		if err := json.Unmarshal(line, &probe); err != nil {
			return nil, bad("malformed line")
		}
		if probe.End != nil {
			var end endLine
			if err := strictDecode(line, &end); err != nil || !end.End || end.Session != session {
				return nil, bad("malformed end line")
			}
			if end.Dropped != 0 {
				return nil, bad("events were dropped")
			}
			if end.Events != int64(len(out)) {
				return nil, bad("end line count does not match")
			}
			ended = true
			continue
		}
		var ev eventLine
		if err := strictDecode(line, &ev); err != nil || ev.Session != session || ev.Type == "" || len(ev.Payload) == 0 {
			return nil, bad("malformed event line")
		}
		wall, err := time.Parse(time.RFC3339Nano, ev.WallTime)
		if err != nil {
			return nil, bad("malformed wall time")
		}
		if ev.Sequence != int64(len(out)) {
			return nil, bad("sequence out of order or with a gap")
		}
		if protocol.EventType(ev.Type) == protocol.EventTypeCaptureGapDetected {
			return nil, bad("capture gap detected")
		}
		if n := len(out); n > 0 && ev.MonotonicNS < out[n-1].MonotonicNS {
			return nil, bad("monotonic time goes back")
		}
		if (len(out) == 0) != (ev.Type == string(protocol.EventTypePTYOpened)) {
			return nil, bad("pty.opened is not exactly once, first")
		}
		if len(out) == 0 {
			var p PTYOpened
			if err := strictDecode(ev.Payload, &p); err != nil || p.Cols < 1 || p.Rows < 1 || !ValidHarness(p.Harness) {
				return nil, bad("malformed pty.opened")
			}
			// A present version must be well formed; a missing or
			// empty one reads back as unknown, so old logs pass.
			if p.HarnessVersion != "" && !ValidHarnessVersion(p.HarnessVersion) {
				return nil, bad("malformed pty.opened harness version")
			}
			if p.ActivityVersion != "" && !outputactivity.ValidVersion(p.ActivityVersion) {
				return nil, bad("malformed pty.opened activity version")
			}
			if p.ObservationEvidenceVersion != "" && p.ObservationEvidenceVersion != PermissionDialogEvidenceVersion {
				return nil, bad("malformed pty.opened observation evidence version")
			}
		}
		if protocol.EventType(ev.Type) == protocol.EventTypeIOOutputRead {
			var p IOOutputRead
			if err := strictDecode(ev.Payload, &p); err != nil || !p.Valid() {
				return nil, bad("malformed io.output_read")
			}
		}
		if protocol.EventType(ev.Type) == EventTypePermissionDialogInput {
			var p PermissionDialogInputEvidence
			if err := strictDecode(ev.Payload, &p); err != nil || !p.Valid() {
				return nil, bad("malformed input observation evidence")
			}
		}
		if protocol.EventType(ev.Type) == EventTypePermissionDialogFrame {
			var p PermissionDialogFrameEvidence
			if err := strictDecode(ev.Payload, &p); err != nil || !p.Valid() {
				return nil, bad("malformed output observation evidence")
			}
		}
		if protocol.EventType(ev.Type) == protocol.EventTypePTYClosed {
			var p PTYClosed
			if err := strictDecode(ev.Payload, &p); err != nil || p.ObservationEvidence != nil && !p.ObservationEvidence.Valid() {
				return nil, bad("malformed pty.closed")
			}
		}
		if n := len(out); n > 0 && out[n-1].Type == protocol.EventTypePTYClosed {
			return nil, bad("an event after pty.closed")
		}
		if len(out) == maxLoggedEvents {
			return nil, bad("too many events")
		}
		out = append(out, LoggedEvent{
			Type: protocol.EventType(ev.Type), WallTime: wall,
			MonotonicNS: ev.MonotonicNS, Sequence: ev.Sequence, Payload: ev.Payload,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEventLog, err)
	}
	if !ended {
		return nil, bad("no end line: cut or still being written")
	}
	if len(out) < 2 || out[len(out)-1].Type != protocol.EventTypePTYClosed {
		return nil, bad("pty.closed is not last")
	}
	return out, nil
}

func strictDecode(line []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if dec.More() {
		return errors.New("trailing data")
	}
	return nil
}

// OpenPair opens a session's recording and events files for reading. Both
// must be private regular files in a private recordings dir; links and
// special files are refused, never followed or waited on.
func OpenPair(dataDir, sessionID string) (raw, events *os.File, err error) {
	if !protocol.OpaqueID(sessionID).Valid() {
		return nil, nil, ErrNoSessionID
	}
	dir := RecordingsDir(dataDir)
	if err := checkPrivateDir(dir); err != nil {
		return nil, nil, err
	}
	raw, err = openPrivate(filepath.Join(dir, sessionID+RecordingSuffix))
	if err != nil {
		return nil, nil, err
	}
	events, err = openPrivate(filepath.Join(dir, sessionID+EventsSuffix))
	if err != nil {
		return nil, nil, errors.Join(err, raw.Close())
	}
	return raw, events, nil
}

// OpenPrivate opens one file of a recordings dir for reading, as
// OpenPair opens its two: a link or a special file is refused, never
// followed or waited on, and so is a file open to others.
func OpenPrivate(path string) (*os.File, error) { return openPrivate(path) }

func openPrivate(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|noFollow|noBlock, 0)
	if err != nil {
		return nil, fmt.Errorf("session: open %s: %w", filepath.Base(path), err)
	}
	fi, err := f.Stat()
	if err == nil && (!fi.Mode().IsRegular() || fi.Mode().Perm()&0o077 != 0) {
		err = ErrUnsafeFile
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("session: open %s: %w", filepath.Base(path), err), f.Close())
	}
	return f, nil
}

// ListSessions names the sessions in dataDir's recordings dir that have
// an events file, sorted. A missing dir has none.
func ListSessions(dataDir string) ([]string, error) {
	dir := RecordingsDir(dataDir)
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err := checkPrivateDir(dir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("session: recordings dir: %w", err)
	}
	var out []string
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), EventsSuffix)
		if ok && protocol.OpaqueID(id).Valid() {
			out = append(out, id)
		}
	}
	return out, nil
}
