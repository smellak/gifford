package capture

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/vt"
)

var (
	// ErrNotImplemented means an import of a directory or of a format
	// with no import yet.
	ErrNotImplemented = errors.New("capture: import not implemented yet")
	// ErrBadOptions means an import's format or attribution is missing
	// or not valid.
	ErrBadOptions = errors.New("capture: bad import options")
	// ErrPartial means part of the session a source imports to is
	// there, or the whole pair without its provenance: nothing is
	// overwritten.
	ErrPartial = errors.New("capture: the session is there in part")
	// ErrConflict means the source was imported before with another
	// provenance: nothing changes.
	ErrConflict = errors.New("capture: imported before with another provenance")
)

const (
	recordingsPerm = 0o700
	// rawBuffer is the write buffer of the recording.
	rawBuffer = 64 << 10
	// ctxEvery is how many cast lines go by between context checks.
	ctxEvery = 4096
)

// Options are one import's source and attribution.
type Options struct {
	// DataDir holds recordings/, created 0700 when missing.
	DataDir string
	// Source is the file to import. Its path is never written anywhere.
	Source string
	Format Format
	// AgentID, Harness and EnvironmentDomain attribute the session: a
	// cast cannot tell them.
	AgentID           protocol.AgentID
	Harness           protocol.Harness
	EnvironmentDomain dataset.EnvironmentDomain
}

func (o Options) validate() error {
	switch {
	case !o.Format.Known():
		return fmt.Errorf("%w: unknown format", ErrBadOptions)
	case !o.AgentID.Valid():
		return fmt.Errorf("%w: not an agent id", ErrBadOptions)
	case !session.ValidHarness(o.Harness):
		return fmt.Errorf("%w: unknown harness", ErrBadOptions)
	case !o.EnvironmentDomain.Valid():
		return fmt.Errorf("%w: unknown environment domain", ErrBadOptions)
	}
	return nil
}

// Result is what an import did: an id and counts, never content.
type Result struct {
	SessionID string
	// Already marks a source imported before with the same provenance:
	// nothing was written.
	Already bool
	Counts  Counts
}

// tuning paces the event log, which drops what its queue cannot hold:
// the import never has more than half the queue waiting, and fails when
// the log writes nothing for stall. maxBytes caps the source and
// maxEvents the events of the session.
type tuning struct {
	buffer     int
	flushEvery time.Duration
	stall      time.Duration
	maxBytes   int64
	maxEvents  int64
}

var defaultTuning = tuning{buffer: 4096, stall: time.Minute, maxBytes: MaxCastBytes, maxEvents: session.MaxLoggedEvents}

// Import turns an asciicast v2 file into the pair a `gifford run` session
// leaves in recordings/, with the same Recorder and EventLog, and its
// provenance in <id>.import.json beside them. The id comes from the
// file's SHA-256, so a second import of the same file finds the session:
// with the same provenance it writes nothing and reports Already, with
// another it fails, and so does a session there in part. Every file is
// written under a temporary name and renamed once whole, the events file
// last; a failed import removes its temporary files and nothing else.
// Typed input and markers are counted, never written, and no keyboard or
// draft event is made up, so a replay leaves the draft unknown.
func Import(ctx context.Context, opts Options) (Result, error) {
	return run(ctx, opts, defaultTuning)
}

func run(ctx context.Context, opts Options, tune tuning) (Result, error) {
	if err := opts.validate(); err != nil {
		return Result{}, err
	}
	if opts.Format != FormatAsciicastV2 {
		return Result{}, fmt.Errorf("%w: format %s", ErrNotImplemented, opts.Format)
	}
	if !recordable {
		return Result{}, fmt.Errorf("%w: recordings need unix file modes", ErrNotImplemented)
	}
	src, err := openSource(opts.Source)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = src.Close() }()
	sum, size, err := digest(src, tune.maxBytes)
	if err != nil {
		return Result{}, err
	}
	prov := Provenance{
		SchemaVersion:     ImportSchemaVersion,
		Source:            Source{Format: opts.Format, SHA256: sum, SizeBytes: size},
		AgentID:           opts.AgentID,
		Harness:           opts.Harness,
		EnvironmentDomain: opts.EnvironmentDomain,
	}
	id := prov.SessionID()
	dir := session.RecordingsDir(opts.DataDir)
	if err := os.MkdirAll(dir, recordingsPerm); err != nil {
		return Result{}, fmt.Errorf("capture: recordings dir: %w", err)
	}
	if err := checkRecordingsDir(dir); err != nil {
		return Result{}, err
	}
	already, err := imported(opts.DataDir, id, prov)
	if err != nil {
		return Result{}, err
	}
	if already {
		return Result{SessionID: id, Already: true}, nil
	}
	w, err := newPairWriter(dir, id, tune)
	if err != nil {
		return Result{}, err
	}
	defer w.discard()
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return Result{}, fmt.Errorf("capture: read cast: %w", err)
	}
	h := sha256.New()
	in := &countReader{r: io.LimitReader(src, tune.maxBytes+1)}
	counts, err := convert(ctx, io.TeeReader(in, h), w, opts.Harness)
	if err != nil {
		return Result{}, err
	}
	if in.n != size || sumOf(h) != sum {
		return Result{}, errors.New("capture: the source changed while it was imported")
	}
	prov.Counts = counts
	if err := w.finish(prov); err != nil {
		return Result{}, err
	}
	return Result{SessionID: id, Counts: counts}, nil
}

// openSource opens the file to import without waiting on a FIFO. A
// directory is an import not implemented yet; anything else but a
// regular file is refused. Errors name the file, not its path.
func openSource(path string) (*os.File, error) {
	name := filepath.Base(path)
	f, err := os.OpenFile(path, os.O_RDONLY|noBlock, 0)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			err = pe.Err
		}
		return nil, fmt.Errorf("capture: open %s: %w", name, err)
	}
	fi, err := f.Stat()
	switch {
	case err != nil:
		err = fmt.Errorf("capture: open %s: %w", name, err)
	case fi.IsDir():
		err = fmt.Errorf("%w: importing a directory", ErrNotImplemented)
	case !fi.Mode().IsRegular():
		err = fmt.Errorf("%w: %s is not a regular file", ErrBadCast, name)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

// digest is the SHA-256 and size of the whole source, which the session
// id comes from. A source past maxBytes is refused before anything is
// written.
func digest(src io.Reader, maxBytes int64) (protocol.Digest, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(src, maxBytes+1))
	switch {
	case err != nil:
		return "", 0, fmt.Errorf("capture: read cast: %w", err)
	case n > maxBytes:
		return "", 0, fmt.Errorf("%w: larger than %d bytes", ErrBadCast, maxBytes)
	case n == 0:
		return "", 0, fmt.Errorf("%w: no header", ErrBadCast)
	}
	return sumOf(h), n, nil
}

func sumOf(h hash.Hash) protocol.Digest {
	return protocol.Digest("sha256:" + hex.EncodeToString(h.Sum(nil)))
}

// countReader counts the bytes read through it.
type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err //nolint:wrapcheck // a reader passes its source's error on, io.EOF included
}

// imported reports a source imported before with the same provenance:
// the whole pair there, and its <id>.import.json the same in digest,
// format, agent, harness and environment domain. Part of a session, a
// whole pair without a valid provenance, or with one that says otherwise,
// fails and is left as it is.
func imported(dataDir, id string, want Provenance) (bool, error) {
	dir := session.RecordingsDir(dataDir)
	there := map[string]bool{}
	for _, suffix := range []string{session.RecordingSuffix, session.EventsSuffix, ImportSuffix} {
		_, err := os.Lstat(filepath.Join(dir, id+suffix))
		switch {
		case err == nil:
			there[suffix] = true
		case !errors.Is(err, fs.ErrNotExist):
			return false, fmt.Errorf("capture: recordings dir: %w", err)
		}
	}
	switch {
	case len(there) == 0:
		return false, nil
	case !there[session.RecordingSuffix] || !there[session.EventsSuffix]:
		return false, fmt.Errorf("%w: %s is there in part; nothing is overwritten", ErrPartial, id)
	}
	raw, events, err := session.OpenPair(dataDir, id)
	if err != nil {
		return false, fmt.Errorf("%w: %s: %w", ErrPartial, id, err)
	}
	_ = raw.Close()
	_ = events.Close()
	got, ok, err := ReadImport(dataDir, id)
	switch {
	case err != nil:
		return false, fmt.Errorf("%w: %s is there with a provenance that does not read: %w", ErrPartial, id, err)
	case !ok:
		return false, fmt.Errorf("%w: %s is there without its provenance", ErrPartial, id)
	}
	if clash := clashes(got, want); len(clash) > 0 {
		verb := "differs"
		if len(clash) > 1 {
			verb = "differ"
		}
		return false, fmt.Errorf("%w: %s: %s %s; nothing changes", ErrConflict, id, strings.Join(clash, ", "), verb)
	}
	return true, nil
}

// clashes names the provenance fields an import asks for otherwise than
// the import before it.
func clashes(got, want Provenance) []string {
	var out []string
	for _, f := range []struct {
		name string
		same bool
	}{
		{"source.sha256", got.Source.SHA256 == want.Source.SHA256},
		{"source.format", got.Source.Format == want.Source.Format},
		{"agent_id", got.AgentID == want.AgentID},
		{"harness", got.Harness == want.Harness},
		{"environment_domain", got.EnvironmentDomain == want.EnvironmentDomain},
	} {
		if !f.same {
			out = append(out, f.name)
		}
	}
	return out
}

// convert writes a cast as a run leaves its pair: pty.opened with the
// cast's size and the harness given, a recorded chunk and its io.chunk
// for each output, with the screen hash a live vt gives, an input.resize
// for each resize, and pty.closed at the cast's last event. Typed input
// and markers are counted, never written.
func convert(ctx context.Context, r io.Reader, w *pairWriter, harness protocol.Harness) (Counts, error) {
	cr := newCastReader(r)
	cols, rows, err := cr.header()
	if err != nil {
		return Counts{}, err
	}
	emu, err := vt.New(cols, rows)
	if err != nil {
		return Counts{}, fmt.Errorf("capture: screen: %w", err)
	}
	defer func() { _ = emu.Close() }()
	w.start = cr.start
	if err := w.emit(0, protocol.EventTypePTYOpened, session.PTYOpened{Cols: cols, Rows: rows, Harness: harness}); err != nil {
		return Counts{}, err
	}
	var c Counts
	var last int64
	for n := 0; ; n++ {
		if n%ctxEvery == 0 {
			if err := ctx.Err(); err != nil {
				return Counts{}, fmt.Errorf("capture: import: %w", err)
			}
		}
		ev, err := cr.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Counts{}, err
		}
		last = ev.at
		if (ev.code == codeOutput || ev.code == codeResize) && w.seq >= w.tune.maxEvents-1 {
			return Counts{}, cr.bad("more output and resizes than one session holds")
		}
		switch ev.code {
		case codeOutput:
			data := []byte(ev.data)
			if _, err := emu.Write(data); err != nil {
				return Counts{}, fmt.Errorf("capture: screen: %w", err)
			}
			if err := w.output(ev.at, data, vt.Hash(emu.Snapshot())); err != nil {
				return Counts{}, err
			}
			c.Output++
		case codeResize:
			cols, rows, ok := parseSize(ev.data)
			if !ok || emu.Resize(cols, rows) != nil {
				return Counts{}, cr.bad("malformed resize")
			}
			// The guard fields stay zero: a replay does not read them.
			if err := w.emit(ev.at, protocol.EventTypeInputResize, session.InputResize{Cols: cols, Rows: rows}); err != nil {
				return Counts{}, err
			}
			c.Resize++
		case codeInput:
			c.InputDropped++
		case codeMarker:
			c.MarkerDropped++
		}
	}
	if err := w.emit(last, protocol.EventTypePTYClosed, session.PTYClosed{}); err != nil {
		return Counts{}, err
	}
	return c, nil
}

// pacedFile is the events file under the log. It counts the lines that
// reach the file, so the import knows how many events still wait in the
// log's queue.
type pacedFile struct {
	f     *os.File
	lines atomic.Int64
	moved chan struct{}
}

func (p *pacedFile) Write(b []byte) (int, error) {
	n, err := p.f.Write(b)
	p.lines.Add(int64(bytes.Count(b[:n], []byte{'\n'})))
	select {
	case p.moved <- struct{}{}:
	default:
	}
	if err != nil {
		return n, fmt.Errorf("capture: events file: %w", err)
	}
	return n, nil
}

// pairWriter writes one session's files under temporary names in the
// recordings dir: none has its own name before finish.
type pairWriter struct {
	dir, id string
	tune    tuning
	start   time.Time

	raw    *os.File
	rawBuf *bufio.Writer
	rec    *session.Recorder
	events *pacedFile
	log    *session.EventLog
	seq    int64

	// files are the ones open; temps the exact paths still under a
	// temporary name, the only ones a failed import removes.
	files []*os.File
	temps []string
}

func newPairWriter(dir, id string, tune tuning) (*pairWriter, error) {
	w := &pairWriter{dir: dir, id: id, tune: tune}
	raw, err := w.temp(session.RecordingSuffix)
	if err != nil {
		return nil, err
	}
	events, err := w.temp(session.EventsSuffix)
	if err != nil {
		w.discard()
		return nil, err
	}
	w.raw, w.rawBuf = raw, bufio.NewWriterSize(raw, rawBuffer)
	w.rec = session.NewRecorder(w.rawBuf)
	// The paced file is no io.Closer: the log leaves the file open, to
	// be synced before it is renamed.
	w.events = &pacedFile{f: events, moved: make(chan struct{}, 1)}
	w.log = session.NewEventLog(w.events, id, session.EventLogOptions{Buffer: tune.buffer, FlushEvery: tune.flushEvery})
	return w, nil
}

// temp creates a file 0600 under a new temporary name: a dot, the
// session's own name, random digits and .tmp, which no reader of the
// recordings dir takes for a session's file.
func (w *pairWriter) temp(suffix string) (*os.File, error) {
	f, err := os.CreateTemp(w.dir, "."+w.id+suffix+".*.tmp")
	if err != nil {
		return nil, fmt.Errorf("capture: temporary file: %w", err)
	}
	w.files = append(w.files, f)
	w.temps = append(w.temps, f.Name())
	return f, nil
}

// emit numbers one event and queues it, at its time on the cast's
// clock, then waits while the log holds half its queue.
func (w *pairWriter) emit(at int64, t protocol.EventType, payload any) error {
	w.log.Emit(session.Event{
		Session: w.id, Type: t, Payload: payload,
		WallTime: w.start.Add(time.Duration(at)), MonotonicNS: at, Sequence: w.seq,
	})
	w.seq++
	return w.pace()
}

// pace waits, when half the log's queue waits for its writer, until a
// quarter does: the queue never fills, so the log never drops an event.
// Every event line ends the only newline it holds, so the lines in the
// file are the events written.
func (w *pairWriter) pace() error {
	high, low := int64(w.tune.buffer/2), int64(w.tune.buffer/4)
	if w.seq-w.events.lines.Load() < high {
		return nil
	}
	stall := time.NewTimer(w.tune.stall)
	defer stall.Stop()
	for w.seq-w.events.lines.Load() > low {
		if w.log.Dropped() != 0 {
			return errors.New("capture: the event log dropped events")
		}
		select {
		case <-w.events.moved:
		case <-stall.C:
			return errors.New("capture: the event log stopped writing")
		}
	}
	return nil
}

// output records one output piece and its io.chunk.
func (w *pairWriter) output(at int64, data []byte, screen protocol.Digest) error {
	seq, err := w.rec.Append(w.start.Add(time.Duration(at)), at, data)
	if err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	return w.emit(at, protocol.EventTypeIOChunk, session.IOChunkEvent{
		ByteCount:         int64(len(data)),
		ANSIPresent:       bytes.Contains(data, []byte{0x1b}),
		ScreenHashAfter:   screen,
		RecordingSequence: seq,
		ChunkDigest:       session.ChunkDigest(data),
	})
}

// finish ends the pair, writes the provenance beside it and renames the
// three files to their own names, the events file last: it is the one a
// reader of the dir finds sessions by. A name taken meanwhile fails the
// import before any rename.
func (w *pairWriter) finish(p Provenance) error {
	if err := w.log.Close(); err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	if n := w.log.Dropped(); n != 0 {
		return fmt.Errorf("capture: the event log dropped %d events", n)
	}
	// Every event and the end line, nothing else.
	if got := w.events.lines.Load(); got != w.seq+1 {
		return fmt.Errorf("capture: the events file holds %d lines for %d events", got, w.seq)
	}
	if err := w.rawBuf.Flush(); err != nil {
		return fmt.Errorf("capture: recording: %w", err)
	}
	for _, f := range []*os.File{w.raw, w.events.f} {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("capture: sync: %w", err)
		}
	}
	body, err := p.encode()
	if err != nil {
		return err
	}
	prov, err := w.temp(ImportSuffix)
	if err != nil {
		return err
	}
	if _, err := prov.Write(body); err != nil {
		return fmt.Errorf("capture: provenance: %w", err)
	}
	if err := prov.Sync(); err != nil {
		return fmt.Errorf("capture: sync: %w", err)
	}
	for _, f := range w.files {
		if err := f.Close(); err != nil {
			return fmt.Errorf("capture: close: %w", err)
		}
	}
	w.files = nil
	// The temporary files in the order they are renamed in.
	renames := []struct{ tmp, suffix string }{
		{w.temps[0], session.RecordingSuffix},
		{w.temps[2], ImportSuffix},
		{w.temps[1], session.EventsSuffix},
	}
	for _, r := range renames {
		if _, err := os.Lstat(filepath.Join(w.dir, w.id+r.suffix)); !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: %s appeared during the import; nothing is overwritten", ErrPartial, w.id)
		}
	}
	for _, r := range renames {
		if err := os.Rename(r.tmp, filepath.Join(w.dir, w.id+r.suffix)); err != nil {
			return fmt.Errorf("capture: rename: %w", err)
		}
		w.forget(r.tmp)
	}
	return nil
}

// forget drops a temporary path once renamed: it is no longer the
// import's to remove.
func (w *pairWriter) forget(tmp string) {
	for i, t := range w.temps {
		if t == tmp {
			w.temps = append(w.temps[:i], w.temps[i+1:]...)
			return
		}
	}
}

// discard closes what is open and removes the temporary files left, by
// their exact paths: never any other file. After finish there are none.
func (w *pairWriter) discard() {
	if w.log != nil {
		_ = w.log.Close()
	}
	for _, f := range w.files {
		_ = f.Close()
	}
	w.files = nil
	for _, t := range w.temps {
		_ = os.Remove(t)
	}
	w.temps = nil
}
