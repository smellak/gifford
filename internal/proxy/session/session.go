// Session execution: wiring between the real terminal, the proxy core,
// the screen emulator, the recorder, the input parser and the draft
// guard. Pumps run apart: output may stall on a slow recording, input
// keys never wait on persistence. Only this file's loop touches the
// emulator, parser, guard and recorder; anything else talks to it through
// channels or under mu.

//go:build unix

package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
	"golang.org/x/term"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/proxy/input"
	"github.com/smellak/gifford/internal/vt"
	"github.com/smellak/gifford/internal/xdg"
)

// ErrNoSink means Options.Sink is nil.
var ErrNoSink = errors.New("session: no event sink")

const (
	defaultChunkMinBytes = 4096
	defaultChunkMaxBytes = 16384
	defaultOutFlushEvery = 20 * time.Millisecond
	defaultKeyFlushEvery = 50 * time.Millisecond
	pumpQueue            = 64
)

// Options configures one Session.
type Options struct {
	// Command is the harness and its arguments.
	Command []string
	// Env is the harness's whole environment, in the form os.Environ
	// returns; nil inherits this process's. It goes to the Proxy as is,
	// and the session never prints, logs or records it.
	Env []string
	// In and Out are the real outer terminal, put in raw mode for the run.
	In, Out *os.File
	// DataDir holds recordings/; "" resolves it through internal/xdg.
	DataDir string
	// SessionID names the recording file; required.
	SessionID string
	// Profile drives the input parser and the draft guard.
	Profile profiles.Profile
	// Clock tells time; nil reads the system clock.
	Clock clock.Clock
	// Sink receives session events in order; required.
	Sink EventSink
	// RecordTo replaces the recording file; tests use it to stall or
	// capture the stream. Nil writes recordings/<session>.raw, owned by
	// the session.
	RecordTo io.Writer
	// ChunkMinBytes closes an output piece past this size.
	ChunkMinBytes int
	// ChunkMaxBytes splits a single read past this size.
	ChunkMaxBytes int
	// OutFlushEvery closes an output piece past this age.
	OutFlushEvery time.Duration
	// KeyFlushEvery emits a pending printable count past this age.
	KeyFlushEvery time.Duration
	// Notice receives the daemon notice; nil discards it.
	Notice io.Writer
	// DaemonSocket is probed once before the start; "" is not running.
	DaemonSocket string
	// HarnessVersion is the measured harness version the run passes
	// to pty.opened; "" omits it, and old logs without it read back
	// as unknown.
	HarnessVersion string
	// SubmitDelay waits between a delivered line and its submit key;
	// zero or negative means the 150 ms default.
	SubmitDelay time.Duration
}

// pendingPiece is one buffered output piece with the screen hash after
// its own bytes, so each io.chunk event carries the hash its bytes left.
type pendingPiece struct {
	data []byte
	hash protocol.Digest
}

// Session is one wrapped harness run.
type Session struct {
	opts Options
	clk  clock.Clock

	mu      sync.Mutex
	emu     *vt.Emulator
	parser  *input.Parser
	guard   *draft.Guard
	rec     *Recorder
	file    *os.File
	started time.Time
	evSeq   int64

	// pending holds buffered output in pieces of at most ChunkMaxBytes,
	// each with the screen hash after its own bytes.
	pending []pendingPiece
	// buffered counts the bytes in pending.
	buffered  int
	chunkBorn time.Time
	lastSnap  vt.Snapshot
	hasSnap   bool

	printable int64
	pasteSize int64
	pasting   bool

	lastCols int
	lastRows int
	sigCore  chan os.Signal
	running  bool
	closed   bool
	mid      *os.File
	fatalErr error
	// humanWritten counts the human chunks the writer put on the
	// middle PTY; only the writer goroutine adds to it.
	humanWritten atomic.Uint64
	// chunksSeen counts the chunks the loop fully processed; the
	// snapshot below publishes it with the guard truth after each
	// chunk. Guarded by mu.
	chunksSeen uint64
	// lastSubmit is the last human submit keypress. Guarded by mu.
	lastSubmit time.Time
	// lastOutput is when the harness last wrote output. Guarded by mu.
	lastOutput time.Time
	// lastKey is the last human input the loop read. Guarded by mu.
	lastKey time.Time
	// changed signals a new guard version to the agent socket,
	// without waiting: one pending signal is enough.
	changed chan struct{}
	// snap is the published guard foto. Guarded by mu.
	snap guardSnapshot
	// prepared holds accepted Prepares by id; lease is the taken
	// one, if any. Guarded by mu; only the writer takes a lease.
	prepared map[protocol.OpaqueID]pendingDelivery
	lease    *deliveryLease
	// arrived counts the human chunks pumpIn read from the terminal,
	// before enqueueing them: a key read but not yet handed to the
	// writer still refuses a commit. Guarded by mu.
	arrived uint64
	// committing reserves one commit handed to the writer before its
	// lease: Cancel and any second Commit see the reservation and
	// refuse while it lives. Guarded by mu.
	committing   bool
	committingID protocol.OpaqueID
	// inCh carries pumpIn chunks to the writer, opCh commit ops;
	// stopCh closes at teardown so methods never hang on a gone
	// writer.
	inCh   chan []byte
	opCh   chan writerOp
	stopCh chan struct{}
}

// NewSession validates options and opens the recording. It touches the
// filesystem (recordings dir and file) but starts nothing.
func NewSession(opts Options) (*Session, error) {
	if len(opts.Command) == 0 {
		return nil, proxy.ErrNoCommand
	}
	// The session id names the recording file, so only opaque ids are
	// safe: the pattern admits no separator and no dot-dot. This runs
	// before anything touches the disk.
	if !protocol.OpaqueID(opts.SessionID).Valid() {
		return nil, ErrNoSessionID
	}
	if opts.Sink == nil {
		return nil, ErrNoSink
	}
	if !ValidHarness(protocol.Harness(opts.Profile.Harness)) {
		return nil, ErrBadHarness
	}
	if opts.In == nil || opts.Out == nil {
		return nil, proxy.ErrNotTerminal
	}
	clk := opts.Clock
	if clk == nil {
		clk = clock.System{}
	}
	size, err := pty.GetsizeFull(opts.Out)
	if err != nil {
		return nil, fmt.Errorf("session: size outer terminal: %w", err)
	}
	dataDir := opts.DataDir
	if dataDir == "" {
		dirs, err := xdg.Resolve()
		if err != nil {
			return nil, fmt.Errorf("session: data dir: %w", err)
		}
		if _, err := xdg.EnsureAll(dirs); err != nil {
			return nil, fmt.Errorf("session: data dir: %w", err)
		}
		dataDir = dirs.Data
	}
	recDir := filepath.Join(dataDir, recordingsDir)
	if err := os.MkdirAll(recDir, recordingsPerm); err != nil {
		return nil, fmt.Errorf("session: recordings dir: %w", err)
	}
	if err := checkRecordingsDir(recDir); err != nil {
		return nil, err
	}
	var file *os.File
	var rec *Recorder
	if opts.RecordTo != nil {
		rec = NewRecorder(opts.RecordTo)
	} else {
		path := filepath.Join(recDir, opts.SessionID+RecordingSuffix)
		file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, recordingPerm)
		if err != nil {
			return nil, fmt.Errorf("session: recording file: %w", err)
		}
		rec = NewRecorder(file)
	}
	emu, err := vt.New(int(size.Cols), int(size.Rows))
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		return nil, fmt.Errorf("session: emulator: %w", err)
	}
	minBytes := opts.ChunkMinBytes
	if minBytes <= 0 {
		minBytes = defaultChunkMinBytes
	}
	maxBytes := opts.ChunkMaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultChunkMaxBytes
	}
	outEvery := opts.OutFlushEvery
	if outEvery <= 0 {
		outEvery = defaultOutFlushEvery
	}
	keyEvery := opts.KeyFlushEvery
	if keyEvery <= 0 {
		keyEvery = defaultKeyFlushEvery
	}
	s := &Session{
		opts:    opts,
		clk:     clk,
		emu:     emu,
		parser:  input.NewParser(opts.Profile),
		guard:   draft.NewGuard(clk, opts.Profile),
		rec:     rec,
		file:    file,
		started: clk.Now(),
	}
	s.opts.ChunkMinBytes = minBytes
	s.opts.ChunkMaxBytes = maxBytes
	s.opts.OutFlushEvery = outEvery
	s.opts.KeyFlushEvery = keyEvery
	s.lastCols, s.lastRows = int(size.Cols), int(size.Rows)
	s.sigCore = make(chan os.Signal, 16)
	s.prepared = make(map[protocol.OpaqueID]pendingDelivery)
	s.inCh = make(chan []byte, pumpQueue)
	s.opCh = make(chan writerOp)
	s.stopCh = make(chan struct{})
	s.changed = make(chan struct{}, 1)
	s.snap = guardSnapshot{
		State:      s.guard.State(),
		Generation: s.guard.Generation(),
		Version:    s.guard.GuardVersion(),
	}
	s.running = true
	// The starting size goes first, before any resize can be reported:
	// a replay starts its screen from it.
	s.mu.Lock()
	s.emitLocked(protocol.EventTypePTYOpened, PTYOpened{
		Cols: s.lastCols, Rows: s.lastRows, Harness: protocol.Harness(opts.Profile.Harness),
		HarnessVersion: opts.HarnessVersion,
	})
	s.mu.Unlock()
	return s, nil
}

// CheckRecordingsDir refuses a recordings dir that is a link, not a
// directory, or not 0700: the check a run makes before it records.
func CheckRecordingsDir(dir string) error { return checkRecordingsDir(dir) }

// checkRecordingsDir enforces the 0700 promise on the live directory.
// MkdirAll neither fixes the mode of a directory that already exists nor
// refuses a symlink, so both are verified here, after creation.
func checkRecordingsDir(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("session: recordings dir: %w", err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return fmt.Errorf("session: recordings dir %s is not a directory", dir)
	}
	if fi.Mode().Perm() != recordingsPerm {
		return fmt.Errorf("session: recordings dir %s mode %04o, want 0700", dir, fi.Mode().Perm())
	}
	return nil
}

var (
	// errSessionDone reports use after Run returned.
	errSessionDone = errors.New("session: run is over")
	// errPumpInput reports the outer input pump failure.
	errPumpInput = errors.New("session: outer input pump")
	// errPumpOutput reports the outer output pump failure.
	errPumpOutput = errors.New("session: outer output pump")
)

// coreResult carries the proxy core outcome.
type coreResult struct {
	exit proxy.Exit
	err  error
}

// Run wraps one harness run until it exits: raw outer terminal, the
// intermediate PTY pair feeding the untouched core, pumps apart, and the
// event loop. It returns the harness exit. The owner closes In afterwards
// if it owns it; Run already left raw mode and deadlines behind.
func (s *Session) Run(ctx context.Context) (proxy.Exit, error) {
	inFD := int(s.opts.In.Fd())
	old, err := term.MakeRaw(inFD)
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("session: raw outer: %w", err)
	}
	defer func() { _ = term.Restore(inFD, old) }()
	waiter, err := newInputWaiter(s.opts.In)
	if err != nil {
		return proxy.Exit{}, err
	}
	defer func() { _ = waiter.close() }()
	midM, midS, err := pty.Open()
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("session: middle pty: %w", err)
	}
	defer func() { _ = midM.Close() }()
	defer func() { _ = midS.Close() }()
	outM, outS, err := pty.Open()
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("session: outer pty: %w", err)
	}
	defer func() { _ = outM.Close() }()
	defer func() { _ = outS.Close() }()
	s.mu.Lock()
	cols, rows := s.lastCols, s.lastRows
	s.mu.Unlock()
	if err := pty.Setsize(midS, winsize(cols, rows)); err != nil {
		return proxy.Exit{}, fmt.Errorf("session: size middle: %w", err)
	}
	if _, err := term.MakeRaw(int(outS.Fd())); err != nil {
		return proxy.Exit{}, fmt.Errorf("session: raw middle out: %w", err)
	}

	sigSO := make(chan os.Signal, 16)
	signal.Notify(sigSO, unix.SIGWINCH, unix.SIGHUP, unix.SIGINT, unix.SIGQUIT, unix.SIGTERM, unix.SIGUSR1, unix.SIGUSR2)
	defer signal.Stop(sigSO)
	s.mu.Lock()
	sigCore := s.sigCore
	s.mu.Unlock()

	coreCtx, coreCancel := context.WithCancel(ctx)
	defer coreCancel()
	coreDone := make(chan coreResult, 1)
	go func() {
		exit, err := proxy.Run(coreCtx, proxy.Options{
			Command:      s.opts.Command,
			Env:          s.opts.Env,
			In:           midS,
			Out:          outS,
			Notice:       s.opts.Notice,
			DaemonSocket: s.opts.DaemonSocket,
			Signals:      sigCore,
		})
		coreDone <- coreResult{exit: exit, err: err}
	}()

	done := make(chan struct{})
	chOut := make(chan []byte, pumpQueue)
	chIn := make(chan []byte, pumpQueue)
	pumpErr := make(chan error, 2)
	s.mu.Lock()
	s.mid = midS
	s.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(2)
	go s.pumpIn(waiter, s.inCh, pumpErr, done, &wg)
	writer := &sessionWriter{s: s, dst: midM, in: s.inCh, loop: chIn, ops: s.opCh, done: s.stopCh, perr: pumpErr}
	wg.Add(1)
	go writer.run(&wg)
	go s.pumpOut(outM, chOut, pumpErr, &wg)
	tickOut := time.NewTicker(s.opts.OutFlushEvery)
	defer tickOut.Stop()
	tickKey := time.NewTicker(s.opts.KeyFlushEvery)
	defer tickKey.Stop()

	var exit proxy.Exit
	var runErr error
	var finished bool
	for !finished {
		select {
		case data := <-chOut:
			s.onOutput(data)
		case data := <-chIn:
			s.onInput(data)
		case <-tickOut.C:
			s.mu.Lock()
			s.flushChunkTimeLocked()
			s.mu.Unlock()
		case <-tickKey.C:
			s.mu.Lock()
			s.flushKeysLocked()
			s.settleLocked()
			s.mu.Unlock()
		case sig := <-sigSO:
			s.onSignal(sig, sigCore)
		case perr := <-pumpErr:
			runErr = perr
			coreCancel()
		case res := <-coreDone:
			exit, runErr = res.exit, errors.Join(runErr, res.err)
			finished = true
		}
	}
	close(done)
	close(s.stopCh)
	waiter.cancel()
	s.mu.Lock()
	s.running = false
	s.sigCore = nil
	s.mu.Unlock()
	// Teardown in order: stop the middle output so the output pump drains
	// to EOF, drain it, then stop input the same way. Bytes the core
	// already wrote stay readable on the pair after close.
	_ = outS.Close()
	for data := range chOut {
		s.onOutput(data)
	}
	_ = midM.Close()
	for data := range chIn {
		s.onInput(data)
	}
	wg.Wait()
	s.mu.Lock()
	s.flushChunkLocked()
	s.flushKeysLocked()
	s.emitLocked(protocol.EventTypePTYClosed, PTYClosed{})
	fatal := s.fatalErr
	if s.file != nil {
		if err := s.file.Close(); err != nil && fatal == nil {
			fatal = fmt.Errorf("session: close recording: %w", err)
		}
		s.file = nil
	}
	s.closed = true
	_ = s.emu.Close()
	s.mu.Unlock()
	return exit, errors.Join(runErr, fatal)
}

// winsize builds a PTY size, clamping to the uint16 range a terminal
// holds; callers already refuse sizes below one.
func winsize(cols, rows int) *pty.Winsize {
	clamp := func(v int) uint16 { return uint16(min(max(v, 0), 0xffff)) }
	return &pty.Winsize{Cols: clamp(cols), Rows: clamp(rows)}
}

// writeFull writes all of b.
func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		b = b[n:]
		if err != nil {
			return fmt.Errorf("session: write: %w", err)
		}
	}
	return nil
}

// pumpIn reads outer keys and hands each chunk to the writer, which
// owns every middle PTY write. It never touches the descriptor: the
// send blocks when the writer stops receiving, so a full lease
// back-pressures the terminal instead of losing keys. It reads only
// once the waiter says the terminal is readable, so the session end
// interrupts it without closing the owner's terminal.
func (s *Session) pumpIn(iw *inputWaiter, out chan<- []byte, perr chan<- error, done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)
	buf := make([]byte, 32*1024)
	for {
		if err := iw.wait(); err != nil {
			if !errors.Is(err, errInputCanceled) {
				select {
				case perr <- fmt.Errorf("%w: %w", errPumpInput, err):
				case <-done:
				}
			}
			return
		}
		n, err := s.opts.In.Read(buf)
		if err != nil {
			select {
			case perr <- fmt.Errorf("%w: %w", errPumpInput, err):
			case <-done:
			}
			return
		}
		if n == 0 {
			continue
		}
		cp := append([]byte(nil), buf[:n]...)
		// Count the arrival under mu before enqueueing: a key the
		// terminal gave us refuses a commit even while it still
		// waits in the channel for the writer.
		s.mu.Lock()
		s.arrived++
		s.mu.Unlock()
		select {
		case out <- cp:
		case <-done:
			return
		}
	}
}

// pumpOut forwards middle output to the real terminal first, then hands a
// copy to the loop. A slow loop back-pressures the core pump through the
// channel, never the keys.
func (s *Session) pumpOut(r io.Reader, ch chan<- []byte, perr chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(ch)
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if werr := writeFull(s.opts.Out, buf[:n]); werr != nil {
				select {
				case perr <- fmt.Errorf("%w: %w", errPumpOutput, werr):
				default:
				}
				return
			}
			ch <- append([]byte(nil), buf[:n]...)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				select {
				case perr <- fmt.Errorf("%w: %w", errPumpOutput, err):
				default:
				}
			}
			return
		}
	}
}

// onOutput folds one outer-output piece: screen first, then the recording
// buffer, then the draft guard over the fresh snapshot. Small reads
// coalesce into the open piece up to ChunkMaxBytes, so each buffered
// piece still carries the screen hash after its own bytes. One flush at
// most runs here: flushChunkLocked stops at the first recording failure,
// so a second call would spin on the same bytes forever.
func (s *Session) onOutput(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) == 0 {
		snap := s.emu.Snapshot()
		s.lastSnap, s.hasSnap = snap, true
		for _, o := range s.guard.Observe(snap) {
			s.emitDraftLocked(o.Kind)
		}
		s.publishLocked()
		return
	}
	// Only the harness's output passes here: the Proxy's own notices
	// go straight to the terminal and never count.
	s.lastOutput = s.clk.Now()
	// Small reads coalesce into the open piece up to ChunkMaxBytes: the
	// contract promises aggregated 4-16 KiB pieces, not one record per
	// read. The open piece hash follows its bytes as they land, so a
	// short last piece still carries the screen after its own bytes.
	for len(data) > 0 {
		if len(s.pending) == 0 || len(s.pending[len(s.pending)-1].data) >= s.opts.ChunkMaxBytes {
			s.pending = append(s.pending, pendingPiece{})
			if s.chunkBorn.IsZero() {
				s.chunkBorn = s.clk.Now()
			}
		}
		last := &s.pending[len(s.pending)-1]
		n := min(len(data), s.opts.ChunkMaxBytes-len(last.data))
		if _, err := s.emu.Write(data[:n]); err != nil {
			s.failLocked(fmt.Errorf("session: emulator write: %w", err))
			return
		}
		last.data = append(last.data, data[:n]...)
		snap := s.emu.Snapshot()
		s.lastSnap, s.hasSnap = snap, true
		last.hash = vt.Hash(snap)
		s.buffered += n
		data = data[n:]
	}
	s.guard.Output(s.lastSnap)
	s.publishLocked()
	if s.buffered >= s.opts.ChunkMinBytes {
		s.flushChunkLocked()
	}
}

// failLocked keeps the first fatal error for the teardown.
func (s *Session) failLocked(err error) {
	if s.fatalErr == nil {
		s.fatalErr = err
	}
}

// flushChunkTimeLocked closes an output piece past its age.
func (s *Session) flushChunkTimeLocked() {
	if s.buffered == 0 || s.chunkBorn.IsZero() {
		return
	}
	if s.clk.Now().Sub(s.chunkBorn) < s.opts.OutFlushEvery {
		return
	}
	s.flushChunkLocked()
}

// flushChunkLocked records every buffered piece and emits one io.chunk
// event per piece with the hash its own bytes left. It stops at the
// first recording failure: the failed piece stays buffered and the
// caller must not flush again, or it would spin on the same bytes.
func (s *Session) flushChunkLocked() {
	for len(s.pending) > 0 {
		head := s.pending[0]
		seq, err := s.rec.Append(s.chunkBorn, s.monoLocked(s.chunkBorn), head.data)
		if err != nil {
			s.failLocked(err)
			return
		}
		s.emitLocked(protocol.EventTypeIOChunk, IOChunkEvent{
			ByteCount:         int64(len(head.data)),
			ANSIPresent:       bytes.Contains(head.data, []byte{0x1b}),
			ScreenHashAfter:   head.hash,
			RecordingSequence: seq,
			ChunkDigest:       ChunkDigest(head.data),
		})
		s.pending[0] = pendingPiece{}
		s.pending = s.pending[1:]
		s.buffered -= len(head.data)
	}
	s.chunkBorn = time.Time{}
}

// monoLocked counts nanoseconds from the session start.
func (s *Session) monoLocked(t time.Time) int64 {
	return t.Sub(s.started).Nanoseconds()
}

// emitLocked numbers one event and hands it to the sink. The sink must
// not call back into the Session.
func (s *Session) emitLocked(t protocol.EventType, payload any) {
	now := s.clk.Now()
	s.opts.Sink.Emit(Event{
		Session:     s.opts.SessionID,
		Type:        t,
		Payload:     payload,
		WallTime:    now,
		MonotonicNS: s.monoLocked(now),
		Sequence:    s.evSeq,
	})
	s.evSeq++
}

// emitDraftLocked reports a draft guard outcome with its generation.
func (s *Session) emitDraftLocked(kind draft.OutcomeKind) {
	var t protocol.EventType
	switch kind {
	case draft.OutcomeSubmitted:
		t = protocol.EventTypeInputDraftSubmitted
	case draft.OutcomeCancelled:
		t = protocol.EventTypeInputDraftCancelled
	default:
		t = protocol.EventTypeInputDraftCleared
	}
	s.emitLocked(t, InputDraft{
		State:        string(s.guard.State()),
		Generation:   s.guard.Generation(),
		GuardVersion: s.guard.GuardVersion(),
	})
}

// onInput folds one outer-input piece through the parser and the guard,
// emitting metadata only, never typed content.
func (s *Session) onInput(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastKey = s.clk.Now()
	for _, ev := range s.parser.Feed(data) {
		s.onInputEventLocked(ev)
	}
	// The harness already has the part of a key the parser holds back:
	// the guard learns of it now, not once the key is whole.
	if s.parser.Pending() {
		before, version := s.guard.State(), s.guard.GuardVersion()
		s.guard.Hold()
		s.reportMoveLocked(before, version)
	}
	// The chunk is fully through the parser and guard: count it and
	// publish the guard foto the delivery checks read.
	s.chunksSeen++
	s.publishLocked()
}

// unreserveLocked drops a commit reservation taken by CommitDelivery
// when that exact commit ends, on every ending: commit, abort,
// failure or teardown refusal. Callers hold s.mu.
func (s *Session) unreserveLocked(id protocol.OpaqueID) {
	if s.committing && s.committingID == id {
		s.committing = false
		s.committingID = ""
	}
}

// publishLocked records the guard foto the delivery checks read:
// chunks seen, state, generation, version, last submit, and whether
// the parser holds part of a human key sequence. Callers hold s.mu.
// Every path that moves the guard calls it, since the screen path
// (output, resize, poll) moves the guard without new input. Only a
// new guard version signals: a held sequence moves it only when it
// opens a draft.
func (s *Session) publishLocked() {
	prev := s.snap.Version
	s.snap = guardSnapshot{
		Chunks:     s.chunksSeen,
		State:      s.guard.State(),
		Generation: s.guard.Generation(),
		Version:    s.guard.GuardVersion(),
		LastSubmit: s.lastSubmit,
		LastOutput: s.lastOutput,
		Pending:    s.parser.Pending(),
	}
	if s.snap.Version != prev {
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
}

// settleLocked lets the guard confirm what the harness's quiet reaction
// to a key now shows, then publishes the guard. The loop's ticks settle
// it, and so does every reader of the guard first: one that reads
// between two ticks, on a clock that moved since, must not find a key
// waiting that the screen has already settled. Settling closes nothing
// a tick would not, and once the session stopped it does nothing.
// Callers hold s.mu.
func (s *Session) settleLocked() {
	if !s.running {
		return
	}
	for _, o := range s.guard.Poll() {
		s.emitDraftLocked(o.Kind)
	}
	s.publishLocked()
}

// onInputEventLocked maps one parsed key to metadata events and the guard.
func (s *Session) onInputEventLocked(ev input.Event) {
	switch ev.Kind {
	case input.KindText:
		s.printable++
		if s.pasting {
			s.pasteSize += int64(len(ev.Text))
		}
		s.guardFeedLocked(ev)
	case input.KindKey:
		s.flushKeysLocked()
		s.emitLocked(protocol.EventTypeInputControlKey, InputControlKey{
			Key:          string(ev.Key.Code),
			Shift:        ev.Key.Mod&input.ModShift != 0,
			Alt:          ev.Key.Mod&input.ModAlt != 0,
			Ctrl:         ev.Key.Mod&input.ModCtrl != 0,
			Generation:   s.guard.Generation(),
			GuardVersion: s.guard.GuardVersion(),
		})
		s.guardFeedLocked(ev)
	case input.KindSubmit:
		s.flushKeysLocked()
		s.lastSubmit = s.clk.Now()
		s.emitLocked(protocol.EventTypeInputSubmitDetected, InputSubmit{
			Generation:   s.guard.Generation(),
			GuardVersion: s.guard.GuardVersion(),
		})
		s.guardFeedLocked(ev)
	case input.KindCancel:
		s.flushKeysLocked()
		s.guardFeedLocked(ev)
	case input.KindClearLine, input.KindInsertNewline:
		s.guardFeedLocked(ev)
	case input.KindPasteStart:
		s.flushKeysLocked()
		s.pasting = true
		s.pasteSize = 0
		s.emitLocked(protocol.EventTypeInputPasteStarted, InputPaste{
			Generation:   s.guard.Generation(),
			GuardVersion: s.guard.GuardVersion(),
		})
		s.guardFeedLocked(ev)
	case input.KindPasteEnd:
		s.guardFeedLocked(ev)
		s.emitLocked(protocol.EventTypeInputPasteCompleted, InputPaste{
			SizeBytes:    s.pasteSize,
			Generation:   s.guard.Generation(),
			GuardVersion: s.guard.GuardVersion(),
		})
		s.pasting = false
		s.pasteSize = 0
	case input.KindUnknown:
		s.flushKeysLocked()
		s.emitLocked(protocol.EventTypeInputControlKey, InputControlKey{
			Key:          "unknown",
			Generation:   s.guard.Generation(),
			GuardVersion: s.guard.GuardVersion(),
		})
		s.guardFeedLocked(ev)
	}
}

// guardFeedLocked runs one key through the draft guard, reporting its
// transitions: outcomes by kind, any other move as reportMoveLocked
// does.
func (s *Session) guardFeedLocked(ev input.Event) {
	before, version := s.guard.State(), s.guard.GuardVersion()
	outs := s.guard.Feed(ev)
	for _, o := range outs {
		s.emitDraftLocked(o.Kind)
	}
	if len(outs) == 0 {
		s.reportMoveLocked(before, version)
	}
}

// reportMoveLocked reports a guard move from before at version that
// closed no draft: openings as opened, losing track as unknown, every
// other move as updated with the landing state, a key that only moved
// the version included. A guard that did not move reports nothing.
func (s *Session) reportMoveLocked(before draft.State, version int64) {
	after := s.guard.State()
	if after == before && s.guard.GuardVersion() == version {
		return
	}
	t := protocol.EventTypeInputDraftUpdated
	if before == draft.StateClosed && (after == draft.StateOpen || after == draft.StatePasting) {
		t = protocol.EventTypeInputDraftOpened
	}
	if after == draft.StateUnknown {
		t = protocol.EventTypeInputDraftUnknown
	}
	s.emitLocked(t, InputDraft{
		State:        string(after),
		Generation:   s.guard.Generation(),
		GuardVersion: s.guard.GuardVersion(),
	})
}

// flushKeysLocked emits a pending printable count, if any.
func (s *Session) flushKeysLocked() {
	if s.printable == 0 {
		return
	}
	s.emitLocked(protocol.EventTypeInputKeyChunk, InputKeyChunk{
		PrintableCount: s.printable,
		Generation:     s.guard.Generation(),
		GuardVersion:   s.guard.GuardVersion(),
	})
	s.printable = 0
}

// onSignal forwards outer signals to the core; SIGWINCH first resizes
// from the real terminal, skipping spurious repeats.
func (s *Session) onSignal(sig os.Signal, sigCore chan<- os.Signal) {
	if sig == unix.SIGWINCH {
		if size, err := pty.GetsizeFull(s.opts.Out); err == nil {
			s.mu.Lock()
			same := int(size.Cols) == s.lastCols && int(size.Rows) == s.lastRows
			s.mu.Unlock()
			if !same {
				_ = s.doResize(int(size.Cols), int(size.Rows), sigCore)
			}
		}
		return
	}
	select {
	case sigCore <- sig:
	default:
	}
}

// doResize records the pending output, resizes the middle terminal, the
// screen and the child (through the core's SIGWINCH path), then reports
// input.resize. A resize inside
// a paste loses sync, as the draft guard requires.
func (s *Session) doResize(cols, rows int, sigCore chan<- os.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSessionDone
	}
	if cols < 1 || rows < 1 {
		return fmt.Errorf("session: bad size %dx%d", cols, rows)
	}
	// Bytes that landed before the resize are recorded, and their
	// io.chunk events emitted, before it: a replay applies them in the
	// same order the live screen did.
	s.flushChunkLocked()
	if err := s.emu.Resize(cols, rows); err != nil {
		return fmt.Errorf("session: resize screen: %w", err)
	}
	if s.mid != nil {
		if err := pty.Setsize(s.mid, winsize(cols, rows)); err != nil {
			return fmt.Errorf("session: resize middle: %w", err)
		}
	}
	s.lastCols, s.lastRows = cols, rows
	select {
	case sigCore <- unix.SIGWINCH:
	default:
	}
	snap := s.emu.Snapshot()
	s.lastSnap, s.hasSnap = snap, true
	before := s.guard.State()
	for _, o := range s.guard.Observe(snap) {
		s.emitDraftLocked(o.Kind)
	}
	after := s.guard.State()
	s.publishLocked()
	if after == draft.StateUnknown && before != draft.StateUnknown {
		s.emitLocked(protocol.EventTypeInputDraftUnknown, InputDraft{
			State:        string(after),
			Generation:   s.guard.Generation(),
			GuardVersion: s.guard.GuardVersion(),
		})
	}
	s.emitLocked(protocol.EventTypeInputResize, InputResize{
		Cols:         cols,
		Rows:         rows,
		Generation:   s.guard.Generation(),
		GuardVersion: s.guard.GuardVersion(),
	})
	return nil
}

// Resize resizes the middle terminal, the screen and the child. It is
// safe to call while Run runs; afterwards it reports errSessionDone.
func (s *Session) Resize(cols, rows int) error {
	s.mu.Lock()
	running, sigCore := s.running, s.sigCore
	s.mu.Unlock()
	if !running || sigCore == nil {
		return errSessionDone
	}
	return s.doResize(cols, rows, sigCore)
}

// Flush records buffered output and pending input counts now.
func (s *Session) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushChunkLocked()
	s.flushKeysLocked()
}

// State exposes the draft guard state, settled first.
func (s *Session) State() draft.State {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleLocked()
	return s.guard.State()
}

// Generation exposes the draft generation, settled first.
func (s *Session) Generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleLocked()
	return s.guard.Generation()
}

// GuardVersion exposes the input guard version, settled first.
func (s *Session) GuardVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleLocked()
	return s.guard.GuardVersion()
}
