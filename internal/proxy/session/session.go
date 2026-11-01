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
	"github.com/smellak/gifford/internal/outputactivity"
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

// ErrDeferredOutputLimit means unresolved input outlived the private output
// spool. Capture stops with a gap, while the PTY and keyboard keep running.
var ErrDeferredOutputLimit = errors.New("session: deferred output limit reached")

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

	mu              sync.Mutex
	emu             *vt.Emulator
	parser          *input.Parser
	guard           *draft.Guard
	rec             *Recorder
	file            *os.File
	recDir          string
	deferred        *deferredOutput
	deferredLimit   int64
	captureDisabled bool
	started         time.Time
	evSeq           int64

	// pending holds buffered output in pieces of at most ChunkMaxBytes,
	// each with the screen hash after its own bytes.
	pending []pendingPiece
	// buffered counts the bytes in pending.
	buffered      int
	chunkBorn     time.Time
	deferredFlush bool
	lastSnap      vt.Snapshot
	hasSnap       bool

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
	// humanWritten counts outer-input chunks the writer put on the middle PTY;
	// only the writer goroutine adds to it. The legacy name distinguishes this
	// direction from harness output, not the chunk's parsed semantics.
	humanWritten atomic.Uint64
	// humanSent counts outer-input chunks the writer started to put on
	// the middle PTY, each before its write: past chunksSeen, the
	// harness may have bytes the loop has not folded yet, and may
	// answer them first, see onOutputRead. Only the writer goroutine
	// adds to it.
	humanSent atomic.Uint64
	// chunksSeen counts the chunks the loop fully processed; the
	// snapshot below publishes it with the guard truth after each
	// chunk. Guarded by mu.
	chunksSeen uint64
	// lastSubmit is the last human submit keypress. Guarded by mu.
	lastSubmit time.Time
	// lastOutput is when the harness last wrote output. Guarded by mu.
	lastOutput time.Time
	// lastEffectiveOutput is when output last changed covered terminal
	// state or could not be proved inert. It is ANALYZE-only; delivery and
	// guard timing continue to use lastOutput.
	lastEffectiveOutput time.Time
	activityDetector    outputactivity.Detector
	// lastKey is the last human input the loop read. Guarded by mu.
	lastKey time.Time
	// changed signals a new guard version to the agent socket,
	// without waiting: one pending signal is enough.
	changed chan struct{}
	// snap is the published guard foto. Guarded by mu.
	snap guardSnapshot
	// obs counts the state observations. Guarded by mu.
	obs observations
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
	obs, err := newObservations()
	if err != nil {
		return nil, err
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
		opts:          opts,
		clk:           clk,
		emu:           emu,
		parser:        input.NewParser(opts.Profile),
		guard:         draft.NewGuard(clk, opts.Profile),
		rec:           rec,
		file:          file,
		recDir:        recDir,
		deferredLimit: deferredOutputSpoolLimit,
		started:       clk.Now(),
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
	s.obs = obs
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
		HarnessVersion: opts.HarnessVersion, ActivityVersion: outputactivity.Version,
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
	chOut := make(chan outputRead, pumpQueue)
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
		case out := <-chOut:
			s.onOutputRead(out)
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
			s.observeExit(res)
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
	for out := range chOut {
		s.onOutputRead(out)
	}
	_ = midM.Close()
	for data := range chIn {
		s.onInput(data)
	}
	wg.Wait()
	s.mu.Lock()
	s.flushInputLocked()
	if !s.inputUnresolvedLocked() {
		s.flushChunkLocked()
	}
	s.flushKeysLocked()
	s.emitLocked(protocol.EventTypePTYClosed, PTYClosed{})
	fatal := s.fatalErr
	if s.file != nil {
		if err := s.file.Close(); err != nil && fatal == nil {
			fatal = fmt.Errorf("session: close recording: %w", err)
		}
		s.file = nil
	}
	if s.deferred != nil {
		if err := s.deferred.close(); err != nil && fatal == nil {
			fatal = err
		}
		s.deferred = nil
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

// outputRead is one read of the harness's output with sent, the human
// chunks the writer had sent once the read returned: the harness answers
// only bytes it was given, and the writer counts a chunk sent before it
// writes it, so this output answers none of the chunks sent after.
type outputRead struct {
	data []byte
	sent uint64
}

// pumpOut forwards middle output to the real terminal first, then hands a
// copy to the loop, with the human chunks sent once it was read. A slow
// loop back-pressures the core pump through the channel, never the keys.
func (s *Session) pumpOut(r io.Reader, ch chan<- outputRead, perr chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(ch)
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			sent := s.humanSent.Load()
			if werr := writeFull(s.opts.Out, buf[:n]); werr != nil {
				select {
				case perr <- fmt.Errorf("%w: %w", errPumpOutput, werr):
				default:
				}
				return
			}
			ch <- outputRead{data: append([]byte(nil), buf[:n]...), sent: sent}
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
//
// Output read just now may answer every human chunk the writer has sent:
// see onOutputRead.
func (s *Session) onOutput(data []byte) {
	s.onOutputRead(outputRead{data: data, sent: s.humanSent.Load()})
}

// onOutputRead folds output as onOutput does, out.sent being the outer-input
// chunks the writer had sent once the output was read. Output can follow
// outer-input bytes the parser has not classified yet: the writer puts a
// chunk on the harness before the loop takes its copy, and Run's select takes
// either channel first. That uncertainty suspends guard settlement and blocks
// delivery, but is not itself human evidence. Output observed across such an
// ordering is conservatively a re-read; only output whose sent count exactly
// matches the classified chunk count is established as a reaction.
//
// Nor is output read before a chunk the loop already folded was sent a
// reaction to that chunk's keys, though Run's select may take it after
// them: the guard takes its screen as a re-read, see draft.Guard.Observe,
// so however long it stays quiet it confirms nothing those keys wait
// for. Only output read after the chunk was sent can.
func (s *Session) onOutputRead(out outputRead) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncInputSuspensionLocked()
	data := out.data
	if len(data) == 0 {
		snap := s.emu.Snapshot()
		s.lastSnap, s.hasSnap = snap, true
		before, version := s.guard.State(), s.guard.GuardVersion()
		s.reportGuardResultsLocked(before, version, s.guard.Observe(snap))
		s.publishLocked()
		return
	}
	// Only the harness's output passes here: the Proxy's own notices
	// go straight to the terminal and never count.
	now := s.clk.Now()
	s.lastOutput = now
	effective := s.activityDetector.Observe(data)
	s.emitAtLocked(protocol.EventTypeIOOutputRead, IOOutputRead{
		ByteCount: int64(len(data)), ReadDigest: ChunkDigest(data), Effective: effective,
	}, now)
	if effective {
		s.observeOutputLocked(len(data), now)
	}
	// Small reads coalesce into the open piece up to ChunkMaxBytes: the
	// contract promises aggregated 4-16 KiB pieces, not one record per
	// read. The open piece hash follows its bytes as they land, so a
	// short last piece still carries the screen after its own bytes.
	for len(data) > 0 {
		if s.captureDisabled {
			n := min(len(data), s.opts.ChunkMaxBytes)
			if _, err := s.emu.Write(data[:n]); err != nil {
				s.failLocked(fmt.Errorf("session: emulator write: %w", err))
				return
			}
			snap := s.emu.Snapshot()
			s.lastSnap, s.hasSnap = snap, true
			data = data[n:]
			continue
		}
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
		if s.inputUnresolvedLocked() && s.buffered >= deferredOutputMemoryLimit {
			s.deferPendingLocked()
			if s.captureDisabled {
				continue
			}
			if s.fatalErr != nil {
				return
			}
		}
	}
	if out.sent == s.chunksSeen {
		s.guard.Output(s.lastSnap)
	} else {
		before, version := s.guard.State(), s.guard.GuardVersion()
		s.reportGuardResultsLocked(before, version, s.guard.Observe(s.lastSnap))
	}
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
	// Output that crossed unclassified outer input cannot yet be ordered
	// under a human-draft event. Spool it until the input resolves: a human
	// event is then recorded before the chunk, while a proven terminal
	// response leaves the previous draft state untouched.
	if s.inputUnresolvedLocked() {
		s.deferPendingLocked()
		return
	}
	if s.deferred != nil {
		deferred := s.deferred
		if err := deferred.drain(func(mono int64, piece pendingPiece) error {
			return s.recordPieceLocked(s.started.Add(time.Duration(mono)), piece)
		}); err != nil {
			s.disableCaptureLocked(err, protocol.CaptureGapCauseIngestDied, nil)
			return
		}
		if err := deferred.close(); err != nil {
			s.disableCaptureLocked(err, protocol.CaptureGapCauseIngestDied, nil)
			return
		}
		s.deferred = nil
	}
	s.deferredFlush = false
	for len(s.pending) > 0 {
		head := s.pending[0]
		if err := s.recordPieceLocked(s.chunkBorn, head); err != nil {
			s.disableCaptureLocked(err, protocol.CaptureGapCauseIngestDied, nil)
			return
		}
		s.pending[0] = pendingPiece{}
		s.pending = s.pending[1:]
		s.buffered -= len(head.data)
	}
	s.chunkBorn = time.Time{}
}

// deferPendingLocked moves unresolved output into a private, unlinked spool.
// It leaves only bounded output in RAM and emits no io.chunk event until input
// classification establishes whether a human event must precede it.
func (s *Session) deferPendingLocked() {
	s.deferredFlush = true
	if len(s.pending) == 0 {
		return
	}
	if s.deferred == nil {
		deferred, err := newDeferredOutput(s.recDir, s.opts.SessionID, s.opts.ChunkMaxBytes, s.deferredLimit)
		if err != nil {
			s.disableCaptureLocked(err, protocol.CaptureGapCauseIngestDied, nil)
			return
		}
		s.deferred = deferred
	}
	mono := s.monoLocked(s.chunkBorn)
	for len(s.pending) > 0 {
		head := s.pending[0]
		if err := s.deferred.append(mono, head); err != nil {
			var limitErr *deferredOutputLimitError
			if errors.As(err, &limitErr) {
				s.disableCaptureLocked(limitErr, protocol.CaptureGapCauseQuota, limitErr)
				return
			}
			s.disableCaptureLocked(err, protocol.CaptureGapCauseIngestDied, nil)
			return
		}
		s.pending[0] = pendingPiece{}
		s.pending = s.pending[1:]
		s.buffered -= len(head.data)
	}
	s.chunkBorn = time.Time{}
}

// disableCaptureLocked records one explicit gap and stops retaining output.
// Screen and guard processing continue, so neither the PTY nor human input
// depends on capture capacity or persistence.
func (s *Session) disableCaptureLocked(err error, cause protocol.CaptureGapCause, quota *deferredOutputLimitError) {
	if s.captureDisabled {
		s.failLocked(err)
		return
	}
	s.captureDisabled = true
	s.pending = nil
	s.buffered = 0
	s.chunkBorn = time.Time{}
	s.failLocked(err)
	if quota == nil {
		if s.deferred != nil {
			_ = s.deferred.close()
			s.deferred = nil
		}
		s.deferredFlush = false
	}
	id := protocol.OpaqueID(s.opts.SessionID)
	if quota != nil {
		s.emitLocked(protocol.EventTypeCaptureQuotaReached, protocol.CaptureQuotaReached{
			CaptureSessionID: id,
			QuotaScope:       protocol.QuotaScopeAgent,
			Bytes:            quota.bytes,
			LimitBytes:       quota.limit,
		})
	}
	s.emitLocked(protocol.EventTypeCaptureGapDetected, protocol.CaptureGapDetected{
		CaptureSessionID: id,
		Cause:            cause,
	})
}

func (s *Session) recordPieceLocked(at time.Time, piece pendingPiece) error {
	seq, err := s.rec.Append(at, s.monoLocked(at), piece.data)
	if err != nil {
		return err
	}
	s.emitLocked(protocol.EventTypeIOChunk, IOChunkEvent{
		ByteCount:         int64(len(piece.data)),
		ANSIPresent:       bytes.Contains(piece.data, []byte{0x1b}),
		ScreenHashAfter:   piece.hash,
		RecordingSequence: seq,
		ChunkDigest:       ChunkDigest(piece.data),
	})
	return nil
}

// monoLocked counts nanoseconds from the session start.
func (s *Session) monoLocked(t time.Time) int64 {
	return t.Sub(s.started).Nanoseconds()
}

// emitLocked numbers one event and hands it to the sink. The sink must
// not call back into the Session.
func (s *Session) emitLocked(t protocol.EventType, payload any) {
	now := s.clk.Now()
	s.emitAtLocked(t, payload, now)
}

// emitAtLocked emits at a time already read from the session clock. A raw
// output read uses one instant for its quiet barrier, activity mark and
// replay evidence.
func (s *Session) emitAtLocked(t protocol.EventType, payload any, now time.Time) {
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
	events := s.parser.Feed(data)
	human := false
	for _, ev := range events {
		if ev.Kind != input.KindTerminal {
			human = true
			break
		}
	}
	if human {
		s.lastKey = s.clk.Now()
	}
	for _, ev := range events {
		s.onInputEventLocked(ev)
	}
	// The chunk is fully through the parser and guard: count it and
	// publish the guard foto the delivery checks read. Pending bytes and
	// later writer chunks suspend settlement without inventing a human key;
	// a human event supplies that evidence when it resolves.
	s.chunksSeen++
	s.syncInputSuspensionLocked()
	s.publishLocked()
	if s.deferredFlush && !s.inputUnresolvedLocked() {
		s.flushChunkLocked()
	}
}

// discardUnresolvedOutputLocked drops output that cannot be ordered after the
// input and output pumps have both drained. The private spool is already
// unlinked; the capture gap is the durable evidence that its bytes are absent.
func (s *Session) discardUnresolvedOutputLocked() {
	lost := s.deferred != nil && s.deferred.count > 0
	if s.deferred != nil {
		if err := s.deferred.close(); err != nil {
			s.failLocked(err)
		}
		s.deferred = nil
	}
	s.pending = nil
	s.buffered = 0
	s.chunkBorn = time.Time{}
	s.deferredFlush = false
	if !lost || s.captureDisabled {
		return
	}
	s.emitLocked(protocol.EventTypeCaptureGapDetected, protocol.CaptureGapDetected{
		CaptureSessionID: protocol.OpaqueID(s.opts.SessionID),
		Cause:            protocol.CaptureGapCauseIngestDied,
	})
}

// flushInputLocked resolves a truncated input unit at teardown. Terminal
// strings replay conservatively as their real Alt/control bytes, so no human
// key can disappear merely because the child exited before the terminator.
// Callers hold s.mu.
func (s *Session) flushInputLocked() {
	events := s.parser.Flush()
	for _, ev := range events {
		if ev.Kind != input.KindTerminal {
			s.lastKey = s.clk.Now()
			break
		}
	}
	for _, ev := range events {
		s.onInputEventLocked(ev)
	}
	s.syncInputSuspensionLocked()
	s.publishLocked()
	if s.deferredFlush && !s.inputUnresolvedLocked() {
		s.flushChunkLocked()
	}
	if s.inputUnresolvedLocked() {
		// Both pumps have been drained before teardown calls this method. Any
		// remaining transport gap is therefore permanent: keep the human bytes
		// private, discard output whose ordering cannot be proved, and make the
		// lost capture visible to replay and dataset consumers.
		s.deferPendingLocked()
		s.discardUnresolvedOutputLocked()
	}
}

// inputUnresolvedLocked reports transport uncertainty: parser bytes waiting
// for classification, or outer-input chunks the writer sent but the loop has
// not folded. It blocks delivery and screen settlement, but says nothing yet
// about whether a human acted. Callers hold s.mu.
func (s *Session) inputUnresolvedLocked() bool {
	return s.parser.Pending() || s.humanSent.Load() > s.chunksSeen
}

// syncInputSuspensionLocked projects transport uncertainty into the guard's
// settlement gate without changing its human state. Callers hold s.mu.
func (s *Session) syncInputSuspensionLocked() {
	if s.inputUnresolvedLocked() {
		s.guard.SuspendInput()
		return
	}
	s.guard.ResumeInput()
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
// outer input remains unclassified. Callers hold s.mu.
// Every path that moves the guard calls it, since the screen path
// (output, resize, poll) moves the guard without new input. Only a new guard
// version signals; transport-only pending state deliberately does not.
func (s *Session) publishLocked() {
	prev := s.snap.Version
	s.snap = guardSnapshot{
		Chunks:     s.chunksSeen,
		State:      s.guard.State(),
		Generation: s.guard.Generation(),
		Version:    s.guard.GuardVersion(),
		LastSubmit: s.lastSubmit,
		LastOutput: s.lastOutput,
		Pending:    s.inputUnresolvedLocked(),
	}
	if s.snap.Version != prev {
		select {
		case s.changed <- struct{}{}:
		default:
		}
		s.observationChangedLocked()
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
	s.syncInputSuspensionLocked()
	before, version := s.guard.State(), s.guard.GuardVersion()
	s.reportGuardResultsLocked(before, version, s.guard.Poll())
	s.publishLocked()
}

// reportGuardResultsLocked records outcomes, or a state move that settles
// without one. Clock-only version changes stay in the published snapshot;
// they are activity changes, not draft input events.
func (s *Session) reportGuardResultsLocked(before draft.State, version int64, outcomes []draft.Outcome) {
	for _, outcome := range outcomes {
		s.emitDraftLocked(outcome.Kind)
	}
	if len(outcomes) == 0 && s.guard.State() != before {
		s.reportMoveLocked(before, version)
	}
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
	case input.KindTerminal:
		// Complete terminal responses, focus reports, and passive mouse
		// motion have no human semantics and carry no payload to events.
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
	s.observationChangedLocked()
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
