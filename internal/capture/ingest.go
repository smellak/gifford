package capture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/xdg"
)

// Ingest exit causes, written to ingest.exit.
const (
	ExitEOF     = "eof"
	ExitStall   = "stall"
	ExitQuota   = "quota"
	ExitStop    = "stop"
	ExitError   = "error"
	ExitRefused = "refused"
)

// ingestLockBound caps the start's wait for the session lock.
const ingestLockBound = 30 * time.Second

// exitGrace caps the shutdown drain once it begins: a stuck writer no
// longer bounds a healthy streaming life. It is a variable so tests
// shrink it; production always runs the stated bound.
var exitGrace = 10 * time.Second

// readPollMs bounds one reader poll: shutdown checks run at least this
// often.
const readPollMs = 100

// flushBatchCap flushes at 200 pending events, under MaxBatchEvents.
const flushBatchCap = 200

// IngestDeps is ingest's injectable environment: the tmux runner for
// the resize poller, the process id, and the clocks.
type IngestDeps struct {
	Runner tmuxRunner
	PID    int
	Clock  clock.Clock
	Mono   Mono
}

// Ingest runs one session writer: it adopts the pane stream, takes
// the session lock, checks the start token, then reads, chunks,
// flushes and rotates until EOF, stall, stop, quota or error. It
// returns the exit cause; a nil error means that path completed.
// Usage failures return ErrUsage before any path is built.
func Ingest(ctx context.Context, deps IngestDeps, opts IngestOptions) (string, error) {
	if opts.In == nil {
		return "", fmt.Errorf("%w: no pane stream", ErrUsage)
	}
	if err := validateCaptureSessionID(opts.Session); err != nil {
		_ = opts.In.Close()
		return "", fmt.Errorf("%w: bad session: %w", ErrUsage, err)
	}
	if !protocol.AgentID(opts.Agent).Valid() {
		_ = opts.In.Close()
		return "", fmt.Errorf("%w: bad agent id", ErrUsage)
	}
	if opts.StartSequence < 0 {
		_ = opts.In.Close()
		return "", fmt.Errorf("%w: bad start sequence", ErrUsage)
	}
	if err := validateNonce(opts.Attempt); err != nil {
		_ = opts.In.Close()
		return "", fmt.Errorf("%w: bad attempt: %w", ErrUsage, err)
	}
	r := &ingestRun{deps: deps, opts: opts}
	return r.run(ctx)
}

// ingestRun is one ingest run: the adopted stream, the held lock, the
// writer state and the shutdown flags.
type ingestRun struct {
	deps  IngestDeps
	opts  IngestOptions
	data  string
	root  *os.Root
	store *Store
	row   SessionRow
	lim   Limits
	node  string
	lock  *os.File
	// mu guards the lock descriptor, the released flag and the
	// poller stop: the shutdown path steals nothing, but it reads
	// all three while the writer may still use them.
	mu sync.Mutex

	stream      *os.File
	streamOnce  sync.Once
	streamDead  atomic.Bool
	readerDone  chan struct{}
	readErr     error
	readEOF     bool
	readStall   bool
	queue       chan readStamp
	queuedBytes atomic.Int64
	stopFlag    atomic.Bool

	log *capLog

	nextSeq     int64
	batch       []protocol.EventEnvelope
	segs        map[string]*segInfo
	segRows     map[string]bool
	dirtySegs   []string
	curID       string
	curFiles    *segFiles
	segStart    int64
	granted     int64
	baseData    int64
	written     int64
	lastNeed    int64
	chunkBuf    []byte
	chunkFirst  readStamp
	chunkOpen   bool
	newSegFiles bool
	flushedRaw  int64
	stopReq     *stopRequest
	resizeMu    sync.Mutex
	resizeCol   int64
	resizeRow   int64
	resizeHit   bool
	pollStop    context.CancelFunc
	pollerWg    sync.WaitGroup
	unlocked    bool
}

// leakedLocks keeps grace-expired lock descriptors reachable: their
// finalizers must never close a lock a blocked writer may still hold.
var (
	leakedLocksMu sync.Mutex
	leakedLocks   []*os.File
)

// ingestOutcome is one finished run: its exit cause and error.
type ingestOutcome struct {
	cause string
	err   error
}

// run adopts the stream and drives start, write and exit. A healthy
// streaming life is unbounded: the exit grace only caps the shutdown
// drain once it begins, which is the reader finishing or the context
// ending. Past the grace the lock descriptor stays open and the run
// keeps it: no second writer can start while the blocked one may
// still write, and the late writer still releases it when done.
func (r *ingestRun) run(ctx context.Context) (string, error) {
	stream, err := adoptStream(r.opts.In)
	if err != nil {
		_ = r.opts.In.Close()
		return "", err
	}
	r.stream = stream
	defer r.streamClose()
	data, err := r.dataDir()
	if err != nil {
		return "", err
	}
	r.data = data
	root, err := openCaptureRoot(data, false)
	if err != nil {
		return "", err
	}
	r.root = root
	defer func() { _ = root.Close() }()
	if err := r.readSessionMeta(); err != nil {
		return "", err
	}
	if err := r.takeLock(); err != nil {
		return "", err
	}
	r.readerDone = make(chan struct{})
	done := make(chan ingestOutcome, 1)
	go func() {
		cause, err := r.start(ctx)
		done <- ingestOutcome{cause: cause, err: err}
	}()
	select {
	case out := <-done:
		return out.cause, out.err
	case <-ctx.Done():
		r.beginShutdown()
		return r.awaitDone(ctx, done)
	case <-r.readerDone:
		return r.awaitDone(ctx, done)
	}
}

// beginShutdown stops the intake so the writer drains: the stream
// closes, the reader ends, and the poller stops. It is idempotent.
func (r *ingestRun) beginShutdown() {
	r.streamClose()
	r.stopFlag.Store(true)
	r.mu.Lock()
	stop := r.pollStop
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// awaitDone waits for the draining writer, at most the exit grace.
// A stuck drain keeps the lock and reports the expired grace.
func (r *ingestRun) awaitDone(ctx context.Context, done <-chan ingestOutcome) (string, error) {
	timer := time.NewTimer(exitGrace)
	defer timer.Stop()
	for {
		select {
		case out := <-done:
			return out.cause, out.err
		case <-ctx.Done():
			r.beginShutdown()
		case <-timer.C:
			r.leakLock()
			return ExitError, fmt.Errorf("exit grace expired")
		}
	}
}

// leakLock keeps a lock whose writer is stuck past the grace: the
// descriptor stays open and registered, so no second writer can start
// while the blocked one may still write. The late writer still owns
// the descriptor and releases it when its drain finishes.
func (r *ingestRun) leakLock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lock == nil {
		return
	}
	leakedLocksMu.Lock()
	leakedLocks = append(leakedLocks, r.lock)
	leakedLocksMu.Unlock()
}

// dataDir resolves the ingest data directory: --data-dir, or the
// platform default. It touches nothing.
func (r *ingestRun) dataDir() (string, error) {
	if r.opts.Data != "" {
		return r.opts.Data, nil
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return "", fmt.Errorf("%w: data dir: %w", ErrUsage, err)
	}
	return dirs.Data, nil
}

// streamClose closes the owned stream exactly once.
func (r *ingestRun) streamClose() {
	r.streamOnce.Do(func() {
		r.streamDead.Store(true)
		_ = r.stream.Close()
	})
}

// unlock releases the session lock once. It shares the mutex with the
// grace path, so a late drain still frees the lock it kept.
func (r *ingestRun) unlock() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lock != nil && !r.unlocked {
		r.unlocked = true
		_ = releaseLock(r.lock)
	}
}

// markHeld rewrites the lock line on the held descriptor, failing
// when the run holds no lock.
func (r *ingestRun) markHeld(line string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lock == nil {
		return fmt.Errorf("%w: no session lock", ErrUnsafePath)
	}
	return writeLockLine(r.lock, line)
}

// readSessionMeta opens the root without creating it, checks the
// session directory, and reads the immutable metadata with its
// limits. Any failure writes nothing at all: no lock is held.
func (r *ingestRun) readSessionMeta() error {
	dir := r.opts.Agent + "/" + r.opts.Session
	if _, err := openChecked(r.root, dir, true); err != nil {
		return err
	}
	var meta SessionMeta
	if err := readControlAt(r.root, dir+"/session.json", 16384, &meta); err != nil {
		return err
	}
	if meta.CaptureSessionID != r.opts.Session || meta.AgentID != r.opts.Agent {
		return fmt.Errorf("%w: session identity mismatch", ErrUnsafePath)
	}
	r.lim = meta.Limits.resolve()
	if err := r.lim.Validate(); err != nil {
		return fmt.Errorf("%w: bad session limits: %w", ErrUnsafePath, err)
	}
	return nil
}

// takeLock takes the session lock, retrying up to the bound. Past it
// the stream closes and nothing at all is written. It retries with
// its own loop because withLock's held descriptor would block a
// second exclusive take of the same file.
func (r *ingestRun) takeLock() error {
	name := r.opts.Agent + "/" + r.opts.Session + "/ingest.lock"
	if err := ensureLock(r.root, name); err != nil {
		return err
	}
	for i := 0; ; i++ {
		held, err := tryLock(r.root, name, true)
		if err == nil {
			r.mu.Lock()
			r.lock = held
			r.mu.Unlock()
			return nil
		}
		if !errors.Is(err, ErrBusy) {
			return err
		}
		if time.Duration(i+1)*lockPollInterval >= ingestLockBound {
			return err
		}
		time.Sleep(lockPollInterval)
	}
}

// start runs the §8.2 step 5 checks, the repair, the budget and the
// writer. It returns the exit cause with a nil error when that path
// completed.
func (r *ingestRun) start(ctx context.Context) (string, error) {
	if err := r.openLog(); err != nil {
		return r.exitError(fmt.Errorf("open log: %w", err))
	}
	node, err := readNodeID(r.root)
	if err != nil {
		return r.exitError(fmt.Errorf("node id: %w", err))
	}
	r.node = node
	store, err := OpenStore(ctx, filepath.Join(r.data, "gifford.db"))
	if err != nil {
		return r.exitError(fmt.Errorf("open store: %w", err))
	}
	r.store = store
	defer func() { _ = store.Close() }()
	row, found, err := store.SessionRow(ctx, r.opts.Session)
	if err != nil {
		return r.exitError(fmt.Errorf("read row: %w", err))
	}
	if !found {
		return r.refuse(ctx, "no row")
	}
	r.row = row
	events, err := readAllEvents(ctx, store, r.opts.Session)
	if err != nil {
		return r.exitError(fmt.Errorf("read events: %w", err))
	}
	if !r.tokenOK(events) {
		return r.refuse(ctx, "start token mismatch")
	}
	if req, err := readStopRequest(r.root, r.opts.Agent, r.opts.Session); err != nil {
		return r.exitError(fmt.Errorf("stop request: %w", err))
	} else if req != nil {
		return r.stopEarly(ctx, req)
	}
	if row.State == "REATTACHING" {
		row.State = stateAttached
		if err := store.UpdateSession(ctx, row); err != nil {
			return r.exitError(fmt.Errorf("repair cache: %w", err))
		}
		r.row = row
	}
	if err := r.markHeld(fmt.Sprintf("ingest %d %s", r.deps.PID, r.opts.Attempt)); err != nil {
		return r.exitError(fmt.Errorf("readiness marker: %w", err))
	}
	if err := store.RepairSession(ctx, r.root, r.opts.Agent, r.opts.Session, r.deps.Clock); err != nil {
		return r.exitError(fmt.Errorf("repair: %w", err))
	}
	row, _, err = store.SessionRow(ctx, r.opts.Session)
	if err != nil {
		return r.exitError(fmt.Errorf("re-read row: %w", err))
	}
	r.row = row
	if cause, done, err := r.budgetOrStop(ctx); err != nil {
		return cause, err
	} else if done {
		return cause, nil
	}
	return r.loop(ctx)
}

// tokenOK checks the §6.4 start token: the row exists with a committed
// attach, the session is live, the next sequence matches, and the
// latest boundary screen before n carries this attempt's nonce.
func (r *ingestRun) tokenOK(events []protocol.EventEnvelope) bool {
	if DeriveLife(true, events) != LifeLive {
		return false
	}
	if maxSequence(events)+1 != r.opts.StartSequence {
		return false
	}
	n := r.opts.StartSequence
	var screen *protocol.EventEnvelope
	for i := range events {
		ev := &events[i]
		if ev.EventType == protocol.EventTypeCaptureInitialScreen && ev.Sequence < n {
			screen = ev
		}
	}
	if screen == nil {
		return false
	}
	var initial protocol.CaptureInitialScreen
	if err := decodePayload(screen, &initial); err != nil {
		return false
	}
	// The boundary carries this attempt's nonce: attach publishes
	// initial-1-<nonce>.ansi, so the check is containment, not a
	// suffix match.
	return strings.Contains(string(initial.SnapshotBlobRef), "-"+r.opts.Attempt)
}

// refuse runs the §6.4 refusal: the stream closes, no segment opens
// and no stream event is appended; the rows are repaired, the bounded
// exit record and log lines are written, and the lock releases.
func (r *ingestRun) refuse(ctx context.Context, reason string) (string, error) {
	r.logLine("refused: " + reason)
	if r.store != nil {
		if err := r.store.RepairSession(ctx, r.root, r.opts.Agent, r.opts.Session, r.deps.Clock); err != nil {
			return r.exitError(fmt.Errorf("repair: %w", err))
		}
	}
	r.writeExit(ExitRefused)
	r.unlock()
	return ExitRefused, nil
}

// stopEarly appends the stop the lock holder found waiting: stopped at
// n with STOPPED, then the stream closes and the lock releases.
func (r *ingestRun) stopEarly(ctx context.Context, req *stopRequest) (string, error) {
	r.logLine("stop requested: " + req.Reason)
	ev, err := stoppedEnvelopeFor(r.deps.Clock, r.node, r.row, r.opts.StartSequence, req.Reason, req.Detail, r.deps.Mono.MonotonicNS())
	if err != nil {
		return r.exitError(err)
	}
	if err := r.store.AppendEvents(ctx, []protocol.EventEnvelope{ev}); err != nil {
		return r.exitError(fmt.Errorf("append stop: %w", err))
	}
	r.row.State = stoppedStateOf(req.Reason)
	r.row.StoppedAt = formatWall(ev.WallTime)
	r.row.StopReason = req.Reason
	if err := r.store.UpdateSession(ctx, r.row); err != nil {
		return r.exitError(fmt.Errorf("stop row: %w", err))
	}
	r.writeExit(ExitStop)
	r.unlock()
	return ExitStop, nil
}

// stoppedStateOf maps a stop request reason to its row state.
func stoppedStateOf(reason string) string {
	return StoppedRowState(protocol.CaptureStopReason(reason))
}

// exitError writes the error exit record, releases the lock and
// reports the failure.
func (r *ingestRun) exitError(err error) (string, error) {
	r.logLine("error: " + err.Error())
	r.writeExit(ExitError)
	r.unlock()
	return ExitError, err
}
