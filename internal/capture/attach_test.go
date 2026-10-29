package capture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// advClock advances on every read, so bound polls terminate. Reads
// are serialized: the reader and the writer share the clock.
type advClock struct {
	mu   sync.Mutex
	now  time.Time
	step time.Duration
}

func (c *advClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(c.step)
	return c.now
}

// fakeMono is a fixed monotonic source.
type fakeMono struct{ v int64 }

func (f fakeMono) MonotonicNS() int64 { return f.v }

// fakeRunner scripts the typed tmux interface and logs calls.
type fakeRunner struct {
	serverOut serverFacts
	paneBy    map[string]paneFacts
	openOut   openOutcome
	openErr   error
	calls     []string
	onOpen    func()
}

func (f *fakeRunner) server(_ context.Context) (serverFacts, error) {
	f.calls = append(f.calls, "server")
	return f.serverOut, nil
}

func (f *fakeRunner) pane(_ context.Context, target string) (paneFacts, error) {
	f.calls = append(f.calls, "pane:"+target)
	p, ok := f.paneBy[target]
	if !ok {
		return paneFacts{}, fmt.Errorf("%w: no such pane", ErrPaneNotFound)
	}
	return p, nil
}

func (f *fakeRunner) panes(_ context.Context) ([]paneFacts, error) {
	f.calls = append(f.calls, "panes")
	return nil, nil
}

func (f *fakeRunner) open(_ context.Context, _ openRequest) (openOutcome, error) {
	f.calls = append(f.calls, "open")
	if f.onOpen != nil {
		f.onOpen()
	}
	return f.openOut, f.openErr
}

const (
	attachSocket = "/tmp/attach-test.sock"
	attachTarget = "%7"
)

func attachServer() serverFacts {
	return serverFacts{pid: 4242, startTime: 1793231354, socket: attachSocket}
}

func attachPane() paneFacts {
	return paneFacts{
		id: "%7", sessionID: "$3", sessionName: "sess",
		windowIndex: 1, paneIndex: 2, cols: 80, rows: 24,
	}
}

// skipUnlessAttach skips entry-point tests where flock is unavailable.
func skipUnlessAttach(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("flock entry points need unix")
	}
}

func attachDeps(runner *fakeRunner, data string, clk *advClock) AttachDeps {
	return AttachDeps{
		Runner:       runner,
		Discover:     func(context.Context) (string, error) { return attachSocket, nil },
		Exe:          func() (string, error) { return "/bin/gifford", nil },
		PID:          4242,
		Clock:        clk,
		Mono:         fakeMono{v: 1000},
		DataDir:      func() string { return data },
		ReadyTimeout: 200 * time.Millisecond, ReadyPoll: 10 * time.Millisecond,
	}
}

func attachClock() *advClock {
	return &advClock{now: time.Date(2026, time.January, 2, 5, 0, 0, 0, time.UTC), step: 20 * time.Millisecond}
}

func attachOpts() AttachOptions {
	return AttachOptions{
		Agent: "miner-1", Harness: protocol.HarnessMuseCode,
		Target: attachTarget, Socket: attachSocket, Data: "",
	}
}

// sessionFileCount counts the session directory entries.
func sessionFileCount(t *testing.T, data, agent, session string) int {
	t.Helper()
	names, err := os.ReadDir(filepath.Join(data, "captures", agent, session))
	if err != nil {
		t.Fatal(err)
	}
	return len(names)
}

func TestAttachReadinessUnconfirmed(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	snapshot := []byte("\x1b[0mhello-pane")
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{opened: true, prePipe: false, snapshot: snapshot},
	}
	deps := attachDeps(runner, data, attachClock())
	_, err := Attach(context.Background(), deps, attachOpts())
	if !errors.Is(err, ErrAttach) {
		t.Fatalf("attach err = %v, want ErrAttach", err)
	}
	// The commit happened before the handover timed out: row, files
	// and the two events exist. The timeout path is read-only: the row
	// stays ATTACHED and no third event is appended.
	if err.Error() != "capture attach failed: capture committed; readiness unconfirmed" {
		t.Fatalf("attach err = %v", err)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.AllSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("sessions = %d, want 1", len(rows))
	}
	row := rows[0]
	if row.State != stateAttached {
		t.Fatalf("row = %+v, want ATTACHED", row)
	}
	if row.InitialScreenBlobRef == "" {
		t.Fatal("missing snapshot ref")
	}
	events, err := store.SessionEvents(context.Background(), row.CaptureSessionID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want attached/screen only", len(events))
	}
	if n := sessionFileCount(t, data, "miner-1", row.CaptureSessionID); n < 5 {
		t.Fatalf("session files = %d, want session.json, snapshot, locks, ledger", n)
	}
}

// TestAttachCommitRowFailureHandsOver injects a row-cache failure
// after the boundary append: the capture is already committed, so the
// report goes through readiness, the events stay, and the reservation
// the committed capture owns is never rolled back.
func TestAttachCommitRowFailureHandsOver(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	snapshot := []byte("\x1b[0mhello-pane")
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{opened: true, prePipe: false, snapshot: snapshot},
	}
	deps := attachDeps(runner, data, attachClock())
	deps.CommitRow = func(context.Context, SessionRow) error {
		return errors.New("synthetic row-cache failure")
	}
	_, err := Attach(context.Background(), deps, attachOpts())
	if !errors.Is(err, ErrAttach) ||
		err.Error() != "capture attach failed: capture committed; readiness unconfirmed" {
		t.Fatalf("attach err = %v", err)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.AllSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("sessions = %d, want 1", len(rows))
	}
	events, err := store.SessionEvents(context.Background(), rows[0].CaptureSessionID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want the committed attached/screen", len(events))
	}
	lim := attachOpts().Limits.resolve()
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	ledger, err := readLedger(root, attachOpts().Agent, rows[0].CaptureSessionID)
	if err != nil {
		t.Fatal(err)
	}
	// Abort would have lowered the ledger by SnapshotMaxBytes minus
	// the snapshot size; the committed capture keeps its floor.
	floor := controlRetain + lim.LogMaxBytes + int64(len(snapshot))
	if ledger < floor {
		t.Fatalf("ledger = %d, want at least %d (no rollback)", ledger, floor)
	}
}

func TestAttachRefusedPipe(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{refused: true, reason: refusePipePresent},
	}
	_, err := Attach(context.Background(), attachDeps(runner, data, attachClock()), attachOpts())
	if !errors.Is(err, ErrPipeExists) {
		t.Fatalf("attach err = %v, want ErrPipeExists", err)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.AllSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("sessions = %d, want none", len(rows))
	}
}

func TestAttachAgentConflict(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	ctx := context.Background()
	// A live session of the same agent already claims a capture.
	store, err := OpenStore(ctx, filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	row := testSessionRow()
	if err := store.InsertSession(ctx, row); err != nil {
		t.Fatal(err)
	}
	prior := []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
	}
	if err := store.AppendEvents(ctx, prior); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	makeSession(t, root, row.AgentID, row.CaptureSessionID)
	lim := Limits{}.resolve()
	if err := publishSessionJSON(root, attachOpts(), row.CaptureSessionID, "capture-0123456789abcdef0123456789abcdef", attachServer(), attachPane(), lim); err != nil {
		t.Fatal(err)
	}
	_ = root.Close()
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
	}
	_, err = Attach(ctx, attachDeps(runner, data, attachClock()), attachOpts())
	if !errors.Is(err, ErrAlreadyCaptured) {
		t.Fatalf("attach err = %v, want ErrAlreadyCaptured", err)
	}
}

func TestAttachValidation(t *testing.T) {
	data := t.TempDir()
	runner := &fakeRunner{}
	bad := attachOpts()
	bad.Agent = "Miner-1"
	if _, err := Attach(context.Background(), attachDeps(runner, data, attachClock()), bad); !errors.Is(err, ErrUsage) {
		t.Fatalf("bad agent err = %v, want ErrUsage", err)
	}
	if _, err := os.Stat(filepath.Join(data, "captures")); !os.IsNotExist(err) {
		t.Fatal("validation created state")
	}
}

func TestAttachAbortUnknown(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openErr:   fmt.Errorf("%w: deadline", ErrTmuxCommand),
	}
	_, err := Attach(context.Background(), attachDeps(runner, data, attachClock()), attachOpts())
	if !errors.Is(err, ErrAttach) {
		t.Fatalf("attach err = %v, want ErrAttach", err)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.AllSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("sessions = %d, want none", len(rows))
	}
}

func TestReady(t *testing.T) {
	skipUnlessAttach(t)
	root, _ := openTestRoot(t)
	makeSession(t, root, testAgent, testSessionA)
	lockPath := testAgent + "/" + testSessionA + "/ingest.lock"
	if err := ensureLock(root, lockPath); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{paneBy: map[string]paneFacts{attachTarget: {pipe: true}}}
	deps := attachDeps(runner, "", attachClock())
	ctx := context.Background()
	// No marker: not ready, fast with the advancing clock.
	if ready(ctx, deps, root, lockPath, "abc", attachTarget, 200*time.Millisecond, 10*time.Millisecond) {
		t.Fatal("ready without marker")
	}
	// Marker plus a holder plus a pipe: ready.
	held, err := tryLock(root, lockPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseLock(held) }()
	if err := writeLockLine(held, "ingest 1 abc"); err != nil {
		t.Fatal(err)
	}
	if !ready(ctx, deps, root, lockPath, "abc", attachTarget, 5*time.Second, 10*time.Millisecond) {
		t.Fatal("not ready with marker, holder and pipe")
	}
	// The ingest runs in another process: its own pid with this
	// attempt's nonce is ready, even when it is not the opener's.
	if err := writeLockLine(held, "ingest 99999 abc"); err != nil {
		t.Fatal(err)
	}
	if !ready(ctx, deps, root, lockPath, "abc", attachTarget, 5*time.Second, 10*time.Millisecond) {
		t.Fatal("not ready with another pid and this nonce")
	}
	// Another attempt's nonce is never ready.
	if err := writeLockLine(held, "ingest 99999 abd"); err != nil {
		t.Fatal(err)
	}
	if ready(ctx, deps, root, lockPath, "abc", attachTarget, 200*time.Millisecond, 10*time.Millisecond) {
		t.Fatal("ready with another nonce")
	}
	// A marker without a valid pid is never ready.
	for _, bad := range []string{"ingest 0 abc", "ingest -4 abc", "ingest x abc", "attach 1 abc"} {
		if err := writeLockLine(held, bad); err != nil {
			t.Fatal(err)
		}
		if ready(ctx, deps, root, lockPath, "abc", attachTarget, 200*time.Millisecond, 10*time.Millisecond) {
			t.Fatalf("ready with marker %q", bad)
		}
	}
}

// TestAttachQuotaRefusedLockEmpty proves the CAP-A1 ordering: the
// attach marker is written only after the budget grants the
// allowances, so a refused allocation leaves an empty lock file, no
// row and no pipe.
func TestAttachQuotaRefusedLockEmpty(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := Limits{
		ChunkMaxBytes: 1024, FrameMaxBytes: 1048576, SegmentMaxBytes: 1048576,
		SegmentMaxAge: Duration(time.Hour), ChunkMaxAge: Duration(50 * time.Millisecond),
		FlushInterval: Duration(10 * time.Millisecond), QueueMaxBytes: 65536,
		ResizeInterval: Duration(time.Second), ReserveBlockBytes: 8192,
		SnapshotMaxBytes: 65536, LogMaxBytes: 4096,
		AgentQuotaBytes: 208896, NodeQuotaBytes: 300000,
	}
	snapshot := []byte("\x1b[0mhello-pane")
	first := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{opened: true, snapshot: snapshot},
	}
	firstOpts := attachOpts()
	firstOpts.Limits = lim
	// The first session commits (its handover then times out on the
	// fake runner), retaining its allowances on the ledgers.
	_, err := Attach(context.Background(), attachDeps(first, data, attachClock()), firstOpts)
	if !errors.Is(err, ErrAttach) {
		t.Fatalf("first attach err = %v, want ErrAttach", err)
	}
	// The refused session is the only one of its agent: the agent
	// directory appears with it.
	before, err := os.ReadDir(filepath.Join(data, "captures", "miner-2"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	other := attachPane()
	other.id = "%8"
	other.sessionID = "$4"
	other.windowIndex = 2
	other.paneIndex = 0
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{"%8": other},
	}
	deps := attachDeps(runner, data, attachClock())
	opts := attachOpts()
	opts.Agent = "miner-2"
	opts.Target = "%8"
	opts.Limits = lim
	_, err = Attach(context.Background(), deps, opts)
	if !errors.Is(err, ErrQuota) {
		t.Fatalf("attach err = %v, want ErrQuota", err)
	}
	for _, call := range runner.calls {
		if len(call) >= 4 && call[:4] == "open" {
			t.Fatalf("pipe opened on a refused attach")
		}
	}
	after, err := os.ReadDir(filepath.Join(data, "captures", opts.Agent))
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before)+1 {
		t.Fatalf("sessions = %d, want %d", len(after), len(before)+1)
	}
	known := map[string]bool{}
	for _, e := range before {
		known[e.Name()] = true
	}
	session := ""
	for _, e := range after {
		if !known[e.Name()] {
			session = e.Name()
		}
	}
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	line, err := readLockLine(root, opts.Agent+"/"+session+"/ingest.lock")
	if err != nil {
		t.Fatal(err)
	}
	if line != "" {
		t.Fatalf("lock line = %q, want empty", line)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	rows, err := store.AllSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Only the first session committed: the refused one left no row.
	if len(rows) != 1 || rows[0].AgentID != "miner-1" {
		t.Fatalf("sessions = %+v, want only the first", rows)
	}
}

func TestDiscoverSocket(t *testing.T) {
	ctx := context.Background()
	ok := func(_ context.Context, _ []string, _ bool, _ int) execResult {
		return execResult{stdout: []byte("/tmp/sock\n")}
	}
	sock, err := discoverSocket(ctx, ok)
	if err != nil || sock != "/tmp/sock" {
		t.Fatalf("discovery = %q, %v", sock, err)
	}
	unreachable := func(_ context.Context, _ []string, _ bool, _ int) execResult {
		return execResult{err: errors.New("exit status 1"), stderr: []byte("no server running")}
	}
	if _, err := discoverSocket(ctx, unreachable); !errors.Is(err, ErrUsage) {
		t.Fatalf("no server err = %v, want ErrUsage", err)
	}
	relative := func(_ context.Context, _ []string, _ bool, _ int) execResult {
		return execResult{stdout: []byte("rel/sock\n")}
	}
	if _, err := discoverSocket(ctx, relative); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("relative err = %v, want ErrUnsafePath", err)
	}
}
