package capture

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

func stopDeps() StopDeps {
	return StopDeps{
		PID: 4242, Clock: attachClock(), Mono: fakeMono{v: 1000000000},
		DataDir: func() string { return "" },
	}
}

// stopFixture builds the live session stop needs: directory, locks,
// node identity, ledger, session.json, row and boundary events.
func stopFixture(t *testing.T) string {
	t.Helper()
	data := t.TempDir()
	lim := ingestLimits()
	_ = ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes)
	return data
}

func TestStopHonoured(t *testing.T) {
	skipUnlessAttach(t)
	data := stopFixture(t)
	msg, err := Stop(context.Background(), stopDeps(), StopOptions{Data: data, Agent: storeTestAgent})
	if err != nil {
		t.Fatal(err)
	}
	if msg != "stop recorded for "+storeTestSession {
		t.Fatalf("msg = %q", msg)
	}
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	req, err := readStopRequest(root, storeTestAgent, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if req == nil || req.Reason != testReasonManual {
		t.Fatalf("request = %+v", req)
	}
	// A repeated stop on the terminal session finds no live
	// capture and creates nothing.
	_, err = Stop(context.Background(), stopDeps(), StopOptions{Data: data, Agent: storeTestAgent})
	if !errors.Is(err, ErrNotCaptured) {
		t.Fatalf("err = %v, want ErrNotCaptured", err)
	}
	events := sessionEventsOf(t, data)
	stopped := 0
	for _, ev := range events {
		if ev.EventType == protocol.EventTypeCaptureStopped {
			stopped++
		}
	}
	if stopped != 1 {
		t.Fatalf("stopped events = %d, want exactly one", stopped)
	}
}

// TestStopTerminalUnderLock proves the poll's TOCTOU branch: a
// session that turned terminal between the hint and the lock only
// gets its rows repaired, with no second terminal transition.
func TestStopTerminalUnderLock(t *testing.T) {
	skipUnlessAttach(t)
	data := stopFixture(t)
	if _, err := Stop(context.Background(), stopDeps(), StopOptions{Data: data, Agent: storeTestAgent}); err != nil {
		t.Fatal(err)
	}
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	held, err := tryLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock", true)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	msg, err := stopUnderLock(context.Background(), stopDeps(), store, root, storeTestAgent, storeTestSession, held)
	if err != nil {
		t.Fatal(err)
	}
	if msg != "capture "+storeTestSession+" already STOPPED" {
		t.Fatalf("msg = %q", msg)
	}
	events := sessionEventsOf(t, data)
	stopped := 0
	for _, ev := range events {
		if ev.EventType == protocol.EventTypeCaptureStopped {
			stopped++
		}
	}
	if stopped != 1 {
		t.Fatalf("stopped events = %d, want exactly one", stopped)
	}
}

func TestStopPending(t *testing.T) {
	skipUnlessAttach(t)
	data := stopFixture(t)
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	// An ingest that holds the lock and never flushes: the stop
	// records its request and waits out the bound.
	held, err := tryLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseLock(held) }()
	deps := stopDeps()
	deps.Timeout = 300 * time.Millisecond
	deps.Poll = 10 * time.Millisecond
	start := time.Now()
	_, err = Stop(context.Background(), deps, StopOptions{Data: data, Agent: storeTestAgent})
	if !errors.Is(err, ErrStopPending) {
		t.Fatalf("err = %v, want ErrStopPending", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("stop exceeded its bound")
	}
	req, err := readStopRequest(root, storeTestAgent, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if req == nil {
		t.Fatal("stop request was not recorded")
	}
}

// TestStopPendingHonouredAfterRelease proves a repeated pending stop
// still polls: the first stop times out under a held lock, and after
// the release the second stop honours the durable request instead of
// reporting success over a live session.
func TestStopPendingHonouredAfterRelease(t *testing.T) {
	skipUnlessAttach(t)
	data := stopFixture(t)
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	held, err := tryLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock", true)
	if err != nil {
		t.Fatal(err)
	}
	deps := stopDeps()
	deps.Timeout = 100 * time.Millisecond
	deps.Poll = 10 * time.Millisecond
	if _, err := Stop(context.Background(), deps, StopOptions{Data: data, Agent: storeTestAgent}); !errors.Is(err, ErrStopPending) {
		t.Fatalf("first stop: %v, want ErrStopPending", err)
	}
	if err := releaseLock(held); err != nil {
		t.Fatal(err)
	}
	if _, err := Stop(context.Background(), deps, StopOptions{Data: data, Agent: storeTestAgent}); err != nil {
		t.Fatalf("second stop: %v", err)
	}
	events := sessionEventsOf(t, data)
	last := events[len(events)-1]
	if last.EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("last event = %s, want capture.stopped", last.EventType)
	}
}

func TestStopValidation(t *testing.T) {
	data := stopFixture(t)
	for _, opts := range []StopOptions{
		{Data: data, Agent: storeTestAgent, Reason: "later"},
		{Data: data, Agent: storeTestAgent, Reason: testReasonManual, Detail: "bad\x00detail"},
	} {
		if _, err := Stop(context.Background(), stopDeps(), opts); !errors.Is(err, ErrUsage) {
			t.Fatalf("opts = %+v, err = %v, want ErrUsage", opts, err)
		}
	}
	if _, err := Stop(context.Background(), stopDeps(), StopOptions{Data: t.TempDir(), Agent: storeTestAgent}); !errors.Is(err, ErrNotCaptured) {
		t.Fatalf("err = %v, want ErrNotCaptured", err)
	}
	if _, err := Stop(context.Background(), stopDeps(), StopOptions{Data: data, Agent: "miner-9"}); !errors.Is(err, ErrNotCaptured) {
		t.Fatalf("err = %v, want ErrNotCaptured", err)
	}
}

func TestConcurrentStops(t *testing.T) {
	skipUnlessAttach(t)
	data := stopFixture(t)
	const stops = 8
	var wg sync.WaitGroup
	errs := make([]error, stops)
	for i := range stops {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			deps := stopDeps()
			deps.Timeout = 5 * time.Second
			deps.Poll = 5 * time.Millisecond
			_, errs[i] = Stop(context.Background(), deps, StopOptions{Data: data, Agent: storeTestAgent})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("stop %d err = %v", i, err)
		}
	}
	events := sessionEventsOf(t, data)
	stopped := 0
	for _, ev := range events {
		if ev.EventType == protocol.EventTypeCaptureStopped {
			stopped++
		}
	}
	if stopped != 1 {
		t.Fatalf("stopped events = %d, want exactly one", stopped)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	row, found, err := store.SessionRow(context.Background(), storeTestSession)
	if err != nil || !found {
		t.Fatal("missing row")
	}
	if row.State != testStateStopped || row.StopReason != testReasonManual {
		t.Fatalf("row = %+v", row)
	}
}
