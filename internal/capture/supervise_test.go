package capture

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// superviseRunner scripts one server: reachability, panes, and the
// guarded open, recording the open requests.
type superviseRunner struct {
	serverOut serverFacts
	serverErr error
	paneBy    map[string]paneFacts
	panesOut  []paneFacts
	panesErr  error
	openOut   openOutcome
	openErr   error
	opens     []openRequest
}

func (r *superviseRunner) server(context.Context) (serverFacts, error) {
	if r.serverErr != nil {
		return serverFacts{}, r.serverErr
	}
	return r.serverOut, nil
}

func (r *superviseRunner) pane(_ context.Context, target string) (paneFacts, error) {
	p, ok := r.paneBy[target]
	if !ok {
		return paneFacts{}, os.ErrNotExist
	}
	return p, nil
}

func (r *superviseRunner) panes(context.Context) ([]paneFacts, error) {
	if r.panesErr != nil {
		return nil, r.panesErr
	}
	return r.panesOut, nil
}

func (r *superviseRunner) open(_ context.Context, req openRequest) (openOutcome, error) {
	if r.openErr != nil {
		return openOutcome{}, r.openErr
	}
	r.opens = append(r.opens, req)
	return r.openOut, nil
}

func superviseDeps(runner *superviseRunner) SuperviseDeps {
	return SuperviseDeps{
		RunnerFor: func(string) tmuxRunner { return runner },
		Exe:       func() (string, error) { return "/bin/gifford-test", nil },
		PID:       4242, Clock: attachClock(), Mono: fakeMono{v: 1000000000},
		DataDir:      func() string { return "" },
		ReadyTimeout: 100 * time.Millisecond, ReadyPoll: 10 * time.Millisecond,
	}
}

// superviseFixture builds one committed session with the attach
// server identity, plus claims.lock for the pass.
func superviseFixture(t *testing.T) string {
	t.Helper()
	data := t.TempDir()
	lim := ingestLimits()
	_ = ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes)
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if err := ensureLock(root, "claims.lock"); err != nil {
		t.Fatal(err)
	}
	return data
}

// putExit writes an ingest.exit record with the given cause.
func putExit(t *testing.T, data, cause string) {
	t.Helper()
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	payload, err := jsonBytes(ingestExit{
		Schema: "gifford.capture.exit.v1", Cause: cause,
		At: formatWall(storeWall), PID: 4242, Attempt: testNonce,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := publishFile(root, storeTestAgent+"/"+storeTestSession, "ingest.exit", []byte(payload), false); err != nil {
		t.Fatal(err)
	}
}

func paneNoPipe() paneFacts {
	pane := attachPane()
	pane.pipe = false
	return pane
}

func TestSuperviseReattachGap(t *testing.T) {
	skipUnlessAttach(t)
	data := superviseFixture(t)
	putExit(t, data, ExitEOF)
	runner := &superviseRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{"%1": paneNoPipe()},
		openOut:   openOutcome{opened: true, snapshot: []byte("\x1b[0mre-snap")},
	}
	if err := Supervise(context.Background(), superviseDeps(runner), SuperviseOptions{Data: data, Once: true}); err != nil {
		t.Fatal(err)
	}
	if len(runner.opens) != 1 {
		t.Fatalf("opens = %d, want 1", len(runner.opens))
	}
	req := runner.opens[0]
	if req.target != "%1" || req.mode != modeNormal || req.nonce == "" {
		t.Fatalf("open = %+v", req)
	}
	var seqArg, attempt string
	for i, word := range req.argv {
		if word == "--start-sequence" && i+1 < len(req.argv) {
			seqArg = req.argv[i+1]
		}
		if word == "--attempt" && i+1 < len(req.argv) {
			attempt = req.argv[i+1]
		}
	}
	// Boundary batch gap@2, screen@3, reattached@4: the first chunk
	// starts at 5.
	if seqArg != "5" {
		t.Fatalf("start-sequence = %q, want 5", seqArg)
	}
	if attempt == "" || attempt != req.nonce {
		t.Fatalf("attempt = %q, nonce = %q", attempt, req.nonce)
	}
	events := sessionEventsOf(t, data)
	if len(events) != 5 {
		t.Fatalf("events = %d, want attached/screen/gap/screen/reattached", len(events))
	}
	for i, ev := range events {
		if ev.Sequence != int64(i) {
			t.Fatalf("event %d sequence = %d", i, ev.Sequence)
		}
	}
	if events[2].EventType != protocol.EventTypeCaptureGapDetected ||
		events[3].EventType != protocol.EventTypeCaptureInitialScreen ||
		events[4].EventType != protocol.EventTypeCaptureReattached {
		t.Fatalf("batch = %s/%s/%s", events[2].EventType, events[3].EventType, events[4].EventType)
	}
	var gap protocol.CaptureGapDetected
	if err := decodePayload(&events[2], &gap); err != nil {
		t.Fatal(err)
	}
	if gap.Cause != protocol.CaptureGapCausePipeClosed {
		t.Fatalf("gap cause = %s, want pipe_closed", gap.Cause)
	}
	var screen protocol.CaptureInitialScreen
	if err := decodePayload(&events[3], &screen); err != nil {
		t.Fatal(err)
	}
	wantRef := "blob://capture/" + storeTestSession + "/initial-3-" + attempt
	if string(screen.SnapshotBlobRef) != wantRef {
		t.Fatalf("screen ref = %s, want %s", screen.SnapshotBlobRef, wantRef)
	}
	if screen.Cols != 80 || screen.Rows != 24 {
		t.Fatalf("screen size = %dx%d", screen.Cols, screen.Rows)
	}
	names, err := os.ReadDir(filepath.Join(data, "captures", storeTestAgent, storeTestSession))
	if err != nil {
		t.Fatal(err)
	}
	snapName := "initial-3-" + attempt + ".ansi"
	found := false
	for _, name := range names {
		if name.Name() == snapName {
			found = true
		}
	}
	if !found {
		t.Fatalf("snapshot %s not published", snapName)
	}
	var reattached protocol.CaptureReattached
	if err := decodePayload(&events[4], &reattached); err != nil {
		t.Fatal(err)
	}
	if reattached.GapFromSequence != 1 || reattached.GapToSequence != 4 {
		t.Fatalf("gap = %d->%d, want 1->4", reattached.GapFromSequence, reattached.GapToSequence)
	}
	if reattached.GapMS != 1000000000 {
		t.Fatalf("gap_ms = %d", reattached.GapMS)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	row, _, err := store.SessionRow(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	// No ingest started on the fake open, so the handover timeout
	// parked the session with no further event.
	if row.ReattachCount != 1 || row.State != stateReattaching || row.TmuxPane != "%1" {
		t.Fatalf("row = %+v", row)
	}
	if row.PaneCols != 80 || row.PaneRows != 24 {
		t.Fatalf("row size = %dx%d", row.PaneCols, row.PaneRows)
	}
}

func TestSupervisePipePreserved(t *testing.T) {
	skipUnlessAttach(t)
	data := superviseFixture(t)
	pipe := paneNoPipe()
	pipe.pipe = true
	runner := &superviseRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{"%1": pipe},
	}
	if err := Supervise(context.Background(), superviseDeps(runner), SuperviseOptions{Data: data, Once: true}); err != nil {
		t.Fatal(err)
	}
	// A pipe no running ingest holds is never opened and never
	// replaced: the session parks with nothing appended.
	if len(runner.opens) != 0 {
		t.Fatalf("opens = %d, want none", len(runner.opens))
	}
	events := sessionEventsOf(t, data)
	if len(events) != 2 {
		t.Fatalf("events = %d, want the boundary only", len(events))
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	row, _, err := store.SessionRow(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != "REATTACHING" {
		t.Fatalf("row = %+v, want REATTACHING", row)
	}
}

func TestSuperviseServerReplaced(t *testing.T) {
	skipUnlessAttach(t)
	data := superviseFixture(t)
	server := attachServer()
	server.pid++
	runner := &superviseRunner{
		serverOut: server,
		paneBy:    map[string]paneFacts{"%1": paneNoPipe()},
	}
	if err := Supervise(context.Background(), superviseDeps(runner), SuperviseOptions{Data: data, Once: true}); err != nil {
		t.Fatal(err)
	}
	if len(runner.opens) != 0 {
		t.Fatalf("opens = %d, want none", len(runner.opens))
	}
	events := sessionEventsOf(t, data)
	last := events[len(events)-1]
	if last.EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("last event = %s, want stopped", last.EventType)
	}
	var stopped protocol.CaptureStopped
	if err := decodePayload(&last, &stopped); err != nil {
		t.Fatal(err)
	}
	if stopped.Reason != protocol.CaptureStopReasonError || stopped.Detail != "tmux server replaced" {
		t.Fatalf("stopped = %+v", stopped)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	row, _, err := store.SessionRow(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != testStateError {
		t.Fatalf("row = %+v, want ERROR", row)
	}
}

func TestSuperviseSecondSupervisor(t *testing.T) {
	skipUnlessAttach(t)
	data := superviseFixture(t)
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if err := ensureLock(root, "supervise.lock"); err != nil {
		t.Fatal(err)
	}
	held, err := tryLock(root, "supervise.lock", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseLock(held) }()
	runner := &superviseRunner{serverOut: attachServer()}
	if err := Supervise(context.Background(), superviseDeps(runner), SuperviseOptions{Data: data, Once: true}); err != nil {
		t.Fatal(err)
	}
	if len(runner.opens) != 0 {
		t.Fatalf("opens = %d, want none", len(runner.opens))
	}
}

func TestSuperviseIncompleteAndTerminal(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	makeSession(t, root, storeTestAgent, statusHalfSession)
	makeSession(t, root, storeTestAgent, statusTerminalSession)
	for _, session := range []string{statusHalfSession, statusTerminalSession} {
		if err := ensureLock(root, storeTestAgent+"/"+session+"/ingest.lock"); err != nil {
			t.Fatal(err)
		}
	}
	if err := ensureLock(root, "claims.lock"); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	half := testSessionRow()
	half.CaptureSessionID = statusHalfSession
	if err := store.InsertSession(context.Background(), half); err != nil {
		t.Fatal(err)
	}
	term := testSessionRow()
	term.CaptureSessionID = statusTerminalSession
	term.State = testStateStopped
	term.StopReason = testReasonManual
	if err := store.InsertSession(context.Background(), term); err != nil {
		t.Fatal(err)
	}
	stopped := statusEnvelope(t, statusTerminalSession, protocol.EventTypeCaptureStopped, 0, storeWall, protocol.CaptureStopped{
		CaptureSessionID: protocol.OpaqueID(statusTerminalSession),
		Reason:           protocol.CaptureStopReasonManual,
	})
	if err := store.AppendEvents(context.Background(), []protocol.EventEnvelope{stopped}); err != nil {
		t.Fatal(err)
	}
	runner := &superviseRunner{serverOut: attachServer()}
	deps := superviseDeps(runner)
	if err := Supervise(context.Background(), deps, SuperviseOptions{Data: data, Once: true}); err != nil {
		t.Fatal(err)
	}
	// The incomplete row finishes ERROR row-only with no events; the
	// terminal session is untouched.
	row, _, err := store.SessionRow(context.Background(), statusHalfSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != testStateError || row.StopReason != testReasonError {
		t.Fatalf("half row = %+v", row)
	}
	events, err := store.SessionEvents(context.Background(), statusHalfSession, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("half events = %d, want none", len(events))
	}
	termEvents, err := store.SessionEvents(context.Background(), statusTerminalSession, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(termEvents) != 1 {
		t.Fatalf("terminal events = %d, want one", len(termEvents))
	}
}

func TestSuperviseStopRequest(t *testing.T) {
	skipUnlessAttach(t)
	data := superviseFixture(t)
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	payload, err := jsonBytes(&stopRequest{
		Schema: "gifford.capture.stop.v1", Reason: "manual",
		Detail: testDetailHalt, RequestedAt: formatWall(storeWall),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := publishFile(root, storeTestAgent+"/"+storeTestSession, "stop.request", []byte(payload), true); err != nil {
		t.Fatal(err)
	}
	runner := &superviseRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{"%1": paneNoPipe()},
		openOut:   openOutcome{opened: true, snapshot: []byte("x")},
	}
	if err := Supervise(context.Background(), superviseDeps(runner), SuperviseOptions{Data: data, Once: true}); err != nil {
		t.Fatal(err)
	}
	// The waiting request stops the session: never reattached.
	if len(runner.opens) != 0 {
		t.Fatalf("opens = %d, want none", len(runner.opens))
	}
	events := sessionEventsOf(t, data)
	last := events[len(events)-1]
	if last.EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("last event = %s, want stopped", last.EventType)
	}
	var stopped protocol.CaptureStopped
	if err := decodePayload(&last, &stopped); err != nil {
		t.Fatal(err)
	}
	if stopped.Reason != protocol.CaptureStopReasonManual || stopped.Detail != testDetailHalt {
		t.Fatalf("stopped = %+v", stopped)
	}
}
