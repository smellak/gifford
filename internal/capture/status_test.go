package capture

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// statusRunner scripts server reachability and pane facts per socket.
type statusRunner struct {
	serverErr error
	paneBy    map[string]paneFacts
}

func (r *statusRunner) server(context.Context) (serverFacts, error) {
	if r.serverErr != nil {
		return serverFacts{}, r.serverErr
	}
	return serverFacts{pid: 4242, startTime: 1793231354, socket: claimServer}, nil
}

func (r *statusRunner) pane(_ context.Context, target string) (paneFacts, error) {
	p, ok := r.paneBy[target]
	if !ok {
		return paneFacts{}, fmt.Errorf("%w: no such pane", ErrPaneNotFound)
	}
	return p, nil
}

func (r *statusRunner) panes(context.Context) ([]paneFacts, error) {
	return nil, nil
}

func (r *statusRunner) open(context.Context, openRequest) (openOutcome, error) {
	return openOutcome{}, nil
}

const (
	statusLiveSession     = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAL"
	statusTerminalSession = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAT"
	statusHalfSession     = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAH"
)

// statusEnvelope builds an envelope for an arbitrary session.
func statusEnvelope(t *testing.T, session string, eventType protocol.EventType, seq int64, wall time.Time, payload any) protocol.EventEnvelope {
	t.Helper()
	ev := testEnvelope(t, eventType, seq, payload)
	ev.SessionID = protocol.OpaqueID(session)
	ev.AgentID = protocol.AgentID(storeTestAgent)
	ev.WallTime = wall
	ev.EventID = protocol.ULID(fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G%02d%02d", session[len(session)-1]%100, seq))
	if eventType == protocol.EventTypeCaptureChunk {
		ev.Direction = protocol.DirectionOut
		ev.Origin = protocol.OriginHarness
	}
	return ev
}

// statusFixture builds a root with a live session (attached, screen,
// one chunk), a terminal session (stopped manual) and an incomplete
// row without events.
func statusFixture(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	makeSession(t, root, storeTestAgent, statusLiveSession)
	makeSession(t, root, storeTestAgent, statusTerminalSession)
	makeSession(t, root, storeTestAgent, statusHalfSession)
	for _, session := range []string{statusLiveSession, statusTerminalSession, statusHalfSession} {
		if err := ensureLock(root, storeTestAgent+"/"+session+"/ingest.lock"); err != nil {
			t.Fatal(err)
		}
	}
	store, err := OpenStore(ctx, filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	liveWall := storeWall.Add(time.Hour)
	live := testSessionRow()
	live.CaptureSessionID = statusLiveSession
	live.StartedAt = formatWall(liveWall)
	live.BytesCaptured = 21
	if err := store.InsertSession(ctx, live); err != nil {
		t.Fatal(err)
	}
	screen := testScreenPayload()
	screen.CaptureSessionID = protocol.OpaqueID(statusLiveSession)
	chunkWall := liveWall.Add(2 * time.Second)
	chunk := protocol.CaptureChunk{
		CaptureSessionID: protocol.OpaqueID(statusLiveSession),
		ByteCount:        21, ANSIPresent: true,
		RawBlobRef: protocol.BlobRef("blob://capture/" + statusLiveSession + "/seg-1/0-21.raw"),
	}
	if err := store.AppendEvents(ctx, []protocol.EventEnvelope{
		statusEnvelope(t, statusLiveSession, protocol.EventTypeCaptureAttached, 0, liveWall, testAttachedPayload()),
		statusEnvelope(t, statusLiveSession, protocol.EventTypeCaptureInitialScreen, 1, liveWall.Add(time.Second), screen),
		statusEnvelope(t, statusLiveSession, protocol.EventTypeCaptureChunk, 2, chunkWall, chunk),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.InsertSegment(ctx, SegmentRow{
		SegmentID: statusLiveSession + "-seg-1", CaptureSessionID: statusLiveSession,
		SeqFrom: 2, SeqTo: 2, StartedAt: formatWall(liveWall), BytesRaw: 21,
		StoragePath: "seg-1.raw", IndexPath: "seg-1.idx",
		SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}); err != nil {
		t.Fatal(err)
	}

	termWall := storeWall
	term := testSessionRow()
	term.CaptureSessionID = statusTerminalSession
	term.StartedAt = formatWall(termWall)
	term.State = testStateStopped
	term.StoppedAt = formatWall(termWall.Add(3 * time.Second))
	term.StopReason = testReasonManual
	if err := store.InsertSession(ctx, term); err != nil {
		t.Fatal(err)
	}
	stopped := protocol.CaptureStopped{
		CaptureSessionID: protocol.OpaqueID(statusTerminalSession),
		Reason:           protocol.CaptureStopReasonManual,
	}
	if err := store.AppendEvents(ctx, []protocol.EventEnvelope{
		statusEnvelope(t, statusTerminalSession, protocol.EventTypeCaptureAttached, 0, termWall, testAttachedPayload()),
		statusEnvelope(t, statusTerminalSession, protocol.EventTypeCaptureInitialScreen, 1, termWall.Add(time.Second), screen),
		statusEnvelope(t, statusTerminalSession, protocol.EventTypeCaptureStopped, 2, termWall.Add(3*time.Second), stopped),
	}); err != nil {
		t.Fatal(err)
	}

	half := testSessionRow()
	half.CaptureSessionID = statusHalfSession
	half.StartedAt = formatWall(storeWall.Add(2 * time.Hour))
	if err := store.InsertSession(ctx, half); err != nil {
		t.Fatal(err)
	}
	return data
}

func statusDeps(pipes map[string]paneFacts) StatusDeps {
	return StatusDeps{
		RunnerFor: func(string) tmuxRunner { return &statusRunner{paneBy: pipes} },
		DataDir:   func() string { return "" },
	}
}

func TestStatus(t *testing.T) {
	data := statusFixture(t)
	pane := attachPane()
	pane.pipe = true
	lines, err := Status(context.Background(), statusDeps(map[string]paneFacts{"%1": pane}), StatusOptions{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	// Newest first: the live session (started later) before the
	// terminal one; the incomplete row never appears.
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2", len(lines))
	}
	live := lines[0]
	if live.Agent != storeTestAgent || live.Capture != statusLiveSession || live.State != stateAttached {
		t.Fatalf("live = %+v", live)
	}
	if live.Pane != "%1" || live.Bytes != 21 {
		t.Fatalf("live = %+v", live)
	}
	if live.LastActivity == "-" {
		t.Fatalf("live = %+v, want last chunk wall", live)
	}
	if live.Segments != 1 || live.Compressed != 0 || live.Rotations != 0 || live.Reattaches != 0 {
		t.Fatalf("live = %+v", live)
	}
	if live.Ingest != "dead" || live.Pipe != "1" {
		t.Fatalf("live = %+v", live)
	}
	term := lines[1]
	if term.Capture != statusTerminalSession || term.State != testStateStopped {
		t.Fatalf("terminal = %+v", term)
	}
	if term.LastActivity != "-" {
		t.Fatalf("terminal = %+v, want no chunk wall", term)
	}
}

func TestStatusEmpty(t *testing.T) {
	data := t.TempDir()
	for _, opts := range []StatusOptions{{Data: data}, {Data: filepath.Join(data, "missing")}} {
		lines, err := Status(context.Background(), statusDeps(nil), opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(lines) != 0 {
			t.Fatalf("lines = %+v, want header only", lines)
		}
	}
}

func TestStatusNoTables(t *testing.T) {
	data := t.TempDir()
	if err := os.WriteFile(filepath.Join(data, "gifford.db"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Status(context.Background(), statusDeps(nil), StatusOptions{Data: data})
	if err == nil || err.Error()[:21] != "store not initialised" {
		t.Fatalf("err = %v, want store not initialised", err)
	}
}

func TestStatusPipeUnknown(t *testing.T) {
	data := statusFixture(t)
	deps := StatusDeps{
		RunnerFor: func(string) tmuxRunner {
			return &statusRunner{serverErr: fmt.Errorf("%w: gone", ErrTmuxCommand), paneBy: map[string]paneFacts{}}
		},
		DataDir: func() string { return "" },
	}
	lines, err := Status(context.Background(), deps, StatusOptions{Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || lines[0].Pipe != "?" || lines[0].Ingest != "dead" {
		t.Fatalf("lines = %+v", lines)
	}
}

func TestStatusReadOnly(t *testing.T) {
	skipUnlessAttach(t)
	data := statusFixture(t)
	before := captureTree(t, data)
	pane := attachPane()
	if _, err := Status(context.Background(), statusDeps(map[string]paneFacts{"%1": pane}), StatusOptions{Data: data}); err != nil {
		t.Fatal(err)
	}
	after := captureTree(t, data)
	if len(before) != len(after) {
		t.Fatalf("files before %d after %d", len(before), len(after))
	}
	for name, meta := range before {
		if after[name] != meta {
			t.Fatalf("file %s changed: %+v -> %+v", name, meta, after[name])
		}
	}
}

// captureTree records every file under the captures root with size
// and mtime: status must change none of them.
func captureTree(t *testing.T, data string) map[string]string {
	t.Helper()
	out := map[string]string{}
	root := filepath.Join(data, "captures")
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			t.Fatal(err)
		}
		out[rel] = fmt.Sprintf("%d-%d", info.Size(), info.ModTime().UnixNano())
		return nil
	})
	return out
}
