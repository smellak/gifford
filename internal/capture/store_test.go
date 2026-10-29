package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	storeTestSession = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAV"
	storeTestAgent   = "miner-1"
	storeTestAgent2  = "miner-2"
	storeTestNode    = "node-1"
	claimServer      = "/tmp/sock"
	claimServer2     = "/tmp/sock2"
	testStateStopped = "STOPPED"
	testReasonManual = "manual"
	testReasonQuota  = "quota"
	testReasonError  = "error"
	testStateError   = "ERROR"
	testDetailHalt   = "halt"
)

// openTestStore opens a migrated store file under a temp directory.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(context.Background(), filepath.Join(t.TempDir(), "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// eventID renders a deterministic ULID for sequence n.
func eventID(n int64) protocol.ULID {
	const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	return protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FA" + string(crockford[n%32]))
}

var storeWall = time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

// testEnvelope builds a valid capture envelope for the store tests.
func testEnvelope(t *testing.T, eventType protocol.EventType, seq int64, payload any) protocol.EventEnvelope {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.EventEnvelope{
		EventID:       eventID(seq),
		SchemaVersion: 1,
		NodeID:        storeTestNode,
		SessionID:     storeTestSession,
		AgentID:       storeTestAgent,
		Harness:       protocol.HarnessMuseCode,
		AdapterType:   protocol.AdapterTypeNone,
		SourceQuality: protocol.SourceQualityBootstrapPipePane,
		EventType:     eventType,
		Stream:        protocol.StreamPTY,
		WallTime:      storeWall.Add(time.Duration(seq) * time.Second),
		Sequence:      seq,
		Payload:       protocol.Payload(data),
		Sensitivity:   protocol.SensitivityNormal,
	}
}

func testAttachedPayload() protocol.CaptureAttached {
	return protocol.CaptureAttached{
		CaptureSessionID: storeTestSession,
		AgentID:          storeTestAgent,
		Harness:          protocol.HarnessMuseCode,
		TmuxServer:       claimServer,
		TmuxSession:      "s",
		TmuxWindow:       "w",
		TmuxPane:         "%1",
		PaneCols:         80,
		PaneRows:         24,
	}
}

func testScreenPayload() protocol.CaptureInitialScreen {
	return protocol.CaptureInitialScreen{
		CaptureSessionID: storeTestSession,
		SnapshotBlobRef:  "blob://snap/1",
		ScreenHash:       protocol.Digest("sha256:" + fmt.Sprintf("%064d", 0)),
		Cols:             80,
		Rows:             24,
	}
}

func testSessionRow() SessionRow {
	return SessionRow{
		CaptureSessionID: storeTestSession,
		AgentID:          storeTestAgent,
		Harness:          string(protocol.HarnessMuseCode),
		TmuxServer:       claimServer,
		TmuxSession:      "s",
		TmuxWindow:       "0",
		TmuxPane:         "%1",
		PaneCols:         80,
		PaneRows:         24,
		State:            stateAttached,
		StartedAt:        formatWall(storeWall),
		SourceQuality:    string(protocol.SourceQualityBootstrapPipePane),
	}
}

func TestSessionRowRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	want := testSessionRow()
	if err := s.InsertSession(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("session not found")
	}
	if got != want {
		t.Fatalf("session row = %+v, want %+v", got, want)
	}
	if _, found, err := s.SessionRow(ctx, "cap-01BBBBBBBBBBBBBBBBBBBBBBBB"); err != nil || found {
		t.Fatalf("missing session: found=%v err=%v", found, err)
	}
	// NULL columns read back empty and update sticks.
	updated := want
	updated.State = testStateStopped
	updated.StoppedAt = formatWall(storeWall.Add(time.Minute))
	updated.StopReason = testReasonManual
	updated.BytesCaptured = 42
	if err := s.UpdateSession(ctx, updated); err != nil {
		t.Fatal(err)
	}
	got, _, err = s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if got != updated {
		t.Fatalf("updated row = %+v, want %+v", got, updated)
	}
	// Updating a missing row touches nothing: fail closed.
	if err := s.UpdateSession(ctx, SessionRow{CaptureSessionID: "cap-01BBBBBBBBBBBBBBBBBBBBBBBB"}); err == nil {
		t.Fatal("want error updating a missing session")
	}
}

func TestSessionsByAgent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	first := testSessionRow()
	second := testSessionRow()
	second.CaptureSessionID = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAW"
	second.StartedAt = formatWall(storeWall.Add(time.Hour))
	other := testSessionRow()
	other.CaptureSessionID = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAX"
	other.AgentID = storeTestAgent2
	for _, r := range []SessionRow{second, other, first} {
		if err := s.InsertSession(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.SessionsByAgent(ctx, storeTestAgent)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].CaptureSessionID != first.CaptureSessionID || got[1].CaptureSessionID != second.CaptureSessionID {
		t.Fatalf("by agent = %v, want oldest first", got)
	}
	all, err := s.AllSessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("all sessions = %d, want 3", len(all))
	}
}

func TestSegmentRows(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	comp := int64(100)
	open := SegmentRow{
		SegmentID: "seg-01ARZ3NDEKTSV4RRFFQ69G5FAV", CaptureSessionID: storeTestSession,
		SeqFrom: 2, SeqTo: 5, StartedAt: formatWall(storeWall),
		StoragePath: "seg-01ARZ3NDEKTSV4RRFFQ69G5FAV.raw",
		IndexPath:   "seg-01ARZ3NDEKTSV4RRFFQ69G5FAV.idx",
		BytesRaw:    500,
	}
	if err := s.InsertSegment(ctx, open); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.OpenSegment(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if !found || got != open {
		t.Fatalf("open segment = %+v found=%v, want %+v", got, found, open)
	}
	// Close it: NULLs become values.
	open.EndedAt = formatWall(storeWall.Add(time.Minute))
	open.SeqTo = 9
	open.BytesRaw = 900
	open.BytesCompressed = &comp
	open.Compressed = true
	open.SHA256 = fmt.Sprintf("%064d", 1)
	if err := s.UpdateSegment(ctx, open); err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.OpenSegment(ctx, storeTestSession); err != nil || found {
		t.Fatalf("open after close: found=%v err=%v", found, err)
	}
	segs, err := s.SegmentsOf(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("segments = %+v, want one", segs)
	}
	seg := segs[0]
	if seg.SegmentID != open.SegmentID || seg.SeqFrom != 2 || seg.SeqTo != 9 ||
		seg.BytesRaw != 900 || !seg.Compressed || seg.SHA256 != open.SHA256 ||
		seg.EndedAt != open.EndedAt || seg.StoragePath != open.StoragePath ||
		seg.BytesCompressed == nil || *seg.BytesCompressed != comp {
		t.Fatalf("segments = %+v, want closed %+v", segs, open)
	}
}

func TestAppendAndReadEvents(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	events := []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
	}
	if err := s.AppendEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
	back, err := s.SessionEvents(ctx, storeTestSession, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 2 || back[0].Sequence != 0 || back[1].Sequence != 1 {
		t.Fatalf("session events = %d events", len(back))
	}
	if back[0].EventType != protocol.EventTypeCaptureAttached {
		t.Fatalf("first event = %s", back[0].EventType)
	}
}

func TestReadOnlyOpenMissing(t *testing.T) {
	// A missing store opens the handle; first use reports no table.
	db, err := OpenStoreReadOnly(filepath.Join(t.TempDir(), "missing.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(context.Background(), `SELECT capture_session_id FROM capture_sessions`)
	if err == nil {
		defer func() { _ = rows.Close() }()
		if err := rows.Err(); err == nil {
			t.Fatal("want error querying a missing store")
		}
	}
	if _, err := OpenStoreReadOnly(filepath.Join(t.TempDir(), "a?b.sqlite")); err == nil {
		t.Fatal("want error for a path with ?")
	}
}
