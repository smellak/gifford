package capture

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const repairSegment = "seg-01ARZ3NDEKTSV4RRFFQ69G5FAV"

const (
	repairSegA = "seg-01AAAAAAAAAAAAAAAAAAAAAAAA"
	repairSegB = "seg-01BBBBBBBBBBBBBBBBBBBBBBBB"
)

var repairClock = fakeClock{now: time.Date(2026, time.January, 2, 4, 0, 0, 0, time.UTC)}

// chunkEnvelopeSeg builds a chunk whose raw_blob_ref names its segment
// and raw offset, the producer form repair parses for identity.
func chunkEnvelopeSeg(t *testing.T, seg string, seq, offset, nbytes int64) protocol.EventEnvelope {
	t.Helper()
	ev := testEnvelope(t, protocol.EventTypeCaptureChunk, seq, protocol.CaptureChunk{
		CaptureSessionID: storeTestSession,
		ByteCount:        nbytes,
		RawBlobRef:       protocol.BlobRef("blob://capture/" + storeTestSession + "/" + seg + "/" + strconv.FormatInt(offset, 10)),
	})
	ev.Direction = protocol.DirectionOut
	ev.Origin = protocol.OriginHarness
	return ev
}

func repairFixture(t *testing.T, s *Store, row SessionRow, events []protocol.EventEnvelope) {
	t.Helper()
	ctx := context.Background()
	if err := s.InsertSession(ctx, row); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
}

func TestRepairTerminal(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	makeSession(t, root, storeTestAgent, storeTestSession)
	raw, err := root.Create(storeTestAgent + "/" + storeTestSession + "/" + repairSegment + ".raw")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write(make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	stoppedAt := storeWall.Add(10 * time.Second)
	repairFixture(t, s, testSessionRow(), []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
		chunkEnvelopeSeg(t, repairSegment, 2, 0, 100),
		chunkEnvelopeSeg(t, repairSegment, 3, 100, 100),
		chunkEnvelopeSeg(t, repairSegment, 4, 200, 100),
		testEnvelope(t, protocol.EventTypeCaptureResize, 5, protocol.CaptureResize{
			CaptureSessionID: storeTestSession, Cols: 100, Rows: 30,
		}),
		func() protocol.EventEnvelope {
			ev := stoppedEnvelope(t, 6, protocol.CaptureStopReasonQuota)
			ev.WallTime = stoppedAt
			return ev
		}(),
	})
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	row, found, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("session gone")
	}
	if row.State != "QUOTA" || row.StopReason != testReasonQuota || row.StoppedAt != formatWall(stoppedAt) {
		t.Fatalf("terminal row = %+v", row)
	}
	if row.PaneCols != 100 || row.PaneRows != 30 {
		t.Fatalf("sizes = %dx%d", row.PaneCols, row.PaneRows)
	}
	if row.BytesCaptured != 300 {
		t.Fatalf("bytes = %d, want 300", row.BytesCaptured)
	}
	if row.InitialScreenBlobRef != "blob://snap/1" {
		t.Fatalf("blob ref = %q", row.InitialScreenBlobRef)
	}
	segs, err := s.SegmentsOf(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 || segs[0].SegmentID != repairSegment || segs[0].SeqFrom != 2 ||
		segs[0].SeqTo != 4 || segs[0].BytesRaw != 300 ||
		segs[0].EndedAt != formatWall(storeWall.Add(4*time.Second)) {
		t.Fatalf("segments = %+v", segs)
	}
}

func TestRepairLiveKeepsState(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	makeSession(t, root, storeTestAgent, storeTestSession)
	raw, err := root.Create(storeTestAgent + "/" + storeTestSession + "/" + repairSegment + ".raw")
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	row := testSessionRow()
	row.State = stateReattaching
	repairFixture(t, s, row, []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
		chunkEnvelopeSeg(t, repairSegment, 2, 0, 50),
		testEnvelope(t, protocol.EventTypeCapturePaneRecreated, 3, protocol.CapturePaneRecreated{
			CaptureSessionID: storeTestSession, OldPane: "%1", NewPane: "%2",
		}),
	})
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "REATTACHING" {
		t.Fatalf("live repair changed state to %s", got.State)
	}
	if got.TmuxPane != "%2" || got.BytesCaptured != 50 {
		t.Fatalf("live row = %+v", got)
	}
}

func TestRepairIncomplete(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	makeSession(t, root, storeTestAgent, storeTestSession)
	if err := s.InsertSession(ctx, testSessionRow()); err != nil {
		t.Fatal(err)
	}
	// No lock file: attach is gone, the row finishes as ERROR.
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != testStateError || got.StopReason != testReasonError || got.StoppedAt != formatWall(repairClock.now) {
		t.Fatalf("finished row = %+v", got)
	}
}

func TestRepairIncompleteHeldLock(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	makeSession(t, root, storeTestAgent, storeTestSession)
	if err := s.InsertSession(ctx, testSessionRow()); err != nil {
		t.Fatal(err)
	}
	if err := ensureLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock"); err != nil {
		t.Fatal(err)
	}
	held, err := tryLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseLock(held) }()
	// The caller holds the session lock by contract, so the repair
	// finishes its own incomplete row instead of mistaking the hold
	// for a live attach.
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != stateError || got.StopReason != testReasonError {
		t.Fatalf("held-lock repair left %+v", got)
	}
}

func TestRepairRotated(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	const closed = repairSegA
	const opened = repairSegB
	closedAt := storeWall.Add(5 * time.Second)
	repairFixture(t, s, testSessionRow(), []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
		chunkEnvelopeSeg(t, closed, 2, 0, 100),
		chunkEnvelopeSeg(t, closed, 3, 100, 200),
		func() protocol.EventEnvelope {
			ev := testEnvelope(t, protocol.EventTypeCaptureRotated, 4, protocol.CaptureRotated{
				CaptureSessionID: storeTestSession,
				ClosedSegmentID:  closed, NewSegmentID: opened,
				Bytes: 300,
			})
			ev.WallTime = closedAt
			return ev
		}(),
		chunkEnvelopeSeg(t, opened, 5, 0, 50),
	})
	// The closed row exists but stayed open across the crash.
	if err := s.InsertSegment(ctx, SegmentRow{
		SegmentID: closed, CaptureSessionID: storeTestSession,
		SeqFrom: 2, SeqTo: 3, StartedAt: formatWall(storeWall),
		StoragePath: closed + ".raw", IndexPath: closed + ".idx", BytesRaw: 300,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	segs, err := s.SegmentsOf(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 2 {
		t.Fatalf("segments = %+v", segs)
	}
	if segs[0].SegmentID != closed || segs[0].SeqTo != 3 || segs[0].BytesRaw != 300 ||
		segs[0].EndedAt != formatWall(closedAt) {
		t.Fatalf("closed = %+v", segs[0])
	}
	if segs[1].SegmentID != opened || segs[1].SeqFrom != 5 || segs[1].SeqTo != 5 ||
		segs[1].BytesRaw != 50 || segs[1].EndedAt != formatWall(storeWall.Add(5*time.Second)) {
		t.Fatalf("opened = %+v", segs[1])
	}
	row, _, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.BytesCaptured != 350 {
		t.Fatalf("bytes = %d, want 350", row.BytesCaptured)
	}
}

func TestRepairRestoreStop(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	makeSession(t, root, storeTestAgent, storeTestSession)
	putFile(t, root, ".", "node-id", []byte("capture-0123456789abcdef0123456789abcdef\n"))
	putFile(t, root, storeTestAgent+"/"+storeTestSession, "stop.request", []byte(
		`{"schema":"gifford.capture.stop.v1","reason":"manual","detail":"halt","requested_at":"2026-01-02T03:04:05Z"}`))
	raw, err := root.Create(storeTestAgent + "/" + storeTestSession + "/" + repairSegment + ".raw")
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	repairFixture(t, s, testSessionRow(), []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
		chunkEnvelopeSeg(t, repairSegment, 2, 0, 10),
	})
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	events, err := s.SessionEvents(ctx, storeTestSession, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 || events[3].EventType != protocol.EventTypeCaptureStopped || events[3].Sequence != 3 {
		t.Fatalf("restored events = %d", len(events))
	}
	row, _, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != testStateStopped || row.StopReason != testReasonManual {
		t.Fatalf("restored row = %+v", row)
	}
}

// TestRepairBadChunkRefFailsClosed proves identity comes only from
// the chunk refs: a chunk whose ref parses nowhere fails the repair
// instead of guessing a segment from the row-less raw files on disk.
func TestRepairBadChunkRefFailsClosed(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	makeSession(t, root, storeTestAgent, storeTestSession)
	for _, id := range []string{repairSegA, repairSegB} {
		f, err := root.Create(storeTestAgent + "/" + storeTestSession + "/" + id + ".raw")
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	bad := testEnvelope(t, protocol.EventTypeCaptureChunk, 2, protocol.CaptureChunk{
		CaptureSessionID: storeTestSession,
		ByteCount:        10,
		RawBlobRef:       "blob://chunk/2",
	})
	bad.Direction = protocol.DirectionOut
	bad.Origin = protocol.OriginHarness
	repairFixture(t, s, testSessionRow(), []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
		bad,
	})
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err == nil {
		t.Fatal("want error for unparseable chunk ref")
	}
}

// TestRepairRunBoundary proves spans never cross a reattach: chunks
// from the dead run close under their own segment id, and the next
// run's chunks under its own, each resolved by sequence range. A
// lumped span would mix the runs into one id.
func TestRepairRunBoundary(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	root, _ := openTestRoot(t)
	makeSession(t, root, storeTestAgent, storeTestSession)
	for _, id := range []string{repairSegA, repairSegB} {
		f, err := root.Create(storeTestAgent + "/" + storeTestSession + "/" + id + ".raw")
		if err != nil {
			t.Fatal(err)
		}
		_ = f.Close()
	}
	gap := testEnvelope(t, protocol.EventTypeCaptureGapDetected, 4, protocol.CaptureGapDetected{
		CaptureSessionID: storeTestSession, Cause: protocol.CaptureGapCauseIngestDied,
	})
	screen := testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 5, testScreenPayload())
	reattached := testEnvelope(t, protocol.EventTypeCaptureReattached, 6, protocol.CaptureReattached{
		CaptureSessionID: storeTestSession, GapFromSequence: 3, GapToSequence: 6,
	})
	repairFixture(t, s, testSessionRow(), []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload()),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, testScreenPayload()),
		chunkEnvelopeSeg(t, repairSegA, 2, 0, 48),
		chunkEnvelopeSeg(t, repairSegA, 3, 48, 48),
		gap, screen, reattached,
		chunkEnvelopeSeg(t, repairSegB, 7, 0, 16),
	})
	for _, seg := range []SegmentRow{
		{SegmentID: repairSegA, CaptureSessionID: storeTestSession, SeqFrom: 2, SeqTo: 3},
		{SegmentID: repairSegB, CaptureSessionID: storeTestSession, SeqFrom: 7, SeqTo: 7},
	} {
		if err := s.InsertSegment(ctx, seg); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RepairSession(ctx, root, storeTestAgent, storeTestSession, repairClock); err != nil {
		t.Fatal(err)
	}
	segs, err := s.SegmentsOf(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]SegmentRow{}
	for _, seg := range segs {
		got[seg.SegmentID] = seg
	}
	if got[repairSegA].BytesRaw != 96 || got[repairSegA].EndedAt == "" {
		t.Fatalf("run A segment = %+v", got[repairSegA])
	}
	if got[repairSegB].BytesRaw != 16 || got[repairSegB].EndedAt == "" {
		t.Fatalf("run B segment = %+v", got[repairSegB])
	}
	row, _, err := s.SessionRow(ctx, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.BytesCaptured != 112 {
		t.Fatalf("bytes_captured = %d, want 112", row.BytesCaptured)
	}
}

func TestRepairFromEventsCorrupt(t *testing.T) {
	ev := testEnvelope(t, protocol.EventTypeCaptureChunk, 2, protocol.CaptureChunk{
		CaptureSessionID: storeTestSession,
		ByteCount:        10,
		RawBlobRef:       "blob://chunk/2",
	})
	ev.Payload = protocol.Payload(`{"capture_session_id":"x","byte_count":0}`)
	if _, err := repairFromEvents(testSessionRow(), []protocol.EventEnvelope{ev}); err == nil {
		t.Fatal("want error for a corrupt chunk payload")
	}
}
