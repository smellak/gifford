package capture

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// ingestLimits returns small but valid limits for the ingest tests.
func ingestLimits() Limits {
	return Limits{
		ChunkMaxBytes:     1024,
		FrameMaxBytes:     1048576,
		SegmentMaxBytes:   1048576,
		SegmentMaxAge:     Duration(time.Hour),
		ChunkMaxAge:       Duration(time.Second),
		FlushInterval:     Duration(10 * time.Millisecond),
		QueueMaxBytes:     65536,
		ResizeInterval:    Duration(100 * time.Millisecond),
		ReserveBlockBytes: 8192,
		SnapshotMaxBytes:  65536,
		LogMaxBytes:       4096,
		AgentQuotaBytes:   16777216,
		NodeQuotaBytes:    33554432,
	}
}

// ingestFixture builds a complete attach-equivalent session: root,
// locks, ledger, session.json, node id, row and the boundary events
// carrying the attempt nonce.
func ingestFixture(t *testing.T, data string, lim Limits, ledger int64) *os.Root {
	session := storeTestSession
	t.Helper()
	ctx := context.Background()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	makeSession(t, root, storeTestAgent, session)
	if err := ensureLock(root, storeTestAgent+"/"+session+"/ingest.lock"); err != nil {
		t.Fatal(err)
	}
	putFile(t, root, ".", "node-id", []byte("capture-0123456789abcdef0123456789abcdef\n"))
	if err := writeLedger(root, storeTestAgent, session, ledger); err != nil {
		t.Fatal(err)
	}
	server := attachServer()
	pane := attachPane()
	opts := AttachOptions{Agent: storeTestAgent, Harness: protocol.HarnessMuseCode, Target: "%1", Socket: server.socket}
	if err := publishSessionJSON(root, opts, session, "capture-0123456789abcdef0123456789abcdef", server, pane, lim); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(ctx, filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	row := testSessionRow()
	row.CaptureSessionID = session
	row.Harness = string(protocol.HarnessMuseCode)
	row.TmuxServer = server.socket
	row.TmuxPane = "%1"
	row.SourceQuality = string(protocol.SourceQualityBootstrapPipePane)
	if err := store.InsertSession(ctx, row); err != nil {
		t.Fatal(err)
	}
	screen := testScreenPayload()
	screen.CaptureSessionID = protocol.OpaqueID(session)
	screen.SnapshotBlobRef = protocol.BlobRef("blob://capture/" + session + "/initial-1-" + testNonce)
	attached := testAttachedPayload()
	attached.CaptureSessionID = protocol.OpaqueID(session)
	if err := store.AppendEvents(ctx, []protocol.EventEnvelope{
		testEnvelope(t, protocol.EventTypeCaptureAttached, 0, attached),
		testEnvelope(t, protocol.EventTypeCaptureInitialScreen, 1, screen),
	}); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	return root
}

func ingestDeps() IngestDeps {
	return IngestDeps{
		Runner: &fakeRunner{paneBy: map[string]paneFacts{"%1": attachPane()}},
		PID:    4242,
		Clock:  &advClock{now: time.Unix(0, 0).UTC()},
		Mono:   fakeMono{v: 1000000000},
	}
}

func ingestOpts(data string, in *os.File) IngestOptions {
	return ingestOptsNonce(data, testNonce, in)
}

// ingestOptsNonce is the wrong-nonce variant for the refused test.
func ingestOptsNonce(data, nonce string, in *os.File) IngestOptions {
	return IngestOptions{
		Data: data, Agent: storeTestAgent, Session: storeTestSession,
		StartSequence: 2, Attempt: nonce, In: in,
	}
}

// runIngest writes input, closes it, and runs the ingest to its cause.
func runIngest(t *testing.T, deps IngestDeps, opts IngestOptions, input []byte) (string, error) {
	t.Helper()
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	opts.In = inReader
	if _, err := inWriter.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := inWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return Ingest(context.Background(), deps, opts)
}

// TestIngestReaderDrainsBeforeLock proves the §8.2 start order: with
// the session lock held, a blocking input write past one pipe buffer
// still completes, so the bounded reader runs before the lock wait.
// The queue is sized past the input so the lock-less reader never
// back-pressures into a stall during the test.
func TestIngestReaderDrainsBeforeLock(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.QueueMaxBytes = 1 << 20
	root := ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes)
	name := storeTestAgent + "/" + storeTestSession + "/ingest.lock"
	held, err := tryLock(root, name, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = releaseLock(held) }()
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inReader.Close() }()
	opts := ingestOpts(data, inReader)
	type outcome struct {
		cause string
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		cause, err := Ingest(context.Background(), ingestDeps(), opts)
		done <- outcome{cause, err}
	}()
	input := make([]byte, 256<<10)
	wrote := make(chan error, 1)
	go func() {
		_, err := inWriter.Write(input)
		wrote <- err
	}()
	release := func() {
		_ = releaseLock(held)
		_ = inWriter.Close()
	}
	select {
	case err := <-wrote:
		if err != nil {
			release()
			<-done
			t.Fatalf("input write: %v", err)
		}
	case <-time.After(15 * time.Second):
		release()
		<-done
		t.Fatal("input stalled with the lock held: the reader did not start before the lock wait")
	}
	release()
	select {
	case out := <-done:
		if out.err != nil || out.cause != ExitEOF {
			t.Fatalf("run = %s, %v; want EOF after the handover", out.cause, out.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not finish after the lock released")
	}
}

// TestIngestMultiRotation drives many rotations through few batches:
// every rotated segment closes with exact bytes, the rotated events
// number segments minus one, and the byte sum is the whole input.
// Rotations sync their prior segment before closing it, so the rows
// the repair derives never reference unsynced data.
func TestIngestMultiRotation(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.ChunkMaxBytes = 512
	lim.FrameMaxBytes = 1024
	lim.SegmentMaxBytes = 2048
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes + 65536
	_ = ingestFixture(t, data, lim, ledger)
	input := make([]byte, 40*1024)
	for i := range input {
		input[i] = byte(i)
	}
	cause, err := runIngest(t, ingestDeps(), ingestOpts(data, nil), input)
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitEOF {
		t.Fatalf("cause = %s, want eof", cause)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	segs, err := store.SegmentsOf(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 4 {
		t.Fatalf("segments = %d, want at least 4 rotations", len(segs))
	}
	var total int64
	for _, seg := range segs {
		if seg.EndedAt == "" {
			t.Fatalf("segment %s left open", seg.SegmentID)
		}
		total += seg.BytesRaw
	}
	if total != int64(len(input)) {
		t.Fatalf("bytes = %d, want %d", total, len(input))
	}
	events := sessionEventsOf(t, data)
	var rotated int
	for _, ev := range events {
		if ev.EventType == protocol.EventTypeCaptureRotated {
			rotated++
		}
	}
	if rotated != len(segs)-1 {
		t.Fatalf("rotated = %d, segments = %d", rotated, len(segs))
	}
}

// TestEnsureFundsRetainsAllowances proves the retained control and
// log allowances are not data credit: a grant holding only F plus
// the log allowance cannot fund one data byte, while real data
// credit above the allowances spends.
func TestEnsureFundsRetainsAllowances(t *testing.T) {
	root, _ := openTestRoot(t)
	lim := ingestLimits()
	lim.AgentQuotaBytes = 1
	lim.NodeQuotaBytes = 1
	bare := &ingestRun{root: root, lim: lim, granted: controlRetain + lim.LogMaxBytes}
	if funds := bare.ensureFunds(1); funds == fundsOK {
		t.Fatalf("funds = %s with only retained allowances granted", funds)
	}
	funded := &ingestRun{root: root, lim: lim, granted: controlRetain + lim.LogMaxBytes + 16}
	if funds := funded.ensureFunds(1); funds != fundsOK {
		t.Fatalf("funds = %s with data credit granted", funds)
	}
}

// TestSyncSegFilesFault proves a failed segment sync fails the sync,
// never silently: closed descriptors cannot sync.
func TestSyncSegFilesFault(t *testing.T) {
	raw, err := os.Create(filepath.Join(t.TempDir(), "s.raw"))
	if err != nil {
		t.Fatal(err)
	}
	idx, err := os.Create(filepath.Join(t.TempDir(), "s.idx"))
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	if err := syncSegFiles(&segFiles{raw: raw, idx: idx}); err == nil {
		t.Fatal("want error syncing closed segment files")
	}
}

// sessionEventsOf reads one session's full stream through a store.
func sessionEventsOf(t *testing.T, data string) []protocol.EventEnvelope {
	t.Helper()
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	events, err := store.SessionEvents(context.Background(), storeTestSession, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func TestIngestEOF(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	cause, err := runIngest(t, ingestDeps(),
		ingestOpts(data, nil), []byte("hello pane \x1b[0m world"))
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitEOF {
		t.Fatalf("cause = %s, want eof", cause)
	}
	events := sessionEventsOf(t, data)
	if len(events) != 3 || events[2].EventType != protocol.EventTypeCaptureChunk {
		t.Fatalf("events = %d, want attached/screen/chunk", len(events))
	}
	var chunk protocol.CaptureChunk
	if err := decodePayload(&events[2], &chunk); err != nil {
		t.Fatal(err)
	}
	if chunk.ByteCount != 21 || !chunk.ANSIPresent {
		t.Fatalf("chunk = %+v", chunk)
	}
	// The final segment closed with verification: .zst and .frames
	// exist and the .raw is gone.
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	segs, err := store.SegmentsOf(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 || !segs[0].Compressed || segs[0].SHA256 == "" {
		t.Fatalf("segments = %+v", segs)
	}
	row, _, err := store.SessionRow(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if row.State != stateAttached || row.BytesCaptured != 21 {
		t.Fatalf("row = %+v", row)
	}
}

func TestIngestRefused(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	cause, err := runIngest(t, ingestDeps(),
		ingestOptsNonce(data, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitRefused {
		t.Fatalf("cause = %s, want refused", cause)
	}
	events := sessionEventsOf(t, data)
	if len(events) != 2 {
		t.Fatalf("events = %d, want only the boundary", len(events))
	}
}

func TestIngestStopRequest(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	root := ingestFixture(t, data, lim, ledger)
	putFile(t, root, storeTestAgent+"/"+storeTestSession, "stop.request", []byte(
		`{"schema":"gifford.capture.stop.v1","reason":"manual","detail":"halt","requested_at":"2026-01-02T03:04:05Z"}`))
	cause, err := runIngest(t, ingestDeps(),
		ingestOpts(data, nil), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitStop {
		t.Fatalf("cause = %s, want stop", cause)
	}
	events := sessionEventsOf(t, data)
	if len(events) != 3 || events[2].EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("events = %d, want boundary plus stop", len(events))
	}
}

func TestIngestQuotaStart(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.AgentQuotaBytes = 210000
	lim.NodeQuotaBytes = 275536
	_ = ingestFixture(t, data, lim, 100)
	// A second session's ledger fills the agent quota.
	bSession := "cap-01ARZ3NDEKTSV4RRFFQ69G5FAW"
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	makeSession(t, root, storeTestAgent, bSession)
	if err := writeLedger(root, storeTestAgent, bSession, 209500); err != nil {
		t.Fatal(err)
	}
	cause, err := runIngest(t, ingestDeps(),
		ingestOpts(data, nil), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitQuota {
		t.Fatalf("cause = %s, want quota", cause)
	}
	events := sessionEventsOf(t, data)
	if len(events) != 5 {
		t.Fatalf("events = %d, want boundary plus quota triple", len(events))
	}
	if events[2].EventType != protocol.EventTypeCaptureQuotaReached ||
		events[3].EventType != protocol.EventTypeCaptureGapDetected ||
		events[4].EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("quota order = %s %s %s", events[2].EventType, events[3].EventType, events[4].EventType)
	}
}

func TestIngestBudgetUnavailable(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	root := ingestFixture(t, data, lim, ledger)
	putFile(t, root, storeTestAgent+"/"+storeTestSession, "evil.txt", []byte("x"))
	cause, err := runIngest(t, ingestDeps(),
		ingestOpts(data, nil), []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitError {
		t.Fatalf("cause = %s, want error", cause)
	}
	events := sessionEventsOf(t, data)
	if len(events) != 3 || events[2].EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("events = %d, want boundary plus error stop", len(events))
	}
}

func TestIngestChunking(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.ChunkMaxBytes = 64
	lim.ReserveBlockBytes = 8192
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	input := make([]byte, 200)
	for i := range input {
		input[i] = 'a' + byte(i%26)
	}
	cause, err := runIngest(t, ingestDeps(),
		ingestOpts(data, nil), input)
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitEOF {
		t.Fatalf("cause = %s, want eof", cause)
	}
	events := sessionEventsOf(t, data)
	var total int64
	var chunks int
	for _, ev := range events {
		if ev.EventType != protocol.EventTypeCaptureChunk {
			continue
		}
		var chunk protocol.CaptureChunk
		if err := decodePayload(&ev, &chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.ByteCount > 64 {
			t.Fatalf("chunk = %d bytes, over max", chunk.ByteCount)
		}
		total += chunk.ByteCount
		chunks++
	}
	if chunks != 4 || total != 200 {
		t.Fatalf("chunks = %d total = %d, want 4 and 200", chunks, total)
	}
}

func TestIngestRotation(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.ChunkMaxBytes = 1024
	lim.FrameMaxBytes = 4096
	lim.SegmentMaxBytes = 16384
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	input := make([]byte, 40*1024)
	for i := range input {
		input[i] = byte(i)
	}
	cause, err := runIngest(t, ingestDeps(),
		ingestOpts(data, nil), input)
	if err != nil {
		t.Fatal(err)
	}
	if cause != ExitEOF {
		t.Fatalf("cause = %s, want eof", cause)
	}
	events := sessionEventsOf(t, data)
	var rotated int
	for _, ev := range events {
		if ev.EventType == protocol.EventTypeCaptureRotated {
			rotated++
		}
	}
	if rotated != 2 {
		t.Fatalf("rotated = %d, want 2", rotated)
	}
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	segs, err := store.SegmentsOf(context.Background(), storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 3 {
		t.Fatalf("segments = %d, want 3", len(segs))
	}
	raw := 0
	for _, seg := range segs {
		if !seg.Compressed {
			raw++
		}
	}
	if raw != 2 {
		t.Fatalf("raw segments = %d, want 2 (final compressed)", raw)
	}
}

func TestIngestTerminalOrdering(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	root := ingestFixture(t, data, lim, ledger)
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	opts := ingestOpts(data, inReader)
	done := make(chan struct {
		cause string
		err   error
	}, 1)
	go func() {
		cause, err := Ingest(context.Background(), ingestDeps(), opts)
		done <- struct {
			cause string
			err   error
		}{cause, err}
	}()
	if _, err := inWriter.Write([]byte("early bytes ")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	putFile(t, root, storeTestAgent+"/"+storeTestSession, "stop.request", []byte(
		`{"schema":"gifford.capture.stop.v1","reason":"manual","detail":"halt","requested_at":"2026-01-02T03:04:05Z"}`))
	time.Sleep(200 * time.Millisecond)
	_ = inWriter.Close()
	out := <-done
	if out.err != nil {
		t.Fatal(out.err)
	}
	if out.cause != ExitStop {
		t.Fatalf("cause = %s, want stop", out.cause)
	}
	// The terminal event is last: chunks, then stopped.
	events := sessionEventsOf(t, data)
	if len(events) < 4 {
		t.Fatalf("events = %d, want boundary plus chunk plus stop", len(events))
	}
	last := events[len(events)-1]
	if last.EventType != protocol.EventTypeCaptureStopped {
		t.Fatalf("last event = %s, want stopped", last.EventType)
	}
}

// ingestResult carries one run's exit cause across a goroutine.
type ingestResult struct {
	cause string
	err   error
}

// TestIngestGraceStartsAtShutdown proves the exit grace does not
// bound a healthy streaming life: with the grace shrunk below the
// run's length, a paced capture still ends at EOF with its chunks
// committed.
func TestIngestGraceStartsAtShutdown(t *testing.T) {
	skipUnlessAttach(t)
	oldGrace := exitGrace
	exitGrace = 300 * time.Millisecond
	defer func() { exitGrace = oldGrace }()
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan ingestResult, 1)
	go func() {
		cause, err := Ingest(context.Background(), ingestDeps(), ingestOpts(data, inReader))
		done <- ingestResult{cause: cause, err: err}
	}()
	for range 3 {
		time.Sleep(250 * time.Millisecond)
		if _, err := inWriter.Write([]byte("paced line\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := inWriter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("ingest err = %v", res.err)
		}
		if res.cause != ExitEOF {
			t.Fatalf("cause = %s, want eof", res.cause)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("paced ingest did not finish")
	}
	events := sessionEventsOf(t, data)
	chunks := 0
	for _, ev := range events {
		if ev.EventType == protocol.EventTypeCaptureChunk {
			chunks++
		}
	}
	if chunks == 0 {
		t.Fatal("paced run committed no chunks")
	}
}

// TestIngestStallShutdown proves the grace bounds a stuck drain: with
// the writer blocked in its commit and the context cancelled, Ingest
// returns the expired grace with the session lock still held; once
// the writer is released it finishes, frees the lock and leaves no
// goroutine behind.
func TestIngestStallShutdown(t *testing.T) {
	skipUnlessAttach(t)
	oldGrace := exitGrace
	exitGrace = 300 * time.Millisecond
	defer func() { exitGrace = oldGrace }()
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	base := runtime.NumGoroutine()
	entered := make(chan struct{})
	var once sync.Once
	release := make(chan struct{})
	block := func() {
		once.Do(func() { close(entered) })
		<-release
	}
	commitHook.Store(&block)
	defer commitHook.Store(nil)
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan ingestResult, 1)
	go func() {
		cause, err := Ingest(ctx, ingestDeps(), ingestOpts(data, inReader))
		done <- ingestResult{cause: cause, err: err}
	}()
	// One over-cap chunk reaches the hooked commit on the first
	// flush; the pipe stays open so nothing else ends the run.
	if _, err := inWriter.Write(make([]byte, 2048)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("writer did not reach the commit")
	}
	cancel()
	select {
	case res := <-done:
		if res.cause != ExitError || res.err == nil || res.err.Error() != "exit grace expired" {
			t.Fatalf("result = %+v, want expired grace", res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stuck ingest did not return within its bound")
	}
	if err := expectLockBusy(t, data); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitLockFree(t, data)
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > base {
		if !time.Now().Before(deadline) {
			t.Fatalf("goroutines = %d, baseline %d", runtime.NumGoroutine(), base)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// expectLockBusy proves the session lock is still held: a stuck
// writer keeps it, so no second writer can start.
func expectLockBusy(t *testing.T, data string) error {
	t.Helper()
	root, err := openCaptureRoot(data, false)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	_, err = tryLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock", true)
	if err == nil {
		return errors.New("session lock is free while the writer is stuck")
	}
	if !errors.Is(err, ErrBusy) {
		return err
	}
	return nil
}

// TestTokenOKNonceCarried proves the §4.8 start token: the boundary
// screen names exactly initial-<seq>-<nonce> with no extension, and
// n-1 is a real boundary. An .ansi-suffixed ref, a foreign nonce and
// a chunk after an older snapshot all refuse.
func TestTokenOKNonceCarried(t *testing.T) {
	nonce := testNonce
	screenRef := func(seq int64, ref string) protocol.EventEnvelope {
		screen := testScreenPayload()
		screen.SnapshotBlobRef = protocol.BlobRef(ref)
		ev := testEnvelope(t, protocol.EventTypeCaptureInitialScreen, seq, screen)
		ev.Direction = protocol.DirectionOut
		ev.Origin = protocol.OriginHarness
		return ev
	}
	chunkRef := func(seq int64) protocol.EventEnvelope {
		ev := testEnvelope(t, protocol.EventTypeCaptureChunk, seq, protocol.CaptureChunk{
			CaptureSessionID: protocol.OpaqueID(storeTestSession),
			ByteCount:        1, RawBlobRef: "blob://capture/" + storeTestSession + "/seg-1/0-1.raw",
		})
		ev.Direction = protocol.DirectionOut
		ev.Origin = protocol.OriginHarness
		return ev
	}
	attached := testEnvelope(t, protocol.EventTypeCaptureAttached, 0, testAttachedPayload())
	exact := "blob://capture/" + storeTestSession + "/initial-1-" + nonce
	run := &ingestRun{opts: IngestOptions{Session: storeTestSession, StartSequence: 2, Attempt: nonce}}
	if !run.tokenOK([]protocol.EventEnvelope{attached, screenRef(1, exact)}) {
		t.Fatal("exact boundary refused")
	}
	if run.tokenOK([]protocol.EventEnvelope{attached, screenRef(1, exact+".ansi")}) {
		t.Fatal("extension-suffixed ref accepted")
	}
	foreign := "blob://capture/" + storeTestSession + "/initial-1-ffffffffffffffffffffffffffffffff"
	if run.tokenOK([]protocol.EventEnvelope{attached, screenRef(1, foreign)}) {
		t.Fatal("foreign nonce boundary accepted")
	}
	// A chunk after the older snapshot is not a boundary at n-1.
	stale := &ingestRun{opts: IngestOptions{Session: storeTestSession, StartSequence: 3, Attempt: nonce}}
	if stale.tokenOK([]protocol.EventEnvelope{attached, screenRef(1, exact), chunkRef(2)}) {
		t.Fatal("chunk after older snapshot accepted")
	}
}

// TestIngestBatchCap proves one read never builds an unappendable
// batch: 2048 one-byte chunks pause at the flush cap and drain,
// committing every byte instead of failing the store append.
func TestIngestBatchCap(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.ChunkMaxBytes = 1
	ledger := controlRetain + lim.LogMaxBytes + lim.SnapshotMaxBytes
	_ = ingestFixture(t, data, lim, ledger)
	inReader, inWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan ingestResult, 1)
	go func() {
		cause, err := Ingest(context.Background(), ingestDeps(), ingestOpts(data, inReader))
		done <- ingestResult{cause: cause, err: err}
	}()
	input := make([]byte, 2048)
	for i := range input {
		input[i] = 'x'
	}
	if _, err := inWriter.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := inWriter.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("ingest err = %v", res.err)
		}
		if res.cause != ExitEOF {
			t.Fatalf("cause = %s, want eof", res.cause)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("capped ingest did not finish")
	}
	var total int64
	chunks := 0
	for _, ev := range sessionEventsOf(t, data) {
		if ev.EventType != protocol.EventTypeCaptureChunk {
			continue
		}
		var chunk protocol.CaptureChunk
		if err := decodePayload(&ev, &chunk); err != nil {
			t.Fatal(err)
		}
		chunks++
		total += chunk.ByteCount
	}
	if total != 2048 {
		t.Fatalf("chunk bytes = %d, want 2048", total)
	}
	if chunks == 0 {
		t.Fatal("no chunks committed")
	}
}

// waitLockFree polls until the late writer finishes and frees the
// session lock it kept past the grace.
func waitLockFree(t *testing.T, data string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		root, err := openCaptureRoot(data, false)
		if err != nil {
			t.Fatal(err)
		}
		held, err := tryLock(root, storeTestAgent+"/"+storeTestSession+"/ingest.lock", true)
		if err == nil {
			_ = releaseLock(held)
			_ = root.Close()
			return
		}
		_ = root.Close()
		if !errors.Is(err, ErrBusy) {
			t.Fatal(err)
		}
		if !time.Now().Before(deadline) {
			t.Fatal("late writer did not free the lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
