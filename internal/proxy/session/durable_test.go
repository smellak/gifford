package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// durableWall is the wall time of the first durable test event.
var durableWall = time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)

// The ids of the durable test events.
const (
	durableVersion   = "1.2.3"
	durableRecording = "rec-9"
	testDelivery     = protocol.OpaqueID("dlv-9")
	// deliveryStep is the durable payload of the test delivery's steps.
	deliveryStep = `{"delivery_id":"dlv-9","generation":4,"version":10}`
)

// testLineage is the lineage of the durable test events.
func testLineage() DurableLineage {
	return DurableLineage{
		NodeID: "node-1", SessionID: "sess-9", AgentID: "agent-1",
		Harness: protocol.HarnessCodex, HarnessVersion: durableVersion, AdapterType: protocol.AdapterTypeGenericPTY,
	}
}

func newTestQueue(t *testing.T) *DurableQueue {
	t.Helper()
	q, err := NewDurableQueue(testLineage())
	require.NoError(t, err)
	return q
}

// sessionEvent is the nth event of a session, of type t with payload. Its
// wall time is local to some zone, as the session's clock reads it.
func sessionEvent(n int64, t protocol.EventType, payload any) Event {
	return Event{
		Session: durableRecording, Type: t, Payload: payload,
		WallTime:    durableWall.Add(time.Duration(n) * time.Millisecond).In(time.FixedZone("test", 2*3600)),
		MonotonicNS: n * 1000, Sequence: n,
	}
}

func stored(id protocol.ULID) protocol.EmitEventResult {
	return protocol.EmitEventResult{EventID: id, Status: protocol.EmitEventStatusStored}
}

// testDigest is a content hash the session keeps in its recording.
const testDigest protocol.Digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// durableCase is one session payload and the durable payload it becomes.
type durableCase struct {
	t       protocol.EventType
	payload any
	want    string
}

// everyDurableType is one case of each durable type, with the payload the
// session emits for it.
func everyDurableType() []durableCase {
	delivery := testDelivery
	return []durableCase{
		{
			protocol.EventTypePTYOpened,
			PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex, HarnessVersion: durableVersion},
			`{"cols":80,"rows":24,"harness":"codex","harness_version":"1.2.3"}`,
		},
		{
			protocol.EventTypeIOChunk,
			IOChunkEvent{ByteCount: 5, ANSIPresent: true, ScreenHashAfter: testDigest, RecordingSequence: 3, ChunkDigest: testDigest},
			`{"byte_count":5,"ansi_present":true,"recording_sequence":3}`,
		},
		{
			protocol.EventTypeInputKeyChunk,
			InputKeyChunk{PrintableCount: 3, Generation: 1, GuardVersion: 1},
			`{"printable_count":3,"draft_generation":1,"input_guard_version":1}`,
		},
		{
			protocol.EventTypeInputControlKey,
			InputControlKey{Key: "arrow_up", Ctrl: true, Generation: 1, GuardVersion: 1},
			`{"key":"arrow_up","shift":false,"alt":false,"ctrl":true,"draft_generation":1,"input_guard_version":1}`,
		},
		{
			protocol.EventTypeInputPasteStarted,
			InputPaste{Generation: 1, GuardVersion: 2},
			`{"size_bytes":0,"draft_generation":1,"input_guard_version":2}`,
		},
		{
			protocol.EventTypeInputPasteCompleted,
			InputPaste{SizeBytes: 12, Generation: 1, GuardVersion: 3},
			`{"size_bytes":12,"draft_generation":1,"input_guard_version":3}`,
		},
		{
			protocol.EventTypeInputSubmitDetected,
			InputSubmit{Generation: 1, GuardVersion: 4},
			`{"draft_generation":1,"input_guard_version":4}`,
		},
		{
			protocol.EventTypeInputResize,
			InputResize{Cols: 100, Rows: 30, Generation: 1, GuardVersion: 4},
			`{"cols":100,"rows":30,"draft_generation":1,"input_guard_version":4}`,
		},
		{
			protocol.EventTypeInputDraftOpened,
			InputDraft{State: string(protocol.DurableDraftStateOpen), Generation: 2, GuardVersion: 5},
			`{"state":"open","draft_generation":2,"input_guard_version":5}`,
		},
		{
			protocol.EventTypeInputDraftUpdated,
			InputDraft{State: string(protocol.DurableDraftStatePasting), Generation: 2, GuardVersion: 6},
			`{"state":"pasting","draft_generation":2,"input_guard_version":6}`,
		},
		{
			protocol.EventTypeInputDraftSubmitted,
			InputDraft{State: string(protocol.DurableDraftStateClosed), Generation: 2, GuardVersion: 7},
			`{"state":"closed","draft_generation":2,"input_guard_version":7}`,
		},
		{
			protocol.EventTypeInputDraftCancelled,
			InputDraft{State: string(protocol.DurableDraftStateClosed), Generation: 3, GuardVersion: 8},
			`{"state":"closed","draft_generation":3,"input_guard_version":8}`,
		},
		{
			protocol.EventTypeInputDraftCleared,
			InputDraft{State: string(protocol.DurableDraftStateClosed), Generation: 4, GuardVersion: 9},
			`{"state":"closed","draft_generation":4,"input_guard_version":9}`,
		},
		{
			protocol.EventTypeInputDraftUnknown,
			InputDraft{State: string(protocol.DurableDraftStateUnknown), Generation: 4, GuardVersion: 10},
			`{"state":"unknown","draft_generation":4,"input_guard_version":10}`,
		},
		{
			protocol.EventTypeInputDeliveryPrepared,
			DeliveryPrepared{DeliveryID: delivery, Generation: 4, Version: 10},
			deliveryStep,
		},
		{
			protocol.EventTypeInputDeliveryRevalidated,
			DeliveryRevalidated{DeliveryID: delivery, Generation: 4, Version: 10},
			deliveryStep,
		},
		{
			protocol.EventTypeInputDeliveryStarted,
			DeliveryStarted{DeliveryID: delivery, Generation: 4, Version: 10},
			deliveryStep,
		},
		{
			protocol.EventTypeInputDeliveryChunk,
			DeliveryChunk{DeliveryID: delivery, Generation: 4, Version: 10, Bytes: 7},
			`{"delivery_id":"dlv-9","generation":4,"version":10,"bytes":7}`,
		},
		{
			protocol.EventTypeInputDeliveryCommitted,
			DeliveryCommitted{DeliveryID: delivery, Generation: 4, Version: 10},
			deliveryStep,
		},
		{
			protocol.EventTypeInputDeliveryAbortedHumanPriority,
			DeliveryAborted{DeliveryID: delivery, Generation: 4, Version: 11, Reason: "human input arrived"},
			`{"delivery_id":"dlv-9","generation":4,"version":11,"reason":"human input arrived"}`,
		},
		{
			protocol.EventTypeInputDeliveryCancelled,
			DeliveryCancelled{DeliveryID: delivery, Reason: string(protocol.CancelReasonHumanPriority)},
			`{"delivery_id":"dlv-9","reason":"HUMAN_PRIORITY"}`,
		},
		{
			protocol.EventTypeInputDeliveryFailed,
			deliveryFailed(delivery, fmt.Errorf("commit: %w", errDeliveryLine)),
			`{"delivery_id":"dlv-9","reason":"LINE_INVALID"}`,
		},
		{
			protocol.EventTypeCaptureQuotaReached,
			protocol.CaptureQuotaReached{
				CaptureSessionID: durableRecording, QuotaScope: protocol.QuotaScopeAgent, Bytes: 2048, LimitBytes: 1024,
			},
			`{"capture_session_id":"rec-9","quota_scope":"agent","bytes":2048,"limit_bytes":1024}`,
		},
		{
			protocol.EventTypeCaptureGapDetected,
			protocol.CaptureGapDetected{CaptureSessionID: durableRecording, Cause: protocol.CaptureGapCauseQuota},
			`{"capture_session_id":"rec-9","cause":"quota"}`,
		},
		{protocol.EventTypePTYClosed, PTYClosed{}, `{}`},
	}
}

// Every durable type the session emits builds: its closed payload, with
// nothing of the text, the output or the screen, in an envelope of the
// queue's lineage with the profile of the allowlist, in the session's order
// and with its times and sequence.
func TestDurableEventOfEveryType(t *testing.T) {
	t.Parallel()
	cases := everyDurableType()
	covered := make(map[protocol.EventType]bool)
	q := newTestQueue(t)
	for n, c := range cases {
		covered[c.t] = true
		q.Emit(sessionEvent(int64(n), c.t, c.payload))
	}
	for _, spec := range protocol.DurableEventSpecs() {
		require.True(t, covered[spec.Type], "no case for %s", spec.Type)
	}
	require.Len(t, covered, len(protocol.DurableEventSpecs()))

	l := testLineage()
	for n, c := range cases {
		e, ok := q.Head()
		require.True(t, ok, c.t)
		require.NoError(t, protocol.ValidateDurableEventEnvelope(e), c.t)
		require.Equal(t, c.t, e.EventType)
		require.JSONEq(t, c.want, string(e.Payload), c.t)
		require.True(t, e.EventID.Valid())
		require.Equal(t, int64(protocol.SchemaVersion), e.SchemaVersion)
		require.Equal(t, l.NodeID, e.NodeID)
		require.Equal(t, l.SessionID, e.SessionID)
		require.Equal(t, l.AgentID, e.AgentID)
		require.Equal(t, l.Harness, e.Harness)
		require.Equal(t, l.HarnessVersion, e.HarnessVersion)
		require.Equal(t, l.AdapterType, e.AdapterType)
		require.Equal(t, protocol.SourceQualityPTYScreen, e.SourceQuality)
		require.Equal(t, protocol.StreamPTY, e.Stream)
		require.Equal(t, protocol.SensitivityNormal, e.Sensitivity)
		require.False(t, e.RedactionApplied)
		want := sessionEvent(int64(n), c.t, nil)
		require.True(t, want.WallTime.Equal(e.WallTime))
		require.Equal(t, time.UTC, e.WallTime.Location())
		require.Equal(t, want.MonotonicNS, e.MonotonicNS)
		require.Equal(t, want.Sequence, e.Sequence)
		q.Settle(stored(e.EventID))
	}
	_, ok := q.Head()
	require.False(t, ok)
	require.Equal(t, DurableReport{Stored: int64(len(cases))}, q.Report())
}

// Origin and direction follow the type: the pty and capture events are the
// system's, with no direction; output is the harness's, out; the human's
// input goes in; and a delivery step is the orchestrator's, in, with its
// delivery id on the envelope too.
func TestDurableEventProfiles(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		t         protocol.EventType
		origin    protocol.Origin
		direction protocol.Direction
		delivery  protocol.OpaqueID
	}{
		{t: protocol.EventTypePTYOpened, origin: protocol.OriginSystem},
		{t: protocol.EventTypePTYClosed, origin: protocol.OriginSystem},
		{t: protocol.EventTypeCaptureQuotaReached, origin: protocol.OriginSystem},
		{t: protocol.EventTypeCaptureGapDetected, origin: protocol.OriginSystem},
		{t: protocol.EventTypeIOChunk, origin: protocol.OriginHarness, direction: protocol.DirectionOut},
		{t: protocol.EventTypeInputKeyChunk, origin: protocol.OriginHuman, direction: protocol.DirectionIn},
		{t: protocol.EventTypeInputDraftUnknown, origin: protocol.OriginHuman, direction: protocol.DirectionIn},
		{
			t: protocol.EventTypeInputDeliveryPrepared, origin: protocol.OriginOrchestrator,
			direction: protocol.DirectionIn, delivery: testDelivery,
		},
		{
			t: protocol.EventTypeInputDeliveryFailed, origin: protocol.OriginOrchestrator,
			direction: protocol.DirectionIn, delivery: testDelivery,
		},
	} {
		q := newTestQueue(t)
		for n, d := range everyDurableType() {
			if d.t == c.t {
				q.Emit(sessionEvent(int64(n), d.t, d.payload))
			}
		}
		e, ok := q.Head()
		require.True(t, ok, c.t)
		require.Equal(t, c.origin, e.Origin, c.t)
		require.Equal(t, c.direction, e.Direction, c.t)
		require.Equal(t, c.delivery, e.DeliveryID, c.t)
	}
}

// A delivery failure keeps its error's text for the events file, which
// never gets the code, and its fixed code for the durable event, read with
// errors.Is: any failure the session does not name is a failed PTY write.
func TestDurableDeliveryFailedCodes(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		err  error
		want protocol.DurableDeliveryFailureReason
	}{
		{errDeliveryLine, protocol.DurableDeliveryFailureLineInvalid},
		{fmt.Errorf("commit: %w", errDeliveryLine), protocol.DurableDeliveryFailureLineInvalid},
		{errInjectMode, protocol.DurableDeliveryFailureModeInvalid},
		{fmt.Errorf("commit: %w", errInjectMode), protocol.DurableDeliveryFailureModeInvalid},
		{fmt.Errorf("write: %w", ErrSessionNotRunning), protocol.DurableDeliveryFailureSessionNotRunning},
		{fmt.Errorf("write /dev/pts/9: %w", fs.ErrClosed), protocol.DurableDeliveryFailurePTYWriteFailed},
		{errors.New("input/output error"), protocol.DurableDeliveryFailurePTYWriteFailed},
	} {
		failed := deliveryFailed(testDelivery, c.err)
		require.Equal(t, c.want, failed.Code, c.err.Error())
		require.Equal(t, c.err.Error(), failed.Reason)
		logged, err := json.Marshal(failed)
		require.NoError(t, err)
		require.JSONEq(t, fmt.Sprintf(`{"delivery_id":"dlv-9","reason":%q}`, c.err.Error()), string(logged),
			"the events file keeps the text only")

		q := newTestQueue(t)
		q.Emit(sessionEvent(1, protocol.EventTypeInputDeliveryFailed, failed))
		e, ok := q.Head()
		require.True(t, ok)
		require.JSONEq(t, fmt.Sprintf(`{"delivery_id":"dlv-9","reason":%q}`, c.want), string(e.Payload))
	}
}

// Each event is built once, with its own id: the head is the same envelope,
// byte for byte, every time it is read, until the Core settles it, and a
// change to what a read returned changes nothing queued.
func TestDurableEventKeepsItsEnvelope(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t)
	q.Emit(sessionEvent(1, protocol.EventTypeInputSubmitDetected, InputSubmit{Generation: 1, GuardVersion: 2}))
	q.Emit(sessionEvent(1, protocol.EventTypeInputSubmitDetected, InputSubmit{Generation: 1, GuardVersion: 2}))

	first, ok := q.Head()
	require.True(t, ok)
	raw, err := json.Marshal(first)
	require.NoError(t, err)
	first.Payload[0] = 'X'
	q.Sent(first.EventID)
	again, ok := q.Head()
	require.True(t, ok)
	rawAgain, err := json.Marshal(again)
	require.NoError(t, err)
	require.Equal(t, raw, rawAgain, "the same envelope, byte for byte")

	q.Settle(stored(again.EventID))
	second, ok := q.Head()
	require.True(t, ok)
	require.NotEqual(t, again.EventID, second.EventID, "one id per event, even for equal events")
	require.Equal(t, again.Sequence, second.Sequence)
}

// The queue takes off only its head: a result or a refusal for any other
// event changes nothing, and neither does marking another event written.
func TestDurableQueueTakesOnlyTheHead(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t)
	q.Emit(sessionEvent(1, protocol.EventTypePTYClosed, PTYClosed{}))
	q.Emit(sessionEvent(2, protocol.EventTypePTYClosed, PTYClosed{}))
	head, _ := q.Head()
	q.Settle(stored(head.EventID))
	next, _ := q.Head()

	q.Sent(head.EventID)
	q.Settle(stored(head.EventID))
	q.Refuse(head.EventID)
	got, ok := q.Head()
	require.True(t, ok)
	require.Equal(t, next.EventID, got.EventID)
	require.Equal(t, DurableReport{Stored: 1, Pending: 1}, q.Report())
}

// The report counts, without content, what became of each event: stored or
// already stored, rejected, refused by the socket, dropped when full, not
// built, and, among those still queued, the one written and waiting for its
// result and the ones never written. A drop or a rejection is never counted
// as stored.
func TestDurableReport(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t)
	for n := range int64(6) {
		q.Emit(sessionEvent(n, protocol.EventTypeInputKeyChunk, InputKeyChunk{PrintableCount: 1}))
	}
	q.Emit(sessionEvent(6, protocol.EventTypeInputKeyChunk, InputKeyChunk{}))

	settle := func(r protocol.EmitEventResult) {
		head, ok := q.Head()
		require.True(t, ok)
		q.Sent(head.EventID)
		r.EventID = head.EventID
		q.Settle(r)
	}
	settle(protocol.EmitEventResult{Status: protocol.EmitEventStatusStored})
	settle(protocol.EmitEventResult{Status: protocol.EmitEventStatusAlreadyStored})
	settle(protocol.EmitEventResult{
		Status: protocol.EmitEventStatusRejected, Reason: protocol.EmitEventRejectReasonConflict,
	})
	head, _ := q.Head()
	q.Refuse(head.EventID)
	head, _ = q.Head()
	q.Sent(head.EventID)
	require.Equal(t, DurableReport{
		Stored: 2, Rejected: 1, Refused: 1, Invalid: 1, Pending: 1, Unconfirmed: 1,
	}, q.Report())
}

// A full queue drops the new events and counts them: the queued ones stay,
// the one written and waiting for its result included, unchanged and in
// order. The dropped ones leave their gap in the sequence, and once the head
// is settled there is room again.
func TestDurableQueueIsBounded(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t)
	chunk := func(n int64) Event {
		return sessionEvent(n, protocol.EventTypeIOChunk, IOChunkEvent{ByteCount: 64, RecordingSequence: n})
	}
	n := int64(0)
	for ; q.dropped() == 0 && n < 100_000; n++ {
		q.Emit(chunk(n))
	}
	require.Equal(t, int64(1), q.Report().Dropped, "the queue fills")
	queued := n - 1
	first, ok := q.Head()
	require.True(t, ok)
	q.Sent(first.EventID)
	q.mu.Lock()
	full := q.bytes
	q.mu.Unlock()
	require.LessOrEqual(t, full, MaxDurableBytes)
	require.Greater(t, full+q.queued[0].size, MaxDurableBytes, "filled to the cap")

	for range 10 {
		q.Emit(chunk(n))
		n++
	}
	require.Equal(t, DurableReport{Dropped: 11, Pending: queued - 1, Unconfirmed: 1}, q.Report())
	head, ok := q.Head()
	require.True(t, ok)
	require.Equal(t, first, head, "the head stays, neither dropped nor rebuilt")

	q.Settle(stored(first.EventID))
	q.Emit(chunk(n))
	require.Equal(t, DurableReport{Stored: 1, Dropped: 11, Pending: queued}, q.Report())
	var last protocol.EventEnvelope
	for {
		e, ok := q.Head()
		if !ok {
			break
		}
		last = e
		q.Settle(stored(e.EventID))
	}
	require.Equal(t, n, last.Sequence, "the dropped events leave their gap in the sequence")
}

// dropped is Report().Dropped without counting what is queued, so a loop
// that fills the queue stays linear.
func (q *DurableQueue) dropped() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.report.Dropped
}

// The cap is inclusive: an event that fills the queue exactly to
// MaxDurableBytes is queued, and one byte less room drops it. The room is
// set directly, so the test does not fill 4 MiB.
func TestDurableQueueTakesAnExactFit(t *testing.T) {
	t.Parallel()
	chunk := sessionEvent(1, protocol.EventTypeIOChunk, IOChunkEvent{ByteCount: 64, RecordingSequence: 1})
	probe := newTestQueue(t)
	probe.Emit(chunk)
	probe.mu.Lock()
	size := probe.bytes
	probe.mu.Unlock()
	require.Positive(t, size)

	for name, tc := range map[string]struct {
		room int
		want DurableReport
	}{
		"exactly its size": {room: size, want: DurableReport{Pending: 1}},
		"one byte less":    {room: size - 1, want: DurableReport{Dropped: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			q := newTestQueue(t)
			q.mu.Lock()
			q.bytes = MaxDurableBytes - tc.room
			q.mu.Unlock()
			q.Emit(chunk)

			require.Equal(t, tc.want, q.Report())
			q.mu.Lock()
			defer q.mu.Unlock()
			want := MaxDurableBytes - tc.room
			if tc.want.Pending == 1 {
				want = MaxDurableBytes
			}
			require.Equal(t, want, q.bytes, "the same event has the same size in every queue")
		})
	}
}

// Only the durable types are the queue's: another one is not counted. A
// durable type whose payload is not the session's, or does not validate as
// a durable one, is counted and never queued.
func TestDurableQueueBuildsOnlyWhatValidates(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t)
	q.Emit(sessionEvent(1, protocol.EventTypeOutputPTYChunk, IOChunkEvent{ByteCount: 1}))
	q.Emit(sessionEvent(2, protocol.EventType("control.pause"), PTYClosed{}))
	require.Equal(t, DurableReport{}, q.Report())

	for _, c := range []struct {
		name    string
		t       protocol.EventType
		payload any
	}{
		{"no payload", protocol.EventTypeIOChunk, nil},
		{"another payload", protocol.EventTypeIOChunk, InputPaste{SizeBytes: 1}},
		{"an empty chunk", protocol.EventTypeIOChunk, IOChunkEvent{}},
		{"a chunk too large", protocol.EventTypeIOChunk, IOChunkEvent{ByteCount: 16385}},
		{"an unknown key", protocol.EventTypeInputControlKey, InputControlKey{Key: "a"}},
		{"an unknown state", protocol.EventTypeInputDraftUpdated, InputDraft{State: "typing"}},
		{"a closed draft opened", protocol.EventTypeInputDraftOpened, InputDraft{State: string(protocol.DurableDraftStateClosed)}},
		{
			"a free abort reason", protocol.EventTypeInputDeliveryAbortedHumanPriority,
			DeliveryAborted{DeliveryID: testDelivery, Reason: "the screen said so"},
		},
		{
			"a failure with no code", protocol.EventTypeInputDeliveryFailed,
			DeliveryFailed{DeliveryID: testDelivery, Reason: "write: broken pipe"},
		},
		{"no delivery id", protocol.EventTypeInputDeliveryPrepared, DeliveryPrepared{}},
		{
			"an unmeasured harness", protocol.EventTypePTYOpened,
			PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex},
		},
		{
			"another harness version", protocol.EventTypePTYOpened,
			PTYOpened{Cols: 80, Rows: 24, Harness: protocol.HarnessCodex, HarnessVersion: "9.9.9"},
		},
		{"a screen too large", protocol.EventTypeInputResize, InputResize{Cols: 1000, Rows: 1000}},
	} {
		before := q.Report().Invalid
		q.Emit(sessionEvent(3, c.t, c.payload))
		require.Equal(t, before+1, q.Report().Invalid, c.name)
	}
	_, ok := q.Head()
	require.False(t, ok, "nothing queued")
}

// A queue needs a whole lineage: without a valid node, session, agent or
// harness no event is built at all.
func TestNewDurableQueueRefusesALineage(t *testing.T) {
	t.Parallel()
	for name, edit := range map[string]func(*DurableLineage){
		"no node":            func(l *DurableLineage) { l.NodeID = "" },
		"an invalid node":    func(l *DurableLineage) { l.NodeID = "Node 1" },
		"no session":         func(l *DurableLineage) { l.SessionID = "" },
		"no agent":           func(l *DurableLineage) { l.AgentID = "" },
		"no harness":         func(l *DurableLineage) { l.Harness = "" },
		"no harness version": func(l *DurableLineage) { l.HarnessVersion = "" },
		"no adapter":         func(l *DurableLineage) { l.AdapterType = "" },
	} {
		l := testLineage()
		edit(&l)
		q, err := NewDurableQueue(l)
		require.ErrorIs(t, err, ErrInvalidLineage, name)
		require.Nil(t, q)
	}
}

// A queued event signals the head, never waiting: one pending signal stands
// for any number of events.
func TestDurableQueueSignalsTheHead(t *testing.T) {
	t.Parallel()
	q := newTestQueue(t)
	q.Emit(sessionEvent(1, protocol.EventTypePTYClosed, PTYClosed{}))
	q.Emit(sessionEvent(2, protocol.EventTypePTYClosed, PTYClosed{}))
	select {
	case <-q.HeadChanged():
	default:
		t.Fatal("no signal")
	}
	select {
	case <-q.HeadChanged():
		t.Fatal("a second signal")
	default:
	}
}

// An event id is a ULID minted from the event's time and fresh entropy, so
// ids order by time and two of the same millisecond differ.
func TestMintDurableEventID(t *testing.T) {
	t.Parallel()
	a, err := mintDurableEventID(durableWall)
	require.NoError(t, err)
	b, err := mintDurableEventID(durableWall)
	require.NoError(t, err)
	later, err := mintDurableEventID(durableWall.Add(time.Millisecond))
	require.NoError(t, err)
	for _, id := range []protocol.ULID{a, b, later} {
		require.True(t, id.Valid(), id)
	}
	require.NotEqual(t, a, b)
	require.Equal(t, a[:10], b[:10], "the same millisecond")
	require.Less(t, string(a[:10]), string(later[:10]))
	_, err = mintDurableEventID(time.Unix(-1, 0))
	require.Error(t, err)
}
