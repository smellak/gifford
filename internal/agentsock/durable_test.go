//go:build unix

package agentsock

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// fakeDurable is a durable event queue the test fills. It logs, in order,
// what the socket did with its events, and counts its reads of the head.
type fakeDurable struct {
	mu      sync.Mutex
	events  []protocol.EventEnvelope
	reads   int
	log     []string
	changed chan struct{}
}

func newFakeDurable(events ...protocol.EventEnvelope) *fakeDurable {
	return &fakeDurable{events: events, changed: make(chan struct{}, 1)}
}

func (d *fakeDurable) Head() (protocol.EventEnvelope, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reads++
	if len(d.events) == 0 {
		return protocol.EventEnvelope{}, false
	}
	return d.events[0], true
}

func (d *fakeDurable) HeadChanged() <-chan struct{} { return d.changed }

func (d *fakeDurable) Sent(id protocol.ULID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.log = append(d.log, sentEntry(id))
}

func (d *fakeDurable) Settle(result protocol.EmitEventResult) {
	d.take(result.EventID, settledEntry(result))
}

func (d *fakeDurable) Refuse(id protocol.ULID) { d.take(id, refusedEntry(id)) }

// take logs entry and takes the head off. Anything but the head is logged
// as such and stays.
func (d *fakeDurable) take(id protocol.ULID, entry string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.events) == 0 || d.events[0].EventID != id {
		entry += " (not the head)"
	} else {
		d.events = d.events[1:]
	}
	d.log = append(d.log, entry)
}

// push queues events and signals the head, never waiting, as the session
// does.
func (d *fakeDurable) push(events ...protocol.EventEnvelope) {
	d.mu.Lock()
	d.events = append(d.events, events...)
	d.mu.Unlock()
	select {
	case d.changed <- struct{}{}:
	default:
	}
}

// taken is the log so far.
func (d *fakeDurable) taken() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.log...)
}

func (d *fakeDurable) headReads() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.reads
}

func sentEntry(id protocol.ULID) string { return "sent " + string(id) }

func settledEntry(r protocol.EmitEventResult) string {
	return fmt.Sprintf("settled %s %s %s", r.EventID, r.Status, r.Reason)
}

func refusedEntry(id protocol.ULID) string { return "refused " + string(id) }

// durableEvent is the nth durable event of the test connection: an io.chunk
// of the registered harness, with its own id and sequence.
func durableEvent(n int64) protocol.EventEnvelope {
	manifest := testRegister().Manifest
	return protocol.EventEnvelope{
		EventID:       protocol.ULID(fmt.Sprintf("01JB000000000000000000D%03d", n)),
		SchemaVersion: protocol.SchemaVersion, NodeID: testNode,
		SessionID: testSession, AgentID: testAgent,
		Harness: manifest.Harness, HarnessVersion: manifest.HarnessVersion, AdapterType: manifest.AdapterType,
		SourceQuality: protocol.SourceQualityPTYScreen, EventType: protocol.EventTypeIOChunk,
		Direction: protocol.DirectionOut, Stream: protocol.StreamPTY, Origin: protocol.OriginHarness,
		WallTime: sentAt, MonotonicNS: n, Sequence: n,
		Payload: protocol.Payload(fmt.Sprintf(
			`{"byte_count":1,"ansi_present":false,"recording_sequence":%d}`, n)),
		Sensitivity: protocol.SensitivityNormal,
	}
}

func nativeDurableEvent(n int64) protocol.EventEnvelope {
	manifest := testRegister().Manifest
	return protocol.EventEnvelope{
		EventID:       protocol.ULID(fmt.Sprintf("01JB000000000000000000N%03d", n)),
		SchemaVersion: protocol.SchemaVersion, NodeID: testNode,
		SessionID: testSession, AgentID: testAgent,
		Harness: manifest.Harness, HarnessVersion: manifest.HarnessVersion, AdapterType: manifest.AdapterType,
		SourceQuality: protocol.SourceQualityNativeHook, EventType: protocol.EventTypeQuestionResolved,
		Stream: protocol.StreamNativeEvent, Origin: protocol.OriginHarness,
		WallTime: sentAt.UTC(), Sequence: n, CorrelationID: protocol.OpaqueID(fmt.Sprintf("request-%d", n)),
		Payload: protocol.Payload(fmt.Sprintf(
			`{"emitter_event_id":"native-%d","result":"ok"}`, n)),
		Sensitivity: protocol.SensitivityNormal,
	}
}

// takesEvents is the announcement of a Core that takes durable events.
var takesEvents = protocol.AgentSocketSupport{DurableEvents: true}

func stored(id protocol.ULID) protocol.EmitEventResult {
	return protocol.EmitEventResult{EventID: id, Status: protocol.EmitEventStatusStored}
}

func rejected(id protocol.ULID, reason protocol.EmitEventRejectReason) protocol.EmitEventResult {
	return protocol.EmitEventResult{EventID: id, Status: protocol.EmitEventStatusRejected, Reason: reason}
}

// result sends the Core's result answering frame and returns its frame id.
func (k *core) result(frame int64, res protocol.EmitEventResult) int64 {
	k.t.Helper()
	return k.send(protocol.AgentOpEmitEventResult, frame, res)
}

func listenDurable(t *testing.T, queue *fakeDurable, edit func(*Options)) *testSocket {
	t.Helper()
	return listenTest(t, func(opts *Options) {
		opts.Durable = queue
		if edit != nil {
			edit(opts)
		}
	})
}

// greetAgain registers a Core that redialled, reading its first report.
func (k *core) greetAgain() {
	k.t.Helper()
	k.send(protocol.AgentOpRegisterAgentResult, greetingFrameID,
		protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(k.t, k.read(), 0)
}

func TestDurableTestEventsValidate(t *testing.T) {
	t.Parallel()
	for n := int64(1); n <= 9; n++ {
		require.NoError(t, protocol.ValidateDurableEventEnvelope(durableEvent(n)))
	}
	require.NotEqual(t, durableEvent(1).EventID, durableEvent(2).EventID, "one id per event")
}

func TestNativeDurableHeadUsesItsOwnClosedProfile(t *testing.T) {
	t.Parallel()
	event := nativeDurableEvent(1)
	require.NoError(t, protocol.ValidateNativeDurableEvent(event))
	require.Error(t, protocol.ValidateDurableEventEnvelope(event))
	queue := newFakeDurable(event)
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	frame := k.read()
	require.Equal(t, event, eventOf(t, frame))
	k.result(frame.FrameID, stored(event.EventID))
	require.Eventually(t, func() bool {
		return len(queue.taken()) == 2
	}, time.Second, time.Millisecond)
}

// The head goes only once the Core of the connection announces it takes
// durable events, with the next frame id; the guard and the heartbeat do not
// wait for it.
func TestDurableHeadGoesOnTheAnnouncement(t *testing.T) {
	t.Parallel()
	queue := newFakeDurable(durableEvent(1))
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)

	k.announce(takesEvents)
	f := k.read()
	require.Equal(t, int64(4), f.FrameID, "greeting 1, report 2, heartbeat 3, event 4")
	require.Equal(t, durableEvent(1), eventOf(t, f))
}

// Without an announcement that takes durable events, nothing durable goes
// and the queue is not even read, while the guard and the heartbeat go on.
// The first valid announcement is the connection's even when it takes none,
// and a later one that takes them is only counted.
func TestDurableWaitsForTheAnnouncement(t *testing.T) {
	t.Parallel()
	for name, first := range map[string]*protocol.AgentSocketSupport{
		"no announcement": nil,
		"nothing":         {},
		"observations":    {ReportStateObservation: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			queue := newFakeDurable(durableEvent(1))
			ts := listenDurable(t, queue, nil)
			k := dialCore(t, ts.srv.Path())
			k.greet()
			if first != nil {
				k.announce(*first)
				k.announce(takesEvents)
			}
			k.synced()
			ts.guard.set(protocol.DraftStateOpen, 1, 1)
			require.Equal(t, int64(1), reportOf(t, k.read(), 0).InputGuardVersion)
			ts.clock.fire(t, HeartbeatEvery)
			require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)
			require.Zero(t, queue.headReads(), "the queue is not read")
			require.Empty(t, queue.taken())
			if first != nil {
				require.Equal(t, int64(1), ts.srv.DuplicateAnnouncements())
			}
		})
	}
}

// One durable event at a time: the next goes only once the Core settles the
// last, and right then. Each keeps its own id, and the queue learns, in
// order, what was sent and how each one settled.
func TestDurableOneEventAtATime(t *testing.T) {
	t.Parallel()
	e1, e2, e3 := durableEvent(1), durableEvent(2), durableEvent(3)
	queue := newFakeDurable(e1, e2)
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	f1 := k.read()
	require.Equal(t, e1, eventOf(t, f1))

	// e2 waits, and so does e3, queued while e1 is in flight.
	queue.push(e3)
	k.synced()
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)
	require.Equal(t, []string{sentEntry(e1.EventID)}, queue.taken())

	k.result(f1.FrameID, stored(e1.EventID))
	f2 := k.read()
	require.Equal(t, e2, eventOf(t, f2))
	require.Equal(t, f1.FrameID+3, f2.FrameID, "report, heartbeat, event")
	already := protocol.EmitEventResult{EventID: e2.EventID, Status: protocol.EmitEventStatusAlreadyStored}
	k.result(f2.FrameID, already)
	f3 := k.read()
	require.Equal(t, e3, eventOf(t, f3))
	k.result(f3.FrameID, stored(e3.EventID))
	k.synced()
	require.Equal(t, []string{
		sentEntry(e1.EventID), settledEntry(stored(e1.EventID)),
		sentEntry(e2.EventID), settledEntry(already),
		sentEntry(e3.EventID), settledEntry(stored(e3.EventID)),
	}, queue.taken())
}

// A result settles the event in flight only when it answers that event's
// frame and names that event. Any other is refused as an invalid frame and
// settles nothing: the event goes on waiting, and the next stays queued.
func TestDurableResultMustAnswerTheEventInFlight(t *testing.T) {
	t.Parallel()
	e1, e2 := durableEvent(1), durableEvent(2)
	queue := newFakeDurable(e1, e2)
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	f1 := k.read()
	require.Equal(t, e1, eventOf(t, f1))

	for name, wrong := range map[string]struct {
		frame  int64
		result protocol.EmitEventResult
	}{
		"an earlier frame":       {f1.FrameID - 1, stored(e1.EventID)},
		"a later frame":          {f1.FrameID + 1, stored(e1.EventID)},
		"another event":          {f1.FrameID, stored(e2.EventID)},
		"another event rejected": {f1.FrameID, rejected(e2.EventID, protocol.EmitEventRejectReasonConflict)},
	} {
		id := k.result(wrong.frame, wrong.result)
		require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id), name)
	}
	k.synced()
	require.Equal(t, []string{sentEntry(e1.EventID)}, queue.taken(), "nothing settled")

	k.result(f1.FrameID, stored(e1.EventID))
	f2 := k.read()
	require.Equal(t, e2, eventOf(t, f2))
	// The same result again answers a frame no longer in flight.
	id := k.result(f1.FrameID, stored(e1.EventID))
	require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id))
	k.synced()
	require.Equal(t, []string{
		sentEntry(e1.EventID), settledEntry(stored(e1.EventID)), sentEntry(e2.EventID),
	}, queue.taken())
}

// REJECTED is terminal: the queue gets the result as it came, never as
// stored, the event is not written again, on this connection or the next,
// and the next event goes.
func TestDurableRejectedIsTerminal(t *testing.T) {
	t.Parallel()
	e1, e2, e3 := durableEvent(1), durableEvent(2), durableEvent(3)
	queue := newFakeDurable(e1, e2, e3)
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	f1 := k.read()
	require.Equal(t, e1, eventOf(t, f1))
	conflict := rejected(e1.EventID, protocol.EmitEventRejectReasonConflict)
	k.result(f1.FrameID, conflict)
	f2 := k.read()
	require.Equal(t, e2, eventOf(t, f2))
	notAllowed := rejected(e2.EventID, protocol.EmitEventRejectReasonNotAllowed)
	k.result(f2.FrameID, notAllowed)
	require.Equal(t, e3, eventOf(t, k.read()))
	require.NoError(t, k.c.Close())

	next := redial(t, ts.srv.Path())
	next.greetAgain()
	next.announce(takesEvents)
	require.Equal(t, e3, eventOf(t, next.read()), "only the event not settled goes again")
	next.synced()
	require.Equal(t, []string{
		sentEntry(e1.EventID), settledEntry(conflict),
		sentEntry(e2.EventID), settledEntry(notAllowed),
		sentEntry(e3.EventID), sentEntry(e3.EventID),
	}, queue.taken())
}

// A connection that ends before the result leaves the event in the queue,
// neither dropped nor renumbered. The next connection writes it again once
// its own Core announces: byte for byte the same envelope, with that
// connection's next frame id. Only a result answering the new frame settles
// it.
func TestDurableHeadGoesAgainOnTheNextConnection(t *testing.T) {
	t.Parallel()
	e1, e2 := durableEvent(1), durableEvent(2)
	queue := newFakeDurable(e1, e2)
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.synced()
	k.synced()
	k.announce(takesEvents)
	first := k.read()
	require.Equal(t, int64(5), first.FrameID, "greeting 1, reports 2 to 4, event 5")
	require.Equal(t, e1, eventOf(t, first))
	require.NoError(t, k.c.Close())

	next := redial(t, ts.srv.Path())
	next.greetAgain()
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, next.read().Op, "support is not inherited")
	next.announce(takesEvents)
	again := next.read()
	require.Equal(t, int64(4), again.FrameID, "greeting 1, report 2, heartbeat 3, event 4")
	require.Equal(t, []byte(first.Body), []byte(again.Body), "the same envelope, byte for byte")

	id := next.result(first.FrameID, stored(e1.EventID))
	require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, next.read(), id),
		"the frame of the connection that ended")
	next.result(again.FrameID, stored(e1.EventID))
	require.Equal(t, e2, eventOf(t, next.read()))
	next.synced()
	require.Equal(t, []string{
		sentEntry(e1.EventID), sentEntry(e1.EventID), settledEntry(stored(e1.EventID)),
		sentEntry(e2.EventID),
	}, queue.taken())
}

// While an event waits for its result, everything else goes on: delivery
// orders and their results, cancel, the guard reports, the heartbeat and
// resync. Only the next durable event waits.
func TestDurableWaitHoldsNothingElse(t *testing.T) {
	t.Parallel()
	e1, e2 := durableEvent(1), durableEvent(2)
	queue := newFakeDurable(e1, e2)
	d := newFakeDelivery()
	ts := listenDurable(t, queue, func(opts *Options) { opts.Delivery = d })
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	f1 := k.read()
	require.Equal(t, e1, eventOf(t, f1))

	k.prepared("dlv-1")
	cancel := protocol.CancelDelivery{DeliveryID: "dlv-1", Reason: protocol.CancelReasonHumanPriority}
	require.Equal(t, protocol.DeliveryResultKindCancelled, k.order(protocol.AgentOpCancelDelivery, cancel).Result)
	k.prepared("dlv-2")
	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-2"))
	d.waitCall(t, commitCall("dlv-2", protocol.InjectModeTyped))
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), id).Result)
	ts.guard.set(protocol.DraftStateOpen, 1, 1)
	require.Equal(t, int64(1), reportOf(t, k.read(), 0).InputGuardVersion)
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)
	k.synced()
	require.Equal(t, []string{sentEntry(e1.EventID)}, queue.taken(), "e2 still waits")

	k.result(f1.FrameID, stored(e1.EventID))
	require.Equal(t, e2, eventOf(t, k.read()))
}

// A head that is not of the connection, or does not validate as a durable
// event, is refused unwritten, never stored, and the next head goes with
// the next frame id: refusals leave no gap in the frame ids.
func TestDurableRefusesWhatCannotGo(t *testing.T) {
	t.Parallel()
	otherSession := durableEvent(1)
	otherSession.SessionID = "sess-2"
	foreign := durableEvent(2)
	foreign.AgentID = otherAgent
	harness := durableEvent(3)
	harness.Harness = protocol.HarnessClaudeCode
	version := durableEvent(4)
	version.HarnessVersion = "9.9.9"
	adapter := durableEvent(5)
	adapter.AdapterType = protocol.AdapterTypeNativeMod
	noNode := durableEvent(6)
	noNode.NodeID = ""
	notDurable := durableEvent(7)
	notDurable.EventType, notDurable.Payload = protocol.EventTypeOutputPTYChunk, protocol.Payload(`{}`)
	digest := durableEvent(8)
	digest.Payload = protocol.Payload(`{"byte_count":1,"ansi_present":false,"recording_sequence":8,` +
		`"chunk_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)
	// The run's retained event is never durable: no result answers it.
	retained := testAdapterEvent()
	good := durableEvent(9)
	refused := []protocol.EventEnvelope{
		otherSession, foreign, harness, version, adapter, noNode, notDurable, digest, retained,
	}
	queue := newFakeDurable(refused...)
	queue.push(good)
	ts := listenDurable(t, queue, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	f := k.read()
	require.Equal(t, int64(3), f.FrameID, "greeting 1, report 2, event 3")
	require.Equal(t, good, eventOf(t, f))
	k.synced()
	want := make([]string, 0, len(refused)+1)
	for _, e := range refused {
		want = append(want, refusedEntry(e.EventID))
	}
	require.Equal(t, append(want, sentEntry(good.EventID)), queue.taken())
}

// Durable events need both the queue and the Core's announcement. Without
// either, nothing durable goes, the queue is not read and a result is
// refused as an order this proxy does not run, as before; with both, a
// result answering no event in flight is an invalid frame.
func TestDurableNeedsTheQueueAndTheAnnouncement(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		queue, announces bool
		delivery         bool
		want             protocol.FrameErrorCode
	}{
		{name: "neither", want: protocol.FrameErrorCodeUnsupportedOp},
		{name: "no queue", announces: true, want: protocol.FrameErrorCodeUnsupportedOp},
		{name: "no queue with deliveries", announces: true, delivery: true, want: protocol.FrameErrorCodeUnsupportedOp},
		{name: "no announcement", queue: true, want: protocol.FrameErrorCodeUnsupportedOp},
		{name: "no announcement with deliveries", queue: true, delivery: true, want: protocol.FrameErrorCodeUnsupportedOp},
		{name: "both", queue: true, announces: true, want: protocol.FrameErrorCodeInvalidFrame},
		{name: "both with deliveries", queue: true, announces: true, delivery: true, want: protocol.FrameErrorCodeInvalidFrame},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			queue := newFakeDurable()
			ts := listenTest(t, func(opts *Options) {
				if tc.queue {
					opts.Durable = queue
				}
				if tc.delivery {
					opts.Delivery = newFakeDelivery()
				}
			})
			k := dialCore(t, ts.srv.Path())
			k.greet()
			if tc.announces {
				k.announce(takesEvents)
			} else {
				k.announce(protocol.AgentSocketSupport{ReportStateObservation: true})
			}
			id := k.result(2, stored(durableEvent(1).EventID))
			require.Equal(t, tc.want, errorOf(t, k.read(), id))

			queue.push(durableEvent(1))
			if tc.queue && tc.announces {
				require.Equal(t, durableEvent(1), eventOf(t, k.read()), "queued, then written")
				return
			}
			k.synced()
			ts.clock.fire(t, HeartbeatEvery)
			require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)
			require.Zero(t, queue.headReads(), "the queue is not read")
		})
	}
}

// The run's retained event goes as before, after the greeting and with no
// result, beside the durable ones: it is never the event in flight.
func TestDurableLeavesTheRetainedEventAlone(t *testing.T) {
	t.Parallel()
	events := newFakeEvents()
	retained := testAdapterEvent()
	events.set(retained)
	e1, e2 := durableEvent(1), durableEvent(2)
	queue := newFakeDurable(e1)
	ts := listenDurable(t, queue, func(opts *Options) { opts.Events = events })
	k := dialCore(t, ts.srv.Path())
	hello := k.read()
	k.send(protocol.AgentOpRegisterAgentResult, hello.FrameID,
		protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	legacy := k.read()
	require.Equal(t, retained, eventOf(t, legacy))
	reportOf(t, k.read(), 0)
	k.announce(takesEvents)
	f1 := k.read()
	require.Equal(t, e1, eventOf(t, f1))

	id := k.result(legacy.FrameID, stored(retained.EventID))
	require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id))
	k.result(f1.FrameID, stored(e1.EventID))
	k.synced()
	queue.push(e2)
	require.Equal(t, e2, eventOf(t, k.read()))
	k.synced()
	require.Equal(t, []string{
		sentEntry(e1.EventID), settledEntry(stored(e1.EventID)), sentEntry(e2.EventID),
	}, queue.taken())
}

// Results take a place among the bounded replies: a Core that keeps sending
// them without reading the refusals fills it, and the connection closes.
func TestDurableResultsAreBounded(t *testing.T) {
	t.Parallel()
	ts := listenDurable(t, newFakeDurable(), nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takesEvents)
	require.NoError(t, k.c.(*net.UnixConn).SetReadBuffer(4096))

	closed := false
	for range 200000 {
		f := testFrame(t, protocol.AgentOpEmitEventResult, k.next, 2, stored(durableEvent(1).EventID))
		if err := WriteFrame(k.c, f); err != nil {
			closed = true
			break
		}
		k.next++
	}
	require.True(t, closed, "the session kept queueing results")
}
