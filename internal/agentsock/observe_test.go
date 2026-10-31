//go:build unix

package agentsock

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// fakeObservations hands out the cuts a test queues, one per Observe, and
// counts the calls.
type fakeObservations struct {
	mu      sync.Mutex
	queued  []protocol.StateObservationReport
	calls   int
	changed chan struct{}
}

func newFakeObservations(cuts ...protocol.StateObservationReport) *fakeObservations {
	return &fakeObservations{queued: cuts, changed: make(chan struct{}, 1)}
}

func (o *fakeObservations) Observe() (protocol.StateObservationReport, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls++
	if len(o.queued) == 0 {
		return protocol.StateObservationReport{}, false
	}
	cut := o.queued[0]
	o.queued = o.queued[1:]
	return cut, true
}

func (o *fakeObservations) ObservationChanged() <-chan struct{} { return o.changed }

// queue adds cuts for the next Observe calls, without signalling.
func (o *fakeObservations) queue(cuts ...protocol.StateObservationReport) {
	o.mu.Lock()
	o.queued = append(o.queued, cuts...)
	o.mu.Unlock()
}

// signal tells the socket a new cut may differ, never waiting.
func (o *fakeObservations) signal() {
	select {
	case o.changed <- struct{}{}:
	default:
	}
}

func (o *fakeObservations) observed() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls
}

// observationCut is a valid cut of generation g, as a source makes it: the
// socket fills in the ids and the harness.
func observationCut(g int64, rows ...string) protocol.StateObservationReport {
	r := protocol.StateObservationReport{
		ObservationID:         protocol.OpaqueID(fmt.Sprintf("observation-%d", g)),
		ObservedAt:            sentAt.Add(time.Duration(g) * time.Second),
		ObservationGeneration: g, OutputGeneration: g, EventGeneration: 1, InputGuardVersion: 1,
		ScreenState: protocol.StateObservationScreenWithheld, ScreenRows: []string{},
		Activity: protocol.StateObservationActivity{
			BytesLast500ms: 1, BytesLast2s: 2, BytesLast10s: 3, SecondsSinceLastOutput: 0.25,
		},
		NativeEvents: []protocol.StateObservationEvent{},
	}
	if len(rows) > 0 {
		r.ScreenState = protocol.StateObservationScreenAvailable
		r.ScreenRows = rows
		r.Cols = 80
		r.Cursor = &protocol.StateObservationCursor{Row: int64(len(rows) - 1)}
	}
	return r
}

// sent is cut as the socket writes it, with the ids and the harness of the
// test registration.
func sent(cut protocol.StateObservationReport) protocol.StateObservationReport {
	manifest := testRegister().Manifest
	cut.AgentID, cut.SessionID = testAgent, testSession
	cut.Harness, cut.HarnessVersion = manifest.Harness, manifest.HarnessVersion
	return cut
}

func observationOf(t *testing.T, f protocol.AgentFrame) protocol.StateObservationReport {
	t.Helper()
	require.Equal(t, protocol.AgentOpReportStateObservation, f.Op)
	require.Nil(t, f.ReplyTo)
	var r protocol.StateObservationReport
	require.NoError(t, json.Unmarshal(f.Body, &r))
	return r
}

func listenObserved(t *testing.T, source *fakeObservations) *testSocket {
	t.Helper()
	return listenTest(t, func(opts *Options) { opts.Observations = source })
}

// takes is the announcement of a Core that takes state observations.
var takes = protocol.AgentSocketSupport{ReportStateObservation: true}

// announce sends the Core's support announcement and returns its frame id.
func (k *core) announce(support protocol.AgentSocketSupport) int64 {
	k.t.Helper()
	return k.send(protocol.AgentOpAnnounceSupport, 0, support)
}

// synced sends a resync and reads its answer as the next frame. The reader
// takes the frames in order, so every frame sent before it has been read,
// and none of them was answered.
func (k *core) synced() {
	k.t.Helper()
	id := k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: testAgent})
	reportOf(k.t, k.read(), id)
}

// requireNoObservationYet proves nothing was cut or written since the
// guard report: the next frame is the heartbeat the test fires.
func requireNoObservationYet(t *testing.T, ts *testSocket, k *core, source *fakeObservations, cuts int) {
	t.Helper()
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)
	require.Equal(t, cuts, source.observed())
}

// redial is the next Core connection to a socket whose previous Core
// left: it retries while the socket still serves the old one.
func redial(t *testing.T, path string) *core {
	t.Helper()
	var k *core
	require.Eventually(t, func() bool {
		c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err != nil {
			return false
		}
		if c.SetReadDeadline(time.Now().Add(250*time.Millisecond)) != nil {
			_ = c.Close()
			return false
		}
		hello, err := ReadFrame(c)
		if err != nil || hello.Op != protocol.AgentOpRegisterAgent {
			_ = c.Close()
			return false
		}
		k = &core{t: t, c: c, next: 1}
		return true
	}, 10*time.Second, 10*time.Millisecond, "a new Core connection was not admitted")
	t.Cleanup(func() { _ = k.c.Close() })
	require.NoError(t, k.c.SetReadDeadline(time.Time{}))
	return k
}

// Nothing is cut before the Core of the connection announces it takes
// observations, while the guard and the heartbeat go on: a Core older than
// v1.4, which never announces, never gets one. The first one then carries
// the connection's ids and the registered harness, and takes the next frame
// id.
func TestObservationWaitsForTheAnnouncement(t *testing.T) {
	t.Parallel()
	cut := observationCut(1, "$ make", "ok")
	cut.AgentID, cut.SessionID, cut.HarnessVersion = otherAgent, "sess-other", "9.9.9"
	source := newFakeObservations(cut)
	ts := listenObserved(t, source)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	requireNoObservationYet(t, ts, k, source, 0)

	k.announce(takes)
	f := k.read()
	require.Equal(t, int64(4), f.FrameID, "greeting 1, report 2, heartbeat 3, observation 4")
	require.Equal(t, sent(cut), observationOf(t, f))
}

// A change signalled while the gap after an observation is open is cut once
// the gap ends, not before; a quiet session is cut again every second.
func TestObservationOnChangeAfterTheGapAndEverySecond(t *testing.T) {
	t.Parallel()
	source := newFakeObservations(observationCut(1))
	ts := listenObserved(t, source)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takes)
	require.Equal(t, sent(observationCut(1)), observationOf(t, k.read()))

	source.queue(observationCut(2))
	source.signal()
	ts.guard.set(protocol.DraftStateOpen, 1, 1)
	require.Equal(t, int64(1), reportOf(t, k.read(), 0).InputGuardVersion)
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op, "the change waits for the gap")
	ts.clock.fire(t, ObservationGap)
	require.Equal(t, sent(observationCut(2)), observationOf(t, k.read()))

	ts.clock.fire(t, ObservationGap)
	source.queue(observationCut(3))
	ts.clock.fire(t, ObservationEvery)
	require.Equal(t, sent(observationCut(3)), observationOf(t, k.read()), "the refresh of a quiet session")
	require.Equal(t, 3, source.observed())
}

// Support is not inherited: a new connection gets no observation until its
// own Core announces it. It then first gets the last observation written,
// unchanged: not an older one and not a new cut. A change signalled
// meanwhile is cut after.
func TestReconnectReplaysOnlyTheLastObservation(t *testing.T) {
	t.Parallel()
	source := newFakeObservations(observationCut(1))
	ts := listenObserved(t, source)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takes)
	observationOf(t, k.read())
	ts.clock.fire(t, ObservationGap)
	source.queue(observationCut(2))
	source.signal()
	last := observationOf(t, k.read())
	require.Equal(t, sent(observationCut(2)), last)
	require.NoError(t, k.c.Close())

	replay := redial(t, ts.srv.Path())
	source.queue(observationCut(3))
	source.signal()
	replay.send(protocol.AgentOpRegisterAgentResult, greetingFrameID,
		protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, replay.read(), 0)
	requireNoObservationYet(t, ts, replay, source, 2)
	replay.announce(takes)
	require.Equal(t, last, observationOf(t, replay.read()), "the same id and generations")
	require.Equal(t, 2, source.observed(), "a replay is not a new cut")
	// The writer arms the replay's gap only once the replay is written, and
	// the closed connection's gap is still pending on the clock: answered by
	// the same writer, a resync proves the replay's gap is armed, so the fire
	// below reaches it and not only the old one.
	replay.synced()
	ts.clock.fire(t, ObservationGap)
	require.Equal(t, sent(observationCut(3)), observationOf(t, replay.read()))
}

// A cut refused unwritten is not the last observation: a reconnecting Core
// gets the last one written again, not the one that could not go.
func TestReplayIsTheLastObservationWritten(t *testing.T) {
	t.Parallel()
	source := newFakeObservations(observationCut(1))
	ts := listenObserved(t, source)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takes)
	written := observationOf(t, k.read())
	ts.clock.fire(t, ObservationGap)
	source.queue(observationCut(2, strings.Repeat("<", 400<<10)))
	source.signal()
	require.Eventually(t, func() bool { return source.observed() == 2 }, 10*time.Second, time.Millisecond)
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op, "the writer is past the refused cut")
	require.NoError(t, k.c.Close())

	replay := redial(t, ts.srv.Path())
	replay.send(protocol.AgentOpRegisterAgentResult, greetingFrameID,
		protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, replay.read(), 0)
	replay.announce(takes)
	require.Equal(t, written, observationOf(t, replay.read()), "the last written, not the refused one")
}

// The last observation of one session is not replayed to a Core that
// registers the connection under another.
func TestNoReplayIntoAnotherSession(t *testing.T) {
	t.Parallel()
	source := newFakeObservations(observationCut(1))
	ts := listenTest(t, func(opts *Options) {
		opts.Observations = source
		opts.SessionID = ""
	})
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takes)
	observationOf(t, k.read())
	require.NoError(t, k.c.Close())

	const otherSession protocol.OpaqueID = "sess-2"
	source.queue(observationCut(1))
	next := redial(t, ts.srv.Path())
	next.send(protocol.AgentOpRegisterAgentResult, greetingFrameID,
		protocol.RegisterAgentResult{AgentID: testAgent, SessionID: otherSession})
	r := reportOf2(t, next.read(), otherSession)
	require.Equal(t, otherSession, r.SessionID)
	next.announce(takes)
	got := observationOf(t, next.read())
	want := sent(observationCut(1))
	want.SessionID = otherSession
	require.Equal(t, want, got, "a new cut of the new session")
	require.Equal(t, 2, source.observed())
}

// reportOf2 decodes a guard report of session.
func reportOf2(t *testing.T, f protocol.AgentFrame, session protocol.OpaqueID) protocol.InputGuardReport {
	t.Helper()
	require.Equal(t, protocol.AgentOpReportInputGuard, f.Op)
	var r protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(f.Body, &r))
	require.Equal(t, session, r.SessionID)
	return r
}

// A cut that does not validate, does not fit a frame or goes back from the
// last observation is not written, and nothing is cut to fit. The
// connection stays, and the next frame takes the next id.
func TestObservationThatCannotGoIsNotWritten(t *testing.T) {
	t.Parallel()
	withheldRows := observationCut(2)
	withheldRows.ScreenRows = []string{"row"}
	// Valid, but each '<' is six bytes of JSON: no frame holds it.
	tooLarge := observationCut(2, strings.Repeat("<", 400<<10))
	for name, cut := range map[string]protocol.StateObservationReport{
		"not valid": withheldRows,
		"too large": tooLarge,
		"same id": func() protocol.StateObservationReport {
			r := observationCut(2)
			r.ObservationID = "observation-1"
			return r
		}(),
		"same generation":    func() protocol.StateObservationReport { r := observationCut(2); r.ObservationGeneration = 1; return r }(),
		"output goes back":   func() protocol.StateObservationReport { r := observationCut(2); r.OutputGeneration = 0; return r }(),
		"events go back":     func() protocol.StateObservationReport { r := observationCut(2); r.EventGeneration = 0; return r }(),
		"guard goes back":    func() protocol.StateObservationReport { r := observationCut(2); r.InputGuardVersion = 0; return r }(),
		"earlier than last":  func() protocol.StateObservationReport { r := observationCut(2); r.ObservedAt = sentAt; return r }(),
		"nothing to observe": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := newFakeObservations(observationCut(1))
			ts := listenObserved(t, source)
			k := dialCore(t, ts.srv.Path())
			k.greet()
			k.announce(takes)
			first := k.read()
			observationOf(t, first)
			ts.clock.fire(t, ObservationGap)
			if name != "nothing to observe" {
				source.queue(cut)
			}
			source.signal()
			require.Eventually(t, func() bool { return source.observed() == 2 }, 10*time.Second, time.Millisecond)

			ts.clock.fire(t, HeartbeatEvery)
			f := k.read()
			require.Equal(t, protocol.AgentOpHeartbeat, f.Op, "nothing was written, and the connection stays")
			require.Equal(t, first.FrameID+1, f.FrameID, "no frame id was spent")
		})
	}
}

// A Core that stops reading never blocks the source: its changes return at
// once and coalesce, so far fewer cuts are made than changes signalled.
func TestJammedCoreCoalescesObservations(t *testing.T) {
	t.Parallel()
	source := newFakeObservations(observationCut(1))
	ts := listenObserved(t, source)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takes)
	observationOf(t, k.read())

	const changes = 20000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range changes {
			source.signal()
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the source waited on the socket")
	}
	source.queue(observationCut(2))
	ts.clock.fire(t, ObservationGap)
	require.Equal(t, sent(observationCut(2)), observationOf(t, k.read()))
	require.Equal(t, 2, source.observed(), "every change before the gap ended is one cut")
}

// Without a source, a registered connection writes no observation, even
// once its Core announces it takes them.
func TestNoObservationWithoutASource(t *testing.T) {
	t.Parallel()
	ts := listenTest(t, nil)
	k := dialCore(t, ts.srv.Path())
	k.greet()
	k.announce(takes)
	k.synced()
	ts.clock.fire(t, HeartbeatEvery)
	require.Equal(t, protocol.AgentOpHeartbeat, k.read().Op)
}

// The first valid announcement fixes what the connection takes. A later
// one, even one that says more, changes nothing: it is never answered, so
// the Core cannot take it for a refusal, it neither restarts nor replays
// the observations, and it is counted.
func TestALaterAnnouncementIsOnlyCounted(t *testing.T) {
	t.Parallel()
	for name, delivery := range map[string]bool{"reports only": false, "with deliveries": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := newFakeObservations(observationCut(1))
			ts := listenTest(t, func(opts *Options) {
				opts.Observations = source
				if delivery {
					opts.Delivery = newFakeDelivery()
				}
			})
			k := dialCore(t, ts.srv.Path())
			k.greet()
			k.announce(takes)
			observationOf(t, k.read())

			k.announce(takes)
			k.announce(protocol.AgentSocketSupport{ReportStateObservation: true, DurableEvents: true})
			k.synced()
			require.Equal(t, int64(2), ts.srv.DuplicateAnnouncements())
			requireNoObservationYet(t, ts, k, source, 1)
		})
	}
}

// An announcement that takes no observations is the connection's first all
// the same: nothing is cut, and a later one that takes them is ignored and
// counted, never turned into support.
func TestAnAnnouncementWithoutObservationsIsKept(t *testing.T) {
	t.Parallel()
	for name, first := range map[string]protocol.AgentSocketSupport{
		"nothing":        {},
		"durable events": {DurableEvents: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := newFakeObservations(observationCut(1))
			ts := listenObserved(t, source)
			k := dialCore(t, ts.srv.Path())
			k.greet()
			k.announce(first)
			k.synced()
			requireNoObservationYet(t, ts, k, source, 0)

			k.announce(takes)
			k.synced()
			require.Equal(t, int64(1), ts.srv.DuplicateAnnouncements())
			requireNoObservationYet(t, ts, k, source, 0)
		})
	}
}

// An announcement before the greeting is refused like any order and does
// not count: the connection takes observations only from one after it.
func TestAnAnnouncementBeforeTheGreetingIsRefused(t *testing.T) {
	t.Parallel()
	source := newFakeObservations(observationCut(1))
	ts := listenObserved(t, source)
	k := dialCore(t, ts.srv.Path())
	require.Equal(t, protocol.AgentOpRegisterAgent, k.read().Op)
	id := k.announce(takes)
	require.Equal(t, protocol.FrameErrorCodeNotRegistered, errorOf(t, k.read(), id))
	k.send(protocol.AgentOpRegisterAgentResult, greetingFrameID,
		protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, k.read(), 0)
	requireNoObservationYet(t, ts, k, source, 0)

	k.announce(takes)
	require.Equal(t, sent(observationCut(1)), observationOf(t, k.read()))
	require.Zero(t, ts.srv.DuplicateAnnouncements())
}

// An announcement that does not validate gets a frame error answering it,
// which the Core reads as a Proxy that takes nothing. It does not count: the
// connection stays, and the first valid one after it fixes the support.
func TestAnInvalidAnnouncementIsRefused(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"not a bool":    `{"report_state_observation":"yes","durable_events":false}`,
		"unknown field": `{"report_state_observation":true,"durable_events":false,"screen":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := newFakeObservations(observationCut(1))
			ts := listenObserved(t, source)
			k := dialCore(t, ts.srv.Path())
			k.greet()
			id := k.next
			k.next++
			k.sendRaw(fmt.Sprintf(
				`{"op":"announce_support","frame_id":%d,"sent_at":"2026-10-29T12:00:00Z","body":%s}`, id, body))
			require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id))
			requireNoObservationYet(t, ts, k, source, 0)

			k.announce(takes)
			require.Equal(t, sent(observationCut(1)), observationOf(t, k.read()))
			require.Zero(t, ts.srv.DuplicateAnnouncements())
		})
	}
}
