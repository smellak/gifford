//go:build unix

package agentsock

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// errBadLine is the fake session's refusal of a line.
var errBadLine = errors.New("bad line")

// fakeDelivery is a session side the test drives. It records every call
// that would reach the session, answers a prepare with the generation
// and version it was asked for, and holds commits while hold is set.
type fakeDelivery struct {
	mu    sync.Mutex
	calls []string
	// prepared keeps the generation and version of each prepare, which
	// a commit or a cancel answers with, as the session does.
	prepared map[protocol.OpaqueID][2]int64
	// abort makes Prepare answer ABORTED_HUMAN_PRIORITY; commitResult
	// is what Commit answers, COMMITTED when empty.
	abort        bool
	commitResult protocol.DeliveryResultKind
	// holds is what InputHolds answers; paste makes the profile's mode
	// bracketed_paste.
	holds bool
	paste bool
	// hold, when set, keeps Commit running until it closes.
	hold chan struct{}
}

func newFakeDelivery() *fakeDelivery {
	return &fakeDelivery{prepared: make(map[protocol.OpaqueID][2]int64)}
}

func (d *fakeDelivery) recordf(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, fmt.Sprintf(format, args...))
}

// called is every call so far, in order.
func (d *fakeDelivery) called() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.calls)
}

// waitCall waits until call has been made.
func (d *fakeDelivery) waitCall(t *testing.T, call string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return slices.Contains(d.called(), call)
	}, 10*time.Second, time.Millisecond, "never called: %s", call)
}

// holdCommits keeps every commit running until release is called or
// the test ends, so a failing test never leaves a commit held.
func (d *fakeDelivery) holdCommits(t *testing.T) (release func()) {
	t.Helper()
	hold := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(hold) }) }
	t.Cleanup(release)
	d.set(func(d *fakeDelivery) { d.hold = hold })
	return release
}

// set changes the fake under its lock.
func (d *fakeDelivery) set(edit func(*fakeDelivery)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	edit(d)
}

// CheckLine refuses a line holding "bad". It reaches nothing, so it
// records nothing.
func (d *fakeDelivery) CheckLine(line string) error {
	if strings.Contains(line, "bad") {
		return errBadLine
	}
	return nil
}

func (d *fakeDelivery) Prepare(id protocol.OpaqueID, generation uint64, version int64) (DeliveryReport, error) {
	d.recordf("prepare %s %d %d", id, generation, version)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.abort {
		return DeliveryReport{Result: protocol.DeliveryResultKindAbortedHumanPriority, Generation: generation + 1, Version: version + 1}, nil
	}
	d.prepared[id] = [2]int64{int64(generation), version}
	return DeliveryReport{Result: protocol.DeliveryResultKindPrepared, Generation: generation, Version: version}, nil
}

func (d *fakeDelivery) Commit(id protocol.OpaqueID, line string, mode protocol.InjectMode) (DeliveryReport, error) {
	d.recordf("commit %s %s %s", id, line, mode)
	d.mu.Lock()
	hold, kind, at := d.hold, d.commitResult, d.prepared[id]
	d.mu.Unlock()
	if hold != nil {
		<-hold
	}
	if kind == "" {
		kind = protocol.DeliveryResultKindCommitted
	}
	return DeliveryReport{Result: kind, Generation: uint64(at[0]), Version: at[1]}, nil
}

func (d *fakeDelivery) Cancel(id protocol.OpaqueID, reason protocol.CancelReason) (DeliveryReport, error) {
	d.recordf("cancel %s %s", id, reason)
	d.mu.Lock()
	defer d.mu.Unlock()
	at := d.prepared[id]
	return DeliveryReport{Result: protocol.DeliveryResultKindCancelled, Generation: uint64(at[0]), Version: at[1]}, nil
}

func (d *fakeDelivery) DefaultMode() protocol.InjectMode {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.paste {
		return protocol.InjectModeBracketedPaste
	}
	return protocol.InjectModeTyped
}

func (d *fakeDelivery) InputHolds(line string) bool {
	d.recordf("holds %s", line)
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.holds
}

// armed waits until a timer of d is armed, without firing it.
func (c *fakeClock) armed(t *testing.T, d time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, tm := range c.timers {
			if tm.d == d {
				return true
			}
		}
		return false
	}, 10*time.Second, time.Millisecond, "no %s timer armed", d)
}

// testNotice is the notice line of the test deliveries.
const testNotice protocol.NoticeLine = "[GIFFORD] 1 pending message"

// prepareOrder is the prepare of delivery id at generation 2, version 5.
func prepareOrder(id protocol.OpaqueID) protocol.PrepareDelivery {
	return protocol.PrepareDelivery{
		DeliveryID: id, AgentID: testAgent,
		MessageRef: protocol.MessageRef{
			MessageID: "01JB0000000000000000000011", FromAgent: otherAgent,
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine:             testNotice,
		OrchestratorDecisionID: "01JB0000000000000000000002",
		IdempotencyKey:         "idem-1",
		ExpectedStateVersion:   9, ExpectedInputGuardVersion: 5, ExpectedDraftGeneration: 2,
		ControlVersions: protocol.ControlVersions{"global": 18, "agent": 3},
	}
}

// commitOrder is the commit of delivery id under the Core's lease.
func commitOrder(id protocol.OpaqueID) protocol.CommitDelivery {
	return protocol.CommitDelivery{DeliveryID: id, WriterLeaseID: "lease-" + id}
}

// prepareCall and commitCall are the calls a test order makes.
func prepareCall(id protocol.OpaqueID) string { return fmt.Sprintf("prepare %s 2 5", id) }

func commitCall(id protocol.OpaqueID, mode protocol.InjectMode) string {
	return fmt.Sprintf("commit %s %s %s", id, testNotice, mode)
}

// deliverySocket is a registered Core on a socket with a fake delivery.
func deliverySocket(t *testing.T) (*testSocket, *fakeDelivery, *core) {
	t.Helper()
	d := newFakeDelivery()
	ts := listenTest(t, func(o *Options) { o.Delivery = d })
	k := dialCore(t, ts.srv.Path())
	k.greet()
	return ts, d, k
}

// resultOf decodes a delivery result answering replyTo.
func resultOf(t *testing.T, f protocol.AgentFrame, replyTo int64) protocol.DeliveryResult {
	t.Helper()
	require.Equal(t, protocol.AgentOpReportDeliveryResult, f.Op)
	requireReply(t, f, replyTo)
	var r protocol.DeliveryResult
	require.NoError(t, json.Unmarshal(f.Body, &r))
	require.Equal(t, testAgent, r.AgentID)
	require.True(t, sentAt.Equal(r.ObservedAt))
	require.NotNil(t, r.ObservedInputGuardVersion)
	return r
}

// order sends one order and reads the result answering it.
func (k *core) order(op protocol.AgentOp, body any) protocol.DeliveryResult {
	k.t.Helper()
	id := k.send(op, 0, body)
	return resultOf(k.t, k.read(), id)
}

// prepared prepares the test delivery id and checks it is PREPARED.
func (k *core) prepared(id protocol.OpaqueID) protocol.DeliveryResult {
	k.t.Helper()
	r := k.order(protocol.AgentOpPrepareDelivery, prepareOrder(id))
	require.Equal(k.t, protocol.DeliveryResultKindPrepared, r.Result, "prepare %s", id)
	return r
}

// failedWith checks a FAILED result and its fixed error.
func failedWith(t *testing.T, r protocol.DeliveryResult, msg string) {
	t.Helper()
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	require.Equal(t, msg, r.Error)
}

// TestDeliveryPrepareThenCommit runs a delivery end to end on the
// socket: PREPARED with the generation and version the session ran
// under, then COMMITTED once the consume window has passed, with the
// Core's lease, the mode used and the control versions as they came.
func TestDeliveryPrepareThenCommit(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	p := prepareOrder("dlv-1")
	r := k.order(protocol.AgentOpPrepareDelivery, p)
	require.Equal(t, protocol.DeliveryResultKindPrepared, r.Result)
	require.Equal(t, protocol.OpaqueID("dlv-1"), r.DeliveryID)
	require.Equal(t, int64(2), r.ObservedDraftGeneration)
	require.Equal(t, int64(5), *r.ObservedInputGuardVersion)
	require.Equal(t, p.ControlVersions, r.ControlVersions)
	require.Empty(t, r.WriterLeaseID)

	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-1"))
	d.waitCall(t, commitCall("dlv-1", protocol.InjectModeTyped))
	// The input line is read only once the window has passed.
	ts.clock.armed(t, ConsumeWindow)
	require.Equal(t, []string{prepareCall("dlv-1"), commitCall("dlv-1", protocol.InjectModeTyped)}, d.called())
	ts.clock.fire(t, ConsumeWindow)
	r = resultOf(t, k.read(), id)
	require.Equal(t, protocol.DeliveryResultKindCommitted, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-dlv-1"), r.WriterLeaseID)
	require.Equal(t, protocol.InjectModeTyped, r.InjectMode)
	require.Equal(t, p.ControlVersions, r.ControlVersions)
	require.Equal(t, int64(2), r.ObservedDraftGeneration)
	require.Equal(t, int64(5), *r.ObservedInputGuardVersion)
	require.Equal(t, "holds "+string(testNotice), d.called()[2])
}

// TestDeliveryModeOfThePrepare commits in the prepare's inject mode,
// and in the profile's when the prepare named none.
func TestDeliveryModeOfThePrepare(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	p := prepareOrder("dlv-paste")
	p.InjectMode = protocol.InjectModeBracketedPaste
	require.Equal(t, protocol.DeliveryResultKindPrepared, k.order(protocol.AgentOpPrepareDelivery, p).Result)
	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-paste"))
	ts.clock.fire(t, ConsumeWindow)
	r := resultOf(t, k.read(), id)
	require.Equal(t, protocol.DeliveryResultKindCommitted, r.Result)
	require.Equal(t, protocol.InjectModeBracketedPaste, r.InjectMode)
	d.waitCall(t, commitCall("dlv-paste", protocol.InjectModeBracketedPaste))

	d.set(func(d *fakeDelivery) { d.paste = true })
	k.prepared("dlv-profile")
	id = k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-profile"))
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.InjectModeBracketedPaste, resultOf(t, k.read(), id).InjectMode)
	d.waitCall(t, commitCall("dlv-profile", protocol.InjectModeBracketedPaste))
}

// TestDeliveryNotConsumedNeedsEvidence answers COMMIT_NOT_CONSUMED only
// when the session shows the notice still in the input line. A commit
// the session aborted reads nothing and waits for nothing.
func TestDeliveryNotConsumedNeedsEvidence(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	d.set(func(d *fakeDelivery) { d.holds = true })
	k.prepared("dlv-1")
	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-1"))
	ts.clock.fire(t, ConsumeWindow)
	r := resultOf(t, k.read(), id)
	require.Equal(t, protocol.DeliveryResultKindCommitNotConsumed, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-dlv-1"), r.WriterLeaseID)
	require.Equal(t, protocol.InjectModeTyped, r.InjectMode)

	d.set(func(d *fakeDelivery) { d.commitResult = protocol.DeliveryResultKindAbortedHumanPriority })
	k.prepared("dlv-2")
	r = k.order(protocol.AgentOpCommitDelivery, commitOrder("dlv-2"))
	require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-dlv-2"), r.WriterLeaseID)
	require.Empty(t, r.InjectMode, "nothing was written")
	holds := 0
	for _, call := range d.called() {
		if strings.HasPrefix(call, "holds ") {
			holds++
		}
	}
	require.Equal(t, 1, holds, "the aborted commit read no input line")
}

// TestDeliveryPrepareRefusals answers FAILED, without asking the
// session, for a line the session would refuse and for a prepare past
// the 16 that may wait for their commit.
func TestDeliveryPrepareRefusals(t *testing.T) {
	t.Parallel()
	_, d, k := deliverySocket(t)

	p := prepareOrder("dlv-bad")
	p.NoticeLine = "a bad line"
	r := k.order(protocol.AgentOpPrepareDelivery, p)
	failedWith(t, r, failLine)
	require.Equal(t, p.ControlVersions, r.ControlVersions)
	require.Empty(t, d.called(), "the session was never asked")

	for i := range maxPrepared {
		k.prepared(protocol.OpaqueID(fmt.Sprintf("dlv-%02d", i)))
	}
	calls := len(d.called())
	failedWith(t, k.order(protocol.AgentOpPrepareDelivery, prepareOrder("dlv-17")), failFull)
	require.Len(t, d.called(), calls, "the 17th never reached the session")

	// Once one closes, another fits.
	require.Equal(t, protocol.DeliveryResultKindCancelled,
		k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: "dlv-00", Reason: protocol.CancelReasonManual}).Result)
	k.prepared("dlv-17")
}

// TestDeliveryRepeatedPrepare answers a repeat of a live prepare with
// the same PREPARED, holding no other entry. A prepare with the same id
// and anything else different is FAILED, and the delivery it names
// stays as it was.
func TestDeliveryRepeatedPrepare(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	first := k.prepared("dlv-1")
	again := k.prepared("dlv-1")
	require.Equal(t, first, again, "the same PREPARED")
	require.Equal(t, []string{prepareCall("dlv-1")}, d.called())
	// The repeat held no entry: 15 more still fit.
	for i := range maxPrepared - 1 {
		k.prepared(protocol.OpaqueID(fmt.Sprintf("dlv-%02d", i+2)))
	}
	calls := len(d.called())

	for name, edit := range map[string]func(*protocol.PrepareDelivery){
		"notice line": func(p *protocol.PrepareDelivery) { p.NoticeLine = "[GIFFORD] 2 pending messages" },
		"inject mode": func(p *protocol.PrepareDelivery) { p.InjectMode = protocol.InjectModeTyped },
		"a control version": func(p *protocol.PrepareDelivery) {
			p.ControlVersions["global"] = 19
		},
		"one more control": func(p *protocol.PrepareDelivery) {
			p.ControlVersions["x"] = 1
		},
		"message ref":                  func(p *protocol.PrepareDelivery) { p.MessageRef.ThreadID = "thread-2" },
		"decision":                     func(p *protocol.PrepareDelivery) { p.OrchestratorDecisionID = "01JB0000000000000000000003" },
		"idempotency key":              func(p *protocol.PrepareDelivery) { p.IdempotencyKey = "idem-2" },
		"expected state version":       func(p *protocol.PrepareDelivery) { p.ExpectedStateVersion = 10 },
		"expected input guard version": func(p *protocol.PrepareDelivery) { p.ExpectedInputGuardVersion = 6 },
		"expected draft generation":    func(p *protocol.PrepareDelivery) { p.ExpectedDraftGeneration = 3 },
	} {
		p := prepareOrder("dlv-1")
		edit(&p)
		failedWith(t, k.order(protocol.AgentOpPrepareDelivery, p), failConflict)
		require.Len(t, d.called(), calls, "%s: the session was never asked", name)
	}

	// The first delivery is intact: its line and mode commit.
	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-1"))
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), id).Result)
	d.waitCall(t, commitCall("dlv-1", protocol.InjectModeTyped))
}

// TestDeliveryPrepareAgain prepares a closed id again: one whose commit
// ran is FAILED, since its line may be on the PTY, while one closed
// before any write starts afresh.
func TestDeliveryPrepareAgain(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	k.prepared("dlv-done")
	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-done"))
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), id).Result)
	calls := len(d.called())
	failedWith(t, k.order(protocol.AgentOpPrepareDelivery, prepareOrder("dlv-done")), failClosed)
	require.Len(t, d.called(), calls)

	d.set(func(d *fakeDelivery) { d.abort = true })
	r := k.order(protocol.AgentOpPrepareDelivery, prepareOrder("dlv-aborted"))
	require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result)
	require.Equal(t, int64(3), r.ObservedDraftGeneration, "the session's generation")
	require.Equal(t, int64(6), *r.ObservedInputGuardVersion, "the session's version")
	failedWith(t, leasedResult(t, k, "dlv-aborted"), failClosed)
	d.set(func(d *fakeDelivery) { d.abort = false })
	k.prepared("dlv-aborted")

	k.prepared("dlv-cancelled")
	require.Equal(t, protocol.DeliveryResultKindCancelled,
		k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: "dlv-cancelled", Reason: protocol.CancelReasonControlChanged}).Result)
	k.prepared("dlv-cancelled")
}

// leasedResult commits id and reads an answer that comes at once.
func leasedResult(t *testing.T, k *core, id protocol.OpaqueID) protocol.DeliveryResult {
	t.Helper()
	r := k.order(protocol.AgentOpCommitDelivery, commitOrder(id))
	require.Equal(t, "lease-"+id, r.WriterLeaseID)
	return r
}

// TestDeliveryCancel cancels before the commit: CANCELLED with the
// reason. During the commit or after it there is nothing to cancel, and
// an unknown id is UNKNOWN_DELIVERY.
func TestDeliveryCancel(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	p := prepareOrder("dlv-1")
	k.prepared("dlv-1")
	r := k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: "dlv-1", Reason: protocol.CancelReasonHumanPriority})
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonHumanPriority, r.CancelReason)
	require.Equal(t, p.ControlVersions, r.ControlVersions)
	require.Equal(t, int64(2), r.ObservedDraftGeneration)
	require.Equal(t, "cancel dlv-1 HUMAN_PRIORITY", d.called()[1])

	release := d.holdCommits(t)
	k.prepared("dlv-2")
	commitID := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-2"))
	d.waitCall(t, commitCall("dlv-2", protocol.InjectModeTyped))
	cancel := protocol.CancelDelivery{DeliveryID: "dlv-2", Reason: protocol.CancelReasonManual}
	failedWith(t, k.order(protocol.AgentOpCancelDelivery, cancel), failActive)
	release()
	// Still running in the consume window.
	ts.clock.armed(t, ConsumeWindow)
	failedWith(t, k.order(protocol.AgentOpCancelDelivery, cancel), failActive)
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), commitID).Result)
	failedWith(t, k.order(protocol.AgentOpCancelDelivery, cancel), failClosed)
	for _, call := range d.called() {
		require.NotEqual(t, "cancel dlv-2 MANUAL", call, "a commit is never cancelled")
	}

	id := k.send(protocol.AgentOpCancelDelivery, 0, protocol.CancelDelivery{DeliveryID: "dlv-nope", Reason: protocol.CancelReasonManual})
	require.Equal(t, protocol.FrameErrorCodeUnknownDelivery, errorOf(t, k.read(), id))
}

// TestDeliveryCommitRefusals refuses a commit without a prepare with
// UNKNOWN_DELIVERY, and a second commit while one runs with FAILED.
// Neither reaches the session.
func TestDeliveryCommitRefusals(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-nope"))
	require.Equal(t, protocol.FrameErrorCodeUnknownDelivery, errorOf(t, k.read(), id))
	require.Empty(t, d.called())

	release := d.holdCommits(t)
	k.prepared("dlv-1")
	k.prepared("dlv-2")
	first := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-1"))
	d.waitCall(t, commitCall("dlv-1", protocol.InjectModeTyped))
	failedWith(t, leasedResult(t, k, "dlv-2"), failActive)
	failedWith(t, leasedResult(t, k, "dlv-1"), failActive)
	require.Equal(t, []string{prepareCall("dlv-1"), prepareCall("dlv-2"), commitCall("dlv-1", protocol.InjectModeTyped)}, d.called())

	release()
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), first).Result)
	// The refused commit left its delivery prepared.
	second := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-2"))
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), second).Result)
}

// TestDeliveryPause cancels every prepare and commit that comes while
// automation is paused, with STATE_CHANGED and nothing written, until
// resume. A commit already running goes on to its end.
func TestDeliveryPause(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	k.prepared("dlv-before")
	release := d.holdCommits(t)
	k.prepared("dlv-running")
	running := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-running"))
	d.waitCall(t, commitCall("dlv-running", protocol.InjectModeTyped))

	k.send(protocol.AgentOpPauseAutomation, 0, protocol.AgentTarget{AgentID: testAgent})
	calls := len(d.called())
	r := k.order(protocol.AgentOpPrepareDelivery, prepareOrder("dlv-paused"))
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonStateChanged, r.CancelReason)
	require.Len(t, d.called(), calls, "the paused prepare never reached the session")
	r = leasedResult(t, k, "dlv-before")
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonStateChanged, r.CancelReason)
	require.Equal(t, []string{"cancel dlv-before STATE_CHANGED"}, d.called()[calls:], "cancelled, never committed")

	release()
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), running).Result)

	k.send(protocol.AgentOpResumeAutomation, 0, protocol.AgentTarget{AgentID: testAgent})
	k.prepared("dlv-paused")
	id := k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-paused"))
	ts.clock.fire(t, ConsumeWindow)
	require.Equal(t, protocol.DeliveryResultKindCommitted, resultOf(t, k.read(), id).Result)
	d.waitCall(t, commitCall("dlv-paused", protocol.InjectModeTyped))
}

// TestDeliveryDisconnectCancels drops the Core with deliveries
// prepared: each is cancelled with STATE_CHANGED, a commit running goes
// on, and the next connection starts with no delivery.
func TestDeliveryDisconnectCancels(t *testing.T) {
	t.Parallel()
	ts, d, k := deliverySocket(t)

	k.prepared("dlv-1")
	k.prepared("dlv-2")
	release := d.holdCommits(t)
	k.prepared("dlv-3")
	k.send(protocol.AgentOpCommitDelivery, 0, commitOrder("dlv-3"))
	d.waitCall(t, commitCall("dlv-3", protocol.InjectModeTyped))

	require.NoError(t, k.c.Close())
	d.waitCall(t, "cancel dlv-1 STATE_CHANGED")
	d.waitCall(t, "cancel dlv-2 STATE_CHANGED")
	require.NotContains(t, d.called(), "cancel dlv-3 STATE_CHANGED", "a running commit goes on")
	release()

	// The socket takes the next Core once the commit has ended.
	dialGreetedDelivery(t, ts.srv.Path(), func(k *core) {
		k.prepared("dlv-1")
		cancel := protocol.CancelDelivery{DeliveryID: "dlv-2", Reason: protocol.CancelReasonManual}
		id := k.send(protocol.AgentOpCancelDelivery, 0, cancel)
		require.Equal(t, protocol.FrameErrorCodeUnknownDelivery, errorOf(t, k.read(), id), "the map started empty")
	})
}

// dialGreetedDelivery dials until the socket greets, completes the
// greeting and runs fn on that Core.
func dialGreetedDelivery(t *testing.T, path string, fn func(*core)) {
	t.Helper()
	var k *core
	require.Eventually(t, func() bool {
		c := dialCore(t, path)
		if c.c.SetReadDeadline(time.Now().Add(time.Second)) != nil {
			return false
		}
		f, err := ReadFrame(c.c)
		if err != nil || f.Op != protocol.AgentOpRegisterAgent {
			_ = c.c.Close()
			return false
		}
		k = c
		return true
	}, 10*time.Second, 10*time.Millisecond, "the socket never greeted")
	k.send(protocol.AgentOpRegisterAgentResult, greetingFrameID, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, k.read(), 0)
	fn(k)
}

// TestDeliveryClosedAreBounded remembers the last 16 closed deliveries:
// a cancel for an older one is UNKNOWN_DELIVERY.
func TestDeliveryClosedAreBounded(t *testing.T) {
	t.Parallel()
	_, _, k := deliverySocket(t)

	for i := range maxClosed + 1 {
		id := protocol.OpaqueID(fmt.Sprintf("dlv-%02d", i))
		k.prepared(id)
		require.Equal(t, protocol.DeliveryResultKindCancelled,
			k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: id, Reason: protocol.CancelReasonManual}).Result)
	}
	cancel := protocol.CancelDelivery{DeliveryID: "dlv-00", Reason: protocol.CancelReasonManual}
	id := k.send(protocol.AgentOpCancelDelivery, 0, cancel)
	require.Equal(t, protocol.FrameErrorCodeUnknownDelivery, errorOf(t, k.read(), id))
	cancel.DeliveryID = "dlv-01"
	failedWith(t, k.order(protocol.AgentOpCancelDelivery, cancel), failClosed)
}

// TestDeliveryOrdersFrameErrors refuses delivery orders before the
// greeting, orders for another agent, and the Core ops that are not
// this Proxy's, all without reaching the session.
func TestDeliveryOrdersFrameErrors(t *testing.T) {
	t.Parallel()
	d := newFakeDelivery()
	ts := listenTest(t, func(o *Options) { o.Delivery = d })
	k := dialCore(t, ts.srv.Path())
	hello := k.read()
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	id := k.send(protocol.AgentOpPrepareDelivery, 0, prepareOrder("dlv-1"))
	require.Equal(t, protocol.FrameErrorCodeNotRegistered, errorOf(t, k.read(), id))
	k.send(protocol.AgentOpRegisterAgentResult, greetingFrameID, protocol.RegisterAgentResult{AgentID: testAgent, SessionID: testSession})
	reportOf(t, k.read(), 0)

	other := prepareOrder("dlv-1")
	other.AgentID = otherAgent
	for op, body := range map[protocol.AgentOp]any{
		protocol.AgentOpPrepareDelivery:  other,
		protocol.AgentOpPauseAutomation:  protocol.AgentTarget{AgentID: otherAgent},
		protocol.AgentOpResumeAutomation: protocol.AgentTarget{AgentID: otherAgent},
	} {
		id := k.send(op, 0, body)
		require.Equal(t, protocol.FrameErrorCodeInvalidFrame, errorOf(t, k.read(), id), op)
	}

	ref := json.RawMessage(`{"message_id":"01JB0000000000000000000011","from_agent":"agent-1","priority":"NORMAL","thread_id":"thread-1"}`)
	for op, body := range map[protocol.AgentOp]string{
		protocol.AgentOpNativeDelivery:          `{"delivery_id":"dlv-1","agent_id":"agent-1","message_ref":` + string(ref) + `,"orchestrator_decision_id":"01JB0000000000000000000013","control_versions":{"global":18}}`,
		protocol.AgentOpNotifyMessageAvailable:  `{"message_ref":` + string(ref) + `}`,
		protocol.AgentOpNotifyArtifactAvailable: `{"artifact_ref":{"artifact_id":"art-1","from_agent":"agent-1","purpose":"review","state":"AVAILABLE"}}`,
	} {
		id := k.send(op, 0, json.RawMessage(body))
		require.Equal(t, protocol.FrameErrorCodeUnsupportedOp, errorOf(t, k.read(), id), op)
	}
	require.Empty(t, d.called())
}
