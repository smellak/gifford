//go:build unix

package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// writeRawHarnessScript drops a fake harness that keeps every byte it
// reads as it reads it: with no line discipline, a delivered line shows
// up before its submit does. A cancel key ends it.
func writeRawHarnessScript(t *testing.T) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "rawharness")
	body := "#!/bin/sh\nOUT=\"$1\"\nstty -icanon -echo min 1 time 0\necho deliver-ready\nexec cat >> \"$OUT\"\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	return script
}

// rawSession runs a delivery session over the raw harness, with its
// agent socket and a registered Core, and the submit delay given.
func rawSession(t *testing.T, id string, submitDelay time.Duration) (*deliveryRun, *fakeCore) {
	t.Helper()
	dr, k, _ := startSocketDelivery(t, id, newWindowClock(false), func(o *Options) {
		o.Command[0] = writeRawHarnessScript(t)
		o.SubmitDelay = submitDelay
	})
	return dr, k
}

// endRawSession ends the raw harness with a cancel key and waits for the
// run.
func endRawSession(t *testing.T, dr *deliveryRun) {
	t.Helper()
	writeKeys(t, dr.master, "\x03")
	waitDone(t, dr.done)
}

// waitHarness waits until the harness has read exactly want.
func waitHarness(t *testing.T, dr *deliveryRun, want, what string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == want
	}, 10*time.Second, time.Millisecond, what)
}

// TestCancelBeforeTheCommit cancels a prepared delivery before its
// commit: CANCELLED, no lease taken and no human key held, and no byte
// of the notice on the middle PTY. The human's line, typed around the
// cancel, arrives whole.
func TestCancelBeforeTheCommit(t *testing.T) {
	t.Parallel()
	dr, k := rawSession(t, "cancel-first", 0)

	const id protocol.OpaqueID = "dlv-first"
	const canary = "canary-q8w2"
	notice := protocol.NoticeLine("[GIFFORD] " + canary + " pending")
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare(id, notice, 0, 0)).Result)
	// The human starts a line: with no lease, its keys go through at
	// once.
	writeKeys(t, dr.master, "human-")
	waitHarness(t, dr, "human-", "the human's keys were held")

	r := k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: id, Reason: protocol.CancelReasonHumanPriority})
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonHumanPriority, r.CancelReason)
	require.Empty(t, r.WriterLeaseID)
	dr.session.mu.Lock()
	lease, committing := dr.session.lease, dr.session.committing
	_, held := dr.session.prepared[id]
	dr.session.mu.Unlock()
	require.Nil(t, lease, "no lease was taken")
	require.False(t, committing)
	require.False(t, held, "the session dropped the prepare")
	require.Equal(t, protocol.DeliveryResultKindFailed,
		k.order(protocol.AgentOpCommitDelivery, sockCommit(id)).Result, "a commit after the cancel writes nothing")

	writeKeys(t, dr.master, "line\r")
	waitHarness(t, dr, "human-line\n", "the human's line never arrived whole")
	endRawSession(t, dr)
	out := harnessOut(t, dr.out)
	require.Equal(t, "human-line\n", out)
	require.NotContains(t, out, canary, "no byte of the notice")
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted), "no commit ever started")
}

// TestCancelUnderTheLease cancels while the commit holds the lease. The
// submit waits a real 3 s delay, and the cancel goes out once the whole
// line is on the harness and its submit is not. The cancel is FAILED, and
// the session refuses it too; the commit is COMMITTED, and the line is
// completed once, never left half.
func TestCancelUnderTheLease(t *testing.T) {
	t.Parallel()
	dr, k := rawSession(t, "cancel-lease", 3*time.Second)

	const id protocol.OpaqueID = "dlv-lease"
	const canary = "canary-z5r1"
	notice := protocol.NoticeLine("[GIFFORD] " + canary + " pending")
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare(id, notice, 0, 0)).Result)
	commitID := k.send(protocol.AgentOpCommitDelivery, 0, sockCommit(id))

	// The milestone: the whole line is on the harness, its submit is not.
	waitHarness(t, dr, string(notice), "the line never reached the harness")
	dr.session.mu.Lock()
	leased := dr.session.lease != nil && dr.session.lease.id == id
	dr.session.mu.Unlock()
	require.True(t, leased, "the commit holds the lease")

	r := k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: id, Reason: protocol.CancelReasonManual})
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	require.Equal(t, "delivery commit in progress", r.Error)
	_, err := dr.session.CancelDelivery(id, string(protocol.CancelReasonManual))
	require.ErrorIs(t, err, ErrDeliveryActive, "the session refuses it under the lease")
	require.Equal(t, string(notice), harnessOut(t, dr.out), "the submit has still not gone out")

	f := k.answer(commitID)
	require.Equal(t, protocol.AgentOpReportDeliveryResult, f.Op)
	var res protocol.DeliveryResult
	require.NoError(t, json.Unmarshal(f.Body, &res))
	require.Equal(t, protocol.DeliveryResultKindCommitted, res.Result)
	require.Equal(t, "lease-"+id, res.WriterLeaseID)
	waitHarness(t, dr, string(notice)+"\n", "the submit never completed the line")

	endRawSession(t, dr)
	out := harnessOut(t, dr.out)
	require.Equal(t, string(notice)+"\n", out, "the line, completed once")
	require.Equal(t, 1, strings.Count(out, canary))
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled), "nothing was cancelled")
}

// cancelRun is what one cancel leaves: its result, the session's
// delivery events and the bytes the harness read.
type cancelRun struct {
	result protocol.DeliveryResult
	events []any
	bytes  string
}

// cancelWith prepares a delivery, cancels it with reason while the human
// types around it, and keeps what the cancel left.
func cancelWith(t *testing.T, reason protocol.CancelReason) cancelRun {
	t.Helper()
	dr, k := rawSession(t, "cancel-reason", 0)

	const id protocol.OpaqueID = "dlv-reason"
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare(id, "[GIFFORD] canary-m4t6 pending", 0, 0)).Result)
	writeKeys(t, dr.master, "before-")
	waitHarness(t, dr, "before-", "the human's keys were held")
	run := cancelRun{result: k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: id, Reason: reason})}
	writeKeys(t, dr.master, "after\r")
	waitHarness(t, dr, "before-after\n", "the human's line never arrived whole")
	endRawSession(t, dr)

	run.bytes = harnessOut(t, dr.out)
	for _, e := range dr.sink.all() {
		if strings.HasPrefix(string(e.Type), "input.delivery_") {
			run.events = append(run.events, e.Payload)
		}
	}
	return run
}

// withoutReason is a run with its reason taken out of the result and of
// the cancelled event, once it is checked to be reason as it came.
func withoutReason(t *testing.T, run cancelRun, reason protocol.CancelReason) cancelRun {
	t.Helper()
	require.Equal(t, protocol.DeliveryResultKindCancelled, run.result.Result)
	require.Equal(t, reason, run.result.CancelReason)
	run.result.CancelReason = ""
	events := make([]any, 0, len(run.events))
	cancelled := 0
	for _, e := range run.events {
		if c, ok := e.(DeliveryCancelled); ok {
			require.Equal(t, string(reason), c.Reason, "the session records the reason as it came")
			c.Reason = ""
			e = c
			cancelled++
		}
		events = append(events, e)
	}
	require.Equal(t, 1, cancelled)
	run.events = events
	return run
}

// TestCancelReasonsAreOpaque cancels the same delivery with each reason:
// the bytes on the middle PTY are those of a HUMAN_PRIORITY cancel, and
// so are the result and the session's events, but for the reason
// itself.
func TestCancelReasonsAreOpaque(t *testing.T) {
	t.Parallel()
	want := withoutReason(t, cancelWith(t, protocol.CancelReasonHumanPriority), protocol.CancelReasonHumanPriority)
	require.Equal(t, "before-after\n", want.bytes)
	for _, reason := range []protocol.CancelReason{
		protocol.CancelReasonControlChanged, protocol.CancelReasonStateChanged, protocol.CancelReasonManual,
	} {
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()
			got := withoutReason(t, cancelWith(t, reason), reason)
			require.Equal(t, want.bytes, got.bytes, "the same bytes on the PTY")
			require.Equal(t, want.result, got.result, "the same result")
			require.Equal(t, want.events, got.events, "the same events")
		})
	}
}
