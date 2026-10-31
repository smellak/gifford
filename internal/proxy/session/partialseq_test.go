//go:build unix

package session

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// fragment is a human key sequence cut between two writes: first leaves
// the parser holding bytes back, rest completes the sequence.
type fragment struct {
	name, first, rest string
}

// fragments are the sequences a terminal can cut: a lone escape, an
// escape sequence, a paste start, an Alt key and a rune.
var fragments = []fragment{
	{name: "escape then a key", first: "\x1b", rest: "x"},
	{name: "csi cut", first: "\x1b[1;5", rest: "A"},
	{name: "ss3 cut", first: "\x1bO", rest: "A"},
	{name: "paste start cut", first: "\x1b[20", rest: "0~pasted\x1b[201~"},
	{name: "alt rune cut", first: "\x1b\xc3", rest: "\xa9"},
	{name: "rune cut", first: "\xc3", rest: "\xa9"},
}

// partialNotice is the line the partial sequence tests deliver.
const partialNotice = "[GIFFORD] canary-p3x7 pending"

// partialSession runs a delivery session over the raw harness on a step
// clock, with its agent socket and a registered Core.
func partialSession(t *testing.T, id string) (*deliveryRun, *fakeCore, *stepClock) {
	t.Helper()
	clk := &stepClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	dr, k, _ := startSocketDelivery(t, id, newWindowClock(false), func(o *Options) {
		o.Command[0] = writeRawHarnessScript(t)
		o.Clock = clk
	})
	return dr, k, clk
}

// waitSeen waits until the loop went through every human chunk the
// terminal gave: what the session decides on is the whole of it.
func waitSeen(t *testing.T, s *Session) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		written := s.humanWritten.Load()
		return s.arrived == written && s.snap.Chunks == written
	}, 10*time.Second, time.Millisecond, "the loop never caught up with the human")
}

// abortReasons lists the reasons of the aborted deliveries, in order.
func abortReasons(dr *deliveryRun) []string {
	payloads := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryAbortedHumanPriority)
	out := make([]string, 0, len(payloads))
	for _, p := range payloads {
		out = append(out, p.(DeliveryAborted).Reason)
	}
	return out
}

// prepareNow prepares id against the guard as it reads now.
func prepareNow(t *testing.T, s *Session, id protocol.OpaqueID) DeliveryOutcome {
	t.Helper()
	rep, err := s.PrepareDelivery(id, s.Generation(), s.GuardVersion())
	require.NoError(t, err)
	return rep.Outcome
}

// TestPartialSequenceHoldsTheDelivery cuts a human key sequence between
// two writes and tries to deliver in between. The first part already
// reached the harness, which may answer it before the key is whole. The
// session and socket refuse while the parser holds the first part back,
// but transport uncertainty alone moves no guard state, version, or human
// timestamp. Once the rest arrives, its human event moves the guard. The
// harness gets the human's bytes as typed, and no notice.
func TestPartialSequenceHoldsTheDelivery(t *testing.T) {
	t.Parallel()
	for _, f := range fragments {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			dr, k, clk := partialSession(t, "partial-hold")
			before := publishedSnapshot(t, dr.session)
			writeKeys(t, dr.master, f.first)
			waitHarness(t, dr, f.first, "the first part never reached the harness")
			waitSeen(t, dr.session)
			held := publishedSnapshot(t, dr.session)
			require.True(t, held.Pending, "the parser holds the first part back")
			require.Equal(t, before.State, held.State)
			require.Equal(t, before.Generation, held.Generation)
			require.Equal(t, before.Version, held.Version, "unclassified transport is not human evidence")
			require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftUnknown))

			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-held"))
			r := k.order(protocol.AgentOpPrepareDelivery,
				sockPrepare("dlv-sock", partialNotice, int64(held.Generation), held.Version))
			require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result, "the node is refused too")

			// Neither the session's clock nor the loop's tickers end a
			// hold: three key flush periods pass in real time.
			clk.advance(time.Hour)
			time.Sleep(3 * defaultKeyFlushEvery)
			require.True(t, publishedSnapshot(t, dr.session).Pending)
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-paused"))
			require.Equal(t, []string{reasonIncomplete, reasonIncomplete, reasonIncomplete}, abortReasons(dr))

			writeKeys(t, dr.master, f.rest)
			waitHarness(t, dr, f.first+f.rest, "the rest never reached the harness")
			waitSeen(t, dr.session)
			require.False(t, publishedSnapshot(t, dr.session).Pending, "the whole sequence holds nothing back")
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-open"))
			require.Equal(t, "snapshot mismatch", abortReasons(dr)[3], "the guard's refusal, not the parser's")
			endRawSession(t, dr)
			require.Equal(t, f.first+f.rest, harnessOut(t, dr.out), "the human's bytes as typed, and no notice")
		})
	}
}

// TestPartialSequenceBetweenPrepareAndCommit prepares before the human
// starts a cut sequence: the commit that follows the first part aborts
// and writes nothing, and once the sequence is whole a new delivery
// waits for the guard the hold moved.
func TestPartialSequenceBetweenPrepareAndCommit(t *testing.T) {
	t.Parallel()
	for _, f := range fragments {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()
			dr, _, _ := partialSession(t, "partial-between")
			require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, dr.session, "dlv-early"))
			writeKeys(t, dr.master, f.first)
			waitHarness(t, dr, f.first, "the first part never reached the harness")
			waitSeen(t, dr.session)
			rep, err := dr.session.CommitDelivery("dlv-early", partialNotice, protocol.InjectModeTyped)
			require.NoError(t, err)
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
			require.Equal(t, f.first, harnessOut(t, dr.out), "none of the notice")

			writeKeys(t, dr.master, f.rest)
			waitHarness(t, dr, f.first+f.rest, "the rest never reached the harness")
			waitSeen(t, dr.session)
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-late"))
			reasons := abortReasons(dr)
			require.Equal(t, "snapshot mismatch", reasons[len(reasons)-1], "the guard's refusal, not the parser's")
			endRawSession(t, dr)
			require.Equal(t, f.first+f.rest, harnessOut(t, dr.out))
		})
	}
}

// TestPartialSequenceReportsTheHold holds and releases a lone ESC. The
// transport-only hold blocks delivery without moving the reported guard.
// Once the follower proves a human Alt key, that event moves the guard and
// is reported normally.
func TestPartialSequenceReportsTheHold(t *testing.T) {
	t.Parallel()
	dr, k, _ := partialSession(t, "partial-feed")
	waitSeen(t, dr.session)
	before := publishedSnapshot(t, dr.session)

	press(t, dr, "\x1b")
	held := publishedSnapshot(t, dr.session)
	require.True(t, held.Pending)
	require.Equal(t, before.Version, held.Version)

	press(t, dr, "x")
	moved := publishedSnapshot(t, dr.session)
	require.Greater(t, moved.Version, held.Version, "the resolved human key moves the guard")
	rep := k.report()
	require.Equal(t, protocol.DraftStateUnknown, rep.DraftState)
	require.Equal(t, moved.Version, rep.InputGuardVersion)
	r := k.order(protocol.AgentOpPrepareDelivery,
		sockPrepare("dlv-sock", partialNotice, int64(before.Generation), before.Version))
	require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result)
	require.Equal(t, 2, strings.Count(k.raw.String(), `"op":"`+string(protocol.AgentOpReportInputGuard)+`"`),
		"the greeting's report and the hold's")
	endRawSession(t, dr)
}

// TestPartialSequenceSignalsTheHold holds and releases a lone ESC with no
// socket to take the change signal first. Transport uncertainty signals no
// guard change; the resolved human Alt key does.
func TestPartialSequenceSignalsTheHold(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "harness.out")
	master, slave := testPTY(t)
	opts := testOptions(t.TempDir(), "partial-signal", slave, slave, &chanSink{}, []string{writeRawHarnessScript(t), out})
	s, err := NewSession(opts)
	require.NoError(t, err)
	dr := &deliveryRun{session: s, master: master, out: out, done: runAsync(t, s)}
	waitRaw(t, slave)
	readUntil(t, master, "deliver-ready")
	waitSeen(t, s)
	select {
	case <-s.GuardChanged():
	default:
	}
	press(t, dr, "\x1b")
	select {
	case <-s.GuardChanged():
		t.Fatal("transport uncertainty signalled a guard change")
	default:
	}
	press(t, dr, "x")
	select {
	case <-s.GuardChanged():
	default:
		t.Fatal("the resolved human key signalled no guard change")
	}
	endRawSession(t, dr)
}

// TestRevalidationRefusesAHold puts a prepare in place while the parser
// holds part of a sequence, as no prepare can since it checks: the
// recheck before the lease refuses it all the same, and writes nothing.
func TestRevalidationRefusesAHold(t *testing.T) {
	t.Parallel()
	dr, _, _ := partialSession(t, "partial-recheck")
	writeKeys(t, dr.master, "\x1b[")
	waitHarness(t, dr, "\x1b[", "the first part never reached the harness")
	waitSeen(t, dr.session)
	s := dr.session
	s.mu.Lock()
	require.True(t, s.snap.Pending)
	s.prepared["dlv-forced"] = pendingDelivery{
		generation: s.snap.Generation, version: s.snap.Version,
		written: s.humanWritten.Load(), arrived: s.arrived,
	}
	s.mu.Unlock()
	rep, err := s.CommitDelivery("dlv-forced", partialNotice, protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	require.Equal(t, []string{reasonIncomplete}, abortReasons(dr))
	writeKeys(t, dr.master, "A")
	waitHarness(t, dr, "\x1b[A", "the rest never reached the harness")
	endRawSession(t, dr)
	require.Equal(t, "\x1b[A", harnessOut(t, dr.out), "none of the notice")
}
