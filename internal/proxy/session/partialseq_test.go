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
	// waits marks a sequence that, once complete, leaves the delivery
	// waiting for the guard: text and a paste open a draft, and a key
	// may have typed, which on this profile, with no input line to show
	// it, leaves the guard UNKNOWN.
	waits bool
}

// pasteMarkerHead is a paste marker cut before its last digit: the
// parser holds it back until its final byte.
const pasteMarkerHead = "\x1b[20"

// fragments are the sequences a terminal can cut: a lone escape, an
// escape sequence, a paste start, an Alt key, a rune and a paste end
// with no paste open, the one that moves no draft.
var fragments = []fragment{
	{name: "escape then a key", first: "\x1b", rest: "x", waits: true},
	{name: "csi cut", first: "\x1b[1;5", rest: "A", waits: true},
	{name: "ss3 cut", first: "\x1bO", rest: "A", waits: true},
	{name: "paste start cut", first: pasteMarkerHead, rest: "0~pasted\x1b[201~", waits: true},
	{name: "alt rune cut", first: "\x1b\xc3", rest: "\xa9", waits: true},
	{name: "rune cut", first: "\xc3", rest: "\xa9", waits: true},
	{name: "paste end cut", first: pasteMarkerHead, rest: "1~"},
}

// lonePasteEnd is a paste end with no paste open, cut in two: once
// whole it moves no draft.
var lonePasteEnd = []string{pasteMarkerHead, "1~"}

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
// two writes and tries to deliver in between: the session and the
// socket refuse while the parser holds the first part back, a pause on
// the session's clock and in real time changes nothing, and once the
// rest arrives the parser holds nothing back: the delivery waits for the
// guard alone, and goes through when the whole sequence moved no draft.
// The harness gets the human's bytes as typed, with no notice between
// them.
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
			require.Equal(t, before.Version, held.Version, "a hold moves no version")

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
			want := f.first + f.rest
			if f.waits {
				// The whole sequence moved the guard: the delivery
				// waits for it, as for any draft, not for the parser.
				require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-open"))
				require.Equal(t, "snapshot mismatch", abortReasons(dr)[3])
			} else {
				require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, dr.session, "dlv-after"))
				rep, err := dr.session.CommitDelivery("dlv-after", partialNotice, protocol.InjectModeTyped)
				require.NoError(t, err)
				require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
				want += partialNotice + "\n"
				waitHarness(t, dr, want, "the notice never followed the whole sequence")
			}
			endRawSession(t, dr)
			require.Equal(t, want, harnessOut(t, dr.out), "the human's bytes as typed, the notice only after them")
		})
	}
}

// TestPartialSequenceBetweenPrepareAndCommit prepares before the human
// starts a cut sequence: the commit that follows the first part aborts
// and writes nothing. Once the sequence is whole a new delivery waits
// for the guard alone, and goes through when the sequence moved no
// draft.
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
			want := f.first + f.rest
			if f.waits {
				require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-late"))
				require.Equal(t, []string{"human input since prepare", "snapshot mismatch"}, abortReasons(dr),
					"the early commit's, then the guard's, not the parser's")
			} else {
				require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, dr.session, "dlv-late"))
				rep, err = dr.session.CommitDelivery("dlv-late", partialNotice, protocol.InjectModeBracketedPaste)
				require.NoError(t, err)
				require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
				want += pasteBegin + partialNotice + pasteEnd + "\n"
				waitHarness(t, dr, want, "the notice never followed the whole sequence")
			}
			endRawSession(t, dr)
			require.Equal(t, want, harnessOut(t, dr.out))
		})
	}
}

// TestPartialSequenceKeepsTheGuardFeed holds and releases a sequence
// that moves no draft, a paste end with no paste open: the guard version
// and the generation stay as they were, and the Core gets no guard
// report for either, so it is never told to wait for a version that may
// not come. Its prepare against the version it already has goes through
// once the sequence is whole.
func TestPartialSequenceKeepsTheGuardFeed(t *testing.T) {
	t.Parallel()
	dr, k, _ := partialSession(t, "partial-feed")
	waitSeen(t, dr.session)
	before := publishedSnapshot(t, dr.session)
	for _, part := range lonePasteEnd {
		writeKeys(t, dr.master, part)
		waitSeen(t, dr.session)
		snap := publishedSnapshot(t, dr.session)
		require.Equal(t, before.Version, snap.Version)
		require.Equal(t, before.Generation, snap.Generation)
		require.Equal(t, before.Version, dr.session.GuardVersion())
		// The loop publishes the foto again on its key flush ticks
		// while the sequence is held.
		time.Sleep(3 * defaultKeyFlushEvery)
	}
	r := k.order(protocol.AgentOpPrepareDelivery,
		sockPrepare("dlv-sock", partialNotice, int64(before.Generation), before.Version))
	require.Equal(t, protocol.DeliveryResultKindPrepared, r.Result)
	require.Equal(t, 1, strings.Count(k.raw.String(), `"op":"`+string(protocol.AgentOpReportInputGuard)+`"`),
		"the greeting's report only: a hold and its release report nothing")
	endRawSession(t, dr)
}

// TestPartialSequenceSignalsNothing holds and releases a sequence that
// moves no draft, a paste end with no paste open, with no socket to take
// the change signal first: no guard change is signalled for either,
// ticks included.
func TestPartialSequenceSignalsNothing(t *testing.T) {
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
	for _, part := range lonePasteEnd {
		writeKeys(t, master, part)
		waitSeen(t, s)
		time.Sleep(3 * defaultKeyFlushEvery)
		select {
		case <-s.GuardChanged():
			t.Fatalf("a guard change signalled after %q", part)
		default:
		}
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
