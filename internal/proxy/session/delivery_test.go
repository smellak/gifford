//go:build unix

package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

// writeHarnessScript drops a fake harness: stty maps the submit CR to
// a newline and stops the echo, every received line is appended to
// out, and a line containing __done__ ends it. Setup lines run after
// stty, so once a ready line they print shows, no key is echoed.
func writeHarnessScript(t *testing.T, setup ...string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "fakeharness")
	body := "#!/bin/sh\nOUT=\"$1\"\nstty icrnl -echo\n" + strings.Join(append(setup, ""), "\n") +
		"while IFS= read -r line; do\n" +
		"  printf '%s\\n' \"$line\" >> \"$OUT\"\n" +
		"  case \"$line\" in *__done__*) exit 0;;\n  esac\ndone\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	return script
}

// deliveryRun holds one live delivery session and its harness output.
type deliveryRun struct {
	session *Session
	master  *os.File
	sink    *chanSink
	out     string
	done    chan runResult
}

// startDeliverySession runs one session with the fake harness and
// returns once its ready line is past the output quiet period.
func startDeliverySession(t *testing.T, id string, submitDelay time.Duration, paste bool) *deliveryRun {
	t.Helper()
	out := filepath.Join(t.TempDir(), "harness.out")
	script := writeHarnessScript(t, "echo deliver-ready")
	master, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), id, slave, slave, sink, []string{script, out})
	opts.Profile = deliveryProfile()
	opts.SubmitDelay = submitDelay
	if paste {
		opts.Profile.BracketedPaste = true
	}
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	readUntil(t, master, "deliver-ready")
	waitOutputQuiet(t, s, "deliver-ready")
	return &deliveryRun{session: s, master: master, sink: sink, out: out, done: done}
}

// waitScreen waits until the emulator's last snapshot satisfies cond.
func waitScreen(t *testing.T, s *Session, what string, cond func(vt.Snapshot) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return cond(s.lastSnap)
	}, 10*time.Second, time.Millisecond, what)
}

// below is a screen whose cursor has moved below the line holding text:
// the loop has folded the output up to that line.
func below(text string) func(vt.Snapshot) bool {
	return func(snap vt.Snapshot) bool {
		for row, line := range snap.Lines {
			if strings.Contains(line, text) {
				return snap.CursorRow > row
			}
		}
		return false
	}
}

// waitOutputQuiet waits until the loop has folded the harness's ready
// line, the last output it prints unasked, then waits out the output
// quiet period. Any delivery before is refused.
func waitOutputQuiet(t *testing.T, s *Session, ready string) {
	t.Helper()
	waitScreen(t, s, "the loop never folded the ready line", below(ready))
	waitQuiet(t, s)
}

// waitQuiet lets the output quiet period pass after the last output the
// loop folded, on the session's clock: a step clock moves past it, the
// system clock is waited out.
func waitQuiet(t *testing.T, s *Session) {
	t.Helper()
	last := publishedSnapshot(t, s).LastOutput
	require.False(t, last.IsZero(), "the harness wrote output")
	if clk, ok := s.clk.(*stepClock); ok {
		clk.advance(outputQuietPeriod)
	}
	require.Eventually(t, func() bool {
		return s.clk.Now().Sub(last) >= outputQuietPeriod
	}, 10*time.Second, 10*time.Millisecond, "the harness never went quiet")
}

// endDeliverySession releases the harness and waits for the run.
func endDeliverySession(t *testing.T, master *os.File, done chan runResult) {
	t.Helper()
	writeKeys(t, master, "__done__\n")
	waitDone(t, done)
}

// harnessOut reads the bytes the harness recorded so far.
func harnessOut(t *testing.T, out string) string {
	t.Helper()
	raw, err := os.ReadFile(out)
	if os.IsNotExist(err) {
		return ""
	}
	require.NoError(t, err)
	return string(raw)
}

// publishedSnapshot reads the loop's guard foto race-clean.
func publishedSnapshot(t *testing.T, s *Session) guardSnapshot {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap
}

// waitSnapshot waits until the foto satisfies cond.
func waitSnapshot(t *testing.T, s *Session, what string, cond func(guardSnapshot) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		return cond(publishedSnapshot(t, s))
	}, 10*time.Second, time.Millisecond, what)
}

// waitLease waits until a commit holds the delivery lease: the only
// sync point inside the submit delay, since the harness prints the
// line only after the submit lands and the lease is gone.
func waitLease(t *testing.T, s *Session) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.lease != nil
	}, 10*time.Second, time.Millisecond, "lease never taken")
}

// sinkPayloads returns the payloads of one event type from the sink.
func sinkPayloads(sink *chanSink, typ protocol.EventType) []any {
	var out []any
	for _, e := range sink.all() {
		if e.Type == typ {
			out = append(out, e.Payload)
		}
	}
	return out
}

// TestDeliveryCommitClosedDraft delivers one line to a closed draft:
// the harness gets exactly line plus submit and the answer is
// COMMITTED with prepared, revalidated, started and committed
// events.
func TestDeliveryCommitClosedDraft(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-ok", 0, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d1", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	require.Equal(t, uint64(0), rep.Generation)
	require.False(t, rep.At.IsZero())
	// The child prints only after the submit lands, after Commit
	// answers: wait for the round trip instead of reading too early.
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "hi\n"
	}, 10*time.Second, 10*time.Millisecond, "line never reached the harness")
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryPrepared))
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryRevalidated))
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCommitted))

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryAbortHumanBetween aborts on a human key between Prepare
// and Commit: the harness gets only the key, none of the line.
func TestDeliveryAbortHumanBetween(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-abort", 0, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	writeKeys(t, dr.master, "x\n")
	require.Eventually(t, func() bool {
		return dr.session.humanWritten.Load() >= 1
	}, 10*time.Second, time.Millisecond, "human key never reached the writer")
	rep, err = dr.session.CommitDelivery("d1", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	// The key reaches the harness through the loop and child, after
	// the abort answers: wait for it instead of reading too early.
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "x\n"
	}, 10*time.Second, 10*time.Millisecond, "human key never reached the harness")
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryAbortedHumanPriority))

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryMouseAndPasteKeepHumanPriority covers the active terminal input
// that must still invalidate a Prepare. Coordinates remain transport bytes,
// but no delivery line may follow them.
func TestDeliveryMouseAndPasteKeepHumanPriority(t *testing.T) {
	t.Parallel()

	for name, sequence := range map[string]string{
		"click":       "\x1b[<0;12;5M",
		"wheel":       "\x1b[<65;12;5M",
		"paste start": pasteBegin,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dr := startDeliverySession(t, "deliver-active-terminal", 0, false)
			chunks := publishedSnapshot(t, dr.session).Chunks

			rep, err := dr.session.PrepareDelivery("d1", 0, 0)
			require.NoError(t, err)
			require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
			writeKeysAll(t, dr.master, sequence)
			waitSnapshot(t, dr.session, "active terminal input never reached the loop", func(snap guardSnapshot) bool {
				return snap.Chunks > chunks && !snap.Pending
			})
			rep, err = dr.session.CommitDelivery("d1", "must-not-land", protocol.InjectModeTyped)
			require.NoError(t, err)
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
			require.NotContains(t, harnessOut(t, dr.out), "must-not-land")

			endDeliverySession(t, dr.master, dr.done)
			require.Equal(t, sequence+"__done__\n", harnessOut(t, dr.out))
		})
	}
}

// TestDeliveryAbortOpenDraft aborts a Prepare while the human edits.
func TestDeliveryAbortOpenDraft(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-open", 0, false)
	// The first typed rune opens the draft at generation 1; the
	// newline keeps it open.
	writeKeys(t, dr.master, "hello\n")
	waitSnapshot(t, dr.session, "draft never opened", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen
	})

	rep, err := dr.session.PrepareDelivery("d1", 1, 1)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
}

// fanoutSink forwards every event to each sink.
type fanoutSink struct{ sinks []EventSink }

func (f fanoutSink) Emit(e Event) {
	for _, s := range f.sinks {
		s.Emit(e)
	}
}

// TestDeliveryAbortTextOnlyDraft aborts a Prepare while bare printable
// text without Enter holds the draft open: the text opens the draft and
// publishes the snapshot, so Prepare with the current foto answers
// ABORTED_HUMAN_PRIORITY and no delivery byte reaches the harness.
// Typed content must not leak into events or logs.
func TestDeliveryAbortTextOnlyDraft(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "harness.out")
	script := writeHarnessScript(t, "echo deliver-ready")
	master, slave := testPTY(t)
	sink := &chanSink{}
	logPath := filepath.Join(t.TempDir(), "events.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	logs := NewEventLog(logFile, "deliver-textonly", EventLogOptions{})
	opts := testOptions(t.TempDir(), "deliver-textonly", slave, slave,
		fanoutSink{sinks: []EventSink{sink, logs}}, []string{script, out})
	opts.Profile = deliveryProfile()
	opts.SubmitDelay = 0
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	// Typed only once the harness turned its echo off, so the keys
	// never reach the screen.
	readUntil(t, master, "deliver-ready")

	const typed = "qk7x2"
	writeKeys(t, master, typed)
	waitSnapshot(t, s, "text-only draft never opened", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen
	})
	snap := publishedSnapshot(t, s)
	require.Equal(t, uint64(1), snap.Generation)
	require.Positive(t, snap.Version, "opening the draft moves the version")

	rep, err := s.PrepareDelivery("d1", snap.Generation, snap.Version)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	_, err = s.CommitDelivery("d1", "canary-line-5", protocol.InjectModeTyped)
	require.Error(t, err)
	require.NotContains(t, harnessOut(t, out), "canary-line-5")

	endDeliverySession(t, master, done)
	require.Equal(t, typed+"__done__\n", harnessOut(t, out), "only the human's bytes reach the harness")
	require.NoError(t, logs.Close())
	raw, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotContains(t, string(raw), typed)
	events, err := json.Marshal(sink.all())
	require.NoError(t, err)
	require.NotContains(t, string(events), typed)
}

// TestDeliveryAbortTextBetween aborts a Commit after bare text typed
// between Prepare and Commit: the text alone opens the draft, so none
// of the line reaches the harness.
func TestDeliveryAbortTextBetween(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-text-between", 0, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	writeKeys(t, dr.master, "x")
	waitSnapshot(t, dr.session, "bare text never opened the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen && snap.Generation == 1
	})
	rep, err = dr.session.CommitDelivery("d1", "canary-line-6", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryAbortedHumanPriority))

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "x__done__\n", harnessOut(t, dr.out), "only the human's bytes reach the harness")
}

// TestDeliverySubmitWithNoInputLineBlocks types a submit on a measured
// profile without an input line, once the harness has turned its echo
// off, so the keys write nothing on the screen: no screen can show what
// the submit did, so the guard goes UNKNOWN at once, no quiet window
// closes it, and no delivery goes through. Only the human's line
// reaches the harness.
func TestDeliverySubmitWithNoInputLineBlocks(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-typed-submit", 0, false)
	s := dr.session
	ready := publishedSnapshot(t, s).LastOutput
	writeKeys(t, dr.master, "hello")
	waitSnapshot(t, s, "text never opened the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen && snap.Generation == 1
	})
	writeKeys(t, dr.master, "\r")
	waitSnapshot(t, s, "the submit never reached the guard", func(snap guardSnapshot) bool {
		return snap.State == draft.StateUnknown
	})
	waitHarness(t, dr, "hello\n", "the human's line never reached the harness")
	require.Equal(t, ready, publishedSnapshot(t, s).LastOutput, "the harness echoed nothing")
	require.Never(t, func() bool {
		return publishedSnapshot(t, s).State != draft.StateUnknown
	}, 600*time.Millisecond, 10*time.Millisecond, "no quiet window closes it")
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftSubmitted))

	snap := publishedSnapshot(t, s)
	rep, err := s.PrepareDelivery("d1", snap.Generation, snap.Version)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "hello\n__done__\n", harnessOut(t, dr.out), "only the human's lines")
}

// TestUnmeasuredProfileNeverDelivers runs a profile not yet measured, as
// every profile is until keycap measures it: every prepare is refused,
// through the session and through the socket, with the guard closed,
// after a submit, after the cancel that closes the draft again and after
// the unlock that ends an UNKNOWN. No commit has anything to write, and
// one planted as if its prepare had gone through is refused on its own.
// No byte but the human's reaches the harness.
func TestUnmeasuredProfileNeverDelivers(t *testing.T) {
	t.Parallel()
	dr, k, _ := startSocketDelivery(t, "deliver-unmeasured", newWindowClock(false), func(o *Options) {
		o.Profile = testProfile()
	})
	s := dr.session
	require.False(t, s.opts.Profile.Measured)
	refuse := func(id protocol.OpaqueID) {
		t.Helper()
		snap := publishedSnapshot(t, s)
		rep, err := s.PrepareDelivery(id, snap.Generation, snap.Version)
		require.NoError(t, err)
		require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
		gen, ver := guardAt(s)
		r := k.order(protocol.AgentOpPrepareDelivery, sockPrepare(id+"-sock", sockNotice, gen, ver))
		require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result)
	}
	refuse("dlv-closed")

	writeKeys(t, dr.master, "hello\r")
	waitSnapshot(t, s, "the submit never reached the guard", func(snap guardSnapshot) bool {
		return snap.State == draft.StateSubmitting && snap.Generation == 1
	})
	refuse("dlv-submitted")
	// The guard saw the submit, which says nothing of the harness: a
	// cancel before it reads the line flushes the line from its terminal.
	waitHarness(t, dr, "hello\n", "the human's line never reached the harness")

	// The first cancel reopens the draft, as any key after a submit
	// does; the second closes it at once, as an unmeasured cancel does.
	writeKeys(t, dr.master, "\x03\x03")
	waitSnapshot(t, s, "the cancel never closed the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed && snap.Generation == 2
	})
	refuse("dlv-cancelled")
	_, err := s.CommitDelivery("dlv-cancelled", "hi", protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryUnknown)
	require.Equal(t, protocol.DeliveryResultKindFailed,
		k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-cancelled-sock")).Result)

	// A resize mid-paste loses the guard; the owner's resync from
	// scratch unlocks it, and still nothing goes through.
	writeKeys(t, dr.master, "\x1b[200~")
	waitSnapshot(t, s, "the paste never started", func(snap guardSnapshot) bool {
		return snap.State == draft.StatePasting
	})
	require.NoError(t, s.Resize(100, 30))
	waitSnapshot(t, s, "the resize mid-paste never lost the guard", func(snap guardSnapshot) bool {
		return snap.State == draft.StateUnknown
	})
	press(t, dr, "\x1b[201~")
	s.mu.Lock()
	s.guard.Unlock()
	s.publishLocked()
	s.mu.Unlock()
	require.Equal(t, draft.StateClosed, publishedSnapshot(t, s).State)
	refuse("dlv-unlocked")

	s.mu.Lock()
	s.prepared["dlv-planted"] = pendingDelivery{
		generation: s.snap.Generation, version: s.snap.Version,
		written: s.humanWritten.Load(), arrived: s.arrived,
	}
	s.mu.Unlock()
	rep, err := s.CommitDelivery("dlv-planted", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome, "the commit checks on its own")

	require.Equal(t, slices.Repeat([]string{reasonUnmeasured}, 9), abortReasons(dr))
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))
	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "hello\n\x1b[200~\x1b[201~__done__\n", harnessOut(t, dr.out), "only the human's lines")
}

// TestDeliveryAbortUnknown aborts a Prepare while the guard is lost:
// a resize mid-paste drops sync.
func TestDeliveryAbortUnknown(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-unknown", 0, false)
	// The fake harness never prints to stdout (everything goes to
	// its file), so no output baseline exists: a first resize at
	// the default size sets the guard's screen baseline instead.
	// The second resize, mid-paste, is the one that loses sync.
	require.NoError(t, dr.session.Resize(80, 24))
	writeKeys(t, dr.master, "\x1b[200~abc")
	waitSnapshot(t, dr.session, "paste never started", func(snap guardSnapshot) bool {
		return snap.State == draft.StatePasting
	})
	require.NoError(t, dr.session.Resize(100, 30))
	waitSnapshot(t, dr.session, "guard never went unknown", func(snap guardSnapshot) bool {
		return snap.State == draft.StateUnknown
	})

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryAbortMismatch aborts on generation and version drift.
func TestDeliveryAbortMismatch(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-drift", 0, false)

	rep, err := dr.session.PrepareDelivery("d1", 999, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	rep, err = dr.session.PrepareDelivery("d2", 0, 999)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	require.Empty(t, harnessOut(t, dr.out))

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryAbortRecentSubmit commits a delivery prepared before a
// lone human submit. On a measured profile a submit closes nothing by
// itself, even with no draft open: the guard leaves CLOSED at once, the
// loop has seen the key, and the commit aborts with nothing written.
func TestDeliveryAbortRecentSubmit(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-recent", 0, false)
	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)

	writeKeys(t, dr.master, "\r")
	waitSnapshot(t, dr.session, "submit never processed", func(snap guardSnapshot) bool {
		return snap.Chunks >= 1 && snap.State == draft.StateUnknown &&
			dr.session.humanWritten.Load() == snap.Chunks
	})
	rep, err = dr.session.CommitDelivery("d1", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "\n__done__\n", harnessOut(t, dr.out), "only the human's keys")
}

// TestDeliveryAbortSecondLease aborts a Commit while another holds
// the lease, then lets the first finish committed.
func TestDeliveryAbortSecondLease(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-lease", 500*time.Millisecond, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	type commitOut struct {
		rep DeliveryReport
		err error
	}
	first := make(chan commitOut, 1)
	go func() {
		rep, err := dr.session.CommitDelivery("d1", "first", protocol.InjectModeTyped)
		first <- commitOut{rep: rep, err: err}
	}()
	waitLease(t, dr.session)

	rep, err = dr.session.PrepareDelivery("d2", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	// A second commit while the first owns the writer refuses: the
	// refused line never reaches the harness.
	_, err = dr.session.CommitDelivery("d2", "second", protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryActive)

	select {
	case out := <-first:
		require.NoError(t, out.err)
		require.Equal(t, DeliveryCommittedOutcome, out.rep.Outcome)
	case <-time.After(15 * time.Second):
		t.Fatal("first commit never finished")
	}
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "first\n"
	}, 10*time.Second, 10*time.Millisecond, "first line never reached the harness")

	endDeliverySession(t, dr.master, dr.done)
}

// waitCommitting waits until CommitDelivery reserved the commit:
// the barrier every racing Cancel or second Commit must observe.
func waitCommitting(t *testing.T, s *Session) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.committing
	}, 10*time.Second, time.Millisecond, "commit never reserved")
}

// TestDeliveryCancelDuringCommit races Cancel against an in-flight
// Commit past the reservation barrier: Cancel refuses with
// ErrDeliveryActive, never CANCELLED, and the line still lands
// exactly once.
func TestDeliveryCancelDuringCommit(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-cancel-race", 500*time.Millisecond, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	type commitOut struct {
		rep DeliveryReport
		err error
	}
	done := make(chan commitOut, 1)
	go func() {
		rep, err := dr.session.CommitDelivery("d1", "race", protocol.InjectModeTyped)
		done <- commitOut{rep: rep, err: err}
	}()
	waitCommitting(t, dr.session)

	_, err = dr.session.CancelDelivery("d1", "late")
	require.ErrorIs(t, err, ErrDeliveryActive)

	select {
	case out := <-done:
		require.NoError(t, out.err)
		require.Equal(t, DeliveryCommittedOutcome, out.rep.Outcome)
	case <-time.After(15 * time.Second):
		t.Fatal("commit never finished")
	}
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "race\n"
	}, 10*time.Second, 10*time.Millisecond, "line never reached the harness")
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled))

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryReservationRefuses pins the pre-lease reservation with
// no timing: while a commit is reserved but holds no lease yet, a
// Cancel for its id and any second Commit refuse with
// ErrDeliveryActive; once released, both paths work again.
func TestDeliveryReservationRefuses(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-reserve", 0, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.PrepareDelivery("d2", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)

	dr.session.mu.Lock()
	dr.session.committing = true
	dr.session.committingID = "d1"
	dr.session.mu.Unlock()

	_, err = dr.session.CancelDelivery("d1", "late")
	require.ErrorIs(t, err, ErrDeliveryActive)
	_, err = dr.session.CommitDelivery("d2", "x", protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryActive)

	dr.session.mu.Lock()
	dr.session.committing = false
	dr.session.committingID = ""
	dr.session.mu.Unlock()

	rep, err = dr.session.CancelDelivery("d1", "late")
	require.NoError(t, err)
	require.Equal(t, DeliveryCancelledOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d2", "ok", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "ok\n"
	}, 10*time.Second, 10*time.Millisecond, "line never reached the harness")

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryDoubleCommitOneSubmit races two Commits of one id past
// the reservation barrier: the second refuses with ErrDeliveryActive
// and the harness sees exactly one submit.
func TestDeliveryDoubleCommitOneSubmit(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-double", 500*time.Millisecond, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	type commitOut struct {
		rep DeliveryReport
		err error
	}
	done := make(chan commitOut, 1)
	go func() {
		rep, err := dr.session.CommitDelivery("d1", "one", protocol.InjectModeTyped)
		done <- commitOut{rep: rep, err: err}
	}()
	waitCommitting(t, dr.session)

	_, err = dr.session.CommitDelivery("d1", "two", protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryActive)

	select {
	case out := <-done:
		require.NoError(t, out.err)
		require.Equal(t, DeliveryCommittedOutcome, out.rep.Outcome)
	case <-time.After(15 * time.Second):
		t.Fatal("first commit never finished")
	}
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "one\n"
	}, 10*time.Second, 10*time.Millisecond, "line never reached the harness")

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "one\n__done__\n", harnessOut(t, dr.out))
}

// TestDeliveryParkedKeyAborts replays the exact lost interleave with
// no timing: pumpIn read a key (the arrival counter moved) but has
// not handed it to the writer yet when the commit drains and
// revalidates. The commit aborts and the line never reaches the
// descriptor; without the parked key the same commit lands.
func TestDeliveryParkedKeyAborts(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-parked", 0, false)
	s := dr.session

	pipeR, pipeW, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = pipeR.Close() }()
	defer func() { _ = pipeW.Close() }()
	require.NoError(t, pipeR.SetReadDeadline(time.Now().Add(200*time.Millisecond)))

	// bumpBefore replays a key pumpIn published before Prepare that
	// never reached the channel; bumpAfter, one published between
	// Prepare and the commit drain.
	drive := func(id protocol.OpaqueID, bumpBefore, bumpAfter bool) (DeliveryReport, error) {
		s.mu.Lock()
		if bumpBefore {
			s.arrived++
		}
		arrived := s.arrived
		s.prepared[id] = pendingDelivery{
			generation: 0, version: 0,
			written: s.humanWritten.Load(), arrived: arrived,
		}
		s.committing = true
		s.committingID = id
		if bumpAfter {
			s.arrived++
		}
		s.mu.Unlock()
		w := &sessionWriter{
			s: s, dst: pipeW, in: make(chan []byte),
			loop: make(chan []byte, 16), ops: nil,
			done: make(chan struct{}), perr: make(chan error, 1),
		}
		done := make(chan commitResult, 1)
		w.handleCommit(writerOp{commit: &commitRequest{
			id: id, line: "hi", gen: 0, ver: 0,
			written: 0, arrived: arrived,
			submit: []byte("\n"), delay: time.Millisecond, done: done,
		}})
		res := <-done
		return res.report, res.err
	}

	rep, err := drive("d9", false, true)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	// Nothing reached the descriptor: the read times out.
	var one [16]byte
	_, err = pipeR.Read(one[:])
	require.Error(t, err)

	// An arrival published before Prepare but still parked refuses
	// the same way, with nothing written either.
	rep, err = drive("d11", true, false)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
	_, err = pipeR.Read(one[:])
	require.Error(t, err)
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryAbortedHumanPriority))

	// The replayed arrivals were never real keys, so none will ever
	// be written: realign the counter before the control commit.
	s.mu.Lock()
	s.arrived = s.humanWritten.Load()
	s.mu.Unlock()

	rep, err = drive("d10", false, false)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	require.NoError(t, pipeR.SetReadDeadline(time.Now().Add(10*time.Second)))
	got := make([]byte, 0, 4)
	for len(got) < 3 {
		n, err := pipeR.Read(one[:])
		require.NoError(t, err)
		got = append(got, one[:n]...)
	}
	require.Equal(t, "hi\n", string(got))

	endDeliverySession(t, dr.master, dr.done)
}

// writeKeysAll writes every byte, looping over partial PTY writes.
func writeKeysAll(t *testing.T, w *os.File, s string) {
	t.Helper()
	rest := []byte(s)
	for len(rest) > 0 {
		n, err := w.Write(rest)
		require.NoError(t, err)
		rest = rest[n:]
	}
}

// TestDeliveryLeaseBuffersKeys holds the lease open while over 4 KiB
// of human keys arrive: they come out after the submit, in order and
// complete, with the writer back-pressuring instead of losing any. The
// keys are newline-terminated lines so the line-reading harness keeps
// consuming: a newline-less flood would fill the PTY queue while the
// child waits for its terminator, which is flow control, not delivery.
func TestDeliveryLeaseBuffersKeys(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-buffer", 500*time.Millisecond, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	type commitOut struct {
		rep DeliveryReport
		err error
	}
	done := make(chan commitOut, 1)
	go func() {
		rep, err := dr.session.CommitDelivery("d1", "line", protocol.InjectModeTyped)
		done <- commitOut{rep: rep, err: err}
	}()
	waitLease(t, dr.session)
	keys := strings.Repeat(strings.Repeat("y", 100)+"\n", 50)
	require.Greater(t, len(keys), leaseBufferMax)
	writeKeysAll(t, dr.master, keys)

	select {
	case out := <-done:
		require.NoError(t, out.err)
		require.Equal(t, DeliveryCommittedOutcome, out.rep.Outcome)
	case <-time.After(15 * time.Second):
		t.Fatal("commit never finished")
	}
	require.NotEmpty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryChunk))

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "line\n"+keys+"__done__\n", harnessOut(t, dr.out))
}

// TestDeliveryModeDecidesThePaste wraps the line by the commit's mode,
// whatever the profile says: bracketed_paste on a typed profile wraps
// it, and typed on a paste profile sends it raw. A mode the session
// does not write is refused before anything is written.
func TestDeliveryModeDecidesThePaste(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		paste bool
		mode  protocol.InjectMode
		want  string
	}{
		{name: "paste over a typed profile", paste: false, mode: protocol.InjectModeBracketedPaste, want: "\x1b[200~hi\x1b[201~\n"},
		{name: "typed over a paste profile", paste: true, mode: protocol.InjectModeTyped, want: "hi\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dr := startDeliverySession(t, "deliver-mode", 0, tc.paste)

			rep, err := dr.session.PrepareDelivery("d1", 0, 0)
			require.NoError(t, err)
			require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
			_, err = dr.session.CommitDelivery("d1", "hi", "")
			require.ErrorIs(t, err, errInjectMode)
			require.Len(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryFailed), 1)
			require.Empty(t, harnessOut(t, dr.out), "a refused mode writes nothing")

			rep, err = dr.session.CommitDelivery("d1", "hi", tc.mode)
			require.NoError(t, err)
			require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
			require.Eventually(t, func() bool {
				return harnessOut(t, dr.out) == tc.want
			}, 10*time.Second, 10*time.Millisecond, "the line never reached the harness")

			endDeliverySession(t, dr.master, dr.done)
		})
	}
}

// TestDeliveryLineValidation rejects a bad line before writing
// anything, while the 4096-byte boundary still commits.
func TestDeliveryLineValidation(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-line", 0, false)

	// An empty line is refused too: it would deliver a bare submit.
	for _, line := range []string{"", "a\nb", "a\x1bb", strings.Repeat("z", 4097)} {
		rep, err := dr.session.PrepareDelivery("d1", 0, 0)
		require.NoError(t, err)
		require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
		_, err = dr.session.CommitDelivery("d1", line, protocol.InjectModeTyped)
		require.Error(t, err)
	}
	require.Empty(t, harnessOut(t, dr.out))
	require.Len(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryFailed), 4)

	rep, err := dr.session.PrepareDelivery("d2", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d2", strings.Repeat("z", 4096), protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryCancel discards a Prepare before its Commit: nothing is
// written, and the id is spent.
func TestDeliveryCancel(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-cancel", 0, false)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CancelDelivery("d1", "superseded")
	require.NoError(t, err)
	require.Equal(t, DeliveryCancelledOutcome, rep.Outcome)
	cancelled := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled)
	require.Len(t, cancelled, 1)
	require.Equal(t, "superseded", cancelled[0].(DeliveryCancelled).Reason)
	_, err = dr.session.CommitDelivery("d1", "hi", protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryUnknown)
	require.Empty(t, harnessOut(t, dr.out))

	endDeliverySession(t, dr.master, dr.done)
}
