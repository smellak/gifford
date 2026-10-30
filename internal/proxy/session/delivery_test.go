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
	"github.com/smellak/gifford/internal/proxy/draft"
)

// writeHarnessScript drops a fake harness: stty maps the submit CR to
// a newline, every received line is appended to out, and a line
// containing __done__ ends it. Setup lines run first.
func writeHarnessScript(t *testing.T, setup ...string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "fakeharness")
	body := "#!/bin/sh\nOUT=\"$1\"\n" + strings.Join(append(setup, ""), "\n") +
		"stty icrnl -echo\nwhile IFS= read -r line; do\n" +
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

// startDeliverySession runs one session with the fake harness.
func startDeliverySession(t *testing.T, id string, submitDelay time.Duration, paste bool) *deliveryRun {
	t.Helper()
	out := filepath.Join(t.TempDir(), "harness.out")
	script := writeHarnessScript(t)
	master, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), id, slave, slave, sink, []string{script, out})
	opts.SubmitDelay = submitDelay
	if paste {
		opts.Profile.BracketedPaste = true
	}
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	return &deliveryRun{session: s, master: master, sink: sink, out: out, done: done}
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
	script := writeHarnessScript(t)
	master, slave := testPTY(t)
	sink := &chanSink{}
	logPath := filepath.Join(t.TempDir(), "events.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	logs := NewEventLog(logFile, "deliver-textonly", EventLogOptions{})
	opts := testOptions(t.TempDir(), "deliver-textonly", slave, slave,
		fanoutSink{sinks: []EventSink{sink, logs}}, []string{script, out})
	opts.SubmitDelay = 0
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)

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

// eventOf returns the first event of one type from the sink.
func eventOf(t *testing.T, sink *chanSink, typ protocol.EventType) Event {
	t.Helper()
	for _, e := range sink.all() {
		if e.Type == typ {
			return e
		}
	}
	t.Fatalf("no %s event", typ)
	return Event{}
}

// TestDeliveryTypedSubmitMeasured pins a typed submit on a measured
// profile without an input line, whose signal is the quiet window: the
// text opens the draft, the submit closes it only after the 300 ms
// quiet window, and a delivery then commits after the human's line.
func TestDeliveryTypedSubmitMeasured(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "harness.out")
	master, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), "deliver-typed-submit", slave, slave, sink,
		[]string{writeHarnessScript(t), out})
	opts.SubmitDelay = 0
	opts.Profile.Measured = true
	opts.Profile.InputLinePattern = ""
	opts.Profile.InputLine = nil
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)

	writeKeys(t, master, "hello")
	waitSnapshot(t, s, "text never opened the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen && snap.Generation == 1
	})
	writeKeys(t, master, "\r")
	waitSnapshot(t, s, "the submit never closed the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed && snap.Generation == 1
	})
	submit := eventOf(t, sink, protocol.EventTypeInputSubmitDetected)
	closed := eventOf(t, sink, protocol.EventTypeInputDraftSubmitted)
	require.GreaterOrEqual(t, time.Duration(closed.MonotonicNS-submit.MonotonicNS), 300*time.Millisecond,
		"the draft closes only after the quiet window")

	snap := publishedSnapshot(t, s)
	rep, err := s.PrepareDelivery("d1", snap.Generation, snap.Version)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = s.CommitDelivery("d1", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	require.Eventually(t, func() bool {
		return harnessOut(t, out) == "hello\nhi\n"
	}, 10*time.Second, 10*time.Millisecond, "the line never followed the human's")

	endDeliverySession(t, master, done)
}

// TestDeliveryTypedSubmitUnmeasured pins a typed submit on an
// unmeasured profile, as every profile is until keycap measures it:
// nothing confirms the submit, so the draft stays SUBMITTING past the
// quiet window and every delivery aborts, until a valid cancel closes
// the draft. The first cancel reopens it, as any human key does; the
// second cancels it, and a delivery then commits.
func TestDeliveryTypedSubmitUnmeasured(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "harness.out")
	master, slave := testPTY(t)
	sink := &chanSink{}
	// The harness ignores the interrupt a cancel raises in its tty.
	opts := testOptions(t.TempDir(), "deliver-typed-unmeasured", slave, slave, sink,
		[]string{writeHarnessScript(t, "trap '' INT"), out})
	opts.SubmitDelay = 0
	require.False(t, opts.Profile.Measured)
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)

	writeKeys(t, master, "hello\r")
	waitSnapshot(t, s, "the typed submit never reached the guard", func(snap guardSnapshot) bool {
		return snap.State == draft.StateSubmitting && snap.Generation == 1
	})
	require.Never(t, func() bool {
		return publishedSnapshot(t, s).State != draft.StateSubmitting
	}, 600*time.Millisecond, 10*time.Millisecond, "an unmeasured submit is never confirmed")
	snap := publishedSnapshot(t, s)
	rep, err := s.PrepareDelivery("d1", snap.Generation, snap.Version)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	writeKeys(t, master, "\x03")
	waitSnapshot(t, s, "the cancel never reopened the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen && snap.Generation == 2
	})
	snap = publishedSnapshot(t, s)
	rep, err = s.PrepareDelivery("d2", snap.Generation, snap.Version)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	writeKeys(t, master, "\x03")
	waitSnapshot(t, s, "the second cancel never closed the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed && snap.Generation == 2
	})
	require.NotEmpty(t, sinkPayloads(sink, protocol.EventTypeInputDraftCancelled))
	snap = publishedSnapshot(t, s)
	rep, err = s.PrepareDelivery("d3", snap.Generation, snap.Version)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = s.CommitDelivery("d3", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	require.Eventually(t, func() bool {
		return harnessOut(t, out) == "hello\nhi\n"
	}, 10*time.Second, 10*time.Millisecond, "the line never followed the human's")

	endDeliverySession(t, master, done)
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

// TestDeliveryAbortRecentSubmit aborts a Commit right after a human
// submit the loop has fully seen: nothing is in flight, yet the calm
// window has not passed.
func TestDeliveryAbortRecentSubmit(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-recent", 0, false)

	// A lone CR submits without opening: the loop sees it fully
	// while the draft stays closed, then Prepare records the
	// counters and Commit must refuse inside the calm window.
	writeKeys(t, dr.master, "\r")
	waitSnapshot(t, dr.session, "submit never processed", func(snap guardSnapshot) bool {
		return snap.Chunks >= 1 && snap.State == draft.StateClosed &&
			dr.session.humanWritten.Load() == snap.Chunks
	})

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d1", "hi", protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
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
