//go:build unix

package session

import (
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
// containing __done__ ends it.
func writeHarnessScript(t *testing.T) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "fakeharness")
	body := "#!/bin/sh\nOUT=\"$1\"\nstty icrnl -echo\nwhile IFS= read -r line; do\n" +
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
	rep, err = dr.session.CommitDelivery("d1", "hi")
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
	rep, err = dr.session.CommitDelivery("d1", "hi")
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
	// Bare text stays inside the input parser: the terminating
	// newline flushes it and opens the draft at generation 1.
	writeKeys(t, dr.master, "hello\n")
	waitSnapshot(t, dr.session, "draft never opened", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen
	})

	rep, err := dr.session.PrepareDelivery("d1", 1, 1)
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

	endDeliverySession(t, dr.master, dr.done)
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
	rep, err = dr.session.CommitDelivery("d1", "hi")
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
		rep, err := dr.session.CommitDelivery("d1", "first")
		first <- commitOut{rep: rep, err: err}
	}()
	waitLease(t, dr.session)

	rep, err = dr.session.PrepareDelivery("d2", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d2", "second")
	require.NoError(t, err)
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)

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
		rep, err := dr.session.CommitDelivery("d1", "line")
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

// TestDeliveryBracketedPaste wraps the line per profile while a typed
// profile sends it raw.
func TestDeliveryBracketedPaste(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-paste", 0, true)

	rep, err := dr.session.PrepareDelivery("d1", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d1", "hi")
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "\x1b[200~hi\x1b[201~\n"
	}, 10*time.Second, 10*time.Millisecond, "paste never reached the harness")

	endDeliverySession(t, dr.master, dr.done)
}

// TestDeliveryLineValidation rejects a bad line before writing
// anything, while the 4096-byte boundary still commits.
func TestDeliveryLineValidation(t *testing.T) {
	t.Parallel()

	dr := startDeliverySession(t, "deliver-line", 0, false)

	for _, line := range []string{"a\nb", "a\x1bb", strings.Repeat("z", 4097)} {
		rep, err := dr.session.PrepareDelivery("d1", 0, 0)
		require.NoError(t, err)
		require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
		_, err = dr.session.CommitDelivery("d1", line)
		require.Error(t, err)
	}
	require.Empty(t, harnessOut(t, dr.out))
	require.Len(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryFailed), 3)

	rep, err := dr.session.PrepareDelivery("d2", 0, 0)
	require.NoError(t, err)
	require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
	rep, err = dr.session.CommitDelivery("d2", strings.Repeat("z", 4096))
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
	_, err = dr.session.CommitDelivery("d1", "hi")
	require.ErrorIs(t, err, ErrDeliveryUnknown)
	require.Empty(t, harnessOut(t, dr.out))

	endDeliverySession(t, dr.master, dr.done)
}
