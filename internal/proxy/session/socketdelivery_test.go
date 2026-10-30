//go:build unix

package session

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

// sockNotice is the notice line the socket tests deliver.
const sockNotice protocol.NoticeLine = "[GIFFORD] 1 pending message"

// windowClock is a socket clock with no greeting deadline and no
// heartbeat. The consume window passes at once, or, with window set,
// when the test sends on it.
type windowClock struct {
	*stepClock
	window chan time.Time
}

func newWindowClock(manual bool) windowClock {
	c := windowClock{stepClock: &stepClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}}
	if manual {
		c.window = make(chan time.Time, 1)
	}
	return c
}

func (c windowClock) After(d time.Duration) <-chan time.Time {
	if d != agentsock.ConsumeWindow {
		return nil
	}
	if c.window != nil {
		return c.window
	}
	passed := make(chan time.Time, 1)
	passed <- c.Now()
	return passed
}

// startSocketDelivery runs a delivery session whose harness ignores
// the interrupt a cancel raises and says when it is ready, so no key
// reaches it before, with its agent socket and a registered Core. It
// returns once the ready line is past the output quiet period.
func startSocketDelivery(t *testing.T, id string, clk windowClock, edit func(*Options)) (*deliveryRun, *fakeCore, *agentsock.Server) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "harness.out")
	master, slave := testPTY(t)
	sink := &chanSink{}
	opts := testOptions(t.TempDir(), id, slave, slave, sink,
		[]string{writeHarnessScript(t, "trap '' INT", "echo deliver-ready"), out})
	opts.Profile = deliveryProfile()
	opts.SubmitDelay = 0
	if edit != nil {
		edit(&opts)
	}
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	readUntil(t, master, "deliver-ready")
	waitOutputQuiet(t, s, "deliver-ready")
	srv := listenAgentSocket(t, s, clk)
	k, _ := greetCore(t, srv.Path())
	return &deliveryRun{session: s, master: master, sink: sink, out: out, done: done}, k, srv
}

// sockPrepare is the prepare of delivery id with line, expecting the
// draft generation and guard version given.
func sockPrepare(id protocol.OpaqueID, line protocol.NoticeLine, generation, version int64) protocol.PrepareDelivery {
	return protocol.PrepareDelivery{
		DeliveryID: id, AgentID: sockAgent,
		MessageRef: protocol.MessageRef{
			MessageID: "01JB0000000000000000000011", FromAgent: "agent-2",
			Priority: protocol.PriorityNormal, ThreadID: "thread-1",
		},
		NoticeLine:             line,
		OrchestratorDecisionID: "01JB0000000000000000000002",
		ExpectedStateVersion:   1, ExpectedInputGuardVersion: version, ExpectedDraftGeneration: generation,
		ControlVersions: protocol.ControlVersions{"global": 18, "agent": 3},
	}
}

// sockCommit is the commit of id under the Core's lease.
func sockCommit(id protocol.OpaqueID) protocol.CommitDelivery {
	return protocol.CommitDelivery{DeliveryID: id, WriterLeaseID: "lease-" + id}
}

// answer reads the frame answering id, past the guard reports the
// session sends on its own.
func (k *fakeCore) answer(id int64) protocol.AgentFrame {
	k.t.Helper()
	for {
		f := k.read()
		if f.Op == protocol.AgentOpReportInputGuard && f.ReplyTo == nil {
			continue
		}
		require.NotNil(k.t, f.ReplyTo, "%s answers nothing", f.Op)
		require.Equal(k.t, id, *f.ReplyTo)
		return f
	}
}

// order sends one delivery order and reads its result.
func (k *fakeCore) order(op protocol.AgentOp, body any) protocol.DeliveryResult {
	k.t.Helper()
	f := k.answer(k.send(op, 0, body))
	require.Equal(k.t, protocol.AgentOpReportDeliveryResult, f.Op)
	var r protocol.DeliveryResult
	require.NoError(k.t, json.Unmarshal(f.Body, &r))
	require.Equal(k.t, sockAgent, r.AgentID)
	require.NotNil(k.t, r.ObservedInputGuardVersion)
	return r
}

// refused sends one order and reads the frame error answering it.
func (k *fakeCore) refused(op protocol.AgentOp, body any) protocol.FrameErrorCode {
	k.t.Helper()
	f := k.answer(k.send(op, 0, body))
	require.Equal(k.t, protocol.AgentOpError, f.Op)
	var e protocol.FrameError
	require.NoError(k.t, json.Unmarshal(f.Body, &e))
	return e.Code
}

// guardAt reads the session's draft generation and guard version.
func guardAt(s *Session) (generation, version int64) {
	g := s.InputGuard()
	return g.DraftGeneration, g.InputGuardVersion
}

// deliveryEvents counts the session's delivery events for id.
func deliveryEvents(sink *chanSink, id protocol.OpaqueID) int {
	n := 0
	for _, e := range sink.all() {
		if !strings.HasPrefix(string(e.Type), "input.delivery_") {
			continue
		}
		raw, err := json.Marshal(e.Payload)
		if err == nil && strings.Contains(string(raw), `"`+string(id)+`"`) {
			n++
		}
	}
	return n
}

// TestSocketDeliveryCommits runs a delivery from the Core to the
// harness: PREPARED, then COMMITTED with the Core's lease, the control
// versions as they came, the profile's mode, and the generation and
// version it ran under. The line reaches the harness once.
func TestSocketDeliveryCommits(t *testing.T) {
	t.Parallel()
	run := paintSession(t, "sock-commit", measuredInput)
	dr, k := run.deliveryRun, run.core
	run.screen(t, emptyBox)

	// A draft typed and emptied moves the guard off zero: the
	// backspace closes it once the harness has drawn the empty input
	// line after it.
	press(t, dr, "a\x7f")
	run.screen(t, emptyBox)
	waitSnapshot(t, dr.session, "the draft never closed", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed && snap.Generation == 1 && snap.Version == 2
	})

	p := sockPrepare("dlv-1", sockNotice, 1, 2)
	r := k.order(protocol.AgentOpPrepareDelivery, p)
	require.Equal(t, protocol.DeliveryResultKindPrepared, r.Result)
	require.Equal(t, int64(1), r.ObservedDraftGeneration)
	require.Equal(t, int64(2), *r.ObservedInputGuardVersion)
	require.Equal(t, p.ControlVersions, r.ControlVersions)

	r = k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-1"))
	require.Equal(t, protocol.DeliveryResultKindCommitted, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-dlv-1"), r.WriterLeaseID)
	require.Equal(t, protocol.InjectModeTyped, r.InjectMode)
	require.Equal(t, p.ControlVersions, r.ControlVersions)
	require.Equal(t, int64(1), r.ObservedDraftGeneration)
	require.Equal(t, int64(2), *r.ObservedInputGuardVersion)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "a\x7f"+string(sockNotice)+"\n"
	}, 10*time.Second, 10*time.Millisecond, "the line never reached the harness")

	endRawSession(t, dr)
	require.Equal(t, "a\x7f"+string(sockNotice)+"\n", harnessOut(t, dr.out), "the line once, after the human's keys")
}

// TestSocketDeliveryHumanKeyAborts types a key between the prepare and
// the commit: ABORTED_HUMAN_PRIORITY, and the line never comes.
func TestSocketDeliveryHumanKeyAborts(t *testing.T) {
	t.Parallel()
	dr, k, _ := startSocketDelivery(t, "sock-human", newWindowClock(false), nil)

	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-1", sockNotice, 0, 0)).Result)
	writeKeys(t, dr.master, "x")
	waitSnapshot(t, dr.session, "the key never opened a draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen
	})
	r := k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-1"))
	require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-dlv-1"), r.WriterLeaseID)

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "x__done__\n", harnessOut(t, dr.out), "only the human's keys")
}

// writeRedrawScript drops a fake harness that leaves each line it reads
// in its input: below its ready line it draws an empty prompt, its
// blank released by a reset the emulator does not hold back, then it
// prints each line back over the prompt through the printf format in
// $2, as a TUI that did not consume the submit redraws its input line.
func writeRedrawScript(t *testing.T) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "redrawharness")
	body := "#!/bin/sh\nOUT=\"$1\"\ntrap '' INT\nstty icrnl -echo\nprintf 'redraw-ready\\n> \\033[0m'\n" +
		"while IFS= read -r line; do\n" +
		"  printf '%s\\n' \"$line\" >> \"$OUT\"\n" +
		"  case \"$line\" in *__done__*) exit 0;; esac\n" +
		"  printf '\\r'\n  printf \"$2\" \"$line\"\ndone\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	return script
}

// TestSocketDeliveryNotConsumed commits to a harness that leaves the
// line in its input, on a measured profile. Only a settled screen, a
// visible cursor and an input line that is exactly the notice give
// COMMIT_NOT_CONSUMED; anything else is COMMITTED. A profile not yet
// measured delivers nothing, see TestUnmeasuredProfileNeverDelivers.
// The input line reads with or without the blank after the prompt,
// since the screen drops trailing blanks from an empty one.
func TestSocketDeliveryNotConsumed(t *testing.T) {
	t.Parallel()
	inputLine := regexp.MustCompile(`^> ?(.*)$`)
	for _, tc := range []struct {
		name   string
		redraw string
		want   protocol.DeliveryResultKind
	}{
		{name: "the exact line", redraw: `> %s\033[0m`, want: protocol.DeliveryResultKindCommitNotConsumed},
		// The row reads exactly the notice, but the emulator still
		// holds back the x after it.
		{name: "a pending snapshot", redraw: `> %s\033[0mx`, want: protocol.DeliveryResultKindCommitted},
		{name: "a hidden cursor", redraw: `\033[?25l> %s\033[0m`, want: protocol.DeliveryResultKindCommitted},
		{name: "the notice among other text", redraw: `> see %s now\033[0m`, want: protocol.DeliveryResultKindCommitted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := filepath.Join(t.TempDir(), "harness.out")
			master, slave := testPTY(t)
			opts := testOptions(t.TempDir(), "sock-consume", slave, slave, &chanSink{},
				[]string{writeRedrawScript(t), out, tc.redraw})
			opts.SubmitDelay = 0
			opts.Profile.Measured = true
			opts.Profile.InputLinePattern = inputLine.String()
			opts.Profile.InputLine = inputLine
			s, err := NewSession(opts)
			require.NoError(t, err)
			done := runAsync(t, s)
			waitRaw(t, slave)
			readUntil(t, master, "redraw-ready")
			waitScreen(t, s, "the harness never drew its empty prompt", func(snap vt.Snapshot) bool {
				return below("redraw-ready")(snap) && snap.Lines[snap.CursorRow] == ">" && snap.CursorCol == 2
			})
			waitQuiet(t, s)
			clk := newWindowClock(true)
			srv := listenAgentSocket(t, s, clk)
			k, _ := greetCore(t, srv.Path())

			gen, ver := guardAt(s)
			require.Equal(t, protocol.DeliveryResultKindPrepared,
				k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-1", sockNotice, gen, ver)).Result)
			id := k.send(protocol.AgentOpCommitDelivery, 0, sockCommit("dlv-1"))
			// The window closes once the redraw is on the screen.
			require.Eventually(t, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				snap := s.lastSnap
				if snap.CursorRow < 0 || snap.CursorRow >= len(snap.Lines) {
					return false
				}
				row := snap.Lines[snap.CursorRow]
				return strings.HasPrefix(row, "> ") && strings.Contains(row, "pending")
			}, 10*time.Second, 10*time.Millisecond, "the harness never redrew its input")
			clk.window <- time.Time{}
			f := k.answer(id)
			var r protocol.DeliveryResult
			require.NoError(t, json.Unmarshal(f.Body, &r))
			require.Equal(t, tc.want, r.Result)
			require.Equal(t, protocol.OpaqueID("lease-dlv-1"), r.WriterLeaseID)
			require.Equal(t, protocol.InjectModeTyped, r.InjectMode)
			require.Equal(t, string(sockNotice)+"\n", harnessOut(t, out), "delivered once")

			holds := tc.want == protocol.DeliveryResultKindCommitNotConsumed
			require.Equal(t, holds, s.SocketDelivery().InputHolds(string(sockNotice)))
			endDeliverySession(t, master, done)
			// The screen still shows the line, but a session that
			// ended is no evidence.
			require.False(t, s.SocketDelivery().InputHolds(string(sockNotice)))
		})
	}
}

// TestSocketDeliveryPasteOverProfile prepares bracketed_paste on a
// typed profile: the line reaches the middle PTY between the paste
// markers.
func TestSocketDeliveryPasteOverProfile(t *testing.T) {
	t.Parallel()
	dr, k, _ := startSocketDelivery(t, "sock-paste", newWindowClock(false), nil)
	require.False(t, dr.session.opts.Profile.BracketedPaste)

	p := sockPrepare("dlv-1", sockNotice, 0, 0)
	p.InjectMode = protocol.InjectModeBracketedPaste
	require.Equal(t, protocol.DeliveryResultKindPrepared, k.order(protocol.AgentOpPrepareDelivery, p).Result)
	r := k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-1"))
	require.Equal(t, protocol.DeliveryResultKindCommitted, r.Result)
	require.Equal(t, protocol.InjectModeBracketedPaste, r.InjectMode)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "\x1b[200~"+string(sockNotice)+"\x1b[201~\n"
	}, 10*time.Second, 10*time.Millisecond, "the paste never reached the harness")

	endDeliverySession(t, dr.master, dr.done)
}

// TestSocketDeliveryRefusesC1 prepares a notice with a C1 control, which
// the contract's pattern lets through: FAILED, and the session is never
// touched.
func TestSocketDeliveryRefusesC1(t *testing.T) {
	t.Parallel()
	dr, k, _ := startSocketDelivery(t, "sock-c1", newWindowClock(false), nil)

	line := protocol.NoticeLine("[GIFFORD] \u0085 1 pending message")
	require.True(t, line.Valid(), "the contract's check alone lets it through")
	r := k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-1", line, 0, 0))
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	require.Equal(t, "notice line refused", r.Error)
	require.Zero(t, deliveryEvents(dr.sink, "dlv-1"), "the session was never asked")
	r = k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-1"))
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "__done__\n", harnessOut(t, dr.out), "nothing was written")
}

// TestSocketDeliveryCancelAndRefusals cancels before the commit, then
// tries to cancel and to commit twice while a commit runs and after it,
// and orders for unknown deliveries: only the one commit writes.
func TestSocketDeliveryCancelAndRefusals(t *testing.T) {
	t.Parallel()
	clk := newWindowClock(true)
	dr, k, _ := startSocketDelivery(t, "sock-cancel", clk, nil)

	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-0", sockNotice, 0, 0)).Result)
	r := k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: "dlv-0", Reason: protocol.CancelReasonManual})
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonManual, r.CancelReason)
	cancelled := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled)
	require.Len(t, cancelled, 1)
	require.Equal(t, "MANUAL", cancelled[0].(DeliveryCancelled).Reason)

	require.Equal(t, protocol.FrameErrorCodeUnknownDelivery, k.refused(protocol.AgentOpCommitDelivery, sockCommit("dlv-nope")))
	require.Equal(t, protocol.FrameErrorCodeUnknownDelivery,
		k.refused(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: "dlv-nope", Reason: protocol.CancelReasonManual}))

	for _, id := range []protocol.OpaqueID{"dlv-1", "dlv-2"} {
		require.Equal(t, protocol.DeliveryResultKindPrepared,
			k.order(protocol.AgentOpPrepareDelivery, sockPrepare(id, sockNotice, 0, 0)).Result)
	}
	first := k.send(protocol.AgentOpCommitDelivery, 0, sockCommit("dlv-1"))
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == string(sockNotice)+"\n"
	}, 10*time.Second, 10*time.Millisecond, "the line never reached the harness")
	// The commit runs until its window closes.
	cancel := protocol.CancelDelivery{DeliveryID: "dlv-1", Reason: protocol.CancelReasonManual}
	r = k.order(protocol.AgentOpCancelDelivery, cancel)
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	r = k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-2"))
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	require.Equal(t, protocol.OpaqueID("lease-dlv-2"), r.WriterLeaseID)
	clk.window <- time.Time{}
	require.Equal(t, protocol.AgentOpReportDeliveryResult, k.answer(first).Op)
	r = k.order(protocol.AgentOpCancelDelivery, cancel)
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result, "after the commit too")

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, string(sockNotice)+"\n__done__\n", harnessOut(t, dr.out), "one line, written once")
}

// TestSocketDeliveryPause pauses automation: a prepare and a commit are
// CANCELLED with STATE_CHANGED and write nothing, while the human's keys
// still reach the harness. After resume the delivery enters.
func TestSocketDeliveryPause(t *testing.T) {
	t.Parallel()
	// The test draws the screen the human's submit leaves: the empty
	// input line after it confirms the submit, so the draft closes
	// again.
	p := paintSession(t, "sock-pause", measuredInput)
	dr, k := p.deliveryRun, p.core
	p.screen(t, emptyBox)

	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-1", sockNotice, 0, 0)).Result)
	k.send(protocol.AgentOpPauseAutomation, 0, protocol.AgentTarget{AgentID: sockAgent})
	r := k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-2", sockNotice, 0, 0))
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonStateChanged, r.CancelReason)
	require.Zero(t, deliveryEvents(dr.sink, "dlv-2"), "the paused prepare never reached the session")
	r = k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-1"))
	require.Equal(t, protocol.DeliveryResultKindCancelled, r.Result)
	require.Equal(t, protocol.CancelReasonStateChanged, r.CancelReason)

	writeKeys(t, dr.master, "hello\r")
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "hello\n"
	}, 10*time.Second, 10*time.Millisecond, "the pause held the human's keys")
	waitSeen(t, dr.session)
	p.screen(t, emptyBox)
	waitSnapshot(t, dr.session, "the submit never closed the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed && snap.Generation == 1
	})

	k.send(protocol.AgentOpResumeAutomation, 0, protocol.AgentTarget{AgentID: sockAgent})
	gen, ver := guardAt(dr.session)
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-2", sockNotice, gen, ver)).Result)
	require.Equal(t, protocol.DeliveryResultKindCommitted,
		k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-2")).Result)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == "hello\n"+string(sockNotice)+"\n"
	}, 10*time.Second, 10*time.Millisecond, "the delivery never entered after resume")

	endRawSession(t, dr)
}

// TestSocketDeliveryPauseVoidsPrepared prepares, pauses and resumes on
// the real session: the pause cancelled the delivery in the session, so
// its commit after resume is FAILED and writes nothing. A new prepare of
// the same id delivers the line once.
func TestSocketDeliveryPauseVoidsPrepared(t *testing.T) {
	t.Parallel()
	dr, k, _ := startSocketDelivery(t, "sock-pause-void", newWindowClock(false), nil)

	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-x", sockNotice, 0, 0)).Result)
	k.send(protocol.AgentOpPauseAutomation, 0, protocol.AgentTarget{AgentID: sockAgent})
	k.send(protocol.AgentOpResumeAutomation, 0, protocol.AgentTarget{AgentID: sockAgent})
	require.Eventually(t, func() bool {
		cancelled := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled)
		return len(cancelled) == 1 && cancelled[0].(DeliveryCancelled).Reason == "STATE_CHANGED"
	}, 10*time.Second, 10*time.Millisecond, "the pause never cancelled the delivery")

	r := k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-x"))
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	r = k.order(protocol.AgentOpCancelDelivery, protocol.CancelDelivery{DeliveryID: "dlv-x", Reason: protocol.CancelReasonManual})
	require.Equal(t, protocol.DeliveryResultKindFailed, r.Result)
	_, err := dr.session.CommitDelivery("dlv-x", string(sockNotice), protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryUnknown, "the session holds no prepare for it either")
	require.Empty(t, harnessOut(t, dr.out), "nothing was written")

	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-x", sockNotice, 0, 0)).Result)
	require.Equal(t, protocol.DeliveryResultKindCommitted,
		k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-x")).Result)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == string(sockNotice)+"\n"
	}, 10*time.Second, 10*time.Millisecond, "the new prepare never delivered")

	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, string(sockNotice)+"\n__done__\n", harnessOut(t, dr.out), "one line, written once")
}

// regreetCore dials until the socket greets, as a Core reconnecting
// while the last connection winds down, and answers the greeting.
func regreetCore(t *testing.T, path string) *fakeCore {
	t.Helper()
	var k *fakeCore
	require.Eventually(t, func() bool {
		c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
		if err != nil {
			return false
		}
		if c.SetReadDeadline(time.Now().Add(time.Second)) != nil {
			_ = c.Close()
			return false
		}
		hello, err := agentsock.ReadFrame(c)
		if err != nil || hello.Op != protocol.AgentOpRegisterAgent {
			_ = c.Close()
			return false
		}
		t.Cleanup(func() { _ = c.Close() })
		k = &fakeCore{t: t, c: c, next: 1}
		return true
	}, 10*time.Second, 10*time.Millisecond, "the socket never greeted again")
	k.send(protocol.AgentOpRegisterAgentResult, 1, protocol.RegisterAgentResult{AgentID: sockAgent, SessionID: "sess-2"})
	k.report()
	return k
}

// TestSocketDeliveryDisconnect drops the Core with a delivery prepared:
// the session cancels it with STATE_CHANGED, and a Core that connects
// again delivers.
func TestSocketDeliveryDisconnect(t *testing.T) {
	t.Parallel()
	dr, k, srv := startSocketDelivery(t, "sock-drop", newWindowClock(false), nil)

	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-1", sockNotice, 0, 0)).Result)
	require.NoError(t, k.c.Close())
	require.Eventually(t, func() bool {
		cancelled := sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryCancelled)
		return len(cancelled) == 1 && cancelled[0].(DeliveryCancelled).Reason == "STATE_CHANGED"
	}, 10*time.Second, 10*time.Millisecond, "the delivery outlived its Core")
	_, err := dr.session.CommitDelivery("dlv-1", string(sockNotice), protocol.InjectModeTyped)
	require.ErrorIs(t, err, ErrDeliveryUnknown)

	k = regreetCore(t, srv.Path())
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		k.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-1", sockNotice, 0, 0)).Result)
	require.Equal(t, protocol.DeliveryResultKindCommitted,
		k.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-1")).Result)
	require.Eventually(t, func() bool {
		return harnessOut(t, dr.out) == string(sockNotice)+"\n"
	}, 10*time.Second, 10*time.Millisecond, "the line never reached the harness")

	endDeliverySession(t, dr.master, dr.done)
}
