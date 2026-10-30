//go:build unix

package session

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

// reactionNotice is the line the reaction tests deliver.
const reactionNotice = "[GIFFORD] react-p4v8 pending"

// linePaint gives a paint run the delivery harness, which reads whole
// lines and ignores the interrupt a cancel raises, so a submit, a clear
// or a cancel reaches it as a key and does not end it. Its terminal
// erases the line on a clear and flushes it on a cancel. It still
// leaves the path of its terminal in $2, and ends on __done__.
func linePaint(t *testing.T) func(*Options) {
	t.Helper()
	script := writeHarnessScript(t, "trap '' INT", `tty > "$2"`, "echo deliver-ready")
	return func(o *Options) { o.Command[0] = script }
}

// press types keys one byte at a time, each once the loop has gone
// through the one before, so the first chunk the session reads after a
// write holds that byte: the loop has seen it once the session has read
// a chunk more and the loop has caught up. Caught up alone proves
// nothing, as it holds before the byte is read at all. A sequence cut
// this way reads as the whole one: no timer ends its first part.
func press(t *testing.T, dr *deliveryRun, keys string) {
	t.Helper()
	s := dr.session
	for i := range len(keys) {
		s.mu.Lock()
		before := s.arrived
		s.mu.Unlock()
		writeKeys(t, dr.master, keys[i:i+1])
		require.Eventually(t, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			written := s.humanWritten.Load()
			return s.arrived > before && s.arrived == written && s.snap.Chunks == written
		}, 10*time.Second, time.Millisecond, "the loop never saw the key")
	}
}

// requireStaysOpen checks that the guard does not close over a few of
// the session's ticks, each of which polls it.
func requireStaysOpen(t *testing.T, s *Session, what string) {
	t.Helper()
	require.Never(t, func() bool { return s.State() == draft.StateClosed },
		3*defaultKeyFlushEvery, defaultKeyFlushEvery/5, what)
}

// TestHarnessReactionClosesTheDraft presses a submit, a clear or a
// cancel, on a draft and with none, on a measured profile with an input
// line. The key closes nothing by itself: the guard is out of CLOSED at
// once with a new version, which the recording reports even when the
// state stays, a delivery prepared before the key loses its commit, and
// neither the clock nor the screen from before the key closes it. The harness's own paint of an empty input line closes it
// with the key's outcome, only once quiet, and a delivery then goes
// through.
func TestHarnessReactionClosesTheDraft(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		draft   bool
		key     string
		pressed draft.State
		report  protocol.EventType
		event   protocol.EventType
		out     string
	}{
		{
			name: "submit on a draft", draft: true, key: "\r", pressed: draft.StateSubmitting,
			report: protocol.EventTypeInputDraftUpdated, event: protocol.EventTypeInputDraftSubmitted, out: "hello\n" + reactionNotice + "\n__done__\n",
		},
		{
			name: "clear on a draft", draft: true, key: "\x15", pressed: draft.StateOpen,
			report: protocol.EventTypeInputDraftUpdated, event: protocol.EventTypeInputDraftCleared, out: reactionNotice + "\n__done__\n",
		},
		{
			name: "cancel on a draft", draft: true, key: "\x03", pressed: draft.StateOpen,
			report: protocol.EventTypeInputDraftUpdated, event: protocol.EventTypeInputDraftCancelled, out: reactionNotice + "\n__done__\n",
		},
		{
			name: "submit with no draft", key: "\r", pressed: draft.StateSubmitting,
			report: protocol.EventTypeInputDraftUpdated, event: protocol.EventTypeInputDraftSubmitted, out: "\n" + reactionNotice + "\n__done__\n",
		},
		{
			name: "clear with no draft", key: "\x15", pressed: draft.StateOpen,
			report: protocol.EventTypeInputDraftOpened, event: protocol.EventTypeInputDraftCleared, out: reactionNotice + "\n__done__\n",
		},
		{
			name: "cancel with no draft", key: "\x03", pressed: draft.StateOpen,
			report: protocol.EventTypeInputDraftOpened, event: protocol.EventTypeInputDraftCancelled, out: reactionNotice + "\n__done__\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := paintSession(t, "react-close", measuredInput, linePaint(t))
			dr := p.deliveryRun
			s := dr.session
			p.screen(t, emptyBox)
			early := false
			if tc.draft {
				press(t, dr, "hello")
				p.screen(t, draftBox)
				require.Equal(t, draft.StateOpen, s.State())
			} else {
				require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, s, "dlv-early"))
				early = true
			}
			before := publishedSnapshot(t, s)

			press(t, dr, tc.key)
			pressed := publishedSnapshot(t, s)
			require.Equal(t, tc.pressed, pressed.State, "the key closes nothing by itself")
			require.Equal(t, uint64(1), pressed.Generation)
			require.Greater(t, pressed.Version, before.Version, "the key ends every permit")
			require.Contains(t, sinkPayloads(dr.sink, tc.report), InputDraft{
				State: string(tc.pressed), Generation: 1, GuardVersion: pressed.Version,
			}, "the recording has the key's version")
			if early {
				rep, err := s.CommitDelivery("dlv-early", reactionNotice, protocol.InjectModeTyped)
				require.NoError(t, err)
				require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
			}
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, s, "dlv-pressed"))

			p.clock.advance(time.Minute)
			requireStaysOpen(t, s, "the clock and the screen from before the key closed the draft")
			p.draw(t, emptyBox)
			requireStaysOpen(t, s, "a reaction closed the draft before it was quiet")
			require.Empty(t, sinkPayloads(dr.sink, tc.event))

			p.clock.advance(outputQuietPeriod)
			waitSnapshot(t, s, "the quiet reaction never closed the draft", func(snap guardSnapshot) bool {
				return snap.State == draft.StateClosed
			})
			closed := sinkPayloads(dr.sink, tc.event)
			require.Len(t, closed, 1)
			require.Equal(t, InputDraft{
				State: string(draft.StateClosed), Generation: 1, GuardVersion: publishedSnapshot(t, s).Version,
			}, closed[0])

			require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, s, "dlv-late"))
			rep, err := s.CommitDelivery("dlv-late", reactionNotice, protocol.InjectModeTyped)
			require.NoError(t, err)
			require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
			endDeliverySession(t, dr.master, dr.done)
			require.Equal(t, tc.out, harnessOut(t, dr.out), "the notice only after the reaction")
		})
	}
}

// TestHarnessReactionKeepsTheDraft presses a key the harness answers
// with no empty input line to read, once quiet: the draft stays open
// and no delivery goes through, however long the clock runs.
func TestHarnessReactionKeepsTheDraft(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		key   string
		react func(t *testing.T, p *paintRun)
		out   string
	}{
		{name: "the clear leaves the draft", key: "\x15", react: func(t *testing.T, p *paintRun) {
			p.draw(t, draftBox)
		}, out: "__done__\n"},
		{name: "an empty frame, then the draft", key: "\r", react: func(t *testing.T, p *paintRun) {
			p.draw(t, emptyBox)
			p.clock.advance(100 * time.Millisecond)
			p.draw(t, draftBox)
		}, out: "hello\n__done__\n"},
		{name: "a key before the reaction is quiet", key: "\x03", react: func(t *testing.T, p *paintRun) {
			p.draw(t, emptyBox)
			press(t, p.deliveryRun, "x")
		}, out: "x__done__\n"},
		{name: "a second cancel before the reaction is quiet", key: "\x03", react: func(t *testing.T, p *paintRun) {
			p.draw(t, emptyBox)
			press(t, p.deliveryRun, "\x03")
		}, out: "__done__\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := paintSession(t, "react-keep", measuredInput, linePaint(t))
			dr := p.deliveryRun
			s := dr.session
			p.screen(t, emptyBox)
			press(t, dr, "hello")
			p.screen(t, draftBox)
			press(t, dr, tc.key)

			tc.react(t, p)
			p.clock.advance(time.Minute)
			requireStaysOpen(t, s, "the draft closed with no empty input line to show for the key")
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, s, "dlv-kept"))
			require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftSubmitted))
			require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftCleared))
			require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftCancelled))
			endDeliverySession(t, dr.master, dr.done)
			require.Equal(t, tc.out, harnessOut(t, dr.out), "no notice reached the harness")
		})
	}
}

// TestResizeRestartsTheReaction resizes the screen after the harness
// answered a submit and before its answer is quiet: the submit still
// waits, the answer from before the resize no longer counts, and only
// the harness's paint after it closes the draft.
func TestResizeRestartsTheReaction(t *testing.T) {
	t.Parallel()
	p := paintSession(t, "react-resize", measuredInput, linePaint(t))
	dr := p.deliveryRun
	s := dr.session
	p.screen(t, emptyBox)
	press(t, dr, "hello")
	p.screen(t, draftBox)
	press(t, dr, "\r")

	p.draw(t, emptyBox)
	require.NoError(t, s.Resize(100, 30))
	p.clock.advance(time.Minute)
	requireStaysOpen(t, s, "a reaction from before the resize closed the draft")
	require.Equal(t, draft.StateSubmitting, s.State(), "the resize keeps the submit waiting")

	p.draw(t, emptyBox)
	p.clock.advance(outputQuietPeriod)
	waitSnapshot(t, s, "the paint after the resize never closed the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed && snap.Generation == 1
	})
	require.Len(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftSubmitted), 1)
	endDeliverySession(t, dr.master, dr.done)
	require.Equal(t, "hello\n__done__\n", harnessOut(t, dr.out))
}

// TestReadSettlesTheGuard reads the guard on a session whose ticks never
// come, once the harness answered a submit and the clock moved past the
// quiet period: every reader settles the guard first, so it finds the
// draft closed, a prepare against the closed draft goes through, and the
// recording has the submit once.
func TestReadSettlesTheGuard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		read func(t *testing.T, s *Session, before guardSnapshot)
	}{
		{name: "a prepare", read: func(t *testing.T, s *Session, before guardSnapshot) {
			// The close is the one transition the settle makes.
			rep, err := s.PrepareDelivery("dlv-read", before.Generation, before.Version+1)
			require.NoError(t, err)
			require.Equal(t, DeliveryPreparedOutcome, rep.Outcome)
		}},
		{name: "the Core's report", read: func(t *testing.T, s *Session, _ guardSnapshot) {
			require.Equal(t, protocol.DraftStateClosed, s.InputGuard().DraftState)
		}},
		{name: "the guard's state", read: func(t *testing.T, s *Session, _ guardSnapshot) {
			require.Equal(t, draft.StateClosed, s.State())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := paintSession(t, "react-read", measuredInput, linePaint(t), func(o *Options) {
				o.KeyFlushEvery = time.Hour
			})
			dr := p.deliveryRun
			s := dr.session
			p.screen(t, emptyBox)
			press(t, dr, "hello")
			p.screen(t, draftBox)
			press(t, dr, "\r")
			p.draw(t, emptyBox)
			p.clock.advance(outputQuietPeriod)
			before := publishedSnapshot(t, s)
			require.Equal(t, draft.StateSubmitting, before.State, "no tick settled the guard")

			tc.read(t, s, before)
			require.Equal(t, draft.StateClosed, publishedSnapshot(t, s).State, "the read published what it settled")
			require.Len(t, sinkPayloads(dr.sink, protocol.EventTypeInputDraftSubmitted), 1)
			endDeliverySession(t, dr.master, dr.done)
		})
	}
}

// TestReadAfterTheEndSettlesNothing ends a session on a submit the
// harness answered, quiet since, that no tick settled: its harness
// leaves on its own once told to, with no key that would end the wait.
// A read after the end settles nothing, and the recording ends on the
// terminal closing.
func TestReadAfterTheEndSettlesNothing(t *testing.T) {
	t.Parallel()
	stop := filepath.Join(t.TempDir(), "stop")
	script := filepath.Join(t.TempDir(), "leavingharness")
	body := "#!/bin/sh\nstty -icanon -echo min 1 time 0\ntty > \"$2\"\necho deliver-ready\n" +
		"while [ ! -e \"$3\" ]; do sleep 0.01; done\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	p := paintSession(t, "react-end", measuredInput, func(o *Options) {
		o.Command[0] = script
		o.Command = append(o.Command, stop)
		o.KeyFlushEvery = time.Hour
	})
	dr := p.deliveryRun
	s := dr.session
	p.screen(t, emptyBox)
	press(t, dr, "\r")
	p.draw(t, emptyBox)
	p.clock.advance(outputQuietPeriod)
	require.NoError(t, os.WriteFile(stop, nil, 0o600))
	waitDone(t, dr.done)

	events := len(dr.sink.all())
	require.Equal(t, draft.StateSubmitting, s.State())
	require.Equal(t, protocol.DraftStateSubmitting, s.InputGuard().DraftState)
	_, _ = s.Generation(), s.GuardVersion()
	all := dr.sink.all()
	require.Len(t, all, events, "a read after the end records nothing")
	require.Equal(t, protocol.EventTypePTYClosed, all[len(all)-1].Type)
}
