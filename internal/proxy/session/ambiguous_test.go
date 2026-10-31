//go:build unix

package session

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/screenproj"
)

// ambiguousNotice is the line the ambiguous key tests deliver.
const ambiguousNotice = "[GIFFORD] ambig-k7r2 pending"

// arrowUp is the arrow up key as a terminal sends it.
const arrowUp = "\x1b[A"

// csiUKeys makes the profile one that asked for CSI u.
func csiUKeys(o *Options) { o.Profile.ExtendedKeyboard = profiles.ExtendedCSIU }

// sessionAmbiguousKey is a human key a guard with no draft open cannot
// tell typed nothing, pressed in parts the session's clock moves gap
// between, and the text a harness may draw on its input line for it.
type sessionAmbiguousKey struct {
	name  string
	parts []string
	gap   time.Duration
	edits []func(*Options)
	text  string
}

// sessionAmbiguousKeys are a letter after a lone ESC, three seconds
// apart, which the parser joins into Alt+letter while a harness that
// times its ESC out types the letter; a letter in CSI u; the arrows up
// and down, which may bring back an earlier prompt; a Tab, which may
// complete one; and a sequence the parser does not recognise, which a
// harness may type in part. What each harness does is not claimed: the
// synthetic harness draws the text.
var sessionAmbiguousKeys = []sessionAmbiguousKey{
	{name: "a letter after a lone ESC", parts: []string{"\x1b", "a"}, gap: 3 * time.Second, text: "a"},
	{name: "a letter in CSI u", parts: []string{"\x1b[97u"}, edits: []func(*Options){csiUKeys}, text: "a"},
	{name: "arrow up", parts: []string{arrowUp}, text: "an earlier prompt"},
	{name: "arrow down", parts: []string{"\x1b[B"}, text: "a later prompt"},
	{name: "a Tab", parts: []string{"\t"}, text: "a completed prompt"},
	{name: "an unknown sequence", parts: []string{"\x1b[5x"}, text: "[5x"},
}

// closedAfterWarmup runs a paint session on a measured profile through
// a submit the screen confirmed: the guard is CLOSED by a genuine
// outcome, not by its initial state.
func closedAfterWarmup(t *testing.T, id string, edits ...func(*Options)) *paintRun {
	t.Helper()
	p := paintSession(t, id, append([]func(*Options){measuredInput}, edits...)...)
	p.screen(t, emptyBox)
	press(t, p.deliveryRun, "warmup\r")
	p.clock.advance(time.Millisecond)
	p.screen(t, emptyBox)
	require.Equal(t, protocol.DraftStateClosed, p.session.InputGuard().DraftState)
	require.NotEmpty(t, sinkPayloads(p.sink, protocol.EventTypeInputDraftSubmitted), "a genuine closed event")
	return p
}

// pressParts presses k's parts, moving the session's clock gap between
// them.
func pressParts(t *testing.T, p *paintRun, k sessionAmbiguousKey) {
	t.Helper()
	for i, part := range k.parts {
		if i > 0 {
			p.clock.advance(k.gap)
		}
		press(t, p.deliveryRun, part)
	}
}

// recordedDraft is the draft state the session's recording reports
// last, the one a replay reads, starting UNKNOWN.
func recordedDraft(p *paintRun) protocol.DraftState { return lastDraft(p.sink) }

// lastDraft is the draft state sink holds last, starting UNKNOWN.
func lastDraft(sink *chanSink) protocol.DraftState {
	recorded := protocol.DraftStateUnknown
	for _, e := range sink.all() {
		if d, ok := e.Payload.(InputDraft); ok {
			recorded = draftState(draft.State(d.State))
		}
	}
	return recorded
}

// projected projects the session's last screen under the draft state
// its recording reports last, with the real redactor and no mask, as the
// dataset build and the Analyst project a screen.
func projected(t *testing.T, p *paintRun) string {
	t.Helper()
	return projectedUnder(t, p.session, recordedDraft(p))
}

// projectedUnder projects s's last screen under state, as projected
// does.
func projectedUnder(t *testing.T, s *Session, state protocol.DraftState) string {
	t.Helper()
	s.mu.Lock()
	snap := s.lastSnap
	s.mu.Unlock()
	r, err := redact.New(redact.Options{})
	require.NoError(t, err)
	out, err := screenproj.Project(screenproj.Input{
		Rows: snap.Lines, Cols: snap.Cols, Draft: state,
		Cursor: screenproj.Cursor{Row: snap.CursorRow, Known: true, Visible: snap.CursorVisible},
	}, screenproj.Policies{Lines: 10, Redactor: r, Mask: screenproj.NoMask()})
	require.NoError(t, err)
	return strings.Join(out.Lines, "\n")
}

// TestEscapeThenLetterNeverReachesTheProjection presses a lone ESC, then
// a letter three seconds later, on a draft the screen confirmed closed:
// the parser joins the two into Alt+a, and the synthetic harness draws
// the letter, as one that timed its ESC out would. The delivery is
// refused, and the recording does not report the draft closed, so the
// projection keeps the human's letter out.
func TestEscapeThenLetterNeverReachesTheProjection(t *testing.T) {
	t.Parallel()
	p := closedAfterWarmup(t, "ambig-escape")
	defer endRawSession(t, p.deliveryRun)

	press(t, p.deliveryRun, "\x1b")
	p.clock.advance(3 * time.Second)
	press(t, p.deliveryRun, "a")
	p.clock.advance(time.Millisecond)
	p.screen(t, "> a\x1b[0m")
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, p.session, "dlv-escape"))
	recorded := recordedDraft(p)
	require.NotContains(t, projected(t, p), "> a", "a human draft must not reach the projection; recorded draft=%s", recorded)
}

// TestAmbiguousKeyKeepsTheDraftOutOfTheProjection presses each key a
// CLOSED guard cannot tell typed nothing, then has the synthetic harness
// draw text on its input line for it. The recording reports a draft
// opened at the key itself, before the harness draws, and never closed
// after, so a delivery is refused and the projection holds none of the
// text.
func TestAmbiguousKeyKeepsTheDraftOutOfTheProjection(t *testing.T) {
	t.Parallel()
	for _, k := range sessionAmbiguousKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			p := closedAfterWarmup(t, "ambig-text", k.edits...)
			defer endRawSession(t, p.deliveryRun)

			pressParts(t, p, k)
			opened := sinkPayloads(p.sink, protocol.EventTypeInputDraftOpened)
			require.NotEmpty(t, opened, "the key opened a draft")
			require.Equal(t, string(draft.StateOpen), opened[len(opened)-1].(InputDraft).State)
			require.Equal(t, uint64(2), opened[len(opened)-1].(InputDraft).Generation, "a fresh draft, before the harness draws")
			require.Equal(t, protocol.DraftStateOpen, recordedDraft(p))

			p.clock.advance(time.Millisecond)
			p.screen(t, "> "+k.text+"\x1b[0m")
			p.clock.advance(time.Minute)
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, p.session, "dlv-ambiguous"))
			require.NotEqual(t, protocol.DraftStateClosed, recordedDraft(p))
			require.Equal(t, protocol.DraftStateOpen, p.session.InputGuard().DraftState)
			require.NotContains(t, projected(t, p), k.text, "the text the key may have left must not reach the projection")
		})
	}
}

// TestAmbiguousKeyClosesOnAnEmptyLine presses the same keys, then has the
// harness draw nothing for a minute, and then an empty input line: the
// draft stays open with no reaction to read, and closes, as cleared and
// once, only once the empty line drawn after the key has been quiet. A
// delivery then goes through.
func TestAmbiguousKeyClosesOnAnEmptyLine(t *testing.T) {
	t.Parallel()
	for _, k := range sessionAmbiguousKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			p := closedAfterWarmup(t, "ambig-empty", k.edits...)
			s := p.session
			defer endRawSession(t, p.deliveryRun)

			pressParts(t, p, k)
			p.clock.advance(time.Minute)
			requireStaysOpen(t, s, "the clock closed the draft the key may have left")
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, s, "dlv-unseen"))

			p.draw(t, emptyBox)
			requireStaysOpen(t, s, "a reaction closed the draft before it was quiet")
			p.clock.advance(outputQuietPeriod)
			waitSnapshot(t, s, "the quiet empty line never closed the draft", func(snap guardSnapshot) bool {
				return snap.State == draft.StateClosed
			})
			cleared := sinkPayloads(p.sink, protocol.EventTypeInputDraftCleared)
			require.Len(t, cleared, 1)
			require.Equal(t, uint64(2), cleared[0].(InputDraft).Generation)
			require.Equal(t, protocol.DraftStateClosed, recordedDraft(p))

			require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, s, "dlv-seen"))
			rep, err := s.CommitDelivery("dlv-seen", ambiguousNotice, protocol.InjectModeTyped)
			require.NoError(t, err)
			require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
			// The harness's terminal reads a carriage return as a newline.
			waitHarness(t, p.deliveryRun, "warmup\n"+strings.Join(k.parts, "")+ambiguousNotice+"\n",
				"the notice never followed the human's keys")
		})
	}
}

// TestAmbiguousKeyClosesOnALaterEmptyLine presses each key a CLOSED
// guard cannot tell typed nothing, and has the harness answer with text
// first, quiet, and an empty input line only later, as one that brings
// a prompt back and then clears it. While the text shows, the draft
// stays open, a delivery is refused and the projection holds none of
// it. The empty line drawn after the text closes the draft, as cleared
// and once, under the key's generation, and a delivery then goes
// through.
func TestAmbiguousKeyClosesOnALaterEmptyLine(t *testing.T) {
	t.Parallel()
	for _, k := range sessionAmbiguousKeys {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()
			p := closedAfterWarmup(t, "ambig-later", k.edits...)
			s := p.session
			defer endRawSession(t, p.deliveryRun)

			pressParts(t, p, k)
			p.clock.advance(time.Millisecond)
			p.screen(t, "> "+k.text+"\x1b[0m")
			p.clock.advance(time.Minute)
			requireStaysOpen(t, s, "the text closed the draft")
			require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, s, "dlv-text-first"))
			require.NotContains(t, projected(t, p), k.text, "the text the key may have left must not reach the projection")

			p.draw(t, emptyBox)
			requireStaysOpen(t, s, "the empty line closed the draft before it was quiet")
			p.clock.advance(outputQuietPeriod)
			waitSnapshot(t, s, "the empty line after the text never closed the draft", func(snap guardSnapshot) bool {
				return snap.State == draft.StateClosed
			})
			cleared := sinkPayloads(p.sink, protocol.EventTypeInputDraftCleared)
			require.Len(t, cleared, 1)
			require.Equal(t, uint64(2), cleared[0].(InputDraft).Generation, "the draft the key opened")
			require.Equal(t, protocol.DraftStateClosed, recordedDraft(p))

			require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, s, "dlv-later-empty"))
			rep, err := s.CommitDelivery("dlv-later-empty", ambiguousNotice, protocol.InjectModeTyped)
			require.NoError(t, err)
			require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
			waitHarness(t, p.deliveryRun, "warmup\n"+strings.Join(k.parts, "")+ambiguousNotice+"\n",
				"the notice never followed the human's keys")
		})
	}
}

// TestPendingEscapeNeverReachesTheProjection presses a lone ESC on a
// draft the screen confirmed closed and leaves it pending: the parser
// gives no key yet, while the synthetic harness answers the ESC alone by
// drawing a draft back, as one that restores it might. The delivery is
// refused and the output chunk stays deferred until the bytes are classified,
// so replay never observes the new screen under a stale CLOSED state.
func TestPendingEscapeNeverReachesTheProjection(t *testing.T) {
	t.Parallel()
	p := closedAfterWarmup(t, "ambig-pending")
	defer endRawSession(t, p.deliveryRun)
	// Count only after the startup output has crossed the ticker boundary.
	p.clock.advance(p.session.opts.OutFlushEvery)
	require.Eventually(t, func() bool {
		p.session.mu.Lock()
		defer p.session.mu.Unlock()
		return p.session.buffered == 0 && p.session.deferred == nil && !p.session.deferredFlush
	}, 10*time.Second, time.Millisecond, "warmup output never flushed")
	chunks := len(sinkPayloads(p.sink, protocol.EventTypeIOChunk))
	press(t, p.deliveryRun, "\x1b")
	require.True(t, publishedSnapshot(t, p.session).Pending)
	p.clock.advance(3 * time.Second)
	p.screen(t, "> restored-human-draft\x1b[0m")
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, p.session, "dlv-pending-escape"))
	// Exercise the ticker's flush deterministically while the ESC is unresolved.
	p.session.Flush()
	require.Len(t, sinkPayloads(p.sink, protocol.EventTypeIOChunk), chunks, "output crossing unclassified input must stay deferred")
	press(t, p.deliveryRun, "a")
	require.NotContains(t, projected(t, p), "restored-human-draft", "the human event must precede the deferred output chunk")
}

// TestHeldEscapeClosesOnceWholeAndEmpty presses a lone ESC on a draft
// the screen confirmed closed. The partial byte suspends settlement and
// blocks delivery without opening a speculative draft. Once the next byte
// proves Alt+a, the human event opens the draft before deferred output is
// recorded. The empty line from before that key closes nothing; only one
// drawn after it does, as cleared and once, and a delivery then goes through.
func TestHeldEscapeClosesOnceWholeAndEmpty(t *testing.T) {
	t.Parallel()
	p := closedAfterWarmup(t, "ambig-held")
	s := p.session
	defer endRawSession(t, p.deliveryRun)

	press(t, p.deliveryRun, "\x1b")
	held := publishedSnapshot(t, s)
	require.True(t, held.Pending)
	require.Equal(t, draft.StateClosed, held.State, "transport uncertainty is not human evidence")
	require.Equal(t, protocol.DraftStateClosed, recordedDraft(p))
	p.clock.advance(time.Millisecond)
	p.screen(t, emptyBox)
	p.clock.advance(time.Minute)
	require.Equal(t, draft.StateClosed, publishedSnapshot(t, s).State)
	require.True(t, publishedSnapshot(t, s).Pending)

	press(t, p.deliveryRun, "a")
	opened := publishedSnapshot(t, s)
	require.False(t, opened.Pending)
	require.Equal(t, draft.StateOpen, opened.State)
	p.clock.advance(time.Minute)
	requireStaysOpen(t, s, "the empty line from before the key closed the draft")
	p.draw(t, emptyBox)
	p.clock.advance(outputQuietPeriod)
	waitSnapshot(t, s, "the quiet empty line never closed the draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateClosed
	})
	cleared := sinkPayloads(p.sink, protocol.EventTypeInputDraftCleared)
	require.Len(t, cleared, 1)
	require.Equal(t, opened.Generation, cleared[0].(InputDraft).Generation, "the draft the resolved key opened")
	require.Equal(t, protocol.DraftStateClosed, recordedDraft(p))

	require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, s, "dlv-held"))
	rep, err := s.CommitDelivery("dlv-held", ambiguousNotice, protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	waitHarness(t, p.deliveryRun, "warmup\n\x1ba"+ambiguousNotice+"\n", "the notice never followed the human's keys")
}
