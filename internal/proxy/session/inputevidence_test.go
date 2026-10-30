//go:build unix

package session

import (
	"regexp"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

// clearScreen homes the cursor and blanks the screen.
const clearScreen = "\x1b[H\x1b[2J"

// emptyBox is an empty input line under the cursor, draftBox the same
// line holding a draft. The reset releases the last rune, which the
// emulator otherwise holds back.
const (
	emptyBox = "> \x1b[0m"
	draftBox = "> hello\x1b[0m"
)

// Where a delivery meets the screen under test.
const (
	atPrepare = "prepare"
	atCommit  = "commit"
)

// measuredInput makes the profile measured, with an input line read
// with or without the blank after its prompt: the screen drops
// trailing blanks from an empty one.
func measuredInput(o *Options) {
	o.Profile.Measured = true
	o.Profile.InputLinePattern = `^> ?(.*)$`
	o.Profile.InputLine = regexp.MustCompile(o.Profile.InputLinePattern)
}

// unmeasuredInput is the same input line on a profile still unmeasured.
func unmeasuredInput(o *Options) {
	measuredInput(o)
	o.Profile.Measured = false
}

// measuredNoInput is a measured profile with no input line pattern.
func measuredNoInput(o *Options) {
	o.Profile.Measured = true
	o.Profile.InputLinePattern = ""
	o.Profile.InputLine = nil
}

// screen draws raw over a cleared screen and returns once the
// emulator shows what raw draws on a fresh one, then moves the clock
// past the output quiet period, so only the screen decides.
func (p *paintRun) screen(t *testing.T, raw string) {
	t.Helper()
	e, err := vt.New(80, 24) // testPTY's size
	require.NoError(t, err)
	_, err = e.Write([]byte(clearScreen + raw))
	require.NoError(t, err)
	want := e.Snapshot()
	p.write(t, clearScreen+raw)
	waitScreen(t, p.session, "the loop never drew the screen", func(snap vt.Snapshot) bool {
		return slices.Equal(snap.Lines, want.Lines) && snap.CursorRow == want.CursorRow &&
			snap.CursorCol == want.CursorCol && snap.CursorVisible == want.CursorVisible &&
			snap.Pending == want.Pending
	})
	p.clock.advance(outputQuietPeriod)
}

// TestDeliveryNeedsAnEmptyInputLine delivers over synthetic screens: on
// a measured profile the prepare, or the commit of a delivery prepared
// on an empty box, goes through only with the input line under the
// cursor positively empty, and is refused with no byte written
// otherwise. Any other profile delivers as before, whatever the screen.
func TestDeliveryNeedsAnEmptyInputLine(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		profile func(*Options)
		screen  string
		reason  string
	}{
		{name: "a stable empty box", profile: measuredInput, screen: emptyBox},
		{name: "a draft on the input line", profile: measuredInput, screen: draftBox, reason: reasonInputNotEmpty},
		// The emulator holds back the last rune until more output.
		{name: "a pending snapshot", profile: measuredInput, screen: "> hello", reason: reasonNoInputEvidence},
		{name: "no input line", profile: measuredInput, screen: "working\x1b[0m", reason: reasonNoInputEvidence},
		{name: "the input line away from the cursor", profile: measuredInput, screen: emptyBox + "\r\nworking\x1b[0m", reason: reasonNoInputEvidence},
		{name: "a hidden cursor", profile: measuredInput, screen: emptyBox + "\x1b[?25l", reason: reasonNoInputEvidence},
		{name: "an unmeasured profile", profile: unmeasuredInput, screen: draftBox},
		{name: "a measured profile without an input line", profile: measuredNoInput, screen: draftBox},
	} {
		for _, at := range []string{atPrepare, atCommit} {
			t.Run(tc.name+" at the "+at, func(t *testing.T) {
				t.Parallel()
				p := paintSession(t, "input-evidence", tc.profile)
				dr := p.deliveryRun
				if at == atCommit {
					p.screen(t, emptyBox)
					require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, dr.session, "dlv-1"))
				}
				p.screen(t, tc.screen)
				if at == atPrepare {
					want := DeliveryPreparedOutcome
					if tc.reason != "" {
						want = DeliveryAbortedHumanPriorityOutcome
					}
					require.Equal(t, want, prepareNow(t, dr.session, "dlv-1"))
				}
				if tc.reason != "" && at == atPrepare {
					requireRefused(t, dr, tc.reason)
					return
				}
				rep, err := dr.session.CommitDelivery("dlv-1", quietNotice, protocol.InjectModeTyped)
				require.NoError(t, err)
				if tc.reason != "" {
					require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
					require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryRevalidated), "no lease taken")
					requireRefused(t, dr, tc.reason)
					return
				}
				require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
				waitHarness(t, dr, quietNotice+"\n", "the notice never reached the harness")
				require.Empty(t, abortReasons(dr))
				endRawSession(t, dr)
			})
		}
	}
}

// requireRefused checks that the one delivery was refused for reason,
// ends the run and checks that no byte reached the harness.
func requireRefused(t *testing.T, dr *deliveryRun, reason string) {
	t.Helper()
	require.Equal(t, []string{reason}, abortReasons(dr))
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))
	endRawSession(t, dr)
	require.Empty(t, harnessOut(t, dr.out), "no byte reached the harness")
}

// TestDraftBackAfterATornFrame types a draft and submits it, and the
// harness draws a torn frame, its prompt redrawn before the text: the
// guard confirms the submit on it and closes. The draft comes back on
// the next frame, and the guard, CLOSED, never reads the screen again.
// On a measured profile the prepare, or the commit of a delivery
// prepared on the torn frame, is refused and writes no byte.
func TestDraftBackAfterATornFrame(t *testing.T) {
	t.Parallel()
	for _, at := range []string{atPrepare, atCommit} {
		t.Run("at the "+at, func(t *testing.T) {
			t.Parallel()
			p := paintSession(t, "torn-frame", measuredInput)
			dr := p.deliveryRun
			p.screen(t, emptyBox)
			writeKeys(t, dr.master, "hello")
			waitSnapshot(t, dr.session, "the keys never opened a draft", func(snap guardSnapshot) bool {
				return snap.State == draft.StateOpen
			})
			p.screen(t, draftBox)
			writeKeys(t, dr.master, "\r")
			waitSnapshot(t, dr.session, "the submit never reached the guard", func(snap guardSnapshot) bool {
				return snap.State == draft.StateSubmitting
			})
			p.screen(t, emptyBox)
			waitSnapshot(t, dr.session, "the guard never confirmed the submit", func(snap guardSnapshot) bool {
				return snap.State == draft.StateClosed
			})
			closed := publishedSnapshot(t, dr.session)
			if at == atCommit {
				rep, err := dr.session.PrepareDelivery("dlv-1", closed.Generation, closed.Version)
				require.NoError(t, err)
				require.Equal(t, DeliveryPreparedOutcome, rep.Outcome, "the torn frame reads as an empty box")
			}

			p.screen(t, draftBox)
			back := publishedSnapshot(t, dr.session)
			require.Equal(t, draft.StateClosed, back.State, "CLOSED never reads the screen again")
			require.Equal(t, closed.Version, back.Version)
			require.Equal(t, closed.Generation, back.Generation)
			if at == atPrepare {
				rep, err := dr.session.PrepareDelivery("dlv-1", back.Generation, back.Version)
				require.NoError(t, err)
				require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
			} else {
				rep, err := dr.session.CommitDelivery("dlv-1", quietNotice, protocol.InjectModeTyped)
				require.NoError(t, err)
				require.Equal(t, DeliveryAbortedHumanPriorityOutcome, rep.Outcome)
				require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryRevalidated), "no lease taken")
			}
			require.Equal(t, []string{reasonInputNotEmpty}, abortReasons(dr))
			require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))
			endRawSession(t, dr)
			require.Equal(t, "hello\n", harnessOut(t, dr.out), "the human's keys only")
		})
	}
}
