//go:build unix

package session

import (
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

// clearScreen homes the cursor and blanks the screen.
const clearScreen = "\x1b[H\x1b[2J"

// emptyBox is the canonical empty editable area, draftBox the same area
// with a draft. Each leaves the cursor on the input row; the reset releases
// the last rune, which the emulator otherwise holds back.
const (
	inputTopLine      = "input-top"
	inputBottomLine   = "input-bottom"
	backToInput       = "\x1b[1A\r"
	emptyBox          = inputTopLine + "\r\n> \r\n" + inputBottomLine + backToInput + "\x1b[2C\x1b[0m"
	draftBox          = inputTopLine + "\r\n> hello\r\n" + inputBottomLine + backToInput + "\x1b[7C\x1b[0m"
	multilineBlankBox = inputTopLine + "\r\n> \r\n  hidden draft\r\n" + inputBottomLine +
		"\x1b[2A\r\x1b[2C\x1b[0m"
)

// syncBegin and syncEnd open and end a synchronized update.
const (
	syncBegin = "\x1b[?2026h"
	syncEnd   = "\x1b[?2026l"
)

// placeholderHint is the input placeholder of measuredPlaceholder.
// placeholderBox is its empty input line: the hint dimmed, the cursor on
// its first column. promptBox is the prompt alone, hintBox the hint's
// text typed.
const (
	placeholderHint = "Ask the agent anything"
	placeholderBox  = inputTopLine + "\r\n› \x1b[2m" + placeholderHint + "\x1b[0m\r\n" + inputBottomLine + backToInput + "\x1b[2C"
	promptBox       = inputTopLine + "\r\n› \x1b[0m\r\n" + inputBottomLine + backToInput + "\x1b[2C\x1b[0m"
	hintBox         = inputTopLine + "\r\n› " + placeholderHint + "\x1b[0m\r\n" + inputBottomLine + backToInput + "\x1b[24C\x1b[0m"
)

const (
	museClearedFirstRow = "────────\r\n❯ \r\n  dos\r\n────────\r\nBuild · modelo" +
		"\x1b[3A\r\x1b[2C\x1b[0m"
	musePermissionDialog = "────────\r\n" +
		"Would you like to run the following command?\r\n" +
		"rm -rf /tmp/example\r\n\r\n" +
		"⟩ 1. Yes, proceed (y)\r\n" +
		"  2. No, cancel (n)\r\n" +
		"────────\r\nBuild · modelo" +
		"\x1b[4A\r\x1b[0m"
)

// placeholderYAML is a measured Codex fixture for one exact version, its
// input placeholder declared.
const placeholderYAML = `harness: codex
measured: true
version_range: "1.2.3"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^› ?(.*)$"
input_placeholder: "` + placeholderHint + `"
input_bounds:
  above: ["^input-top$"]
  below: ["^input-bottom$"]
`

const museInputYAML = `harness: muse-code
measured: true
version_range: "1.4.13-R6996.1"
submit: "\r"
insert_newline: "\u001b\r"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: false
extended_keyboard: none
input_line: "^❯ ?(.*)$"
input_bounds:
  above: ["^─+$"]
  below: ["^─+$"]
`

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
	bounded, err := profiles.ParseFile("codex", []byte(placeholderYAML))
	if err != nil {
		panic(err)
	}
	o.Profile.InputBounds = bounded.InputBounds
}

// unmeasuredInput is the same input line on a profile still unmeasured.
func unmeasuredInput(o *Options) {
	measuredInput(o)
	o.Profile.Measured = false
}

// measuredPlaceholder makes the profile the placeholder profile.
func measuredPlaceholder(o *Options) {
	p, err := profiles.ParseFile("codex", []byte(placeholderYAML))
	if err != nil {
		panic(err)
	}
	o.Profile = p
}

func measuredMuseInput(o *Options) {
	p, err := profiles.ParseFile("muse-code", []byte(museInputYAML))
	if err != nil {
		panic(err)
	}
	o.Profile = p
}

// measuredNoInput is a measured profile with no input line pattern.
func measuredNoInput(o *Options) {
	o.Profile.Measured = true
	o.Profile.InputLinePattern = ""
	o.Profile.InputLine = nil
}

// screen draws raw, then moves the clock past the output quiet period,
// so only the screen decides.
func (p *paintRun) screen(t *testing.T, raw string) {
	t.Helper()
	p.draw(t, raw)
	p.clock.advance(outputQuietPeriod)
}

// draw paints raw over a cleared screen and returns once the loop has
// folded this paint. A paint can leave the screen as it was, so the
// screen alone cannot tell it arrived: the loop has folded it once the
// session's last output is the paint's own instant, which no earlier
// paint has, and the emulator shows what raw draws on a fresh one of
// the screen's size.
func (p *paintRun) draw(t *testing.T, raw string) {
	t.Helper()
	s := p.session
	at := p.clock.Now()
	s.mu.Lock()
	last, cols, rows := s.lastOutput, s.lastSnap.Cols, s.lastSnap.Rows
	s.mu.Unlock()
	require.True(t, at.After(last), "a paint at the instant of the last output cannot be told apart")
	e, err := vt.New(cols, rows)
	require.NoError(t, err)
	defer func() { _ = e.Close() }()
	_, err = e.Write([]byte(clearScreen + raw))
	require.NoError(t, err)
	want := e.Snapshot()
	p.write(t, clearScreen+raw)
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		snap := s.lastSnap
		return s.lastOutput.Equal(at) && slices.Equal(snap.Lines, want.Lines) &&
			snap.CursorRow == want.CursorRow && snap.CursorCol == want.CursorCol &&
			snap.CursorVisible == want.CursorVisible && snap.Pending == want.Pending
	}, 10*time.Second, time.Millisecond, "the loop never folded the paint")
}

// TestDeliveryNeedsAnEmptyInputLine delivers over synthetic screens: on
// a measured profile the prepare, or the commit of a delivery prepared
// on an empty box, goes through only with the input line under the
// cursor positively empty, on a whole frame, and is refused with no
// byte written otherwise. On a placeholder profile only the dimmed
// placeholder is empty: the prompt alone proves nothing and the hint's
// text typed is a draft. A measured profile without an input line
// delivers whatever the screen; a profile not yet measured never does,
// empty box or not.
func TestDeliveryNeedsAnEmptyInputLine(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		profile func(*Options)
		// empty is the screen a delivery prepares on before its commit
		// meets screen; emptyBox when left out.
		empty  string
		screen string
		reason string
	}{
		{name: "a stable empty box", profile: measuredInput, screen: emptyBox},
		{name: "an empty box inside an open synchronized update", profile: measuredInput, screen: syncBegin + emptyBox, reason: reasonNoInputEvidence},
		{name: "an empty box once a synchronized update ended", profile: measuredInput, screen: syncBegin + emptyBox + syncEnd},
		{name: "the dimmed placeholder", profile: measuredPlaceholder, empty: placeholderBox, screen: placeholderBox},
		{name: "the prompt alone, a placeholder declared", profile: measuredPlaceholder, empty: placeholderBox, screen: promptBox, reason: reasonNoInputEvidence},
		{name: "the placeholder's text, typed", profile: measuredPlaceholder, empty: placeholderBox, screen: hintBox, reason: reasonInputNotEmpty},
		{name: "the dimmed placeholder inside an open synchronized update", profile: measuredPlaceholder, empty: placeholderBox, screen: syncBegin + placeholderBox, reason: reasonNoInputEvidence},
		{name: "a draft on the input line", profile: measuredInput, screen: draftBox, reason: reasonInputNotEmpty},
		{name: "a blank cursor row with text elsewhere in the box", profile: measuredInput, screen: multilineBlankBox, reason: reasonNoInputEvidence},
		// The emulator holds back the last rune until more output.
		{name: "a pending snapshot", profile: measuredInput, screen: "> hello", reason: reasonNoInputEvidence},
		{name: "no input line", profile: measuredInput, screen: "working\x1b[0m", reason: reasonNoInputEvidence},
		{name: "the input line away from the cursor", profile: measuredInput, screen: emptyBox + "\r\nworking\x1b[0m", reason: reasonNoInputEvidence},
		{name: "a hidden cursor", profile: measuredInput, screen: emptyBox + "\x1b[?25l", reason: reasonNoInputEvidence},
		{name: "an unmeasured profile", profile: unmeasuredInput, screen: emptyBox, reason: reasonUnmeasured},
		{name: "a measured profile without an input line", profile: measuredNoInput, screen: draftBox},
	} {
		for _, at := range []string{atPrepare, atCommit} {
			t.Run(tc.name+" at the "+at, func(t *testing.T) {
				t.Parallel()
				p := paintSession(t, "input-evidence", tc.profile)
				dr := p.deliveryRun
				if at == atCommit {
					empty := tc.empty
					if empty == "" {
						empty = emptyBox
					}
					p.screen(t, empty)
					if tc.reason == reasonUnmeasured {
						require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-1"))
						requireRefused(t, dr, tc.reason)
						return
					}
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

// TestSessionAndSocketUseTheSameInputReader checks the exact snapshot through
// the shared reader, PREPARE/COMMIT's inputEvidenceLocked and the socket plug.
// The three consumers must agree on canonical empty, hidden multiline text
// and a visible nonempty draft.
func TestSessionAndSocketUseTheSameInputReader(t *testing.T) {
	p := paintSession(t, "shared-input-reader", measuredInput)
	for _, tc := range []struct {
		name   string
		raw    string
		line   string
		reason string
	}{
		{name: "canonical empty", raw: emptyBox},
		{name: "text outside blank cursor row", raw: multilineBlankBox, reason: reasonNoInputEvidence},
		{name: "nonempty cursor row", raw: draftBox, line: "hello", reason: reasonInputNotEmpty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p.screen(t, tc.raw)
			p.session.mu.Lock()
			snap := p.session.lastSnap
			reason := p.session.inputEvidenceLocked()
			p.session.mu.Unlock()
			line, ok := draft.CursorInput(p.session.opts.Profile, snap)
			wantOK := tc.reason != reasonNoInputEvidence
			require.Equal(t, tc.reason, reason)
			require.Equal(t, tc.line, line)
			require.Equal(t, wantOK, ok)
			require.Equal(t, wantOK, p.session.SocketDelivery().InputHolds(tc.line))
		})
	}
	endRawSession(t, p.deliveryRun)
}

// TestMuseClearedFirstRowKeepsTheMultilineDraft follows the measured Muse
// keys: type uno, Alt+Enter, type dos, move up, then Ctrl+U. Muse draws an
// empty prompt row with dos still below it. The guard settlement, delivery
// evidence check and socket plug all fail closed on that same screen.
func TestMuseClearedFirstRowKeepsTheMultilineDraft(t *testing.T) {
	t.Parallel()
	p := paintSession(t, "muse-multiline-clear", measuredMuseInput)
	dr := p.deliveryRun
	const (
		keys        = "uno\x1b\rdos\x1b[A\x15"
		harnessKeys = "uno\x1b\ndos\x1b[A\x15"
	)
	writeKeys(t, dr.master, keys)
	waitHarness(t, dr, harnessKeys, "the Muse key sequence never reached the harness")
	waitSnapshot(t, dr.session, "the Muse key sequence never opened a draft", func(snap guardSnapshot) bool {
		return snap.State == draft.StateOpen
	})

	p.screen(t, museClearedFirstRow)
	require.Equal(t, draft.StateOpen, dr.session.State(), "settlement must not close the multiline draft")
	dr.session.mu.Lock()
	snap := dr.session.lastSnap
	reason := dr.session.inputEvidenceLocked()
	dr.session.mu.Unlock()
	text, ok := draft.CursorInput(dr.session.opts.Profile, snap)
	require.False(t, ok)
	require.Empty(t, text)
	require.Equal(t, reasonNoInputEvidence, reason)
	require.False(t, dr.session.SocketDelivery().InputHolds(""))
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-muse"))
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))

	endRawSession(t, dr)
	require.Equal(t, harnessKeys, harnessOut(t, dr.out), "only the human's Muse keys reached the harness")
}

// TestMusePermissionDialogIsNotAnEmptyEditableArea keeps the permission
// choice untouched. Its rules and blank-looking row do not have the declared
// nearest-row shape of the editable area, so no reader permits injection.
func TestMusePermissionDialogIsNotAnEmptyEditableArea(t *testing.T) {
	t.Parallel()
	p := paintSession(t, "muse-permission", measuredMuseInput)
	dr := p.deliveryRun
	p.screen(t, musePermissionDialog)

	dr.session.mu.Lock()
	snap := dr.session.lastSnap
	reason := dr.session.inputEvidenceLocked()
	dr.session.mu.Unlock()
	text, ok := draft.CursorInput(dr.session.opts.Profile, snap)
	require.False(t, ok)
	require.Empty(t, text)
	require.Equal(t, reasonNoInputEvidence, reason)
	require.False(t, dr.session.SocketDelivery().InputHolds(""))
	require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-permission"))
	require.Equal(t, []string{reasonNoInputEvidence}, abortReasons(dr))
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))

	endRawSession(t, dr)
	require.Empty(t, harnessOut(t, dr.out), "the permission dialog was never answered")
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
