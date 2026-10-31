//go:build unix

package session

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

var recognizedTerminalReports = []string{
	"\x1bP>|tmux 3.4\x1b\\",
	"\x1b]11;rgb:1111/2222/3333\a",
}

func TestSplitTerminalReportPreservesGuard(t *testing.T) {
	r := startGuardSession(t, "split-terminal-report")
	defer endDeliverySession(t, r.master, r.done)
	before := r.session.InputGuard()
	writeKeys(t, r.master, "\x1bP>|tmux")
	waitSnapshot(t, r.session, "report prefix not folded", func(s guardSnapshot) bool { return s.Pending })
	chunks := publishedSnapshot(t, r.session).Chunks
	writeKeys(t, r.master, " 3.4\x1b\\")
	waitSnapshot(t, r.session, "report suffix not folded", func(s guardSnapshot) bool {
		return s.Chunks > chunks && !s.Pending
	})
	require.Equal(t, before, r.session.InputGuard(), "input chunking must not turn a terminal report into human evidence")
}

func TestUnknownDCSDoesNotHideHumanKeys(t *testing.T) {
	r := startGuardSession(t, "unknown-dcs-human")
	defer endDeliverySession(t, r.master, r.done)
	chunks := publishedSnapshot(t, r.session).Chunks
	writeKeys(t, r.master, "\x1bPtyped-draft\x1b\\")
	waitSnapshot(t, r.session, "input not folded", func(s guardSnapshot) bool { return s.Chunks > chunks })
	require.NotEqual(t, protocol.DraftStateClosed, r.session.InputGuard().DraftState,
		"an unrecognised DCS envelope is human Alt+P and typed text")
}

func TestUnknownTerminalStringsStayHumanAtSession(t *testing.T) {
	for name, sequence := range map[string]string{
		"DCS Alt+P": "\x1bPtyped-draft\x1b\\",
		"OSC Alt+]": "\x1b]typed-draft\x1b\\",
		"APC Alt+_": "\x1b_Gpayload\x1b\\",
	} {
		t.Run(name, func(t *testing.T) {
			r := startGuardSession(t, "unknown-terminal-human")
			defer endDeliverySession(t, r.master, r.done)
			chunks := publishedSnapshot(t, r.session).Chunks
			writeKeys(t, r.master, sequence)
			waitSnapshot(t, r.session, "input not folded", func(s guardSnapshot) bool {
				return s.Chunks > chunks && !s.Pending
			})
			guard := r.session.InputGuard()
			require.NotEqual(t, protocol.DraftStateClosed, guard.DraftState)
			require.False(t, guard.LastHumanKeyAt.IsZero())
			require.NotEmpty(t, sinkPayloads(r.sink, protocol.EventTypeInputControlKey))
		})
	}
}

func TestEscThenPTextAndSubmitNotRetained(t *testing.T) {
	r := startGuardSession(t, "esc-p-text-submit")
	defer endDeliverySession(t, r.master, r.done)
	for _, chunk := range []string{"\x1b", "P", "uedes", " seguir", "\r"} {
		previous := publishedSnapshot(t, r.session).Chunks
		writeKeys(t, r.master, chunk)
		waitSnapshot(t, r.session, "input chunk not folded", func(s guardSnapshot) bool {
			return s.Chunks > previous
		})
	}
	after := publishedSnapshot(t, r.session)
	require.False(t, after.Pending, "ordinary P-prefixed text and Enter must resolve during the session")
	require.NotEmpty(t, sinkPayloads(r.sink, protocol.EventTypeInputSubmitDetected), "the human Enter must reach the guard")
}

func TestTerminalReportsPreserveClosedAndOpenGuardAtEveryCut(t *testing.T) {
	for _, open := range []bool{false, true} {
		name := "closed"
		if open {
			name = "open"
		}
		t.Run(name, func(t *testing.T) {
			r := startGuardSession(t, "terminal-cuts-"+name)
			defer endDeliverySession(t, r.master, r.done)
			if open {
				chunks := publishedSnapshot(t, r.session).Chunks
				writeKeys(t, r.master, "h")
				waitSnapshot(t, r.session, "human key not folded", func(s guardSnapshot) bool { return s.Chunks > chunks })
			}
			baseline := r.session.InputGuard()
			for reportIndex, report := range recognizedTerminalReports {
				for cut := 1; cut < len(report); cut++ {
					chunks := publishedSnapshot(t, r.session).Chunks
					writeKeys(t, r.master, report[:cut])
					waitSnapshot(t, r.session, "report prefix not folded", func(s guardSnapshot) bool {
						return s.Chunks > chunks && s.Pending
					})
					require.Equal(t, baseline, r.session.InputGuard(), "report %d cut %d prefix", reportIndex, cut)

					chunks = publishedSnapshot(t, r.session).Chunks
					writeKeys(t, r.master, report[cut:])
					waitSnapshot(t, r.session, "report suffix not folded", func(s guardSnapshot) bool {
						return s.Chunks > chunks && !s.Pending
					})
					require.Equal(t, baseline, r.session.InputGuard(), "report %d cut %d complete", reportIndex, cut)
				}
			}
		})
	}
}

func TestTerminalReportBetweenHumanTextPreservesBothKeys(t *testing.T) {
	r := startGuardSession(t, "terminal-between-text")
	defer endDeliverySession(t, r.master, r.done)
	chunks := publishedSnapshot(t, r.session).Chunks
	writeKeys(t, r.master, "a"+recognizedTerminalReports[0]+"b\r")
	waitSnapshot(t, r.session, "mixed input not folded", func(s guardSnapshot) bool {
		return s.Chunks > chunks && !s.Pending
	})
	require.NotEqual(t, protocol.DraftStateClosed, r.session.InputGuard().DraftState)
	require.NotEmpty(t, sinkPayloads(r.sink, protocol.EventTypeInputSubmitDetected))
	r.session.mu.Lock()
	printable := r.session.printable
	r.session.mu.Unlock()
	require.Equal(t, int64(0), printable, "submit flushes the two surrounding printable keys")
	keyChunks := sinkPayloads(r.sink, protocol.EventTypeInputKeyChunk)
	require.NotEmpty(t, keyChunks)
	require.Equal(t, int64(2), keyChunks[len(keyChunks)-1].(InputKeyChunk).PrintableCount)
}

func TestOutputCrossingTerminalReportIsNotHumanEvidence(t *testing.T) {
	for _, open := range []bool{false, true} {
		name := fmt.Sprintf("open=%t", open)
		t.Run(name, func(t *testing.T) {
			o := newOrderRun(t, "terminal-output-order")
			o.closed(t)
			if open {
				o.typed(t, "h")
			}
			before := o.s.InputGuard()
			drafts := len(sinkPayloads(o.sink, protocol.EventTypeInputDraftOpened)) +
				len(sinkPayloads(o.sink, protocol.EventTypeInputDraftUnknown))
			chunks := len(sinkPayloads(o.sink, protocol.EventTypeIOChunk))

			input := o.answered(t, recognizedTerminalReports[0], "agent output\r\n"+emptyBox)
			require.True(t, publishedSnapshot(t, o.s).Pending)
			require.Equal(t, before, o.s.InputGuard())
			require.Len(t, sinkPayloads(o.sink, protocol.EventTypeIOChunk), chunks, "output waits for classification")

			o.s.onInput(input)
			require.False(t, publishedSnapshot(t, o.s).Pending)
			require.Equal(t, before, o.s.InputGuard())
			afterDrafts := len(sinkPayloads(o.sink, protocol.EventTypeInputDraftOpened)) +
				len(sinkPayloads(o.sink, protocol.EventTypeInputDraftUnknown))
			require.Equal(t, drafts, afterDrafts)
			require.Greater(t, len(sinkPayloads(o.sink, protocol.EventTypeIOChunk)), chunks)
		})
	}
}
