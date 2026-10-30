//go:build unix

package fuzz

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/proxy/session"
)

// event is one session event with only what the oracle reads.
func event(typ protocol.EventType, payload any) session.Event {
	return session.Event{Type: typ, Payload: payload}
}

// draftEvent is one draft transition landing in state.
func draftEvent(typ protocol.EventType, state draft.State) session.Event {
	return event(typ, session.InputDraft{State: string(state)})
}

// firstTry is the id of the first attempt on notice 0.
const firstTry protocol.OpaqueID = "n0-a1"

// started is a delivery that started writing.
var started = event(protocol.EventTypeInputDeliveryStarted, session.DeliveryStarted{DeliveryID: firstTry})

// TestOracleFindsACommitWithTheDraftOpen follows the draft through the
// events: a delivery that starts with it open, pasting, submitting or
// unknown is one too many; with it closed, from the start or after a
// submit, cancel or clear, none is.
func TestOracleFindsACommitWithTheDraftOpen(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		events []session.Event
		open   int
	}{
		{"closed from the start", []session.Event{started}, 0},
		{"open", []session.Event{draftEvent(protocol.EventTypeInputDraftOpened, draft.StateOpen), started}, 1},
		{"pasting", []session.Event{draftEvent(protocol.EventTypeInputDraftOpened, draft.StatePasting), started}, 1},
		{"submitting", []session.Event{
			draftEvent(protocol.EventTypeInputDraftOpened, draft.StateOpen),
			draftEvent(protocol.EventTypeInputDraftUpdated, draft.StateSubmitting), started,
		}, 1},
		{"unknown", []session.Event{draftEvent(protocol.EventTypeInputDraftUnknown, draft.StateUnknown), started}, 1},
		{"submitted", []session.Event{
			draftEvent(protocol.EventTypeInputDraftOpened, draft.StateOpen),
			draftEvent(protocol.EventTypeInputDraftSubmitted, draft.StateClosed), started,
		}, 0},
		{"cancelled", []session.Event{
			draftEvent(protocol.EventTypeInputDraftOpened, draft.StateOpen),
			draftEvent(protocol.EventTypeInputDraftCancelled, draft.StateClosed), started,
		}, 0},
		{"cleared", []session.Event{
			draftEvent(protocol.EventTypeInputDraftOpened, draft.StateOpen),
			draftEvent(protocol.EventTypeInputDraftCleared, draft.StateClosed), started,
		}, 0},
		{"unknown then submitted", []session.Event{
			draftEvent(protocol.EventTypeInputDraftUnknown, draft.StateUnknown),
			draftEvent(protocol.EventTypeInputDraftUpdated, draft.StateClosed), started,
		}, 0},
		{"open again", []session.Event{
			started,
			draftEvent(protocol.EventTypeInputDraftOpened, draft.StateOpen),
			event(protocol.EventTypeInputDeliveryCommitted, session.DeliveryCommitted{DeliveryID: firstTry}),
			started,
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			commits, open, detail := checkCommits(tc.events)
			want := 0
			for _, e := range tc.events {
				if e.Type == protocol.EventTypeInputDeliveryStarted {
					want++
				}
			}
			require.Equal(t, want, commits)
			require.Equal(t, tc.open, open)
			require.Len(t, detail, tc.open)
		})
	}
}

// judged runs the verdict on a scenario whose human typed "hi" and
// Enter, with one notice, the harness record got, and what the injector
// and the session said.
func judged(t *testing.T, got []Record, committed bool, events int, attempts ...attempt) result {
	t.Helper()
	dir := t.TempDir()
	var raw []byte
	for _, r := range got {
		raw = append(raw, r.String()+"\n"...)
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, recordFile), raw, 0o600))
	sc := Script{
		Steps:   []Step{{Kind: StepType, Keys: "hi"}, {Kind: StepEnter, Keys: "\r"}},
		Notices: []Notice{{Text: oracleNotice}},
	}
	art := artifacts{done: []bool{committed}, ids: map[protocol.OpaqueID]int{firstTry: 0}, attempts: attempts}
	for range events {
		art.events = append(art.events, event(protocol.EventTypeInputDeliveryCommitted, session.DeliveryCommitted{DeliveryID: firstTry}))
	}
	var r result
	require.NoError(t, r.judge(sc, dir, art, false))
	return r
}

// TestOracleChecksEachNotice wants each notice received once when
// committed and never when not, with as many committed events, and no
// delivery both committed and cancelled.
func TestOracleChecksEachNotice(t *testing.T) {
	t.Parallel()
	notice := Record{RecordLine, oracleNotice}
	hi := Record{RecordLine, "hi"}

	r := judged(t, []Record{notice, hi}, true, 1)
	require.Zero(t, r.noticeFaults)
	require.Equal(t, 1, r.delivered)
	require.Zero(t, r.requeued)
	r = judged(t, []Record{hi}, false, 0)
	require.Zero(t, r.noticeFaults)
	require.Equal(t, 1, r.requeued, "a notice never committed stays queued")

	for name, r := range map[string]result{
		"committed, never received": judged(t, []Record{hi}, true, 1),
		"received, never committed": judged(t, []Record{notice, hi}, false, 0),
		"committed, received twice": judged(t, []Record{notice, hi, notice}, true, 1),
		"committed with no event":   judged(t, []Record{notice, hi}, true, 0),
		"one commit, two events":    judged(t, []Record{notice, hi}, true, 2),
		"committed and also cancelled": judged(t, []Record{notice, hi}, true, 1, attempt{
			id:     firstTry,
			commit: answer{outcome: session.DeliveryCommittedOutcome},
			cancel: answer{outcome: session.DeliveryCancelledOutcome},
		}),
	} {
		require.Equal(t, 1, r.noticeFaults, name)
		require.True(t, r.failed(false), name)
		require.Len(t, r.detail, 1, name)
	}
}
