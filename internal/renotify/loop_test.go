package renotify

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

var loopT0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

var errLoopBoom = errors.New("loop test mark failure")

func loopItem(when time.Duration) Item {
	return Item{
		MessageID: protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0001"),
		AgentID:   protocol.AgentID("agent-1"),
		Facts: fallback.Facts{
			PendingSince: loopT0, GuardsPassed: true,
			Model: fallback.ModelOff, Pane: fallback.PaneIdle,
			Now: loopT0.Add(when),
		},
	}
}

type loopEffects struct {
	notifies int
	admits   int
	marks    int
	markErr  error
	markFail int
}

func (e *loopEffects) notify(context.Context, *sql.Tx, protocol.ULID, protocol.AgentID) error {
	e.notifies++
	return nil
}

func (e *loopEffects) admit(context.Context, *sql.Tx, protocol.ULID, protocol.AgentID, protocol.AgentID) error {
	e.admits++
	return nil
}

func (e *loopEffects) mark(context.Context, *sql.Tx, protocol.ULID, protocol.AgentID) error {
	e.marks++
	if e.markFail > 0 {
		e.markFail--
		return e.markErr
	}
	return nil
}

func loopRunner(t *testing.T, effects *loopEffects) *Runner {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "renotify.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return &Runner{
		DB:            store.DB(),
		Operator:      protocol.AgentID("agent-operator"),
		Notify:        effects.notify,
		AdmitOperator: effects.admit,
		MarkEscalated: effects.mark,
	}
}

func TestLoopRenoticesWhenDue(t *testing.T) {
	t.Parallel()

	effects := &loopEffects{}
	runner := loopRunner(t, effects)
	wake, err := runner.RunOnce(context.Background(), loopT0, []Item{loopItem(0)})
	require.NoError(t, err)
	require.Equal(t, 1, effects.notifies, "due slot triggers one renotice")
	require.Equal(t, 0, effects.admits, "no escalation before the deadline")
	require.Equal(t, loopT0.Add(5*time.Minute), wake, "wake at the next slot")
}

func TestLoopWaitsWhenCovered(t *testing.T) {
	t.Parallel()

	effects := &loopEffects{}
	runner := loopRunner(t, effects)
	item := loopItem(4 * time.Minute)
	item.Facts.Notices = []time.Time{loopT0}
	wake, err := runner.RunOnce(context.Background(), loopT0.Add(4*time.Minute), []Item{item})
	require.NoError(t, err)
	require.Equal(t, 0, effects.notifies, "covered slot triggers nothing")
	require.Equal(t, 0, effects.admits)
	require.Equal(t, loopT0.Add(5*time.Minute), wake)
}

func TestLoopEscalatesOnceAtThirtyMinutes(t *testing.T) {
	t.Parallel()

	effects := &loopEffects{}
	runner := loopRunner(t, effects)
	item := loopItem(30 * time.Minute)
	item.Facts.Pane = fallback.PaneBusy
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{item})
	require.NoError(t, err)
	require.Equal(t, 0, effects.notifies, "busy agent gets no notice")
	require.Equal(t, 1, effects.admits, "deadline admits one operator notice")
	require.Equal(t, 1, effects.marks, "deadline marks escalation once")

	item.Facts.Escalated = true
	_, err = runner.RunOnce(context.Background(), loopT0.Add(31*time.Minute), []Item{item})
	require.NoError(t, err)
	require.Equal(t, 1, effects.admits, "escalation never repeats")
	require.Equal(t, 1, effects.marks)
}

func TestLoopHoldsWithoutEffects(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*Item){
		"controls hold": func(item *Item) { item.Facts.Controls.Hold = true },
		"invalid model": func(item *Item) { item.Facts.Model = fallback.ModelOutcome(99) },
		"rejected":      func(item *Item) { item.Facts.Controls.Rejected = true },
	} {
		effects := &loopEffects{}
		runner := loopRunner(t, effects)
		item := loopItem(time.Hour)
		mutate(&item)
		wake, err := runner.RunOnce(context.Background(), loopT0.Add(time.Hour), []Item{item})
		require.NoError(t, err, name)
		require.Zero(t, effects.notifies, name)
		require.Zero(t, effects.admits, name)
		require.Zero(t, effects.marks, name)
		_ = wake
	}
}

// A rejection ends the attempt even when the slot is covered and the
// deadline passed: the schedule never outranks a terminal control,
// so a dropped attempt neither renotifies nor escalates.
func TestLoopDropsRejectedOnCoveredSlot(t *testing.T) {
	t.Parallel()

	effects := &loopEffects{}
	runner := loopRunner(t, effects)
	item := loopItem(31 * time.Minute)
	item.Facts.Notices = []time.Time{loopT0.Add(15 * time.Minute)}
	item.Facts.Controls.Rejected = true
	_, err := runner.RunOnce(context.Background(), loopT0.Add(31*time.Minute), []Item{item})
	require.NoError(t, err)
	require.Zero(t, effects.notifies, "rejection ends the attempt even on a covered slot")
	require.Zero(t, effects.admits, "a dropped attempt never escalates")
	require.Zero(t, effects.marks)
}

func TestLoopRetriesEscalationAfterMarkFailure(t *testing.T) {
	t.Parallel()

	effects := &loopEffects{markErr: errLoopBoom, markFail: 1}
	runner := loopRunner(t, effects)
	item := loopItem(30 * time.Minute)
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{item})
	require.Error(t, err, "mark failure aborts the turn")
	require.Equal(t, 1, effects.admits)
	require.Equal(t, 1, effects.marks)

	_, err = runner.RunOnce(context.Background(), loopT0.Add(31*time.Minute), []Item{item})
	require.NoError(t, err, "retry completes the turn")
	require.Equal(t, 2, effects.admits, "admission retries idempotently")
	require.Equal(t, 2, effects.marks)
}

func TestLoopWakesAtEarliestSlot(t *testing.T) {
	t.Parallel()

	effects := &loopEffects{}
	runner := loopRunner(t, effects)
	late := loopItem(50 * time.Minute)
	early := loopItem(50 * time.Minute)
	early.MessageID = protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0002")
	early.Facts.Notices = []time.Time{loopT0.Add(45 * time.Minute)}
	wake, err := runner.RunOnce(context.Background(), loopT0.Add(50*time.Minute), []Item{late, early})
	require.NoError(t, err)
	require.Equal(t, 1, effects.notifies, "only the due item notifies")
	require.Equal(t, loopT0.Add(2*time.Hour), wake, "wake at the earliest next slot")
}
