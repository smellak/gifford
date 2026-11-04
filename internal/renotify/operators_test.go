package renotify

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
)

var errOperatorBoom = errors.New("operator filing failure")

// operatorFiler answers each operator's filing with a fixed error and
// records the operators it was asked to file to, in order.
type operatorFiler struct {
	errs  map[protocol.AgentID]error
	calls []protocol.AgentID
}

func (f *operatorFiler) file(_ context.Context, _ protocol.ULID, _ protocol.AgentID, operator protocol.AgentID, _ time.Time) error {
	f.calls = append(f.calls, operator)
	return f.errs[operator]
}

func operatorRunner(t *testing.T, filer *operatorFiler, effects *loopEffects, operators ...protocol.AgentID) *Runner {
	t.Helper()
	runner := loopRunner(t, effects)
	runner.Operators = operators
	runner.FileOperatorNotice = filer.file
	return runner
}

func escalationItem(when time.Duration) Item {
	item := loopItem(when)
	item.Facts.Pane = fallback.PaneBusy
	return item
}

func TestLoopEscalatesToEveryOperatorInOrder(t *testing.T) {
	t.Parallel()
	filer := &operatorFiler{}
	effects := &loopEffects{}
	runner := operatorRunner(t, filer, effects, loopOperator, loopSecondOperator)
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{escalationItem(30 * time.Minute)})
	require.NoError(t, err)
	require.Equal(t, []protocol.AgentID{loopOperator, loopSecondOperator}, filer.calls)
	require.Equal(t, 1, effects.marks, "one mark covers every operator")
}

// An operator with no session that can take the notice does not hold back
// the escalation once another operator took it; the skip is reported.
func TestLoopSkipsOperatorThatCannotTakeTheNotice(t *testing.T) {
	t.Parallel()
	filer := &operatorFiler{errs: map[protocol.AgentID]error{loopOperator: ErrOperatorNotDeliverable}}
	effects := &loopEffects{}
	runner := operatorRunner(t, filer, effects, loopOperator, loopSecondOperator)
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{escalationItem(30 * time.Minute)})
	require.ErrorIs(t, err, ErrOperatorNotDeliverable)
	require.Equal(t, 1, effects.marks, "the second operator took the notice")
}

// When no operator can take the notice, nothing is marked: a later turn
// retries, as with a single operator.
func TestLoopRetriesWhenNoOperatorCanTakeTheNotice(t *testing.T) {
	t.Parallel()
	filer := &operatorFiler{errs: map[protocol.AgentID]error{
		loopOperator: ErrOperatorNotDeliverable, loopSecondOperator: ErrOperatorNotDeliverable,
	}}
	effects := &loopEffects{}
	runner := operatorRunner(t, filer, effects, loopOperator, loopSecondOperator)
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{escalationItem(30 * time.Minute)})
	require.ErrorIs(t, err, ErrOperatorNotDeliverable)
	require.Zero(t, effects.marks)
}

// Any other failure leaves the escalation unmarked, so every filing
// retries under its own idempotency key.
func TestLoopRetriesEveryOperatorAfterAFailedFiling(t *testing.T) {
	t.Parallel()
	filer := &operatorFiler{errs: map[protocol.AgentID]error{loopSecondOperator: errOperatorBoom}}
	effects := &loopEffects{}
	runner := operatorRunner(t, filer, effects, loopOperator, loopSecondOperator)
	item := escalationItem(30 * time.Minute)
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{item})
	require.ErrorIs(t, err, errOperatorBoom)
	require.Zero(t, effects.marks)

	delete(filer.errs, loopSecondOperator)
	_, err = runner.RunOnce(context.Background(), loopT0.Add(31*time.Minute), []Item{item})
	require.NoError(t, err)
	require.Equal(t, []protocol.AgentID{loopOperator, loopSecondOperator, loopOperator, loopSecondOperator}, filer.calls)
	require.Equal(t, 1, effects.marks)
}

// An operator is never told about a message that waits on it; the other
// operators still are.
func TestLoopNeverTellsAnOperatorAboutItsOwnMessage(t *testing.T) {
	t.Parallel()
	filer := &operatorFiler{}
	effects := &loopEffects{}
	runner := operatorRunner(t, filer, effects, "agent-1", loopSecondOperator)
	_, err := runner.RunOnce(context.Background(), loopT0.Add(30*time.Minute), []Item{escalationItem(30 * time.Minute)})
	require.NoError(t, err)
	require.Equal(t, []protocol.AgentID{loopSecondOperator}, filer.calls)
	require.Equal(t, 1, effects.marks)
}

func TestLoopEscalatesAtConfiguredDeadline(t *testing.T) {
	t.Parallel()
	filer := &operatorFiler{}
	effects := &loopEffects{}
	runner := operatorRunner(t, filer, effects, loopOperator)
	runner.EscalateAfter = 10 * time.Minute
	_, err := runner.RunOnce(context.Background(), loopT0.Add(9*time.Minute), []Item{escalationItem(9 * time.Minute)})
	require.NoError(t, err)
	require.Empty(t, filer.calls, "nothing escalates before the configured deadline")
	_, err = runner.RunOnce(context.Background(), loopT0.Add(10*time.Minute), []Item{escalationItem(10 * time.Minute)})
	require.NoError(t, err)
	require.Equal(t, []protocol.AgentID{loopOperator}, filer.calls)
	require.Equal(t, 1, effects.marks)
}
