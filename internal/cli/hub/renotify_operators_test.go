package hub

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/eventstore"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

// secondOperator is the operator listed after liveOperator.
const secondOperator = protocol.AgentID("agent-second")

// offlineService refuses its first operator the way the hub does when
// that operator has no session that takes deliveries.
type offlineService struct {
	recordingService
}

func (s *offlineService) FileOperatorNotice(context.Context, protocol.AgentID, protocol.AgentID, protocol.ULID, time.Time) error {
	return fmt.Errorf("admit operator notice: %w", hubsrv.ErrOperatorNotDeliverable)
}

// The first operator keeps the notice key it always had; every other
// operator gets the same notice under its own, and the configured
// deadline replaces the default one.
func TestRenotifyRunnerEscalatesToEveryOperator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "operators.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021")
	seedOperatorMessage(t, db, id, protocol.AgentID("agent-7"))

	service := &recordingService{}
	runner := newRenotifyRunner(db, []protocol.AgentID{liveOperator, secondOperator}, 10*time.Minute,
		recordingEngine{}, service, fixedTestClock{at: operatorTestT0})
	item := overdueCoveredItem(id)
	item.Facts.Notices = []time.Time{operatorTestT0.Add(5 * time.Minute)}
	_, err = runner.RunOnce(ctx, operatorTestT0.Add(11*time.Minute), []renotify.Item{item})
	require.NoError(t, err)
	require.Len(t, service.filings, 2)
	require.Equal(t, liveOperator, service.filings[0].operator)
	require.False(t, service.filings[0].copy, "the first operator keeps its key")
	require.Equal(t, secondOperator, service.filings[1].operator)
	require.True(t, service.filings[1].copy)
	escalated, err := escalate.Escalated(ctx, db, id)
	require.NoError(t, err)
	require.True(t, escalated)
}

// An operator the hub cannot deliver to reaches the loop as its own
// sentinel, so the other operator's notice is enough to mark.
func TestRenotifyRunnerSkipsOperatorTheHubCannotReach(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "offline.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0022")
	seedOperatorMessage(t, db, id, protocol.AgentID("agent-8"))

	service := &offlineService{}
	runner := newRenotifyRunner(db, []protocol.AgentID{liveOperator, secondOperator}, 0,
		recordingEngine{}, service, fixedTestClock{at: operatorTestT0})
	_, err = runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), []renotify.Item{overdueCoveredItem(id)})
	require.ErrorIs(t, err, renotify.ErrOperatorNotDeliverable)
	require.Len(t, service.filings, 1)
	require.Equal(t, secondOperator, service.filings[0].operator)
	escalated, err := escalate.Escalated(ctx, db, id)
	require.NoError(t, err)
	require.True(t, escalated)
}

func TestFirstOperatorOwnsTheIncidentRegister(t *testing.T) {
	t.Parallel()
	require.Equal(t, protocol.AgentID(""), firstOperator(nil))
	require.Equal(t, liveOperator,
		firstOperator([]protocol.AgentID{liveOperator, secondOperator}))
}
