package deliveryincident

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const secondOperator = protocol.AgentID("agent-operator-two")

// offlineNotifier refuses the destinations it holds offline the way the
// hub does when a recipient has no session that takes deliveries.
type offlineNotifier struct {
	*recordingNotifier
	offline map[protocol.AgentID]bool
}

func (n *offlineNotifier) FileDeliveryIncidentNotice(ctx context.Context, destination protocol.AgentID, notice Notice) error {
	if n.offline[destination] {
		return fmt.Errorf("file notice to %s: %w", destination, ErrNotDeliverable)
	}
	return n.recordingNotifier.FileDeliveryIncidentNotice(ctx, destination, notice)
}

// withOperators rebinds the fixture's worker to operators, through a
// notifier that holds offline the given destinations.
func withOperators(t *testing.T, f *workerFixture, offline map[protocol.AgentID]bool, operators ...protocol.AgentID) *offlineNotifier {
	t.Helper()
	notifier := &offlineNotifier{recordingNotifier: f.notifier, offline: offline}
	worker, err := NewWorker(Options{
		DB: f.db, Clock: f.clock, NodeID: incidentLocalNode,
		Operators: operators, Notifier: notifier,
	})
	require.NoError(t, err)
	f.worker = worker
	return notifier
}

func TestAlertReachesEveryOperator(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	withOperators(t, f, nil, incidentOperator, secondOperator)
	f.threeFailures(t)

	_, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.notifier.noticesFor(incidentOperator), 1)
	require.Len(t, f.notifier.noticesFor(secondOperator), 1)
	require.Len(t, f.notifier.noticesFor(incidentSender), 1)

	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.notifier.noticesFor(incidentOperator), 1, "the alert is marked once for every operator")
	require.Len(t, f.notifier.noticesFor(secondOperator), 1)
}

// An operator that cannot take the alert now does not hold it back once
// another operator took it.
func TestAlertSkipsOperatorThatCannotTakeIt(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	withOperators(t, f, map[protocol.AgentID]bool{incidentOperator: true}, incidentOperator, secondOperator)
	f.threeFailures(t)

	_, err := f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Empty(t, f.notifier.noticesFor(incidentOperator))
	require.Len(t, f.notifier.noticesFor(secondOperator), 1)

	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.notifier.noticesFor(secondOperator), 1, "the alert was marked")
}

// When no operator can take the alert, nothing is marked: the next pass
// files it once an operator can.
func TestAlertWaitsUntilAnOperatorCanTakeIt(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	notifier := withOperators(t, f, map[protocol.AgentID]bool{incidentOperator: true, secondOperator: true},
		incidentOperator, secondOperator)
	f.threeFailures(t)

	_, err := f.worker.RunOnce(t.Context())
	require.ErrorIs(t, err, ErrNotDeliverable)
	require.Empty(t, f.notifier.calls)

	delete(notifier.offline, secondOperator)
	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.notifier.noticesFor(secondOperator), 1)
}

// A sender that is any of the operators already has the operator alert.
func TestOperatorSenderGetsNoSecondNotice(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	withOperators(t, f, nil, incidentOperator, secondOperator)
	_, err := f.db.ExecContext(t.Context(), `UPDATE messages SET from_agent = ? WHERE message_id = ?`,
		secondOperator, incidentMessage)
	require.NoError(t, err)
	f.threeFailures(t)

	_, err = f.worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, f.notifier.noticesFor(secondOperator), 1, "only the operator alert")
}

func TestNewWorkerRefusesInvalidOperators(t *testing.T) {
	t.Parallel()
	f := newWorkerFixture(t)
	for name, operators := range map[string][]protocol.AgentID{
		"malformed": {"Bad Agent"},
		"twice":     {incidentOperator, incidentOperator},
	} {
		_, err := NewWorker(Options{
			DB: f.db, Clock: f.clock, NodeID: incidentLocalNode,
			Operators: operators, Notifier: f.notifier,
		})
		require.Error(t, err, name)
	}
}
