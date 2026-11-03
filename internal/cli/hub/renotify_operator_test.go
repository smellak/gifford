package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/renotify"
)

var operatorTestT0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

type fixedTestClock struct{ at time.Time }

func (f fixedTestClock) Now() time.Time { return f.at }

type recordingEngine struct{}

func (recordingEngine) InitializeDelivery(context.Context, *sql.Tx, protocol.ULID, protocol.AgentID, int64) error {
	return nil
}

type recordedAdmission struct {
	sender   protocol.AgentID
	operator protocol.AgentID
	key      string
}

type recordingService struct {
	admissions []recordedAdmission
}

func (s *recordingService) AdmitMessage(_ context.Context, sender protocol.AgentID, raw json.RawMessage) (any, error) {
	var params struct {
		ClientMsgID string `json:"client_msg_id"`
		Recipients  []struct {
			AgentID protocol.AgentID `json:"agent_id"`
		} `json:"recipients"`
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, err
	}
	operator := protocol.AgentID("")
	if len(params.Recipients) == 1 {
		operator = params.Recipients[0].AgentID
	}
	s.admissions = append(s.admissions, recordedAdmission{
		sender: sender, operator: operator, key: params.ClientMsgID,
	})
	return nil, nil
}

func seedOperatorMessage(t *testing.T, db *sql.DB, id protocol.ULID, agent protocol.AgentID) {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `INSERT INTO agents(agent_id, node_id, harness, created_at, updated_at)
		VALUES(?, 'node-1', 'test', ?, ?)`,
		string(agent), eventstore.FormatTime(operatorTestT0), eventstore.FormatTime(operatorTestT0))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO messages(message_id, client_msg_id, origin_node, from_agent,
		destination_node, thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES(?, ?, 'node-1', 'agent-sender', 'node-1', 'thr_00000000000000000000000001', 'thread-1',
		'NORMAL', 'PENDING', ?, ?, ?, 0, 'sha256:00')`,
		string(id), "client-"+string(id),
		eventstore.FormatTime(operatorTestT0), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
		eventstore.FormatTime(operatorTestT0))
	require.NoError(t, err)
}

// overdueCoveredItem is unread past the deadline with its latest slot
// covered: the loop waits on the schedule and only escalates.
func overdueCoveredItem(id protocol.ULID, agent protocol.AgentID) renotify.Item {
	return renotify.Item{
		MessageID: id,
		AgentID:   agent,
		Facts: fallback.Facts{
			PendingSince: operatorTestT0,
			Notices:      []time.Time{operatorTestT0.Add(15 * time.Minute)},
			GuardsPassed: true,
			Pane:         fallback.PaneIdle,
		},
	}
}

// With operator_agent configured, an unanswered message past the
// renotices ends in the operator notice, addressed to the configured
// operator and keyed idempotently.
func TestRenotifyRunnerEscalatesToConfiguredOperator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "operator.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0011")
	agent := protocol.AgentID("agent-9")
	seedOperatorMessage(t, db, id, agent)

	service := &recordingService{}
	runner := newRenotifyRunner(db, protocol.AgentID("agent-operator"), recordingEngine{}, service,
		fixedTestClock{at: operatorTestT0})
	_, err = runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), []renotify.Item{overdueCoveredItem(id, agent)})
	require.NoError(t, err)
	require.Len(t, service.admissions, 1, "one operator notice")
	got := service.admissions[0]
	require.Equal(t, agent, got.sender)
	require.Equal(t, protocol.AgentID("agent-operator"), got.operator)
	require.NotEmpty(t, got.operator, "escalation never goes to an empty operator")
	require.Equal(t, string(escalate.ClientMsgID(id)), got.key)
	escalated, err := escalate.Escalated(ctx, db, id)
	require.NoError(t, err)
	require.True(t, escalated, "escalation marked durable")
}

// Without operator_agent the loop keeps the renotices and skips
// escalation quietly: no admission, no mark, no error. This fails if
// escalation ever goes to an empty operator again.
func TestRenotifyRunnerStaysQuietWithoutOperator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "no-operator.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0012")
	agent := protocol.AgentID("agent-9")
	seedOperatorMessage(t, db, id, agent)

	service := &recordingService{}
	runner := newRenotifyRunner(db, protocol.AgentID(""), recordingEngine{}, service,
		fixedTestClock{at: operatorTestT0})
	require.False(t, runner.Operator.Valid())
	_, err = runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), []renotify.Item{overdueCoveredItem(id, agent)})
	require.NoError(t, err)
	require.Empty(t, service.admissions, "no notice without a configured operator")
	escalated, err := escalate.Escalated(ctx, db, id)
	require.NoError(t, err)
	require.False(t, escalated)
}
