package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/fallback"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
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

// staticKeys is the fail-closed key source for the live-service test:
// only listed agents sign, nobody is blocked, everyone routes local.
type staticKeys struct {
	signing map[protocol.AgentID]registry.KeyInfo
}

var errNoTestKey = errors.New("renotify test: no key")

func (s staticKeys) ActiveSigningKey(_ context.Context, _ registry.Querier, agent protocol.AgentID) (registry.KeyInfo, error) {
	key, ok := s.signing[agent]
	if !ok {
		return registry.KeyInfo{}, errNoTestKey
	}
	return key, nil
}

func (s staticKeys) Blocked(context.Context, registry.Querier, protocol.AgentID) (bool, error) {
	return false, nil
}

func (s staticKeys) Route(_ context.Context, _ registry.Querier, agent protocol.AgentID) (registry.RouteResult, error) {
	if !agent.Valid() {
		return registry.RouteResult{}, errNoTestKey
	}
	return registry.RouteResult{Kind: registry.RouteLocal, Node: protocol.NodeID("node-1")}, nil
}

func (s staticKeys) Register(context.Context, registry.Execer, registry.RegisterRequest) (registry.RegisterResult, error) {
	return registry.RegisterResult{}, errNoTestKey
}

// liveServiceFixture wires the real escalation chain: file event store
// with the production DSN, channel engine and controller, the real hub
// service, and the runner exactly as newRenotifyRunner builds it.
type liveServiceFixture struct {
	db       *sql.DB
	service  *hubsrv.Service
	runner   *renotify.Runner
	operator protocol.AgentID
}

func newLiveServiceFixture(t *testing.T, operator protocol.AgentID) *liveServiceFixture {
	t.Helper()
	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "live.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
	clk := fixedTestClock{at: operatorTestT0}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	ctl, err := channel.NewController(channel.ControllerOptions{
		DB: db, Engine: engine, Clock: clk, NodeID: protocol.NodeID("node-1"),
	})
	require.NoError(t, err)
	require.NoError(t, ctl.Reload(ctx))
	public, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	service, err := hubsrv.NewService(hubsrv.ServiceOptions{
		DB: db, NodeID: protocol.NodeID("node-1"), Engine: engine, Approvals: ctl,
		Registry: staticKeys{signing: map[protocol.AgentID]registry.KeyInfo{
			liveSender: {KeyID: "ed25519:agent-9:v1", Agent: liveSender, PublicKey: public},
		}},
		Clock: clk,
	})
	require.NoError(t, err)
	return &liveServiceFixture{
		db:       db,
		service:  service,
		runner:   newRenotifyRunner(db, operator, engine, service, clk),
		operator: operator,
	}
}

const liveSender = protocol.AgentID("agent-9")

func seedLiveMessage(t *testing.T, db *sql.DB, id protocol.ULID) {
	t.Helper()
	ctx := context.Background()
	for _, agent := range []protocol.AgentID{liveSender, "agent-operator"} {
		_, err := db.ExecContext(ctx, `INSERT INTO agents(agent_id, node_id, harness, created_at, updated_at)
			VALUES(?, 'node-1', 'test', ?, ?)`,
			string(agent), eventstore.FormatTime(operatorTestT0), eventstore.FormatTime(operatorTestT0))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO sessions(session_id, agent_id, state, capture_mode,
			deliverable, manifest_json, started_at, updated_at)
			VALUES(?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?)`,
			"session-"+string(agent), string(agent),
			eventstore.FormatTime(operatorTestT0), eventstore.FormatTime(operatorTestT0))
		require.NoError(t, err)
	}
	_, err := db.ExecContext(ctx, `INSERT INTO messages(message_id, client_msg_id, origin_node, from_agent,
		destination_node, thread_ref, thread_id, priority, state, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES(?, ?, 'node-1', 'agent-sender', 'node-1', 'thr_00000000000000000000000001', 'thread-1',
		'NORMAL', 'PENDING', ?, ?, ?, 0, 'sha256:00')`,
		string(id), "client-"+string(id),
		eventstore.FormatTime(operatorTestT0), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
		eventstore.FormatTime(operatorTestT0))
	require.NoError(t, err)
}

func countOperatorNotices(t *testing.T, db *sql.DB, id protocol.ULID) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM messages WHERE client_msg_id = ?`,
		string(escalate.ClientMsgID(id))).Scan(&n))
	return n
}

// With the real service, an escalation turn admits exactly one operator
// notice and marks it, without error. This fails with the old shared
// transaction: the admission deadlocks against the turn's own lock.
func TestRenotifyEscalationEndToEndWithRealService(t *testing.T) {
	t.Parallel()

	fix := newLiveServiceFixture(t, protocol.AgentID("agent-operator"))
	seedLiveMessage(t, fix.db, protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021"))
	item := overdueCoveredItem(protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021"), liveSender)
	_, err := fix.runner.RunOnce(context.Background(), operatorTestT0.Add(31*time.Minute), []renotify.Item{item})
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021")),
		"one operator notice")
	escalated, err := escalate.Escalated(context.Background(), fix.db, protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021"))
	require.NoError(t, err)
	require.True(t, escalated, "escalation marked durable")

	// A second turn adds nothing: the mark suppresses the escalation.
	item.Facts.Escalated = true
	_, err = fix.runner.RunOnce(context.Background(), operatorTestT0.Add(32*time.Minute), []renotify.Item{item})
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021")),
		"no second notice")
}

// A mark failure between the admission and the mark retries with the
// same idempotency key: the next turn still leaves exactly one
// operator notice and sets the mark.
func TestRenotifyEscalationRetryAfterMarkFailure(t *testing.T) {
	t.Parallel()

	fix := newLiveServiceFixture(t, protocol.AgentID("agent-operator"))
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0022")
	seedLiveMessage(t, fix.db, id)
	item := overdueCoveredItem(id, liveSender)

	markErr := errors.New("mark unavailable")
	realMark := fix.runner.MarkEscalated
	fix.runner.MarkEscalated = func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
		if markErr != nil {
			return markErr
		}
		return realMark(ctx, tx, messageID, agentID)
	}
	_, err := fix.runner.RunOnce(context.Background(), operatorTestT0.Add(31*time.Minute), []renotify.Item{item})
	require.ErrorIs(t, err, markErr)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, id), "admission happened before the mark failure")

	markErr = nil
	_, err = fix.runner.RunOnce(context.Background(), operatorTestT0.Add(31*time.Minute), []renotify.Item{item})
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, id), "retry deduplicates on the same key")
	escalated, err := escalate.Escalated(context.Background(), fix.db, id)
	require.NoError(t, err)
	require.True(t, escalated)
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
