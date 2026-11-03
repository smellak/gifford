package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
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
func overdueCoveredItem(id protocol.ULID) renotify.Item {
	return renotify.Item{
		MessageID: id,
		AgentID:   liveSender,
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
	_, err = runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), []renotify.Item{overdueCoveredItem(id)})
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
	engine   *channel.Engine
	ctl      *channel.Controller
}

func newLiveServiceFixture(t *testing.T) *liveServiceFixture {
	t.Helper()
	return newLiveServiceFixtureWithClock(t, fixedTestClock{at: operatorTestT0})
}

func newLiveServiceFixtureWithClock(t *testing.T, clk clock.Clock) *liveServiceFixture {
	t.Helper()
	operator := liveOperator
	ctx := context.Background()
	store, err := eventstore.Open(ctx, filepath.Join(t.TempDir(), "live.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	db := store.DB()
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
		engine:   engine,
		ctl:      ctl,
	}
}

const liveSender = protocol.AgentID("agent-9")

const liveOperator = protocol.AgentID("agent-operator")

func seedLiveAgent(t *testing.T, db *sql.DB, agent protocol.AgentID) {
	t.Helper()
	ctx := context.Background()
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

func seedLiveMessage(t *testing.T, db *sql.DB, id protocol.ULID) {
	t.Helper()
	ctx := context.Background()
	for _, agent := range []protocol.AgentID{liveSender, "agent-operator"} {
		seedLiveAgent(t, db, agent)
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
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM messages WHERE client_msg_id = ?`,
		string(escalate.ClientMsgID(id))).Scan(&n))
	return n
}

// With the real service, an escalation turn admits exactly one operator
// notice and marks it, without error. This fails with the old shared
// transaction: the admission deadlocks against the turn's own lock.
func TestRenotifyEscalationEndToEndWithRealService(t *testing.T) {
	t.Parallel()

	fix := newLiveServiceFixture(t)
	seedLiveMessage(t, fix.db, protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021"))
	item := overdueCoveredItem(protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0021"))
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

	fix := newLiveServiceFixture(t)
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0022")
	seedLiveMessage(t, fix.db, id)
	item := overdueCoveredItem(id)

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
	_, err = runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), []renotify.Item{overdueCoveredItem(id)})
	require.NoError(t, err)
	require.Empty(t, service.admissions, "no notice without a configured operator")
	escalated, err := escalate.Escalated(ctx, db, id)
	require.NoError(t, err)
	require.False(t, escalated)
}

// seedLiveQueue files a first delivered generation through the real
// channel engine, so the runner's renotice walks the production
// requeue path: a real engine, a real queue row, real notices.
func seedLiveQueue(t *testing.T, fix *liveServiceFixture, id protocol.ULID, agent protocol.AgentID, pending time.Time, notices []time.Time) {
	t.Helper()
	ctx := context.Background()
	admit := channel.Message{
		ID: id, From: protocol.AgentID("agent-sender"), FromNode: protocol.NodeID("node-1"),
		To:     []channel.Recipient{{Agent: agent, Deliverable: true, Policy: channel.PolicyAllowed}},
		ToNode: protocol.NodeID("node-1"), ThreadID: protocol.ThreadID("thread-1"),
	}
	tx, err := fix.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, _, err = fix.engine.Admit(ctx, tx, admit, func(ctx context.Context, _ protocol.AdmissionResult) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO messages(message_id, client_msg_id, origin_node,
			from_agent, destination_node, thread_ref, thread_id, priority, state,
			created_at, expires_at, updated_at, hop_count, route_digest)
			VALUES(?, ?, 'node-1', 'agent-sender', 'node-1', 'thr_00000000000000000000000001',
			'thread-1', 'NORMAL', 'PENDING', ?, ?, ?, 0, 'sha256:00')`,
			string(id), "client-"+string(id),
			eventstore.FormatTime(operatorTestT0), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
			eventstore.FormatTime(operatorTestT0))
		return err
	})
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_queue(message_id, agent_id, delivery_generation,
		from_agent, thread_ref, state, priority, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES(?, ?, 1, 'agent-sender', 'thr_00000000000000000000000001', 'DELIVERED',
		'NORMAL', ?, ?, ?, 0, 'sha256:00')`,
		string(id), string(agent),
		eventstore.FormatTime(pending), eventstore.FormatTime(time.Now().UTC().Add(24*time.Hour)),
		eventstore.FormatTime(pending))
	require.NoError(t, err)
	require.NoError(t, fix.engine.InitializeDelivery(ctx, tx, id, agent, 1))
	require.NoError(t, tx.Commit())
	for i, at := range notices {
		_, err = fix.db.ExecContext(ctx, `INSERT INTO delivery_attempts(delivery_id, message_id, agent_id,
			delivery_generation, adapter_session_id, orchestrator_decision_id, channel_decision_id,
			draft_generation, input_guard_version, control_versions_json, state, created_at, updated_at)
			VALUES(?, ?, ?, 1, 'pane-1', 'dec-1', 'ch-1', 0, 0, '{}', 'COMMITTED', ?, ?)`,
			"att-"+string(id)+"-"+string(rune('a'+i)), string(id), string(agent),
			eventstore.FormatTime(at), eventstore.FormatTime(at))
		require.NoError(t, err)
	}
}

// settleLiveGeneration simulates the worker between loop turns: the
// queued generation gets delivered and its attempt commits, so the
// next turn sees durable notices instead of a live queued row.
func settleLiveGeneration(t *testing.T, db *sql.DB, id protocol.ULID, agent protocol.AgentID, generation int64, at time.Time) {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ? AND agent_id = ? AND delivery_generation = ?`,
		string(id), string(agent), generation)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO delivery_attempts(delivery_id, message_id, agent_id,
		delivery_generation, adapter_session_id, orchestrator_decision_id, channel_decision_id,
		draft_generation, input_guard_version, control_versions_json, state, created_at, updated_at)
		VALUES(?, ?, ?, ?, 'pane-1', 'dec-1', 'ch-1', 0, 0, '{}', 'COMMITTED', ?, ?)`,
		"att-settled-"+string(id)+"-"+string(rune('0'+generation)), string(id), string(agent), generation,
		eventstore.FormatTime(at), eventstore.FormatTime(at))
	require.NoError(t, err)
}

// liveGenerations lists every generation filed for a message, oldest
// first: one more entry per renotice the loop files.
func liveGenerations(t *testing.T, db *sql.DB, id protocol.ULID) []int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT delivery_generation FROM delivery_queue
		WHERE message_id = ? AND agent_id = ? ORDER BY delivery_generation`, string(id), string(liveSender))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var gens []int64
	for rows.Next() {
		var g int64
		require.NoError(t, rows.Scan(&g))
		gens = append(gens, g)
	}
	require.NoError(t, rows.Err())
	return gens
}

func liveLoader(fix *liveServiceFixture) *renotify.Loader {
	return &renotify.Loader{DB: fix.db, Channels: fix.ctl, Clock: fixedTestClock{at: operatorTestT0}}
}

// After an escalation the loop keeps filing renotices at every later
// uncovered mark: one more generation at +45 min, 2 h and 4 h, with a
// single operator notice and a single mark in total. This fails if an
// escalation ever swallows the schedule again.
func TestRenotifyPostEscalationRenoticesKeepFiling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fix := newLiveServiceFixture(t)
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0041")
	seedLiveAgent(t, fix.db, liveSender)
	seedLiveAgent(t, fix.db, "agent-operator")
	seedLiveQueue(t, fix, id, liveSender, operatorTestT0, []time.Time{operatorTestT0.Add(15 * time.Minute)})
	loader := liveLoader(fix)

	items, err := loader.Items(ctx)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = fix.runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, id), "one operator notice")

	for _, turn := range []struct {
		at  time.Time
		gen int64
	}{
		{operatorTestT0.Add(45 * time.Minute), 2},
		{operatorTestT0.Add(2 * time.Hour), 3},
		{operatorTestT0.Add(4 * time.Hour), 4},
	} {
		items, err := loader.Items(ctx)
		require.NoError(t, err)
		require.Len(t, items, 1)
		_, err = fix.runner.RunOnce(ctx, turn.at, items)
		require.NoError(t, err)
		require.Equal(t, []int64{1, 2, 3, 4}[:turn.gen], liveGenerations(t, fix.db, id),
			"one more generation filed at %v", turn.at)
		settleLiveGeneration(t, fix.db, id, liveSender, turn.gen, turn.at)
	}

	require.Equal(t, 1, countOperatorNotices(t, fix.db, id), "no second notice after the escalation")
	var marks int
	require.NoError(t, fix.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM escalations WHERE message_id = ?`,
		string(id)).Scan(&marks))
	require.Equal(t, 1, marks, "escalation marked once")
}

// A failing admission never starves the rest of the turn: with two
// messages in one turn and the first admission failing once, the
// second still gets its renotice in that same turn. The first keeps
// the renotice it filed, and the next turn admits it exactly once and
// marks it. This fails if one item error ever aborts the turn again.
func TestRenotifyTurnContinuesAfterAdmissionFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	fix := newLiveServiceFixture(t)
	first := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0042")
	second := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0043")
	seedLiveAgent(t, fix.db, liveSender)
	seedLiveAgent(t, fix.db, "agent-operator")
	seedLiveQueue(t, fix, first, liveSender, operatorTestT0, []time.Time{operatorTestT0.Add(5 * time.Minute)})
	seedLiveQueue(t, fix, second, liveSender, operatorTestT0.Add(25*time.Minute), nil)
	loader := liveLoader(fix)

	admitCalls := 0
	realAdmit := fix.runner.AdmitOperator
	fix.runner.AdmitOperator = func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error {
		admitCalls++
		if admitCalls == 1 {
			return errors.New("operator admission unavailable")
		}
		return realAdmit(ctx, messageID, agentID, operator)
	}

	items, err := loader.Items(ctx)
	require.NoError(t, err)
	require.Len(t, items, 2)
	_, err = fix.runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), items)
	require.Error(t, err, "the injected admission failure surfaces")
	require.Equal(t, []int64{1, 2}, liveGenerations(t, fix.db, first),
		"the first message keeps the renotice it filed before its admission failed")
	require.Equal(t, []int64{1, 2}, liveGenerations(t, fix.db, second),
		"the second message is renoticed in the same turn")
	require.Equal(t, 0, countOperatorNotices(t, fix.db, first), "no notice admitted on the failed turn")

	items, err = loader.Items(ctx)
	require.NoError(t, err)
	require.Len(t, items, 2)
	_, err = fix.runner.RunOnce(ctx, operatorTestT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, first), "the retry admits exactly one notice")
	require.Equal(t, []int64{1, 2}, liveGenerations(t, fix.db, first),
		"no duplicate renotice on the retry turn")
	require.Equal(t, []int64{1, 2}, liveGenerations(t, fix.db, second),
		"no duplicate renotice for the second message either")
	escalated, err := escalate.Escalated(ctx, fix.db, first)
	require.NoError(t, err)
	require.True(t, escalated, "the first message ends marked")
}

// advancingClock moves like the production loop: every turn reads a
// later time, so a retry never shares the first attempt's stamps.
type advancingClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *advancingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *advancingClock) set(at time.Time) {
	c.mu.Lock()
	c.at = at
	c.mu.Unlock()
}

// A retry a minute later still deduplicates: the second admission
// reuses the first attempt's stamps, so the hub answers the stored
// admission instead of rejecting different params. This fails without
// the stamp reuse: the hub answers «client_msg_id binds other
// parameters» and the mark never lands.
func TestRenotifyEscalationRetryWithAdvancingClock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	clk := &advancingClock{at: operatorTestT0.Add(31 * time.Minute)}
	fix := newLiveServiceFixtureWithClock(t, clk)
	id := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G0044")
	seedLiveMessage(t, fix.db, id)
	item := overdueCoveredItem(id)

	markErr := errors.New("mark unavailable")
	realMark := fix.runner.MarkEscalated
	fix.runner.MarkEscalated = func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
		if markErr != nil {
			return markErr
		}
		return realMark(ctx, tx, messageID, agentID)
	}
	_, err := fix.runner.RunOnce(ctx, clk.Now(), []renotify.Item{item})
	require.ErrorIs(t, err, markErr)
	require.Equal(t, 1, countOperatorNotices(t, fix.db, id), "admission happened before the mark failure")

	// The next turn runs a minute later, as the production loop does.
	markErr = nil
	clk.set(operatorTestT0.Add(32 * time.Minute))
	_, err = fix.runner.RunOnce(ctx, clk.Now(), []renotify.Item{item})
	require.NoError(t, err, "a retry a minute later must still dedup and mark")
	require.Equal(t, 1, countOperatorNotices(t, fix.db, id), "the retry deduplicates on the same key")
	escalated, err := escalate.Escalated(ctx, fix.db, id)
	require.NoError(t, err)
	require.True(t, escalated, "the mark lands on the retry")
}
