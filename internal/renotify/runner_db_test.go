package renotify

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/protocol"
)

type fakeEngine struct {
	generations []int64
	err         error
}

func (f *fakeEngine) InitializeDelivery(_ context.Context, _ *sql.Tx, _ protocol.ULID, _ protocol.AgentID, generation int64) error {
	if f.err != nil {
		return f.err
	}
	f.generations = append(f.generations, generation)
	return nil
}

type fakeAdmitter struct {
	keys []string
	err  error
}

func (f *fakeAdmitter) AdmitMessage(_ context.Context, _ protocol.AgentID, raw json.RawMessage) (any, error) {
	if f.err != nil {
		return nil, f.err
	}
	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, err
	}
	key, _ := params["client_msg_id"].(string)
	f.keys = append(f.keys, key)
	return nil, nil
}

func openTurnRunner(db *sql.DB, engine *fakeEngine, admit *fakeAdmitter, mark func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error) *Runner {
	return &Runner{
		DB:       db,
		Operator: protocol.AgentID("agent-operator"),
		Notify: func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
			return Requeue(ctx, tx, fixedClock{at: loadT0}, engine, messageID, agentID)
		},
		AdmitOperator: func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error {
			return AdmitOperatorNotice(ctx, admit, tx, fixedClock{at: loadT0}, agentID, operator, messageID)
		},
		MarkEscalated: mark,
	}
}

func queuedGenerations(t *testing.T, db *sql.DB, id protocol.ULID) []int64 {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT delivery_generation FROM delivery_queue
		WHERE message_id = ? AND state = 'QUEUED' ORDER BY delivery_generation`, string(id))
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

// A due slot renotices exactly once: the second turn waits on the live
// queued generation instead of filing another.
func TestLoopRenoticesDueSlotOnce(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, allowingChannels(), baseSeed())
	engine := &fakeEngine{}
	admit := &fakeAdmitter{}
	runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(time.Minute), items)
	require.NoError(t, err)
	require.Equal(t, []int64{2}, engine.generations)
	require.Equal(t, []int64{2}, queuedGenerations(t, db, loadMsg))

	items, err = loader.Items(context.Background())
	require.NoError(t, err)
	wake, err := runner.RunOnce(context.Background(), loadT0.Add(time.Minute), items)
	require.NoError(t, err)
	require.Len(t, engine.generations, 1, "no second generation while one waits")
	require.True(t, wake.IsZero(), "held turn schedules no wake; the driver tick re-evaluates")
	require.Empty(t, admit.keys)
}

// A read message is never renotified, even when a slot is due.
func TestLoopNeverRenotifiesRead(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.acked = true
	db, loader := seedLoopDB(t, allowingChannels(), seed)
	engine := &fakeEngine{}
	runner := openTurnRunner(db, engine, &fakeAdmitter{}, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(6*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, engine.generations, "a read message is never renotified")
}

// Past the deadline the loop escalates once: a second turn sees the
// durable mark and stays quiet.
func TestLoopEscalatesOncePastDeadline(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
	db, loader := seedLoopDB(t, allowingChannels(), seed)
	engine := &fakeEngine{}
	admit := &fakeAdmitter{}
	runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Len(t, admit.keys, 1)
	require.True(t, isEscalated(t, db, loadMsg))

	items, err = loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(32*time.Minute), items)
	require.NoError(t, err)
	require.Len(t, admit.keys, 1, "durable mark suppresses the second escalation")
	require.Empty(t, engine.generations, "covered slot does not renotify")
}

// A mark failure retries with the same admission key, so the operator
// still gets one notice.
func TestLoopEscalationRetryKeepsOneNotice(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
	db, loader := seedLoopDB(t, allowingChannels(), seed)
	engine := &fakeEngine{}
	admit := &fakeAdmitter{}
	markErr := errors.New("mark unavailable")
	runner := openTurnRunner(db, engine, admit,
		func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
			if markErr != nil {
				return markErr
			}
			return escalatorMark(t, db)(ctx, tx, messageID, agentID)
		})

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.ErrorIs(t, err, markErr)

	markErr = nil
	items, err = loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Len(t, admit.keys, 2)
	require.Equal(t, admit.keys[0], admit.keys[1], "same idempotency key on retry")
	require.True(t, isEscalated(t, db, loadMsg))
}

// A denied message is dropped even past the deadline: no renotice,
// no operator notice.
func TestLoopDropsDeniedPastDeadline(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.control = "DENIED"
	seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
	db, loader := seedLoopDB(t, allowingChannels(), seed)
	engine := &fakeEngine{}
	admit := &fakeAdmitter{}
	runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, engine.generations)
	require.Empty(t, admit.keys)
	require.False(t, isEscalated(t, db, loadMsg))
}

// Pause, manual mode and pending approval all hold the loop quietly,
// even when a slot is due.
func TestLoopHoldsOnControls(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		control string
	}{
		{"held", "HELD"},
		{"approval", "APPROVAL_REQUIRED"},
		{"unknown", "WEIRD"},
	} {
		seed := baseSeed()
		seed.control = tc.control
		db, loader := seedLoopDB(t, allowingChannels(), seed)
		engine := &fakeEngine{}
		admit := &fakeAdmitter{}
		runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

		items, err := loader.Items(context.Background())
		require.NoError(t, err)
		_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
		require.NoError(t, err, tc.name)
		require.Empty(t, engine.generations, tc.name)
		require.Empty(t, admit.keys, tc.name)
	}

	manual := allowingChannels()
	manual.view.AutoDelivery = false
	db, loader := seedLoopDB(t, manual, baseSeed())
	engine := &fakeEngine{}
	runner := openTurnRunner(db, engine, &fakeAdmitter{}, escalatorMark(t, db))
	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, engine.generations, "manual")
}

// A missing fact never errors the turn: the runner holds instead of
// failing every round.
func TestLoopStaysQuietOnMissingFacts(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, scriptChannels{err: errScriptChannels}, baseSeed())
	engine := &fakeEngine{}
	admit := &fakeAdmitter{}
	runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, engine.generations)
	require.Empty(t, admit.keys)
}

// The escalation table itself is a missing fact: past the deadline
// the turn holds without admitting and without an error.
func TestLoopStaysQuietWithoutEscalationTable(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
	db, loader := seedLoopDB(t, allowingChannels(), seed)
	_, err := db.ExecContext(t.Context(), `DROP TABLE escalations`)
	require.NoError(t, err)
	engine := &fakeEngine{}
	admit := &fakeAdmitter{}
	runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, admit.keys)
}

func escalatorMark(t *testing.T, _ *sql.DB) func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
	t.Helper()
	esc, err := escalate.New(protocol.AgentID("agent-operator"), fixedClock{at: loadT0})
	require.NoError(t, err)
	return func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
		return esc.Escalate(ctx, tx, messageID, agentID)
	}
}

type fixedClock struct{ at time.Time }

func (f fixedClock) Now() time.Time { return f.at }

func isEscalated(t *testing.T, db *sql.DB, id protocol.ULID) bool {
	t.Helper()
	ok, err := escalate.Escalated(context.Background(), db, id)
	require.NoError(t, err)
	return ok
}
