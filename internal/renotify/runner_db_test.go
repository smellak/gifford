package renotify

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/escalate"
	"github.com/smellak/gifford/internal/eventstore"
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

type fakeFiler struct {
	keys  []string
	since []time.Time
	err   error
}

func (f *fakeFiler) file(_ context.Context, id protocol.ULID, _ protocol.AgentID, _ protocol.AgentID, since time.Time) error {
	if f.err != nil {
		return f.err
	}
	f.keys = append(f.keys, "operator-notice:"+string(id))
	f.since = append(f.since, since)
	return nil
}

func openTurnRunner(db *sql.DB, engine *fakeEngine, file *fakeFiler, mark func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error) *Runner {
	return &Runner{
		DB:        db,
		Operators: []protocol.AgentID{loopOperator},
		Notify: func(ctx context.Context, tx *sql.Tx, messageID protocol.ULID, agentID protocol.AgentID) error {
			return Requeue(ctx, tx, fixedClock{at: loadT0}, engine, messageID, agentID)
		},
		FileOperatorNotice: file.file,
		MarkEscalated:      mark,
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

	db, loader := seedLoopDB(t, baseSeed())
	engine := &fakeEngine{}
	file := &fakeFiler{}
	runner := openTurnRunner(db, engine, file, escalatorMark(t, db))

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
	require.Empty(t, file.keys)
}

// A read message is never renotified, even when a slot is due.
func TestLoopNeverRenotifiesRead(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.acked = true
	db, loader := seedLoopDB(t, seed)
	engine := &fakeEngine{}
	runner := openTurnRunner(db, engine, &fakeFiler{}, escalatorMark(t, db))

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
	db, loader := seedLoopDB(t, seed)
	engine := &fakeEngine{}
	file := &fakeFiler{}
	runner := openTurnRunner(db, engine, file, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Len(t, file.keys, 1)
	require.True(t, isEscalated(t, db, loadMsg))

	items, err = loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(32*time.Minute), items)
	require.NoError(t, err)
	require.Len(t, file.keys, 1, "durable mark suppresses the second escalation")
	require.Empty(t, engine.generations, "covered slot does not renotify")
}

// A mark failure retries with the same admission key, so the operator
// still gets one notice.
func TestLoopEscalationRetryKeepsOneNotice(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
	db, loader := seedLoopDB(t, seed)
	engine := &fakeEngine{}
	file := &fakeFiler{}
	markErr := errors.New("mark unavailable")
	runner := openTurnRunner(db, engine, file,
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
	require.Len(t, file.keys, 2)
	require.Equal(t, file.keys[0], file.keys[1], "same idempotency key on retry")
	require.True(t, isEscalated(t, db, loadMsg))
}

// A held delivery and a manual agent both hold the loop quietly, even
// past the deadline: no renotice, no operator notice.
func TestLoopHoldsOnControls(t *testing.T) {
	t.Parallel()

	held := baseSeed()
	held.allowed = false

	manual := baseSeed()
	manual.setup = setupManual

	for _, tc := range []struct {
		name string
		seed seedMessage
	}{
		{"held", held},
		{"manual", manual},
	} {
		db, loader := seedLoopDB(t, tc.seed)
		engine := &fakeEngine{}
		file := &fakeFiler{}
		runner := openTurnRunner(db, engine, file, escalatorMark(t, db))

		items, err := loader.Items(context.Background())
		require.NoError(t, err)
		_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
		require.NoError(t, err, tc.name)
		require.Empty(t, engine.generations, tc.name)
		require.Empty(t, file.keys, tc.name)
	}
}

// A missing fact never errors the turn: the runner holds instead of
// failing every round.
func TestLoopStaysQuietOnMissingFacts(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed())
	loader.Channels = scriptChannels{err: errScriptChannels}
	engine := &fakeEngine{}
	file := &fakeFiler{}
	runner := openTurnRunner(db, engine, file, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, engine.generations)
	require.Empty(t, file.keys)
}

// The escalation table itself is a missing fact: past the deadline
// the turn holds without admitting and without an error.
func TestLoopStaysQuietWithoutEscalationTable(t *testing.T) {
	t.Parallel()

	seed := baseSeed()
	seed.notices = []time.Time{loadT0.Add(15 * time.Minute)}
	db, loader := seedLoopDB(t, seed)
	_, err := db.ExecContext(t.Context(), `DROP TABLE escalations`)
	require.NoError(t, err)
	engine := &fakeEngine{}
	file := &fakeFiler{}
	runner := openTurnRunner(db, engine, file, escalatorMark(t, db))

	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, file.keys)
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

// An acknowledgement landing between the load and the renotice files
// nothing: the turn must not queue a generation the worker would
// deliver after the ack. This fails if Requeue ever skips its check
// for an acknowledged recipient.
func TestLoopSkipsRenoticeWhenAckLandsAfterLoad(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed())
	items, err := loader.Items(context.Background())
	require.NoError(t, err)
	require.Len(t, items, 1, "loaded before the ack lands")
	// The ack commits after the load, before the turn files: the same
	// row the ack path writes for the delivered generation.
	_, err = db.ExecContext(context.Background(), `INSERT INTO acks(message_id, agent_id,
		delivery_generation, status, acked_at) VALUES(?, ?, 1, 'ACKNOWLEDGED', ?)`,
		string(loadMsg), string(loadAgent), eventstore.FormatTime(loadT0.Add(5*time.Minute)))
	require.NoError(t, err)

	engine := &fakeEngine{}
	runner := openTurnRunner(db, engine, &fakeFiler{}, escalatorMark(t, db))
	_, err = runner.RunOnce(context.Background(), loadT0.Add(6*time.Minute), items)
	require.NoError(t, err)
	require.Empty(t, engine.generations, "no generation filed after the ack")
	rows, err := db.QueryContext(context.Background(), `SELECT delivery_generation FROM delivery_queue
		WHERE message_id = ? ORDER BY delivery_generation`, string(loadMsg))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var gens []int64
	for rows.Next() {
		var g int64
		require.NoError(t, rows.Scan(&g))
		gens = append(gens, g)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1}, gens, "the delivered generation stands alone")
}

func isEscalated(t *testing.T, db *sql.DB, id protocol.ULID) bool {
	t.Helper()
	ok, err := escalate.Escalated(context.Background(), db, id)
	require.NoError(t, err)
	return ok
}
