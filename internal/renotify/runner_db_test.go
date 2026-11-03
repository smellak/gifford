package renotify

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
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

// countingEngine is race-safe for the concurrent enqueue canary: it
// counts filings without touching shared slices.
type countingEngine struct {
	filings atomic.Int64
}

func (c *countingEngine) InitializeDelivery(_ context.Context, _ *sql.Tx, _ protocol.ULID, _ protocol.AgentID, _ int64) error {
	c.filings.Add(1)
	return nil
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
		AdmitOperator: func(ctx context.Context, messageID protocol.ULID, agentID protocol.AgentID, operator protocol.AgentID) error {
			return AdmitOperatorNotice(ctx, admit, db, fixedClock{at: loadT0}, agentID, operator, messageID)
		},
		EnqueueOperator: func(context.Context, protocol.ULID, protocol.AgentID, protocol.AgentID) error {
			return nil
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

	db, loader := seedLoopDB(t, baseSeed())
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
	db, loader := seedLoopDB(t, seed)
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
	db, loader := seedLoopDB(t, seed)
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
	db, loader := seedLoopDB(t, seed)
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
		admit := &fakeAdmitter{}
		runner := openTurnRunner(db, engine, admit, escalatorMark(t, db))

		items, err := loader.Items(context.Background())
		require.NoError(t, err)
		_, err = runner.RunOnce(context.Background(), loadT0.Add(31*time.Minute), items)
		require.NoError(t, err, tc.name)
		require.Empty(t, engine.generations, tc.name)
		require.Empty(t, admit.keys, tc.name)
	}
}

// A missing fact never errors the turn: the runner holds instead of
// failing every round.
func TestLoopStaysQuietOnMissingFacts(t *testing.T) {
	t.Parallel()

	db, loader := seedLoopDB(t, baseSeed())
	loader.Channels = scriptChannels{err: errScriptChannels}
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
	db, loader := seedLoopDB(t, seed)
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

// TestOperatorEnqueueSerializesAgainstOpenFiling is the canary for the
// transaction-scoped gate: while another filing holds the queue row
// uncommitted, a concurrent enqueue of the same admitted notice must
// serialize behind it and back off, filing nothing. Without the gate —
// the duplicate check outside a transaction — the loser files after
// the commit and dies on the primary key, failing closed with an
// error instead of a quiet nil.
func TestOperatorEnqueueSerializesAgainstOpenFiling(t *testing.T) {
	t.Parallel()

	db, _ := seedLoopDB(t, baseSeed())
	ctx := context.Background()
	seedAdmittedNotice(t, db, "WHEN_SAFE")

	// The rival filing, by hand: begin, file, hold the transaction
	// open, commit. Its uncommitted row is invisible to the loser's
	// snapshot, forcing the overlap the gate must survive.
	rival, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = rival.ExecContext(ctx, `INSERT INTO delivery_queue(message_id, agent_id,
		delivery_generation, from_agent, thread_ref, state, priority, created_at, expires_at,
		updated_at, thread_sequence, hop_count, route_digest)
		VALUES('01ARZ3NDEKTSV4RRFFQ69G0099', 'agent-operator', 1, ?, 'thr_00000000000000000000000001',
		'QUEUED', 2, ?, ?, ?, 0, 0, 'sha256:00')`,
		string(loadAgent), eventstore.FormatTime(loadT0),
		eventstore.FormatTime(loadT0.Add(24*time.Hour)), eventstore.FormatTime(loadT0))
	require.NoError(t, err)

	engine := &countingEngine{}
	var wg sync.WaitGroup
	wg.Add(1)
	var loserErr error
	go func() {
		defer wg.Done()
		loserErr = EnqueueOperatorNotice(ctx, db, engine, fixedClock{at: loadT0},
			loadAgent, protocol.AgentID("agent-operator"), loadMsg)
	}()
	time.Sleep(100 * time.Millisecond) // the loser arrives mid-filing
	require.NoError(t, rival.Commit())
	wg.Wait()
	require.NoError(t, loserErr)
	var queued int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM delivery_queue
		WHERE message_id = '01ARZ3NDEKTSV4RRFFQ69G0099'`).Scan(&queued))
	require.Equal(t, 1, queued, "the loser files nothing behind an open filing")
	require.Equal(t, int64(0), engine.filings.Load())
}

// seedAdmittedNotice files the admitted operator notice the enqueue
// gate reads back: the notice row plus its recipient row carrying the
// given delivery mode.
func seedAdmittedNotice(t *testing.T, db *sql.DB, mode string) {
	t.Helper()
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `INSERT INTO messages(message_id, client_msg_id,
		origin_node, from_agent, destination_node, thread_ref, thread_id, priority, state,
		created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES('01ARZ3NDEKTSV4RRFFQ69G0099', ?, 'node-1', ?, 'node-1',
		'thr_00000000000000000000000001', 'thread-1', 'NORMAL', 'ADMITTED', ?, ?, ?, 0, 'sha256:00')`,
		string(escalate.ClientMsgID(loadMsg)), string(loadAgent),
		eventstore.FormatTime(loadT0), eventstore.FormatTime(loadT0.Add(24*time.Hour)),
		eventstore.FormatTime(loadT0))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO message_recipients(message_id, agent_id,
		ordinal, delivery_mode, requires_ack, created_at)
		VALUES('01ARZ3NDEKTSV4RRFFQ69G0099', 'agent-operator', 0, ?, 1, ?)`,
		mode, eventstore.FormatTime(loadT0))
	require.NoError(t, err)
}

// TestOperatorEnqueueHonoursRecipientMode is the F2 pair: a WHEN_SAFE
// operator gets the queue row filed, a DISPLAY_ONLY operator gets
// none — the enqueue honours the recorded mode instead of filing
// blindly.
func TestOperatorEnqueueHonoursRecipientMode(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"WHEN_SAFE", "DISPLAY_ONLY"} {
		db, _ := seedLoopDB(t, baseSeed())
		seedAdmittedNotice(t, db, mode)
		engine := &countingEngine{}
		err := EnqueueOperatorNotice(context.Background(), db, engine,
			fixedClock{at: loadT0}, loadAgent, protocol.AgentID("agent-operator"), loadMsg)
		require.NoError(t, err, "mode %s files closed", mode)
		var queued int
		require.NoError(t, db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM delivery_queue
			WHERE message_id = '01ARZ3NDEKTSV4RRFFQ69G0099'`).Scan(&queued))
		if mode == "WHEN_SAFE" {
			require.Equal(t, 1, queued, "a WHEN_SAFE operator gets the queue row")
			require.Equal(t, int64(1), engine.filings.Load())
		} else {
			require.Equal(t, 0, queued, "a DISPLAY_ONLY operator gets no queue row")
			require.Equal(t, int64(0), engine.filings.Load())
		}
	}
}
