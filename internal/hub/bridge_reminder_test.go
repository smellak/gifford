package hub

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	reminderProducerAgent = protocol.AgentID("response-watch")
	reminderExpectationA  = "rsp-0123456789abcdef0123456789abcdef"
	reminderExpectationB  = "rsp-0123456789abcdef0123456789abcde0"
	reminderSourceMsgA    = "987654321"
)

func newReminderFixture(t *testing.T) (*hubFixture, *Service) {
	t.Helper()
	f := newHubFixture(t)
	insertHubAgent(t, f.store, reminderProducerAgent)
	engine, err := channel.New(channel.Options{Clock: f.clock})
	require.NoError(t, err)
	service, err := NewService(ServiceOptions{
		DB: f.store.DB(), NodeID: testHubNode, Engine: engine, Approvals: f.controller,
		Registry: f.keys, Clock: f.clock, EventStore: f.store,
		ResponseWatchProducer: reminderProducerAgent,
	})
	require.NoError(t, err)
	insertBridgeActiveInstance(t, f.store, testHubRecipient)
	return f, service
}

func reminderPut(t *testing.T, params BridgeReminderParams) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	return raw
}

func validReminderParams() BridgeReminderParams {
	return BridgeReminderParams{
		Schema: reminderSchema, SourceNodeID: string(testHubNode),
		ExpectationID: reminderExpectationA, Generation: 1,
		SourceMessageID: reminderSourceMsgA, ToAgent: testHubRecipient,
		SourceReadAt: "2026-10-29T12:00:01Z", EligibleAt: "2026-10-29T12:00:02Z",
	}
}

func callPutReminder(t *testing.T, service *Service, params BridgeReminderParams) BridgeReminderReceipt {
	t.Helper()
	answer, err := service.PutReminder(context.Background(), reminderPut(t, params))
	require.NoError(t, err)
	receipt, ok := answer.(BridgeReminderReceipt)
	require.True(t, ok, "PUT must return a receipt, got %T", answer)
	return receipt
}

func requireInvalidParams(t *testing.T, err error) {
	t.Helper()
	var callErr *callError
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, codeInvalidRequest, callErr.code)
	require.Equal(t, messageInvalid, callErr.message)
}

func requireDomainError(t *testing.T, err error, code protocol.ErrorCode) {
	t.Helper()
	var callErr *callError
	require.ErrorAs(t, err, &callErr)
	require.Equal(t, code, callErr.domain)
}

func reminderRow(t *testing.T, db *sql.DB, node, expectation string) (state string) {
	t.Helper()
	require.NoError(t, db.QueryRowContext(context.Background(), `SELECT state FROM bridge_reminders
		WHERE source_node_id = ? AND expectation_id = ? AND generation = 1`,
		node, expectation).Scan(&state))
	return state
}

func TestReminderSchemaValueIsFrozen(t *testing.T) {
	t.Parallel()
	require.Equal(t, "gifford.bridge.response-reminder/v1", reminderSchema,
		"the wire schema is byte-frozen; a rename needs a contract amendment")
}

func TestReminderPutRejectsShape(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*BridgeReminderParams)
	}{
		{"wrong schema", func(p *BridgeReminderParams) { p.Schema = "other/v1" }},
		{"wrong node", func(p *BridgeReminderParams) { p.SourceNodeID = "elsewhere" }},
		{"bad expectation", func(p *BridgeReminderParams) { p.ExpectationID = "not-an-expectation" }},
		{"uuid is not an expectation", func(p *BridgeReminderParams) { p.ExpectationID = "123e4567-e89b-12d3-a456-426614174000" }},
		{"uppercase expectation", func(p *BridgeReminderParams) { p.ExpectationID = "rsp-0123456789ABCDEF0123456789ABCDEF" }},
		{"zero generation", func(p *BridgeReminderParams) { p.Generation = 0 }},
		{"bad source message", func(p *BridgeReminderParams) { p.SourceMessageID = "12x" }},
		{"bad read time", func(p *BridgeReminderParams) { p.SourceReadAt = "yesterday" }},
		{"truncated time", func(p *BridgeReminderParams) { p.EligibleAt = "2026-10-29T12:00:02" }},
		{"self notify", func(p *BridgeReminderParams) { p.ToAgent = reminderProducerAgent }},
		{"unknown to", func(p *BridgeReminderParams) { p.ToAgent = "ghost-agent-x" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, service := newReminderFixture(t)
			params := validReminderParams()
			test.mutate(&params)
			_, err := service.PutReminder(context.Background(), reminderPut(t, params))
			require.Error(t, err)
		})
	}
}

func TestReminderPutRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	raw := `{"schema":"gifford.bridge.response-reminder/v1",` +
		`"source_node_id":"node-hub-test","expectation_id":"123e4567-e89b-12d3-a456-426614174000",` +
		`"generation":1,"source_message_id":"987654321","to_agent":"agent-recipient",` +
		`"source_read_at":"2026-10-29T12:00:01Z","eligible_at":"2026-10-29T12:00:02Z",` +
		`"from_agent":"response-watch","text":"hi","body":"x","keys":[],"command":"run","path":"/x"}`
	_, err := service.PutReminder(context.Background(), json.RawMessage(raw))
	require.Error(t, err)
	requireInvalidParams(t, err)
	var count int
	require.NoError(t, service.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM bridge_reminders`).Scan(&count))
	require.Zero(t, count, "a rejected PUT must not store anything")
}

func TestReminderPutCreatesDurably(t *testing.T) {
	t.Parallel()
	f, service := newReminderFixture(t)
	receipt := callPutReminder(t, service, validReminderParams())
	require.Equal(t, reminderDispositionCreated, receipt.Disposition)
	require.Equal(t, reminderStatePending, receipt.State)
	require.Nil(t, receipt.DeliveryAttemptID)
	require.Equal(t, eventstore.FormatTime(f.clock.now), receipt.AcceptedAt)
	require.Equal(t, eventstore.FormatTime(f.clock.now.Add(reminderTTL)), receipt.ExpiresAt)
	require.Equal(t, 15*time.Minute, reminderTTL, "the server TTL is fixed at fifteen minutes")

	var messageState, queueState, mapping string
	require.NoError(t, f.store.DB().QueryRowContext(context.Background(), `SELECT state FROM messages
		WHERE message_id = ?`, receipt.GiffordMessageID).Scan(&messageState))
	require.Equal(t, "PENDING", messageState)
	require.NoError(t, f.store.DB().QueryRowContext(context.Background(), `SELECT state, expires_at FROM delivery_queue
		WHERE message_id = ?`, receipt.GiffordMessageID).Scan(&queueState, &mapping))
	require.Equal(t, "QUEUED", queueState)
	require.Equal(t, receipt.ExpiresAt, mapping, "the queue row carries the reminder TTL")
	var fromAgent string
	require.NoError(t, f.store.DB().QueryRowContext(context.Background(), `SELECT from_agent FROM bridge_reminders
		WHERE reminder_id = ?`, receipt.ReminderID).Scan(&fromAgent))
	require.Equal(t, string(reminderProducerAgent), fromAgent, "the producer derives from config, never the caller")
}

func TestReminderPutReplayIsDuplicate(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	first := callPutReminder(t, service, params)
	answer, err := service.PutReminder(context.Background(), reminderPut(t, params))
	require.NoError(t, err)
	second := answer.(BridgeReminderReceipt)
	require.Equal(t, reminderDispositionDuplicate, second.Disposition)
	require.Equal(t, first.ReminderID, second.ReminderID)
	require.Equal(t, first.GiffordMessageID, second.GiffordMessageID)
	var count int
	require.NoError(t, service.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM bridge_reminders`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestReminderPutConflictKeepsHistory(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	first := callPutReminder(t, service, params)
	params.SourceMessageID = "111222333"
	_, err := service.PutReminder(context.Background(), reminderPut(t, params))
	require.Error(t, err)
	requireDomainError(t, err, protocol.ErrorCodeControlVersionConflict)
	get, err := service.GetReminder(context.Background(), reminderKey(t, params))
	require.NoError(t, err)
	require.Equal(t, first.GiffordMessageID, get.(BridgeReminderReceipt).GiffordMessageID)
}

func reminderKey(t *testing.T, params BridgeReminderParams) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(BridgeReminderKey{
		SourceNodeID: params.SourceNodeID, ExpectationID: params.ExpectationID,
		Generation: params.Generation,
	})
	require.NoError(t, err)
	return raw
}

func TestReminderPutConcurrentIsSingleCreator(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	raw := reminderPut(t, params)
	var wg sync.WaitGroup
	results := make([]any, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = service.PutReminder(context.Background(), raw)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	dispositions := map[string]int{}
	for _, result := range results {
		dispositions[result.(BridgeReminderReceipt).Disposition]++
	}
	require.Equal(t, map[string]int{reminderDispositionCreated: 1, reminderDispositionDuplicate: 1}, dispositions)
	var count int
	require.NoError(t, service.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM bridge_reminders`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestReminderGetRoundTrip(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	got, err := service.GetReminder(context.Background(), reminderKey(t, params))
	require.NoError(t, err)
	receipt := got.(BridgeReminderReceipt)
	require.Equal(t, created.ReminderID, receipt.ReminderID)
	require.Equal(t, reminderStatePending, receipt.State)
	_, err = service.GetReminder(context.Background(), reminderKey(t, BridgeReminderParams{
		SourceNodeID: params.SourceNodeID, ExpectationID: reminderExpectationB, Generation: 1,
	}))
	require.Error(t, err)
	requireDomainError(t, err, protocol.ErrorCodeNotFound)
}

// An expired reminder whose worker attempt parked UNCERTAIN reads
// UNCERTAIN, never EXPIRED: the idle queue row does not clear a sealed
// commit frame.
func TestReminderGetExpiredUncertainWorkerIsUncertain(t *testing.T) {
	t.Parallel()
	f, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	f.clock.now = f.clock.now.Add(16 * time.Minute)
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_attempts SET state = 'UNCERTAIN'
		WHERE message_id = (SELECT gifford_message_id FROM bridge_reminders
		WHERE source_node_id = ? AND expectation_id = ? AND generation = 1)`,
		params.SourceNodeID, params.ExpectationID)
	require.NoError(t, err)
	got, err := service.GetReminder(context.Background(), reminderKey(t, params))
	require.NoError(t, err)
	require.Equal(t, reminderStateUncertain, got.(BridgeReminderReceipt).State)
}

func TestReminderCancelBeforeEffect(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelResolved))
	require.NoError(t, err)
	receipt := cancelled.(BridgeReminderReceipt)
	require.Equal(t, reminderStateCancelled, receipt.State)
	require.Equal(t, created.ReminderID, receipt.ReminderID)
	require.Equal(t, "CANCELLED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
	var queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "CANCELLED", queueState, "no worker can prepare it after")
	got, err := service.GetReminder(context.Background(), reminderKey(t, params))
	require.NoError(t, err)
	require.Equal(t, reminderStateCancelled, got.(BridgeReminderReceipt).State)
}

func reminderCancel(t *testing.T, params BridgeReminderParams, reason string) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(BridgeReminderCancelParams{
		SourceNodeID: params.SourceNodeID, ExpectationID: params.ExpectationID,
		Generation: params.Generation, Reason: reason,
	})
	require.NoError(t, err)
	return raw
}

// transitionReminderAttempt moves the worker side through the engine
// attempt store, so cancel tests meet the same serialization production
// does: the worker drives Transition, the bridge drives Transition, and
// the loser of the compare-and-swap defines the outcome.
func transitionReminderAttempt(t *testing.T, service *Service, messageID protocol.ULID, from, to daemon.AttemptState, reason daemon.AttemptReason) {
	t.Helper()
	tx, err := service.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, service.attempts.Transition(context.Background(), tx,
		protocol.OpaqueID("attempt-"+string(messageID)),
		daemon.Transition{From: from, To: to, Reason: reason}))
	require.NoError(t, tx.Commit())
}

// uncertainWorkerAttempt replays the engine path of an uncertain commit:
// the worker seals the commit frame, the attempt parks UNCERTAIN, and the
// queue returns to QUEUED the way the executor and the restart recovery
// leave it.
func uncertainWorkerAttempt(t *testing.T, service *Service, messageID protocol.ULID) {
	t.Helper()
	transitionReminderAttempt(t, service, messageID,
		daemon.AttemptPrepared, daemon.AttemptCommitSent, "")
	tx, err := service.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, service.attempts.MarkUncertain(context.Background(), tx,
		protocol.OpaqueID("attempt-"+string(messageID)), daemon.ReasonTimedOut))
	require.NoError(t, tx.Commit())
	resetReminderQueue(t, service, messageID)
}

// resetReminderQueue walks the queue DELIVERING back to QUEUED, the way the
// executor and the restart recovery leave it after an uncertain commit.
func resetReminderQueue(t *testing.T, service *Service, messageID protocol.ULID) {
	t.Helper()
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'DELIVERING'
		WHERE message_id = ?`, messageID)
	require.NoError(t, err)
	_, err = service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'QUEUED'
		WHERE message_id = ?`, messageID)
	require.NoError(t, err)
}

func insertReminderAttempt(t *testing.T, db *sql.DB, messageID protocol.ULID) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation, input_guard_version,
		 control_versions_json, state, created_at, updated_at)
		VALUES (?, ?, ?, 1, 'adapter-test', 'orch-test', 'chan-test', 0, 0, '{}', ?, ?, ?)`,
		"attempt-"+string(messageID), messageID, testHubRecipient, "PREPARED",
		"2026-10-29T12:00:00Z", "2026-10-29T12:00:00Z")
	require.NoError(t, err)
}

func TestReminderCancelPreparedIdleCancels(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateCancelled, cancelled.(BridgeReminderReceipt).State,
		"a prepared attempt with an idle queue retires with zero effect")
	var attemptState, queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState))
	require.Equal(t, "CANCELLED", attemptState)
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "CANCELLED", queueState)
	require.Equal(t, "CANCELLED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

// A claimed queue means the guard may be past the commit frame even when
// the attempt row still reads PREPARED: the cancel wins the attempt
// compare-and-swap, loses the queue one, and the reminder freezes
// UNCERTAIN instead of reporting a silent cancel.
func TestReminderCancelDuringPreparingIsUncertain(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'CLAIMED'
		WHERE message_id = ?`, created.GiffordMessageID)
	require.NoError(t, err)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateUncertain, cancelled.(BridgeReminderReceipt).State)
	var attemptState, queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState))
	require.Equal(t, "CANCELLED", attemptState, "the won attempt CAS stays")
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "CLAIMED", queueState, "a claimed queue row is never rewritten")
	require.Equal(t, "UNCERTAIN", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

func TestReminderCancelDuringCommitIsUncertain(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	transitionReminderAttempt(t, service, created.GiffordMessageID,
		daemon.AttemptPrepared, daemon.AttemptCommitSent, "")
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'DELIVERING'
		WHERE message_id = ?`, created.GiffordMessageID)
	require.NoError(t, err)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateUncertain, cancelled.(BridgeReminderReceipt).State)
	var attemptState, queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState))
	require.Equal(t, "COMMIT_SENT", attemptState, "a possible effect is never rewritten")
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "DELIVERING", queueState)
}

func TestReminderCancelAfterDeliveryReportsDelivered(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ?`, created.GiffordMessageID)
	require.NoError(t, err)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelResolved))
	require.NoError(t, err)
	require.Equal(t, reminderStateDelivered, cancelled.(BridgeReminderReceipt).State)
	require.Equal(t, "DELIVERED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

func TestReminderExpiryRetiresWithoutEffect(t *testing.T) {
	t.Parallel()
	f, service := newReminderFixture(t)
	params := validReminderParams()
	callPutReminder(t, service, params)
	f.clock.now = f.clock.now.Add(16 * time.Minute)
	reconciled, err := service.ReconcileReminders(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, reconciled)
	require.Equal(t, "EXPIRED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
	var queueState, attempts string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = (SELECT gifford_message_id FROM bridge_reminders
		WHERE source_node_id = ? AND expectation_id = ?)`,
		params.SourceNodeID, params.ExpectationID).Scan(&queueState))
	require.Equal(t, "EXPIRED", queueState)
	require.NoError(t, service.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM delivery_attempts`).Scan(&attempts))
	require.Equal(t, "0", attempts)
	got, err := service.GetReminder(context.Background(), reminderKey(t, params))
	require.NoError(t, err)
	require.Equal(t, reminderStateExpired, got.(BridgeReminderReceipt).State)
}

func TestReminderExpiryCancelsPreparingAttempt(t *testing.T) {
	t.Parallel()
	f, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	f.clock.now = f.clock.now.Add(16 * time.Minute)
	reconciled, err := service.ReconcileReminders(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, reconciled)
	var attemptState, queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState))
	require.Equal(t, "CANCELLED", attemptState)
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "EXPIRED", queueState)
	require.Equal(t, "EXPIRED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

// An expired reminder whose queue the guard already claimed parks
// UNCERTAIN: the attempt compare-and-swap still wins, but a claimed queue
// means the panel may already hold the effect.
func TestReminderExpiryClaimedIsUncertain(t *testing.T) {
	t.Parallel()
	f, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'CLAIMED'
		WHERE message_id = ?`, created.GiffordMessageID)
	require.NoError(t, err)
	f.clock.now = f.clock.now.Add(16 * time.Minute)
	reconciled, err := service.ReconcileReminders(context.Background())
	require.NoError(t, err)
	require.Zero(t, reconciled, "parking UNCERTAIN is not an advance")
	require.Equal(t, "UNCERTAIN", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
	var attemptState, queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState))
	require.Equal(t, "CANCELLED", attemptState, "the won attempt CAS stays")
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "CLAIMED", queueState, "a claimed queue row is never rewritten")
}

// A worker attempt that sealed its commit frame and parked UNCERTAIN, with
// the queue back to QUEUED the way the executor leaves it, is a possible
// panel effect even though no row reads COMMIT_SENT: the cancel freezes
// UNCERTAIN and rewrites nothing.
func TestReminderCancelUncertainWorkerIsUncertain(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	uncertainWorkerAttempt(t, service, created.GiffordMessageID)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateUncertain, cancelled.(BridgeReminderReceipt).State)
	var attemptState, commitSealed string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state,
		CASE WHEN commit_sent_at IS NOT NULL THEN 'sealed' ELSE 'open' END FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState, &commitSealed))
	require.Equal(t, "UNCERTAIN", attemptState, "the worker attempt is never rewritten")
	require.Equal(t, "sealed", commitSealed, "the commit frame survives the uncertain parking")
	var queueState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_queue
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&queueState))
	require.Equal(t, "QUEUED", queueState, "an idle queue row is never retired over a possible effect")
}

// The restart recovery parks the same shape: COMMIT_SENT becomes UNCERTAIN
// with its commit frame sealed and the queue returns to QUEUED.
func TestReminderCancelAfterRestartRecoveryIsUncertain(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	transitionReminderAttempt(t, service, created.GiffordMessageID,
		daemon.AttemptPrepared, daemon.AttemptCommitSent, "")
	tx, err := service.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	func() {
		defer func() { _ = tx.Rollback() }()
		recovered, err := service.attempts.RecoverCommitSent(context.Background(), tx, daemon.ReasonProcessRestart)
		require.NoError(t, err)
		require.Equal(t, int64(1), recovered)
		require.NoError(t, tx.Commit())
	}()
	resetReminderQueue(t, service, created.GiffordMessageID)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateUncertain, cancelled.(BridgeReminderReceipt).State)
	var attemptState string
	require.NoError(t, service.db.QueryRowContext(context.Background(), `SELECT state FROM delivery_attempts
		WHERE message_id = ?`, created.GiffordMessageID).Scan(&attemptState))
	require.Equal(t, "UNCERTAIN", attemptState)
}

// A FAILED attempt that sealed its commit frame first still counts: the
// frame, not the final state, decides.
func TestReminderCancelFailedAfterCommitIsUncertain(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	transitionReminderAttempt(t, service, created.GiffordMessageID,
		daemon.AttemptPrepared, daemon.AttemptCommitSent, "")
	transitionReminderAttempt(t, service, created.GiffordMessageID,
		daemon.AttemptCommitSent, daemon.AttemptFailed, daemon.ReasonTimedOut)
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateUncertain, cancelled.(BridgeReminderReceipt).State)
}

// A CANCELLED attempt that never sealed a commit frame stays silent: the
// closed list must not turn an effect-free history into UNCERTAIN.
func TestReminderCancelCancelledWithoutCommitStaysSilent(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	insertReminderAttempt(t, service.db, created.GiffordMessageID)
	transitionReminderAttempt(t, service, created.GiffordMessageID,
		daemon.AttemptPrepared, daemon.AttemptCancelled, "reminder_cancelled")
	cancelled, err := service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelMode))
	require.NoError(t, err)
	require.Equal(t, reminderStateCancelled, cancelled.(BridgeReminderReceipt).State)
	require.Equal(t, "CANCELLED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

// An unreadable effect check fails closed: the reminder parks UNCERTAIN
// and the read error still travels up instead of a silent cancel.
func TestReminderCancelReadFailureParksUncertain(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	callPutReminder(t, service, params)
	_, err := service.db.ExecContext(context.Background(), `DROP TABLE delivery_attempts`)
	require.NoError(t, err)
	_, err = service.CancelReminder(context.Background(), reminderCancel(t, params, reminderCancelResolved))
	require.Error(t, err, "an unreadable effect check fails closed")
	require.Equal(t, "UNCERTAIN", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

func TestReminderReconcileLoopAdvancesAndStops(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ?`, created.GiffordMessageID)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var mu sync.Mutex
	reports := 0
	go func() {
		defer close(done)
		service.ReconcileRemindersLoop(ctx, 5*time.Millisecond, func(error) {
			mu.Lock()
			defer mu.Unlock()
			reports++
		})
	}()
	deadline := time.Now().Add(5 * time.Second)
	for reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID) != "DELIVERED" {
		if time.Now().After(deadline) {
			t.Fatal("the loop did not materialize DELIVERED")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the loop did not stop")
	}
}

func TestReminderDeliveredThroughQueue(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	created := callPutReminder(t, service, params)
	_, err := service.db.ExecContext(context.Background(), `UPDATE delivery_queue SET state = 'DELIVERED'
		WHERE message_id = ?`, created.GiffordMessageID)
	require.NoError(t, err)
	got, err := service.GetReminder(context.Background(), reminderKey(t, params))
	require.NoError(t, err)
	require.Equal(t, reminderStateDelivered, got.(BridgeReminderReceipt).State)
	reconciled, err := service.ReconcileReminders(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, reconciled)
	require.Equal(t, "DELIVERED", reminderRow(t, service.db, params.SourceNodeID, params.ExpectationID))
}

func TestReminderGuardsScope(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	params.ToAgent = "ghost-agent-no-instance"
	_, err := service.PutReminder(context.Background(), reminderPut(t, params))
	require.Error(t, err)
	requireDomainError(t, err, protocol.ErrorCodeNotFound)
}

func TestReminderInvalidCancelReason(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	params := validReminderParams()
	callPutReminder(t, service, params)
	_, err := service.CancelReminder(context.Background(), reminderCancel(t, params, "BECAUSE"))
	require.Error(t, err)
	requireInvalidParams(t, err)
	_, err = service.CancelReminder(context.Background(), reminderCancel(t, BridgeReminderParams{
		SourceNodeID: params.SourceNodeID, ExpectationID: reminderExpectationB, Generation: 1,
	}, reminderCancelResolved))
	require.Error(t, err)
	requireDomainError(t, err, protocol.ErrorCodeNotFound)
}

func TestReminderMalformedRPC(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	_, err := service.PutReminder(context.Background(), json.RawMessage(`{"schema":`))
	require.Error(t, err)
	_, err = service.GetReminder(context.Background(), json.RawMessage(`{"source_node_id":`))
	require.Error(t, err)
	_, err = service.CancelReminder(context.Background(), json.RawMessage(`{"reason":`))
	require.Error(t, err)
	var count int
	require.NoError(t, service.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM bridge_reminders`).Scan(&count))
	require.Zero(t, count)
}

func TestReminderSingleDeliveredPerExpectation(t *testing.T) {
	t.Parallel()
	_, service := newReminderFixture(t)
	ctx := context.Background()
	params := validReminderParams()
	callPutReminder(t, service, params)
	params.Generation = 2
	params.SourceMessageID = "222333444"
	callPutReminder(t, service, params)
	_, err := service.db.ExecContext(ctx, `UPDATE bridge_reminders SET state = 'DELIVERED',
		delivered_at = accepted_at, terminal_at = accepted_at
		WHERE source_node_id = ? AND expectation_id = ? AND generation = 1`,
		params.SourceNodeID, params.ExpectationID)
	require.NoError(t, err)
	_, err = service.db.ExecContext(ctx, `UPDATE bridge_reminders SET state = 'DELIVERED',
		delivered_at = accepted_at, terminal_at = accepted_at
		WHERE source_node_id = ? AND expectation_id = ? AND generation = 2`,
		params.SourceNodeID, params.ExpectationID)
	require.Error(t, err, "the base itself refuses a second DELIVERED reminder")
}
