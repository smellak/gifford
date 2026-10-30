package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

var notificationTestTime = time.Date(2026, time.October, 30, 23, 45, 0, 0, time.UTC)

type notificationTestClock struct {
	mu    sync.Mutex
	now   time.Time
	ticks chan time.Time
}

func (c *notificationTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *notificationTestClock) After(time.Duration) <-chan time.Time { return c.ticks }

func (c *notificationTestClock) advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type recordingAdapter struct {
	mu    sync.Mutex
	calls []Notification
	err   error
}

func (*recordingAdapter) Name() AdapterKind { return AdapterTMUX }

func (a *recordingAdapter) Render(_ context.Context, notification Notification) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	notification.LocalRecipients = append([]protocol.AgentID(nil), notification.LocalRecipients...)
	a.calls = append(a.calls, notification)
	return a.err
}

func (a *recordingAdapter) snapshot() []Notification {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Notification(nil), a.calls...)
}

type notificationFixture struct {
	store      *eventstore.Store
	clock      *notificationTestClock
	controller *channel.Controller
}

func newNotificationFixture(t *testing.T) *notificationFixture {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "notifications.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	clock := &notificationTestClock{now: notificationTestTime, ticks: make(chan time.Time)}
	engine, err := channel.New(channel.Options{Clock: clock})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock, NodeID: "node-local",
		Entropy: bytes.NewReader(bytes.Repeat([]byte{0x41}, 32_000)),
	})
	require.NoError(t, err)
	return &notificationFixture{
		store: store, clock: clock, controller: controller,
	}
}

func newNotificationService(
	t *testing.T,
	fixture *notificationFixture,
	adapter Adapter,
	seed byte,
	mutate func(*ServiceOptions),
) *Service {
	t.Helper()
	opts := ServiceOptions{
		Store: fixture.store, NodeID: "node-local", Clock: fixture.clock,
		Adapter: adapter, Entropy: bytes.NewReader(bytes.Repeat([]byte{seed}, 32_000)),
		BatchSize: 7,
	}
	if mutate != nil {
		mutate(&opts)
	}
	service, err := NewService(opts)
	require.NoError(t, err)
	return service
}

func notificationMessageID(number int) protocol.ULID {
	return protocol.ULID(fmt.Sprintf("01JB0%021d", number))
}

type sourceMessage struct {
	id         protocol.ULID
	control    protocol.ControlState
	recipients []protocol.DeliveryMode
	envelope   bool
	canary     string
}

func insertSourceMessage(t *testing.T, fixture *notificationFixture, source sourceMessage) {
	t.Helper()
	stamp := eventstore.FormatTime(fixture.clock.Now())
	expires := eventstore.FormatTime(fixture.clock.Now().Add(time.Hour))
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		 thread_ref, priority, state, created_at, expires_at, updated_at,
		 hop_count, route_digest, control_state, control_reason, control_version)
		VALUES (?, ?, 'node-origin', 'agent-sender', 'node-local', ?, 'HIGH',
		 'PENDING', ?, ?, ?, 0, 'sha256:route', ?, 'TEST_REASON', 1)`,
		source.id, "client-"+string(source.id), "thr_"+string(source.id),
		stamp, expires, stamp, source.control)
	require.NoError(t, err)
	for ordinal, mode := range source.recipients {
		agent := protocol.AgentID(fmt.Sprintf("agent-recipient-%d", ordinal))
		_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO message_recipients
			(message_id, agent_id, ordinal, delivery_mode, requires_ack, created_at)
			VALUES (?, ?, ?, ?, 0, ?)`, source.id, agent, ordinal, mode, stamp)
		require.NoError(t, err)
	}
	if !source.envelope {
		return
	}
	recipientIDs := make([]protocol.AgentID, 0, len(source.recipients))
	for ordinal := range source.recipients {
		recipientIDs = append(recipientIDs, protocol.AgentID(fmt.Sprintf("agent-recipient-%d", ordinal)))
	}
	recipientsJSON, err := json.Marshal(recipientIDs)
	require.NoError(t, err)
	envelopeBytes := []byte("opaque-envelope:" + source.canary)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO message_envelopes
		(message_id, envelope, protocol, version, client_msg_id, origin_node,
		 origin_agent, destination_node, recipient_agents_json, created_at,
		 expires_at, priority, thread_ref, encryption_profile,
		 sender_signing_key_id, ciphertext_sha256, stored_at, hop_count,
		 route_digest, wire_control_version, control_state, control_reason,
		 control_version)
		VALUES (?, ?, 'gifford-message', 1, ?, 'node-origin', 'agent-sender',
		 'node-local', ?, ?, ?, 'HIGH', ?, 'none',
		 'ed25519:agent-sender:v1', 'sha256:ciphertext', ?, 0,
		 'sha256:route', 1, ?, 'TEST_REASON', 1)`,
		source.id, envelopeBytes, "client-"+string(source.id), string(recipientsJSON),
		stamp, expires, "thr_"+string(source.id), stamp, source.control)
	require.NoError(t, err)
}

func createPendingApproval(
	t *testing.T,
	fixture *notificationFixture,
	messageID protocol.ULID,
	recipient protocol.AgentID,
	requestNote string,
) protocol.ApprovalID {
	t.Helper()
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	request, err := fixture.controller.CreateOrchestratorApproval(
		t.Context(), tx, messageID, recipient, requestNote,
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, request.ApprovalID.Valid())
	return request.ApprovalID
}

func notificationCount(t *testing.T, fixture *notificationFixture, class Class) int {
	t.Helper()
	var count int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM human_notifications WHERE class = ?", class).Scan(&count))
	return count
}

func eventCount(t *testing.T, fixture *notificationFixture, eventType protocol.EventType) int {
	t.Helper()
	var count int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT count(*) FROM events WHERE event_type = ?", eventType).Scan(&count))
	return count
}

func TestFirstStartBaselinesHistoryWithoutEffects(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	historicalID := notificationMessageID(1)
	insertSourceMessage(t, fixture, sourceMessage{
		id: historicalID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	createPendingApproval(t, fixture, historicalID,
		"agent-recipient-0", "historical request note")
	adapter := &recordingAdapter{}
	service := newNotificationService(t, fixture, adapter, 1, nil)
	count, err := service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	require.Empty(t, adapter.snapshot())
	require.Equal(t, 1, notificationCount(t, fixture, ClassMessage))
	require.Equal(t, 1, notificationCount(t, fixture, ClassApproval))
	var nonBaseline int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM human_notifications WHERE class <> 'SERVICE'
		AND (state <> 'BASELINED' OR reason <> 'BASELINE')`).Scan(&nonBaseline))
	require.Zero(t, nonBaseline)
	require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))
	require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationAcknowledged))
	var notifiedAtCount int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
		FROM messages WHERE human_notified_at IS NOT NULL`).Scan(&notifiedAtCount))
	require.Zero(t, notifiedAtCount)
	restartedAdapter := &recordingAdapter{}
	restarted := newNotificationService(t, fixture, restartedAdapter, 10, nil)
	count, err = restarted.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	require.Empty(t, restartedAdapter.snapshot())
	require.Equal(t, 1, notificationCount(t, fixture, ClassMessage))
	require.Equal(t, 1, notificationCount(t, fixture, ClassApproval))

	newID := notificationMessageID(2)
	insertSourceMessage(t, fixture, sourceMessage{
		id: newID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	_, err = restarted.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, restartedAdapter.snapshot(), 1)
	var state State
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM human_notifications WHERE class = 'MESSAGE' AND object_id = ?`, newID).Scan(&state))
	require.Equal(t, StateRendered, state)
	require.Equal(t, 1, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))
	require.Equal(t, 1, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
}

func TestConcurrentAdmissionFallsAfterAtomicBaselineCut(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	adapter := &recordingAdapter{}
	messageID := notificationMessageID(3)
	insertSourceMessage(t, fixture, sourceMessage{
		id: messageID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	type approvalWriteResult struct {
		id  protocol.ApprovalID
		err error
	}
	writeDone := make(chan approvalWriteResult, 1)
	service := newNotificationService(t, fixture, adapter, 11, func(opts *ServiceOptions) {
		opts.duringBaseline = func() {
			go func() {
				tx, err := fixture.store.DB().BeginTx(context.Background(), nil)
				if err != nil {
					writeDone <- approvalWriteResult{err: err}
					return
				}
				defer func() { _ = tx.Rollback() }()
				request, err := fixture.controller.CreateOrchestratorApproval(
					context.Background(), tx, messageID, "agent-recipient-0", "post-cut note",
				)
				if err == nil {
					err = tx.Commit()
				}
				writeDone <- approvalWriteResult{id: request.ApprovalID, err: err}
			}()
		}
	})

	count, err := service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Zero(t, count)
	writeResult := <-writeDone
	require.NoError(t, writeResult.err)
	require.Zero(t, notificationCount(t, fixture, ClassApproval),
		"an admission after the transaction snapshot is not historical")

	count, err = service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Len(t, adapter.snapshot(), 1)
	var state State
	var reason Reason
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, reason
		FROM human_notifications WHERE class = 'APPROVAL' AND object_id = ?`, writeResult.id).
		Scan(&state, &reason))
	require.Equal(t, StateRendered, state)
	require.Equal(t, ReasonApprovalPending, reason)
}

func TestNewApprovalAfterGenericMessageGetsIndependentNotice(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	adapter := &recordingAdapter{}
	service := newNotificationService(t, fixture, adapter, 12, nil)
	require.NoError(t, service.Initialize(t.Context()))
	messageID := notificationMessageID(4)
	insertSourceMessage(t, fixture, sourceMessage{
		id: messageID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	_, err := service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, adapter.snapshot(), 1)
	require.Equal(t, 1, notificationCount(t, fixture, ClassMessage))
	require.Zero(t, notificationCount(t, fixture, ClassApproval))

	createPendingApproval(t, fixture, messageID, "agent-recipient-0", "private note")
	_, err = service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, adapter.snapshot(), 2)
	require.Equal(t, 1, notificationCount(t, fixture, ClassMessage))
	require.Equal(t, 1, notificationCount(t, fixture, ClassApproval))
	require.Equal(t, 2, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))
	require.Equal(t, 2, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
}

func TestCandidatesAggregateAndExcludePrivateText(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	adapter := &recordingAdapter{}
	var logged []error
	service := newNotificationService(t, fixture, adapter, 2, func(opts *ServiceOptions) {
		opts.ErrorSink = func(err error) { logged = append(logged, err) }
	})
	require.NoError(t, service.Initialize(t.Context()))

	bodyCanary := "BODY-CANARY-NEVER-RENDER"
	noteCanary := "REQUEST-NOTE-CANARY-NEVER-RENDER"
	displayID := notificationMessageID(10)
	insertSourceMessage(t, fixture, sourceMessage{
		id: displayID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{
			protocol.DeliveryModeDisplayOnly, protocol.DeliveryModeWhenSafe,
		}, envelope: true, canary: bodyCanary,
	})
	heldID := notificationMessageID(11)
	insertSourceMessage(t, fixture, sourceMessage{
		id: heldID, control: protocol.ControlStateHeld,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	approvalRequiredID := notificationMessageID(14)
	insertSourceMessage(t, fixture, sourceMessage{
		id: approvalRequiredID, control: protocol.ControlStateApprovalRequired,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	admittedID := notificationMessageID(12)
	insertSourceMessage(t, fixture, sourceMessage{
		id: admittedID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: false,
	})
	approvalMessageID := notificationMessageID(13)
	insertSourceMessage(t, fixture, sourceMessage{
		id: approvalMessageID, control: protocol.ControlStateApprovalRequired,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	approvalID := createPendingApproval(t, fixture, approvalMessageID,
		"agent-recipient-0", noteCanary)

	_, err := service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Empty(t, logged)
	calls := adapter.snapshot()
	require.Len(t, calls, 4)
	resolveTx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = fixture.controller.ResolveOrchestratorApproval(t.Context(), resolveTx,
		protocol.ResolveApprovalParams{
			ApprovalID: approvalID, Option: "deliver_now", Reason: "test resolution",
		}, channel.Actor{Type: protocol.ActorTypeHuman, ID: "test-human"})
	require.NoError(t, err)
	require.NoError(t, resolveTx.Commit())
	_, err = service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, adapter.snapshot(), 4,
		"a resolved approval notice continues to suppress its prior generic duplicate")
	require.Equal(t, 3, notificationCount(t, fixture, ClassMessage))
	require.Equal(t, 1, notificationCount(t, fixture, ClassApproval))

	var displayReason Reason
	var recipientsJSON string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT reason,
		recipient_agents_json FROM human_notifications
		WHERE class = 'MESSAGE' AND object_id = ?`, displayID).
		Scan(&displayReason, &recipientsJSON))
	require.Equal(t, ReasonDisplayOnly, displayReason)
	require.JSONEq(t, `["agent-recipient-0","agent-recipient-1"]`, recipientsJSON)
	var heldReason Reason
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT reason
		FROM human_notifications WHERE class = 'MESSAGE' AND object_id = ?`, heldID).
		Scan(&heldReason))
	require.Equal(t, ReasonChannelHeld, heldReason)
	var approvalRequiredReason Reason
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT reason
		FROM human_notifications WHERE class = 'MESSAGE' AND object_id = ?`, approvalRequiredID).
		Scan(&approvalRequiredReason))
	require.Equal(t, ReasonChannelApprovalRequired, approvalRequiredReason)

	for _, absent := range []protocol.OpaqueID{
		protocol.OpaqueID(admittedID), protocol.OpaqueID(approvalMessageID),
	} {
		var count int
		require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*)
			FROM human_notifications WHERE class = 'MESSAGE' AND object_id = ?`, absent).
			Scan(&count))
		require.Zero(t, count)
	}
	require.Equal(t, 4, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))
	require.Equal(t, 4, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))

	registryRows, err := fixture.store.DB().QueryContext(t.Context(), `SELECT notification_id,
		class, object_id, COALESCE(sender_agent, ''), recipient_agents_json,
		COALESCE(priority, ''), reason, state, adapter, COALESCE(last_error_reason, '')
		FROM human_notifications`)
	require.NoError(t, err)
	defer func() { _ = registryRows.Close() }()
	var registry bytes.Buffer
	for registryRows.Next() {
		var fields [10]string
		require.NoError(t, registryRows.Scan(
			&fields[0], &fields[1], &fields[2], &fields[3], &fields[4],
			&fields[5], &fields[6], &fields[7], &fields[8], &fields[9],
		))
		for _, field := range fields {
			registry.WriteString(field)
		}
	}
	require.NoError(t, registryRows.Err())
	var payloads string
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(),
		"SELECT COALESCE(group_concat(payload_json, ''), '') FROM events").Scan(&payloads))
	callJSON, err := json.Marshal(calls)
	require.NoError(t, err)
	for _, canary := range []string{bodyCanary, noteCanary} {
		require.NotContains(t, registry.String(), canary)
		require.NotContains(t, payloads, canary)
		require.NotContains(t, string(callJSON), canary)
	}
}

func TestPendingRecoveryAndDurableGiffordSequence(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	initial := newNotificationService(t, fixture, &recordingAdapter{}, 3, nil)
	require.NoError(t, initial.Initialize(t.Context()))
	firstID := notificationMessageID(20)
	insertSourceMessage(t, fixture, sourceMessage{
		id: firstID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	discovered, err := initial.discoverBatch(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, discovered)
	require.Equal(t, 1, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))

	recoveredAdapter := &recordingAdapter{}
	recovered := newNotificationService(t, fixture, recoveredAdapter, 4, nil)
	_, err = recovered.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, recoveredAdapter.snapshot(), 1)

	secondID := notificationMessageID(21)
	insertSourceMessage(t, fixture, sourceMessage{
		id: secondID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	afterEffect := &recordingAdapter{}
	crashing := newNotificationService(t, fixture, afterEffect, 5, func(opts *ServiceOptions) {
		opts.afterAdapter = func(Notification) error { return errors.New("simulated process loss") }
	})
	_, err = crashing.RunOnce(t.Context())
	require.ErrorContains(t, err, "simulated process loss")
	require.Len(t, afterEffect.snapshot(), 1)
	var state State
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state
		FROM human_notifications WHERE class = 'MESSAGE' AND object_id = ?`, secondID).Scan(&state))
	require.Equal(t, StatePending, state)

	retryAdapter := &recordingAdapter{}
	retry := newNotificationService(t, fixture, retryAdapter, 6, nil)
	_, err = retry.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, retryAdapter.snapshot(), 1, "external effect can repeat with the same durable id")
	afterRendered := &recordingAdapter{}
	_, err = newNotificationService(t, fixture, afterRendered, 7, nil).RunOnce(t.Context())
	require.NoError(t, err)
	require.Empty(t, afterRendered.snapshot())

	thirdID := notificationMessageID(22)
	insertSourceMessage(t, fixture, sourceMessage{
		id: thirdID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	_, err = newNotificationService(t, fixture, &recordingAdapter{}, 8, nil).RunOnce(t.Context())
	require.NoError(t, err)
	rows, err := fixture.store.DB().QueryContext(t.Context(), `SELECT sequence FROM events
		WHERE session_id = '' ORDER BY sequence`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var sequences []int64
	for rows.Next() {
		var sequence int64
		require.NoError(t, rows.Scan(&sequence))
		sequences = append(sequences, sequence)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{0, 1, 2, 3, 4, 5}, sequences)
}

func TestCreatedRegistryRowAndEventRollbackTogether(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	service := newNotificationService(t, fixture, &recordingAdapter{}, 14,
		func(opts *ServiceOptions) {
			opts.Entropy = bytes.NewReader(bytes.Repeat([]byte{0x55}, 10))
		})
	require.NoError(t, service.Initialize(t.Context()))
	messageID := notificationMessageID(24)
	insertSourceMessage(t, fixture, sourceMessage{
		id: messageID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	_, err := service.RunOnce(t.Context())
	require.ErrorContains(t, err, "read notification id entropy")
	require.Zero(t, notificationCount(t, fixture, ClassMessage))
	require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))
}

func TestRetryableAdapterFailureIsBounded(t *testing.T) {
	t.Parallel()

	fixture := newNotificationFixture(t)
	adapter := &recordingAdapter{
		err: &AdapterError{Reason: FailureAdapterTimeout, Retryable: true},
	}
	service := newNotificationService(t, fixture, adapter, 13, nil)
	require.NoError(t, service.Initialize(t.Context()))
	messageID := notificationMessageID(23)
	insertSourceMessage(t, fixture, sourceMessage{
		id: messageID, control: protocol.ControlStateAllowed,
		recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
	})
	for range defaultMaxAttempts {
		_, err := service.RunOnce(t.Context())
		require.NoError(t, err)
		fixture.clock.advance(defaultRetryDelay)
	}
	_, err := service.RunOnce(t.Context())
	require.NoError(t, err)
	require.Len(t, adapter.snapshot(), defaultMaxAttempts)
	var state State
	var attempts int
	require.NoError(t, fixture.store.DB().QueryRowContext(t.Context(), `SELECT state, attempts
		FROM human_notifications WHERE class = 'MESSAGE' AND object_id = ?`, messageID).
		Scan(&state, &attempts))
	require.Equal(t, StatePending, state)
	require.Equal(t, defaultMaxAttempts, attempts)
	require.Zero(t, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
}

func TestCoalescedWakeDrainsTenCommits(t *testing.T) {
	fixture := newNotificationFixture(t)
	wake := make(chan struct{}, 1)
	adapter := &recordingAdapter{}
	service := newNotificationService(t, fixture, adapter, 9, func(opts *ServiceOptions) {
		opts.Wake = wake
		opts.BatchSize = 3
	})
	require.NoError(t, service.Initialize(t.Context()))
	for number := 30; number < 40; number++ {
		insertSourceMessage(t, fixture, sourceMessage{
			id: notificationMessageID(number), control: protocol.ControlStateAllowed,
			recipients: []protocol.DeliveryMode{protocol.DeliveryModeWhenSafe}, envelope: true,
		})
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	require.Len(t, wake, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	require.Eventually(t, func() bool {
		var rendered int
		err := fixture.store.DB().QueryRowContext(t.Context(), `SELECT count(*) FROM events
			WHERE event_type = ?`, protocol.EventTypeHumanNotificationRendered).Scan(&rendered)
		return err == nil && rendered == 10
	}, 3*time.Second, 10*time.Millisecond)
	cancel()
	<-done
	require.Equal(t, 10, notificationCount(t, fixture, ClassMessage))
	require.Equal(t, 10, eventCount(t, fixture, protocol.EventTypeHumanNotificationCreated))
	require.Equal(t, 10, eventCount(t, fixture, protocol.EventTypeHumanNotificationRendered))
}
