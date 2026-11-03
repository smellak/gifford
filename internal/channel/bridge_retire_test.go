package channel

import (
	"bytes"
	"database/sql"
	"errors"
	"testing"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	retireStateQueued    = "QUEUED"
	retireStateCancelled = "CANCELLED"
)

const (
	retireSourceA = "node-test-a"
	retireSourceB = "node-test-b"
	retireAgentA  = protocol.AgentID("retire-agent-a")
	retireAgentB  = protocol.AgentID("retire-agent-b")
	retireActor   = "uid:0:pid:1"
)

var retireMsgSeq = 0

func nextRetireMsg() string {
	retireMsgSeq++
	return "01JB0000000000000000000R" + string(rune('0'+retireMsgSeq))
}

func seedRetireMessage(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	seedRetireMessageFrom(t, db, id, "bridge-relay")
}

func seedRetireMessageFrom(t *testing.T, db *sql.DB, id, from string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO messages
		(message_id, client_msg_id, origin_node, from_agent, destination_node,
		thread_ref, priority, state, created_at, expires_at, updated_at,
		hop_count, route_digest)
		VALUES (?, ?, 'node-test-a', ?, 'local', ?, 'NORMAL', 'DELIVERY_QUEUED',
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`,
		id, "cm-"+id, from, "thr-"+id)
	if err != nil {
		t.Fatal(err)
	}
}

func seedRetireMapping(t *testing.T, db *sql.DB, source, agent, gifford string, srcID int64) {
	t.Helper()
	seedRetireMappingFrom(t, db, source, "bridge-relay", agent, gifford, srcID)
}

func seedRetireMappingFrom(t *testing.T, db *sql.DB, source, from, agent, gifford string, srcID int64) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO bridge_message_mappings
		(source_node_id, source_message_id, gifford_message_id, from_agent, to_agent,
		source_created_at, source_read_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, '2026-11-03T00:00:00Z', NULL,
		'2026-11-03T00:00:00Z', '2026-11-03T00:00:00Z')`,
		source, srcID, gifford, from, agent)
	if err != nil {
		t.Fatal(err)
	}
}

func seedRetireQueue(t *testing.T, db *sql.DB, id, agent, state string) {
	t.Helper()
	seedRetireQueueFrom(t, db, id, agent, "bridge-relay", state)
}

func seedRetireQueueFrom(t *testing.T, db *sql.DB, id, agent, from, state string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref, state,
		priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, ?, 2,
		'2026-11-03T00:00:00Z', '2026-11-04T00:00:00Z', '2026-11-03T00:00:00Z',
		0, 'sha256:abc')`,
		id, agent, from, "thr-"+id, state)
	if err != nil {
		t.Fatal(err)
	}
}

func seedRetireAttempt(t *testing.T, db *sql.DB, id, agent, state string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		orchestrator_decision_id, channel_decision_id, draft_generation,
		input_guard_version, control_versions_json, state, created_at, updated_at)
		VALUES (?, ?, ?, 1, 'sess', 'odec', 'cdec', 0, 0, '{}', ?,
		'2026-11-03T00:00:00Z', '2026-11-03T00:00:00Z')`,
		"att-"+id, id, agent, state)
	if err != nil {
		t.Fatal(err)
	}
}

func retireParams() protocol.BridgeRetireRecipientParams {
	return protocol.BridgeRetireRecipientParams{SourceNodeID: retireSourceA, AgentID: retireAgentA}
}

func queueState(t *testing.T, db *sql.DB, id string) string {
	t.Helper()
	var state string
	if err := db.QueryRowContext(t.Context(), `SELECT state FROM delivery_queue WHERE message_id = ?`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func queueReason(t *testing.T, db *sql.DB, id string) string {
	t.Helper()
	var reason string
	if err := db.QueryRowContext(t.Context(), `SELECT control_reason FROM delivery_queue WHERE message_id = ?`, id).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	return reason
}

func testRetireController(t *testing.T, db *sql.DB) *Controller {
	t.Helper()
	clk := &mutableControlClock{now: testEngineTime}
	engine, err := New(Options{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	sink := &recordingControlEvents{}
	c, err := NewController(ControllerOptions{DB: db, Engine: engine, Clock: clk, NodeID: testNodeA, Entropy: bytes.NewReader(make([]byte, 1024)), Events: sink})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRetireCancelsNonTerminalBridgeDeliveries(t *testing.T) {
	store := testChannelStore(t)
	db := store.DB()
	c := testRetireController(t, db)
	nonTerminal := []string{retireStateQueued, "CLAIMED", "DELIVERING", "RETRY_WAIT"}
	terminal := []string{"DELIVERED", "FAILED", "EXPIRED", retireStateCancelled}
	ids := map[string]string{}
	n := int64(0)
	for _, st := range append(append([]string{}, nonTerminal...), terminal...) {
		n++
		id := nextRetireMsg()
		ids[st] = id
		seedRetireMessage(t, db, id)
		seedRetireMapping(t, db, retireSourceA, string(retireAgentA), id, n)
		seedRetireQueue(t, db, id, string(retireAgentA), st)
	}
	// Other source node and other agent stay.
	otherMsg := nextRetireMsg()
	seedRetireMessage(t, db, otherMsg)
	seedRetireMapping(t, db, retireSourceB, string(retireAgentA), otherMsg, 100)
	seedRetireQueue(t, db, otherMsg, string(retireAgentA), "QUEUED")
	otherAgent := nextRetireMsg()
	seedRetireMessage(t, db, otherAgent)
	seedRetireMapping(t, db, retireSourceA, string(retireAgentB), otherAgent, 101)
	seedRetireQueue(t, db, otherAgent, string(retireAgentB), "QUEUED")

	res, err := c.RetireBridgeRecipient(t.Context(), retireParams(), Actor{Type: protocol.ActorTypeHuman, ID: retireActor})
	if err != nil {
		t.Fatal(err)
	}
	if res.Cancelled != int64(len(nonTerminal)) || res.AlreadyTerminal != int64(len(terminal)) || res.InFlight {
		t.Fatalf("result = %+v, want cancelled %d terminal %d", res, len(nonTerminal), len(terminal))
	}
	for _, st := range nonTerminal {
		if got := queueState(t, db, ids[st]); got != "CANCELLED" {
			t.Fatalf("state = %s, want CANCELLED", got)
		}
		if got := queueReason(t, db, ids[st]); got != "RECIPIENT_RETIRED" {
			t.Fatalf("reason = %s, want RECIPIENT_RETIRED", got)
		}
	}
	for _, st := range terminal {
		if got := queueState(t, db, ids[st]); got != st {
			t.Fatalf("terminal state changed to %s", got)
		}
	}
	if got := queueState(t, db, otherMsg); got != retireStateQueued {
		t.Fatalf("other source touched: %s", got)
	}
	if got := queueState(t, db, otherAgent); got != retireStateQueued {
		t.Fatalf("other agent touched: %s", got)
	}
	// Idempotent: nothing left to cancel.
	res2, err := c.RetireBridgeRecipient(t.Context(), retireParams(), Actor{Type: protocol.ActorTypeHuman, ID: retireActor})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Cancelled != 0 || res2.AlreadyTerminal != int64(len(nonTerminal)+len(terminal)) {
		t.Fatalf("second retire = %+v", res2)
	}
}

func TestRetireBusyWithInFlightAttemptChangesNothing(t *testing.T) {
	for _, state := range []string{"COMMIT_SENT", "UNCERTAIN"} {
		store := testChannelStore(t)
		db := store.DB()
		c := testRetireController(t, db)
		id := nextRetireMsg()
		seedRetireMessage(t, db, id)
		seedRetireMapping(t, db, retireSourceA, string(retireAgentA), id, 1)
		seedRetireQueue(t, db, id, string(retireAgentA), "DELIVERING")
		seedRetireAttempt(t, db, id, string(retireAgentA), state)
		_, err := c.RetireBridgeRecipient(t.Context(), retireParams(), Actor{Type: protocol.ActorTypeHuman, ID: retireActor})
		if !errors.Is(err, ErrBridgeRecipientBusy) {
			t.Fatalf("state %s: err = %v, want busy", state, err)
		}
		if got := queueState(t, db, id); got != "DELIVERING" {
			t.Fatalf("state %s touched: %s", state, got)
		}
	}
}

func TestRetirePreservesMessagesMappingsAcksAndReadMarks(t *testing.T) {
	store := testChannelStore(t)
	db := store.DB()
	c := testRetireController(t, db)
	id := nextRetireMsg()
	seedRetireMessage(t, db, id)
	seedRetireMapping(t, db, retireSourceA, string(retireAgentA), id, 7)
	if _, err := db.ExecContext(t.Context(), `UPDATE bridge_message_mappings SET source_read_at = '2026-11-03T00:01:00Z'
		WHERE gifford_message_id = ? AND source_read_at IS NULL`, id); err != nil {
		t.Fatal(err)
	}
	seedRetireQueue(t, db, id, string(retireAgentA), "QUEUED")
	if _, err := db.ExecContext(t.Context(), `INSERT INTO acks (message_id, agent_id, delivery_generation, status, acked_at)
		VALUES (?, ?, 1, 'ACKNOWLEDGED', '2026-11-03T00:02:00Z')`, id, string(retireAgentA)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RetireBridgeRecipient(t.Context(), retireParams(), Actor{Type: protocol.ActorTypeHuman, ID: retireActor}); err != nil {
		t.Fatal(err)
	}
	var read string
	if err := db.QueryRowContext(t.Context(), `SELECT source_read_at FROM bridge_message_mappings WHERE gifford_message_id = ?`, id).Scan(&read); err != nil {
		t.Fatal(err)
	}
	if read != "2026-11-03T00:01:00Z" {
		t.Fatalf("source_read_at changed: %s", read)
	}
	var ack string
	if err := db.QueryRowContext(t.Context(), `SELECT status FROM acks WHERE message_id = ?`, id).Scan(&ack); err != nil {
		t.Fatal(err)
	}
	if ack != "ACKNOWLEDGED" {
		t.Fatalf("ack changed: %s", ack)
	}
	var msg string
	if err := db.QueryRowContext(t.Context(), `SELECT state FROM messages WHERE message_id = ?`, id).Scan(&msg); err != nil {
		t.Fatal(err)
	}
	if msg != "DELIVERY_QUEUED" {
		t.Fatalf("message changed: %s", msg)
	}
}

func TestRetireNewMessageAfterRetireIsNewDelivery(t *testing.T) {
	store := testChannelStore(t)
	db := store.DB()
	c := testRetireController(t, db)
	old := nextRetireMsg()
	seedRetireMessage(t, db, old)
	seedRetireMapping(t, db, retireSourceA, string(retireAgentA), old, 3)
	seedRetireQueue(t, db, old, string(retireAgentA), "QUEUED")
	if _, err := c.RetireBridgeRecipient(t.Context(), retireParams(), Actor{Type: protocol.ActorTypeHuman, ID: retireActor}); err != nil {
		t.Fatal(err)
	}
	fresh := nextRetireMsg()
	seedRetireMessage(t, db, fresh)
	seedRetireMapping(t, db, retireSourceA, string(retireAgentA), fresh, 4)
	seedRetireQueue(t, db, fresh, string(retireAgentA), "QUEUED")
	if got := queueState(t, db, fresh); got != retireStateQueued {
		t.Fatalf("fresh delivery = %s", got)
	}
	if got := queueState(t, db, old); got != "CANCELLED" {
		t.Fatalf("retired delivery = %s", got)
	}
}

// TestRetireSelfSendRecipientCancelsOwnDelivery retires a self-send mirror:
// the mirrored recipient is also the sender, and the retire still cancels
// the queued delivery with RECIPIENT_RETIRED while the message and its
// mapping stay preserved.
func TestRetireSelfSendRecipientCancelsOwnDelivery(t *testing.T) {
	store := testChannelStore(t)
	db := store.DB()
	c := testRetireController(t, db)
	id := nextRetireMsg()
	seedRetireMessageFrom(t, db, id, string(retireAgentA))
	seedRetireMappingFrom(t, db, retireSourceA, string(retireAgentA), string(retireAgentA), id, 9)
	seedRetireQueueFrom(t, db, id, string(retireAgentA), string(retireAgentA), "QUEUED")
	var fromAgent, toAgent string
	if err := db.QueryRowContext(t.Context(), `SELECT from_agent, to_agent
		FROM bridge_message_mappings WHERE gifford_message_id = ?`, id).Scan(&fromAgent, &toAgent); err != nil {
		t.Fatal(err)
	}
	if fromAgent != toAgent {
		t.Fatalf("fixture is not a self-send: from %s to %s", fromAgent, toAgent)
	}
	res, err := c.RetireBridgeRecipient(t.Context(), retireParams(), Actor{Type: protocol.ActorTypeHuman, ID: retireActor})
	if err != nil {
		t.Fatal(err)
	}
	if res.Cancelled != 1 || res.AlreadyTerminal != 0 || res.InFlight {
		t.Fatalf("result = %+v, want one cancellation", res)
	}
	if got := queueState(t, db, id); got != retireStateCancelled {
		t.Fatalf("state = %s, want CANCELLED", got)
	}
	if got := queueReason(t, db, id); got != "RECIPIENT_RETIRED" {
		t.Fatalf("reason = %s, want RECIPIENT_RETIRED", got)
	}
	var mappings int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM bridge_message_mappings
		WHERE gifford_message_id = ?`, id).Scan(&mappings); err != nil {
		t.Fatal(err)
	}
	if mappings != 1 {
		t.Fatalf("mappings = %d, want 1", mappings)
	}
}

func TestRetireRejectsBadInput(t *testing.T) {
	store := testChannelStore(t)
	c := testRetireController(t, store.DB())
	for _, p := range []protocol.BridgeRetireRecipientParams{
		{SourceNodeID: "", AgentID: retireAgentA},
		{SourceNodeID: retireSourceA, AgentID: "!!!"},
	} {
		if _, err := c.RetireBridgeRecipient(t.Context(), p, Actor{Type: protocol.ActorTypeHuman, ID: retireActor}); err == nil {
			t.Fatalf("params %+v accepted", p)
		}
	}
}
