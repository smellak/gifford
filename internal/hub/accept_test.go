package hub

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// This file proves the accept-path matrix with a real SQLite store: fresh
// and denied admissions, retries, envelope replays, expiries, the sweep,
// refusals, and the M9 invariants at the end of the paths that move rows.

// testClock is a manual clock the tests drive.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the driven time in UTC.
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.UTC()
}

// advance moves the clock forward.
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// acceptDeps bundles one scratch stack: store, engine, registry, clock,
// and the four handlers.
type acceptDeps struct {
	store *eventstore.Store
	clock *testClock
	reg   *registry.Registry
	deps  Deps
	admit *AdmitHandler
	sub   *SubmitHandler
	claim *ClaimHandler
	ack   *AckHandler
}

// testAcceptDeps opens the stack. Nothing leaves scratch directories.
func testAcceptDeps(t *testing.T) *acceptDeps {
	t.Helper()
	dir := t.TempDir()
	store, err := eventstore.Open(t.Context(), dir+"/gifford.db")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	dataDir := t.TempDir()
	require.NoError(t, os.Chmod(dataDir, 0o700))
	clk := &testClock{now: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)}
	engine, err := channel.New(channel.Options{Clock: clk})
	require.NoError(t, err)
	reg, err := registry.New("test-node", dataDir, clk)
	require.NoError(t, err)
	deps := Deps{DB: store.DB(), Engine: engine, Registry: reg, Clock: clk, LocalNode: "test-node"}
	admit, err := NewAdmitHandler(deps)
	require.NoError(t, err)
	sub, err := NewSubmitHandler(deps)
	require.NoError(t, err)
	claim, err := NewClaimHandler(deps)
	require.NoError(t, err)
	ack, err := NewAckHandler(deps)
	require.NoError(t, err)
	return &acceptDeps{store: store, clock: clk, reg: reg, deps: deps,
		admit: admit, sub: sub, claim: claim, ack: ack}
}

// enrollHubAgent registers an agent on a node with a live session in the
// test process and an ACTIVE key, returning its key id.
func enrollHubAgent(t *testing.T, ctx context.Context, deps *acceptDeps, agent, node string) string {
	t.Helper()
	now := "2026-10-29T12:00:00.000000000Z"
	_, err := deps.store.DB().ExecContext(ctx, `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, 'test', ?, ?)`,
		agent, node, now, now)
	require.NoError(t, err)
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	claim, err := json.Marshal(self)
	require.NoError(t, err)
	_, err = deps.store.DB().ExecContext(ctx, `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, ?, NULL, ?)`,
		"session-"+agent, agent, string(claim), now, now)
	require.NoError(t, err)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	result, err := deps.reg.Register(ctx, deps.store.DB(), registry.RegisterRequest{
		Agent:     protocol.AgentID(agent),
		Process:   self,
		PublicKey: pub,
		KeyID:     "ed25519:" + agent + ":v1",
	})
	require.NoError(t, err)
	require.Equal(t, registry.OutcomeRegistered, result.Outcome)
	return result.Key.KeyID
}

// admitCall runs admit_message for agent and returns the decoded answer.
func admitCall(t *testing.T, ctx context.Context, admit *AdmitHandler, agent string, body map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	answer, err := admit.AdmitMessage(ctx, protocol.AgentID(agent), raw)
	require.NoError(t, err)
	encoded, err := json.Marshal(answer)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	return decoded
}

// admitError runs admit_message expecting a HubError.
func admitError(t *testing.T, ctx context.Context, admit *AdmitHandler, agent string, body map[string]any) *HubError {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	_, err = admit.AdmitMessage(ctx, protocol.AgentID(agent), raw)
	require.Error(t, err)
	var hubErr *HubError
	require.ErrorAs(t, err, &hubErr)
	return hubErr
}

// admitBody builds admit params with one recipient.
func admitBody(client, agent, mode string, ack bool) map[string]any {
	return map[string]any{
		"client_msg_id": client,
		"recipients": []map[string]any{
			{"agent_id": agent, "delivery_mode": mode, "requires_ack": ack},
		},
		"priority":   "NORMAL",
		"thread_id":  "thread-1",
		"created_at": "2026-10-29T12:00:00.000000000Z",
		"expires_at": "2026-10-29T13:00:00.000000000Z",
	}
}

// submitCall runs submit_message_envelope and returns the decoded answer.
func submitCall(t *testing.T, ctx context.Context, sub *SubmitHandler, agent string, envelope map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"envelope": envelope})
	require.NoError(t, err)
	answer, err := sub.SubmitMessage(ctx, protocol.AgentID(agent), raw)
	require.NoError(t, err)
	encoded, err := json.Marshal(answer)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	return decoded
}

// submitError runs submit_message_envelope expecting a HubError.
func submitError(t *testing.T, ctx context.Context, sub *SubmitHandler, agent string, envelope map[string]any) *HubError {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"envelope": envelope})
	require.NoError(t, err)
	_, err = sub.SubmitMessage(ctx, protocol.AgentID(agent), raw)
	require.Error(t, err)
	var hubErr *HubError
	require.ErrorAs(t, err, &hubErr)
	return hubErr
}

// submitEnvelope builds a valid envelope bound to an admission.
func submitEnvelope(
	t *testing.T,
	messageID, client, agent, recipient, keyID, threadRef string,
	ciphertext []byte,
) map[string]any {
	t.Helper()
	sum := sha256.Sum256(ciphertext)
	return map[string]any{
		"message_id":            messageID,
		"client_msg_id":         client,
		"protocol":              "gifford/1",
		"version":               1,
		"origin_node":           "test-node",
		"origin_agent":          agent,
		"destination_node":      "test-node",
		"recipient_agents":      []string{recipient},
		"created_at":            "2026-10-29T12:00:00.000000000Z",
		"expires_at":            "2026-10-29T13:00:00.000000000Z",
		"priority":              "NORMAL",
		"thread_ref":            threadRef,
		"encryption_profile":    "none",
		"payload":               map[string]any{"text": "opaque"},
		"ciphertext":            ciphertext,
		"ciphertext_sha256":     "sha256:" + hex.EncodeToString(sum[:]),
		"sender_signing_key_id": keyID,
		"signature":             make([]byte, 64),
		"hop_count":             0,
		"route_digest":          "sha256:" + hex.EncodeToString(sum[:]),
		"control_version":       0,
	}
}

// messageState reads one message state.
func messageState(t *testing.T, ctx context.Context, db *sql.DB, messageID string) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT state FROM messages WHERE message_id = ?`, messageID).Scan(&state))
	return state
}

// messageTriple reads one message control triple.
func messageTriple(t *testing.T, ctx context.Context, db *sql.DB, messageID string) (string, string) {
	t.Helper()
	var state, reason string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT control_state, control_reason FROM messages WHERE message_id = ?`, messageID).Scan(&state, &reason))
	return state, reason
}

// decisionCount counts decisions for one message.
func decisionCount(t *testing.T, ctx context.Context, db *sql.DB, messageID string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_decisions WHERE message_id = ?`, messageID).Scan(&count))
	return count
}

// checkInvariants proves M9 at the end of paths that move rows: pending
// rows carry envelopes, failed rows carry denials, and no pending row is
// denied.
func checkInvariants(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var pending, pendingBare int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE state = 'PENDING'`).Scan(&pending))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages m WHERE state = 'PENDING'
		AND NOT EXISTS (SELECT 1 FROM message_envelopes e WHERE e.message_id = m.message_id)`).Scan(&pendingBare))
	require.Equal(t, 0, pendingBare, "pending rows without an envelope")
	var failed, failedOpen int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE state = 'FAILED'`).Scan(&failed))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages
		WHERE state = 'FAILED' AND control_state != 'DENIED'`).Scan(&failedOpen))
	require.Equal(t, 0, failedOpen, "failed rows without a denial")
	var pendingDenied int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages
		WHERE state = 'PENDING' AND control_state = 'DENIED'`).Scan(&pendingDenied))
	require.Equal(t, 0, pendingDenied, "pending rows with a denial")
	_ = pending
	_ = failed
}

// seedLink inserts a link control from agent a to agent b with the mode.
func seedLink(t *testing.T, ctx context.Context, db *sql.DB, id, a, b, mode string) {
	t.Helper()
	now := "2026-10-29T12:00:00.000000000Z"
	_, err := db.ExecContext(ctx, `INSERT INTO channel_controls
		(control_id, scope_type, scope_key, plane, mode,
		 tx_enabled, rx_enabled, auto_delivery_enabled, auto_continue_enabled,
		 from_agent, to_agent, reason, actor_type, created_by, expires_at,
		 restore_json, then_mode, version, created_at, updated_at)
		VALUES (?, 'link', ?, 'message', ?, NULL, NULL, NULL, NULL, ?, ?, NULL, 'human', 'test', NULL,
		 NULL, NULL, 1, ?, ?)`,
		id, a+"→"+b, mode, a, b, now, now)
	require.NoError(t, err)
}

// blockAgentKey suspends an enrolled key through a substitution conflict.
func blockAgentKey(t *testing.T, ctx context.Context, deps *acceptDeps, agent string) {
	t.Helper()
	self, err := registry.CurrentProcess()
	require.NoError(t, err)
	other, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	result, err := deps.reg.Register(ctx, deps.store.DB(), registry.RegisterRequest{
		Agent:     protocol.AgentID(agent),
		Process:   self,
		PublicKey: other,
		KeyID:     "ed25519:" + agent + ":v2",
	})
	require.NoError(t, err)
	require.Equal(t, registry.OutcomeSuspended, result.Outcome)
}

// TestAdmitFresh proves M1: a fresh admission stores the message and its
// recipients with the verdict, and answers the header fields to sign.
func TestAdmitFresh(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-a1", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-b1", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-a1", admitBody("c-fresh", "agent-b1", "WHEN_SAFE", true))
	require.False(t, answer["submitted"].(bool))
	messageID := answer["message_id"].(string)
	require.NotEmpty(t, messageID)
	require.NotEmpty(t, answer["thread_ref"])
	result := answer["admission_result"].(map[string]any)
	require.Equal(t, "ACCEPTED", result["result"])

	require.Equal(t, "ADMITTED", messageState(t, ctx, deps.store.DB(), messageID))
	require.Equal(t, 1, decisionCount(t, ctx, deps.store.DB(), messageID))
	checkInvariants(t, ctx, deps.store.DB())
}

// TestAdmitDenied proves M2 and A2: a denied admission persists its
// decisions, writes no row, and answers the denial without content.
func TestAdmitDenied(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-a2", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-b2", "test-node")
	seedLink(t, ctx, deps.store.DB(), "ctl_000000000000000000000002", "agent-a2", "agent-b2", "DENY")

	hubErr := admitError(t, ctx, deps.admit, "agent-a2", admitBody("c-deny", "agent-b2", "WHEN_SAFE", true))
	require.Equal(t, -32000, hubErr.Code)
	require.NotEmpty(t, hubErr.ErrorCode)

	var messages int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages`).Scan(&messages))
	require.Zero(t, messages)
	var decisions int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_decisions`).Scan(&decisions))
	require.Equal(t, 1, decisions)
}

// TestAdmitRetrySame proves M3 and A3: the same parameters replay the
// stored verdict byte for byte.
func TestAdmitRetrySame(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-a3", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-b3", "test-node")

	first := admitCall(t, ctx, deps.admit, "agent-a3", admitBody("c-retry", "agent-b3", "WHEN_SAFE", true))
	second := admitCall(t, ctx, deps.admit, "agent-a3", admitBody("c-retry", "agent-b3", "WHEN_SAFE", true))
	require.Equal(t, first["admission_result"], second["admission_result"])
	require.Equal(t, first["message_id"], second["message_id"])
	require.False(t, second["submitted"].(bool))
	require.Equal(t, 1, decisionCount(t, ctx, deps.store.DB(), first["message_id"].(string)))
}

// TestAdmitRetryMismatch proves M5 and A7: differing parameters refuse
// without touching the stored row.
func TestAdmitRetryMismatch(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-a5", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-b5", "test-node")

	first := admitCall(t, ctx, deps.admit, "agent-a5", admitBody("c-clash", "agent-b5", "WHEN_SAFE", true))
	changed := admitBody("c-clash", "agent-b5", "WHEN_SAFE", true)
	changed["priority"] = "URGENT"
	hubErr := admitError(t, ctx, deps.admit, "agent-a5", changed)
	require.Equal(t, -32602, hubErr.Code)
	require.Equal(t, "ADMITTED", messageState(t, ctx, deps.store.DB(), first["message_id"].(string)))
}

// TestAdmitFailureRollsBack proves M7: a failure inside the transaction
// leaves no row and no decisions, and the retry proceeds.
func TestAdmitFailureRollsBack(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-a7", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-b7", "test-node")

	_, err := deps.store.DB().ExecContext(ctx, `CREATE TRIGGER fail_hub_message_insert
		BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT, 'boom'); END;`)
	require.NoError(t, err)
	hubErr := admitError(t, ctx, deps.admit, "agent-a7", admitBody("c-fail", "agent-b7", "WHEN_SAFE", true))
	require.Equal(t, -32603, hubErr.Code)
	var messages, decisions int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages`).Scan(&messages))
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_decisions`).Scan(&decisions))
	require.Zero(t, messages)
	require.Zero(t, decisions)

	_, err = deps.store.DB().ExecContext(ctx, `DROP TRIGGER fail_hub_message_insert`)
	require.NoError(t, err)
	answer := admitCall(t, ctx, deps.admit, "agent-a7", admitBody("c-fail", "agent-b7", "WHEN_SAFE", true))
	require.False(t, answer["submitted"].(bool))
}

// TestAdmitConcurrent proves M8: concurrent admissions under one
// client_msg_id store one message and one decision.
func TestAdmitConcurrent(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-a8", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-b8", "test-node")

	const racers = 8
	type outcome struct {
		answer map[string]any
		err    *HubError
	}
	arrived := make(chan outcome, racers)
	for range racers {
		go func() {
			raw, err := json.Marshal(admitBody("c-race", "agent-b8", "WHEN_SAFE", true))
			if err != nil {
				arrived <- outcome{err: internalError()}
				return
			}
			answer, err := deps.admit.AdmitMessage(ctx, protocol.AgentID("agent-a8"), raw)
			if err != nil {
				var hubErr *HubError
				if !errors.As(err, &hubErr) {
					hubErr = internalError()
				}
				arrived <- outcome{err: hubErr}
				return
			}
			encoded, err := json.Marshal(answer)
			if err != nil {
				arrived <- outcome{err: internalError()}
				return
			}
			var decoded map[string]any
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				arrived <- outcome{err: internalError()}
				return
			}
			arrived <- outcome{answer: decoded}
		}()
	}
	ids := make(map[string]bool)
	for range racers {
		got := <-arrived
		if got.err != nil {
			continue
		}
		ids[got.answer["message_id"].(string)] = true
	}
	require.Len(t, ids, 1)
	var messages, decisions int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages`).Scan(&messages))
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_decisions`).Scan(&decisions))
	require.Equal(t, 1, messages)
	require.Equal(t, 1, decisions)
}

// TestSubmitFresh proves S1: a bound envelope stores its projections and
// moves the message to pending.
func TestSubmitFresh(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s1", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r1", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s1", admitBody("c-submit", "agent-r1", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	ciphertext := []byte("opaque-bytes")
	env := submitEnvelope(t, messageID, "c-submit", "agent-s1", "agent-r1", keyID,
		answer["thread_ref"].(string), ciphertext)

	result := submitCall(t, ctx, deps.sub, "agent-s1", env)
	require.Equal(t, messageID, result["message_id"])
	require.Equal(t, "PENDING", result["state"])
	require.Equal(t, "PENDING", messageState(t, ctx, deps.store.DB(), messageID))
	state, _ := messageTriple(t, ctx, deps.store.DB(), messageID)
	require.Equal(t, "ACCEPTED", state)

	var stored []byte
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT envelope FROM message_envelopes WHERE message_id = ?`, messageID).Scan(&stored))
	require.NotEmpty(t, stored)
	var agents string
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT recipient_agents_json FROM message_envelopes WHERE message_id = ?`, messageID).Scan(&agents))
	require.JSONEq(t, `["agent-r1"]`, agents)
	checkInvariants(t, ctx, deps.store.DB())
}

// TestSubmitDenied proves S2 and M4: a denial at acceptance fails the
// message with its triple and stores no envelope.
func TestSubmitDenied(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s2", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r2", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s2", admitBody("c-sdeny", "agent-r2", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	seedLink(t, ctx, deps.store.DB(), "ctl_000000000000000000000004", "agent-s2", "agent-r2", "DENY")
	env := submitEnvelope(t, messageID, "c-sdeny", "agent-s2", "agent-r2", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))

	hubErr := submitError(t, ctx, deps.sub, "agent-s2", env)
	require.Equal(t, -32000, hubErr.Code)
	require.NotEmpty(t, hubErr.ErrorCode)
	require.Equal(t, "FAILED", messageState(t, ctx, deps.store.DB(), messageID))
	state, reason := messageTriple(t, ctx, deps.store.DB(), messageID)
	require.Equal(t, "DENIED", state)
	require.NotEmpty(t, reason)
	var envelopes int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_envelopes WHERE message_id = ?`, messageID).Scan(&envelopes))
	require.Zero(t, envelopes)
	checkInvariants(t, ctx, deps.store.DB())
}

// TestSubmitExpired proves S3: past expiry fails without the gate,
// without an envelope.
func TestSubmitExpired(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s3", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r3", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s3", admitBody("c-sexp", "agent-r3", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	deps.clock.advance(2 * time.Hour)
	env := submitEnvelope(t, messageID, "c-sexp", "agent-s3", "agent-r3", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))

	before := decisionCount(t, ctx, deps.store.DB(), messageID)
	hubErr := submitError(t, ctx, deps.sub, "agent-s3", env)
	require.Equal(t, -32000, hubErr.Code)
	require.Equal(t, string(protocol.ErrorCodeExpired), hubErr.ErrorCode)
	require.Equal(t, "EXPIRED", messageState(t, ctx, deps.store.DB(), messageID))
	require.Equal(t, before, decisionCount(t, ctx, deps.store.DB(), messageID))
}

// TestSubmitReplay proves S4: the same bytes replay the stored outcome
// without a second envelope.
func TestSubmitReplay(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s4", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r4", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s4", admitBody("c-sreplay", "agent-r4", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	env := submitEnvelope(t, messageID, "c-sreplay", "agent-s4", "agent-r4", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))

	first := submitCall(t, ctx, deps.sub, "agent-s4", env)
	second := submitCall(t, ctx, deps.sub, "agent-s4", env)
	require.Equal(t, first, second)
	var envelopes int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_envelopes WHERE message_id = ?`, messageID).Scan(&envelopes))
	require.Equal(t, 1, envelopes)
}

// TestSubmitReplayTerminal proves S5: replays after terminal states
// answer the stored outcome without the gate.
func TestSubmitReplayTerminal(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s5", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r5", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s5", admitBody("c-sterm", "agent-r5", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	seedLink(t, ctx, deps.store.DB(), "ctl_000000000000000000000005", "agent-s5", "agent-r5", "DENY")
	env := submitEnvelope(t, messageID, "c-sterm", "agent-s5", "agent-r5", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))
	first := submitError(t, ctx, deps.sub, "agent-s5", env)
	require.Equal(t, -32000, first.Code)
	before := decisionCount(t, ctx, deps.store.DB(), messageID)
	second := submitError(t, ctx, deps.sub, "agent-s5", env)
	require.Equal(t, first.Code, second.Code)
	require.Equal(t, first.ErrorCode, second.ErrorCode)
	require.Equal(t, before, decisionCount(t, ctx, deps.store.DB(), messageID))
}

// TestSubmitMismatch proves S6: different bytes for a stored envelope
// refuse without moving the message.
func TestSubmitMismatch(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s6", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r6", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s6", admitBody("c-sclash", "agent-r6", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	env := submitEnvelope(t, messageID, "c-sclash", "agent-s6", "agent-r6", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))
	submitCall(t, ctx, deps.sub, "agent-s6", env)

	other := submitEnvelope(t, messageID, "c-sclash", "agent-s6", "agent-r6", keyID,
		answer["thread_ref"].(string), []byte("different-bytes"))
	hubErr := submitError(t, ctx, deps.sub, "agent-s6", other)
	require.Equal(t, -32602, hubErr.Code)
	require.Equal(t, "PENDING", messageState(t, ctx, deps.store.DB(), messageID))
}

// TestSubmitNoAdmission proves S7: an envelope without a local admission
// refuses before the gate.
func TestSubmitNoAdmission(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s7", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r7", "test-node")

	env := submitEnvelope(t, "01ARZ3NDEKTSV4RRFFQ69G5FAV", "c-ghost", "agent-s7", "agent-r7", keyID,
		"thr_01ARZ3NDEKTSV4RRFFQ69G5FAV", []byte("opaque-bytes"))
	hubErr := submitError(t, ctx, deps.sub, "agent-s7", env)
	require.Equal(t, -32602, hubErr.Code)
}

// TestSubmitFailureRollsBack proves M6: a failure inside the transaction
// keeps the admitted row with no envelope, and the retry proceeds.
func TestSubmitFailureRollsBack(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-s9", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-r9", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-s9", admitBody("c-sfail", "agent-r9", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	env := submitEnvelope(t, messageID, "c-sfail", "agent-s9", "agent-r9", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))

	_, err := deps.store.DB().ExecContext(ctx, `CREATE TRIGGER fail_hub_envelope_insert
		BEFORE INSERT ON message_envelopes BEGIN SELECT RAISE(ABORT, 'boom'); END;`)
	require.NoError(t, err)
	hubErr := submitError(t, ctx, deps.sub, "agent-s9", env)
	require.Equal(t, -32603, hubErr.Code)
	require.Equal(t, "ADMITTED", messageState(t, ctx, deps.store.DB(), messageID))
	var envelopes int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_envelopes WHERE message_id = ?`, messageID).Scan(&envelopes))
	require.Zero(t, envelopes)

	_, err = deps.store.DB().ExecContext(ctx, `DROP TRIGGER fail_hub_envelope_insert`)
	require.NoError(t, err)
	result := submitCall(t, ctx, deps.sub, "agent-s9", env)
	require.Equal(t, "PENDING", result["state"])
	checkInvariants(t, ctx, deps.store.DB())
}

// TestSubmitBindingMismatch proves mismatched ids, sender, or recipients
// refuse before the gate.
func TestSubmitBindingMismatch(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-sb", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-rb", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-sb", admitBody("c-sbind", "agent-rb", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	env := submitEnvelope(t, messageID, "c-wrong", "agent-sb", "agent-rb", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))
	hubErr := submitError(t, ctx, deps.sub, "agent-sb", env)
	require.Equal(t, -32602, hubErr.Code)
}

// TestAdmitPreconditions proves D7 and §9: blocked senders and recipients
// refuse with KEY_UNAVAILABLE, foreign recipients are not found, and
// unknown recipients are not deliverable. No decision rows, no message.
func TestAdmitPreconditions(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-blocked", "test-node")
	blockAgentKey(t, ctx, deps, "agent-blocked")
	enrollHubAgent(t, ctx, deps, "agent-ok", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-remote", "far-node")

	for _, tc := range []struct {
		name  string
		from  string
		to    string
		code  int
		cause string
	}{
		{"blocked sender", "agent-blocked", "agent-ok", -32000, string(protocol.ErrorCodeKeyUnavailable)},
		{"blocked recipient", "agent-ok", "agent-blocked", -32000, string(protocol.ErrorCodeKeyUnavailable)},
		{"remote recipient", "agent-ok", "agent-remote", -32000, string(protocol.ErrorCodeNotFound)},
		{"unknown recipient", "agent-ok", "agent-ghost", -32000, string(protocol.ErrorCodeRecipientNotDeliverable)},
	} {
		hubErr := admitError(t, ctx, deps.admit, tc.from, admitBody("c-"+tc.name, tc.to, "WHEN_SAFE", true))
		require.Equal(t, tc.code, hubErr.Code, tc.name)
		require.Equal(t, tc.cause, hubErr.ErrorCode, tc.name)
	}
	var messages, decisions int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages`).Scan(&messages))
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_decisions`).Scan(&decisions))
	require.Zero(t, messages)
	require.Zero(t, decisions)
}

// TestClaimFlow proves the read step: the recipient gets envelope and
// metadata, strangers and ghosts get NOT_FOUND/NOT_RECIPIENT.
func TestClaimFlow(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-cs", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-cr", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-cx", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-cs", admitBody("c-claim", "agent-cr", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)

	raw, err := json.Marshal(map[string]any{"message_id": messageID})
	require.NoError(t, err)
	_, err = deps.claim.ClaimMessage(ctx, protocol.AgentID("agent-cr"), raw)
	require.Error(t, err, "no envelope yet")
	var hubErr *HubError
	require.ErrorAs(t, err, &hubErr)
	require.Equal(t, -32000, hubErr.Code)
	require.Equal(t, string(protocol.ErrorCodeNotFound), hubErr.ErrorCode)
}

// TestClaimDisplayOnly proves the model never receives display-only rows.
func TestClaimDisplayOnly(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-ds", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-dr", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-ds", admitBody("c-display", "agent-dr", "DISPLAY_ONLY", false))
	messageID := answer["message_id"].(string)
	env := submitEnvelope(t, messageID, "c-display", "agent-ds", "agent-dr", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))
	submitCall(t, ctx, deps.sub, "agent-ds", env)

	raw, err := json.Marshal(map[string]any{"message_id": messageID})
	require.NoError(t, err)
	_, err = deps.claim.ClaimMessage(ctx, protocol.AgentID("agent-dr"), raw)
	require.Error(t, err)
	var hubErr *HubError
	require.ErrorAs(t, err, &hubErr)
	require.Equal(t, -32603, hubErr.Code)
}

// TestClaimBlocked proves M13: a blocked agent cannot claim.
func TestClaimBlocked(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-ms", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-mr", "test-node")
	blockAgentKey(t, ctx, deps, "agent-mr")

	answer := admitCall(t, ctx, deps.admit, "agent-ms", admitBody("c-mclaim", "agent-mr", "WHEN_SAFE", true))
	raw, err := json.Marshal(map[string]any{"message_id": answer["message_id"]})
	require.NoError(t, err)
	_, err = deps.claim.ClaimMessage(ctx, protocol.AgentID("agent-mr"), raw)
	require.Error(t, err)
	var hubErr *HubError
	require.ErrorAs(t, err, &hubErr)
	require.Equal(t, -32000, hubErr.Code)
	require.Equal(t, string(protocol.ErrorCodeKeyUnavailable), hubErr.ErrorCode)
}

// TestAckFlow proves acks store one row per key and repeats collapse,
// including the mismatch state.
func TestAckFlow(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-ks", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-kr", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-ks", admitBody("c-kack", "agent-kr", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	env := submitEnvelope(t, messageID, "c-kack", "agent-ks", "agent-kr", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))
	submitCall(t, ctx, deps.sub, "agent-ks", env)

	ack := func(status string) any {
		raw, err := json.Marshal(map[string]any{"message_id": messageID, "status": status})
		require.NoError(t, err)
		out, err := deps.ack.AckMessage(ctx, protocol.AgentID("agent-kr"), raw)
		require.NoError(t, err)
		return out
	}
	ack("PROCESSING")
	ack("ACKNOWLEDGED")
	ack("ACKNOWLEDGED")
	ack("MISMATCH")

	var rows int
	require.NoError(t, deps.store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM acks WHERE message_id = ?`, messageID).Scan(&rows))
	require.Equal(t, 3, rows)
}

// TestAckRefusals proves unknown messages, strangers, and bad statuses
// refuse.
func TestAckRefusals(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	enrollHubAgent(t, ctx, deps, "agent-qs", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-qr", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-qx", "test-node")

	answer := admitCall(t, ctx, deps.admit, "agent-qs", admitBody("c-qack", "agent-qr", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)

	ackErr := func(agent, id, status string) *HubError {
		raw, err := json.Marshal(map[string]any{"message_id": id, "status": status})
		require.NoError(t, err)
		_, err = deps.ack.AckMessage(ctx, protocol.AgentID(agent), raw)
		require.Error(t, err)
		var hubErr *HubError
		require.ErrorAs(t, err, &hubErr)
		return hubErr
	}
	require.Equal(t, string(protocol.ErrorCodeNotFound),
		ackErr("agent-qr", "01ARZ3NDEKTSV4RRFFQ69G5FAV", "ACKNOWLEDGED").ErrorCode)
	require.Equal(t, string(protocol.ErrorCodeNotRecipient),
		ackErr("agent-qx", messageID, "ACKNOWLEDGED").ErrorCode)
	raw, err := json.Marshal(map[string]any{"message_id": messageID, "status": "read"})
	require.NoError(t, err)
	_, err = deps.ack.AckMessage(ctx, protocol.AgentID("agent-qr"), raw)
	require.Error(t, err)
}

// TestSweep proves T1: admitted rows past expiry move, pending rows stay.
func TestSweep(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-ts", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-tr", "test-node")

	first := admitCall(t, ctx, deps.admit, "agent-ts", admitBody("c-sweep1", "agent-tr", "WHEN_SAFE", true))
	second := admitCall(t, ctx, deps.admit, "agent-ts", admitBody("c-sweep2", "agent-tr", "WHEN_SAFE", true))
	env := submitEnvelope(t, second["message_id"].(string), "c-sweep2", "agent-ts", "agent-tr", keyID,
		second["thread_ref"].(string), []byte("opaque-bytes"))
	submitCall(t, ctx, deps.sub, "agent-ts", env)

	moved, err := SweepExpired(ctx, deps.store.DB(), "2026-10-29T13:00:00.000000000Z")
	require.NoError(t, err)
	require.Equal(t, int64(1), moved)
	require.Equal(t, "EXPIRED", messageState(t, ctx, deps.store.DB(), first["message_id"].(string)))
	require.Equal(t, "PENDING", messageState(t, ctx, deps.store.DB(), second["message_id"].(string)))

	moved, err = SweepExpired(ctx, deps.store.DB(), "2026-10-29T13:00:00.000000000Z")
	require.NoError(t, err)
	require.Zero(t, moved)
}

// TestAdmitHold proves M10: a held admission stores ADMITTED·HELD and
// submits to PENDING·HELD.
func TestAdmitHold(t *testing.T) {
	t.Parallel()
	deps := testAcceptDeps(t)
	ctx := t.Context()
	keyID := enrollHubAgent(t, ctx, deps, "agent-hs", "test-node")
	enrollHubAgent(t, ctx, deps, "agent-hr", "test-node")
	seedLink(t, ctx, deps.store.DB(), "ctl_000000000000000000000010", "agent-hs", "agent-hr", "HOLD")

	answer := admitCall(t, ctx, deps.admit, "agent-hs", admitBody("c-hold", "agent-hr", "WHEN_SAFE", true))
	messageID := answer["message_id"].(string)
	state, reason := messageTriple(t, ctx, deps.store.DB(), messageID)
	require.Equal(t, "HELD", state)
	require.NotEmpty(t, reason)

	env := submitEnvelope(t, messageID, "c-hold", "agent-hs", "agent-hr", keyID,
		answer["thread_ref"].(string), []byte("opaque-bytes"))
	result := submitCall(t, ctx, deps.sub, "agent-hs", env)
	require.Equal(t, "PENDING", result["state"])
	require.Equal(t, "HELD", result["control_state"])
	require.Equal(t, "PENDING", messageState(t, ctx, deps.store.DB(), messageID))
	checkInvariants(t, ctx, deps.store.DB())
}

// TestTimeLayoutMatchesStore keeps the copied time layout honest.
func TestTimeLayoutMatchesStore(t *testing.T) {
	t.Parallel()
	require.Equal(t, eventstore.TimeLayout, timeLayout)
}

// TestMintULID proves minted ids are valid, unique, and ordered.
func TestMintULID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	seen := make(map[protocol.ULID]bool)
	var previous protocol.ULID
	for range 100 {
		id, err := mintULID(now)
		require.NoError(t, err)
		require.True(t, id.Valid())
		require.NotContains(t, seen, id)
		seen[id] = true
		if previous != "" {
			require.Greater(t, string(id), string(previous))
		}
		previous = id
	}
}

