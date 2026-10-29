package registry

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the maintained verification of the registry: enrolment,
// substitution, review, signature failures, and the node key. Lineage
// resolution lives in resolve_test.go.

// testStore opens a migrated store in a scratch directory.
func testStore(t *testing.T) *eventstore.Store {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// testRegistry returns a registry backed by a migrated store, both in
// scratch directories. Nothing is written outside them.
func testRegistry(t *testing.T) (*Registry, *eventstore.Store) {
	t.Helper()
	store := testStore(t)
	reg, err := New("test-node", testDataDir(t), clock.System{})
	require.NoError(t, err)
	return reg, store
}

// testDataDir returns a scratch data directory at mode 0700: the node key
// loader refuses anything else, and TempDir alone does not guarantee it.
func testDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	return dir
}

// testAgent inserts the agents row sessions reference.
func testAgent(ctx context.Context, t *testing.T, db *sql.DB, agent string) {
	t.Helper()
	_, err := db.ExecContext(ctx, `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at)
		VALUES (?, 'test-node', 'test', '2026-01-01T00:00:00.000000000Z', '2026-01-01T00:00:00.000000000Z')`, agent)
	require.NoError(t, err)
}

// testSession inserts a live session row with the given process claim. A
// nil process stores NULL process_json; a non-nil endedAt ends the session.
func testSession(
	ctx context.Context,
	t *testing.T,
	db *sql.DB,
	session, agent string,
	process *protocol.ProcessIdentity,
	endedAt *string,
) {
	t.Helper()
	var raw any
	if process != nil {
		encoded, err := json.Marshal(process)
		require.NoError(t, err)
		raw = string(encoded)
	}
	_, err := db.ExecContext(ctx, `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, '{}', ?, '2026-01-01T00:00:00.000000000Z', ?, '2026-01-01T00:00:00.000000000Z')`,
		session, agent, raw, endedAt)
	require.NoError(t, err)
}

// testRequest builds a register request for agent anchored in the calling
// test process, with a fresh synthetic key.
func testRequest(t *testing.T, agent, version string) (RegisterRequest, *protocol.ProcessIdentity) {
	t.Helper()
	self, err := CurrentProcess()
	require.NoError(t, err)
	claim := self
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return RegisterRequest{
		Agent:     protocol.AgentID(agent),
		Process:   claim,
		PublicKey: pub,
		KeyID:     "ed25519:" + agent + ":v" + version,
	}, &claim
}

// enrollLiveSession registers agent in the database with a live session
// anchored in the calling test process, and returns its request.
func enrollLiveSession(ctx context.Context, t *testing.T, db *sql.DB, agent, session string) RegisterRequest {
	t.Helper()
	testAgent(ctx, t, db, agent)
	req, claim := testRequest(t, agent, "1")
	testSession(ctx, t, db, session, agent, claim, nil)
	return req
}

// TestRegisterEnroll proves the happy path: lineage through the live
// session, an ACTIVE key readable back, and no block.
func TestRegisterEnroll(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-enroll", "session-enroll")

	result, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)
	require.Equal(t, OutcomeRegistered, result.Outcome)
	require.Equal(t, req.KeyID, result.Key.KeyID)
	require.Equal(t, req.Agent, result.Key.Agent)
	require.Equal(t, []byte(req.PublicKey), []byte(result.Key.PublicKey))

	back, err := reg.ActiveSigningKey(ctx, store.DB(), req.Agent)
	require.NoError(t, err)
	require.Equal(t, result.Key, back)

	blocked, err := reg.Blocked(ctx, store.DB(), req.Agent)
	require.NoError(t, err)
	require.False(t, blocked)
}

// TestRegisterIdempotent proves re-registering the same key stays ACTIVE
// with a single row.
func TestRegisterIdempotent(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-idem", "session-idem")

	first, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)
	second, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)
	require.Equal(t, OutcomeRegistered, second.Outcome)
	require.Equal(t, first.Key, second.Key)

	var count int
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_keys WHERE agent_id = ?`, string(req.Agent)).Scan(&count))
	require.Equal(t, 1, count)
}

// TestRegisterKeyIDs proves the key ID shape: ed25519:<agent>:v<n>.
func TestRegisterKeyIDs(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-keyid", "session-keyid")

	for _, keyID := range []string{
		"ed25519:other-agent:v1",
		"ed25519:agent-keyid",
		"ed25519:agent-keyid:v",
		"ed25519:agent-keyid:vx",
		"x25519:agent-keyid:v1",
		"",
	} {
		bad := req
		bad.KeyID = keyID
		_, err := reg.Register(ctx, store.DB(), bad)
		require.ErrorIs(t, err, ErrInvalidArgument, "key id %q", keyID)
	}
}

// TestRegisterUnrelatedProcess proves lineage failures: a recycled pid
// borrowing another start time, and a pid with no session at all.
func TestRegisterUnrelatedProcess(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-lineage", "session-lineage")

	shifted := req
	shifted.Process.ProcessStartTime++
	_, err := reg.Register(ctx, store.DB(), shifted)
	require.ErrorIs(t, err, ErrUnrelatedProcess)

	ended := "2026-01-02T00:00:00.000000000Z"
	testAgent(ctx, t, store.DB(), "agent-ended")
	testSession(ctx, t, store.DB(), "session-ended", "agent-ended", &req.Process, &ended)
	endedReq := RegisterRequest{
		Agent:     "agent-ended",
		Process:   req.Process,
		PublicKey: req.PublicKey,
		KeyID:     "ed25519:agent-ended:v1",
	}
	_, err = reg.Register(ctx, store.DB(), endedReq)
	require.ErrorIs(t, err, ErrNoRegisteredAncestor)
}

// TestSubstitutionSuspends proves the conflict path: a second key for an
// agent with a usable ACTIVE key lands REJECTED, the current key moves to
// SUSPENDED, and the agent is left blocked with no usable key.
func TestSubstitutionSuspends(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	first := enrollLiveSession(ctx, t, store.DB(), "agent-sub", "session-sub")

	_, err := reg.Register(ctx, store.DB(), first)
	require.NoError(t, err)

	secondPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	second := RegisterRequest{
		Agent:     first.Agent,
		Process:   first.Process,
		PublicKey: secondPub,
		KeyID:     "ed25519:agent-sub:v2",
	}
	result, err := reg.Register(ctx, store.DB(), second)
	require.NoError(t, err)
	require.Equal(t, OutcomeSuspended, result.Outcome)
	require.Equal(t, second.KeyID, result.Key.KeyID)

	_, err = reg.ActiveSigningKey(ctx, store.DB(), first.Agent)
	require.ErrorIs(t, err, ErrNoUsableKey)

	blocked, err := reg.Blocked(ctx, store.DB(), first.Agent)
	require.NoError(t, err)
	require.True(t, blocked)

	var firstStatus, secondStatus string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT status FROM agent_keys WHERE key_id = ?`, first.KeyID).Scan(&firstStatus))
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT status FROM agent_keys WHERE key_id = ?`, second.KeyID).Scan(&secondStatus))
	require.Equal(t, "SUSPENDED", firstStatus)
	require.Equal(t, "REJECTED", secondStatus)
}

// TestAdoptKeepsState proves the same key ID with the same key material
// never duplicates: re-registering the suspended key reports suspended
// with one row, and another key under the taken ID is refused.
func TestAdoptKeepsState(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	again, err := reg.Register(ctx, store.DB(), suspended)
	require.NoError(t, err)
	require.Equal(t, OutcomeSuspended, again.Outcome)

	var count int
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_keys WHERE key_id = ?`, suspended.KeyID).Scan(&count))
	require.Equal(t, 1, count)

	otherPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	clash := suspended
	clash.PublicKey = otherPub
	_, err = reg.Register(ctx, store.DB(), clash)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// TestReviewRestore proves a human restores the suspended key to ACTIVE.
func TestReviewRestore(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	require.NoError(t, reg.Review(ctx, store.DB(), suspended.Agent, ReviewDecision{Action: ReviewRestore}))

	back, err := reg.ActiveSigningKey(ctx, store.DB(), suspended.Agent)
	require.NoError(t, err)
	require.Equal(t, suspended.KeyID, back.KeyID)

	blocked, err := reg.Blocked(ctx, store.DB(), suspended.Agent)
	require.NoError(t, err)
	require.False(t, blocked)
}

// TestReviewRotate proves the authorized rotation: the rejected key
// becomes ACTIVE and the suspended one retires to ROTATED.
func TestReviewRotate(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	var rejectedID string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT key_id FROM agent_keys WHERE agent_id = ? AND status = 'REJECTED'`,
		string(suspended.Agent)).Scan(&rejectedID))

	require.NoError(t, reg.Review(ctx, store.DB(), suspended.Agent,
		ReviewDecision{Action: ReviewRotate, KeyID: rejectedID}))

	back, err := reg.ActiveSigningKey(ctx, store.DB(), suspended.Agent)
	require.NoError(t, err)
	require.Equal(t, rejectedID, back.KeyID)

	var old string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT status FROM agent_keys WHERE key_id = ?`, suspended.KeyID).Scan(&old))
	require.Equal(t, "ROTATED", old)
}

// TestReviewRotateUnknownKey proves rotation names a REJECTED key of the
// agent: anything else is rejected.
func TestReviewRotateUnknownKey(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	for _, decision := range []ReviewDecision{
		{Action: ReviewRotate},
		{Action: ReviewRotate, KeyID: suspended.KeyID},
		{Action: ReviewRotate, KeyID: "ed25519:agent-rot:v9"},
		{Action: "archive"},
	} {
		require.ErrorIs(t, reg.Review(ctx, store.DB(), suspended.Agent, decision), ErrInvalidArgument)
	}
}

// TestReviewRevokeIsTerminal proves revocation: the agent stays blocked,
// no Review restores it, and Register refuses.
func TestReviewRevokeIsTerminal(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	require.NoError(t, reg.Review(ctx, store.DB(), suspended.Agent, ReviewDecision{Action: ReviewRevoke}))

	blocked, err := reg.Blocked(ctx, store.DB(), suspended.Agent)
	require.NoError(t, err)
	require.True(t, blocked)
	_, err = reg.ActiveSigningKey(ctx, store.DB(), suspended.Agent)
	require.ErrorIs(t, err, ErrNoUsableKey)

	require.ErrorIs(t,
		reg.Review(ctx, store.DB(), suspended.Agent, ReviewDecision{Action: ReviewRestore}),
		ErrRevoked)

	freshPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = reg.Register(ctx, store.DB(), RegisterRequest{
		Agent:     suspended.Agent,
		Process:   suspended.Process,
		PublicKey: freshPub,
		KeyID:     "ed25519:" + string(suspended.Agent) + ":v3",
	})
	require.ErrorIs(t, err, ErrRevoked)
}

// TestReviewWithoutBlock proves Review needs a suspended key.
func TestReviewWithoutBlock(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-calm", "session-calm")
	_, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)

	require.ErrorIs(t,
		reg.Review(ctx, store.DB(), req.Agent, ReviewDecision{Action: ReviewRestore}),
		ErrNotBlocked)
}

// suspendAgent enrolls one key and triggers the substitution conflict,
// returning the suspended first request.
func suspendAgent(ctx context.Context, t *testing.T, reg *Registry, store *eventstore.Store) RegisterRequest {
	t.Helper()
	agent := "agent-" + strings.ToLower(t.Name())
	first := enrollLiveSession(ctx, t, store.DB(), agent, "session-"+t.Name())
	_, err := reg.Register(ctx, store.DB(), first)
	require.NoError(t, err)

	secondPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	result, err := reg.Register(ctx, store.DB(), RegisterRequest{
		Agent:     first.Agent,
		Process:   first.Process,
		PublicKey: secondPub,
		KeyID:     "ed25519:" + agent + ":v2",
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeSuspended, result.Outcome)
	return first
}

// TestTamperedRowFailsClosed proves reads fail closed: a row whose status
// changed without a fresh signature, and one with a damaged signature,
// both count as absent.
func TestTamperedRowFailsClosed(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-tamper", "session-tamper")
	_, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)

	_, err = store.DB().ExecContext(ctx,
		`UPDATE agent_keys SET status = 'SUSPENDED' WHERE key_id = ?`, req.KeyID)
	require.NoError(t, err)
	_, err = reg.ActiveSigningKey(ctx, store.DB(), req.Agent)
	require.ErrorIs(t, err, ErrNoUsableKey)

	_, err = store.DB().ExecContext(ctx,
		`UPDATE agent_keys SET record_signature = randomblob(64) WHERE key_id = ?`, req.KeyID)
	require.NoError(t, err)
	_, err = reg.ActiveSigningKey(ctx, store.DB(), req.Agent)
	require.ErrorIs(t, err, ErrNoUsableKey)
}

// TestExpiredKeyUnusable proves an ACTIVE row past its valid_until counts
// as no key. The row is signed with the past expiry, so the signature is
// genuine: only the clock disqualifies it.
func TestExpiredKeyUnusable(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	loaded, err := loadNodeKey(dir)
	require.NoError(t, err)

	keyPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	past := time.Now().UTC().Add(-time.Hour).Format(timeLayout)
	rec := keyRecord{
		keyID: "ed25519:agent-old:v1", agentID: "agent-old", nodeID: "test-node",
		purpose: keyPurposeSigning, algorithm: keyAlgorithmEd25519,
		publicKey: []byte(keyPub), status: statusActive,
		validFrom:  time.Now().UTC().Add(-2 * time.Hour).Format(timeLayout),
		validUntil: &past, issuer: "node:test-node",
	}

	store := testStore(t)
	ctx := t.Context()
	testAgent(ctx, t, store.DB(), "agent-old")
	require.NoError(t, insertRow(ctx, store.DB(), loaded.priv, rec))

	reg, err := New("test-node", dir, clock.System{})
	require.NoError(t, err)
	_, err = reg.ActiveSigningKey(ctx, store.DB(), "agent-old")
	require.ErrorIs(t, err, ErrNoUsableKey)
}

// TestLostNodeKeyFailsClosed proves losing the node key verifies nothing,
// and agents register anew under the replacement: the old rows stay but
// count as absent.
func TestLostNodeKeyFailsClosed(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-lost", "session-lost")
	_, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)

	replacement, err := New("test-node", testDataDir(t), clock.System{})
	require.NoError(t, err)
	_, err = replacement.ActiveSigningKey(ctx, store.DB(), req.Agent)
	require.ErrorIs(t, err, ErrNoUsableKey)

	result, err := replacement.Register(ctx, store.DB(), req)
	require.NoError(t, err)
	require.Equal(t, OutcomeRegistered, result.Outcome)
}

// TestNodeKeyPermissions proves the key layout: directories at 0700 and
// the seed file at 0600, created on first use and reused afterwards.
func TestNodeKeyPermissions(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	first, err := loadNodeKey(dir)
	require.NoError(t, err)
	second, err := loadNodeKey(dir)
	require.NoError(t, err)
	require.Equal(t, first.priv, second.priv)

	for _, path := range []string{
		filepath.Join(dir, "keys"),
		filepath.Join(dir, "keys", "node"),
	} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		require.Equal(t, os.FileMode(0o700), info.Mode().Perm(), path)
	}
	info, err := os.Stat(filepath.Join(dir, "keys", "node", "node.seed"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

// TestTimeLayoutMatchesEventstore keeps the copied time layout honest.
func TestTimeLayoutMatchesEventstore(t *testing.T) {
	t.Parallel()
	require.Equal(t, eventstore.TimeLayout, timeLayout)
}

// blockAgentKeyUpdates forbids every UPDATE on agent_keys, so the second
// write of a two-write change fails and must roll the first back.
func blockAgentKeyUpdates(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `CREATE TRIGGER fail_registry_update BEFORE UPDATE ON agent_keys
		BEGIN SELECT RAISE(ABORT, 'boom'); END;`)
	require.NoError(t, err)
}

// agentKeyStatus reads the stored status of one key.
func agentKeyStatus(ctx context.Context, t *testing.T, db *sql.DB, keyID string) string {
	t.Helper()
	var status string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT status FROM agent_keys WHERE key_id = ?`, keyID).Scan(&status))
	return status
}

// agentKeyCount counts the stored rows of one agent.
func agentKeyCount(ctx context.Context, t *testing.T, db *sql.DB, agent protocol.AgentID) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM agent_keys WHERE agent_id = ?`, string(agent)).Scan(&count))
	return count
}

// TestSubstituteRollsBack proves substitution is atomic: when the
// suspension write fails, the rejected attempt rolls back with it instead
// of stranding half a conflict.
func TestSubstituteRollsBack(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	first := enrollLiveSession(ctx, t, store.DB(), "agent-rollback", "session-rollback")
	_, err := reg.Register(ctx, store.DB(), first)
	require.NoError(t, err)

	blockAgentKeyUpdates(ctx, t, store.DB())
	secondPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, err = reg.Register(ctx, store.DB(), RegisterRequest{
		Agent:     first.Agent,
		Process:   first.Process,
		PublicKey: secondPub,
		KeyID:     "ed25519:agent-rollback:v2",
	})
	require.Error(t, err)

	require.Equal(t, 1, agentKeyCount(ctx, t, store.DB(), first.Agent))
	back, err := reg.ActiveSigningKey(ctx, store.DB(), first.Agent)
	require.NoError(t, err)
	require.Equal(t, first.KeyID, back.KeyID)
}

// TestRotateRollsBack proves rotation is atomic: when the retirement
// write fails, the promotion rolls back with it and both rows keep their
// pre-review states.
func TestRotateRollsBack(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	var rejectedID string
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT key_id FROM agent_keys WHERE agent_id = ? AND status = 'REJECTED'`,
		string(suspended.Agent)).Scan(&rejectedID))

	blockAgentKeyUpdates(ctx, t, store.DB())
	err := reg.Review(ctx, store.DB(), suspended.Agent,
		ReviewDecision{Action: ReviewRotate, KeyID: rejectedID})
	require.Error(t, err)

	require.Equal(t, "SUSPENDED", agentKeyStatus(ctx, t, store.DB(), suspended.KeyID))
	require.Equal(t, "REJECTED", agentKeyStatus(ctx, t, store.DB(), rejectedID))
}

// TestAdoptTamperedSiblingFailsClosed proves adopt never launders a
// tampered row: with a verifying sibling the node key is alive, so the
// broken row fails closed instead of being re-signed.
func TestAdoptTamperedSiblingFailsClosed(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	suspended := suspendAgent(ctx, t, reg, store)

	var rejectedID string
	var rejectedKey []byte
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT key_id, public_key FROM agent_keys WHERE agent_id = ? AND status = 'REJECTED'`,
		string(suspended.Agent)).Scan(&rejectedID, &rejectedKey))
	_, err := store.DB().ExecContext(ctx,
		`UPDATE agent_keys SET record_signature = randomblob(64) WHERE key_id = ?`, rejectedID)
	require.NoError(t, err)

	_, err = reg.Register(ctx, store.DB(), RegisterRequest{
		Agent:     suspended.Agent,
		Process:   suspended.Process,
		PublicKey: rejectedKey,
		KeyID:     rejectedID,
	})
	require.ErrorIs(t, err, ErrNoUsableKey)

	require.Equal(t, 2, agentKeyCount(ctx, t, store.DB(), suspended.Agent))
	require.Equal(t, "SUSPENDED", agentKeyStatus(ctx, t, store.DB(), suspended.KeyID))
}

// TestRebuiltRowDropsTampering proves recovery substitutes the whole row:
// after the node key is lost, a row with poisoned columns comes back
// built from the request and local invariants only.
func TestRebuiltRowDropsTampering(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-rebuild", "session-rebuild")
	_, err := reg.Register(ctx, store.DB(), req)
	require.NoError(t, err)

	_, err = store.DB().ExecContext(ctx,
		`UPDATE agent_keys SET issuer = 'node:evil', valid_until = '2030-01-01T00:00:00.000000000Z',
			status = 'SUSPENDED' WHERE key_id = ?`, req.KeyID)
	require.NoError(t, err)

	replacement, err := New("test-node", testDataDir(t), clock.System{})
	require.NoError(t, err)
	result, err := replacement.Register(ctx, store.DB(), req)
	require.NoError(t, err)
	require.Equal(t, OutcomeRegistered, result.Outcome)

	var issuer, status string
	var validUntil sql.NullString
	require.NoError(t, store.DB().QueryRowContext(ctx,
		`SELECT issuer, status, valid_until FROM agent_keys WHERE key_id = ?`,
		req.KeyID).Scan(&issuer, &status, &validUntil))
	require.Equal(t, "node:test-node", issuer)
	require.Equal(t, "ACTIVE", status)
	require.False(t, validUntil.Valid)

	back, err := replacement.ActiveSigningKey(ctx, store.DB(), req.Agent)
	require.NoError(t, err)
	require.Equal(t, req.KeyID, back.KeyID)
}

// TestRegisterInCallerTx proves a caller-owned transaction is honoured:
// the registry writes through it and the caller decides the commit.
func TestRegisterInCallerTx(t *testing.T) {
	t.Parallel()
	reg, store := testRegistry(t)
	ctx := t.Context()
	req := enrollLiveSession(ctx, t, store.DB(), "agent-tx", "session-tx")

	tx, err := store.DB().BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = reg.Register(ctx, tx, req)
	require.NoError(t, err)
	back, err := reg.ActiveSigningKey(ctx, tx, req.Agent)
	require.NoError(t, err)
	require.Equal(t, req.KeyID, back.KeyID)
	require.NoError(t, tx.Rollback())

	_, err = reg.ActiveSigningKey(ctx, store.DB(), req.Agent)
	require.ErrorIs(t, err, ErrNoUsableKey)
}

// TestNodeKeyRefusesInsecure proves loading fails closed on loosened
// modes and on a short file: nothing is repaired silently.
func TestNodeKeyRefusesInsecure(t *testing.T) {
	t.Parallel()
	looseDir := t.TempDir()
	require.NoError(t, os.Chmod(looseDir, 0o755))
	_, err := loadNodeKey(looseDir)
	require.ErrorIs(t, err, ErrInvalidArgument)

	shortDir := testDataDir(t)
	shortKeys := filepath.Join(shortDir, "keys", "node")
	require.NoError(t, os.MkdirAll(shortKeys, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(shortKeys, "node.seed"), []byte("short"), 0o600))
	_, err = loadNodeKey(shortDir)
	require.ErrorIs(t, err, ErrInvalidArgument)

	looseFileDir := testDataDir(t)
	looseKeys := filepath.Join(looseFileDir, "keys", "node")
	require.NoError(t, os.MkdirAll(looseKeys, 0o700))
	seed := make([]byte, ed25519.SeedSize)
	_, err = rand.Read(seed)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(looseKeys, "node.seed"), seed, 0o640))
	_, err = loadNodeKey(looseFileDir)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// TestNodeKeyConcurrentCreate proves racing first uses agree on one key:
// creation is atomic, so no reader observes a partial seed.
func TestNodeKeyConcurrentCreate(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	const racers = 8
	type outcome struct {
		key nodeKey
		err error
	}
	arrived := make(chan outcome, racers)
	for range racers {
		go func() {
			loaded, err := loadNodeKey(dir)
			arrived <- outcome{key: loaded, err: err}
		}()
	}
	var first ed25519.PrivateKey
	for range racers {
		got := <-arrived
		require.NoError(t, got.err)
		if first == nil {
			first = got.key.priv
		} else {
			require.Equal(t, []byte(first), []byte(got.key.priv))
		}
	}
}

// TestNewValidation proves the constructor only accepts a canonical node
// id, a data directory, and a clock.
func TestNewValidation(t *testing.T) {
	t.Parallel()
	dir := testDataDir(t)
	for _, id := range []protocol.NodeID{"", "UPPER", "has space", protocol.NodeID(strings.Repeat("a", 65))} {
		_, err := New(id, dir, clock.System{})
		require.ErrorIs(t, err, ErrInvalidArgument, "node %q", id)
	}
	_, err := New("ok-node", "", clock.System{})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = New("ok-node", dir, nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
}
