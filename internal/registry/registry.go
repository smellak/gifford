package registry

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the registry: registering agent signing keys, reporting
// the usable key, tracking blocks, and applying human review decisions.
// Every method takes the store handle to use: reads take a Querier, writes
// take an Execer, and the caller owns the transaction. State changes are
// re-signed inside the same transaction that writes them.

// timeLayout is the store's time format: RFC 3339 in UTC with exactly nine
// fractional digits, so text order is time order. A test requires it to
// stay equal to eventstore.TimeLayout.
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// Key purposes and algorithms are the only combinations this package writes.
const (
	KeyPurposeSigning    = "signing"
	KeyPurposeEncryption = "encryption"
	KeyAlgorithmEd25519  = "ed25519"
	KeyAlgorithmX25519   = "x25519"

	keyPurposeSigning   = KeyPurposeSigning
	keyAlgorithmEd25519 = KeyAlgorithmEd25519
)

// Registry is the key directory of one node: nodeID names the local node
// (issuer "node:<node_id>") and dataDir is the node's data directory
// holding the node signing key. It is safe for concurrent use; Registry
// methods never touch the global data directory, only the configured one,
// so tests run against scratch directories.
type Registry struct {
	nodeID  protocol.NodeID
	dataDir string
	clk     clock.Clock
	mu      sync.Mutex
	key     *nodeKey
}

// New returns the registry of the local node. The node id must be
// canonical, and the clock drives expiry: production passes clock.System
// and tests pass their own. It performs no I/O: the node key is created
// on first use.
func New(nodeID protocol.NodeID, dataDir string, clk clock.Clock) (*Registry, error) {
	if !nodeID.Valid() || dataDir == "" || clk == nil {
		return nil, fmt.Errorf("new registry: %w", ErrInvalidArgument)
	}
	return &Registry{nodeID: nodeID, dataDir: dataDir, clk: clk}, nil
}

// now returns the current time in UTC for stored and compared stamps.
func (r *Registry) now() time.Time {
	return r.clk.Now().UTC()
}

// nodePublic returns the node public key, loading or creating the node
// key on first use.
func (r *Registry) nodePublic() (ed25519.PublicKey, error) {
	priv, err := r.nodePrivate()
	if err != nil {
		return nil, err
	}
	return priv.Public().(ed25519.PublicKey), nil
}

// nodePrivate loads or creates the node key once per Registry.
func (r *Registry) nodePrivate() (ed25519.PrivateKey, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.key != nil {
		return r.key.priv, nil
	}
	loaded, err := loadNodeKey(r.dataDir)
	if err != nil {
		return nil, err
	}
	r.key = &loaded
	return loaded.priv, nil
}

// KeyInfo is one usable agent signing key.
type KeyInfo struct {
	KeyID      string
	Agent      protocol.AgentID
	PublicKey  ed25519.PublicKey
	Purpose    string
	Algorithm  string
	ValidFrom  time.Time
	ValidUntil *time.Time
}

// RegisterRequest registers the signing key of an agent: the agent, its
// verified ProcessIdentity, the Ed25519 public key, and its key ID of the
// form ed25519:<agent>:v<n>.
type RegisterRequest struct {
	Agent     protocol.AgentID
	Process   protocol.ProcessIdentity
	PublicKey ed25519.PublicKey
	KeyID     string
	Purpose   string
	Algorithm string
}

// RegisterOutcome tells what Register did with the request.
type RegisterOutcome string

// Register outcomes.
const (
	// OutcomeRegistered means the key is now the agent's ACTIVE key.
	OutcomeRegistered RegisterOutcome = "registered"
	// OutcomeSuspended means the request looked like a substitution: the
	// attempted key is REJECTED and the agent is left without an ACTIVE
	// key until a Review.
	OutcomeSuspended RegisterOutcome = "suspended"
)

// RegisterResult is what Register stored.
type RegisterResult struct {
	Key     KeyInfo
	Outcome RegisterOutcome
}

// Register records req's key. It succeeds only if req's process descends
// from a live registered session of req's agent. Registering the key that
// is already ACTIVE is idempotent. Registering another key while one is
// usable ACTIVE is a substitution: in the same transaction the attempted
// key lands REJECTED and the current one moves to SUSPENDED, both
// re-signed, and the result reports OutcomeSuspended. While the agent is
// blocked the attempt still lands REJECTED and the agent stays blocked; a
// revoked agent is terminal and Register refuses.
func (r *Registry) Register(ctx context.Context, tx Execer, req RegisterRequest) (RegisterResult, error) {
	req = normalizedRegisterRequest(req)
	if err := validRequest(req); err != nil {
		return RegisterResult{}, err
	}
	var result RegisterResult
	if err := withTx(ctx, tx, func(tx Execer) error {
		var err error
		result, err = r.register(ctx, tx, req)
		return err
	}); err != nil {
		return RegisterResult{}, err
	}
	return result, nil
}

// register is Register inside the transaction: reads and every state
// change share it, so a failing second write rolls the first back.
func (r *Registry) register(ctx context.Context, tx Execer, req RegisterRequest) (RegisterResult, error) {
	process := int(req.Process.PID)
	if err := lineageForAgent(ctx, tx, process, req.Agent, req.Process); err != nil {
		return RegisterResult{}, err
	}
	rows, err := agentRows(ctx, tx, req.Agent)
	if err != nil {
		return RegisterResult{}, err
	}
	if err := requireRowsFromNode(rows, r.nodeID); err != nil {
		return RegisterResult{}, err
	}
	verified, err := r.verifiedRowsFor(ctx, tx, rows, req.Purpose, req.Algorithm)
	if err != nil {
		return RegisterResult{}, err
	}
	if verified.revoked {
		return RegisterResult{}, fmt.Errorf("register %s: %w", req.Agent, ErrRevoked)
	}
	now := r.now()
	// A row already holding this key ID decides by key material. The same
	// key is idempotent. Different material is a substitution attempt even
	// though the primary key prevents storing a second row under that ID:
	// the current ACTIVE key must still be suspended durably.
	for _, rec := range rows {
		if rec.keyID != req.KeyID {
			continue
		}
		if string(rec.publicKey) != string(req.PublicKey) {
			return r.substituteTakenID(ctx, tx, req, verified, now)
		}
		return r.adopt(ctx, tx, rows, req, rec)
	}
	switch active := verified.usable(now); {
	case active != nil && string(active.publicKey) == string(req.PublicKey):
		return RegisterResult{Key: keyInfo(*active), Outcome: OutcomeRegistered}, nil
	case active != nil:
		return r.substitute(ctx, tx, req, *active, now)
	case verified.hasSuspended():
		return r.refuseBlocked(ctx, tx, req, now)
	default:
		return r.enroll(ctx, tx, req, now)
	}
}

// substituteTakenID blocks a material collision under an existing key ID.
// The attempted key cannot get its own row because key_id is the primary
// key, but suspending the usable key makes the substitution durable and
// leaves the agent without signing authority until review.
func (r *Registry) substituteTakenID(
	ctx context.Context,
	tx Execer,
	req RegisterRequest,
	verified verifiedSet,
	now time.Time,
) (RegisterResult, error) {
	attempted := r.newRecord(req, statusRejected, now)
	active := verified.usable(now)
	if active == nil {
		if verified.hasSuspended() {
			return RegisterResult{Key: keyInfo(attempted), Outcome: OutcomeSuspended}, nil
		}
		return RegisterResult{}, fmt.Errorf("register %s: key id %q binds another key: %w",
			req.Agent, req.KeyID, ErrInvalidArgument)
	}
	priv, err := r.nodePrivate()
	if err != nil {
		return RegisterResult{}, err
	}
	active.status = statusSuspended
	if err := updateRow(ctx, tx, priv, *active); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{Key: keyInfo(attempted), Outcome: OutcomeSuspended}, nil
}

// adopt resolves a re-registration of the key already stored under req's
// key ID. A row that still verifies reports the outcome its status
// carries, with no writes: ACTIVE registers, SUSPENDED and REJECTED stay
// suspended, REVOKED stays terminal, and a ROTATED key is refused so
// rotation cannot be undone by re-registering.
//
// A row that no longer verifies is never re-signed as read: inherited
// columns may be tampered, and signing them would launder the tampering
// into a valid record. Instead adopt fails closed while any sibling row
// verifies — then the node key is alive and this row was tampered — and
// otherwise rebuilds the whole row from the request and local invariants
// alone. The database file is trusted storage in the seed's trust domain;
// signatures detect cross-node mixups, and lineage-anchored requests
// recover from them.
func (r *Registry) adopt(
	ctx context.Context,
	tx Execer,
	rows []keyRecord,
	req RegisterRequest,
	rec keyRecord,
) (RegisterResult, error) {
	pub, err := r.nodePublic()
	if err != nil {
		return RegisterResult{}, err
	}
	if verifyRecord(pub, rec, rec.signature) {
		return outcomeByStatus(rec)
	}
	for _, sibling := range rows {
		if sibling.keyID == rec.keyID {
			continue
		}
		if verifyRecord(pub, sibling, sibling.signature) {
			return RegisterResult{}, fmt.Errorf("register %s: key id %q does not verify: %w",
				req.Agent, req.KeyID, ErrNoUsableKey)
		}
	}
	priv, err := r.nodePrivate()
	if err != nil {
		return RegisterResult{}, err
	}
	rebuilt := r.newRecord(req, statusActive, r.now())
	if err := replaceRow(ctx, tx, priv, rec.keyID, rebuilt); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{Key: keyInfo(rebuilt), Outcome: OutcomeRegistered}, nil
}

// outcomeByStatus reports the outcome a verified stored row carries.
func outcomeByStatus(rec keyRecord) (RegisterResult, error) {
	switch rec.status {
	case statusActive:
		return RegisterResult{Key: keyInfo(rec), Outcome: OutcomeRegistered}, nil
	case statusSuspended, statusRejected:
		return RegisterResult{Key: keyInfo(rec), Outcome: OutcomeSuspended}, nil
	case statusRevoked:
		return RegisterResult{}, fmt.Errorf("register %s: %w", rec.agentID, ErrRevoked)
	default:
		return RegisterResult{}, fmt.Errorf("register %s: key id %q is retired: %w",
			rec.agentID, rec.keyID, ErrInvalidArgument)
	}
}

// substitute records a substitution attempt: the attempted key REJECTED
// and the current usable key SUSPENDED, both signed in this transaction.
func (r *Registry) substitute(
	ctx context.Context,
	tx Execer,
	req RegisterRequest,
	current keyRecord,
	now time.Time,
) (RegisterResult, error) {
	priv, err := r.nodePrivate()
	if err != nil {
		return RegisterResult{}, err
	}
	attempted := r.newRecord(req, statusRejected, now)
	if err := insertRow(ctx, tx, priv, attempted); err != nil {
		return RegisterResult{}, err
	}
	current.status = statusSuspended
	if err := updateRow(ctx, tx, priv, current); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{Key: keyInfo(attempted), Outcome: OutcomeSuspended}, nil
}

// refuseBlocked records an attempt made while the agent is blocked: the
// key lands REJECTED and the agent stays without an ACTIVE key.
func (r *Registry) refuseBlocked(
	ctx context.Context,
	tx Execer,
	req RegisterRequest,
	now time.Time,
) (RegisterResult, error) {
	priv, err := r.nodePrivate()
	if err != nil {
		return RegisterResult{}, err
	}
	attempted := r.newRecord(req, statusRejected, now)
	if err := insertRow(ctx, tx, priv, attempted); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{Key: keyInfo(attempted), Outcome: OutcomeSuspended}, nil
}

// enroll records a first (or post-expiry) key as ACTIVE.
func (r *Registry) enroll(
	ctx context.Context,
	tx Execer,
	req RegisterRequest,
	now time.Time,
) (RegisterResult, error) {
	priv, err := r.nodePrivate()
	if err != nil {
		return RegisterResult{}, err
	}
	rec := r.newRecord(req, statusActive, now)
	if err := insertRow(ctx, tx, priv, rec); err != nil {
		return RegisterResult{}, err
	}
	return RegisterResult{Key: keyInfo(rec), Outcome: OutcomeRegistered}, nil
}

// ActiveSigningKey returns the agent's single usable signing key: status
// ACTIVE, unexpired, with a valid issuer signature, and (for remote keys)
// issued by a TRUSTED peer. Every other case is an error wrapping
// ErrNoUsableKey: none, a bad signature, expired, revoked, suspended,
// peer-blocked, or ambiguously duplicated.
func (r *Registry) ActiveSigningKey(ctx context.Context, q Querier, agent protocol.AgentID) (KeyInfo, error) {
	if !agent.Valid() {
		return KeyInfo{}, fmt.Errorf("active key: agent %q: %w", agent, ErrInvalidArgument)
	}
	rows, err := agentRows(ctx, q, agent)
	if err != nil {
		return KeyInfo{}, err
	}
	verified, err := r.verifiedRows(ctx, q, rows)
	if err != nil {
		return KeyInfo{}, err
	}
	active := verified.usable(r.now())
	if active == nil {
		return KeyInfo{}, fmt.Errorf("agent %s: %w", agent, ErrNoUsableKey)
	}
	return keyInfo(*active), nil
}

// ActiveEncryptionKey returns the agent's single usable X25519 key. Local
// encryption keys follow the same signed-record and substitution rules as
// signing keys, but are evaluated independently by purpose.
func (r *Registry) ActiveEncryptionKey(ctx context.Context, q Querier, agent protocol.AgentID) (KeyInfo, error) {
	if !agent.Valid() {
		return KeyInfo{}, fmt.Errorf("active encryption key: agent %q: %w", agent, ErrInvalidArgument)
	}
	rows, err := agentRows(ctx, q, agent)
	if err != nil {
		return KeyInfo{}, err
	}
	verified, err := r.verifiedRowsFor(ctx, q, rows, KeyPurposeEncryption, KeyAlgorithmX25519)
	if err != nil {
		return KeyInfo{}, err
	}
	active := verified.usable(r.now())
	if active == nil {
		return KeyInfo{}, fmt.Errorf("agent %s: %w", agent, ErrNoUsableKey)
	}
	return keyInfo(*active), nil
}

// Blocked reports whether the agent is blocked: no usable ACTIVE key while
// a verified SUSPENDED or REVOKED key exists, multiple ACTIVE keys compete,
// or its issuing peer is blocked. An agent with no keys at all is not
// blocked, it simply has no key.
func (r *Registry) Blocked(ctx context.Context, q Querier, agent protocol.AgentID) (bool, error) {
	if !agent.Valid() {
		return false, fmt.Errorf("blocked: agent %q: %w", agent, ErrInvalidArgument)
	}
	rows, err := agentRows(ctx, q, agent)
	if err != nil {
		return false, err
	}
	verified, err := r.verifiedRows(ctx, q, rows)
	if err != nil {
		return false, err
	}
	if verified.usable(r.now()) != nil {
		return false, nil
	}
	return verified.peerBlocked || verified.hasSuspended() || verified.revoked || verified.ambiguous(r.now()), nil
}

// ReviewAction is the human decision resolving a block.
type ReviewAction string

// Review actions.
const (
	// ReviewRestore returns the suspended key to ACTIVE.
	ReviewRestore ReviewAction = "restore"
	// ReviewRotate promotes a REJECTED key to ACTIVE and retires the
	// suspended one to ROTATED. This is the authorized rotation.
	ReviewRotate ReviewAction = "rotate"
	// ReviewRevoke retires the suspended key to REVOKED. Nothing restores
	// a revoked key.
	ReviewRevoke ReviewAction = "revoke"
)

// ReviewDecision is the human resolution of a block. Rotate names the
// REJECTED key to promote in KeyID.
type ReviewDecision struct {
	Action ReviewAction
	KeyID  string
}

// Review applies a human decision to a blocked agent: restoring the
// suspended key, rotating to a rejected one, or revoking. Only the API:
// no CLI exists. Reviewing a revoked agent or an agent with no suspended
// key is an error.
func (r *Registry) Review(
	ctx context.Context,
	tx Execer,
	agent protocol.AgentID,
	decision ReviewDecision,
) error {
	if !agent.Valid() {
		return fmt.Errorf("review: agent %q: %w", agent, ErrInvalidArgument)
	}
	return withTx(ctx, tx, func(tx Execer) error {
		return r.review(ctx, tx, agent, decision)
	})
}

// review is Review inside the transaction: the two writes of a rotation
// share it, so a failing second write rolls the first back.
func (r *Registry) review(
	ctx context.Context,
	tx Execer,
	agent protocol.AgentID,
	decision ReviewDecision,
) error {
	rows, err := agentRows(ctx, tx, agent)
	if err != nil {
		return err
	}
	if err := requireRowsFromNode(rows, r.nodeID); err != nil {
		return err
	}
	verified, err := r.verifiedRows(ctx, tx, rows)
	if err != nil {
		return err
	}
	if verified.revoked {
		return fmt.Errorf("review %s: %w", agent, ErrRevoked)
	}
	suspended := verified.newestSuspended()
	if suspended == nil {
		return fmt.Errorf("review %s: %w", agent, ErrNotBlocked)
	}
	priv, err := r.nodePrivate()
	if err != nil {
		return err
	}
	switch decision.Action {
	case ReviewRestore:
		suspended.status = statusActive
		return updateRow(ctx, tx, priv, *suspended)
	case ReviewRevoke:
		suspended.status = statusRevoked
		return updateRow(ctx, tx, priv, *suspended)
	case ReviewRotate:
		return r.rotate(ctx, tx, priv, verified, *suspended, decision.KeyID)
	default:
		return fmt.Errorf("review %s: action %q: %w", agent, decision.Action, ErrInvalidArgument)
	}
}

// rotate promotes the named REJECTED key to ACTIVE and retires the
// suspended key to ROTATED, signed in this transaction.
func (r *Registry) rotate(
	ctx context.Context,
	tx Execer,
	priv ed25519.PrivateKey,
	verified verifiedSet,
	suspended keyRecord,
	keyID string,
) error {
	if keyID == "" {
		return fmt.Errorf("rotate: empty key id: %w", ErrInvalidArgument)
	}
	var target *keyRecord
	for i := range verified.rejected {
		if verified.rejected[i].keyID == keyID {
			target = &verified.rejected[i]
		}
	}
	if target == nil {
		return fmt.Errorf("rotate to %q: %w", keyID, ErrInvalidArgument)
	}
	next := *target
	next.status = statusActive
	if err := updateRow(ctx, tx, priv, next); err != nil {
		return err
	}
	suspended.status = statusRotated
	return updateRow(ctx, tx, priv, suspended)
}

// verifiedSet is one agent's rows after signature verification.
// Unverifiable rows are dropped: they count as absent.
type verifiedSet struct {
	active      []keyRecord
	suspended   []keyRecord
	rejected    []keyRecord
	revoked     bool
	peerBlocked bool
}

// usable returns the single usable ACTIVE record: verified, unexpired,
// and unambiguous. Zero or several is nil.
func (v verifiedSet) usable(now time.Time) *keyRecord {
	if v.peerBlocked {
		return nil
	}
	var found *keyRecord
	for i := range v.active {
		rec := &v.active[i]
		if expired(rec, now) {
			continue
		}
		if found != nil {
			return nil
		}
		candidate := *rec
		found = &candidate
	}
	return found
}

// ambiguous reports whether more than one unexpired ACTIVE record competes
// for the agent. Remote substitutions stay signed as received, so this is
// their effective SUSPENDED state.
func (v verifiedSet) ambiguous(now time.Time) bool {
	active := 0
	for i := range v.active {
		if expired(&v.active[i], now) {
			continue
		}
		active++
	}
	return active > 1
}

// newestSuspended returns the latest suspended record, or nil.
func (v verifiedSet) newestSuspended() *keyRecord {
	if len(v.suspended) == 0 {
		return nil
	}
	newest := v.suspended[0]
	for _, rec := range v.suspended[1:] {
		if rec.validFrom > newest.validFrom ||
			(rec.validFrom == newest.validFrom && rec.keyID > newest.keyID) {
			newest = rec
		}
	}
	return &newest
}

// verifiedRows verifies every row against the key pinned for the node that
// issued it and partitions the survivors by status. Rows that do not verify
// are dropped. A blocked peer makes every one of its agents unusable.
func (r *Registry) verifiedRows(ctx context.Context, q Querier, rows []keyRecord) (verifiedSet, error) {
	return r.verifiedRowsFor(ctx, q, rows, KeyPurposeSigning, KeyAlgorithmEd25519)
}

func (r *Registry) verifiedRowsFor(
	ctx context.Context,
	q Querier,
	rows []keyRecord,
	purpose string,
	algorithm string,
) (verifiedSet, error) {
	var set verifiedSet
	if len(rows) == 0 {
		return set, nil
	}
	nodeID := protocol.NodeID(rows[0].nodeID)
	if !nodeID.Valid() {
		return verifiedSet{}, fmt.Errorf("agent key names invalid node %q: %w", nodeID, ErrInvalidArgument)
	}
	for _, rec := range rows[1:] {
		if rec.nodeID != string(nodeID) {
			return verifiedSet{}, fmt.Errorf("agent %s has keys from multiple nodes: %w",
				rows[0].agentID, ErrAgentConflict)
		}
	}
	pub, peerBlocked, err := r.recordVerificationKey(ctx, q, nodeID)
	if err != nil {
		return verifiedSet{}, err
	}
	if peerBlocked {
		set.peerBlocked = true
		return set, nil
	}
	if pub == nil {
		return set, nil
	}
	for _, rec := range rows {
		if rec.nodeID != string(nodeID) || rec.issuer != "node:"+string(nodeID) ||
			rec.purpose != purpose || rec.algorithm != algorithm ||
			!verifyRecord(pub, rec, rec.signature) {
			continue
		}
		switch rec.status {
		case statusActive:
			set.active = append(set.active, rec)
		case statusSuspended:
			set.suspended = append(set.suspended, rec)
		case statusRejected:
			set.rejected = append(set.rejected, rec)
		case statusRevoked:
			set.revoked = true
		case statusRotated:
			// History: retired by a rotation, neither usable nor blocking.
		}
	}
	return set, nil
}

func (r *Registry) recordVerificationKey(
	ctx context.Context,
	q Querier,
	nodeID protocol.NodeID,
) (ed25519.PublicKey, bool, error) {
	if nodeID == r.nodeID {
		pub, err := r.nodePublic()
		return pub, false, err
	}
	node, err := nodeByID(ctx, q, nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	status, err := parsePeerStatus(node.status)
	if err != nil {
		return nil, false, err
	}
	if status != PeerStatusTrusted || len(node.signingPublicKey) != ed25519.PublicKeySize {
		return nil, true, nil
	}
	return node.signingPublicKey, false, nil
}

func requireRowsFromNode(rows []keyRecord, nodeID protocol.NodeID) error {
	for _, rec := range rows {
		if rec.nodeID != string(nodeID) {
			return fmt.Errorf("agent %s belongs to node %s: %w",
				rec.agentID, rec.nodeID, ErrAgentConflict)
		}
	}
	return nil
}

// hasSuspended reports whether the set holds a verified suspended key.
func (v verifiedSet) hasSuspended() bool { return len(v.suspended) > 0 }

// expired reports whether rec is past its valid_until.
func expired(rec *keyRecord, now time.Time) bool {
	if rec.validUntil == nil {
		return false
	}
	limit, err := time.Parse(timeLayout, *rec.validUntil)
	if err != nil {
		return true
	}
	return !now.Before(limit)
}

// newRecord builds the unsigned row Register stores.
func (r *Registry) newRecord(req RegisterRequest, status keyStatus, now time.Time) keyRecord {
	return keyRecord{
		keyID:     req.KeyID,
		agentID:   string(req.Agent),
		nodeID:    string(r.nodeID),
		purpose:   req.Purpose,
		algorithm: req.Algorithm,
		publicKey: append([]byte(nil), req.PublicKey...),
		status:    status,
		validFrom: now.Format(timeLayout),
		issuer:    "node:" + string(r.nodeID),
	}
}

// keyInfo converts a stored record to its public description.
func keyInfo(rec keyRecord) KeyInfo {
	info := KeyInfo{
		KeyID:     rec.keyID,
		Agent:     protocol.AgentID(rec.agentID),
		PublicKey: append(ed25519.PublicKey(nil), rec.publicKey...),
		Purpose:   rec.purpose,
		Algorithm: rec.algorithm,
	}
	if from, err := time.Parse(timeLayout, rec.validFrom); err == nil {
		info.ValidFrom = from
	}
	if rec.validUntil != nil {
		if until, err := time.Parse(timeLayout, *rec.validUntil); err == nil {
			limit := until
			info.ValidUntil = &limit
		}
	}
	return info
}

// withTx runs fn with ex as its transaction. A *sql.Tx is used directly,
// so callers can batch registry writes with their own in one atomic unit.
// A *sql.DB opens a real transaction around fn, so multi-statement
// changes commit or roll back together instead of autocommitting
// statement by statement. Any other Execer is used as given.
func withTx(ctx context.Context, ex Execer, fn func(Execer) error) error {
	if tx, ok := ex.(*sql.Tx); ok {
		return fn(tx)
	}
	db, ok := ex.(*sql.DB)
	if !ok {
		return fn(ex)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin registry transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit registry transaction: %w", err)
	}
	return nil
}

// validRequest checks the shape of a register request: a canonical agent
// id, a valid process identity, a 32-byte Ed25519 key, and a key ID of the
// form ed25519:<agent>:v<n> naming that agent.
func validRequest(req RegisterRequest) error {
	if !req.Agent.Valid() {
		return fmt.Errorf("register: agent %q: %w", req.Agent, ErrInvalidArgument)
	}
	if err := req.Process.Validate(); err != nil {
		return fmt.Errorf("register: process: %w", ErrInvalidArgument)
	}
	if len(req.PublicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("register: public key holds %d bytes: %w",
			len(req.PublicKey), ErrInvalidArgument)
	}
	prefix := ""
	switch {
	case req.Purpose == KeyPurposeSigning && req.Algorithm == KeyAlgorithmEd25519:
		prefix = "ed25519:"
	case req.Purpose == KeyPurposeEncryption && req.Algorithm == KeyAlgorithmX25519:
		prefix = "x25519:"
	default:
		return fmt.Errorf("register: unsupported purpose and algorithm: %w", ErrInvalidArgument)
	}
	agent, version, ok := strings.Cut(strings.TrimPrefix(req.KeyID, prefix), ":v")
	if !strings.HasPrefix(req.KeyID, prefix) || !ok || agent != string(req.Agent) {
		return fmt.Errorf("register: key id %q is not %s%s:v<n>: %w",
			req.KeyID, prefix, req.Agent, ErrInvalidArgument)
	}
	if version == "" {
		return fmt.Errorf("register: key id %q has no version: %w", req.KeyID, ErrInvalidArgument)
	}
	for _, digit := range version {
		if digit < '0' || digit > '9' {
			return fmt.Errorf("register: key id %q has a non-numeric version: %w",
				req.KeyID, ErrInvalidArgument)
		}
	}
	return nil
}

func normalizedRegisterRequest(req RegisterRequest) RegisterRequest {
	if req.Purpose == "" && req.Algorithm == "" {
		req.Purpose = KeyPurposeSigning
		req.Algorithm = KeyAlgorithmEd25519
	}
	return req
}

// agentRows reads every agent_keys row of one agent in row order.
func agentRows(ctx context.Context, q Querier, agent protocol.AgentID) ([]keyRecord, error) {
	text, err := queryText("agent_keys_select_agent.sql")
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, text, string(agent))
	if err != nil {
		return nil, fmt.Errorf("select agent keys: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []keyRecord
	for rows.Next() {
		rec, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		found = append(found, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent keys: %w", err)
	}
	return found, nil
}

// scanRow reads one agent_keys row. A stored value the writer could never
// have produced is an error; signature validity is checked later, in
// verifiedRows, so corrupt rows fail closed instead of failing the read.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanRow(rows rowScanner) (keyRecord, error) {
	var rec keyRecord
	var status string
	var validUntil sql.NullString
	var publicKey, signature []byte
	if err := rows.Scan(
		&rec.keyID, &rec.agentID, &rec.nodeID, &rec.purpose, &rec.algorithm,
		&publicKey, &status, &rec.validFrom, &validUntil, &rec.issuer, &signature,
	); err != nil {
		return keyRecord{}, fmt.Errorf("scan agent key: %w", err)
	}
	parsed, err := parseStatus(status)
	if err != nil {
		return keyRecord{}, err
	}
	rec.status = parsed
	rec.publicKey = append([]byte(nil), publicKey...)
	rec.signature = append([]byte(nil), signature...)
	if validUntil.Valid {
		value := validUntil.String
		rec.validUntil = &value
	}
	return rec, nil
}

// insertRow signs rec and inserts it.
func insertRow(ctx context.Context, tx Execer, priv ed25519.PrivateKey, rec keyRecord) error {
	text, err := queryText("agent_keys_insert.sql")
	if err != nil {
		return err
	}
	signature, err := signRecord(priv, rec)
	if err != nil {
		return err
	}
	var validUntil any
	if rec.validUntil != nil {
		validUntil = *rec.validUntil
	}
	_, err = tx.ExecContext(ctx, text,
		rec.keyID, rec.agentID, rec.nodeID, rec.purpose, rec.algorithm,
		rec.publicKey, string(rec.status), rec.validFrom, validUntil,
		rec.issuer, signature,
	)
	if err != nil {
		return fmt.Errorf("insert agent key %q: %w", rec.keyID, err)
	}
	return nil
}

// updateRow re-signs rec under its new status and stores both.
func updateRow(ctx context.Context, tx Execer, priv ed25519.PrivateKey, rec keyRecord) error {
	text, err := queryText("agent_keys_update_status.sql")
	if err != nil {
		return err
	}
	signature, err := signRecord(priv, rec)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, text, string(rec.status), signature, rec.keyID)
	if err != nil {
		return fmt.Errorf("update agent key %q: %w", rec.keyID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count update of agent key %q: %w", rec.keyID, err)
	}
	if affected != 1 {
		return fmt.Errorf("update of agent key %q touched %d rows: %w",
			rec.keyID, affected, ErrInvalidArgument)
	}
	return nil
}

// replaceRow substitutes the whole row holding keyID with rebuilt,
// re-signed: every column is rewritten from the request and local
// invariants, so no inherited unverified field survives.
func replaceRow(
	ctx context.Context,
	tx Execer,
	priv ed25519.PrivateKey,
	keyID string,
	rebuilt keyRecord,
) error {
	text, err := queryText("agent_keys_replace.sql")
	if err != nil {
		return err
	}
	signature, err := signRecord(priv, rebuilt)
	if err != nil {
		return err
	}
	var validUntil any
	if rebuilt.validUntil != nil {
		validUntil = *rebuilt.validUntil
	}
	result, err := tx.ExecContext(ctx, text,
		rebuilt.agentID, rebuilt.nodeID, rebuilt.purpose, rebuilt.algorithm,
		rebuilt.publicKey, string(rebuilt.status), rebuilt.validFrom, validUntil,
		rebuilt.issuer, signature, keyID,
	)
	if err != nil {
		return fmt.Errorf("replace agent key %q: %w", keyID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count replace of agent key %q: %w", keyID, err)
	}
	if affected != 1 {
		return fmt.Errorf("replace of agent key %q touched %d rows: %w",
			keyID, affected, ErrInvalidArgument)
	}
	return nil
}
