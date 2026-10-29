package registry

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

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

// keyPurpose and keyAlgorithm are the only values this package writes.
const (
	keyPurposeSigning   = "signing"
	keyAlgorithmEd25519 = "ed25519"
)

// Registry is the key directory of one node: nodeID names the local node
// (issuer "node:<node_id>") and dataDir is the node's data directory
// holding the node signing key. It is safe for concurrent use; Registry
// methods never touch the global data directory, only the configured one,
// so tests run against scratch directories.
type Registry struct {
	nodeID  protocol.NodeID
	dataDir string
	mu      sync.Mutex
	key     *nodeKey
}

// New returns the registry of the local node. Empty arguments are an
// error. It performs no I/O: the node key is created on first use.
func New(nodeID protocol.NodeID, dataDir string) (*Registry, error) {
	if nodeID == "" || dataDir == "" {
		return nil, fmt.Errorf("new registry: %w", ErrInvalidArgument)
	}
	return &Registry{nodeID: nodeID, dataDir: dataDir}, nil
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
	if err := validRequest(req); err != nil {
		return RegisterResult{}, err
	}
	process := int(req.Process.PID)
	if err := lineageForAgent(ctx, tx, process, req.Agent, req.Process); err != nil {
		return RegisterResult{}, err
	}
	rows, err := agentRows(ctx, tx, req.Agent)
	if err != nil {
		return RegisterResult{}, err
	}
	verified, err := r.verifiedRows(rows)
	if err != nil {
		return RegisterResult{}, err
	}
	if verified.revoked {
		return RegisterResult{}, fmt.Errorf("register %s: %w", req.Agent, ErrRevoked)
	}
	// A row already holding this key ID decides by key material: another
	// public key under the same version is inconsistent and refused, while
	// the same key is the idempotent path, including adoption after the
	// node key was lost (the row is re-signed as-is under the new key).
	for _, rec := range rows {
		if rec.keyID != req.KeyID {
			continue
		}
		if string(rec.publicKey) != string(req.PublicKey) {
			return RegisterResult{}, fmt.Errorf("register %s: key id %q binds another key: %w",
				req.Agent, req.KeyID, ErrInvalidArgument)
		}
		return r.adopt(ctx, tx, rec)
	}
	now := time.Now().UTC()
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

// adopt re-signs the existing row holding req's key ID under the current
// node key and reports the outcome its status carries: ACTIVE registers,
// SUSPENDED and REJECTED stay suspended, REVOKED stays terminal, and a
// ROTATED key is refused so rotation cannot be undone by re-registering.
func (r *Registry) adopt(ctx context.Context, tx Execer, rec keyRecord) (RegisterResult, error) {
	priv, err := r.nodePrivate()
	if err != nil {
		return RegisterResult{}, err
	}
	pub, err := r.nodePublic()
	if err != nil {
		return RegisterResult{}, err
	}
	if !verifyRecord(pub, rec, rec.signature) {
		if err := updateRow(ctx, tx, priv, rec); err != nil {
			return RegisterResult{}, err
		}
	}
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
// ACTIVE, unexpired, and with a valid record signature. Every other case
// is an error wrapping ErrNoUsableKey: none, a bad signature, expired,
// revoked, suspended, or ambiguously duplicated.
func (r *Registry) ActiveSigningKey(ctx context.Context, q Querier, agent protocol.AgentID) (KeyInfo, error) {
	if agent == "" {
		return KeyInfo{}, fmt.Errorf("active key: %w", ErrInvalidArgument)
	}
	rows, err := agentRows(ctx, q, agent)
	if err != nil {
		return KeyInfo{}, err
	}
	verified, err := r.verifiedRows(rows)
	if err != nil {
		return KeyInfo{}, err
	}
	active := verified.usable(time.Now().UTC())
	if active == nil {
		return KeyInfo{}, fmt.Errorf("agent %s: %w", agent, ErrNoUsableKey)
	}
	return keyInfo(*active), nil
}

// Blocked reports whether the agent is blocked: no usable ACTIVE key while
// a verified SUSPENDED or REVOKED key exists. An agent with no keys at
// all is not blocked, it simply has no key.
func (r *Registry) Blocked(ctx context.Context, q Querier, agent protocol.AgentID) (bool, error) {
	if agent == "" {
		return false, fmt.Errorf("blocked: %w", ErrInvalidArgument)
	}
	rows, err := agentRows(ctx, q, agent)
	if err != nil {
		return false, err
	}
	verified, err := r.verifiedRows(rows)
	if err != nil {
		return false, err
	}
	if verified.usable(time.Now().UTC()) != nil {
		return false, nil
	}
	return verified.hasSuspended() || verified.revoked, nil
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
	if agent == "" {
		return fmt.Errorf("review: %w", ErrInvalidArgument)
	}
	rows, err := agentRows(ctx, tx, agent)
	if err != nil {
		return err
	}
	verified, err := r.verifiedRows(rows)
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
	active    []keyRecord
	suspended []keyRecord
	rejected  []keyRecord
	revoked   bool
}

// usable returns the single usable ACTIVE record: verified, unexpired,
// and unambiguous. Zero or several is nil.
func (v verifiedSet) usable(now time.Time) *keyRecord {
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

// verifiedRows verifies every row against the node key and partitions the
// survivors by status. Rows that do not verify are dropped.
func (r *Registry) verifiedRows(rows []keyRecord) (verifiedSet, error) {
	pub, err := r.nodePublic()
	if err != nil {
		return verifiedSet{}, err
	}
	var set verifiedSet
	for _, rec := range rows {
		if !verifyRecord(pub, rec, rec.signature) {
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
		purpose:   keyPurposeSigning,
		algorithm: keyAlgorithmEd25519,
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

// validRequest checks the shape of a register request: a named agent, a
// valid process identity, a 32-byte Ed25519 key, and a key ID of the form
// ed25519:<agent>:v<n> naming that agent.
func validRequest(req RegisterRequest) error {
	if req.Agent == "" {
		return fmt.Errorf("register: empty agent: %w", ErrInvalidArgument)
	}
	if err := req.Process.Validate(); err != nil {
		return fmt.Errorf("register: process: %w", ErrInvalidArgument)
	}
	if len(req.PublicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("register: public key holds %d bytes: %w",
			len(req.PublicKey), ErrInvalidArgument)
	}
	agent, version, ok := strings.Cut(strings.TrimPrefix(req.KeyID, "ed25519:"), ":v")
	if !strings.HasPrefix(req.KeyID, "ed25519:") || !ok || agent != string(req.Agent) {
		return fmt.Errorf("register: key id %q is not ed25519:%s:v<n>: %w",
			req.KeyID, req.Agent, ErrInvalidArgument)
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
func scanRow(rows *sql.Rows) (keyRecord, error) {
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
	signature := signRecord(priv, rec)
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
	signature := signRecord(priv, rec)
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
