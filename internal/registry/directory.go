package registry

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// DirectoryRecord is one signed agent_keys row exchanged between nodes.
// Its fields are the stored values verbatim; RecordSignature authenticates
// every other field through the package's canonical record encoding.
type DirectoryRecord struct {
	KeyID           string
	Agent           protocol.AgentID
	Node            protocol.NodeID
	Purpose         string
	Algorithm       string
	PublicKey       []byte
	Status          string
	ValidFrom       string
	ValidUntil      *string
	Issuer          string
	RecordSignature []byte
}

// ImportResult reports durable directory changes and any agent that was
// left without authority because two ACTIVE records competed.
type ImportResult struct {
	Inserted        int
	Updated         int
	Unchanged       int
	SuspendedAgents []protocol.AgentID
	PeerSuspended   bool
}

// ExportDirectory returns every locally issued agent key row, including
// retired and rejected history. Corrupt local rows are not exported.
func (r *Registry) ExportDirectory(ctx context.Context, q Querier) ([]DirectoryRecord, error) {
	text, err := queryText("agent_keys_select_node.sql")
	if err != nil {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, text, r.nodeID)
	if err != nil {
		return nil, fmt.Errorf("select local key directory: %w", err)
	}
	defer func() { _ = rows.Close() }()
	pub, err := r.nodePublic()
	if err != nil {
		return nil, err
	}
	var records []DirectoryRecord
	for rows.Next() {
		rec, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		if rec.nodeID != string(r.nodeID) || rec.issuer != "node:"+string(r.nodeID) ||
			!verifyRecord(pub, rec, rec.signature) {
			return nil, fmt.Errorf("local key %q does not verify: %w", rec.keyID, ErrNoUsableKey)
		}
		records = append(records, exportRecord(rec))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read local key directory: %w", err)
	}
	return records, nil
}

// ImportDirectory verifies and stores one peer's complete or partial key
// directory atomically. Validation errors write nothing. A conflicting
// record under an existing key id quarantines the peer without replacing
// the pinned row; competing ACTIVE keys remain signed as received but are
// treated as an effective agent suspension by all registry reads.
func (r *Registry) ImportDirectory(
	ctx context.Context,
	ex Execer,
	peer protocol.NodeID,
	records []DirectoryRecord,
) (ImportResult, error) {
	if !peer.Valid() || peer == r.nodeID {
		return ImportResult{}, fmt.Errorf("import directory from %q: %w", peer, ErrInvalidArgument)
	}
	var result ImportResult
	err := withTx(ctx, ex, func(tx Execer) error {
		var err error
		result, err = r.importDirectory(ctx, tx, peer, records)
		return err
	})
	if err != nil {
		return ImportResult{}, err
	}
	return result, nil
}

type importOperation struct {
	record keyRecord
	insert bool
}

func (r *Registry) importDirectory(
	ctx context.Context,
	tx Execer,
	peer protocol.NodeID,
	records []DirectoryRecord,
) (ImportResult, error) {
	node, err := nodeByID(ctx, tx, peer)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ImportResult{}, fmt.Errorf("import directory from %s: %w", peer, ErrPeerNotFound)
		}
		return ImportResult{}, err
	}
	if node.local {
		return ImportResult{}, fmt.Errorf("import directory from local node %s: %w", peer, ErrInvalidArgument)
	}
	peerStatus, err := parsePeerStatus(node.status)
	if err != nil {
		return ImportResult{}, err
	}
	if peerStatus != PeerStatusTrusted {
		return ImportResult{}, fmt.Errorf("import directory from %s in state %s: %w",
			peer, peerStatus, ErrPeerNotTrusted)
	}
	if len(node.signingPublicKey) != ed25519.PublicKeySize {
		return ImportResult{}, fmt.Errorf("peer %s has an invalid signing key: %w", peer, ErrPeerNotTrusted)
	}

	incoming := make(map[string]keyRecord, len(records))
	agents := make(map[protocol.AgentID]struct{})
	for _, wire := range records {
		rec, err := importRecord(peer, wire)
		if err != nil {
			return ImportResult{}, err
		}
		if !verifyRecord(node.signingPublicKey, rec, rec.signature) {
			return ImportResult{}, fmt.Errorf("peer %s key %q has a bad signature: %w",
				peer, rec.keyID, ErrInvalidArgument)
		}
		if prior, ok := incoming[rec.keyID]; ok {
			if !sameStoredRecord(prior, rec) {
				return ImportResult{}, fmt.Errorf("peer %s repeats key id %q differently: %w",
					peer, rec.keyID, ErrInvalidArgument)
			}
			continue
		}
		incoming[rec.keyID] = rec
		agents[protocol.AgentID(rec.agentID)] = struct{}{}
	}

	existingByAgent := make(map[protocol.AgentID][]keyRecord, len(agents))
	for agent := range agents {
		if err := requireAgentOwner(ctx, tx, agent, peer); err != nil {
			return ImportResult{}, err
		}
		rows, err := agentRows(ctx, tx, agent)
		if err != nil {
			return ImportResult{}, err
		}
		for _, rec := range rows {
			if rec.nodeID != string(peer) {
				return ImportResult{}, fmt.Errorf("agent %s is already claimed by node %s: %w",
					agent, rec.nodeID, ErrAgentConflict)
			}
			if rec.issuer != "node:"+string(peer) ||
				!verifyRecord(node.signingPublicKey, rec, rec.signature) {
				return ImportResult{}, fmt.Errorf("stored peer key %q does not verify: %w",
					rec.keyID, ErrNoUsableKey)
			}
		}
		existingByAgent[agent] = rows
	}

	var collisions []protocol.AgentID
	for _, rec := range incoming {
		stored, found, err := keyByID(ctx, tx, rec.keyID)
		if err != nil {
			return ImportResult{}, err
		}
		if !found {
			continue
		}
		if stored.agentID != rec.agentID || stored.nodeID != rec.nodeID {
			return ImportResult{}, fmt.Errorf("key id %q belongs to %s on %s: %w",
				rec.keyID, stored.agentID, stored.nodeID, ErrAgentConflict)
		}
		if !sameRecordIdentity(stored, rec) {
			collisions = append(collisions, protocol.AgentID(rec.agentID))
		}
	}
	if len(collisions) > 0 {
		if err := updatePeerStatus(ctx, tx, peer, PeerStatusSuspended); err != nil {
			return ImportResult{}, err
		}
		return ImportResult{
			SuspendedAgents: uniqueAgents(collisions),
			PeerSuspended:   true,
		}, nil
	}

	operations := make([]importOperation, 0, len(incoming))
	prospective := make(map[protocol.AgentID]map[string]keyStatus, len(agents))
	for agent, rows := range existingByAgent {
		states := make(map[string]keyStatus, len(rows))
		for _, rec := range rows {
			states[rec.keyID] = rec.status
		}
		prospective[agent] = states
	}
	var result ImportResult
	for _, rec := range incoming {
		agent := protocol.AgentID(rec.agentID)
		stored, found, err := keyByID(ctx, tx, rec.keyID)
		if err != nil {
			return ImportResult{}, err
		}
		if !found {
			operations = append(operations, importOperation{record: rec, insert: true})
			prospective[agent][rec.keyID] = rec.status
			continue
		}
		if stored.status == rec.status {
			result.Unchanged++
			continue
		}
		if advancesStatus(stored.status, rec.status) {
			operations = append(operations, importOperation{record: rec})
			prospective[agent][rec.keyID] = rec.status
			continue
		}
		result.Unchanged++
	}

	for _, operation := range operations {
		if operation.insert {
			if err := insertImportedRow(ctx, tx, operation.record); err != nil {
				return ImportResult{}, err
			}
			result.Inserted++
			continue
		}
		if err := replaceImportedRow(ctx, tx, operation.record); err != nil {
			return ImportResult{}, err
		}
		result.Updated++
	}
	for agent, states := range prospective {
		active := 0
		for _, status := range states {
			if status == statusActive {
				active++
			}
		}
		if active > 1 {
			result.SuspendedAgents = append(result.SuspendedAgents, agent)
		}
	}
	result.SuspendedAgents = uniqueAgents(result.SuspendedAgents)
	return result, nil
}

func importRecord(peer protocol.NodeID, wire DirectoryRecord) (keyRecord, error) {
	if !wire.Agent.Valid() || wire.Node != peer || wire.Issuer != "node:"+string(peer) ||
		wire.Purpose != keyPurposeSigning || wire.Algorithm != keyAlgorithmEd25519 ||
		len(wire.PublicKey) != ed25519.PublicKeySize || !validKeyID(wire.Agent, wire.KeyID) {
		return keyRecord{}, fmt.Errorf("peer %s has invalid key record %q: %w",
			peer, wire.KeyID, ErrInvalidArgument)
	}
	status, err := parseStatus(wire.Status)
	if err != nil {
		return keyRecord{}, err
	}
	if _, err := time.Parse(timeLayout, wire.ValidFrom); err != nil {
		return keyRecord{}, fmt.Errorf("key %q has invalid valid_from: %w", wire.KeyID, ErrInvalidArgument)
	}
	if wire.ValidUntil != nil {
		if _, err := time.Parse(timeLayout, *wire.ValidUntil); err != nil {
			return keyRecord{}, fmt.Errorf("key %q has invalid valid_until: %w", wire.KeyID, ErrInvalidArgument)
		}
	}
	return keyRecord{
		keyID: wire.KeyID, agentID: string(wire.Agent), nodeID: string(wire.Node),
		purpose: wire.Purpose, algorithm: wire.Algorithm,
		publicKey: append([]byte(nil), wire.PublicKey...), status: status,
		validFrom: wire.ValidFrom, validUntil: cloneString(wire.ValidUntil), issuer: wire.Issuer,
		signature: append([]byte(nil), wire.RecordSignature...),
	}, nil
}

func exportRecord(rec keyRecord) DirectoryRecord {
	return DirectoryRecord{
		KeyID: rec.keyID, Agent: protocol.AgentID(rec.agentID), Node: protocol.NodeID(rec.nodeID),
		Purpose: rec.purpose, Algorithm: rec.algorithm,
		PublicKey: append([]byte(nil), rec.publicKey...), Status: string(rec.status),
		ValidFrom: rec.validFrom, ValidUntil: cloneString(rec.validUntil), Issuer: rec.issuer,
		RecordSignature: append([]byte(nil), rec.signature...),
	}
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func validKeyID(agent protocol.AgentID, keyID string) bool {
	prefix := "ed25519:" + string(agent) + ":v"
	version := strings.TrimPrefix(keyID, prefix)
	if version == "" || prefix+version != keyID {
		return false
	}
	for _, digit := range version {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func sameRecordIdentity(left, right keyRecord) bool {
	return left.keyID == right.keyID && left.agentID == right.agentID &&
		left.nodeID == right.nodeID && left.purpose == right.purpose &&
		left.algorithm == right.algorithm && bytes.Equal(left.publicKey, right.publicKey) &&
		left.validFrom == right.validFrom && equalString(left.validUntil, right.validUntil) &&
		left.issuer == right.issuer
}

func sameStoredRecord(left, right keyRecord) bool {
	return sameRecordIdentity(left, right) && left.status == right.status &&
		bytes.Equal(left.signature, right.signature)
}

func equalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func advancesStatus(old, next keyStatus) bool {
	switch old {
	case statusActive:
		return next == statusSuspended || next == statusRotated || next == statusRevoked
	case statusRejected:
		return next == statusActive || next == statusSuspended || next == statusRevoked
	case statusSuspended:
		return next == statusRotated || next == statusRevoked
	case statusRotated, statusRevoked:
		return false
	default:
		return false
	}
}

func requireAgentOwner(
	ctx context.Context,
	q Querier,
	agent protocol.AgentID,
	peer protocol.NodeID,
) error {
	text, err := queryText("agents_select_node.sql")
	if err != nil {
		return err
	}
	var owner protocol.NodeID
	err = q.QueryRowContext(ctx, text, agent).Scan(&owner)
	if err == nil && owner != peer {
		return fmt.Errorf("agent %s belongs to node %s: %w", agent, owner, ErrAgentConflict)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("select owner of agent %s: %w", agent, err)
	}
	return nil
}

func keyByID(ctx context.Context, q Querier, keyID string) (keyRecord, bool, error) {
	text, err := queryText("agent_keys_select_key.sql")
	if err != nil {
		return keyRecord{}, false, err
	}
	rec, err := scanRow(q.QueryRowContext(ctx, text, keyID))
	if errors.Is(err, sql.ErrNoRows) {
		return keyRecord{}, false, nil
	}
	if err != nil {
		return keyRecord{}, false, err
	}
	return rec, true, nil
}

func insertImportedRow(ctx context.Context, tx Execer, rec keyRecord) error {
	text, err := queryText("agent_keys_insert.sql")
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
		rec.issuer, rec.signature,
	)
	if err != nil {
		return fmt.Errorf("insert imported key %q: %w", rec.keyID, err)
	}
	return nil
}

func replaceImportedRow(ctx context.Context, tx Execer, rec keyRecord) error {
	text, err := queryText("agent_keys_import_replace.sql")
	if err != nil {
		return err
	}
	var validUntil any
	if rec.validUntil != nil {
		validUntil = *rec.validUntil
	}
	result, err := tx.ExecContext(ctx, text,
		rec.agentID, rec.nodeID, rec.purpose, rec.algorithm, rec.publicKey,
		string(rec.status), rec.validFrom, validUntil, rec.issuer, rec.signature, rec.keyID,
	)
	if err != nil {
		return fmt.Errorf("replace imported key %q: %w", rec.keyID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count replacement of imported key %q: %w", rec.keyID, err)
	}
	if affected != 1 {
		return fmt.Errorf("replacement of imported key %q touched %d rows: %w",
			rec.keyID, affected, ErrInvalidArgument)
	}
	return nil
}

func uniqueAgents(agents []protocol.AgentID) []protocol.AgentID {
	seen := make(map[protocol.AgentID]struct{}, len(agents))
	unique := make([]protocol.AgentID, 0, len(agents))
	for _, agent := range agents {
		if _, ok := seen[agent]; ok {
			continue
		}
		seen[agent] = struct{}{}
		unique = append(unique, agent)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	return unique
}

// RouteKind is the registry's current route classification for an agent.
type RouteKind string

// Route classifications.
const (
	RouteLocal       RouteKind = "LOCAL"
	RouteFederated   RouteKind = "FEDERATED"
	RoutePeerBlocked RouteKind = "PEER_BLOCKED"
	RouteNotFound    RouteKind = "ROUTE_NOT_FOUND"
)

// RouteResult names the route and, for a federated or blocked route, its
// remote node.
type RouteResult struct {
	Kind RouteKind
	Node protocol.NodeID
}

// Route resolves an agent without performing any network operation.
func (r *Registry) Route(ctx context.Context, q Querier, agent protocol.AgentID) (RouteResult, error) {
	if !agent.Valid() {
		return RouteResult{}, fmt.Errorf("route agent %q: %w", agent, ErrInvalidArgument)
	}
	owner, found, err := storedAgentOwner(ctx, q, agent)
	if err != nil {
		return RouteResult{}, err
	}
	if found && owner == r.nodeID {
		return RouteResult{Kind: RouteLocal, Node: r.nodeID}, nil
	}
	rows, err := agentRows(ctx, q, agent)
	if err != nil {
		return RouteResult{}, err
	}
	if len(rows) == 0 {
		return RouteResult{Kind: RouteNotFound}, nil
	}
	nodeID := protocol.NodeID(rows[0].nodeID)
	for _, rec := range rows[1:] {
		if protocol.NodeID(rec.nodeID) != nodeID {
			return RouteResult{Kind: RouteNotFound}, nil
		}
	}
	if nodeID == r.nodeID {
		return RouteResult{Kind: RouteLocal, Node: r.nodeID}, nil
	}
	node, err := nodeByID(ctx, q, nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return RouteResult{Kind: RouteNotFound}, nil
	}
	if err != nil {
		return RouteResult{}, err
	}
	status, err := parsePeerStatus(node.status)
	if err != nil {
		return RouteResult{}, err
	}
	if status != PeerStatusTrusted {
		return RouteResult{Kind: RoutePeerBlocked, Node: nodeID}, nil
	}
	if _, err := r.ActiveSigningKey(ctx, q, agent); err != nil {
		if errors.Is(err, ErrNoUsableKey) {
			return RouteResult{Kind: RouteNotFound}, nil
		}
		return RouteResult{}, err
	}
	return RouteResult{Kind: RouteFederated, Node: nodeID}, nil
}

func storedAgentOwner(ctx context.Context, q Querier, agent protocol.AgentID) (protocol.NodeID, bool, error) {
	text, err := queryText("agents_select_node.sql")
	if err != nil {
		return "", false, err
	}
	var owner protocol.NodeID
	err = q.QueryRowContext(ctx, text, agent).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("select owner of agent %s: %w", agent, err)
	}
	return owner, true, nil
}
