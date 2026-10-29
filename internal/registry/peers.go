package registry

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

const localNodeStatus = "ACTIVE"

// PeerStatus is the local trust state of one remote node.
type PeerStatus string

// Peer trust states.
const (
	PeerStatusTrusted   PeerStatus = "TRUSTED"
	PeerStatusSuspended PeerStatus = "SUSPENDED"
	PeerStatusRevoked   PeerStatus = "REVOKED"
)

// AddPeerOutcome describes whether AddPeer created or retained a peer, or
// quarantined it after a key substitution attempt.
type AddPeerOutcome string

// AddPeer outcomes.
const (
	AddPeerRegistered AddPeerOutcome = "registered"
	AddPeerUnchanged  AddPeerOutcome = "unchanged"
	AddPeerSuspended  AddPeerOutcome = "suspended"
)

// AddPeerResult is the durable state produced by AddPeer.
type AddPeerResult struct {
	Node    protocol.NodeID
	Status  PeerStatus
	Outcome AddPeerOutcome
}

type nodeRecord struct {
	nodeID           protocol.NodeID
	organizationID   string
	local            bool
	status           string
	signingPublicKey ed25519.PublicKey
}

// EnsureLocalNode pins this registry's node signing key in the local node
// row. Reopening with the same key is idempotent; a missing or different
// key in an existing row is refused instead of silently replacing trust.
func (r *Registry) EnsureLocalNode(ctx context.Context, ex Execer) error {
	pub, err := r.nodePublic()
	if err != nil {
		return err
	}
	return withTx(ctx, ex, func(tx Execer) error {
		node, err := nodeByID(ctx, tx, r.nodeID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			return insertNode(ctx, tx, nodeRecord{
				nodeID: r.nodeID, local: true, status: localNodeStatus,
				signingPublicKey: append(ed25519.PublicKey(nil), pub...),
			}, r.now().Format(timeLayout))
		}
		if !node.local || node.status != localNodeStatus || !bytes.Equal(node.signingPublicKey, pub) {
			return fmt.Errorf("local node %s does not match its pinned signing key: %w",
				r.nodeID, ErrNodeKeyMismatch)
		}
		return nil
	})
}

// AddPeer pins a remote node's organization and signing key. Repeating the
// same key is a no-op. Presenting different key material never replaces the
// pin: the peer becomes SUSPENDED and the result reports the incident.
func (r *Registry) AddPeer(
	ctx context.Context,
	ex Execer,
	nodeID protocol.NodeID,
	organizationID string,
	publicKey ed25519.PublicKey,
) (AddPeerResult, error) {
	if !nodeID.Valid() || nodeID == r.nodeID || organizationID == "" || len(publicKey) != ed25519.PublicKeySize {
		return AddPeerResult{}, fmt.Errorf("add peer %q: %w", nodeID, ErrInvalidArgument)
	}
	var result AddPeerResult
	err := withTx(ctx, ex, func(tx Execer) error {
		node, err := nodeByID(ctx, tx, nodeID)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err := insertNode(ctx, tx, nodeRecord{
				nodeID: nodeID, organizationID: organizationID,
				status:           string(PeerStatusTrusted),
				signingPublicKey: append(ed25519.PublicKey(nil), publicKey...),
			}, r.now().Format(timeLayout)); err != nil {
				return err
			}
			result = AddPeerResult{Node: nodeID, Status: PeerStatusTrusted, Outcome: AddPeerRegistered}
			return nil
		}
		if node.local {
			return fmt.Errorf("add peer %s names a local node: %w", nodeID, ErrInvalidArgument)
		}
		status, err := parsePeerStatus(node.status)
		if err != nil {
			return err
		}
		if bytes.Equal(node.signingPublicKey, publicKey) {
			result = AddPeerResult{Node: nodeID, Status: status, Outcome: AddPeerUnchanged}
			return nil
		}
		if status == PeerStatusRevoked {
			return fmt.Errorf("add peer %s: %w", nodeID, ErrPeerRevoked)
		}
		if status != PeerStatusSuspended {
			if err := updatePeerStatus(ctx, tx, nodeID, PeerStatusSuspended); err != nil {
				return err
			}
		}
		result = AddPeerResult{Node: nodeID, Status: PeerStatusSuspended, Outcome: AddPeerSuspended}
		return nil
	})
	if err != nil {
		return AddPeerResult{}, err
	}
	return result, nil
}

// SetPeerStatus moves a peer to SUSPENDED or REVOKED. REVOKED is terminal,
// and this API deliberately has no transition back to TRUSTED.
func (r *Registry) SetPeerStatus(
	ctx context.Context,
	ex Execer,
	nodeID protocol.NodeID,
	status PeerStatus,
) error {
	if !nodeID.Valid() || nodeID == r.nodeID ||
		(status != PeerStatusSuspended && status != PeerStatusRevoked) {
		return fmt.Errorf("set peer status for %q to %q: %w", nodeID, status, ErrInvalidArgument)
	}
	return withTx(ctx, ex, func(tx Execer) error {
		node, err := nodeByID(ctx, tx, nodeID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("set peer status for %s: %w", nodeID, ErrPeerNotFound)
			}
			return err
		}
		if node.local {
			return fmt.Errorf("set peer status for local node %s: %w", nodeID, ErrInvalidArgument)
		}
		current, err := parsePeerStatus(node.status)
		if err != nil {
			return err
		}
		if current == PeerStatusRevoked {
			if status == PeerStatusRevoked {
				return nil
			}
			return fmt.Errorf("set peer status for %s: %w", nodeID, ErrPeerRevoked)
		}
		if current == status {
			return nil
		}
		return updatePeerStatus(ctx, tx, nodeID, status)
	})
}

func parsePeerStatus(raw string) (PeerStatus, error) {
	switch status := PeerStatus(raw); status {
	case PeerStatusTrusted, PeerStatusSuspended, PeerStatusRevoked:
		return status, nil
	default:
		return "", fmt.Errorf("peer holds status %q: %w", raw, ErrInvalidArgument)
	}
}

func nodeByID(ctx context.Context, q Querier, nodeID protocol.NodeID) (nodeRecord, error) {
	text, err := queryText("nodes_select_id.sql")
	if err != nil {
		return nodeRecord{}, err
	}
	var node nodeRecord
	var local int64
	var endpoint, fingerprint, lastSeen sql.NullString
	var policyVersion, createdAt string
	var publicKey []byte
	err = q.QueryRowContext(ctx, text, string(nodeID)).Scan(
		&node.nodeID, &node.organizationID, &local, &node.status, &endpoint,
		&fingerprint, &policyVersion, &lastSeen, &createdAt, &publicKey,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nodeRecord{}, sql.ErrNoRows
		}
		return nodeRecord{}, fmt.Errorf("select node %s: %w", nodeID, err)
	}
	if local != 0 && local != 1 {
		return nodeRecord{}, fmt.Errorf("node %s has local=%d: %w", nodeID, local, ErrInvalidArgument)
	}
	node.local = local == 1
	node.signingPublicKey = append(ed25519.PublicKey(nil), publicKey...)
	return node, nil
}

func insertNode(ctx context.Context, tx Execer, node nodeRecord, createdAt string) error {
	text, err := queryText("nodes_insert.sql")
	if err != nil {
		return err
	}
	local := 0
	if node.local {
		local = 1
	}
	_, err = tx.ExecContext(ctx, text,
		node.nodeID, node.organizationID, local, node.status,
		nil, nil, "", nil, createdAt, []byte(node.signingPublicKey),
	)
	if err != nil {
		return fmt.Errorf("insert node %s: %w", node.nodeID, err)
	}
	return nil
}

func updatePeerStatus(ctx context.Context, tx Execer, nodeID protocol.NodeID, status PeerStatus) error {
	text, err := queryText("nodes_update_status.sql")
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, text, status, nodeID)
	if err != nil {
		return fmt.Errorf("update peer %s: %w", nodeID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count update of peer %s: %w", nodeID, err)
	}
	if affected != 1 {
		return fmt.Errorf("update of peer %s touched %d rows: %w", nodeID, affected, ErrInvalidArgument)
	}
	return nil
}
