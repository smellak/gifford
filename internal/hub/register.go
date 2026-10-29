package hub

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// RegisterEndpointParams binds one endpoint signing key to the process on
// the authenticated socket connection. Agent identity is never accepted in
// params; it comes from the connection's registered lineage.
type RegisterEndpointParams struct {
	Process   protocol.ProcessIdentity `json:"process"`
	PublicKey []byte                   `json:"public_key"`
	KeyID     string                   `json:"key_id"`
}

// RegisterEndpointResult reports whether the requested key is active. A
// substitution is durably suspended by registry and returns Active false.
type RegisterEndpointResult struct {
	KeyID   string                   `json:"key_id"`
	Outcome registry.RegisterOutcome `json:"outcome"`
	Active  bool                     `json:"active"`
}

// RegisterAgent validates the peer process, persists its CONNECTED session,
// and returns the identity that serveConn binds to this exact connection.
func (s *Service) RegisterAgent(
	ctx context.Context,
	peerPID int,
	raw json.RawMessage,
) (protocol.RegisterAgentResult, error) {
	var params protocol.RegisterAgent
	if err := decodeParams(raw, &params); err != nil {
		return protocol.RegisterAgentResult{}, invalidParams(err)
	}
	if err := params.Validate(); err != nil {
		return protocol.RegisterAgentResult{}, invalidParams(err)
	}
	if params.Process == nil {
		return protocol.RegisterAgentResult{}, invalidParams(fmt.Errorf("process is required: %w", errInvalidArgument))
	}
	if err := verifyPeerProcess(peerPID, *params.Process); err != nil {
		return protocol.RegisterAgentResult{}, invalidParams(err)
	}
	manifestJSON, err := json.Marshal(params.Manifest)
	if err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("encode agent manifest: %w", err)
	}
	processJSON, err := json.Marshal(params.Process)
	if err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("encode agent process: %w", err)
	}
	now := s.clock.Now()
	stamp := eventstore.FormatTime(now)
	ulid, err := mintULID(now)
	if err != nil {
		return protocol.RegisterAgentResult{}, err
	}
	result := protocol.RegisterAgentResult{
		AgentID: params.Manifest.AgentID, SessionID: protocol.OpaqueID("sess-" + string(ulid)),
	}
	if err := result.Validate(); err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("register agent result: %w", err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return protocol.RegisterAgentResult{}, err
	}
	defer rollback(tx)
	var live bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions
		WHERE agent_id = ? AND state = 'CONNECTED' AND ended_at IS NULL)`,
		params.Manifest.AgentID).Scan(&live); err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("select live agent session: %w", err)
	}
	if live {
		return protocol.RegisterAgentResult{}, invalidParams(fmt.Errorf("agent already has a live session: %w", errInvalidArgument))
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET node_id = excluded.node_id,
			harness = excluded.harness, updated_at = excluded.updated_at`,
		params.Manifest.AgentID, s.nodeID, params.Manifest.Harness, stamp, stamp); err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("upsert registered agent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, ended_at, updated_at)
		VALUES (?, ?, 'CONNECTED', ?, ?, ?, ?, ?, NULL, ?)`,
		result.SessionID, result.AgentID, params.Manifest.CaptureMode, params.Manifest.Deliverable,
		string(manifestJSON), string(processJSON), stamp, stamp); err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("insert registered session: %w", err)
	}
	if err := commit(tx); err != nil {
		return protocol.RegisterAgentResult{}, err
	}
	return result, nil
}

// EndSession disconnects the session created by one register_agent call.
func (s *Service) EndSession(ctx context.Context, sessionID protocol.OpaqueID) error {
	if !sessionID.Valid() {
		return fmt.Errorf("end session: %w", errInvalidArgument)
	}
	stamp := eventstore.FormatTime(s.clock.Now())
	_, err := s.db.ExecContext(ctx, `UPDATE sessions
		SET state = 'DISCONNECTED', ended_at = ?, updated_at = ?
		WHERE session_id = ? AND ended_at IS NULL`, stamp, stamp, sessionID)
	if err != nil {
		return fmt.Errorf("end registered session: %w", err)
	}
	return nil
}

// CloseStaleSessions fails closed on pty_proxy sessions left live by a
// previous Hub process. Listen calls it before accepting a new connection.
func (s *Service) CloseStaleSessions(ctx context.Context) error {
	stamp := eventstore.FormatTime(s.clock.Now())
	_, err := s.db.ExecContext(ctx, `UPDATE sessions
		SET state = 'DISCONNECTED', ended_at = ?, updated_at = ?
		WHERE capture_mode = 'pty_proxy' AND ended_at IS NULL`, stamp, stamp)
	if err != nil {
		return fmt.Errorf("close stale registered sessions: %w", err)
	}
	return nil
}

// RegisterEndpoint registers the bound agent's signing key after proving the
// request describes the process at the other end of this connection.
func (s *Service) RegisterEndpoint(
	ctx context.Context,
	agent protocol.AgentID,
	peerPID int,
	raw json.RawMessage,
) (RegisterEndpointResult, error) {
	var params RegisterEndpointParams
	if err := decodeParams(raw, &params); err != nil {
		return RegisterEndpointResult{}, invalidParams(err)
	}
	if params.Process.PID != int64(peerPID) || len(params.PublicKey) != ed25519.PublicKeySize || params.KeyID == "" {
		return RegisterEndpointResult{}, invalidParams(fmt.Errorf("endpoint registration: %w", errInvalidArgument))
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return RegisterEndpointResult{}, err
	}
	defer rollback(tx)
	registered, err := s.registry.Register(ctx, tx, registry.RegisterRequest{
		Agent: agent, Process: params.Process, PublicKey: ed25519.PublicKey(params.PublicKey), KeyID: params.KeyID,
	})
	if err != nil {
		if errors.Is(err, registry.ErrInvalidArgument) {
			return RegisterEndpointResult{}, invalidParams(err)
		}
		return RegisterEndpointResult{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	if err := commit(tx); err != nil {
		return RegisterEndpointResult{}, err
	}
	return RegisterEndpointResult{
		KeyID: registered.Key.KeyID, Outcome: registered.Outcome,
		Active: registered.Outcome == registry.OutcomeRegistered,
	}, nil
}
