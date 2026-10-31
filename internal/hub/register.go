package hub

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

type storedRegistrationSession struct {
	id        protocol.OpaqueID
	agent     protocol.AgentID
	state     protocol.SessionState
	manifest  protocol.CapabilityManifest
	process   protocol.ProcessIdentity
	bootID    sql.NullString
	endedAt   sql.NullString
	updatedAt string

	manifestValid bool
	processValid  bool
}

// RegisterEndpointParams binds one endpoint signing key to the process on
// the authenticated socket connection. Agent identity is never accepted in
// params; it comes from the connection's registered lineage.
type RegisterEndpointParams struct {
	Process             protocol.ProcessIdentity `json:"process"`
	PublicKey           []byte                   `json:"public_key"`
	KeyID               string                   `json:"key_id"`
	EncryptionPublicKey []byte                   `json:"encryption_public_key,omitempty"`
	EncryptionKeyID     string                   `json:"encryption_key_id,omitempty"`
	PublishOnly         bool                     `json:"publish_only,omitempty"`
}

// RegisterEndpointResult reports whether the requested key is active. A
// substitution is durably suspended by registry and returns Active false.
type RegisterEndpointResult struct {
	KeyID             string                   `json:"key_id"`
	Outcome           registry.RegisterOutcome `json:"outcome"`
	Active            bool                     `json:"active"`
	EncryptionKeyID   string                   `json:"encryption_key_id,omitempty"`
	EncryptionOutcome registry.RegisterOutcome `json:"encryption_outcome,omitempty"`
	EncryptionActive  bool                     `json:"encryption_active,omitempty"`
}

// RegisterAgent validates the peer process, persists its CONNECTED session,
// and returns the identity that serveConn binds to this exact connection.
func (s *Service) RegisterAgent(
	ctx context.Context,
	peer peerIdentity,
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
	if err := verifyPeerProcess(peer, *params.Process); err != nil {
		return protocol.RegisterAgentResult{}, invalidParams(err)
	}
	bootID, err := s.sessions.bootID()
	if err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("read registration boot identity: %w", err)
	}
	if err := validateBootID(bootID); err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("validate registration boot identity: %w", err)
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
	stored, err := registrationSessions(ctx, tx)
	if err != nil {
		return protocol.RegisterAgentResult{}, err
	}
	var reusable *storedRegistrationSession
	var staleDetached []protocol.OpaqueID
	for index := range stored {
		row := &stored[index]
		sameAgent := row.agent == params.Manifest.AgentID
		sameProcess := row.processValid && row.process == *params.Process
		resumableIdentity := sameAgent && sameProcess && row.manifestValid &&
			row.bootID.Valid && row.bootID.String == bootID && !row.endedAt.Valid &&
			(row.state == protocol.SessionStateConnected || row.state == protocol.SessionStateDetached)
		manifestMatches := resumableIdentity && row.manifest == params.Manifest
		if resumableIdentity && !manifestMatches && s.capabilityLineage != nil {
			manifestMatches, err = s.capabilityLineage.MatchesCapabilitiesChanged(
				ctx, tx, row.id, params.Manifest, row.manifest,
			)
			if err != nil {
				return protocol.RegisterAgentResult{}, fmt.Errorf("verify capability resume lineage: %w", err)
			}
		}
		if manifestMatches {
			if reusable != nil {
				return protocol.RegisterAgentResult{}, invalidParams(fmt.Errorf(
					"process owns multiple resumable sessions: %w", errInvalidArgument))
			}
			reusable = row
			continue
		}
		// A process identity that already reached any non-resumable session
		// can never acquire a fresh session or inherit a prior binding.
		if sameProcess {
			return protocol.RegisterAgentResult{}, invalidParams(fmt.Errorf(
				"process session cannot be resumed: %w", errInvalidArgument))
		}
		if !sameAgent || row.endedAt.Valid {
			continue
		}
		switch row.state {
		case protocol.SessionStateDetached:
			if row.processValid && row.bootID.Valid &&
				s.sessions.alive(row.process, row.bootID.String) == nil {
				return protocol.RegisterAgentResult{}, invalidParams(fmt.Errorf(
					"agent has a live detached session: %w", errInvalidArgument))
			}
			staleDetached = append(staleDetached, row.id)
		default:
			return protocol.RegisterAgentResult{}, invalidParams(fmt.Errorf(
				"agent already has an open session: %w", errInvalidArgument))
		}
	}
	for _, sessionID := range staleDetached {
		if _, err := tx.ExecContext(ctx, `UPDATE sessions
			SET state = ?, ended_at = ?, updated_at = ?
			WHERE session_id = ? AND state = ? AND ended_at IS NULL`,
			protocol.SessionStateDisconnected, stamp, stamp, sessionID,
			protocol.SessionStateDetached); err != nil {
			return protocol.RegisterAgentResult{}, fmt.Errorf("disconnect stale detached session: %w", err)
		}
	}
	if reusable != nil {
		return resumeRegistrationSession(ctx, tx, reusable, stamp)
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
		 process_json, started_at, ended_at, updated_at, boot_id)
		VALUES (?, ?, 'CONNECTED', ?, ?, ?, ?, ?, NULL, ?, ?)`,
		result.SessionID, result.AgentID, params.Manifest.CaptureMode, params.Manifest.Deliverable,
		string(manifestJSON), string(processJSON), stamp, stamp, bootID); err != nil {
		return protocol.RegisterAgentResult{}, fmt.Errorf("insert registered session: %w", err)
	}
	if err := commit(tx); err != nil {
		return protocol.RegisterAgentResult{}, err
	}
	return result, nil
}

func registrationSessions(ctx context.Context, tx *sql.Tx) ([]storedRegistrationSession, error) {
	rows, err := tx.QueryContext(ctx, `SELECT session_id, agent_id, state,
		manifest_json, process_json, boot_id, ended_at, updated_at FROM sessions
		ORDER BY started_at, session_id`)
	if err != nil {
		return nil, fmt.Errorf("select registration sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var stored []storedRegistrationSession
	for rows.Next() {
		var row storedRegistrationSession
		var manifestJSON string
		var processJSON sql.NullString
		if err := rows.Scan(&row.id, &row.agent, &row.state, &manifestJSON,
			&processJSON, &row.bootID, &row.endedAt, &row.updatedAt); err != nil {
			return nil, fmt.Errorf("scan registration session: %w", err)
		}
		row.manifestValid = decodeParams(json.RawMessage(manifestJSON), &row.manifest) == nil &&
			row.manifest.Validate() == nil
		row.processValid = processJSON.Valid &&
			decodeParams(json.RawMessage(processJSON.String), &row.process) == nil &&
			row.process.Validate() == nil
		stored = append(stored, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read registration sessions: %w", err)
	}
	return stored, nil
}

func resumeRegistrationSession(
	ctx context.Context,
	tx *sql.Tx,
	stored *storedRegistrationSession,
	stamp string,
) (protocol.RegisterAgentResult, error) {
	if stored.state == protocol.SessionStateDetached {
		nextStamp, err := nextSessionUpdate(stamp, stored.updatedAt)
		if err != nil {
			return protocol.RegisterAgentResult{}, err
		}
		if err := connectDetachedSession(ctx, tx, stored.id, nextStamp); err != nil {
			return protocol.RegisterAgentResult{}, err
		}
	}
	if err := commit(tx); err != nil {
		return protocol.RegisterAgentResult{}, err
	}
	return protocol.RegisterAgentResult{
		AgentID: stored.agent, SessionID: stored.id,
	}, nil
}

func connectDetachedSession(
	ctx context.Context,
	tx *sql.Tx,
	sessionID protocol.OpaqueID,
	stamp string,
) error {
	changed, err := tx.ExecContext(ctx, `UPDATE sessions
		SET state = ?, updated_at = ?
		WHERE session_id = ? AND state = ? AND ended_at IS NULL`,
		protocol.SessionStateConnected, stamp, sessionID, protocol.SessionStateDetached)
	if err != nil {
		return fmt.Errorf("resume detached session: %w", err)
	}
	rows, err := changed.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify resumed session: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("resume detached session: %w", errInvalidArgument)
	}
	return nil
}

// nextSessionUpdate keeps updated_at a durable lifecycle revision even when
// a test clock is fixed or the wall clock moves backwards.
func nextSessionUpdate(candidate, previous string) (string, error) {
	now, err := eventstore.ParseTime(candidate)
	if err != nil {
		return "", fmt.Errorf("parse session update time: %w", err)
	}
	prior, err := eventstore.ParseTime(previous)
	if err != nil {
		return "", fmt.Errorf("parse previous session update time: %w", err)
	}
	if !now.After(prior) {
		now = prior.Add(time.Nanosecond)
	}
	return eventstore.FormatTime(now), nil
}

// EndSession releases the transport owner of one register_agent call. A live
// PTY Proxy becomes DETACHED and resumable; every unverifiable process and
// every other capture mode ends as DISCONNECTED.
func (s *Service) EndSession(ctx context.Context, sessionID protocol.OpaqueID) error {
	if !sessionID.Valid() {
		return fmt.Errorf("end session: %w", errInvalidArgument)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var state protocol.SessionState
	var capture protocol.CaptureMode
	var processJSON, bootID, endedAt sql.NullString
	var updatedAt string
	err = tx.QueryRowContext(ctx, `SELECT state, capture_mode, process_json,
		boot_id, ended_at, updated_at FROM sessions WHERE session_id = ?`, sessionID).Scan(
		&state, &capture, &processJSON, &bootID, &endedAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("select ending session: %w", err)
	}
	if endedAt.Valid || state != protocol.SessionStateConnected {
		return nil
	}
	stamp, err := nextSessionUpdate(eventstore.FormatTime(s.clock.Now().UTC()), updatedAt)
	if err != nil {
		return err
	}
	next := protocol.SessionStateDisconnected
	var ended any = stamp
	if capture == protocol.CaptureModePTYProxy {
		var process protocol.ProcessIdentity
		if processJSON.Valid && bootID.Valid &&
			json.Unmarshal([]byte(processJSON.String), &process) == nil &&
			s.sessions.alive(process, bootID.String) == nil {
			next = protocol.SessionStateDetached
			ended = nil
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions
		SET state = ?, ended_at = ?, updated_at = ?
		WHERE session_id = ? AND state = ? AND ended_at IS NULL`,
		next, ended, stamp, sessionID, protocol.SessionStateConnected); err != nil {
		return fmt.Errorf("end registered session: %w", err)
	}
	return commit(tx)
}

// CloseStaleSessions reconciles pty_proxy sessions left open by a prior Hub.
// Exact live identities become DETACHED; missing, changed, legacy or
// unverifiable identities become terminal before Listen accepts connections.
func (s *Service) CloseStaleSessions(ctx context.Context) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	rows, err := tx.QueryContext(ctx, `SELECT session_id, state, process_json, boot_id, updated_at
		FROM sessions WHERE capture_mode = ? AND ended_at IS NULL`, protocol.CaptureModePTYProxy)
	if err != nil {
		return fmt.Errorf("select stale registered sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type staleSession struct {
		id        protocol.OpaqueID
		state     protocol.SessionState
		process   sql.NullString
		bootID    sql.NullString
		updatedAt string
	}
	var sessions []staleSession
	for rows.Next() {
		var session staleSession
		if err := rows.Scan(&session.id, &session.state, &session.process,
			&session.bootID, &session.updatedAt); err != nil {
			return fmt.Errorf("scan stale registered session: %w", err)
		}
		sessions = append(sessions, session)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read stale registered sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close stale registered sessions: %w", err)
	}
	candidate := eventstore.FormatTime(s.clock.Now().UTC())
	for _, session := range sessions {
		stamp, err := nextSessionUpdate(candidate, session.updatedAt)
		if err != nil {
			return err
		}
		next := protocol.SessionStateDisconnected
		var ended any = stamp
		var process protocol.ProcessIdentity
		if (session.state == protocol.SessionStateConnected || session.state == protocol.SessionStateDetached) &&
			session.process.Valid && session.bootID.Valid &&
			json.Unmarshal([]byte(session.process.String), &process) == nil &&
			s.sessions.alive(process, session.bootID.String) == nil {
			next = protocol.SessionStateDetached
			ended = nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions
			SET state = ?, ended_at = ?, updated_at = ?
			WHERE session_id = ? AND ended_at IS NULL`, next, ended, stamp, session.id); err != nil {
			return fmt.Errorf("reconcile stale registered session: %w", err)
		}
	}
	return commit(tx)
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
	if params.Process.PID != int64(peerPID) || len(params.PublicKey) != ed25519.PublicKeySize || params.KeyID == "" ||
		((len(params.EncryptionPublicKey) == 0) != (params.EncryptionKeyID == "")) ||
		(len(params.EncryptionPublicKey) != 0 && len(params.EncryptionPublicKey) != 32) {
		return RegisterEndpointResult{}, invalidParams(fmt.Errorf("endpoint registration: %w", errInvalidArgument))
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return RegisterEndpointResult{}, err
	}
	defer rollback(tx)
	registered, err := s.registry.Register(ctx, tx, registry.RegisterRequest{
		Agent: agent, Process: params.Process, PublicKey: ed25519.PublicKey(params.PublicKey), KeyID: params.KeyID,
		Purpose: registry.KeyPurposeSigning, Algorithm: registry.KeyAlgorithmEd25519,
	})
	if err != nil {
		if errors.Is(err, registry.ErrInvalidArgument) {
			return RegisterEndpointResult{}, invalidParams(err)
		}
		return RegisterEndpointResult{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	var encrypted registry.RegisterResult
	_, supportsEncryption := s.registry.(artifactEncryptionRegistry)
	if len(params.EncryptionPublicKey) != 0 && supportsEncryption {
		encrypted, err = s.registry.Register(ctx, tx, registry.RegisterRequest{
			Agent: agent, Process: params.Process, PublicKey: ed25519.PublicKey(params.EncryptionPublicKey),
			KeyID: params.EncryptionKeyID, Purpose: registry.KeyPurposeEncryption,
			Algorithm: registry.KeyAlgorithmX25519,
		})
		if err != nil {
			if errors.Is(err, registry.ErrInvalidArgument) {
				return RegisterEndpointResult{}, invalidParams(err)
			}
			return RegisterEndpointResult{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
		}
	}
	if err := commit(tx); err != nil {
		return RegisterEndpointResult{}, err
	}
	result := RegisterEndpointResult{
		KeyID: registered.Key.KeyID, Outcome: registered.Outcome,
		Active: registered.Outcome == registry.OutcomeRegistered,
	}
	if len(params.EncryptionPublicKey) != 0 && supportsEncryption {
		result.EncryptionKeyID = encrypted.Key.KeyID
		result.EncryptionOutcome = encrypted.Outcome
		result.EncryptionActive = encrypted.Outcome == registry.OutcomeRegistered
	}
	return result, nil
}
