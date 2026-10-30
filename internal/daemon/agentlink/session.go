package agentlink

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

type sessionRecord struct {
	state       protocol.SessionState
	capture     protocol.CaptureMode
	deliverable bool
	ended       bool
	manifest    protocol.CapabilityManifest
	process     protocol.ProcessIdentity
}

// liveSession reads the exact Hub session the link will claim in its greeting
// response. Requiring every live/deliverable predicate here closes the race
// between the registry's reconciliation query and the socket handshake.
func liveSession(
	ctx context.Context,
	db *sql.DB,
	sessionID protocol.OpaqueID,
	agent protocol.AgentID,
) (sessionRecord, error) {
	session, err := loadSession(ctx, db, sessionID, agent)
	if err != nil {
		return sessionRecord{}, err
	}
	if session.state != protocol.SessionStateConnected ||
		session.capture != protocol.CaptureModePTYProxy || !session.deliverable || session.ended {
		return sessionRecord{}, ErrSessionUnavailable
	}
	return session, nil
}

// loadSession decodes the durable session identity without requiring it to
// remain live. The guard writer uses this after receiving a report so a Hub
// shutdown racing persistence cannot erase the last observation.
func loadSession(
	ctx context.Context,
	db *sql.DB,
	sessionID protocol.OpaqueID,
	agent protocol.AgentID,
) (sessionRecord, error) {
	var (
		gotAgent                    string
		state, capture, manifestRaw string
		deliverable                 bool
		processRaw                  sql.NullString
		endedAt                     sql.NullString
	)
	err := db.QueryRowContext(ctx, `SELECT agent_id, state, capture_mode, deliverable,
		manifest_json, process_json, ended_at FROM sessions WHERE session_id = ?`, sessionID).
		Scan(&gotAgent, &state, &capture, &deliverable, &manifestRaw, &processRaw, &endedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sessionRecord{}, fmt.Errorf("%w: session not found", ErrSessionUnavailable)
		}
		return sessionRecord{}, fmt.Errorf("select agent session: %w", err)
	}
	sessionState := protocol.SessionState(state)
	captureMode := protocol.CaptureMode(capture)
	if protocol.AgentID(gotAgent) != agent || !sessionState.Valid() || !captureMode.Valid() || !processRaw.Valid {
		return sessionRecord{}, ErrSessionUnavailable
	}
	var manifest protocol.CapabilityManifest
	if err := json.Unmarshal([]byte(manifestRaw), &manifest); err != nil {
		return sessionRecord{}, fmt.Errorf("%w: decode session manifest", ErrSessionUnavailable)
	}
	if err := manifest.Validate(); err != nil {
		return sessionRecord{}, fmt.Errorf("%w: manifest: %w", ErrSessionUnavailable, err)
	}
	var process protocol.ProcessIdentity
	if err := json.Unmarshal([]byte(processRaw.String), &process); err != nil {
		return sessionRecord{}, fmt.Errorf("%w: decode session process", ErrSessionUnavailable)
	}
	if err := process.Validate(); err != nil {
		return sessionRecord{}, fmt.Errorf("%w: process: %w", ErrSessionUnavailable, err)
	}
	if manifest.AgentID != agent || manifest.CaptureMode != protocol.CaptureModePTYProxy || !manifest.Deliverable {
		return sessionRecord{}, ErrSessionUnavailable
	}
	return sessionRecord{
		state: sessionState, capture: captureMode,
		deliverable: deliverable, ended: endedAt.Valid, manifest: manifest, process: process,
	}, nil
}
