package tmuxattach

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const tmuxSessionPrefix = "tmux-"

// ErrSessionCollision means a deterministic tmux session id already names
// different durable data. The store never repairs such a row by overwriting
// it because that could transfer one agent's pane identity to another.
var ErrSessionCollision = errors.New("tmux attach: session collision")

// SessionSpec is the public-safe projection of one verified tmux instance.
// The P1 registry remains authoritative for owner, socket and pane identity;
// this spec creates only the Core session used by delivery and MCP auth.
type SessionSpec struct {
	InstanceID     protocol.ULID
	AgentID        protocol.AgentID
	Harness        protocol.Harness
	HarnessVersion string
	Process        protocol.ProcessIdentity
	BootID         string
}

// SessionStoreOptions configure durable tmux_attach sessions.
type SessionStoreOptions struct {
	DB     *sql.DB
	NodeID protocol.NodeID
	Clock  clock.Clock
}

// SessionStore owns only sessions whose capture_mode is tmux_attach.
type SessionStore struct {
	db     *sql.DB
	nodeID protocol.NodeID
	clock  clock.Clock
}

// NewSessionStore validates the node-local dependencies.
func NewSessionStore(opts SessionStoreOptions) (*SessionStore, error) {
	if opts.DB == nil || !opts.NodeID.Valid() || opts.Clock == nil {
		return nil, ErrInvalidOptions
	}
	return &SessionStore{db: opts.DB, nodeID: opts.NodeID, clock: opts.Clock}, nil
}

// SessionID deterministically maps one immutable P1 instance to its Core
// session. A changed pane/process identity gets a new InstanceID and therefore
// cannot silently resume the previous session.
func (s SessionSpec) SessionID() protocol.OpaqueID {
	return protocol.OpaqueID(tmuxSessionPrefix + string(s.InstanceID))
}

// Bind creates or reconnects the session for spec and disconnects an older
// live tmux_attach session of the same agent in the same transaction.
func (s *SessionStore) Bind(ctx context.Context, spec SessionSpec) (protocol.OpaqueID, error) {
	manifest, processJSON, bootID, err := encodeSessionSpec(spec)
	if ctx == nil || err != nil {
		return "", ErrInvalidOptions
	}
	sessionID := spec.SessionID()
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("encode tmux session manifest: %w", err)
	}
	now := s.clock.Now().UTC()
	stamp := eventstore.FormatTime(now)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin tmux session bind: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var foreignLive int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM sessions WHERE agent_id = ? AND ended_at IS NULL
			AND capture_mode <> ?
	)`, spec.AgentID, protocol.CaptureModeTmuxAttach).Scan(&foreignLive); err != nil {
		return "", fmt.Errorf("check tmux session transport collision: %w", err)
	}
	if foreignLive != 0 {
		return "", ErrSessionCollision
	}
	if err := closeOtherTmuxSessions(ctx, tx, spec.AgentID, sessionID, now); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(agent_id) DO UPDATE SET node_id = excluded.node_id,
			harness = excluded.harness, updated_at = excluded.updated_at`,
		spec.AgentID, s.nodeID, spec.Harness, stamp, stamp); err != nil {
		return "", fmt.Errorf("upsert tmux session agent: %w", err)
	}

	var existingAgent, state, capture, storedManifest, storedProcess, storedBoot, updatedAt string
	var deliverable bool
	var endedAt sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT agent_id, state, capture_mode, deliverable,
		manifest_json, process_json, COALESCE(boot_id, ''), ended_at, updated_at
		FROM sessions WHERE session_id = ?`, sessionID).Scan(
		&existingAgent, &state, &capture, &deliverable, &storedManifest, &storedProcess,
		&storedBoot, &endedAt, &updatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `INSERT INTO sessions
			(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
			 process_json, started_at, ended_at, updated_at, boot_id)
			VALUES (?, ?, 'CONNECTED', ?, 1, ?, ?, ?, NULL, ?, ?)`,
			sessionID, spec.AgentID, protocol.CaptureModeTmuxAttach,
			string(manifestJSON), string(processJSON), stamp, stamp, bootID)
		if err != nil {
			return "", fmt.Errorf("insert tmux session: %w", err)
		}
	case err != nil:
		return "", fmt.Errorf("read tmux session collision: %w", err)
	default:
		if protocol.AgentID(existingAgent) != spec.AgentID ||
			protocol.CaptureMode(capture) != protocol.CaptureModeTmuxAttach ||
			storedManifest != string(manifestJSON) || storedProcess != string(processJSON) ||
			storedBoot != bootID {
			return "", ErrSessionCollision
		}
		if state == string(protocol.SessionStateConnected) && deliverable && !endedAt.Valid {
			break
		}
		if state != string(protocol.SessionStateDisconnected) || deliverable || !endedAt.Valid {
			return "", ErrSessionCollision
		}
		next, stampErr := nextLifecycleStamp(now, updatedAt)
		if stampErr != nil {
			return "", stampErr
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET state = 'CONNECTED',
			deliverable = 1, ended_at = NULL, updated_at = ?
			WHERE session_id = ? AND capture_mode = ?`, next, sessionID,
			protocol.CaptureModeTmuxAttach); err != nil {
			return "", fmt.Errorf("reconnect tmux session: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit tmux session bind: %w", err)
	}
	return sessionID, nil
}

// Invalidate disconnects one exact tmux_attach session. It is idempotent and
// never changes a PTY Proxy or another agent's session.
func (s *SessionStore) Invalidate(
	ctx context.Context,
	agent protocol.AgentID,
	sessionID protocol.OpaqueID,
) error {
	if ctx == nil || !agent.Valid() || !sessionID.Valid() {
		return ErrInvalidOptions
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tmux session invalidation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state, updatedAt string
	var endedAt sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT state, ended_at, updated_at FROM sessions
		WHERE session_id = ? AND agent_id = ? AND capture_mode = ?`, sessionID, agent,
		protocol.CaptureModeTmuxAttach).Scan(&state, &endedAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit absent tmux invalidation: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("read tmux session invalidation: %w", err)
	}
	if state == string(protocol.SessionStateDisconnected) && endedAt.Valid {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit disconnected tmux invalidation: %w", err)
		}
		return nil
	}
	if state != string(protocol.SessionStateConnected) || endedAt.Valid {
		return ErrSessionCollision
	}
	stamp, err := nextLifecycleStamp(s.clock.Now().UTC(), updatedAt)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET state = 'DISCONNECTED',
		deliverable = 0, ended_at = COALESCE(ended_at, ?), updated_at = ?
		WHERE session_id = ? AND agent_id = ? AND capture_mode = ?`,
		stamp, stamp, sessionID, agent, protocol.CaptureModeTmuxAttach); err != nil {
		return fmt.Errorf("invalidate tmux session: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tmux session invalidation: %w", err)
	}
	return nil
}

func encodeSessionSpec(spec SessionSpec) (protocol.CapabilityManifest, []byte, string, error) {
	if !spec.InstanceID.Valid() || !spec.AgentID.Valid() || !spec.Harness.Valid() ||
		!validSessionText(spec.HarnessVersion, 64) || !validSessionText(spec.BootID, 128) ||
		!spec.SessionID().Valid() || spec.Process.Validate() != nil {
		return protocol.CapabilityManifest{}, nil, "", ErrInvalidOptions
	}
	manifest := protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            spec.AgentID,
		Harness:            spec.Harness,
		HarnessVersion:     spec.HarnessVersion,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderTmuxAttach,
		CaptureMode:        protocol.CaptureModeTmuxAttach,
		Deliverable:        true,
		Capabilities: protocol.Capabilities{
			PTYCapture: true,
			MCP:        false,
		},
	}
	if err := manifest.Validate(); err != nil {
		return protocol.CapabilityManifest{}, nil, "", ErrInvalidOptions
	}
	processJSON, err := json.Marshal(spec.Process)
	if err != nil {
		return protocol.CapabilityManifest{}, nil, "", ErrInvalidOptions
	}
	return manifest, processJSON, spec.BootID, nil
}

// DisconnectOtherBoots disconnects every live tmux_attach session recorded
// under a kernel boot other than bootID, or under none. A reboot ends every
// pane, so such a session can never be delivered to again; left CONNECTED it
// would keep its queued rows eligible. It returns the sessions disconnected
// and never changes a PTY Proxy session.
func (s *SessionStore) DisconnectOtherBoots(ctx context.Context, bootID string) (int, error) {
	if ctx == nil || !validSessionText(bootID, 128) {
		return 0, ErrInvalidOptions
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin stale boot tmux sessions: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT session_id, updated_at FROM sessions
		WHERE capture_mode = ? AND ended_at IS NULL
			AND (boot_id IS NULL OR boot_id <> ?)`,
		protocol.CaptureModeTmuxAttach, bootID)
	if err != nil {
		return 0, fmt.Errorf("select stale boot tmux sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type staleSession struct {
		id        protocol.OpaqueID
		updatedAt string
	}
	var stale []staleSession
	for rows.Next() {
		var session staleSession
		if err := rows.Scan(&session.id, &session.updatedAt); err != nil {
			return 0, fmt.Errorf("scan stale boot tmux session: %w", err)
		}
		stale = append(stale, session)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read stale boot tmux sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close stale boot tmux sessions: %w", err)
	}
	now := s.clock.Now().UTC()
	for _, session := range stale {
		stamp, err := nextLifecycleStamp(now, session.updatedAt)
		if err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET state = 'DISCONNECTED',
			deliverable = 0, ended_at = ?, updated_at = ?
			WHERE session_id = ? AND capture_mode = ? AND ended_at IS NULL`,
			stamp, stamp, session.id, protocol.CaptureModeTmuxAttach); err != nil {
			return 0, fmt.Errorf("disconnect stale boot tmux session: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit stale boot tmux sessions: %w", err)
	}
	return len(stale), nil
}

func closeOtherTmuxSessions(
	ctx context.Context,
	tx *sql.Tx,
	agent protocol.AgentID,
	keep protocol.OpaqueID,
	now time.Time,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT session_id, state, deliverable, updated_at FROM sessions
		WHERE agent_id = ? AND capture_mode = ? AND session_id <> ?
			AND ended_at IS NULL`, agent, protocol.CaptureModeTmuxAttach, keep)
	if err != nil {
		return fmt.Errorf("select replaced tmux sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type replacedSession struct {
		id        protocol.OpaqueID
		state     string
		deliver   bool
		updatedAt string
	}
	var replaced []replacedSession
	for rows.Next() {
		var session replacedSession
		if err := rows.Scan(&session.id, &session.state, &session.deliver, &session.updatedAt); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan replaced tmux session: %w", err)
		}
		if session.state != string(protocol.SessionStateConnected) || !session.deliver {
			_ = rows.Close()
			return ErrSessionCollision
		}
		replaced = append(replaced, session)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read replaced tmux sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close replaced tmux sessions: %w", err)
	}
	for _, session := range replaced {
		stamp, err := nextLifecycleStamp(now, session.updatedAt)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE sessions SET state = 'DISCONNECTED',
			deliverable = 0, ended_at = ?, updated_at = ?
			WHERE session_id = ? AND agent_id = ? AND capture_mode = ?
				AND ended_at IS NULL`, stamp, stamp, session.id, agent,
			protocol.CaptureModeTmuxAttach); err != nil {
			return fmt.Errorf("disconnect replaced tmux session: %w", err)
		}
	}
	return nil
}

func nextLifecycleStamp(now time.Time, previous string) (string, error) {
	prior, err := eventstore.ParseTime(previous)
	if err != nil {
		return "", fmt.Errorf("parse tmux session lifecycle: %w", err)
	}
	if !now.After(prior) {
		now = prior.Add(time.Nanosecond)
	}
	return eventstore.FormatTime(now), nil
}

func validSessionText(value string, maxRunes int) bool {
	length := utf8.RuneCountInString(value)
	return length > 0 && length <= maxRunes && strings.IndexFunc(value, unicode.IsControl) < 0
}
