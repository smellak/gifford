package capture

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/protocol"
)

// StatusDeps is status's injectable environment: a per-server runner
// for the liveness columns, and the data directory resolver.
type StatusDeps struct {
	RunnerFor func(sock string) tmuxRunner
	DataDir   func() string
}

// StatusLine is one status row: every live session, plus each agent's
// last terminal one, newest first. It carries no pane content, titles
// or session names.
type StatusLine struct {
	Agent        string
	Capture      string
	State        string
	Pane         string
	Bytes        int64
	LastActivity string
	Segments     int64
	Compressed   int64
	Rotations    int64
	Reattaches   int64
	Ingest       string
	Pipe         string
}

// Status is read-only (CAP-S06): it opens the store through the
// read-only DSN, reads files through the root without creating
// anything, probes locks without O_CREATE, and repairs nothing. A
// missing root or store gives no lines; a store without the capture
// tables is "store not initialised".
func Status(ctx context.Context, deps StatusDeps, opts StatusOptions) ([]StatusLine, error) {
	data := opts.Data
	if data == "" {
		data = deps.DataDir()
	}
	root, err := openCaptureRoot(data, false)
	if err != nil {
		return nil, nil
	}
	defer func() { _ = root.Close() }()
	db, err := openStatusStore(data)
	if errors.Is(err, errNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := readOnlyRows(ctx, db, opts.Agent)
	if err != nil {
		return nil, err
	}
	var out []StatusLine
	terminalSeen := map[string]bool{}
	for _, row := range rows {
		terminal, reason, incomplete, err := readOnlyLife(ctx, db, row.CaptureSessionID)
		if err != nil {
			return nil, err
		}
		if terminal {
			if terminalSeen[row.AgentID] {
				continue
			}
			terminalSeen[row.AgentID] = true
		} else if incomplete {
			continue
		}
		line, err := statusLine(ctx, deps, root, db, row, terminal, reason, incomplete)
		if err != nil {
			return nil, err
		}
		out = append(out, line)
	}
	return out, nil
}

// openStatusStore opens the read-only store: a missing file is no
// captures, while a file without the capture tables is an error.
func openStatusStore(data string) (*sql.DB, error) {
	if _, err := os.Stat(filepath.Join(data, "gifford.db")); err != nil {
		if os.IsNotExist(err) {
			return nil, errNotFound
		}
		return nil, fmt.Errorf("%w: store stat: %w", ErrUnsafePath, err)
	}
	return OpenStoreReadOnly(filepath.Join(data, "gifford.db"))
}

// errNotFound marks the header-only outcome: no root or no store.
var errNotFound = errors.New("no captures")

// readOnlyRows reads session rows newest first, optionally for one
// agent. A query failure means the store has no capture tables.
func readOnlyRows(ctx context.Context, db *sql.DB, agent string) ([]SessionRow, error) {
	if db == nil {
		return nil, nil
	}
	query := `SELECT ` + sessionColumns + ` FROM capture_sessions ORDER BY started_at DESC`
	var args []any
	if agent != "" {
		query = `SELECT ` + sessionColumns + ` FROM capture_sessions WHERE agent_id = ? ORDER BY started_at DESC`
		args = append(args, agent)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store not initialised: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SessionRow
	for rows.Next() {
		r, err := scanSession(rows)
		if err != nil {
			return nil, fmt.Errorf("read capture sessions: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read capture sessions: %w", err)
	}
	return out, nil
}

// readOnlyLife derives one session's lifecycle from the read-only
// events: terminal with its stop reason, incomplete, or live.
func readOnlyLife(ctx context.Context, db *sql.DB, session string) (terminal bool, reason string, incomplete bool, err error) {
	var payload string
	err = db.QueryRowContext(ctx,
		`SELECT payload_json FROM events WHERE session_id = ? AND event_type = ? ORDER BY sequence DESC LIMIT 1`,
		session, string(protocol.EventTypeCaptureStopped)).Scan(&payload)
	switch {
	case err == nil:
		var stopped struct {
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal([]byte(payload), &stopped); err != nil {
			return false, "", false, fmt.Errorf("stored stop: %w", err)
		}
		return true, stopped.Reason, false, nil
	case errors.Is(err, sql.ErrNoRows):
	default:
		return false, "", false, fmt.Errorf("read stop event: %w", err)
	}
	var one int
	err = db.QueryRowContext(ctx,
		`SELECT 1 FROM events WHERE session_id = ? AND sequence = 0 AND event_type = ? LIMIT 1`,
		session, string(protocol.EventTypeCaptureAttached)).Scan(&one)
	switch {
	case err == nil:
		return false, "", false, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, "", true, nil
	default:
		return false, "", false, fmt.Errorf("read attach event: %w", err)
	}
}

// statusLine builds one row: the derived state, byte and activity
// counters, segment figures, and the liveness columns. Missing
// ingest.lock means a dead ingest; an unreachable server means an
// unknown pipe.
func statusLine(ctx context.Context, deps StatusDeps, root *os.Root, db *sql.DB, row SessionRow, terminal bool, reason string, incomplete bool) (StatusLine, error) {
	line := StatusLine{
		Agent: row.AgentID, Capture: row.CaptureSessionID,
		Pane: row.TmuxPane, Bytes: row.BytesCaptured,
		LastActivity: "-", Reattaches: row.ReattachCount,
		Ingest: "dead", Pipe: "0",
	}
	switch {
	case incomplete:
		line.State = "INCOMPLETE"
	case terminal:
		line.State = StoppedRowState(protocol.CaptureStopReason(reason))
	default:
		line.State = row.State
	}
	if wall, err := lastChunkWall(ctx, db, row.CaptureSessionID); err != nil {
		return StatusLine{}, err
	} else if wall != "" {
		line.LastActivity = wall
	}
	segs, err := readOnlySegments(ctx, db, row.CaptureSessionID)
	if err != nil {
		return StatusLine{}, err
	}
	for _, seg := range segs {
		line.Segments++
		if seg.Compressed {
			line.Compressed++
		}
	}
	n, err := countEvents(ctx, db, row.CaptureSessionID, protocol.EventTypeCaptureRotated)
	if err != nil {
		return StatusLine{}, err
	}
	line.Rotations = n
	lockPath := row.AgentID + "/" + row.CaptureSessionID + "/ingest.lock"
	if live, err := lockLive(root, lockPath); err != nil {
		return StatusLine{}, err
	} else if live {
		line.Ingest = "alive"
	}
	pipe, err := panePipe(ctx, deps.RunnerFor(row.TmuxServer), row.TmuxPane)
	if err != nil {
		return StatusLine{}, err
	}
	line.Pipe = pipe
	return line, nil
}

// panePipe reports one session's pipe column: 1, 0, or ? when the
// server is unreachable. A gone pane carries no pipe.
func panePipe(ctx context.Context, runner tmuxRunner, target string) (string, error) {
	if _, err := runner.server(ctx); err != nil {
		return "?", nil //nolint:nilerr // An unreachable server is not an error: it reports ?.
	}
	pane, err := runner.pane(ctx, target)
	if errors.Is(err, ErrPaneNotFound) {
		return "0", nil
	}
	if err != nil {
		return "", err
	}
	if pane.pipe {
		return "1", nil
	}
	return "0", nil
}

// lastChunkWall returns the last capture.chunk wall time, or empty.
func lastChunkWall(ctx context.Context, db *sql.DB, session string) (string, error) {
	var wall string
	err := db.QueryRowContext(ctx,
		`SELECT wall_time FROM events WHERE session_id = ? AND event_type = ? ORDER BY sequence DESC LIMIT 1`,
		session, string(protocol.EventTypeCaptureChunk)).Scan(&wall)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read last chunk: %w", err)
	}
	return wall, nil
}

// countEvents counts one event type of a session.
func countEvents(ctx context.Context, db *sql.DB, session string, evType protocol.EventType) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM events WHERE session_id = ? AND event_type = ?`,
		session, string(evType)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count capture events: %w", err)
	}
	return n, nil
}

// readOnlySegments reads one session's segment rows in order.
func readOnlySegments(ctx context.Context, db *sql.DB, session string) ([]SegmentRow, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT `+segmentColumns+` FROM capture_segments WHERE capture_session_id = ? ORDER BY seq_from ASC`,
		session)
	if err != nil {
		return nil, fmt.Errorf("select capture segments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SegmentRow
	for rows.Next() {
		seg, err := scanSegment(rows)
		if err != nil {
			return nil, fmt.Errorf("read capture segments: %w", err)
		}
		out = append(out, seg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read capture segments: %w", err)
	}
	return out, nil
}
