package capture

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

// formatWall renders a TEXT timestamp: UTC RFC 3339.
func formatWall(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// SessionRow mirrors capture_sessions. Empty strings read back from
// NULL columns (initial_screen_blob_ref, stopped_at, stop_reason)
// and write back as NULL.
type SessionRow struct {
	CaptureSessionID     string
	AgentID              string
	Harness              string
	TmuxServer           string
	TmuxSession          string
	TmuxWindow           string
	TmuxPane             string
	PaneCols             int64
	PaneRows             int64
	State                string
	InitialScreenBlobRef string
	StartedAt            string
	StoppedAt            string
	StopReason           string
	ReattachCount        int64
	BytesCaptured        int64
	SourceQuality        string
}

// sessionColumns lists every column in a fixed order.
const sessionColumns = `capture_session_id, agent_id, harness,
	tmux_server, tmux_session, tmux_window, tmux_pane,
	pane_cols, pane_rows, state, initial_screen_blob_ref,
	started_at, stopped_at, stop_reason,
	reattach_count, bytes_captured, source_quality`

// scanSession reads one session row, mapping NULLs to empty strings.
func scanSession(rows *sql.Rows) (SessionRow, error) {
	var r SessionRow
	var blobRef, stoppedAt, stopReason sql.NullString
	err := rows.Scan(
		&r.CaptureSessionID, &r.AgentID, &r.Harness,
		&r.TmuxServer, &r.TmuxSession, &r.TmuxWindow, &r.TmuxPane,
		&r.PaneCols, &r.PaneRows, &r.State, &blobRef,
		&r.StartedAt, &stoppedAt, &stopReason,
		&r.ReattachCount, &r.BytesCaptured, &r.SourceQuality,
	)
	if err != nil {
		return SessionRow{}, fmt.Errorf("scan capture session: %w", err)
	}
	r.InitialScreenBlobRef = blobRef.String
	r.StoppedAt = stoppedAt.String
	r.StopReason = stopReason.String
	return r, nil
}

// nullText writes an empty string as NULL.
func nullText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// SegmentRow mirrors capture_segments. Empty strings map to NULL
// (ended_at, sha256, redacted_export_sha256); a nil BytesCompressed
// maps to NULL (an uncompressed segment).
type SegmentRow struct {
	SegmentID            string
	CaptureSessionID     string
	SeqFrom              int64
	SeqTo                int64
	StartedAt            string
	EndedAt              string
	StoragePath          string
	IndexPath            string
	BytesRaw             int64
	BytesCompressed      *int64
	Compressed           bool
	SHA256               string
	RedactedExportSHA256 string
}

// segmentColumns lists every column in a fixed order.
const segmentColumns = `segment_id, capture_session_id,
	seq_from, seq_to, started_at, ended_at,
	storage_path, index_path, bytes_raw, bytes_compressed,
	compressed, sha256, redacted_export_sha256`

// scanSegment reads one segment row, mapping NULLs to zero values.
func scanSegment(rows *sql.Rows) (SegmentRow, error) {
	var r SegmentRow
	var endedAt, sha, redacted sql.NullString
	var bytesCompressed sql.NullInt64
	var compressed int64
	err := rows.Scan(
		&r.SegmentID, &r.CaptureSessionID,
		&r.SeqFrom, &r.SeqTo, &r.StartedAt, &endedAt,
		&r.StoragePath, &r.IndexPath, &r.BytesRaw, &bytesCompressed,
		&compressed, &sha, &redacted,
	)
	if err != nil {
		return SegmentRow{}, fmt.Errorf("scan capture segment: %w", err)
	}
	r.EndedAt = endedAt.String
	if bytesCompressed.Valid {
		v := bytesCompressed.Int64
		r.BytesCompressed = &v
	}
	r.Compressed = compressed != 0
	r.SHA256 = sha.String
	r.RedactedExportSHA256 = redacted.String
	return r, nil
}

// nullInt writes a nil pointer as NULL.
func nullInt(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

// boolInt writes a bool as 0 or 1.
func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// Store owns the capture tables' writers and queries. Events travel
// only through AppendEvents, never inside another transaction; the
// rows follow the events they summarise in their own short
// transactions. Every SQL keyword is uppercase.
type Store struct {
	inner *eventstore.Store
}

// OpenStore opens the store at path, creating the file and applying
// every migration, as a mutating command does.
func OpenStore(ctx context.Context, path string) (*Store, error) {
	inner, err := eventstore.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("open capture store: %w", err)
	}
	return &Store{inner: inner}, nil
}

// absStorePath resolves the store path to absolute, for the
// percent-encoded read-only DSN.
func absStorePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("%w: store path: %w", ErrUnsafePath, err)
	}
	return abs, nil
}

// Close closes the connection pool.
func (s *Store) Close() error {
	if err := s.inner.Close(); err != nil {
		return fmt.Errorf("close capture store: %w", err)
	}
	return nil
}

// DB returns the pool for the capture tables' short transactions.
func (s *Store) DB() *sql.DB {
	return s.inner.DB()
}

// AppendEvents appends one batch outside any other transaction.
func (s *Store) AppendEvents(ctx context.Context, events []protocol.EventEnvelope) error {
	if err := s.inner.AppendEvents(ctx, events); err != nil {
		return fmt.Errorf("append capture events: %w", err)
	}
	return nil
}

// SessionEvents reads one session's events in sequence order.
func (s *Store) SessionEvents(ctx context.Context, session string, from int64, limit int) ([]protocol.EventEnvelope, error) {
	events, err := s.inner.SessionEvents(ctx, protocol.OpaqueID(session), from, limit)
	if err != nil {
		return nil, fmt.Errorf("read capture events: %w", err)
	}
	return events, nil
}

// OpenStoreReadOnly opens an existing store file read-only for status
// and stop's first look. It never calls eventstore.Open, applies no
// migration and creates nothing: a missing store is "no captures".
// The sqlite driver registers through internal/eventstore.
func OpenStoreReadOnly(path string) (*sql.DB, error) {
	if strings.Contains(path, "?") {
		return nil, fmt.Errorf("%w: store path holds ?", ErrUnsafePath)
	}
	abs, err := absStorePath(path)
	if err != nil {
		return nil, err
	}
	dsn := (&url.URL{Scheme: "file", Path: abs}).String() +
		"?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open store read-only: %w", err)
	}
	return db, nil
}

// InsertSession inserts the attach row. The row exists before any event.
func (s *Store) InsertSession(ctx context.Context, r SessionRow) error {
	_, err := s.inner.DB().ExecContext(ctx,
		`INSERT INTO capture_sessions (`+sessionColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.CaptureSessionID, r.AgentID, r.Harness,
		r.TmuxServer, r.TmuxSession, r.TmuxWindow, r.TmuxPane,
		r.PaneCols, r.PaneRows, r.State, nullText(r.InitialScreenBlobRef),
		r.StartedAt, nullText(r.StoppedAt), nullText(r.StopReason),
		r.ReattachCount, r.BytesCaptured, r.SourceQuality,
	)
	if err != nil {
		return fmt.Errorf("insert capture session: %w", err)
	}
	return nil
}

// UpdateSession replaces the session's mutable columns.
func (s *Store) UpdateSession(ctx context.Context, r SessionRow) error {
	res, err := s.inner.DB().ExecContext(ctx,
		`UPDATE capture_sessions
		SET agent_id = ?, harness = ?,
			tmux_server = ?, tmux_session = ?, tmux_window = ?, tmux_pane = ?,
			pane_cols = ?, pane_rows = ?, state = ?, initial_screen_blob_ref = ?,
			started_at = ?, stopped_at = ?, stop_reason = ?,
			reattach_count = ?, bytes_captured = ?, source_quality = ?
		WHERE capture_session_id = ?`,
		r.AgentID, r.Harness,
		r.TmuxServer, r.TmuxSession, r.TmuxWindow, r.TmuxPane,
		r.PaneCols, r.PaneRows, r.State, nullText(r.InitialScreenBlobRef),
		r.StartedAt, nullText(r.StoppedAt), nullText(r.StopReason),
		r.ReattachCount, r.BytesCaptured, r.SourceQuality,
		r.CaptureSessionID,
	)
	if err != nil {
		return fmt.Errorf("update capture session: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update capture session: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("%w: session %s updates %d rows", ErrUnsafePath, r.CaptureSessionID, n)
	}
	return nil
}

// SessionRow reads one session row.
func (s *Store) SessionRow(ctx context.Context, id string) (SessionRow, bool, error) {
	rows, err := s.inner.DB().QueryContext(ctx,
		`SELECT `+sessionColumns+` FROM capture_sessions WHERE capture_session_id = ?`, id)
	if err != nil {
		return SessionRow{}, false, fmt.Errorf("select capture session: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return SessionRow{}, false, nil
	}
	r, err := scanSession(rows)
	if err != nil {
		return SessionRow{}, false, err
	}
	return r, true, nil
}

// SessionsByAgent reads one agent's sessions, oldest first, over the
// agent index.
func (s *Store) SessionsByAgent(ctx context.Context, agent string) ([]SessionRow, error) {
	return s.querySessions(ctx,
		`SELECT `+sessionColumns+` FROM capture_sessions WHERE agent_id = ? ORDER BY started_at ASC`, agent)
}

// AllSessions reads every session row, oldest first, for the claims
// evaluation over the store.
func (s *Store) AllSessions(ctx context.Context) ([]SessionRow, error) {
	return s.querySessions(ctx,
		`SELECT `+sessionColumns+` FROM capture_sessions ORDER BY started_at ASC`)
}

func (s *Store) querySessions(ctx context.Context, query string, args ...any) ([]SessionRow, error) {
	rows, err := s.inner.DB().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select capture sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SessionRow
	for rows.Next() {
		r, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read capture sessions: %w", err)
	}
	return out, nil
}

// InsertSegment inserts a segment row after the batch that commits
// its first chunk, with real seq_to and bytes values.
func (s *Store) InsertSegment(ctx context.Context, r SegmentRow) error {
	_, err := s.inner.DB().ExecContext(ctx,
		`INSERT INTO capture_segments (`+segmentColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.SegmentID, r.CaptureSessionID,
		r.SeqFrom, r.SeqTo, r.StartedAt, nullText(r.EndedAt),
		r.StoragePath, r.IndexPath, r.BytesRaw, nullInt(r.BytesCompressed),
		boolInt(r.Compressed), nullText(r.SHA256), nullText(r.RedactedExportSHA256),
	)
	if err != nil {
		return fmt.Errorf("insert capture segment: %w", err)
	}
	return nil
}

// UpdateSegment replaces a segment row by id.
func (s *Store) UpdateSegment(ctx context.Context, r SegmentRow) error {
	res, err := s.inner.DB().ExecContext(ctx,
		`UPDATE capture_segments
		SET capture_session_id = ?,
			seq_from = ?, seq_to = ?, started_at = ?, ended_at = ?,
			storage_path = ?, index_path = ?, bytes_raw = ?, bytes_compressed = ?,
			compressed = ?, sha256 = ?, redacted_export_sha256 = ?
		WHERE segment_id = ?`,
		r.CaptureSessionID,
		r.SeqFrom, r.SeqTo, r.StartedAt, nullText(r.EndedAt),
		r.StoragePath, r.IndexPath, r.BytesRaw, nullInt(r.BytesCompressed),
		boolInt(r.Compressed), nullText(r.SHA256), nullText(r.RedactedExportSHA256),
		r.SegmentID,
	)
	if err != nil {
		return fmt.Errorf("update capture segment: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update capture segment: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("%w: segment %s updates %d rows", ErrUnsafePath, r.SegmentID, n)
	}
	return nil
}

// OpenSegment reads the session's segment with ended_at NULL, if any:
// at most one row is ever open.
func (s *Store) OpenSegment(ctx context.Context, session string) (SegmentRow, bool, error) {
	rows, err := s.inner.DB().QueryContext(ctx,
		`SELECT `+segmentColumns+` FROM capture_segments
		WHERE capture_session_id = ? AND ended_at IS NULL`, session)
	if err != nil {
		return SegmentRow{}, false, fmt.Errorf("select open segment: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return SegmentRow{}, false, nil
	}
	r, err := scanSegment(rows)
	if err != nil {
		return SegmentRow{}, false, err
	}
	if rows.Next() {
		return SegmentRow{}, false, fmt.Errorf("%w: two open segments", ErrUnsafePath)
	}
	return r, true, nil
}

// SegmentsOf reads one session's segment rows in sequence order, over
// the session index.
func (s *Store) SegmentsOf(ctx context.Context, session string) ([]SegmentRow, error) {
	rows, err := s.inner.DB().QueryContext(ctx,
		`SELECT `+segmentColumns+` FROM capture_segments
		WHERE capture_session_id = ? ORDER BY seq_from ASC`, session)
	if err != nil {
		return nil, fmt.Errorf("select capture segments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []SegmentRow
	for rows.Next() {
		r, err := scanSegment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read capture segments: %w", err)
	}
	return out, nil
}
