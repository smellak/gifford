package channel

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the database side of the control plane: reloading the
// effective-control cache from the store and encoding rows and decisions.
// Every statement lives in queries/*.sql; this file holds no SQL text.

// timeLayout is the store's time format: RFC 3339 in UTC with exactly nine
// fractional digits, so text order is time order. Non-test code in this
// package may not import the event store, so the string is copied here and
// a test requires it to stay equal to eventstore.TimeLayout.
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// Queryer is what Reload needs; *sql.DB, *sql.Tx and *sql.Conn satisfy it.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Reload replaces the cache with every channel_controls row. A query,
// scan, decode or validation error returns a wrapped error and leaves the
// cache not loaded, so evaluations answer CONTROL_UNAVAILABLE (fail
// closed).
func (e *Engine) Reload(ctx context.Context, q Queryer) error {
	selectAll, err := queryText("controls_select_all.sql")
	if err != nil {
		e.markNotLoaded()
		return err
	}
	rows, err := q.QueryContext(ctx, selectAll)
	if err != nil {
		e.markNotLoaded()
		return fmt.Errorf("select channel controls: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var found []protocol.ChannelControl
	for rows.Next() {
		root, err := scanControl(rows)
		if err != nil {
			e.markNotLoaded()
			return err
		}
		found = append(found, root)
	}
	if err := rows.Err(); err != nil {
		e.markNotLoaded()
		return fmt.Errorf("read channel controls: %w", err)
	}
	if err := e.load(found); err != nil {
		e.markNotLoaded()
		return err
	}
	return nil
}

// markNotLoaded drops the cache so evaluations answer CONTROL_UNAVAILABLE.
func (e *Engine) markNotLoaded() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries = make(map[cacheKey]cacheEntry)
	e.stale = make(map[cacheKey]int64)
	e.loaded = false
}

// scanControl reads one controls_select_all row. NULL and empty read back
// as zero values; any stored value the writer could never have produced
// is an error.
func scanControl(rows *sql.Rows) (protocol.ChannelControl, error) {
	var root protocol.ChannelControl
	var mode, fromAgent, toAgent, reason, thenMode sql.NullString
	var tx, rx, autoDelivery, autoContinue sql.NullInt64
	var expiresAt, restoreJSON sql.NullString
	var createdAt, updatedAt string
	if err := rows.Scan(
		&root.ControlID, &root.ScopeType, &root.ScopeKey, &root.Plane,
		&mode,
		&tx, &rx, &autoDelivery, &autoContinue,
		&fromAgent, &toAgent, &reason, &root.ActorType, &root.CreatedBy, &expiresAt,
		&restoreJSON, &thenMode, &root.Version, &createdAt, &updatedAt,
	); err != nil {
		return protocol.ChannelControl{}, fmt.Errorf("scan channel control: %w", err)
	}
	id := string(root.ControlID)
	root.Mode = protocol.ControlMode(mode.String)
	var err error
	if root.TxEnabled, err = scanFlag(id, "tx_enabled", tx); err != nil {
		return protocol.ChannelControl{}, err
	}
	if root.RxEnabled, err = scanFlag(id, "rx_enabled", rx); err != nil {
		return protocol.ChannelControl{}, err
	}
	if root.AutoDeliveryEnabled, err = scanFlag(id, "auto_delivery_enabled", autoDelivery); err != nil {
		return protocol.ChannelControl{}, err
	}
	if root.AutoContinueEnabled, err = scanFlag(id, "auto_continue_enabled", autoContinue); err != nil {
		return protocol.ChannelControl{}, err
	}
	root.FromAgent = protocol.AgentID(fromAgent.String)
	root.ToAgent = protocol.AgentID(toAgent.String)
	root.Reason = reason.String
	if expiresAt.Valid {
		if root.ExpiresAt, err = parseChannelTime(expiresAt.String); err != nil {
			return protocol.ChannelControl{}, fmt.Errorf("control %s expires_at: %w", id, err)
		}
	}
	if restoreJSON.Valid {
		var restore protocol.ControlSnapshot
		if err := decodeJSON(restoreJSON.String, &restore); err != nil {
			return protocol.ChannelControl{}, fmt.Errorf("control %s restore_json: %w", id, err)
		}
		root.Restore = &restore
	}
	root.ThenMode = protocol.ControlMode(thenMode.String)
	if root.CreatedAt, err = parseChannelTime(createdAt); err != nil {
		return protocol.ChannelControl{}, fmt.Errorf("control %s created_at: %w", id, err)
	}
	if root.UpdatedAt, err = parseChannelTime(updatedAt); err != nil {
		return protocol.ChannelControl{}, fmt.Errorf("control %s updated_at: %w", id, err)
	}
	return root, nil
}

// scanFlag reads a nullable 1/0 agent flag: NULL is nil, anything else
// than 0 or 1 is an error.
func scanFlag(id, column string, value sql.NullInt64) (*bool, error) {
	if !value.Valid {
		return nil, nil
	}
	switch value.Int64 {
	case 0:
		return flagValue(false), nil
	case 1:
		return flagValue(true), nil
	default:
		return nil, fmt.Errorf("control %s %s holds %d, want 0, 1 or NULL", id, column, value.Int64)
	}
}

// controlParams encodes one row into the 20 controls_insert.sql parameters
// in order. Empty optional strings and a zero expiry become NULL. It does
// not validate: validation happens in load when the row is reloaded.
func controlParams(root protocol.ChannelControl) ([]any, error) {
	restore, err := encodeRestore(root.Restore)
	if err != nil {
		return nil, err
	}
	return []any{
		string(root.ControlID),
		string(root.ScopeType),
		root.ScopeKey,
		string(root.Plane),
		nullString(string(root.Mode)),
		flagInt(root.TxEnabled),
		flagInt(root.RxEnabled),
		flagInt(root.AutoDeliveryEnabled),
		flagInt(root.AutoContinueEnabled),
		nullString(string(root.FromAgent)),
		nullString(string(root.ToAgent)),
		nullString(root.Reason),
		string(root.ActorType),
		root.CreatedBy,
		nullTime(root.ExpiresAt),
		restore,
		nullString(string(root.ThenMode)),
		root.Version,
		formatChannelTime(root.CreatedAt),
		formatChannelTime(root.UpdatedAt),
	}, nil
}

// decisionRecord returns the 15 decisions_insert.sql parameters in order,
// taken from d.Record(). JSON columns use encoding/json without
// indentation; the matched ids are [] when empty.
func decisionRecord(d Decision) ([]any, error) {
	rec := d.Record()
	matched := rec.MatchedControlIDs
	if matched == nil {
		matched = []protocol.ControlID{}
	}
	matchedJSON, err := json.Marshal(matched)
	if err != nil {
		return nil, fmt.Errorf("encode matched controls: %w", err)
	}
	effectiveJSON, err := json.Marshal(rec.EffectiveControl)
	if err != nil {
		return nil, fmt.Errorf("encode effective control: %w", err)
	}
	versionsJSON, err := json.Marshal(rec.ControlVersions)
	if err != nil {
		return nil, fmt.Errorf("encode control versions: %w", err)
	}
	explanationJSON, err := json.Marshal(rec.Explanation)
	if err != nil {
		return nil, fmt.Errorf("encode explanation: %w", err)
	}
	return []any{
		string(rec.DecisionID),
		string(rec.ObjectType),
		string(rec.ObjectID),
		string(rec.Stage),
		string(rec.Plane),
		nullString(string(rec.FromAgent)),
		nullString(string(rec.ToAgent)),
		string(rec.Outcome),
		string(rec.Reason),
		string(matchedJSON),
		string(effectiveJSON),
		string(versionsJSON),
		string(explanationJSON),
		nullString(rec.ActorID),
		formatChannelTime(rec.CreatedAt),
	}, nil
}

// nullString binds an empty string as NULL.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// flagInt stores a nil flag as NULL and a set flag as 1 or 0.
func flagInt(flag *bool) any {
	if flag == nil {
		return nil
	}
	if *flag {
		return 1
	}
	return 0
}

// flagValue returns a pointer to its argument.
func flagValue(value bool) *bool { return &value }

// nullTime stores a zero time as NULL.
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return formatChannelTime(t)
}

// formatChannelTime writes a time in the store's layout.
func formatChannelTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

// parseChannelTime reads a time written by formatChannelTime; any other
// text is an error.
func parseChannelTime(s string) (time.Time, error) {
	parsed, err := time.Parse(timeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q", s)
	}
	if formatChannelTime(parsed) != s {
		return time.Time{}, fmt.Errorf("invalid time %q: not UTC with nine fractional digits", s)
	}
	return parsed, nil
}

// decodeJSON decodes one JSON column with unknown fields rejected.
func decodeJSON(raw string, into any) error {
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("bad JSON: %w", err)
	}
	return nil
}

// encodeRestore encodes a nil snapshot as NULL.
func encodeRestore(restore *protocol.ControlSnapshot) (any, error) {
	if restore == nil {
		return nil, nil
	}
	raw, err := json.Marshal(restore)
	if err != nil {
		return nil, fmt.Errorf("encode restore snapshot: %w", err)
	}
	return string(raw), nil
}
