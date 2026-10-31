package protocol

import (
	"fmt"
	"math"
	"time"
	"unicode/utf8"
)

// StateObservationScreen says whether the adapter could prove that the screen
// excludes the current human draft. AVAILABLE never means raw terminal input:
// the adapter has already removed the human-input region before framing it.
// WITHHELD carries no screen bytes.
type StateObservationScreen string

const (
	// StateObservationScreenAvailable carries draft-excluded screen rows.
	StateObservationScreenAvailable StateObservationScreen = "AVAILABLE"
	// StateObservationScreenWithheld carries no screen data.
	StateObservationScreenWithheld StateObservationScreen = "WITHHELD"
)

// EnumValues returns the screen states in contract order.
func (StateObservationScreen) EnumValues() []string {
	return []string{string(StateObservationScreenAvailable), string(StateObservationScreenWithheld)}
}

// Valid reports whether v is a published screen state.
func (v StateObservationScreen) Valid() bool {
	return v == StateObservationScreenAvailable || v == StateObservationScreenWithheld
}

// StateObservationCursor is the terminal cursor captured with ScreenRows.
type StateObservationCursor struct {
	Row int64 `json:"row" jsonschema:"minimum=0,maximum=9999"`
	Col int64 `json:"col" jsonschema:"minimum=0,maximum=9999"`
}

// Validate checks the cursor bounds.
func (c StateObservationCursor) Validate() error { return checkRoot(&c) }

// StateObservationActivity is the monotonic output activity ending at
// ObservedAt. The byte windows are nested.
type StateObservationActivity struct {
	BytesLast500ms         int64   `json:"bytes_last_500ms" jsonschema:"minimum=0"`
	BytesLast2s            int64   `json:"bytes_last_2s" jsonschema:"minimum=0"`
	BytesLast10s           int64   `json:"bytes_last_10s" jsonschema:"minimum=0"`
	SecondsSinceLastOutput float64 `json:"seconds_since_last_output" jsonschema:"minimum=0,maximum=31536000"`
}

// Validate checks the nested byte windows and finite idle duration.
func (a StateObservationActivity) Validate() error {
	if err := checkRoot(&a); err != nil {
		return err
	}
	if math.IsNaN(a.SecondsSinceLastOutput) || math.IsInf(a.SecondsSinceLastOutput, 0) {
		return &ValidationError{Path: "seconds_since_last_output", Reason: "must be finite"}
	}
	if a.BytesLast500ms > a.BytesLast2s || a.BytesLast2s > a.BytesLast10s {
		return &ValidationError{Path: "bytes_last_500ms", Reason: "activity windows must be nested"}
	}
	return nil
}

// StateObservationEvent references one already-durable native event. Payloads
// are deliberately absent: the report carries only identity, type and time.
type StateObservationEvent struct {
	EventID ULID      `json:"event_id"`
	Type    EventType `json:"type"`
	At      time.Time `json:"at"`
}

// Validate checks the event reference.
func (e StateObservationEvent) Validate() error { return checkRoot(&e) }

const (
	maxObservationRows       = 10000
	maxObservationScreenByte = 512 << 10
	maxObservationEvents     = 256
	pathObservationRows      = "screen_rows"
)

// StateObservationReport is the contracts-v1.3 Proxy-to-Core input to the
// StateProducer. It is an observation, never a decision. ObservationID is
// stable across replay; replaying it cannot renew freshness because expiry is
// derived only from ObservedAt.
type StateObservationReport struct {
	AgentID               AgentID                  `json:"agent_id"`
	SessionID             OpaqueID                 `json:"session_id"`
	ObservationID         OpaqueID                 `json:"observation_id"`
	ObservedAt            time.Time                `json:"observed_at"`
	Harness               Harness                  `json:"harness"`
	HarnessVersion        string                   `json:"harness_version" jsonschema:"minLength=1,maxLength=64"`
	ObservationGeneration int64                    `json:"observation_generation" jsonschema:"minimum=1"`
	OutputGeneration      int64                    `json:"output_generation" jsonschema:"minimum=0"`
	EventGeneration       int64                    `json:"event_generation" jsonschema:"minimum=0"`
	InputGuardVersion     int64                    `json:"input_guard_version" jsonschema:"minimum=0"`
	ScreenState           StateObservationScreen   `json:"screen_state"`
	ScreenRows            []string                 `json:"screen_rows" jsonschema:"maxItems=10000"`
	Cols                  int64                    `json:"cols" jsonschema:"minimum=0,maximum=10000"`
	Cursor                *StateObservationCursor  `json:"cursor,omitempty"`
	Activity              StateObservationActivity `json:"activity"`
	ExitCode              *int64                   `json:"exit_code,omitempty" jsonschema:"minimum=-2147483648,maximum=2147483647"`
	NativeEvents          []StateObservationEvent  `json:"native_events" jsonschema:"maxItems=256"`
}

// Validate checks boundedness, coherence and the no-human-draft framing rule.
func (r StateObservationReport) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.ObservedAt.IsZero() {
		return &ValidationError{Path: "observed_at", Reason: reasonRequired}
	}
	switch r.ScreenState {
	case StateObservationScreenAvailable:
		if r.Cols < 1 {
			return &ValidationError{Path: "cols", Reason: "must be positive when screen is available"}
		}
		if r.Cursor == nil {
			return &ValidationError{Path: "cursor", Reason: "required when screen is available"}
		}
		if len(r.ScreenRows) == 0 || r.Cursor.Row >= int64(len(r.ScreenRows)) {
			return &ValidationError{Path: "cursor.row", Reason: "must locate a captured row"}
		}
		if r.Cursor.Col >= r.Cols {
			return &ValidationError{Path: "cursor.col", Reason: "must locate a captured column"}
		}
	case StateObservationScreenWithheld:
		if len(r.ScreenRows) != 0 || r.Cols != 0 || r.Cursor != nil {
			return &ValidationError{Path: pathObservationRows, Reason: "withheld screen carries no screen data"}
		}
	}
	if len(r.ScreenRows) > maxObservationRows || len(r.NativeEvents) > maxObservationEvents {
		return &ValidationError{Path: pathObservationRows, Reason: "observation exceeds a collection bound"}
	}
	total := 0
	for i, row := range r.ScreenRows {
		if !utf8.ValidString(row) {
			return &ValidationError{Path: fmt.Sprintf("screen_rows[%d]", i), Reason: "must be UTF-8"}
		}
		total += len(row)
		if total > maxObservationScreenByte {
			return &ValidationError{Path: pathObservationRows, Reason: "screen exceeds byte bound"}
		}
	}
	seen := make(map[ULID]bool, len(r.NativeEvents))
	for i, event := range r.NativeEvents {
		if event.At.After(r.ObservedAt) {
			return &ValidationError{Path: fmt.Sprintf("native_events[%d].at", i), Reason: "after observed_at"}
		}
		if seen[event.EventID] {
			return &ValidationError{Path: fmt.Sprintf("native_events[%d].event_id", i), Reason: "duplicate"}
		}
		seen[event.EventID] = true
	}
	return nil
}
