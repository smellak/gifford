package eventstore

import (
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// Event store sentinels.
var (
	// ErrInvalidArgument means a caller-supplied argument breaks its rules.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrInvalidMigration means a migration file breaks the runner's format.
	ErrInvalidMigration = errors.New("invalid migration")
	// ErrMigrationMismatch means the database and the embedded migrations disagree.
	ErrMigrationMismatch = errors.New("migrations do not match the database")
	// ErrDuplicateEvent means an event_id or a (session_id, sequence) is already stored.
	ErrDuplicateEvent = errors.New("duplicate event")
)

// DuplicateEventError names the first envelope of a refused batch that is
// already stored; the whole batch was rolled back.
type DuplicateEventError struct {
	Index     int               // position in the batch
	EventID   protocol.ULID     // the duplicate event id
	SessionID protocol.OpaqueID // empty for a Gifford-scoped event
	Sequence  int64             // the duplicate sequence
}

// Error describes the duplicate.
func (e *DuplicateEventError) Error() string {
	return fmt.Sprintf("event %d (%s, session %q, sequence %d): duplicate event",
		e.Index, e.EventID, e.SessionID, e.Sequence)
}

// Unwrap returns ErrDuplicateEvent.
func (e *DuplicateEventError) Unwrap() error { return ErrDuplicateEvent }
