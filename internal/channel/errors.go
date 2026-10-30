package channel

import (
	"errors"

	"github.com/smellak/gifford/internal/protocol"
)

// Channel sentinels: every error the package returns wraps one of these
// and is compared with errors.Is.
var (
	// ErrInvalidOptions means Engine options are unusable.
	ErrInvalidOptions = errors.New("invalid options")
	// ErrInvalidControl means a control row breaks the scoped rules.
	ErrInvalidControl = errors.New("invalid control")
	// ErrInvalidQuery means an evaluation query breaks its rules.
	ErrInvalidQuery = errors.New("invalid query")
	// ErrDenied is wrapped by every DenyError.
	ErrDenied = errors.New("denied")
	// ErrUnsupported means the request needs loop support from 5.7.
	ErrUnsupported = errors.New("not supported yet")
	// ErrApprovalUnavailable means approval requests are not available yet.
	ErrApprovalUnavailable = errors.New("approval requests are not available yet")
	// ErrUnavailable means the node control epoch cannot be read.
	ErrUnavailable = errors.New("control epoch unavailable")
	// ErrStore wraps a failure of a gate's store callback.
	ErrStore = errors.New("store failed")
	// ErrControlUnavailable means the controls cannot be read.
	ErrControlUnavailable = errors.New("control unavailable")
	// ErrUnknownBudgetProfile means the profile is not an embedded file.
	ErrUnknownBudgetProfile = errors.New("unknown budget profile")
	// ErrInvalidBudget means a budget file breaks its rules.
	ErrInvalidBudget = errors.New("invalid budget")
	// ErrVersionConflict means a control compare-and-swap lost its race.
	ErrVersionConflict = errors.New("control version conflict")
	// ErrMutationForbidden means an authenticated agent process attempted a
	// human-only control mutation.
	ErrMutationForbidden = errors.New("control mutation forbidden")
	// ErrNotFound means the addressed control object does not exist.
	ErrNotFound = errors.New("control object not found")
	// ErrControlChanged means a delivery Decision failed immediate
	// revalidation and cannot authorize the pending action.
	ErrControlChanged = errors.New("control changed")
)

// RevalidationError is a typed CONTROL_CHANGED result. The caller commits the
// transaction so DecisionID remains as the audit record, then cancels the
// prepared delivery.
type RevalidationError struct {
	decisionID protocol.ChannelDecisionID
	cause      string
}

func (e *RevalidationError) Error() string { return ErrControlChanged.Error() + ": " + e.cause }

// Unwrap makes RevalidationError match ErrControlChanged.
func (e *RevalidationError) Unwrap() error { return ErrControlChanged }

// DecisionID returns the recorded revalidation decision id.
func (e *RevalidationError) DecisionID() protocol.ChannelDecisionID { return e.decisionID }

// Reason returns the typed reason stored on the revalidation decision.
func (e *RevalidationError) Reason() protocol.DecisionReason {
	return protocol.DecisionReasonControlChanged
}

// Cause returns a stable diagnostic class without extending the wire protocol.
func (e *RevalidationError) Cause() string { return e.cause }

// VersionConflictError reports the current versions observed by a failed
// compare-and-swap. CurrentVersion is used by scalar mutations and
// CurrentVersions by link batches.
type VersionConflictError struct {
	CurrentVersion  int64
	CurrentVersions map[string]int64
}

func (e *VersionConflictError) Error() string { return ErrVersionConflict.Error() }
func (e *VersionConflictError) Unwrap() error { return ErrVersionConflict }
