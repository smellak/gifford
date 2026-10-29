package channel

import "errors"

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
)
