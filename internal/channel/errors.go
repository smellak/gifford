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
	// ErrControlUnavailable means the controls cannot be read.
	ErrControlUnavailable = errors.New("control unavailable")
	// ErrUnknownBudgetProfile means the profile is not an embedded file.
	ErrUnknownBudgetProfile = errors.New("unknown budget profile")
	// ErrInvalidBudget means a budget file breaks its rules.
	ErrInvalidBudget = errors.New("invalid budget")
)
