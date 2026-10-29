package profiles

import "errors"

var (
	// ErrUnknownHarness means the harness is not an embedded profile file.
	ErrUnknownHarness = errors.New("unknown harness")
	// ErrInvalidProfile means a profile file breaks its rules.
	ErrInvalidProfile = errors.New("invalid profile")
)
