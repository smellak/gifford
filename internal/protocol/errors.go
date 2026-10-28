package protocol

import "errors"

// ErrInvalid means a value failed structural or cross-field validation.
// Every *ValidationError unwraps to it.
var ErrInvalid = errors.New("invalid protocol value")

// ErrUnsupportedVersion means a document declares a schema_version or wire
// version this binary does not understand.
var ErrUnsupportedVersion = errors.New("unsupported version")

// ValidationError describes one contract violation. Path uses JSON names
// ("active_loops[0].mode", "control_versions[global]",
// "payload.capture_session_id").
type ValidationError struct {
	Path   string
	Reason string
}

func (e *ValidationError) Error() string { return e.Path + ": " + e.Reason }

// Unwrap returns ErrInvalid so callers match any violation with errors.Is.
func (e *ValidationError) Unwrap() error { return ErrInvalid }
