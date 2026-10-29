package hub

import "errors"

// errInvalidArgument means a caller-supplied argument breaks its rules.
var errInvalidArgument = errors.New("invalid argument")
