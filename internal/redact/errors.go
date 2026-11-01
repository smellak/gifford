package redact

import "errors"

// ErrInvalidOptions means Options failed Validate: too many entries or one
// entry is empty, oversized, not UTF-8, holds a control rune, has
// surrounding spaces, is not absolute or home-relative, is a root, or is
// not clean. The error names the entry index, never the entry.
var ErrInvalidOptions = errors.New("invalid redactor options")

// ErrInvalidInput means the input failed structural validation before any
// rule ran: bad dimensions, invalid UTF-8 or a control rune where printable
// text is required. The error carries indexes, never input text.
var ErrInvalidInput = errors.New("invalid redactor input")

// ErrInputTooLarge means the input passed structural validation but exceeds
// MaxRows or MaxInputBytes. The error carries the observed size and the
// limit, never input text.
var ErrInputTooLarge = errors.New("redactor input too large")

// ErrUnsupportedInput means a masked span touches a cluster-forming rune
// the v2 width model cannot size. Failing closed exposes a caller bug or a
// genuinely unsupported screen, never a partly redacted one.
var ErrUnsupportedInput = errors.New("redactor input outside the v2 width model")

// ErrInternal means the redactor failed without the caller's help: a rule
// panicked, a span left its line, the rule set is missing, or an output
// row lost its width. The result is always the zero value.
var ErrInternal = errors.New("redactor internal failure")
