package vt

import (
	"fmt"
	"regexp"
)

// InputPattern is a compiled input-line pattern with exactly one capture group.
type InputPattern struct {
	expr *regexp.Regexp
}

// NewInputPattern compiles an input-line expression with one capture group.
func NewInputPattern(expr string) (*InputPattern, error) {
	compiled, err := regexp.Compile(expr)
	if err != nil || compiled.NumSubexp() != 1 {
		return nil, fmt.Errorf("%w: expected exactly one capture group", ErrInvalidPattern)
	}
	return &InputPattern{expr: compiled}, nil
}

// InputLine returns the captured text from the cursor row first, then from
// the remaining rows bottom-up. Pending stream bytes suppress every match.
func InputLine(s Snapshot, p *InputPattern) (string, bool) {
	if s.Pending || p == nil || p.expr == nil {
		return "", false
	}
	if s.CursorRow >= 0 && s.CursorRow < len(s.Lines) {
		if value, ok := inputMatch(s.Lines[s.CursorRow], p); ok {
			return value, true
		}
	}
	for row := len(s.Lines) - 1; row >= 0; row-- {
		if row == s.CursorRow {
			continue
		}
		if value, ok := inputMatch(s.Lines[row], p); ok {
			return value, true
		}
	}
	return "", false
}

func inputMatch(line string, p *InputPattern) (string, bool) {
	match := p.expr.FindStringSubmatch(line)
	if match == nil {
		return "", false
	}
	return match[1], true
}
