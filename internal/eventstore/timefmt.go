package eventstore

import (
	"fmt"
	"time"
)

// TimeLayout is how the store writes every time: RFC 3339 in UTC with
// exactly nine fractional digits, so text order is time order.
const TimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// FormatTime returns t in UTC formatted with TimeLayout.
func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeLayout)
}

// ParseTime parses a time written by FormatTime; any other text is an error.
func ParseTime(s string) (time.Time, error) {
	parsed, err := time.Parse(TimeLayout, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time %q: %w", s, err)
	}
	if FormatTime(parsed) != s {
		return time.Time{}, fmt.Errorf("invalid time %q: not in UTC with nine fractional digits", s)
	}
	return parsed, nil
}
