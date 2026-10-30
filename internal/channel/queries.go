package channel

import (
	"embed"
	"fmt"
)

// queryFiles embeds the control, decision and approval queries. The file
// system is read-only by construction.
//
//go:embed queries/*.sql
var queryFiles embed.FS

// queryText returns one embedded query by file name.
func queryText(name string) (string, error) {
	raw, err := queryFiles.ReadFile("queries/" + name)
	if err != nil {
		return "", fmt.Errorf("read query %s: %w", name, ErrInvalidQuery)
	}
	if len(raw) == 0 {
		return "", fmt.Errorf("query %s is empty: %w", name, ErrInvalidQuery)
	}
	return string(raw), nil
}
