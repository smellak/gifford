package hub

import (
	"embed"
	"fmt"
)

// queryFiles embeds the Hub queries; every statement the accept path runs
// lives in one of them. None writes a control column: the triples stay
// with internal/channel.
//
//go:embed queries/*.sql
var queryFiles embed.FS

// queryText returns the contents of one embedded query file.
func queryText(name string) (string, error) {
	raw, err := queryFiles.ReadFile("queries/" + name)
	if err != nil {
		return "", fmt.Errorf("read hub query %s: %w", name, err)
	}
	return string(raw), nil
}
