package registry

import (
	"embed"
)

// queryFiles embeds the registry queries; exactly these files are read by
// queryText and every statement the package runs lives in one of them.
//
//go:embed queries/*.sql
var queryFiles embed.FS

// queryText returns the contents of one embedded query file.
func queryText(name string) (string, error) {
	raw, err := queryFiles.ReadFile("queries/" + name)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
