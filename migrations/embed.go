package migrations

import (
	"embed"
	"io/fs"
)

// files holds every migration. go:embed works only on a package-level
// variable; it is read-only by construction, the R13 exception that the
// channel budgets also use.
//
//go:embed *.sql
var files embed.FS

// FS returns the embedded migration files, read-only.
func FS() fs.FS { return files }
