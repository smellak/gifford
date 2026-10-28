package redacttest_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestImports pins the §3.3 dependency rule for internal/redact/redacttest:
// non-test files import only the standard library plus internal/protocol
// and internal/redact, which the loader and the options need.
func TestImports(t *testing.T) {
	t.Parallel()

	allowed := map[string]bool{
		"github.com/smellak/gifford/internal/protocol": true,
		"github.com/smellak/gifford/internal/redact":   true,
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checked++
		src, err := os.ReadFile(file)
		require.NoError(t, err, "read %s", file)
		parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.ImportsOnly)
		require.NoError(t, err, "parse %s", file)
		for _, imp := range parsed.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if allowed[path] {
				continue
			}
			first, _, _ := strings.Cut(path, "/")
			require.NotContains(t, first, ".", "non-stdlib import %s in %s", path, file)
		}
	}
	require.Positive(t, checked, "no non-test files found")
}
