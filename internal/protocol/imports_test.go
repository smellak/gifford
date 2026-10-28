package protocol

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestImports requires the leaf package rule: non-test files import only
// the standard library (first path element without a dot).
func TestImports(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			src, err := os.ReadFile(file)
			require.NoError(t, err)
			parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.ImportsOnly)
			require.NoError(t, err)
			for _, imp := range parsed.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				first, _, _ := strings.Cut(path, "/")
				require.NotContains(t, first, ".", "non-stdlib import in %s", path)
			}
		})
	}
}

// TestNoDecisionType keeps R46 possible: nobody outside internal/channel
// can build a channel.Decision, so this package declares nothing named
// Decision.
func TestNoDecisionType(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		src, err := os.ReadFile(file)
		require.NoError(t, err)
		parsed, err := parser.ParseFile(token.NewFileSet(), file, src, 0)
		require.NoError(t, err)
		for name := range parsed.Scope.Objects {
			require.NotEqual(t, "Decision", name, "no type named Decision in %s", file)
		}
	}
}
