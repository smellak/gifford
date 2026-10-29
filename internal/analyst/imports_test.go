package analyst

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const module = "github.com/smellak/gifford"

// TestImports pins that the Analyst cannot reach the input guard: none of
// its non-test files, nor any package of this module they import,
// directly or not, imports internal/proxy, which holds the guard, nor
// internal/dataset, which imports the Proxy's session.
func TestImports(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	require.NoError(t, err, "the module root")

	seen := map[string]bool{}
	var walk func(pkg string)
	walk = func(pkg string) {
		if seen[pkg] {
			return
		}
		seen[pkg] = true
		dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(pkg, module+"/")))
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)
		require.NotEmpty(t, files, pkg)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
			require.NoError(t, err)
			for _, imp := range f.Imports {
				if path := strings.Trim(imp.Path.Value, `"`); strings.HasPrefix(path, module+"/") {
					walk(path)
				}
			}
		}
	}
	walk(module + "/internal/analyst")
	require.True(t, seen[module+"/internal/screenproj"], "the projection is reached")
	for pkg := range seen {
		require.False(t, strings.HasPrefix(pkg, module+"/internal/proxy"), "the Analyst reaches %s", pkg)
		require.False(t, strings.HasPrefix(pkg, module+"/internal/dataset"), "the Analyst reaches %s", pkg)
	}
}
