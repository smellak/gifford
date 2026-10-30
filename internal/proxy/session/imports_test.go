package session

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProxyNeverImportsTheChannel reads the imports of every package
// under internal/proxy, their tests included, and of every package of
// this module they reach: none is internal/channel, since the Proxy does
// not know the channel's control plane. Imports are parsed, not searched
// for: a comment about Go channels does not count.
func TestProxyNeverImportsTheChannel(t *testing.T) {
	t.Parallel()
	const module = "github.com/smellak/gifford/"
	const channel = module + "internal/channel"
	root := filepath.Join("..", "..", "..")

	// imports parses the import paths of the Go files in dir, with or
	// without its tests, whatever their build constraints.
	imports := func(dir string, tests bool) []string {
		t.Helper()
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		var out []string
		fset := token.NewFileSet()
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || (!tests && strings.HasSuffix(name, "_test.go")) {
				continue
			}
			f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ImportsOnly)
			require.NoError(t, err)
			for _, spec := range f.Imports {
				path, err := strconv.Unquote(spec.Path.Value)
				require.NoError(t, err)
				out = append(out, path)
			}
		}
		return out
	}

	// Every directory under internal/proxy with Go files is a package
	// of the Proxy.
	var queue []string
	proxyRoot := filepath.Join(root, "internal", "proxy")
	require.NoError(t, filepath.WalkDir(proxyRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "testdata" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(path, ".go") {
			rel, err := filepath.Rel(root, filepath.Dir(path))
			if err != nil {
				return err
			}
			if pkg := module + filepath.ToSlash(rel); !slices.Contains(queue, pkg) {
				queue = append(queue, pkg)
			}
		}
		return nil
	}))
	for _, pkg := range []string{"internal/proxy", "internal/proxy/draft", "internal/proxy/input", "internal/proxy/session"} {
		require.Contains(t, queue, module+pkg)
	}

	// Follow the module's own packages they reach; from records how
	// each was reached, for the failure message.
	from := map[string]string{}
	seen := map[string]bool{}
	for len(queue) > 0 {
		pkg := queue[0]
		queue = queue[1:]
		if seen[pkg] {
			continue
		}
		seen[pkg] = true
		proxy := pkg == module+"internal/proxy" || strings.HasPrefix(pkg, module+"internal/proxy/")
		dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(pkg, module)))
		for _, imp := range imports(dir, proxy) {
			if imp == channel || strings.HasPrefix(imp, channel+"/") {
				chain := []string{imp, pkg}
				for at := from[pkg]; at != ""; at = from[at] {
					chain = append(chain, at)
				}
				t.Errorf("the Proxy reaches the channel: %s", strings.Join(chain, " <- "))
				continue
			}
			if strings.HasPrefix(imp, module) && !seen[imp] {
				if _, ok := from[imp]; !ok {
					from[imp] = pkg
				}
				queue = append(queue, imp)
			}
		}
	}
	require.True(t, seen[module+"internal/protocol"], "the walk follows the module's packages")
}
