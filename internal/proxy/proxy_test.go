package proxy

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestExit checks the shell status and the description of each outcome.
func TestExit(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0, Exit{Code: 0}.Status())
	require.Equal(t, 7, Exit{Code: 7}.Status())
	require.Equal(t, 143, Exit{Code: -1, Signal: syscall.SIGTERM}.Status())
	require.Equal(t, "harness exited with status 0", Exit{Code: 0}.String())
	require.Equal(t, "harness exited with status 7", Exit{Code: 7}.String())
	require.Equal(t,
		"harness killed by signal 15 (terminated), status 143",
		Exit{Code: -1, Signal: syscall.SIGTERM}.String())
}

// TestChildWritesOnlyInWriter keeps every write to the child PTY in
// writer.go: no other non-test file may call unix.Write.
func TestChildWritesOnlyInWriter(t *testing.T) {
	t.Parallel()
	for _, name := range proxySources(t) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if id.Name == "unix" && sel.Sel.Name == "Write" {
				require.Equal(t, "writer.go", name, "unix.Write outside writer.go")
			}
			return true
		})
	}
}

// TestImports keeps non-test files on the imports §3.3 allows: the standard
// library plus creack/pty, x/term and x/sys/unix.
func TestImports(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{
		"github.com/creack/pty": true,
		"golang.org/x/term":     true,
		"golang.org/x/sys/unix": true,
	}
	for _, name := range proxySources(t) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		require.NoError(t, err)
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			require.NoError(t, err)
			if strings.Contains(path, ".") {
				require.True(t, allowed[path], "import %s not allowed", path)
			}
		}
	}
}

// proxySources lists the non-test Go files of this package.
func proxySources(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		names = append(names, name)
	}
	return names
}

// TestGoroutinesOnlyThroughSpawn keeps spawn the only place starting one.
func TestGoroutinesOnlyThroughSpawn(t *testing.T) {
	t.Parallel()
	for _, name := range proxySources(t) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if _, ok := n.(*ast.GoStmt); ok {
					require.Equal(t, "spawn", fn.Name.Name, "go statement outside spawn")
				}
				return true
			})
		}
	}
}
