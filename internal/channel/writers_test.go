package channel

import (
	"bufio"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	writerFixtureMark = "want:control-write"
	writerException   = "internal/eventstore/schema_test.go"
	writerFunction    = "TestEnvelopeControlVersions"
	writerFingerprint = "update message_envelopes set control_state = ?, control_reason = ?, control_version = ? where message_id = ?"
)

var (
	writerTripleRE = regexp.MustCompile(`(?i)\b(?:control_state|control_reason|control_version)\b`)
	writerSetRE    = regexp.MustCompile(`(?i)\bset\b`)
	writerWhereRE  = regexp.MustCompile(`(?i)\bwhere\b`)
	writerScalarRE = regexp.MustCompile(`(?i)\b(?:control_state|control_reason|control_version)\b\s*=`)
	writerTupleRE  = regexp.MustCompile(`(?is)\(([^)]*)\)\s*=`)
	writerInsertRE = regexp.MustCompile(`(?i)\b(?:insert(?:\s+or\s+[a-z_]+)?|replace)\s+(?:into\s+)?([a-z_][a-z0-9_]*(?:\s*\.\s*[a-z_][a-z0-9_]*)*)`)
)

// controlWriterInventory is the reviewed exception outside internal/channel.
// It is anchored to its function and normalized statement, not merely its file.
func controlWriterInventory() map[string]writerInventoryEntry {
	return map[string]writerInventoryEntry{
		writerException: {
			Count:       1,
			Function:    writerFunction,
			Fingerprint: writerFingerprint,
		},
	}
}

type writerInventoryEntry struct {
	Count       int
	Function    string
	Fingerprint string
}

type writerSourcePos struct {
	line   int
	column int
}

type writerFragment struct {
	text      string
	positions []writerSourcePos
	function  string
}

type writerHit struct {
	Path        string
	Line        int
	column      int
	Function    string
	Fingerprint string
}

// TestControlTripleWriters enforces R47 outside internal/channel.
//
// Limits: this is a tripwire, not a SQL parser. For Go it analyzes string
// literals and + chains containing a literal, treating non-literal operands as
// unknown. It cannot see SQL assembled from variable column names,
// strings.Join, or fmt.Sprintf arguments. It reads Go, SQL, shell, YAML,
// Taskfiles, Makefiles and shebang scripts; it does not read Markdown, JSON or
// other data formats. Symlinks are rejected instead of followed.
func TestControlTripleWriters(t *testing.T) {
	t.Parallel()

	hits, err := scanControlTripleWriters(filepath.Join("..", ".."))
	require.NoError(t, err)
	require.NoError(t, validateControlWriters(hits, controlWriterInventory()))
}

// TestControlTripleWriterFixtures requires every marked fixture line, and no
// other line, to be reported. Fixtures are data and are never executed.
func TestControlTripleWriterFixtures(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("testdata", "writers", "*"))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		filepath.Join("testdata", "writers", "go.go"),
		filepath.Join("testdata", "writers", "writes.sh"),
		filepath.Join("testdata", "writers", "writes.sql"),
	}, paths)
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Parallel()

			source, err := os.ReadFile(path)
			require.NoError(t, err)
			hits, err := scanWriterFile(filepath.ToSlash(path), source)
			require.NoError(t, err)
			require.Equal(t, markedWriterLines(source), hitLines(hits))
		})
	}
}

// TestControlTripleWriterPlantedTrees covers repository violations, the exact
// exception anchor, traversal failures and the internal/channel exclusion.
func TestControlTripleWriterPlantedTrees(t *testing.T) {
	t.Parallel()

	t.Run("unexpected Go writer", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, "cmd/x/main.go", "package main\nfunc main() {\n\t_ = `UPDATE messages SET control_state = ?`\n}\n")
		err := checkPlantedWriters(root)
		require.ErrorContains(t, err, "cmd/x/main.go:[3]")
	})

	t.Run("extensionless shebang writer", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, "scripts/write", "#!/bin/sh\nsql='INSERT INTO messages VALUES (1)'\n")
		err := checkPlantedWriters(root)
		require.ErrorContains(t, err, "scripts/write:[2]")
	})

	t.Run("enumerated text formats", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			path     string
			contents string
		}{
			{"scripts/write.bash", "sql='UPDATE messages SET control_state = ?'\n"},
			{"config/write.yml", "sql: UPDATE messages SET control_reason = ?\n"},
			{"config/write.yaml", "sql: UPDATE messages SET control_version = ?\n"},
			{"Taskfile", "sql: UPDATE messages SET control_state = ?\n"},
			{"Makefile", "SQL=UPDATE messages SET control_reason = ?\n"},
		}
		for _, tc := range cases {
			root := plantedWriterTree(t)
			writePlantedFile(t, root, tc.path, tc.contents)
			err := checkPlantedWriters(root)
			require.ErrorContains(t, err, tc.path+":[1]")
		}
	})

	t.Run("migration writer", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, "migrations/next.sql", "UPDATE messages SET control_reason = ?;\n")
		err := checkPlantedWriters(root)
		require.ErrorContains(t, err, "migrations/next.sql:[1]")
	})

	t.Run("second inventoried write", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		path := filepath.Join(root, filepath.FromSlash(writerException))
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		require.NoError(t, err)
		_, writeErr := io.WriteString(file, "\nvar extra = `UPDATE messages SET control_state = ?`\n")
		require.NoError(t, writeErr)
		require.NoError(t, file.Close())
		err = checkPlantedWriters(root)
		require.ErrorContains(t, err, "expected 1 control-triple write, found 2")
	})

	t.Run("inventoried write removed", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, writerException, "package eventstore\nfunc TestEnvelopeControlVersions() {}\n")
		err := checkPlantedWriters(root)
		require.ErrorContains(t, err, "expected 1 control-triple write, found 0")
	})

	t.Run("inventoried statement substituted", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, writerException, plantedException("TestEnvelopeControlVersions",
			"UPDATE message_envelopes SET control_state = ? WHERE message_id = ?"))
		err := checkPlantedWriters(root)
		require.ErrorContains(t, err, "statement fingerprint changed")
	})

	t.Run("inventoried statement moved", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, writerException, plantedException("TestAnotherFunction", writerFingerprint))
		err := checkPlantedWriters(root)
		require.ErrorContains(t, err, "moved from TestEnvelopeControlVersions")
	})

	t.Run("internal channel writer is excluded", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, "internal/channel/local.go", "package channel\nvar local = `UPDATE messages SET control_version = ?`\n")
		require.NoError(t, checkPlantedWriters(root))
	})

	t.Run("invalid Go fails closed", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		writePlantedFile(t, root, "cmd/x/main.go", "package main\nfunc {")
		_, err := scanControlTripleWriters(root)
		require.ErrorContains(t, err, "parse cmd/x/main.go")
	})

	t.Run("missing root fails closed", func(t *testing.T) {
		t.Parallel()

		_, err := scanControlTripleWriters(filepath.Join(t.TempDir(), "missing"))
		require.Error(t, err)
	})

	t.Run("symlink fails closed", func(t *testing.T) {
		t.Parallel()

		root := plantedWriterTree(t)
		target := filepath.Join(t.TempDir(), "outside.sql")
		require.NoError(t, os.WriteFile(target, []byte("SELECT 1;\n"), 0o600))
		require.NoError(t, os.Symlink(target, filepath.Join(root, "outside.sql")))
		_, err := scanControlTripleWriters(root)
		require.ErrorContains(t, err, "refusing symlink outside.sql")
	})
}

func plantedWriterTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writePlantedFile(t, root, writerException, plantedException(writerFunction, writerFingerprint))
	return root
}

func plantedException(function, statement string) string {
	return "package eventstore\nfunc " + function + "() {\n\t_ = `" + statement + "`\n}\n"
}

func writePlantedFile(t *testing.T, root, name, contents string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
}

func checkPlantedWriters(root string) error {
	hits, err := scanControlTripleWriters(root)
	if err != nil {
		return err
	}
	return validateControlWriters(hits, controlWriterInventory())
}

func scanControlTripleWriters(root string) ([]writerHit, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	var hits []writerHit
	err = filepath.WalkDir(absRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk repository: %w", walkErr)
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			return fmt.Errorf("relative path for %s: %w", path, err)
		}
		rel = filepath.ToSlash(rel)
		if rel == ".git" || rel == "internal/channel" {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink %s", rel)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect %s: %w", rel, err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		scan, err := writerFileSelected(path, rel)
		if err != nil {
			return err
		}
		if !scan {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}
		found, err := scanWriterFile(rel, source)
		if err != nil {
			return err
		}
		hits = append(hits, found...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortWriterHits(hits)
	return hits, nil
}

func writerFileSelected(path, rel string) (bool, error) {
	name := filepath.Base(rel)
	switch strings.ToLower(filepath.Ext(name)) {
	case ".go", ".sql", ".sh", ".bash", ".yml", ".yaml":
		return true, nil
	}
	if name == "Taskfile" || name == "Makefile" {
		return true, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("inspect %s for shebang: %w", rel, err)
	}
	defer func() { _ = file.Close() }()
	prefix := make([]byte, 2)
	n, err := io.ReadFull(file, prefix)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, fmt.Errorf("inspect %s for shebang: %w", rel, err)
	}
	return n == 2 && string(prefix) == "#!", nil
}

func scanWriterFile(path string, source []byte) ([]writerHit, error) {
	if strings.EqualFold(filepath.Ext(path), ".go") {
		return scanWriterGo(path, source)
	}
	fragment := writerTextFragment(string(source))
	return fragmentHits(path, fragment), nil
}

func scanWriterGo(path string, source []byte) ([]writerHit, error) {
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, path, source, parser.AllErrors)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	var fragments []writerFragment
	var scanErr error
	ast.Inspect(parsed, func(node ast.Node) bool {
		if scanErr != nil {
			return false
		}
		switch typed := node.(type) {
		case *ast.BasicLit:
			if typed.Kind != token.STRING {
				return true
			}
			fragment, err := writerLiteralFragment(files, typed)
			if err != nil {
				scanErr = fmt.Errorf("scan string in %s: %w", path, err)
				return false
			}
			fragment.function = writerFunctionAt(parsed, typed.Pos())
			fragments = append(fragments, fragment)
		case *ast.BinaryExpr:
			if typed.Op != token.ADD {
				return true
			}
			fragment, hasLiteral, err := writerPlusFragment(files, typed)
			if err != nil {
				scanErr = fmt.Errorf("scan string chain in %s: %w", path, err)
				return false
			}
			if hasLiteral {
				fragment.function = writerFunctionAt(parsed, typed.Pos())
				fragments = append(fragments, fragment)
			}
		}
		return true
	})
	if scanErr != nil {
		return nil, scanErr
	}
	var hits []writerHit
	for _, fragment := range fragments {
		hits = append(hits, fragmentHits(path, fragment)...)
	}
	return dedupeWriterHits(hits), nil
}

func writerFunctionAt(file *ast.File, pos token.Pos) string {
	for _, decl := range file.Decls {
		function, ok := decl.(*ast.FuncDecl)
		if ok && function.Pos() <= pos && pos < function.End() {
			return function.Name.Name
		}
	}
	return ""
}

func writerLiteralFragment(files *token.FileSet, literal *ast.BasicLit) (writerFragment, error) {
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return writerFragment{}, fmt.Errorf("unquote string: %w", err)
	}
	start := files.Position(literal.Pos())
	positions := make([]writerSourcePos, len(value))
	line := start.Line
	column := start.Column + 1
	raw := strings.HasPrefix(literal.Value, "`")
	for index := range len(value) {
		positions[index] = writerSourcePos{line: line, column: column}
		if raw && value[index] == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return writerFragment{text: value, positions: positions}, nil
}

func writerPlusFragment(files *token.FileSet, expression ast.Expr) (writerFragment, bool, error) {
	switch typed := expression.(type) {
	case *ast.BinaryExpr:
		if typed.Op != token.ADD {
			return writerUnknownFragment(files, typed.Pos()), false, nil
		}
		left, leftLiteral, err := writerPlusFragment(files, typed.X)
		if err != nil {
			return writerFragment{}, false, err
		}
		right, rightLiteral, err := writerPlusFragment(files, typed.Y)
		if err != nil {
			return writerFragment{}, false, err
		}
		left.text += right.text
		left.positions = append(left.positions, right.positions...)
		return left, leftLiteral || rightLiteral, nil
	case *ast.BasicLit:
		if typed.Kind != token.STRING {
			return writerUnknownFragment(files, typed.Pos()), false, nil
		}
		fragment, err := writerLiteralFragment(files, typed)
		return fragment, true, err
	default:
		return writerUnknownFragment(files, expression.Pos()), false, nil
	}
}

func writerUnknownFragment(files *token.FileSet, pos token.Pos) writerFragment {
	position := files.Position(pos)
	return writerFragment{
		text:      "\x00",
		positions: []writerSourcePos{{line: position.Line, column: position.Column}},
	}
}

func writerTextFragment(text string) writerFragment {
	positions := make([]writerSourcePos, len(text))
	line, column := 1, 1
	for index := range len(text) {
		positions[index] = writerSourcePos{line: line, column: column}
		if text[index] == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return writerFragment{text: text, positions: positions}
}

func fragmentHits(path string, fragment writerFragment) []writerHit {
	offsets := controlWriteOffsets(fragment.text)
	hits := make([]writerHit, 0, len(offsets))
	for _, offset := range offsets {
		if offset.index < 0 || offset.index >= len(fragment.positions) {
			continue
		}
		position := fragment.positions[offset.index]
		hits = append(hits, writerHit{
			Path:        filepath.ToSlash(path),
			Line:        position.line,
			column:      position.column,
			Function:    fragment.function,
			Fingerprint: normalizeWriterSQL(fragment.text[offset.statementStart:offset.statementEnd]),
		})
	}
	return dedupeWriterHits(hits)
}

type writerOffset struct {
	index          int
	statementStart int
	statementEnd   int
}

func controlWriteOffsets(text string) []writerOffset {
	masked := maskWriterComments(text)
	canonical := canonicalWriterSQL(masked)
	var offsets []writerOffset
	for _, set := range writerSetRE.FindAllStringIndex(canonical, -1) {
		end := len(canonical)
		if where := writerWhereRE.FindStringIndex(canonical[set[1]:]); where != nil {
			end = set[1] + where[0]
		}
		if semicolon := strings.IndexByte(canonical[set[1]:end], ';'); semicolon >= 0 {
			end = set[1] + semicolon
		}
		if assignment := writerAssignmentOffset(canonical[set[1]:end]); assignment >= 0 {
			index := set[1] + assignment
			start, finish := writerStatementBounds(canonical, set[0])
			offsets = append(offsets, writerOffset{index: index, statementStart: start, statementEnd: finish})
		}
	}
	for _, insert := range writerInsertRE.FindAllStringSubmatchIndex(canonical, -1) {
		table := strings.ReplaceAll(canonical[insert[2]:insert[3]], " ", "")
		table = strings.ReplaceAll(table, "\t", "")
		table = strings.ReplaceAll(table, "\n", "")
		table = strings.ReplaceAll(table, "\r", "")
		if dot := strings.LastIndexByte(table, '.'); dot >= 0 {
			table = table[dot+1:]
		}
		cursor := skipWriterSpace(canonical, insert[1])
		if cursor < len(canonical) && canonical[cursor] == '(' {
			endParen := matchingWriterParen(canonical, cursor)
			if endParen > cursor {
				if triple := writerTripleRE.FindStringIndex(canonical[cursor+1 : endParen]); triple != nil {
					index := cursor + 1 + triple[0]
					start, finish := writerStatementBounds(canonical, insert[0])
					offsets = append(offsets, writerOffset{index: index, statementStart: start, statementEnd: finish})
				}
			}
		} else if writerTargetTable(table) {
			start, finish := writerStatementBounds(canonical, insert[0])
			offsets = append(offsets, writerOffset{index: insert[0], statementStart: start, statementEnd: finish})
		}
	}
	for open := strings.IndexByte(canonical, '('); open >= 0; {
		endParen := matchingWriterParen(canonical, open)
		if endParen < 0 {
			break
		}
		triple := writerTripleRE.FindStringIndex(canonical[open+1 : endParen])
		after := skipWriterSpace(canonical, endParen+1)
		if triple != nil && (writerWordAt(canonical, after, "values") || writerWordAt(canonical, after, "select")) {
			index := open + 1 + triple[0]
			start, finish := writerStatementBounds(canonical, open)
			offsets = append(offsets, writerOffset{index: index, statementStart: start, statementEnd: finish})
		}
		next := strings.IndexByte(canonical[endParen+1:], '(')
		if next < 0 {
			break
		}
		open = endParen + 1 + next
	}
	return dedupeWriterOffsets(offsets)
}

func writerAssignmentOffset(segment string) int {
	best := -1
	if scalar := writerScalarRE.FindStringIndex(segment); scalar != nil {
		best = scalar[0]
	}
	for _, tuple := range writerTupleRE.FindAllStringSubmatchIndex(segment, -1) {
		triple := writerTripleRE.FindStringIndex(segment[tuple[2]:tuple[3]])
		if triple == nil {
			continue
		}
		index := tuple[2] + triple[0]
		if best < 0 || index < best {
			best = index
		}
	}
	return best
}

func writerTargetTable(table string) bool {
	switch table {
	case "messages", "message_envelopes", "delivery_queue", "artifact_transfers":
		return true
	default:
		return false
	}
}

func matchingWriterParen(text string, open int) int {
	depth := 0
	for index := open; index < len(text); index++ {
		switch text[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index
			}
		}
	}
	return -1
}

func skipWriterSpace(text string, at int) int {
	for at < len(text) && (text[at] == ' ' || text[at] == '\t' || text[at] == '\n' || text[at] == '\r') {
		at++
	}
	return at
}

func writerWordAt(text string, at int, word string) bool {
	end := at + len(word)
	if end > len(text) || text[at:end] != word {
		return false
	}
	return end == len(text) || !writerWordByte(text[end])
}

func writerWordByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '_'
}

func writerStatementBounds(text string, at int) (int, int) {
	start := strings.LastIndexByte(text[:at], ';') + 1
	end := len(text)
	if next := strings.IndexByte(text[at:], ';'); next >= 0 {
		end = at + next
	}
	return start, end
}

func canonicalWriterSQL(text string) string {
	canonical := strings.ToLower(text)
	canonical = strings.NewReplacer("\"", " ", "`", " ", "[", " ", "]", " ").Replace(canonical)
	return canonical
}

func normalizeWriterSQL(text string) string {
	return strings.Join(strings.Fields(canonicalWriterSQL(maskWriterComments(text))), " ")
}

func maskWriterComments(text string) string {
	masked := []byte(text)
	for index := 0; index < len(masked); {
		switch {
		case index+1 < len(masked) && masked[index] == '/' && masked[index+1] == '*':
			masked[index], masked[index+1] = ' ', ' '
			index += 2
			for index+1 < len(masked) && (masked[index] != '*' || masked[index+1] != '/') {
				if masked[index] != '\n' {
					masked[index] = ' '
				}
				index++
			}
			if index+1 < len(masked) {
				masked[index], masked[index+1] = ' ', ' '
				index += 2
			}
		case index+1 < len(masked) && (masked[index] == '-' && masked[index+1] == '-' || masked[index] == '/' && masked[index+1] == '/'):
			for index < len(masked) && masked[index] != '\n' {
				masked[index] = ' '
				index++
			}
		case masked[index] == '#':
			for index < len(masked) && masked[index] != '\n' {
				masked[index] = ' '
				index++
			}
		default:
			index++
		}
	}
	return string(masked)
}

func validateControlWriters(hits []writerHit, inventory map[string]writerInventoryEntry) error {
	byPath := make(map[string][]writerHit)
	for _, hit := range hits {
		byPath[hit.Path] = append(byPath[hit.Path], hit)
	}
	paths := make([]string, 0, len(byPath))
	for path := range byPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		if _, ok := inventory[path]; !ok {
			hit := byPath[path][0]
			return fmt.Errorf("%s:[%d] writes the control triple outside internal/channel", hit.Path, hit.Line)
		}
	}
	for path, expected := range inventory {
		found := byPath[path]
		if len(found) != expected.Count {
			return fmt.Errorf("%s: expected %d control-triple write, found %d", path, expected.Count, len(found))
		}
		if expected.Count == 1 {
			hit := found[0]
			if hit.Function != expected.Function {
				return fmt.Errorf("%s:[%d] inventoried write moved from %s to %s", path, hit.Line, expected.Function, hit.Function)
			}
			if hit.Fingerprint != expected.Fingerprint {
				return fmt.Errorf("%s:[%d] inventoried statement fingerprint changed", path, hit.Line)
			}
		}
	}
	return nil
}

func dedupeWriterOffsets(offsets []writerOffset) []writerOffset {
	seen := make(map[[3]int]bool)
	unique := make([]writerOffset, 0, len(offsets))
	for _, offset := range offsets {
		key := [3]int{offset.index, offset.statementStart, offset.statementEnd}
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, offset)
	}
	return unique
}

func dedupeWriterHits(hits []writerHit) []writerHit {
	seen := make(map[string]bool)
	unique := make([]writerHit, 0, len(hits))
	for _, hit := range hits {
		key := fmt.Sprintf("%s:%d:%d", hit.Path, hit.Line, hit.column)
		if seen[key] {
			continue
		}
		seen[key] = true
		unique = append(unique, hit)
	}
	sortWriterHits(unique)
	return unique
}

func sortWriterHits(hits []writerHit) {
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Path != hits[j].Path {
			return hits[i].Path < hits[j].Path
		}
		if hits[i].Line != hits[j].Line {
			return hits[i].Line < hits[j].Line
		}
		return hits[i].column < hits[j].column
	})
}

func markedWriterLines(source []byte) []int {
	var lines []int
	scanner := bufio.NewScanner(strings.NewReader(string(source)))
	for line := 1; scanner.Scan(); line++ {
		if strings.Contains(scanner.Text(), writerFixtureMark) {
			lines = append(lines, line)
		}
	}
	return lines
}

func hitLines(hits []writerHit) []int {
	lines := make([]int, 0, len(hits))
	for _, hit := range hits {
		lines = append(lines, hit.Line)
	}
	sort.Ints(lines)
	return lines
}
