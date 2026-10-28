package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	v6 "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

// TestFixtures validates every fixture against its schema, round-trips the
// valid ones through the Go types and rejects the invalid ones.
func TestFixtures(t *testing.T) {
	t.Parallel()

	compiler := v6.NewCompiler()
	compiler.DefaultDraft(v6.Draft2020)
	compiler.AssertFormat()
	byName := make(map[string]Root, len(Roots()))
	for _, root := range Roots() {
		byName[root.Name] = root
		schemaDoc := decodeJSON(t, filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))
		require.NoError(t, compiler.AddResource(schemaID(root), schemaDoc))
	}
	schemas := make(map[string]*v6.Schema, len(byName))
	for _, root := range Roots() {
		compiled, err := compiler.Compile(schemaID(root))
		require.NoError(t, err)
		schemas[root.Name] = compiled
	}

	entries, err := os.ReadDir("testdata")
	require.NoError(t, err)
	seen := make(map[string]bool, len(entries))
	for _, entry := range entries {
		require.True(t, entry.IsDir(), "orphan file in testdata")
		root, ok := byName[entry.Name()]
		require.True(t, ok, "orphan fixture directory")
		seen[entry.Name()] = true
		t.Run(entry.Name(), func(t *testing.T) {
			t.Parallel()
			testRootFixtures(t, root, schemas[root.Name])
		})
	}
	require.Len(t, seen, len(byName), "every root needs a fixture directory")
}

func schemaID(root Root) string {
	return SchemaIDBase + root.Name + ".schema.json"
}

func decodeJSON(t *testing.T, path string) any {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	doc, err := v6.UnmarshalJSON(bytes.NewReader(raw))
	require.NoError(t, err)
	return doc
}

func testRootFixtures(t *testing.T, root Root, schema *v6.Schema) {
	t.Helper()
	files, err := os.ReadDir(filepath.Join("testdata", root.Name))
	require.NoError(t, err)
	var valid, invalid int
	for _, file := range files {
		name := file.Name()
		require.True(t, strings.HasSuffix(name, ".json"), "bad fixture name")
		raw, err := os.ReadFile(filepath.Join("testdata", root.Name, name))
		require.NoError(t, err)
		inst, err := v6.UnmarshalJSON(bytes.NewReader(raw))
		require.NoError(t, err)
		schemaErr := schema.Validate(inst)
		switch {
		case strings.HasPrefix(name, "valid_"):
			valid++
			require.NoError(t, schemaErr, "valid fixture rejected")
			roundTripFixture(t, root, raw)
		case strings.HasPrefix(name, "invalid_schemaonly_"):
			invalid++
			require.Error(t, schemaErr, "invalid fixture accepted")
		case strings.HasPrefix(name, "invalid_"):
			invalid++
			require.Error(t, schemaErr, "invalid fixture accepted")
			rejectFixture(t, root, raw)
		default:
			t.Fatalf("bad fixture name %s", name)
		}
	}
	require.GreaterOrEqual(t, valid, 1, "need a valid fixture")
	require.GreaterOrEqual(t, invalid, 2, "need two invalid fixtures")
}

// roundTripFixture strictly decodes a valid fixture, validates it,
// re-encodes it and requires the encoding to validate and match.
func roundTripFixture(t *testing.T, root Root, raw []byte) {
	t.Helper()
	into := root.New()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(into))
	if v, ok := into.(nestedValidator); ok {
		require.NoError(t, v.Validate())
	}
	encoded, err := json.Marshal(into)
	require.NoError(t, err)
	inst, err := v6.UnmarshalJSON(bytes.NewReader(encoded))
	require.NoError(t, err)
	compiled := compileSchema(t, root)
	require.NoError(t, compiled.Validate(inst))
	again := root.New()
	decAgain := json.NewDecoder(bytes.NewReader(encoded))
	decAgain.DisallowUnknownFields()
	require.NoError(t, decAgain.Decode(again))
	reencoded, err := json.Marshal(again)
	require.NoError(t, err)
	require.JSONEq(t, string(encoded), string(reencoded))
}

func compileSchema(t *testing.T, root Root) *v6.Schema {
	t.Helper()
	compiler := v6.NewCompiler()
	compiler.DefaultDraft(v6.Draft2020)
	compiler.AssertFormat()
	require.NoError(t, compiler.AddResource(schemaID(root), decodeJSON(t, filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))))
	compiled, err := compiler.Compile(schemaID(root))
	require.NoError(t, err)
	return compiled
}

// rejectFixture requires strict decode plus Validate to fail for an
// invalid fixture (schema-only cases excepted).
func rejectFixture(t *testing.T, root Root, raw []byte) {
	t.Helper()
	into := root.New()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return
	}
	if v, ok := into.(nestedValidator); ok {
		err := v.Validate()
		require.Error(t, err)
		var ve *ValidationError
		require.True(t, errors.As(err, &ve) || errors.Is(err, ErrUnsupportedVersion))
		return
	}
	t.Fatal("root without Validate accepted an invalid fixture")
}
