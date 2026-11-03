package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	v6 "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

// guardVersionedRoots pairs each served decision root carrying the guard
// enum with its contracts-v1.9 twin. The frozen root keeps rejecting the
// new guard name byte for byte; the twin advertises it.
var guardVersionedRoots = []struct {
	published string
	twin      string
	fixture   string
}{
	{"decision_get_result", "decision_get_result_v1_9", "valid_orchestrator.json"},
	{"delivery_decision_list_result", "delivery_decision_list_result_v1_9", "valid_uncertain.json"},
}

// TestDecisionGuardScreenUnchangedNeedsV19 pins the published/v1.9 split:
// the runtime already persists and serves the screen_unchanged guard, the
// Go types accept it, but the published schemas predate it. Only the v1.9
// twin admits it; a bogus guard fails on both roots and in Go.
func TestDecisionGuardScreenUnchangedNeedsV19(t *testing.T) {
	t.Parallel()

	compiler := v6.NewCompiler()
	compiler.DefaultDraft(v6.Draft2020)
	compiler.AssertFormat()
	byName := map[string]Root{}
	for _, root := range Roots() {
		byName[root.Name] = root
		require.NoError(t, compiler.AddResource(schemaID(root),
			decodeJSON(t, filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))))
	}
	schemas := map[string]*v6.Schema{}
	for name := range byName {
		compiled, err := compiler.Compile(schemaID(byName[name]))
		require.NoError(t, err)
		schemas[name] = compiled
	}

	for _, pair := range guardVersionedRoots {
		t.Run(pair.published, func(t *testing.T) {
			t.Parallel()

			raw, err := os.ReadFile(filepath.Join("testdata", pair.published, pair.fixture))
			require.NoError(t, err)
			validate := func(rootName string, edited []byte) error {
				doc, err := v6.UnmarshalJSON(bytes.NewReader(edited))
				require.NoError(t, err)
				return schemas[rootName].Validate(doc)
			}
			guards := []string{string(GuardNameHumanDraftClosed), string(GuardNameScreenUnchanged), "bogus_guard"}
			for _, guard := range guards {
				edited := bytes.Replace(raw,
					[]byte(`"guard": "`+string(GuardNameHumanDraftClosed)+`"`),
					[]byte(`"guard": "`+guard+`"`), 1)
				require.Contains(t, string(edited), `"guard": "`+guard+`"`,
					"the substitution must have happened exactly once")

				into := byName[pair.published].New()
				dec := json.NewDecoder(bytes.NewReader(edited))
				dec.DisallowUnknownFields()
				if decErr := dec.Decode(into); decErr != nil {
					require.Equal(t, "bogus_guard", guard,
						"only the invented guard may fail Go decoding")
					require.Error(t, schemas[pair.published].Validate(mustDoc(t, edited)))
					require.Error(t, schemas[pair.twin].Validate(mustDoc(t, edited)))
					continue
				}
				validated, ok := into.(interface{ Validate() error })
				require.True(t, ok, "the decision result must validate itself")
				goErr := validated.Validate()
				if guard == "bogus_guard" {
					require.Error(t, goErr, "Go must reject the invented guard")
					require.Error(t, validate(pair.published, edited))
					require.Error(t, validate(pair.twin, edited))
					continue
				}
				require.NoError(t, goErr, "Go must accept the guard %q", guard)
				require.NoError(t, validate(pair.twin, edited),
					"the v1.9 twin must accept the guard %q", guard)
				if guard == "human_draft_closed" {
					require.NoError(t, validate(pair.published, edited),
						"the frozen root keeps accepting the old guard")
				} else {
					require.Error(t, validate(pair.published, edited),
						"the frozen root must keep rejecting screen_unchanged")
				}
			}
		})
	}
}

func mustDoc(t *testing.T, raw []byte) any {
	t.Helper()
	doc, err := v6.UnmarshalJSON(bytes.NewReader(raw))
	require.NoError(t, err)
	return doc
}
