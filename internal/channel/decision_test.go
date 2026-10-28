package channel

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// decisionAPIAllowlist names the exported functions allowed to return a
// Decision. It is empty until a later subphase adds the gates.
var decisionAPIAllowlist = map[string]bool{}

// testRecordTime is the fixed clock of the hand-built records.
var testRecordTime = time.Date(2026, time.October, 28, 14, 0, 0, 0, time.UTC)

// testMatchedControlID is the control matched by the hand-built records.
const testMatchedControlID = "ctl_00000000000000000000000001"

// testAgent1 and testAgent2 are the sender and receiver of the tests.
const (
	testAgent1 = "agent-1"
	testAgent2 = "agent-2"
)

// testRecord builds a valid ALLOW record with a coherent explanation.
func testRecord() protocol.DecisionRecord {
	version := int64(18)
	generation := int64(1)
	return protocol.DecisionRecord{
		DecisionID:        "chd_00000000000000000000000001",
		ObjectType:        protocol.DecisionObjectTypeMessage,
		ObjectID:          "msg_42",
		Stage:             protocol.DecisionStageAdmission,
		Plane:             protocol.PlaneMessage,
		FromAgent:         testAgent1,
		ToAgent:           testAgent2,
		Outcome:           protocol.DecisionOutcomeAllow,
		Reason:            protocol.DecisionReasonAllowed,
		MatchedControlIDs: []protocol.ControlID{testMatchedControlID},
		EffectiveControl: protocol.EffectiveControl{
			GlobalMode:       protocol.ChannelModeActive,
			FromNodeMode:     protocol.ChannelModeActive,
			ToNodeMode:       protocol.ChannelModeActive,
			FromTX:           true,
			FromAutoContinue: true,
			ToRX:             true,
			ToAutoDelivery:   true,
			ToDeliverable:    true,
			LinkMode:         protocol.LinkModeAllow,
		},
		ControlVersions: protocol.ControlVersions{string(protocol.ScopeTypeGlobal): 18},
		Explanation: protocol.Explanation{
			FromAgent: testAgent1,
			ToAgent:   testAgent2,
			Plane:     protocol.PlaneMessage,
			Outcome:   protocol.DecisionOutcomeAllow,
			Reason:    protocol.DecisionReasonAllowed,
			Lines: []protocol.ExplanationLine{
				{Scope: protocol.ExplanationScopeGlobal, Key: "*", Value: "ACTIVE", Version: &version},
				{Scope: protocol.ExplanationScopeObject, Key: "msg_42", Value: "none"},
			},
			LoopGeneration: &generation,
		},
		CreatedAt: testRecordTime,
	}
}

// testDecision builds a decision from the hand-built record.
func testDecision(t *testing.T) Decision {
	t.Helper()

	rec := testRecord()
	decided, err := newDecision(rec, restrictionHold, protocol.ExplanationScopeLink)
	require.NoError(t, err)
	return decided
}

// TestDecisionAPISurface fails if an exported function, or an exported
// method of an exported type, returns a Decision, unless allowlisted; it
// also requires every Decision field to be unexported.
func TestDecisionAPISurface(t *testing.T) {
	t.Parallel()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Join(".", name))
		require.NoError(t, err)
		parsed, err := parser.ParseFile(token.NewFileSet(), name, src, 0)
		require.NoError(t, err)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() {
				continue
			}
			if fn.Recv != nil && !recvOnExportedType(fn) {
				continue
			}
			if fn.Type.Results == nil {
				continue
			}
			for _, result := range fn.Type.Results.List {
				found := false
				ast.Inspect(result.Type, func(n ast.Node) bool {
					ident, ok := n.(*ast.Ident)
					if ok && ident.Name == "Decision" {
						found = true
					}
					return !found
				})
				if found {
					require.True(t, decisionAPIAllowlist[fn.Name.Name], "%s.%s returns a Decision", name, fn.Name.Name)
				}
			}
		}
	}

	decided := reflect.TypeOf(Decision{})
	require.Equal(t, 1, decided.NumField(), "exactly one field")
	field := decided.Field(0)
	require.Equal(t, "d", field.Name)
	require.NotEmpty(t, field.PkgPath, "field d is unexported")
}

// recvOnExportedType reports whether the method receiver names an exported
// type, dereferenced.
func recvOnExportedType(fn *ast.FuncDecl) bool {
	for _, field := range fn.Recv.List {
		expr := field.Type
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		if ident, ok := expr.(*ast.Ident); ok {
			return ident.IsExported()
		}
	}
	return false
}

// TestDecisionImmutable proves the accessors hand out copies: changing the
// returned slices, maps and records does not change the decision.
func TestDecisionImmutable(t *testing.T) {
	t.Parallel()

	decided := testDecision(t)
	require.Equal(t, restrictionHold, decided.d.class)
	require.Equal(t, protocol.ExplanationScopeLink, decided.d.blocker)

	ids := decided.MatchedControlIDs()
	require.Len(t, ids, 1)
	ids[0] = "ctl_00000000000000000000000002"
	require.Equal(t, protocol.ControlID(testMatchedControlID), decided.MatchedControlIDs()[0])

	vers := decided.Versions()
	vers[string(protocol.ScopeTypeGlobal)] = 999
	vers["node:node-a"] = 1
	require.Equal(t, protocol.ControlVersions{string(protocol.ScopeTypeGlobal): 18}, decided.Versions())

	rec := decided.Record()
	rec.MatchedControlIDs[0] = "ctl_00000000000000000000000002"
	rec.ControlVersions[string(protocol.ScopeTypeGlobal)] = 999
	rec.Explanation.Lines[0].Value = "MUTATED"
	*rec.Explanation.Lines[0].Version = 999
	*rec.Explanation.LoopGeneration = 999
	again := decided.Record()
	require.Equal(t, protocol.ControlID(testMatchedControlID), again.MatchedControlIDs[0])
	require.Equal(t, int64(18), again.ControlVersions[string(protocol.ScopeTypeGlobal)])
	require.Equal(t, "ACTIVE", again.Explanation.Lines[0].Value)
	require.Equal(t, int64(18), *again.Explanation.Lines[0].Version)
	require.Equal(t, int64(1), *again.Explanation.LoopGeneration)
}

// TestDecisionZero proves the zero Decision is inert: every accessor is
// nil-safe, nothing is allowed and the outcome is empty.
func TestDecisionZero(t *testing.T) {
	t.Parallel()

	var decided Decision
	require.True(t, decided.IsZero())
	require.False(t, decided.Allowed())
	require.Empty(t, decided.Outcome())
	require.Empty(t, decided.ID())
	require.Empty(t, decided.Stage())
	require.Empty(t, decided.Plane())
	objectType, objectID := decided.Object()
	require.Empty(t, objectType)
	require.Empty(t, objectID)
	require.Empty(t, decided.From())
	require.Empty(t, decided.To())
	require.Empty(t, decided.Reason())
	require.Nil(t, decided.MatchedControlIDs())
	require.Equal(t, protocol.EffectiveControl{}, decided.Effective())
	require.Nil(t, decided.Versions())
	require.Equal(t, protocol.DecisionRecord{}, decided.Record())
	require.True(t, decided.DecidedAt().IsZero())
}

// TestNewDecisionRejectsInvalid proves the constructor wraps the record's
// error instead of keeping a bad record.
func TestNewDecisionRejectsInvalid(t *testing.T) {
	t.Parallel()

	rec := testRecord()
	rec.Reason = protocol.DecisionReasonLinkHold
	_, err := newDecision(rec, restrictionNone, "")
	require.ErrorIs(t, err, protocol.ErrInvalid)
}
