package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRoots checks the root registry: unique snake_case names and known
// milestones. Later slices extend coverage to the op and method specs.
func TestRoots(t *testing.T) {
	t.Parallel()

	seen := make(map[string]bool)
	for _, root := range Roots() {
		require.NotContains(t, root.Name, "-", "name must be snake_case")
		require.True(t, len(root.Name) > 0 && root.Name[0] >= 'a' && root.Name[0] <= 'z')
		for _, r := range root.Name {
			require.True(t, r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_', "name must be snake_case")
		}
		require.Contains(t, []string{MilestoneV1, MilestoneV11, MilestoneV12, MilestoneV13, MilestoneV14, MilestoneV15, MilestoneV16, MilestoneV17, MilestoneV18, MilestoneV19, MilestoneV110}, root.Since)
		require.False(t, seen[root.Name], "duplicate root")
		seen[root.Name] = true
		require.NotNil(t, root.New())
	}
	require.True(t, strings.HasSuffix(SchemaIDBase, "/schemas/v1/"))
}

// TestPreV12SchemasRemainFrozen proves that adding v1.2 roots did not alter
// any schema already shipped by contracts-v1 or contracts-v1.1.
func TestPreV12SchemasRemainFrozen(t *testing.T) {
	t.Parallel()

	digest := sha256.New()
	for _, root := range Roots() {
		if root.Since != MilestoneV1 && root.Since != MilestoneV11 {
			continue
		}
		_, err := digest.Write(append([]byte(root.Name), 0))
		require.NoError(t, err)
		raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))
		require.NoError(t, err)
		_, err = digest.Write(raw)
		require.NoError(t, err)
	}
	require.Equal(t, "f8842eb6b6be8d538e1fc26ec80ced3d2eced656245888922c762569db4582f1",
		hex.EncodeToString(digest.Sum(nil)))
}

// TestPreV13SchemasRemainFrozen extends the byte-level compatibility proof to
// every root published through contracts-v1.2.
func TestPreV13SchemasRemainFrozen(t *testing.T) {
	t.Parallel()

	digest := sha256.New()
	for _, root := range Roots() {
		if root.Since == MilestoneV13 || root.Since == MilestoneV14 || root.Since == MilestoneV15 || root.Since == MilestoneV16 || root.Since == MilestoneV17 || root.Since == MilestoneV18 || root.Since == MilestoneV19 || root.Since == MilestoneV110 {
			continue
		}
		_, err := digest.Write(append([]byte(root.Name), 0))
		require.NoError(t, err)
		raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))
		require.NoError(t, err)
		_, err = digest.Write(raw)
		require.NoError(t, err)
	}
	require.Equal(t, "94d5879faff790522223e0850e6d35fc3cbb7a05bb169345f64513c5dfc66b85",
		hex.EncodeToString(digest.Sum(nil)))
}

// TestPreV14SchemasRemainFrozen extends the byte-level compatibility proof to
// every root published through contracts-v1.3.
func TestPreV14SchemasRemainFrozen(t *testing.T) {
	t.Parallel()

	digest := sha256.New()
	for _, root := range Roots() {
		if root.Since == MilestoneV14 || root.Since == MilestoneV15 || root.Since == MilestoneV16 || root.Since == MilestoneV17 || root.Since == MilestoneV18 || root.Since == MilestoneV19 || root.Since == MilestoneV110 {
			continue
		}
		_, err := digest.Write(append([]byte(root.Name), 0))
		require.NoError(t, err)
		raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))
		require.NoError(t, err)
		_, err = digest.Write(raw)
		require.NoError(t, err)
	}
	require.Equal(t, "88a4c87f0dc4790119e971ca03cb6d879fc2a26ac3798bbc6ebd99fa1d91e306",
		hex.EncodeToString(digest.Sum(nil)))
}

// TestIncidentV17SchemaRemainsFrozen pins v0.1.0-rc.2 (af9ef78) byte for byte;
// neither regeneration nor an updated file may change the published schema.
func TestIncidentV17SchemaRemainsFrozen(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", "incident_list_result.schema.json"))
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	require.Equal(t, "28951a89fa788b2ad1ea612e8e3174e747f91ede109fc7010c27ed79fd06274e",
		hex.EncodeToString(digest[:]), "published v1.7 bytes must remain frozen")
	for _, root := range Roots() {
		if root.Name == "incident_list_result" {
			require.Equal(t, MilestoneV17, root.Since)
			require.Equal(t, raw, generateSchema(t, root), "regeneration must preserve the frozen bytes")
			return
		}
	}
	t.Fatal("the frozen incident root is absent")
}

// TestDecisionV11SchemaRemainsFrozen pins the published decision_get_result
// root byte for byte; neither regeneration nor the contracts-v1.9 twin may
// change the published schema.
func TestDecisionV11SchemaRemainsFrozen(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", "decision_get_result.schema.json"))
	require.NoError(t, err)
	digest := sha256.Sum256(raw)
	require.Equal(t, "eba6c5eee7d202cdf8a22f631a8dd6d0e643fb24cbfd010ec4002253d3c9c3d9",
		hex.EncodeToString(digest[:]), "published v1.1 bytes must remain frozen")
	for _, root := range Roots() {
		if root.Name == "decision_get_result" {
			require.Equal(t, MilestoneV11, root.Since)
			require.Equal(t, raw, generateSchema(t, root), "regeneration must preserve the frozen bytes")
			return
		}
	}
	t.Fatal("the frozen decision root is absent")
}

// TestAgentOpSpecs checks that the op specs cover every AgentOp value
// exactly once.
func TestAgentOpSpecs(t *testing.T) {
	t.Parallel()

	seen := make(map[AgentOp]bool)
	for _, spec := range AgentOpSpecs() {
		require.False(t, seen[spec.Op], "duplicate op spec")
		seen[spec.Op] = true
		require.NotNil(t, spec.NewBody())
	}
	for _, value := range AgentOp("").EnumValues() {
		require.True(t, seen[AgentOp(value)], "op without spec")
	}
	require.Len(t, seen, len(AgentOp("").EnumValues()))
}
