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
		require.Contains(t, []string{MilestoneV1, MilestoneV11, MilestoneV12, MilestoneV13}, root.Since)
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
	require.Equal(t, "9520f3ea41df4438764a5676a20eeb436e2eb0832f277b6ce181b90aa7b39dfa",
		hex.EncodeToString(digest.Sum(nil)))
}

// TestPreV13SchemasRemainFrozen extends the byte-level compatibility proof to
// every root published through contracts-v1.2.
func TestPreV13SchemasRemainFrozen(t *testing.T) {
	t.Parallel()

	digest := sha256.New()
	for _, root := range Roots() {
		if root.Since == MilestoneV13 {
			continue
		}
		_, err := digest.Write(append([]byte(root.Name), 0))
		require.NoError(t, err)
		raw, err := os.ReadFile(filepath.Join("..", "..", "schemas", "v1", root.Name+".schema.json"))
		require.NoError(t, err)
		_, err = digest.Write(raw)
		require.NoError(t, err)
	}
	require.Equal(t, "3bd8357b21422fdbb88d04b5b987da0d1b1781d7cbf5ea3eb9930c286ab79882",
		hex.EncodeToString(digest.Sum(nil)))
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
