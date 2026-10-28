package protocol

import (
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
		require.Contains(t, []string{MilestoneV1, MilestoneV11}, root.Since)
		require.False(t, seen[root.Name], "duplicate root")
		seen[root.Name] = true
		require.NotNil(t, root.New())
	}
	require.True(t, strings.HasSuffix(SchemaIDBase, "/schemas/v1/"))
}
