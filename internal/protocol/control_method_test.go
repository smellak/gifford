package protocol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestControlMethodSpecs checks that the specs cover every ControlMethod
// value exactly once, in enum order, with usable constructors.
func TestControlMethodSpecs(t *testing.T) {
	t.Parallel()

	values := ControlMethod("").EnumValues()
	specs := ControlMethodSpecs()
	require.Len(t, specs, len(values), "one spec per method")
	for i, spec := range specs {
		require.Equal(t, values[i], string(spec.Method), "specs follow enum order")
		require.NotNil(t, spec.NewParams())
		require.NotNil(t, spec.NewResult())
		name := string(spec.Method)
		reads := strings.Contains(name, ".get") || strings.Contains(name, ".list") ||
			name == string(ControlMethodChannelExplain) || name == string(ControlMethodNodePeers) ||
			name == string(ControlMethodModelStatus)
		require.Equal(t, !reads, spec.Mutates, "mutates flag for %s", name)
	}
}
