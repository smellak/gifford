package version

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	t.Parallel()
	got := String()
	require.True(t, strings.HasPrefix(got, "gifford "), got)
}
