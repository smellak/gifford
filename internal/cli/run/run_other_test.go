//go:build !unix

package run

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunNotImplementedOther(t *testing.T) {
	t.Parallel()
	require.NotNil(t, NewCmd(nil))
}
