//go:build !unix

package proxy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunUnsupported reports ErrUnsupported without PTY support.
func TestRunUnsupported(t *testing.T) {
	t.Parallel()
	_, err := Run(t.Context(), Options{Command: []string{"sh"}})
	require.ErrorIs(t, err, errors.ErrUnsupported)
}
