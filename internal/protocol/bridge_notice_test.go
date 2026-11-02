package protocol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBridgeNoticeLinePointsOnlyAtAuthoritativeInbox(t *testing.T) {
	t.Parallel()

	line := string(BridgeNoticeLine)
	require.Equal(t, "[GIFFORD] 1 pending message: check your inbox", line)
	require.True(t, BridgeNoticeLine.Valid())
	require.True(t, strings.HasPrefix(line, "[GIFFORD]"))
	require.NotContains(t, line, "\n")
	require.NotContains(t, line, "\r")
	require.NotContains(t, line, ";", "the injected notice must not chain terminal commands")
}
