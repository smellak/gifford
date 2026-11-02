package protocol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bridge agents read from the hub: the panel line must point at the inbox
// call without carrying message content or control characters.
func TestBridgeNoticeLine(t *testing.T) {
	t.Parallel()

	line := string(BridgeNoticeLine)
	require.NotEmpty(t, line, "bridge notice line must exist")
	require.True(t, BridgeNoticeLine.Valid(), "bridge notice line must be a valid NoticeLine")
	require.NotContains(t, line, "\n", "notice must be a single line")
	require.NotContains(t, line, "\r", "notice must be a single line")
	require.Contains(t, line, "capyverse_get_messages", "bridge notice must remit to the inbox call")
	require.NotContains(t, line, ";", "tmux would chain commands on a semicolon")
	require.LessOrEqual(t, len([]rune(line)), 512, "notice must fit the NoticeLine bound")
	require.True(t, strings.HasPrefix(line, "[GIFFORD]"), "notice keeps the product prefix")
}
