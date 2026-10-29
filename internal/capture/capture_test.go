package capture

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLimitsResolve(t *testing.T) {
	t.Parallel()
	require.Equal(t, DefaultLimits(), Limits{}.resolve())
	partial := Limits{ChunkMaxBytes: 1024, AgentQuotaBytes: 1 << 30, NodeQuotaBytes: 1<<30 + 65536}
	got := partial.resolve()
	require.Equal(t, int64(1024), got.ChunkMaxBytes)
	require.Equal(t, DefaultLimits().FrameMaxBytes, got.FrameMaxBytes)
	require.Equal(t, int64(1<<30), got.AgentQuotaBytes)
	require.NoError(t, Limits{ChunkMaxBytes: 1024}.resolve().Validate())
}
