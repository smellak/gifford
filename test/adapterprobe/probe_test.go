//go:build adapterprobe

package adapterprobe

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	binaryEnv  = "GIFFORD_ADAPTERPROBE_BINARY"
	harnessEnv = "GIFFORD_ADAPTERPROBE_HARNESS"
)

func TestHarnessCapabilities(t *testing.T) {
	binary := strings.TrimSpace(os.Getenv(binaryEnv))
	harness := protocol.Harness(strings.TrimSpace(os.Getenv(harnessEnv)))
	require.NotEmpty(t, binary, "%s must name the harness binary", binaryEnv)
	require.True(t, harness.Valid(), "%s must name a supported harness", harnessEnv)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := adapters.ProbeHarness(ctx, adapters.ProbeOptions{Binary: binary, Harness: harness})
	if !result.Verifiable {
		t.Log("veredicto: sin canal verificable")
		if result.Version != "" {
			t.Logf("version observada: %s", result.Version)
		}
		return
	}

	capabilities, err := json.Marshal(result.Capabilities)
	require.NoError(t, err)
	t.Logf("veredicto: version verificada=%s capabilities=%s", result.Version, capabilities)
	t.Logf("canal: %s; adjuntable=%t; documentacion=%s", result.Channel, result.Attachable, result.Documentation)
	t.Run("version", func(t *testing.T) {
		require.NotEmpty(t, result.Version)
	})
	t.Run("structured_events", func(t *testing.T) {
		require.True(t, result.Capabilities.StructuredEvents)
	})
}
