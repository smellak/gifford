//go:build fedprobe

// Package fedprobe_test runs the opt-in live federation boundary suite.
package fedprobe_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/federation"
)

const (
	endpointEnv    = "GIFFORD_FEDPROBE_ENDPOINT"
	fingerprintEnv = "GIFFORD_FEDPROBE_FINGERPRINT"
	dataDirEnv     = "GIFFORD_FEDPROBE_DATA_DIR"
	timeoutEnv     = "GIFFORD_FEDPROBE_TIMEOUT"
)

func TestFederationBoundary(t *testing.T) {
	endpoint := requireEnvironment(t, endpointEnv)
	fingerprint := requireEnvironment(t, fingerprintEnv)
	dataDir := requireEnvironment(t, dataDirEnv)
	timeout := probeTimeout(t)
	var output bytes.Buffer
	err := federation.RunProbe(t.Context(), federation.ProbeOptions{
		Endpoint: endpoint, Fingerprint: fingerprint, DataDir: dataDir,
		Timeout: timeout, Output: &output,
	})
	if err != nil && !errors.Is(err, federation.ErrProbeFailed) {
		t.Fatalf("initialize live federation probe: %v", err)
	}

	results := parseResults(t, output.String())
	require.Len(t, results, 7, "probe must report the complete boundary matrix")
	for _, name := range []string{
		"tls-1.3",
		"tls-1.2-rejected",
		"no-client-certificate",
		"unenrolled-client-certificate",
		"forged-origin",
		"invalid-signature",
		"exact-replay",
	} {
		t.Run(name, func(t *testing.T) {
			result, ok := results[name]
			require.True(t, ok, "probe did not report this case")
			require.Equal(t, "PASS", result, "live probe reported %s", result)
		})
	}
}

func requireEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func probeTimeout(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv(timeoutEnv)
	if raw == "" {
		return 0
	}
	timeout, err := time.ParseDuration(raw)
	require.NoError(t, err, "%s must be a Go duration", timeoutEnv)
	require.Positive(t, timeout, "%s must be positive", timeoutEnv)
	return timeout
}

func parseResults(t *testing.T, raw string) map[string]string {
	t.Helper()
	results := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		status, rest, ok := strings.Cut(line, " ")
		require.True(t, ok, "malformed probe result")
		name, detail, _ := strings.Cut(rest, ": ")
		require.NotEmpty(t, name, "probe result has no case name")
		require.NotContains(t, results, name, "probe reported a duplicate case")
		if status == "FAIL" {
			results[name] = fmt.Sprintf("FAIL: %s", detail)
			continue
		}
		require.Equal(t, "PASS", status, "probe result has an unknown status")
		results[name] = status
	}
	return results
}
