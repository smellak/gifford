//go:build unix

package run

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/protocol"
)

// stateAnalysisYAML is a config.yaml that enables state analysis. The run
// reads only that it loads with the key: nothing opens the gateway config.
var stateAnalysisYAML = "state_analysis:\n" +
	"  gateway_config: /synthetic/gateway.yaml\n" +
	"  profile_revision: analyst-profile-v1\n" +
	"  profile_sha256: " + strings.Repeat("a", 64) + "\n" +
	"  calibration_revision: calibration-v1\n" +
	"  calibration_sha256: " + strings.Repeat("a", 64) + "\n" +
	"  temperature: 1.25\n" +
	"  idle_threshold: 0.9\n"

// observationID is an observation id: the session's root and a generation.
var observationID = regexp.MustCompile(`^o-[0-9a-f]{32}\.[1-9][0-9]*$`)

// startConfiguredRun is startSocketRun on the embedded profiles, with
// configYAML as config.yaml, or with no config.yaml when it is empty. The
// harness waits for one line, then exits.
func startConfiguredRun(t *testing.T, configYAML string) *socketRun {
	t.Helper()
	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)
	if configYAML != "" {
		dir := filepath.Join(configHome, "gifford")
		require.NoError(t, os.MkdirAll(dir, 0o700))
		require.NoError(t, os.WriteFile(config.DefaultPath(dir), []byte(configYAML), 0o600))
	}
	script := filepath.Join(t.TempDir(), "fakeharness")
	writeTestExecutable(t, script, []byte("#!/bin/sh\n"+
		"if [ \"$1\" = \"--version\" ]; then echo \"fakeharness 1.2.3\"; exit 0; fi\n"+
		"stty -echo\necho ready\nIFS= read -r line\n"))
	return startSocketRun(t, nil, script)
}

// synced sends a resync and returns every frame the run wrote up to its
// answer, the answer last.
func (r *socketRun) synced() []protocol.AgentFrame {
	r.t.Helper()
	id := r.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: socketAgent})
	var frames []protocol.AgentFrame
	for {
		f, err := agentsock.ReadFrame(r.conn)
		require.NoError(r.t, err)
		frames = append(frames, f)
		if f.ReplyTo != nil && *f.ReplyTo == id {
			return frames
		}
	}
}

// nextObservation reads past the run's other frames to its next state
// observation.
func (r *socketRun) nextObservation() protocol.StateObservationReport {
	r.t.Helper()
	for {
		f, err := agentsock.ReadFrame(r.conn)
		require.NoError(r.t, err)
		if f.Op != protocol.AgentOpReportStateObservation {
			continue
		}
		require.Nil(r.t, f.ReplyTo)
		var report protocol.StateObservationReport
		require.NoError(r.t, json.Unmarshal(f.Body, &report))
		return report
	}
}

// TestRunObservesWithStateAnalysis runs --agent with a Core that
// announces it takes state observations. The run sends them only when
// config.yaml loads with state_analysis. With no config.yaml, one without
// the key, or one that does not load, the session is never the socket's
// source, and nothing else in the run changes.
//
// A source the socket has is cut as soon as its writer is free after the
// announcement: once the run answered three resyncs sent after it, with no
// observation among the frames, no first observation is coming. The
// subtests' names are short: the socket path holds them.
func TestRunObservesWithStateAnalysis(t *testing.T) {
	for name, tc := range map[string]struct {
		config   string
		observed bool
	}{
		"no config":  {},
		"no key":     {config: "schema_version: 1\n"},
		"not YAML":   {config: "state_analysis: [\n"},
		"incomplete": {config: "state_analysis:\n  temperature: 1.25\n"},
		"enabled":    {config: stateAnalysisYAML, observed: true},
	} {
		t.Run(name, func(t *testing.T) {
			r := startConfiguredRun(t, tc.config)
			r.send(protocol.AgentOpAnnounceSupport, 0, protocol.AgentSocketSupport{ReportStateObservation: true})
			if !tc.observed {
				for range 3 {
					for _, f := range r.synced() {
						require.NotEqual(t, protocol.AgentOpReportStateObservation, f.Op)
					}
				}
			} else {
				first := r.nextObservation()
				require.NoError(t, first.Validate())
				require.Equal(t, protocol.AgentID(socketAgent), first.AgentID)
				require.Equal(t, protocol.OpaqueID(socketSession), first.SessionID)
				require.Equal(t, r.registration.Manifest.Harness, first.Harness)
				require.Equal(t, r.registration.Manifest.HarnessVersion, first.HarnessVersion)
				require.Equal(t, int64(1), first.ObservationGeneration)
				require.Regexp(t, observationID, string(first.ObservationID))
				require.Equal(t, protocol.StateObservationScreenWithheld, first.ScreenState,
					"the embedded profiles prove no screen")
				require.Empty(t, first.ScreenRows)

				// A quiet session is cut again: the observations flow.
				next := r.nextObservation()
				require.NoError(t, next.Validate())
				require.Greater(t, next.ObservationGeneration, first.ObservationGeneration)
				require.NotEqual(t, first.ObservationID, next.ObservationID)
			}
			r.release()
			require.Equal(t,
				unmeasuredCodexDaemonNotice(),
				r.stderr.String(), "the run prints nothing more")
		})
	}
}
