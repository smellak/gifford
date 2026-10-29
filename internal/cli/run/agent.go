package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// hubSocketName is hub.sock in the runtime directory: the Hub link
// gifford run registers through.
const hubSocketName = "hub.sock"

// hubSocketPath resolves hub.sock under the runtime directory.
func hubSocketPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, hubSocketName)
}

// unknownVersion is the harness version when the probe cannot answer.
const unknownVersion = "unknown"

// errNoRuntimeDir means the XDG runtime directory did not resolve:
// without it there is no hub.sock to dial.
var errNoRuntimeDir = errors.New("no runtime directory")

// versionProbeTimeout bounds the harness --version probe.
const versionProbeTimeout = 2 * time.Second

// openAgentSession dials hub.sock and calls register_agent for agent,
// holding the connection open until the caller closes it: closing ends
// the session. Any failure prints one notice line and returns a nil
// connection without an error, so the harness always runs transparent
// and without a session.
func openAgentSession(ctx context.Context, notice io.Writer, agent, harness, version string, runtimeDir string) *endpoint.SocketHub {
	if runtimeDir == "" {
		agentNotice(notice, fmt.Errorf("resolve runtime dir: %w", errNoRuntimeDir))
		return nil
	}
	manifest := protocol.CapabilityManifest{
		SchemaVersion:      protocol.SchemaVersion,
		AgentID:            protocol.AgentID(agent),
		Harness:            protocol.Harness(harness),
		HarnessVersion:     version,
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy,
		Deliverable:        true,
		Capabilities:       protocol.Capabilities{MCP: true, PTYCapture: true},
	}
	if err := manifest.Validate(); err != nil {
		agentNotice(notice, fmt.Errorf("bad agent manifest: %w", err))
		return nil
	}
	process, err := registry.CurrentProcess()
	if err != nil {
		agentNotice(notice, fmt.Errorf("read current process: %w", err))
		return nil
	}
	hub, err := endpoint.DialHub(ctx, hubSocketPath(runtimeDir))
	if err != nil {
		agentNotice(notice, err)
		return nil
	}
	raw, err := hub.Call(ctx, "register_agent", protocol.RegisterAgent{
		Manifest: manifest,
		Process:  &process,
	})
	if err != nil {
		agentNotice(notice, err)
		_ = hub.Close()
		return nil
	}
	var result protocol.RegisterAgentResult
	if err := json.Unmarshal(raw, &result); err != nil {
		agentNotice(notice, fmt.Errorf("register_agent: %w", err))
		_ = hub.Close()
		return nil
	}
	if err := result.Validate(); err != nil {
		agentNotice(notice, fmt.Errorf("register_agent: %w", err))
		_ = hub.Close()
		return nil
	}
	return hub
}

// agentNotice prints the one stderr line saying the run continues
// without a session, in the style of the daemon notice.
func agentNotice(notice io.Writer, err error) {
	_, _ = fmt.Fprintf(notice, "gifford: no hub session for this run: %v\n", err)
}

// probeHarnessVersion runs the harness command with --version and
// returns its first output line, or "unknown" when it fails, prints
// nothing useful, or takes longer than versionProbeTimeout. The output
// goes to a file, not a pipe, so grandchildren outliving the timeout
// cannot hold the probe open: the direct child's death ends the wait.
func probeHarnessVersion(ctx context.Context, command []string) string {
	if len(command) == 0 {
		return unknownVersion
	}
	ctx, cancel := context.WithTimeout(ctx, versionProbeTimeout)
	defer cancel()
	tmp, err := os.CreateTemp("", "gifford-version-*")
	if err != nil {
		return unknownVersion
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	//nolint:gosec // the probe runs the user's own harness command with a
	// fixed --version argument under a timeout, never through a shell.
	cmd := exec.CommandContext(ctx, command[0], "--version")
	cmd.Stdout = tmp
	if runErr := cmd.Run(); runErr != nil {
		_ = tmp.Close()
		return unknownVersion
	}
	_ = tmp.Close()
	raw, err := os.ReadFile(tmpName)
	if err != nil {
		return unknownVersion
	}
	first, _, _ := strings.Cut(string(raw), "\n")
	version := strings.TrimSpace(first)
	if version == "" {
		return unknownVersion
	}
	if len(version) > 64 {
		version = version[:64]
	}
	return version
}
