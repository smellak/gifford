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

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/endpoint"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/session"
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

// openRegisteredAgentSession registers an already selected manifest. Native
// adapter startup uses it after the handshake; other platforms pass their
// generic registration through the same path.
func openRegisteredAgentSession(ctx context.Context, notice io.Writer, reg protocol.RegisterAgent, runtimeDir string) *endpoint.SocketHub {
	if runtimeDir == "" {
		agentNotice(notice, fmt.Errorf("resolve runtime dir: %w", errNoRuntimeDir))
		return nil
	}
	if err := reg.Validate(); err != nil {
		agentNotice(notice, fmt.Errorf("register_agent: %w", err))
		return nil
	}
	hub, err := endpoint.DialHub(ctx, hubSocketPath(runtimeDir))
	if err != nil {
		agentNotice(notice, err)
		return nil
	}
	raw, err := hub.Call(ctx, "register_agent", reg)
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

// agentRegistration is the register_agent of this run: the manifest of
// a generic PTY agent behind the Proxy, and this process. The hub
// registration and the agent socket's greeting send the same one.
func agentRegistration(agent, harness, version string) (protocol.RegisterAgent, error) {
	manifest := adapters.GenericManifest(protocol.AgentID(agent), protocol.Harness(harness), version)
	return registrationForManifest(manifest)
}

// registrationForManifest binds one verified manifest to this process. The
// same value greets the agent socket and registers with the Hub.
func registrationForManifest(manifest protocol.CapabilityManifest) (protocol.RegisterAgent, error) {
	if err := manifest.Validate(); err != nil {
		return protocol.RegisterAgent{}, fmt.Errorf("bad agent manifest: %w", err)
	}
	process, err := registry.CurrentProcess()
	if err != nil {
		return protocol.RegisterAgent{}, fmt.Errorf("read current process: %w", err)
	}
	reg := protocol.RegisterAgent{Manifest: manifest, Process: &process}
	if err := reg.Validate(); err != nil {
		return protocol.RegisterAgent{}, fmt.Errorf("bad agent registration: %w", err)
	}
	return reg, nil
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
	// The probe is the single normalization boundary: only a
	// well-formed version leaves it, so the manifest and the
	// recording it feeds always satisfy the reader. Anything else
	// is unknown, never a truncation that could split UTF-8.
	return normalizeVersion(strings.TrimSpace(first))
}

// normalizeVersion keeps a measured version that satisfies the
// session contract (1-64 printable runes) and maps anything else to
// unknown.
func normalizeVersion(version string) string {
	if !session.ValidHarnessVersion(version) {
		return unknownVersion
	}
	return version
}
