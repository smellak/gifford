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
	"sync"
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

const (
	// registrationRetryInitial and registrationRetryLimit match the bounded
	// reconnect cadence of the node's agent link.
	registrationRetryInitial = 100 * time.Millisecond
	registrationRetryLimit   = 5 * time.Second
	// registrationHealthEvery detects an otherwise idle Hub transport loss
	// before an agent socket has connected, or while that link stays open.
	registrationHealthEvery  = 500 * time.Millisecond
	registrationProbeTimeout = time.Second
)

var errRegistrationIdentityChanged = errors.New("register_agent returned another session")

// registeredAgentSession owns the Hub transport for one run. A lost agent
// socket wakes its reconnect loop; the loop repeats the exact registration and
// accepts only the session identity established before the harness started.
type registeredAgentSession struct {
	ctx        context.Context
	cancel     context.CancelFunc
	notice     io.Writer
	reg        protocol.RegisterAgent
	runtimeDir string
	sessionID  protocol.OpaqueID

	check     chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	mu     sync.Mutex
	hub    *endpoint.SocketHub
	closed bool
}

// openRegisteredAgentSession registers an already selected manifest. Native
// adapter startup uses it after the handshake; other platforms pass their
// generic registration through the same path.
func openRegisteredAgentSession(ctx context.Context, notice io.Writer, reg protocol.RegisterAgent, runtimeDir string) *registeredAgentSession {
	if runtimeDir == "" {
		agentNotice(notice, fmt.Errorf("resolve runtime dir: %w", errNoRuntimeDir))
		return nil
	}
	if err := reg.Validate(); err != nil {
		agentNotice(notice, fmt.Errorf("register_agent: %w", err))
		return nil
	}
	hub, result, _, err := registerAgentOnce(ctx, reg, runtimeDir)
	if err != nil {
		agentNotice(notice, err)
		return nil
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &registeredAgentSession{
		ctx: sessionCtx, cancel: cancel, notice: notice, reg: reg,
		runtimeDir: runtimeDir, sessionID: result.SessionID, hub: hub,
		check: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go session.run()
	return session
}

// registerAgentOnce opens one Hub transport and performs the registration.
// retry is true only for transport failures; Hub rejections and malformed or
// mismatched results are definitive for this run.
func registerAgentOnce(
	ctx context.Context,
	reg protocol.RegisterAgent,
	runtimeDir string,
) (hub *endpoint.SocketHub, result protocol.RegisterAgentResult, retry bool, err error) {
	hub, err = endpoint.DialHub(ctx, hubSocketPath(runtimeDir))
	if err != nil {
		return nil, result, true, fmt.Errorf("register_agent: %w", err)
	}
	raw, err := hub.Call(ctx, "register_agent", reg)
	if err != nil {
		_ = hub.Close()
		var rejection *endpoint.HubError
		return nil, result, !errors.As(err, &rejection), fmt.Errorf("register_agent: %w", err)
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		_ = hub.Close()
		return nil, result, false, fmt.Errorf("register_agent: %w", err)
	}
	if err := result.Validate(); err != nil {
		_ = hub.Close()
		return nil, result, false, fmt.Errorf("register_agent: %w", err)
	}
	if result.AgentID != reg.Manifest.AgentID {
		_ = hub.Close()
		return nil, result, false, errRegistrationIdentityChanged
	}
	return hub, result, false, nil
}

// disconnected reports that the node-side link left the agent socket. It only
// advances a Hub health check: a healthy owner must not be replaced merely
// because the independent agent link reconnects.
func (s *registeredAgentSession) disconnected() {
	select {
	case <-s.ctx.Done():
	case s.check <- struct{}{}:
	default:
	}
}

func (s *registeredAgentSession) run() {
	defer close(s.done)
	defer s.dropHub()
	monitor := time.NewTimer(registrationHealthEvery)
	defer monitor.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.check:
		case <-monitor.C:
		}
		if s.ctx.Err() != nil {
			return
		}
		healthy, err := s.hubHealthy()
		if err != nil {
			agentNotice(s.notice, err)
			return
		}
		if !healthy && !s.recoverHub() {
			return
		}
		resetRegistrationMonitor(monitor)
	}
}

// hubHealthy makes a bounded call on the existing transport. No successful
// probe mutates Hub lifecycle state, so an agent-link replacement cannot cause
// a DETACHED/CONNECTED loop.
func (s *registeredAgentSession) hubHealthy() (bool, error) {
	s.mu.Lock()
	hub := s.hub
	s.mu.Unlock()
	if hub == nil {
		return false, nil
	}
	probeCtx, cancel := context.WithTimeout(s.ctx, registrationProbeTimeout)
	defer cancel()
	agent, err := hub.WhoAmI(probeCtx)
	if err != nil {
		return false, nil
	}
	if agent != s.reg.Manifest.AgentID {
		return false, errRegistrationIdentityChanged
	}
	return true, nil
}

// recoverHub replaces only a transport already proven unavailable, then
// repeats the exact registration until it succeeds, is cancelled or receives
// a definitive answer.
func (s *registeredAgentSession) recoverHub() bool {
	s.dropHub()
	delay := registrationRetryInitial
	for {
		if !waitRegistrationRetry(s.ctx, delay) {
			return false
		}
		hub, result, retry, err := registerAgentOnce(s.ctx, s.reg, s.runtimeDir)
		if err != nil {
			if s.ctx.Err() != nil {
				return false
			}
			if !retry {
				agentNotice(s.notice, err)
				return false
			}
			delay = nextRegistrationRetry(delay)
			continue
		}
		if result.SessionID != s.sessionID {
			_ = hub.Close()
			agentNotice(s.notice, errRegistrationIdentityChanged)
			return false
		}
		return s.installHub(hub)
	}
}

func resetRegistrationMonitor(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(registrationHealthEvery)
}

func waitRegistrationRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextRegistrationRetry(delay time.Duration) time.Duration {
	delay *= 2
	if delay > registrationRetryLimit {
		return registrationRetryLimit
	}
	return delay
}

// installHub publishes a recovered transport unless the run is already
// closing. It closes any transport it replaces outside the lock.
func (s *registeredAgentSession) installHub(hub *endpoint.SocketHub) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = hub.Close()
		return false
	}
	old := s.hub
	s.hub = hub
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return true
}

func (s *registeredAgentSession) dropHub() {
	s.mu.Lock()
	hub := s.hub
	s.hub = nil
	s.mu.Unlock()
	if hub != nil {
		_ = hub.Close()
	}
}

// Close stops every pending retry and releases the active Hub transport.
func (s *registeredAgentSession) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		hub := s.hub
		s.hub = nil
		s.mu.Unlock()
		s.cancel()
		if hub != nil {
			_ = hub.Close()
		}
		<-s.done
	})
	return nil
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
