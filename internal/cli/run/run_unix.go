//go:build unix

package run

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/adapters"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/session"
	"github.com/smellak/gifford/internal/xdg"
)

// runSession wraps the harness in the session wrapper: the screen is
// reconstructed, the output recorded and the keys parsed with the
// --harness profile (generic when empty). Two files land under the data
// dir's recordings, named after a generated session id: the raw output
// and its metadata events, which together replay the session.
func runSession(ctx context.Context, p sessionParams) (proxy.Exit, error) {
	name := p.Harness
	if name == "" {
		name = genericHarness
	}
	lookup, err := resolveRunProfileLookup(p)
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	var profile profiles.Profile
	if p.Agent == "" {
		profile, err = selectRunProfile(lookup, name, "", false, p.Notice)
		if err != nil {
			return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
		}
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: data dir: %w", err)
	}
	if _, err := xdg.EnsureAll(dirs); err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: data dir: %w", err)
	}
	// The harness version is probed once, only for --agent: it feeds the
	// generic fallback and the recording, and no probe runs without --agent.
	harnessVersion := session.UnknownHarnessVersion
	var adapter *adapterRun
	var registered *registeredAgentSession
	var registration protocol.RegisterAgent
	var observations bool
	var nodeID protocol.NodeID
	if p.Agent != "" {
		prepared, err := prepareAgentRun(ctx, p, name, dirs.Runtime, dirs.Config)
		if err != nil {
			return proxy.Exit{}, err
		}
		defer prepared.close()
		harnessVersion = prepared.harnessVersion
		registration = prepared.registration
		registered = prepared.session
		adapter = prepared.adapter
		observations = prepared.observations
		nodeID = prepared.nodeID
		profile, err = selectRunProfile(lookup, name, harnessVersion, true, p.Notice)
		if err != nil {
			return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
		}
	}
	id := fmt.Sprintf("run-%d-%d", os.Getpid(), clock.System{}.Now().UnixNano())
	p.Env = setEnvironmentVariable(p.Env, captureSessionEnv, id)
	var sock *agentSocketRun
	if p.Agent != "" {
		sock = &agentSocketRun{
			registration: registration, runtimeDir: dirs.Runtime,
			session: registered, observations: observations,
		}
		if adapter != nil {
			sock.events = adapter
		}
	}
	var native *nativeEventRun
	if registered != nil && nodeID.Valid() && registration.Process != nil {
		native = &nativeEventRun{
			runtimeDir: dirs.Runtime, registration: registration,
			session: registered, nodeID: nodeID, capabilities: adapter,
		}
	}
	return runLogged(ctx, p, dirs.Data, id, profile, harnessVersion, sock, adapter, native)
}

func resolveRunProfileLookup(p sessionParams) (runProfileLookup, error) {
	if p.profileLookup != nil {
		return p.profileLookup, nil
	}
	ps, err := profiles.Load()
	if err != nil {
		return nil, fmt.Errorf("load profiles: %w", err)
	}
	return ps.Profile, nil
}

// selectRunProfile derives only the version-range value from the observed
// line. The full line still belongs to the manifest and recording. Without an
// agent there is no observation, preserving the transparent run path.
func selectRunProfile(
	lookup func(string, string) (profiles.Profile, error),
	name, observedVersion string,
	agent bool,
	notice io.Writer,
) (profiles.Profile, error) {
	version := ""
	if agent {
		version = profiles.ParseVersion(observedVersion)
	}
	unknown := agent && version == ""
	profile, err := lookup(name, version)
	if err != nil {
		return profiles.Profile{}, err
	}
	if unknown && name != genericHarness {
		profile, err = lookup(genericHarness, version)
		if err != nil {
			return profiles.Profile{}, err
		}
	}
	if unknown {
		profile.Measured = false
	}
	fallback := agent && name != genericHarness && (unknown || profile.Harness == genericHarness)
	if !fallback {
		return profile, nil
	}

	// A version mismatch is not measurement evidence, even if a future generic
	// profile is measured. Draft extraction and automation therefore stay off.
	profile.Measured = false
	shownVersion := version
	if shownVersion == "" {
		shownVersion = unknownVersion
	}
	if notice != nil {
		_, _ = fmt.Fprintf(notice,
			"gifford: profile %s version %q sin medir; using generic\n", name, shownVersion)
	}
	return profile, nil
}

type preparedAgentRun struct {
	harnessVersion string
	registration   protocol.RegisterAgent
	adapter        *adapterRun
	session        *registeredAgentSession
	observations   bool
	nodeID         protocol.NodeID
}

var errCapabilityAuditUnavailable = errors.New("capability audit unavailable")

func prepareAgentRun(ctx context.Context, p sessionParams, harness, runtimeDir, configDir string) (*preparedAgentRun, error) {
	version := probeHarnessVersion(ctx, p.Command)
	fallback, err := agentRegistration(p.Agent, harness, version)
	if err != nil {
		return nil, fmt.Errorf("gifford run: %w", err)
	}
	registration := fallback
	client, manifest := connectConfiguredAdapter(ctx, p.Notice, runtimeDir, fallback.Manifest)
	nodeID := configuredNodeID(configDir)
	target := fallback.Manifest
	target.Capabilities.StructuredEvents = true
	target.Capabilities.ToolEvents = true
	target.Capabilities.ApprovalEvents = true
	if client != nil {
		if !nodeID.Valid() {
			_ = client.Close()
			client = nil
			adapterNotice(p.Notice, errCapabilityAuditUnavailable)
		} else {
			target = manifest
			target.Capabilities.StructuredEvents = true
			target.Capabilities.ToolEvents = true
			target.Capabilities.ApprovalEvents = true
			initial := target
			initial.Capabilities = adapters.NativeEventCapabilities(
				target.Capabilities, fallback.Manifest.Capabilities,
			)
			registration.Manifest = initial
			version = manifest.HarnessVersion
		}
	}
	// Register before the child starts and hold the Hub connection until the
	// session ends. A nil connection runs transparent, no session.
	registered := openRegisteredAgentSession(ctx, p.Notice, registration, runtimeDir)
	prepared := &preparedAgentRun{
		harnessVersion: version,
		registration:   registration,
		session:        registered,
		observations:   stateAnalysisEnabled(configDir),
		nodeID:         nodeID,
	}
	if registered != nil && nodeID.Valid() {
		prepared.adapter = newAdapterRun(
			client, nodeID, registered.sessionID, target, registration.Manifest, clock.System{},
		)
	} else if client != nil {
		_ = client.Close()
	}
	return prepared, nil
}

func configuredNodeID(configDir string) protocol.NodeID {
	cfg, err := config.Load(config.DefaultPath(configDir))
	if err != nil || !cfg.NodeID.Valid() {
		return ""
	}
	return cfg.NodeID
}

// stateAnalysisEnabled reports whether config.yaml loads and enables state
// analysis. Only then does the agent socket send state observations. A
// config.yaml that is missing, does not load or has no state_analysis
// leaves them off and changes nothing else in the run.
func stateAnalysisEnabled(configDir string) bool {
	cfg, err := config.Load(config.DefaultPath(configDir))
	return err == nil && cfg.StateAnalysis != nil
}

func adapterSocketPath(runtimeDir string, agent protocol.AgentID) string {
	return filepath.Join(runtimeDir, "adapters", string(agent)+".sock")
}

func connectConfiguredAdapter(ctx context.Context, notice io.Writer, runtimeDir string, fallback protocol.CapabilityManifest) (*adapters.Client, protocol.CapabilityManifest) {
	path := adapterSocketPath(runtimeDir, fallback.AgentID)
	if _, err := os.Lstat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fallback
		}
		adapterNotice(notice, err)
		return nil, fallback
	}
	client, manifest, err := adapters.Connect(ctx, adapters.Options{
		SocketPath: path,
		AgentID:    fallback.AgentID,
		Harness:    fallback.Harness,
		Fallback:   fallback,
	})
	if err != nil {
		adapterNotice(notice, err)
		return nil, fallback
	}
	return client, manifest
}

func (r *preparedAgentRun) close() {
	if r.adapter != nil && r.adapter.client != nil {
		_ = r.adapter.client.Close()
	}
	if r.session != nil {
		_ = r.session.Close()
	}
}

// runLogged runs one session with its events written beside the
// recording. A failing events file never changes the harness exit: it is
// reported on the notice writer, and that session will not replay. With
// sock, the agent socket listens from before the child starts until the
// session ends.
func runLogged(ctx context.Context, p sessionParams, dataDir, id string, profile profiles.Profile, harnessVersion string, sock *agentSocketRun, adapter *adapterRun, native *nativeEventRun) (proxy.Exit, error) {
	log, err := session.CreateEventLog(dataDir, id, session.EventLogOptions{})
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	if sock != nil {
		p.Env = withoutEnvironmentVariable(p.Env, nativeevents.SocketEnvironment)
	}
	var nativeServer *nativeevents.Server
	if native != nil {
		nativeServer = openNativeEventEntry(ctx, p.Notice, *native)
		if nativeServer != nil {
			p.Env = setEnvironmentVariable(p.Env, nativeevents.SocketEnvironment, nativeServer.Path())
			defer func() { _ = nativeServer.Close() }()
			if sock != nil {
				sock.durable = nativeServer.Queue()
			}
		}
	}
	s, err := session.NewSession(session.Options{
		Command:        p.Command,
		Env:            p.Env,
		In:             p.In,
		Out:            p.Out,
		DataDir:        dataDir,
		SessionID:      id,
		Profile:        profile,
		Sink:           log,
		Notice:         p.Notice,
		DaemonSocket:   daemonSocket(),
		HarnessVersion: harnessVersion,
	})
	if err != nil {
		_ = log.Close()
		_ = os.Remove(filepath.Join(session.RecordingsDir(dataDir), id+session.EventsSuffix))
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	var closeAgentSocket func()
	if sock != nil {
		var served agentSession = s
		if sock.observations && nativeServer != nil {
			served = newNativeObservationSession(ctx, s, nativeServer.Queue())
		}
		srv := openAgentSocket(ctx, p.Notice, *sock, served)
		closeAgentSocket = func() {
			// Stop recovery before closing the socket: closing a live Core
			// connection is otherwise indistinguishable from node loss.
			if sock.session != nil {
				_ = sock.session.Close()
			}
			if srv != nil {
				_ = srv.Close()
			}
		}
		defer closeAgentSocket()
	}
	var adapterDone <-chan struct{}
	if adapter != nil {
		adapterDone = adapter.start()
	}
	exit, runErr := s.Run(ctx)
	if closeAgentSocket != nil {
		// The retry lifetime is exactly the harness lifetime, not the later
		// event-log and adapter cleanup.
		closeAgentSocket()
	}
	if adapter != nil {
		adapter.stop(adapterDone)
	}
	if err := log.Close(); err != nil {
		_, _ = fmt.Fprintf(p.Notice, "gifford run: %v; this session will not replay\n", err)
	} else if n := log.Dropped(); n > 0 {
		_, _ = fmt.Fprintf(p.Notice, "gifford run: %d session events dropped; this session will not replay\n", n)
	}
	if runErr != nil {
		return exit, fmt.Errorf("gifford run: %w", runErr)
	}
	return exit, nil
}

func adapterNotice(notice io.Writer, err error) {
	_, _ = fmt.Fprintf(notice, "gifford: native adapter unavailable for this run: %v; using generic_pty\n", err)
}
