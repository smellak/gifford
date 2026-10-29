//go:build unix

package run

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/profiles"
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
		name = "generic"
	}
	ps, err := profiles.Load()
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	profile, err := ps.Profile(name, "")
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	dirs, err := xdg.Resolve()
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: data dir: %w", err)
	}
	if _, err := xdg.EnsureAll(dirs); err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: data dir: %w", err)
	}
	if p.Agent != "" {
		// Register before the child starts and hold the hub
		// connection until the session ends: closing it ends the
		// session. A nil connection runs transparent, no session.
		if hub := openAgentSession(ctx, p.Notice, p.Agent, name, p.Command, dirs.Runtime); hub != nil {
			defer func() { _ = hub.Close() }()
		}
	}
	id := fmt.Sprintf("run-%d-%d", os.Getpid(), clock.System{}.Now().UnixNano())
	return runLogged(ctx, p, dirs.Data, id, profile)
}

// runLogged runs one session with its events written beside the
// recording. A failing events file never changes the harness exit: it is
// reported on the notice writer, and that session will not replay.
func runLogged(ctx context.Context, p sessionParams, dataDir, id string, profile profiles.Profile) (proxy.Exit, error) {
	log, err := session.CreateEventLog(dataDir, id, session.EventLogOptions{})
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	s, err := session.NewSession(session.Options{
		Command:      p.Command,
		In:           p.In,
		Out:          p.Out,
		DataDir:      dataDir,
		SessionID:    id,
		Profile:      profile,
		Sink:         log,
		Notice:       p.Notice,
		DaemonSocket: daemonSocket(),
	})
	if err != nil {
		_ = log.Close()
		_ = os.Remove(filepath.Join(session.RecordingsDir(dataDir), id+session.EventsSuffix))
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	exit, runErr := s.Run(ctx)
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
