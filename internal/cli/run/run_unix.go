//go:build unix

package run

import (
	"context"
	"fmt"
	"os"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/session"
)

// discardSink drops session events: gifford run is the transparent proxy,
// so only the recording stays behind for later passes to read.
type discardSink struct{}

// Emit discards one session event.
func (discardSink) Emit(session.Event) {}

// runSession wraps the harness in the session wrapper: the screen is
// reconstructed, the output recorded and the keys parsed with the
// --harness profile (generic when empty). The recording lands under the
// data dir's recordings, named after a generated session id.
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
	id := fmt.Sprintf("run-%d-%d", os.Getpid(), clock.System{}.Now().UnixNano())
	s, err := session.NewSession(session.Options{
		Command:      p.Command,
		In:           p.In,
		Out:          p.Out,
		SessionID:    id,
		Profile:      profile,
		Sink:         discardSink{},
		Notice:       p.Notice,
		DaemonSocket: daemonSocket(),
	})
	if err != nil {
		return proxy.Exit{}, fmt.Errorf("gifford run: %w", err)
	}
	exit, err := s.Run(ctx)
	if err != nil {
		return exit, fmt.Errorf("gifford run: %w", err)
	}
	return exit, nil
}
