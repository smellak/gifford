//go:build unix

package run

import (
	"context"
	"fmt"
	"io"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
)

// agentSocketRun is what the agent socket of a --agent run needs: the exact
// registration selected before startup, and the runtime directory.
// observations says whether config.yaml enables state analysis: only then
// do the session's state observations carry its screen. Without it they
// still carry the detector verdict, never the screen.
type agentSocketRun struct {
	registration protocol.RegisterAgent
	runtimeDir   string
	session      *registeredAgentSession
	events       agentsock.EventSource
	durable      agentsock.DurableEvents
	observations bool
}

// agentSession is the session an agent socket serves: the guard it
// reports, the deliveries it runs and the state observations it cuts.
type agentSession interface {
	agentsock.Guard
	agentsock.ObservationSource
	SocketDelivery() agentsock.Delivery
}

// openAgentSocket listens on the agent socket of a --agent run, the
// Core's link to this session, at <runtime>/agents/<agent>.sock. It
// greets with the same register_agent as the hub registration, reports
// the session's input guard and runs the Core's deliveries on it. Any
// failure prints one notice line and returns nil: the harness always
// runs, transparent, without a socket.
func openAgentSocket(ctx context.Context, notice io.Writer, a agentSocketRun, s agentSession) *agentsock.Server {
	if a.runtimeDir == "" {
		socketNotice(notice, fmt.Errorf("resolve runtime dir: %w", errNoRuntimeDir))
		return nil
	}
	// A failed Hub registration has no session identity to authenticate. Its
	// one no-session notice was already printed; the harness stays transparent
	// and no unbound socket is exposed.
	if a.session == nil {
		return nil
	}
	opts := agentsock.Options{
		RuntimeDir: a.runtimeDir, Register: a.registration, SessionID: a.session.sessionID,
		Guard: s, Delivery: s.SocketDelivery(), Events: a.events, OnDisconnect: a.session.disconnected,
		Durable: a.durable,
	}
	// Without state analysis the observations withhold the screen: the
	// Core only gets the detector verdict, which carries no pane text.
	opts.Observations = s
	if !a.observations {
		opts.Observations = verdictOnly{s}
	}
	srv, err := agentsock.Listen(ctx, opts)
	if err != nil {
		socketNotice(notice, err)
		return nil
	}
	return srv
}

// socketNotice prints the one stderr line saying the run continues
// without an agent socket, in the style of the hub notice.
func socketNotice(notice io.Writer, err error) {
	_, _ = fmt.Fprintf(notice, "gifford: no agent socket for this run: %v\n", err)
}

// verdictOnly passes a session's observations on with their screen withheld,
// for a run without state analysis: the detector verdict, the activity
// counts and the generations stay; screen rows, width and cursor go. A cut
// without a verdict is not sent at all, so an unmeasured harness sends
// nothing, as before; the Core lets a verdict it stops receiving go stale.
type verdictOnly struct{ source agentsock.ObservationSource }

func (v verdictOnly) Observe() (protocol.StateObservationReport, bool) {
	r, ok := v.source.Observe()
	if !ok || r.Detector == nil {
		return protocol.StateObservationReport{}, false
	}
	r.ScreenState = protocol.StateObservationScreenWithheld
	r.ScreenRows = []string{}
	r.Cols = 0
	r.Cursor = nil
	return r, true
}

func (v verdictOnly) ObservationChanged() <-chan struct{} { return v.source.ObservationChanged() }
