//go:build linux

package proxies

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

type registrySink struct {
	submitted chan protocol.StateObservationReport
}

func (s registrySink) Submit(report protocol.StateObservationReport) error {
	s.submitted <- report
	return nil
}

// listenObservingProxy answers the link's greeting and then reports one
// state observation, which agentsock.Server does not send yet.
func listenObservingProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
	report protocol.StateObservationReport,
) <-chan error {
	t.Helper()
	directory := filepath.Join(runtimeDir, "agents")
	require.NoError(t, os.MkdirAll(directory, 0o700))
	require.NoError(t, os.Chmod(directory, 0o700))
	path := agentsock.SocketPath(runtimeDir, registration.Manifest.AgentID)
	address, err := net.ResolveUnixAddr("unix", path)
	require.NoError(t, err)
	listener, err := net.ListenUnix("unix", address)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	greeting, err := json.Marshal(registration)
	require.NoError(t, err)
	body, err := json.Marshal(report)
	require.NoError(t, err)
	served := make(chan error, 1)
	done := make(chan struct{})
	var accepted *net.UnixConn
	go func() {
		defer close(done)
		conn, err := listener.AcceptUnix()
		if err != nil {
			served <- err
			return
		}
		accepted = conn
		served <- serveObservation(conn, greeting, body)
	}()
	// The registry closes first, which ends a connection still being served.
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		if accepted != nil {
			_ = accepted.Close()
		}
	})
	return served
}

func serveObservation(conn *net.UnixConn, greeting, body []byte) error {
	if err := agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgent, FrameID: 1, SentAt: registryNow, Body: greeting,
	}); err != nil {
		return err
	}
	if _, err := agentsock.ReadFrame(conn); err != nil {
		return err
	}
	go func() { _, _ = io.Copy(io.Discard, conn) }()
	return agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpReportStateObservation, FrameID: 2, SentAt: registryNow, Body: body,
	})
}

func TestRegistryHandsEveryLinkItsObservationSink(t *testing.T) {
	t.Parallel()
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	report := protocol.StateObservationReport{
		AgentID: registryAgentID, SessionID: registrySessionID,
		ObservationID: "observation-1", ObservedAt: registryNow,
		Harness: protocol.HarnessCodex, HarnessVersion: registration.Manifest.HarnessVersion,
		ObservationGeneration: 1, InputGuardVersion: 1,
		ScreenState: protocol.StateObservationScreenWithheld, ScreenRows: []string{},
		NativeEvents: []protocol.StateObservationEvent{},
	}
	served := listenObservingProxy(t, runtimeDir, registration, report)
	clock := newRegistryClock()
	store, err := agentlink.NewInputGuardStore(db, clock)
	require.NoError(t, err)
	sink := registrySink{submitted: make(chan protocol.StateObservationReport, 1)}
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Writer: store, Clock: clock,
		Observations: sink, ReconcileEvery: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the link did not take the greeting")
	}
	select {
	case got := <-sink.submitted:
		require.Equal(t, report, got)
	case <-time.After(5 * time.Second):
		t.Fatal("the registry's link did not hand over the observation")
	}
}
