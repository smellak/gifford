//go:build linux

package proxies

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func TestRegistryResumeBetweenReconcilesStartsFreshClient(t *testing.T) {
	db := registryDB(t)
	runtimeDir := registryRuntime(t)
	registration := registryRegistration(t)
	insertRegistrySession(t, db, registration)
	const bootID = "test-live-boot"
	_, err := db.ExecContext(t.Context(), `UPDATE sessions SET boot_id = ? WHERE session_id = ?`,
		bootID, registrySessionID)
	require.NoError(t, err)
	guard := listenRegistryProxy(t, runtimeDir, registration)
	managed, err := Start(t.Context(), Options{
		DB: db, RuntimeDir: runtimeDir, Clock: agentlink.SystemClock{},
		ReconcileEvery: time.Hour,
		sessionLiveness: &registrySessionLiveness{
			process: *registration.Process, bootID: bootID, live: true,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, managed.Close()) })
	require.Eventually(t, func() bool {
		snap, ok := managed.Snapshot(registryAgentID)
		return ok && snap.State == agentlink.StateUp && snap.HasReport
	}, 5*time.Second, time.Millisecond)
	first, ok := managed.Client(registryAgentID)
	require.True(t, ok)
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = ?, updated_at = ?
		WHERE session_id = ?`, protocol.SessionStateDetached,
		eventstore.FormatTime(registryNow.Add(time.Second)), registrySessionID)
	require.NoError(t, err)
	guard.setVersion(7, 11)
	_, err = db.ExecContext(t.Context(), `UPDATE sessions SET state = ?, updated_at = ?
		WHERE session_id = ?`, protocol.SessionStateConnected,
		eventstore.FormatTime(registryNow.Add(2*time.Second)), registrySessionID)
	require.NoError(t, err)
	require.NoError(t, managed.Reconcile(t.Context()))
	second, ok := managed.Client(registryAgentID)
	require.True(t, ok)
	require.False(t, first == second, //nolint:testifylint // Avoid reflective output racing with live clients.
		"a resumed lifecycle must not retain its prior handshake or report")
}
