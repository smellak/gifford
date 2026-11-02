package tmuxattach

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

type lifecycleRegistryStub struct {
	active      []tmuxinstance.Instance
	invalidated []tmuxinstance.Instance
	sweepErr    error
	listErr     error
	bound       map[protocol.ULID]protocol.OpaqueID
}

func (r *lifecycleRegistryStub) Sweep(context.Context) ([]tmuxinstance.Instance, error) {
	return append([]tmuxinstance.Instance(nil), r.invalidated...), r.sweepErr
}

func (r *lifecycleRegistryStub) ListActive(context.Context) ([]tmuxinstance.Instance, error) {
	return append([]tmuxinstance.Instance(nil), r.active...), r.listErr
}

func (r *lifecycleRegistryStub) BindSession(
	_ context.Context,
	instance protocol.ULID,
	session protocol.OpaqueID,
) error {
	if r.bound == nil {
		r.bound = make(map[protocol.ULID]protocol.OpaqueID)
	}
	r.bound[instance] = session
	return nil
}

func lifecycleInstance(id protocol.ULID, agent protocol.AgentID) tmuxinstance.Instance {
	return tmuxinstance.Instance{
		ID: id, AgentID: agent, BootID: "boot-one",
		Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
		HarnessPID: 4242, HarnessStart: 100, AgentUID: 1001, AgentGID: 1001,
		State: tmuxinstance.StateActive,
	}
}

func TestReconcilerBindsActiveInstanceWithMCPFalse(t *testing.T) {
	t.Parallel()
	sessions, db, _ := openSessionStore(t)
	instance := lifecycleInstance(testInstanceOne, testSessionAgent)
	registry := &lifecycleRegistryStub{active: []tmuxinstance.Instance{instance}}
	reconciler, err := NewReconciler(registry, sessions)
	require.NoError(t, err)

	changed, err := reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	wantSession := protocol.OpaqueID("tmux-" + string(instance.ID))
	require.Equal(t, wantSession, registry.bound[instance.ID])
	var mcp bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT
		COALESCE(json_extract(manifest_json, '$.capabilities.mcp'), 0)
		FROM sessions WHERE session_id = ?`, wantSession).Scan(&mcp))
	require.False(t, mcp)

	instance.SessionID = wantSession
	registry.active = []tmuxinstance.Instance{instance}
	changed, err = reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, changed, "an exact binding is idempotent")
}

func TestReconcilerInvalidatesSweptSessionAndContinuesAfterSweepError(t *testing.T) {
	t.Parallel()
	sessions, db, _ := openSessionStore(t)
	spec := validSessionSpec(testInstanceOne)
	sessionID, err := sessions.Bind(t.Context(), spec)
	require.NoError(t, err)
	invalid := lifecycleInstance(spec.InstanceID, spec.AgentID)
	invalid.SessionID = sessionID
	invalid.State = tmuxinstance.StateInvalid
	active := lifecycleInstance(testInstanceTwo, "agent-two")
	registry := &lifecycleRegistryStub{
		invalidated: []tmuxinstance.Instance{invalid},
		active:      []tmuxinstance.Instance{active},
		sweepErr:    errors.New("one independent instance was unreadable"),
	}
	reconciler, err := NewReconciler(registry, sessions)
	require.NoError(t, err)

	changed, err := reconciler.Reconcile(t.Context())
	require.ErrorContains(t, err, "independent instance")
	require.Equal(t, 2, changed)
	var state string
	var ended sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, ended_at
		FROM sessions WHERE session_id = ?`, sessionID).Scan(&state, &ended))
	require.Equal(t, string(protocol.SessionStateDisconnected), state)
	require.True(t, ended.Valid)
	require.Equal(t, protocol.OpaqueID("tmux-"+string(active.ID)), registry.bound[active.ID])
}

func TestReconcilerRequiresBothLifecycleOwners(t *testing.T) {
	t.Parallel()
	_, err := NewReconciler(nil, &SessionStore{})
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = NewReconciler(&lifecycleRegistryStub{}, nil)
	require.ErrorIs(t, err, ErrInvalidOptions)
}
