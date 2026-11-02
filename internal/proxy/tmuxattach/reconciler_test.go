package tmuxattach

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

type lifecycleRegistryStub struct {
	active         []tmuxinstance.Instance
	invalidated    []tmuxinstance.Instance
	reconciliation tmuxinstance.Reconciliation
	sweepErr       error
	reconcileErr   error
	listErr        error
	reconciled     int
	statements     []tmuxinstance.Registration
	bound          map[protocol.ULID]protocol.OpaqueID
	events         *[]string
}

type lifecycleSessionStub struct {
	sessionID   protocol.OpaqueID
	invalidated []protocol.OpaqueID
	events      *[]string
}

func (s *lifecycleSessionStub) Bind(
	_ context.Context,
	spec SessionSpec,
) (protocol.OpaqueID, error) {
	s.record("bind-session:" + string(spec.InstanceID))
	if s.sessionID != "" {
		return s.sessionID, nil
	}
	return spec.SessionID(), nil
}

func (s *lifecycleSessionStub) Invalidate(
	_ context.Context,
	_ protocol.AgentID,
	session protocol.OpaqueID,
) error {
	s.record("invalidate-session:" + string(session))
	s.invalidated = append(s.invalidated, session)
	return nil
}

func (s *lifecycleSessionStub) record(event string) {
	if s.events != nil {
		*s.events = append(*s.events, event)
	}
}

func (r *lifecycleRegistryStub) Sweep(context.Context) ([]tmuxinstance.Instance, error) {
	r.record("sweep")
	return append([]tmuxinstance.Instance(nil), r.invalidated...), r.sweepErr
}

func (r *lifecycleRegistryStub) Reconcile(
	_ context.Context,
	statements []tmuxinstance.Registration,
) (tmuxinstance.Reconciliation, error) {
	r.record("reconcile")
	r.reconciled++
	r.statements = append([]tmuxinstance.Registration(nil), statements...)
	return r.reconciliation, r.reconcileErr
}

func (r *lifecycleRegistryStub) ListActive(context.Context) ([]tmuxinstance.Instance, error) {
	r.record("list")
	return append([]tmuxinstance.Instance(nil), r.active...), r.listErr
}

func (r *lifecycleRegistryStub) BindSession(
	_ context.Context,
	instance protocol.ULID,
	session protocol.OpaqueID,
) error {
	r.record("bind-instance:" + string(instance))
	if r.bound == nil {
		r.bound = make(map[protocol.ULID]protocol.OpaqueID)
	}
	r.bound[instance] = session
	return nil
}

func (r *lifecycleRegistryStub) record(event string) {
	if r.events != nil {
		*r.events = append(*r.events, event)
	}
}

func missingStatementFile() ([]tmuxinstance.Registration, error) {
	return nil, fs.ErrNotExist
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
	reconciler, err := NewReconciler(registry, sessions, missingStatementFile)
	require.NoError(t, err)

	report, err := reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, report.Changed)
	wantSession := protocol.OpaqueID("tmux-" + string(instance.ID))
	require.Equal(t, wantSession, registry.bound[instance.ID])
	var mcp bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT
		COALESCE(json_extract(manifest_json, '$.capabilities.mcp'), 0)
		FROM sessions WHERE session_id = ?`, wantSession).Scan(&mcp))
	require.False(t, mcp)

	instance.SessionID = wantSession
	registry.active = []tmuxinstance.Instance{instance}
	report, err = reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.Zero(t, report.Changed, "an exact binding is idempotent")
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
	reconciler, err := NewReconciler(registry, sessions, missingStatementFile)
	require.NoError(t, err)

	report, err := reconciler.Reconcile(t.Context())
	require.ErrorContains(t, err, "independent instance")
	require.Equal(t, 2, report.Changed)
	var state string
	var ended sql.NullString
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, ended_at
		FROM sessions WHERE session_id = ?`, sessionID).Scan(&state, &ended))
	require.Equal(t, string(protocol.SessionStateDisconnected), state)
	require.True(t, ended.Valid)
	require.Equal(t, protocol.OpaqueID("tmux-"+string(active.ID)), registry.bound[active.ID])
}

func TestReconcilerOrdersSweepDeclarationsRevocationsAndBindings(t *testing.T) {
	t.Parallel()
	events := make([]string, 0, 8)
	swept := lifecycleInstance(testInstanceOne, "swept-agent")
	swept.SessionID = "swept-session"
	revoked := lifecycleInstance(testInstanceTwo, "revoked-agent")
	revoked.SessionID = "revoked-session"
	active := lifecycleInstance("01JB0000000000000000000003", "active-agent")
	refusal := tmuxinstance.Refusal{
		AgentID: "refused-agent", Code: tmuxinstance.RefusalPaneMissing,
	}
	registry := &lifecycleRegistryStub{
		invalidated: []tmuxinstance.Instance{swept},
		reconciliation: tmuxinstance.Reconciliation{
			Revoked: []tmuxinstance.Instance{revoked}, Refused: []tmuxinstance.Refusal{refusal},
		},
		reconcileErr: errors.New("partial declaration failure"),
		active:       []tmuxinstance.Instance{active}, events: &events,
	}
	sessions := &lifecycleSessionStub{events: &events}
	load := func() ([]tmuxinstance.Registration, error) {
		events = append(events, "load")
		return []tmuxinstance.Registration{{AgentID: "declared-agent"}}, nil
	}
	reconciler, err := NewReconciler(registry, sessions, load)
	require.NoError(t, err)

	report, err := reconciler.Reconcile(t.Context())
	require.ErrorContains(t, err, "partial declaration failure")
	require.Equal(t, 3, report.Changed)
	require.Equal(t, []tmuxinstance.Refusal{refusal}, report.Refused)
	require.NoError(t, report.StatementError)
	require.Equal(t, []string{
		"sweep",
		"invalidate-session:swept-session",
		"load",
		"reconcile",
		"invalidate-session:revoked-session",
		"list",
		"bind-session:01JB0000000000000000000003",
		"bind-instance:01JB0000000000000000000003",
	}, events)
	require.Equal(t, []protocol.OpaqueID{"swept-session", "revoked-session"}, sessions.invalidated)
}

func TestReconcilerSkipsInvalidStatementsAndReportsOnlyChangedErrors(t *testing.T) {
	t.Parallel()
	registry := &lifecycleRegistryStub{}
	sessions := &lifecycleSessionStub{}
	first := errors.New("first statement error")
	second := errors.New("second statement error")
	loadErr := first
	load := func() ([]tmuxinstance.Registration, error) { return nil, loadErr }
	reconciler, err := NewReconciler(registry, sessions, load)
	require.NoError(t, err)

	report, err := reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, report.StatementError, first)
	require.Zero(t, registry.reconciled)

	report, err = reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.NoError(t, report.StatementError, "an unchanged load error is reported only once")
	require.Zero(t, registry.reconciled)

	loadErr = second
	report, err = reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, report.StatementError, second)
	require.Zero(t, registry.reconciled)

	loadErr = fs.ErrNotExist
	report, err = reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.NoError(t, report.StatementError, "a missing file is a silent no-op")
	require.Zero(t, registry.reconciled)

	loadErr = first
	report, err = reconciler.Reconcile(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, report.StatementError, first,
		"a missing file resets error de-duplication")
	require.Zero(t, registry.reconciled)
}

func TestReconcilerRequiresBothLifecycleOwners(t *testing.T) {
	t.Parallel()
	_, err := NewReconciler(nil, &SessionStore{}, missingStatementFile)
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = NewReconciler(&lifecycleRegistryStub{}, nil, missingStatementFile)
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = NewReconciler(&lifecycleRegistryStub{}, &SessionStore{}, nil)
	require.ErrorIs(t, err, ErrInvalidOptions)
}
