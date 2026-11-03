package tmuxattach

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

// InstanceLifecycle is the narrow P1 registry surface needed to keep Core
// sessions aligned with verified pane instances.
type InstanceLifecycle interface {
	Sweep(context.Context) ([]tmuxinstance.Instance, error)
	Reconcile(context.Context, []tmuxinstance.Registration) (tmuxinstance.Reconciliation, error)
	ListActive(context.Context) ([]tmuxinstance.Instance, error)
	BindSession(context.Context, protocol.ULID, protocol.OpaqueID) error
}

// StatementLoader reads the administrator's current desired instances.
type StatementLoader func() ([]tmuxinstance.Registration, error)

// SessionLifecycle is the narrow durable-session surface used by Reconciler.
type SessionLifecycle interface {
	Bind(context.Context, SessionSpec) (protocol.OpaqueID, error)
	Invalidate(context.Context, protocol.AgentID, protocol.OpaqueID) error
}

// ReconcileReport holds non-fatal, content-free observations from one pass.
// StatementError is set only when the loader's error changed since the last
// pass; a missing file is the declared no-op and is never reported.
type ReconcileReport struct {
	Changed        int
	Refused        []tmuxinstance.Refusal
	StatementError error
}

// Reconciler aligns active P1 instances with deterministic tmux_attach
// sessions and disconnects sessions whose instance failed a sweep.
type Reconciler struct {
	instances          InstanceLifecycle
	sessions           SessionLifecycle
	loadStatements     StatementLoader
	lastStatementError string
}

// NewReconciler validates the durable lifecycle owners and declaration source.
func NewReconciler(
	instances InstanceLifecycle,
	sessions SessionLifecycle,
	loadStatements StatementLoader,
) (*Reconciler, error) {
	if instances == nil || sessions == nil || loadStatements == nil {
		return nil, ErrInvalidOptions
	}
	return &Reconciler{
		instances: instances, sessions: sessions, loadStatements: loadStatements,
	}, nil
}

// Reconcile performs one idempotent lifecycle pass. It keeps processing
// independent instances after an error and reports completed session changes,
// new statement refusals and a changed statement-load error.
func (r *Reconciler) Reconcile(ctx context.Context) (ReconcileReport, error) {
	if r == nil || ctx == nil {
		return ReconcileReport{}, ErrInvalidOptions
	}
	var report ReconcileReport
	invalidated, sweepErr := r.instances.Sweep(ctx)
	errs := []error{sweepErr}
	r.invalidateSessions(ctx, invalidated, &report, &errs)

	statements, loadErr := r.loadStatements()
	switch {
	case loadErr == nil:
		r.lastStatementError = ""
		reconciliation, err := r.instances.Reconcile(ctx, statements)
		report.Refused = append(report.Refused, reconciliation.Refused...)
		r.invalidateSessions(ctx, reconciliation.Revoked, &report, &errs)
		if err != nil {
			errs = append(errs, fmt.Errorf("reconcile declared tmux instances: %w", err))
		}
	case errors.Is(loadErr, fs.ErrNotExist):
		r.lastStatementError = ""
	default:
		current := loadErr.Error()
		if current != r.lastStatementError {
			report.StatementError = loadErr
		}
		r.lastStatementError = current
	}

	active, err := r.instances.ListActive(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("list active tmux instances: %w", err))
		return report, errors.Join(errs...)
	}
	for _, instance := range active {
		sessionID, err := r.sessions.Bind(ctx, sessionSpec(instance))
		if err != nil {
			errs = append(errs, fmt.Errorf("bind tmux session for %s: %w", instance.AgentID, err))
			continue
		}
		if instance.SessionID == sessionID {
			continue
		}
		if err := r.instances.BindSession(ctx, instance.ID, sessionID); err != nil {
			errs = append(errs, fmt.Errorf("link tmux instance %s to session: %w", instance.ID, err))
			continue
		}
		report.Changed++
	}
	return report, errors.Join(errs...)
}

func (r *Reconciler) invalidateSessions(
	ctx context.Context,
	instances []tmuxinstance.Instance,
	report *ReconcileReport,
	errs *[]error,
) {
	for _, instance := range instances {
		if !instance.SessionID.Valid() {
			continue
		}
		if err := r.sessions.Invalidate(ctx, instance.AgentID, instance.SessionID); err != nil {
			*errs = append(*errs, fmt.Errorf("invalidate tmux session for %s: %w", instance.AgentID, err))
			continue
		}
		report.Changed++
	}
}

func sessionSpec(instance tmuxinstance.Instance) SessionSpec {
	return SessionSpec{
		InstanceID: instance.ID, AgentID: instance.AgentID,
		Harness: instance.Harness, HarnessVersion: instance.HarnessVersion,
		Process: instance.Process(), BootID: instance.BootID,
	}
}
