package tmuxattach

import (
	"context"
	"errors"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxinstance"
)

// InstanceLifecycle is the narrow P1 registry surface needed to keep Core
// sessions aligned with verified pane instances.
type InstanceLifecycle interface {
	Sweep(context.Context) ([]tmuxinstance.Instance, error)
	ListActive(context.Context) ([]tmuxinstance.Instance, error)
	BindSession(context.Context, protocol.ULID, protocol.OpaqueID) error
}

// SessionLifecycle is the narrow durable-session surface used by Reconciler.
type SessionLifecycle interface {
	Bind(context.Context, SessionSpec) (protocol.OpaqueID, error)
	Invalidate(context.Context, protocol.AgentID, protocol.OpaqueID) error
}

// Reconciler aligns active P1 instances with deterministic tmux_attach
// sessions and disconnects sessions whose instance failed a sweep.
type Reconciler struct {
	instances InstanceLifecycle
	sessions  SessionLifecycle
}

// NewReconciler validates the two durable lifecycle owners.
func NewReconciler(instances InstanceLifecycle, sessions SessionLifecycle) (*Reconciler, error) {
	if instances == nil || sessions == nil {
		return nil, ErrInvalidOptions
	}
	return &Reconciler{instances: instances, sessions: sessions}, nil
}

// Reconcile performs one idempotent lifecycle pass. It keeps processing
// independent instances after an error and returns how many registry/session
// bindings or invalidations it completed.
func (r *Reconciler) Reconcile(ctx context.Context) (int, error) {
	if r == nil || ctx == nil {
		return 0, ErrInvalidOptions
	}
	invalidated, sweepErr := r.instances.Sweep(ctx)
	changed := 0
	errs := []error{sweepErr}
	for _, instance := range invalidated {
		if !instance.SessionID.Valid() {
			continue
		}
		if err := r.sessions.Invalidate(ctx, instance.AgentID, instance.SessionID); err != nil {
			errs = append(errs, fmt.Errorf("invalidate tmux session for %s: %w", instance.AgentID, err))
			continue
		}
		changed++
	}

	active, err := r.instances.ListActive(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("list active tmux instances: %w", err))
		return changed, errors.Join(errs...)
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
		changed++
	}
	return changed, errors.Join(errs...)
}

func sessionSpec(instance tmuxinstance.Instance) SessionSpec {
	return SessionSpec{
		InstanceID: instance.ID, AgentID: instance.AgentID,
		Harness: instance.Harness, HarnessVersion: instance.HarnessVersion,
		Process: instance.Process(), BootID: instance.BootID,
	}
}
