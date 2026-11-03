package tmuxinstance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/smellak/gifford/internal/protocol"
)

// Reconciliation reports what one Reconcile pass changed.
type Reconciliation struct {
	Registered []Instance
	// Revoked instances lost their statement; their sessions must end.
	Revoked []Instance
	// Refused are the statements refused in this pass with another code
	// than in the last one. A refused statement leaves no instance, and the
	// next pass tries it again.
	Refused []Refusal
}

// Reconcile makes the ACTIVE instances follow the administrator's
// statements, as LoadStatements returns them. It revokes every ACTIVE
// instance whose agent no statement names, or whose statement changed, and
// registers every statement that has no ACTIVE instance and was never
// registered before. An instance that was registered and then invalidated
// is not registered again on its own: the administrator bumps the
// statement's generation. Each refused statement keeps its code in
// tmux_instance_refusals until it registers or is removed.
func (r *Registry) Reconcile(ctx context.Context, statements []Registration) (Reconciliation, error) {
	var out Reconciliation
	wanted := make(map[protocol.AgentID]Registration, len(statements))
	for _, reg := range statements {
		if _, dup := wanted[reg.AgentID]; dup {
			return out, fmt.Errorf("reconcile tmux instances: agent %s stated twice: %w", reg.AgentID, ErrInvalidArgument)
		}
		wanted[reg.AgentID] = reg
	}
	active, err := r.ListActive(ctx)
	if err != nil {
		return out, err
	}
	for _, inst := range active {
		source, err := r.source(ctx, inst.ID)
		if err != nil {
			return out, err
		}
		if reg, ok := wanted[inst.AgentID]; ok && reg.Source == source {
			continue
		}
		changed, err := r.invalidate(ctx, inst.ID, ReasonAdminRevoked)
		if err != nil {
			return out, err
		}
		if changed {
			inst.State, inst.InvalidReason = StateInvalid, ReasonAdminRevoked
			out.Revoked = append(out.Revoked, inst)
		}
	}
	refusals, err := r.refusals(ctx)
	if err != nil {
		return out, err
	}
	for agent := range refusals {
		if _, ok := wanted[agent]; !ok {
			if err := r.clearRefusal(ctx, agent); err != nil {
				return out, err
			}
		}
	}
	agents := make([]protocol.AgentID, 0, len(wanted))
	for agent := range wanted {
		agents = append(agents, agent)
	}
	slices.Sort(agents)
	for _, agent := range agents {
		reg := wanted[agent]
		pending, err := r.unregistered(ctx, reg)
		if err != nil {
			return out, err
		}
		var refused error
		if pending {
			var inst Instance
			if inst, refused = r.Register(ctx, reg); refused == nil {
				out.Registered = append(out.Registered, inst)
			}
		}
		if refused == nil {
			if _, ok := refusals[agent]; ok {
				if err := r.clearRefusal(ctx, agent); err != nil {
					return out, err
				}
			}
			continue
		}
		code := RefusalOf(refused)
		if refusals[agent] == (refusalRow{source: reg.Source, code: code}) {
			continue
		}
		if err := r.recordRefusal(ctx, reg, code); err != nil {
			return out, err
		}
		out.Refused = append(out.Refused, Refusal{AgentID: agent, Code: code})
	}
	return out, nil
}

// unregistered reports whether reg has no ACTIVE instance and was never
// registered.
func (r *Registry) unregistered(ctx context.Context, reg Registration) (bool, error) {
	if _, err := r.Lookup(ctx, reg.AgentID); err == nil {
		return false, nil
	} else if !errors.Is(err, ErrNotRegistered) {
		return false, err
	}
	var one int
	err := r.db.QueryRowContext(ctx, `SELECT 1 FROM tmux_instances WHERE source = ? LIMIT 1`, reg.Source).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("tmux instance statement: %w", err)
	}
	return false, nil
}

func (r *Registry) source(ctx context.Context, id protocol.ULID) (string, error) {
	var source string
	err := r.db.QueryRowContext(ctx, `SELECT source FROM tmux_instances WHERE instance_id = ?`, id).Scan(&source)
	if err != nil {
		return "", fmt.Errorf("tmux instance %s source: %w", id, err)
	}
	return source, nil
}
