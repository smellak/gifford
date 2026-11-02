package tmuxinstance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// Registry is the node's registry of tmux instances. The node and the
// administrator call it; agents never do.
type Registry struct {
	db       *sql.DB
	clock    clock.Clock
	sys      system
	panes    PaneLister
	excluded map[protocol.AgentID]struct{}
	reserved map[uint32]struct{}
}

// Options configures New.
type Options struct {
	DB    *sql.DB
	Clock clock.Clock
	// Panes lists one tmux server's panes. Only Register uses it.
	Panes PaneLister
	// ExcludedAgents are agents Register always refuses.
	ExcludedAgents []protocol.AgentID
	// ReservedUIDs can never be an agent's uid: the node's and the bridge's.
	// Root is always reserved.
	ReservedUIDs []uint32
}

// New returns the registry of the tables in opts.DB.
func New(opts Options) (*Registry, error) {
	return newRegistry(opts, newSystem())
}

func newRegistry(opts Options, sys system) (*Registry, error) {
	if opts.DB == nil || opts.Clock == nil || opts.Panes == nil || sys == nil {
		return nil, fmt.Errorf("new tmux instance registry: %w", ErrInvalidArgument)
	}
	r := &Registry{
		db: opts.DB, clock: opts.Clock, sys: sys, panes: opts.Panes,
		excluded: make(map[protocol.AgentID]struct{}),
		reserved: map[uint32]struct{}{0: {}},
	}
	for _, agent := range opts.ExcludedAgents {
		if !agent.Valid() {
			return nil, fmt.Errorf("new tmux instance registry: excluded agent %q: %w", agent, ErrInvalidArgument)
		}
		r.excluded[agent] = struct{}{}
	}
	for _, uid := range opts.ReservedUIDs {
		r.reserved[uid] = struct{}{}
	}
	return r, nil
}

// Register checks reg against tmux and /proc and records it as ACTIVE. It
// refuses while the agent or the pane already has an ACTIVE instance: the
// administrator revokes that one first. RefusalOf gives the code of any
// error it returns.
func (r *Registry) Register(ctx context.Context, reg Registration) (Instance, error) {
	if err := reg.validate(); err != nil {
		return Instance{}, err
	}
	if _, excluded := r.excluded[reg.AgentID]; excluded {
		return Instance{}, refuse(RefusalExcluded, "agent %s is excluded", reg.AgentID)
	}
	ownerUID, err := r.sys.lookupUser(reg.OwnerUser)
	if err != nil {
		return Instance{}, coded(RefusalUnknownUser, fmt.Errorf("register tmux instance: %w", err))
	}
	agentUID, err := r.sys.lookupUser(reg.AgentUser)
	if err != nil {
		return Instance{}, coded(RefusalUnknownUser, fmt.Errorf("register tmux instance: %w", err))
	}
	if ownerUID == 0 || agentUID == 0 {
		return Instance{}, refuse(RefusalRootUser, "%s or %s is root", reg.OwnerUser, reg.AgentUser)
	}
	if _, reserved := r.reserved[agentUID]; reserved {
		return Instance{}, refuse(RefusalReservedUID, "uid %d of %s is reserved", agentUID, reg.AgentUser)
	}
	if reason, err := r.checkSocketDir(reg.Socket, ownerUID); err != nil || reason != "" {
		if err != nil {
			return Instance{}, fmt.Errorf("register tmux instance: %w", err)
		}
		return Instance{}, refuse(RefusalSocketDir, "socket directory of %s: %s", reg.Socket, reason)
	}
	panePID, err := r.findPane(ctx, reg)
	if err != nil {
		return Instance{}, err
	}
	pane, err := r.sys.process(panePID)
	if err != nil {
		return Instance{}, refuseErr(RefusalPaneMissing, fmt.Sprintf("pane %s process %d", reg.PaneID, panePID), err)
	}
	if pane.comm != reg.PaneComm {
		return Instance{}, refuse(RefusalPaneComm, "pane %s runs %q, not %q", reg.PaneID, pane.comm, reg.PaneComm)
	}
	harnessPID, harness, err := r.findHarness(reg, panePID, pane)
	if err != nil {
		return Instance{}, err
	}
	if harness.uid != agentUID {
		return Instance{}, refuse(RefusalAgentUID, "%s runs as uid %d, not %s", reg.HarnessComm, harness.uid, reg.AgentUser)
	}
	bootID, err := r.sys.bootID()
	if err != nil {
		return Instance{}, fmt.Errorf("register tmux instance: %w", err)
	}
	machineID, err := r.sys.machineID()
	if err != nil {
		return Instance{}, fmt.Errorf("register tmux instance: %w", err)
	}
	now := r.clock.Now()
	id, err := mintULID(now)
	if err != nil {
		return Instance{}, fmt.Errorf("register tmux instance: %w", err)
	}
	inst := Instance{
		ID: id, AgentID: reg.AgentID, MachineID: machineID, BootID: bootID,
		OwnerUser: reg.OwnerUser, OwnerUID: ownerUID, Socket: reg.Socket,
		PaneID: reg.PaneID, PanePID: panePID, PaneStart: pane.start, PaneComm: pane.comm,
		Harness: reg.Harness, HarnessVersion: reg.HarnessVersion,
		HarnessPID: harnessPID, HarnessStart: harness.start, HarnessComm: harness.comm,
		AgentUID: harness.uid, AgentGID: harness.gid, State: StateActive,
	}
	stamp := now.UTC().Format(timeLayout)
	_, err = r.db.ExecContext(ctx, `INSERT INTO tmux_instances (
		instance_id, agent_id, machine_id, boot_id, owner_user, owner_uid, socket,
		pane_id, pane_pid, pane_start, pane_comm, harness, harness_version,
		harness_pid, harness_start, harness_comm, agent_uid, agent_gid,
		source, registered_by, state, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)`,
		inst.ID, inst.AgentID, inst.MachineID, inst.BootID, inst.OwnerUser, inst.OwnerUID,
		inst.Socket, inst.PaneID, inst.PanePID, inst.PaneStart, inst.PaneComm, inst.Harness,
		inst.HarnessVersion, inst.HarnessPID, inst.HarnessStart, inst.HarnessComm,
		inst.AgentUID, inst.AgentGID, reg.Source, reg.RegisteredBy, stamp, stamp)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return Instance{}, refuse(RefusalAlreadyActive, "agent %s or pane %s already has an active instance",
				reg.AgentID, reg.PaneID)
		}
		return Instance{}, fmt.Errorf("insert tmux instance: %w", err)
	}
	return inst, nil
}

func (r *Registry) findPane(ctx context.Context, reg Registration) (int64, error) {
	rows, err := r.panes.ListPanes(ctx, reg.OwnerUser, reg.Socket)
	if err != nil {
		return 0, coded(RefusalTmuxUnreachable, fmt.Errorf("register tmux instance: list panes: %w", err))
	}
	for _, row := range rows {
		if row.PaneID == reg.PaneID {
			if row.PanePID <= 0 {
				return 0, refuse(RefusalPaneMissing, "pane %s has no process", reg.PaneID)
			}
			return row.PanePID, nil
		}
	}
	return 0, refuse(RefusalPaneMissing, "pane %s is not on %s", reg.PaneID, reg.Socket)
}

// findHarness accepts the pane's own process, or exactly one direct child
// of it with the CLI's name.
func (r *Registry) findHarness(reg Registration, panePID int64, pane procInfo) (int64, procInfo, error) {
	if reg.HarnessComm == reg.PaneComm {
		return panePID, pane, nil
	}
	children, err := r.sys.children(panePID)
	if err != nil {
		return 0, procInfo{}, refuseErr(RefusalPaneMissing, fmt.Sprintf("children of pane %s", reg.PaneID), err)
	}
	var found []int64
	var info procInfo
	for _, child := range children {
		proc, err := r.sys.process(child)
		if errors.Is(err, errProcessGone) {
			continue
		}
		if err != nil {
			return 0, procInfo{}, fmt.Errorf("register tmux instance: %w", err)
		}
		if proc.comm == reg.HarnessComm && proc.ppid == panePID {
			found, info = append(found, child), proc
		}
	}
	if len(found) != 1 {
		return 0, procInfo{}, refuse(RefusalHarnessMissing, "pane %s has %d direct children named %q, not 1",
			reg.PaneID, len(found), reg.HarnessComm)
	}
	return found[0], info, nil
}

// Verify checks inst against the database, tmux's view of its pane and
// /proc before a write. On any mismatch it marks the instance INVALID and
// returns ErrInvalidated. Any other error leaves the row as it was, and the
// caller must not write either.
func (r *Registry) Verify(ctx context.Context, inst Instance, obs PaneObservation) error {
	current, err := r.get(ctx, inst.ID)
	if err != nil {
		return err
	}
	if current.State != StateActive {
		return fmt.Errorf("tmux instance %s: %s: %w", current.ID, current.InvalidReason, ErrInvalidated)
	}
	if !sameIdentity(current, inst) {
		return fmt.Errorf("verify tmux instance %s: caller's copy differs: %w", inst.ID, ErrInvalidArgument)
	}
	var reason Reason
	switch {
	case obs.PaneID != current.PaneID:
		reason = ReasonPaneGone
	case obs.PanePID != current.PanePID:
		reason = ReasonPanePIDChanged
	default:
		if reason, err = r.check(current); err != nil {
			return fmt.Errorf("verify tmux instance %s: %w", current.ID, err)
		}
	}
	if reason == "" {
		return nil
	}
	if _, err := r.invalidate(ctx, current.ID, reason); err != nil {
		return err
	}
	return fmt.Errorf("tmux instance %s: %s: %w", current.ID, reason, ErrInvalidated)
}

// Sweep re-checks every ACTIVE instance without tmux and returns the ones it
// invalidated. It keeps going past an instance it cannot read.
func (r *Registry) Sweep(ctx context.Context) ([]Instance, error) {
	active, err := r.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	var invalidated []Instance
	var errs []error
	for _, inst := range active {
		reason, err := r.check(inst)
		if err != nil {
			errs = append(errs, fmt.Errorf("sweep tmux instance %s: %w", inst.ID, err))
			continue
		}
		if reason == "" {
			continue
		}
		changed, err := r.invalidate(ctx, inst.ID, reason)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if changed {
			inst.State, inst.InvalidReason = StateInvalid, reason
			invalidated = append(invalidated, inst)
		}
	}
	return invalidated, errors.Join(errs...)
}

// check returns the first reason inst no longer holds, or "".
func (r *Registry) check(inst Instance) (Reason, error) {
	bootID, err := r.sys.bootID()
	if err != nil {
		return "", err
	}
	if bootID != inst.BootID {
		return ReasonBootChanged, nil
	}
	if reason, err := r.checkSocketDir(inst.Socket, inst.OwnerUID); err != nil || reason != "" {
		return reason, err
	}
	pane, err := r.sys.process(inst.PanePID)
	if errors.Is(err, errProcessGone) {
		return ReasonPaneGone, nil
	}
	if err != nil {
		return "", err
	}
	if pane.start != inst.PaneStart {
		return ReasonPaneStartChanged, nil
	}
	if pane.comm != inst.PaneComm {
		return ReasonHarnessChanged, nil
	}
	harness := pane
	if inst.HarnessPID != inst.PanePID {
		harness, err = r.sys.process(inst.HarnessPID)
		if errors.Is(err, errProcessGone) {
			return ReasonHarnessChanged, nil
		}
		if err != nil {
			return "", err
		}
		if harness.ppid != inst.PanePID {
			return ReasonHarnessChanged, nil
		}
	}
	if harness.start != inst.HarnessStart || harness.comm != inst.HarnessComm ||
		harness.uid != inst.AgentUID || harness.gid != inst.AgentGID {
		return ReasonHarnessChanged, nil
	}
	return "", nil
}

// checkSocketDir: the directory must be the owner's, 0700 and not a
// symlink. With -S, tmux itself does not check it.
func (r *Registry) checkSocketDir(socket string, owner uint32) (Reason, error) {
	dir, err := r.sys.socketDir(socket)
	if errors.Is(err, errProcessGone) || isNotExist(err) {
		return ReasonSocketChanged, nil
	}
	if err != nil {
		return "", err
	}
	if !dir.dir || dir.symlink || dir.perm != 0o700 {
		return ReasonSocketChanged, nil
	}
	if dir.uid != owner {
		return ReasonOwnerChanged, nil
	}
	return "", nil
}

// BindSession records the tmux_attach session opened for an ACTIVE
// instance.
func (r *Registry) BindSession(ctx context.Context, id protocol.ULID, session protocol.OpaqueID) error {
	if !session.Valid() {
		return fmt.Errorf("bind tmux instance session: %w", ErrInvalidArgument)
	}
	result, err := r.db.ExecContext(ctx, `UPDATE tmux_instances SET session_id = ?, updated_at = ?
		WHERE instance_id = ? AND state = 'ACTIVE'`,
		session, r.clock.Now().UTC().Format(timeLayout), id)
	if err != nil {
		return fmt.Errorf("bind tmux instance session: %w", err)
	}
	return r.affected(ctx, result, id)
}

// Revoke invalidates the agent's ACTIVE instance on the administrator's
// order.
func (r *Registry) Revoke(ctx context.Context, agent protocol.AgentID) (Instance, error) {
	inst, err := r.Lookup(ctx, agent)
	if err != nil {
		return Instance{}, err
	}
	changed, err := r.invalidate(ctx, inst.ID, ReasonAdminRevoked)
	if err != nil {
		return Instance{}, err
	}
	if !changed {
		return Instance{}, fmt.Errorf("revoke tmux instance %s: %w", inst.ID, ErrInvalidated)
	}
	inst.State, inst.InvalidReason = StateInvalid, ReasonAdminRevoked
	return inst, nil
}

func (r *Registry) invalidate(ctx context.Context, id protocol.ULID, reason Reason) (bool, error) {
	result, err := r.db.ExecContext(ctx, `UPDATE tmux_instances
		SET state = 'INVALID', invalid_reason = ?, updated_at = ?
		WHERE instance_id = ? AND state = 'ACTIVE'`,
		reason, r.clock.Now().UTC().Format(timeLayout), id)
	if err != nil {
		return false, fmt.Errorf("invalidate tmux instance %s: %w", id, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("invalidate tmux instance %s: %w", id, err)
	}
	return n == 1, nil
}

func (r *Registry) affected(ctx context.Context, result sql.Result, id protocol.ULID) error {
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("tmux instance %s: %w", id, err)
	}
	if n == 1 {
		return nil
	}
	inst, err := r.get(ctx, id)
	if err != nil {
		return err
	}
	return fmt.Errorf("tmux instance %s: %s: %w", inst.ID, inst.InvalidReason, ErrInvalidated)
}

// ListActive returns every ACTIVE instance, ordered by agent.
func (r *Registry) ListActive(ctx context.Context) ([]Instance, error) {
	return r.query(ctx, `WHERE state = 'ACTIVE' ORDER BY agent_id`)
}

// List returns every instance, ACTIVE and INVALID, oldest first.
func (r *Registry) List(ctx context.Context) ([]Instance, error) {
	return r.query(ctx, `ORDER BY created_at, instance_id`)
}

// Lookup returns the agent's ACTIVE instance, or ErrNotRegistered.
func (r *Registry) Lookup(ctx context.Context, agent protocol.AgentID) (Instance, error) {
	found, err := r.query(ctx, `WHERE agent_id = ? AND state = 'ACTIVE'`, agent)
	if err != nil {
		return Instance{}, err
	}
	if len(found) != 1 {
		return Instance{}, fmt.Errorf("tmux instance of %s: %w", agent, ErrNotRegistered)
	}
	return found[0], nil
}

func (r *Registry) get(ctx context.Context, id protocol.ULID) (Instance, error) {
	found, err := r.query(ctx, `WHERE instance_id = ?`, id)
	if err != nil {
		return Instance{}, err
	}
	if len(found) != 1 {
		return Instance{}, fmt.Errorf("tmux instance %s: %w", id, ErrNotRegistered)
	}
	return found[0], nil
}

const selectColumns = `SELECT instance_id, agent_id, machine_id, boot_id, owner_user,
	owner_uid, socket, pane_id, pane_pid, pane_start, pane_comm, harness,
	harness_version, harness_pid, harness_start, harness_comm, agent_uid,
	agent_gid, session_id, state, invalid_reason FROM tmux_instances `

func (r *Registry) query(ctx context.Context, where string, args ...any) ([]Instance, error) {
	rows, err := r.db.QueryContext(ctx, selectColumns+where, args...)
	if err != nil {
		return nil, fmt.Errorf("select tmux instances: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Instance
	for rows.Next() {
		var inst Instance
		var session, reason sql.NullString
		if err := rows.Scan(&inst.ID, &inst.AgentID, &inst.MachineID, &inst.BootID,
			&inst.OwnerUser, &inst.OwnerUID, &inst.Socket, &inst.PaneID, &inst.PanePID,
			&inst.PaneStart, &inst.PaneComm, &inst.Harness, &inst.HarnessVersion,
			&inst.HarnessPID, &inst.HarnessStart, &inst.HarnessComm, &inst.AgentUID,
			&inst.AgentGID, &session, &inst.State, &reason); err != nil {
			return nil, fmt.Errorf("scan tmux instance: %w", err)
		}
		inst.SessionID, inst.InvalidReason = protocol.OpaqueID(session.String), Reason(reason.String)
		out = append(out, inst)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read tmux instances: %w", err)
	}
	return out, nil
}

// sameIdentity compares what identifies an instance, not its session or
// state.
func sameIdentity(a, b Instance) bool {
	a.SessionID, b.SessionID = "", ""
	a.State, b.State = "", ""
	a.InvalidReason, b.InvalidReason = "", ""
	return a == b
}

func refuse(code RefusalCode, format string, args ...any) error {
	return coded(code, fmt.Errorf("register tmux instance: "+format+": %w", append(args, ErrRefused)...))
}

func refuseErr(code RefusalCode, what string, err error) error {
	if errors.Is(err, errProcessGone) {
		return coded(code, fmt.Errorf("register tmux instance: %s: %w: %w", what, ErrRefused, err))
	}
	return fmt.Errorf("register tmux instance: %s: %w", what, err)
}
