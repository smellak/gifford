package channel

import (
	"strconv"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the cached control row of §4: one row per
// (scope_type, scope_key, plane), validated on the way in.

// control is one cached control row. Pointers are never shared: the cache
// copies rows on the way in and out.
type control struct {
	id        protocol.ControlID
	scope     protocol.ScopeType
	key       string
	plane     protocol.Plane
	mode      protocol.ControlMode // mode-carrying scopes only
	tx        *bool                // agent scope only
	rx        *bool                // agent scope only
	autoDeliv *bool                // agent scope only
	autoCont  *bool                // agent scope only
	from      protocol.AgentID     // link scope only
	to        protocol.AgentID     // link scope only
	reason    string
	actor     protocol.ActorType
	createdBy string
	createdAt time.Time
	expiresAt time.Time
	restore   *protocol.ControlSnapshot
	thenMode  protocol.ControlMode // global and node only
	version   int64
	updatedAt time.Time
}

// copy returns the row with its own flags and restore snapshot.
func (c control) copy() control {
	out := c
	if c.tx != nil {
		tx := *c.tx
		out.tx = &tx
	}
	if c.rx != nil {
		rx := *c.rx
		out.rx = &rx
	}
	if c.autoDeliv != nil {
		autoDeliv := *c.autoDeliv
		out.autoDeliv = &autoDeliv
	}
	if c.autoCont != nil {
		autoCont := *c.autoCont
		out.autoCont = &autoCont
	}
	if c.restore != nil {
		restore := *c.restore
		out.restore = &restore
	}
	return out
}

// controlFromRoot converts a validated contract row into a cache row.
func controlFromRoot(root protocol.ChannelControl) control {
	return control{
		id:        root.ControlID,
		scope:     root.ScopeType,
		key:       root.ScopeKey,
		plane:     root.Plane,
		mode:      root.Mode,
		tx:        root.TxEnabled,
		rx:        root.RxEnabled,
		autoDeliv: root.AutoDeliveryEnabled,
		autoCont:  root.AutoContinueEnabled,
		from:      root.FromAgent,
		to:        root.ToAgent,
		reason:    root.Reason,
		actor:     root.ActorType,
		createdBy: root.CreatedBy,
		createdAt: root.CreatedAt,
		expiresAt: root.ExpiresAt,
		restore:   root.Restore,
		thenMode:  root.ThenMode,
		version:   root.Version,
		updatedAt: root.UpdatedAt,
	}
}

// validateControl rejects rows that break the scoped rules, one
// *protocol.ValidationError per row at controls[i].<field>.
func validateControl(rows []control) error {
	for i, row := range rows {
		if violation := validateControlRow(row); violation != nil {
			violation.Path = controlPath(i, violation.Path)
			return violation
		}
	}
	seen := make(map[controlTriple]bool, len(rows))
	for i, row := range rows {
		triple := controlTriple{scope: row.scope, key: row.key, plane: row.plane}
		if seen[triple] {
			return &protocol.ValidationError{Path: controlPath(i, "scope_type"), Reason: "duplicate row"}
		}
		seen[triple] = true
	}
	return nil
}

// controlTriple identifies one cache entry.
type controlTriple struct {
	scope protocol.ScopeType
	key   string
	plane protocol.Plane
}

// validateControlRow checks one row against its scope.
func validateControlRow(row control) *protocol.ValidationError {
	switch row.scope {
	case protocol.ScopeTypeGlobal:
		if row.key != "*" || row.plane != protocol.PlaneAll || !protocol.ChannelMode(row.mode).Valid() {
			return &protocol.ValidationError{Path: "scope_type", Reason: "global needs *, all and a channel mode"}
		}
	case protocol.ScopeTypeNode:
		if row.key == "" || row.plane != protocol.PlaneAll || !protocol.ChannelMode(row.mode).Valid() {
			return &protocol.ValidationError{Path: "scope_type", Reason: "node needs a key, all and a channel mode"}
		}
	case protocol.ScopeTypeAgent:
		if row.mode != "" || (row.tx == nil && row.rx == nil && row.autoDeliv == nil && row.autoCont == nil) {
			return &protocol.ValidationError{Path: "scope_type", Reason: "agent needs flags and no mode"}
		}
	case protocol.ScopeTypeLink:
		if (row.plane != protocol.PlaneMessage && row.plane != protocol.PlaneArtifact) ||
			!protocol.LinkMode(row.mode).Valid() || row.from == "" || row.to == "" ||
			row.from == row.to || row.key != linkKey(row.from, row.to) {
			return &protocol.ValidationError{Path: "scope_type", Reason: "link needs a plane, a link mode and joined endpoints"}
		}
	case protocol.ScopeTypeThread:
		if row.plane != protocol.PlaneAll || !protocol.LinkMode(row.mode).Valid() {
			return &protocol.ValidationError{Path: "scope_type", Reason: "thread needs all and a link mode"}
		}
	case protocol.ScopeTypeLoop:
		if row.plane != protocol.PlaneAll ||
			(row.mode != protocol.ControlModePaused && row.mode != protocol.ControlModeStopped) {
			return &protocol.ValidationError{Path: "scope_type", Reason: "loop override is PAUSED or STOPPED"}
		}
	default:
		return &protocol.ValidationError{Path: "scope_type", Reason: "unknown scope"}
	}
	if row.restore != nil && row.expiresAt.IsZero() {
		return &protocol.ValidationError{Path: "restore", Reason: "needs expires_at"}
	}
	if row.thenMode != "" && (row.thenMode != protocol.ControlModeOff ||
		row.mode != protocol.ControlModeDraining ||
		(row.scope != protocol.ScopeTypeGlobal && row.scope != protocol.ScopeTypeNode)) {
		return &protocol.ValidationError{Path: "then_mode", Reason: "OFF with mode DRAINING on global or node"}
	}
	if row.updatedAt.Before(row.createdAt) {
		return &protocol.ValidationError{Path: "updated_at", Reason: "never before created_at"}
	}
	return nil
}

// controlPath names one row field for violations.
func controlPath(i int, field string) string {
	return "controls[" + strconv.Itoa(i) + "]." + field
}

// linkKey joins the two agents of a link scope key.
func linkKey(from, to protocol.AgentID) string {
	return string(from) + "→" + string(to)
}

// restoreTarget merges a row and its restore into the row that takes
// effect after expiry: nil without a restore; the snapshot while the
// restore is still due; the then-mode row once a global or node row with
// --then off has expired; otherwise nil, which means the default.
func restoreTarget(row control, now time.Time) *control {
	if row.restore == nil {
		return nil
	}
	if row.expiresAt.After(now) {
		snapshot := control{
			id:        row.id,
			scope:     row.scope,
			key:       row.key,
			plane:     row.plane,
			mode:      row.restore.Mode,
			tx:        row.restore.TxEnabled,
			rx:        row.restore.RxEnabled,
			autoDeliv: row.restore.AutoDeliveryEnabled,
			autoCont:  row.restore.AutoContinueEnabled,
			reason:    row.restore.Reason,
			actor:     row.restore.ActorType,
			createdBy: row.restore.CreatedBy,
			createdAt: row.createdAt,
			expiresAt: row.restore.ExpiresAt,
			restore:   nil,
			thenMode:  row.restore.ThenMode,
			version:   row.restore.Version,
			updatedAt: row.updatedAt,
		}
		return &snapshot
	}
	if row.thenMode != "" &&
		(row.scope == protocol.ScopeTypeGlobal || row.scope == protocol.ScopeTypeNode) {
		merged := row.copy()
		merged.mode = row.thenMode
		merged.version = row.restore.Version
		merged.restore = nil
		return &merged
	}
	return nil
}
