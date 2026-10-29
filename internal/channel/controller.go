package channel

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// Actor is the authenticated socket peer responsible for a mutation.
// Callers derive it from peer credentials; it is deliberately absent from
// every protocol params root.
type Actor struct {
	Type protocol.ActorType
	ID   string
}

// ControlEvent is one event produced by a committed control mutation.
type ControlEvent struct {
	Type    protocol.EventType
	Payload protocol.ChannelEventPayload
}

// EventSink receives committed control events in mutation order. The hub may
// adapt this to its event writer; a sink error is returned after the durable
// control commit and the cache is invalidated fail-closed.
type EventSink interface {
	EmitControlEvents(context.Context, []ControlEvent) error
}

// ControllerOptions configures the sole channel_controls writer.
type ControllerOptions struct {
	DB      *sql.DB
	Engine  *Engine
	Clock   clock.Clock
	NodeID  protocol.NodeID
	Entropy io.Reader
	Events  EventSink
}

// Controller owns control reads, CAS writes, TTL restoration and cache
// publication. It is safe for concurrent use; SQLite serializes its immediate
// transactions.
type Controller struct {
	db      *sql.DB
	engine  *Engine
	clock   clock.Clock
	nodeID  protocol.NodeID
	entropy io.Reader
	events  EventSink
}

// NewController validates opts and returns a control writer.
func NewController(opts ControllerOptions) (*Controller, error) {
	if opts.DB == nil || opts.Engine == nil || opts.Clock == nil || !opts.NodeID.Valid() {
		return nil, ErrInvalidOptions
	}
	if opts.Entropy == nil {
		opts.Entropy = rand.Reader
	}
	return &Controller{db: opts.DB, engine: opts.Engine, clock: opts.Clock, nodeID: opts.NodeID, entropy: opts.Entropy, events: opts.Events}, nil
}

// VersionConflict returns the version detail carried by err.
func VersionConflict(err error) (*VersionConflictError, bool) {
	var conflict *VersionConflictError
	return conflict, errors.As(err, &conflict)
}

// Reload publishes the current durable controls.
func (c *Controller) Reload(ctx context.Context) error { return c.engine.Reload(ctx, c.db) }

// Global returns the global state, defaulting to ACTIVE at version zero.
func (c *Controller) Global(ctx context.Context) (protocol.ChannelState, error) {
	row, ok, err := c.one(ctx, protocol.ScopeTypeGlobal, "*", protocol.PlaneAll)
	if err != nil {
		return protocol.ChannelState{}, err
	}
	return channelState(protocol.ScopeTypeGlobal, "*", row, ok, protocol.ChannelModeActive), nil
}

// Node returns one node state, including the effective global restriction.
func (c *Controller) Node(ctx context.Context, node protocol.NodeID) (protocol.ChannelState, error) {
	if !node.Valid() {
		return protocol.ChannelState{}, fmt.Errorf("node: %w", ErrInvalidQuery)
	}
	row, ok, err := c.one(ctx, protocol.ScopeTypeNode, string(node), protocol.PlaneAll)
	if err != nil {
		return protocol.ChannelState{}, err
	}
	global, err := c.Global(ctx)
	if err != nil {
		return protocol.ChannelState{}, err
	}
	state := channelState(protocol.ScopeTypeNode, string(node), row, ok, protocol.ChannelModeActive)
	state.EffectiveMode = strongestChannelMode(global.Mode, state.Mode)
	return state, nil
}

func channelState(scope protocol.ScopeType, key string, row protocol.ChannelControl, ok bool, fallback protocol.ChannelMode) protocol.ChannelState {
	state := protocol.ChannelState{ScopeType: scope, ScopeKey: key, Mode: fallback, EffectiveMode: fallback}
	if ok {
		state.Mode = protocol.ChannelMode(row.Mode)
		state.EffectiveMode = state.Mode
		state.Version = row.Version
		copy := row
		state.Control = &copy
	}
	return state
}

func strongestChannelMode(a, b protocol.ChannelMode) protocol.ChannelMode {
	rank := func(v protocol.ChannelMode) int {
		switch v {
		case protocol.ChannelModeOff:
			return 3
		case protocol.ChannelModeDraining:
			return 2
		case protocol.ChannelModePaused:
			return 1
		default:
			return 0
		}
	}
	if rank(a) >= rank(b) {
		return a
	}
	return b
}

// Agent returns one agent's local flags and their effective projection.
func (c *Controller) Agent(ctx context.Context, agent protocol.AgentID) (protocol.AgentChannel, error) {
	if !agent.Valid() {
		return protocol.AgentChannel{}, fmt.Errorf("agent: %w", ErrInvalidQuery)
	}
	row, ok, err := c.one(ctx, protocol.ScopeTypeAgent, string(agent), protocol.PlaneAll)
	if err != nil {
		return protocol.AgentChannel{}, err
	}
	out := protocol.AgentChannel{AgentID: agent, TX: true, RX: true, AutoDelivery: true, AutoContinue: true, Deliverable: true}
	if ok {
		applyAgentFlags(&out, row)
		out.Version = row.Version
		copy := row
		out.Control = &copy
	}
	out.Mode, _ = agentMode(out.TX, out.RX, out.AutoDelivery, out.AutoContinue)
	out.Effective = protocol.ChannelEffective{
		Global: protocol.ChannelModeActive, Node: protocol.ChannelModeActive,
		Tx: out.TX, Rx: out.RX, AutoDelivery: out.AutoDelivery,
		AutoContinue: out.AutoContinue, ControlVersion: out.Version,
	}
	return out, nil
}

func applyAgentFlags(out *protocol.AgentChannel, row protocol.ChannelControl) {
	if row.TxEnabled != nil {
		out.TX = *row.TxEnabled
	}
	if row.RxEnabled != nil {
		out.RX = *row.RxEnabled
	}
	if row.AutoDeliveryEnabled != nil {
		out.AutoDelivery = *row.AutoDeliveryEnabled
	}
	if row.AutoContinueEnabled != nil {
		out.AutoContinue = *row.AutoContinueEnabled
	}
}

func agentMode(tx, rx, delivery, continuation bool) (protocol.AgentMode, bool) {
	switch {
	case tx && rx && delivery && continuation:
		return protocol.AgentModeFull, true
	case tx && rx && !delivery && !continuation:
		return protocol.AgentModeManual, true
	case !tx && rx && delivery && !continuation:
		return protocol.AgentModeReceiveOnly, true
	case tx && !rx && !continuation:
		return protocol.AgentModeSendOnly, true
	case !tx && !rx && !delivery && !continuation:
		return protocol.AgentModeOff, true
	default:
		return "", false
	}
}

// Link returns both planes of one directional link.
func (c *Controller) Link(ctx context.Context, from, to protocol.AgentID) (protocol.LinkState, error) {
	if !from.Valid() || !to.Valid() || from == to {
		return protocol.LinkState{}, fmt.Errorf("link: %w", ErrInvalidQuery)
	}
	state := protocol.LinkState{FromAgent: from, ToAgent: to, MessageMode: protocol.LinkModeAllow, ArtifactMode: protocol.LinkModeAllow}
	for _, plane := range []protocol.Plane{protocol.PlaneMessage, protocol.PlaneArtifact} {
		row, ok, err := c.one(ctx, protocol.ScopeTypeLink, linkKey(from, to), plane)
		if err != nil {
			return protocol.LinkState{}, err
		}
		if !ok {
			continue
		}
		copy := row
		if plane == protocol.PlaneMessage {
			state.MessageMode, state.MessageVersion, state.MessageControl = protocol.LinkMode(row.Mode), row.Version, &copy
		} else {
			state.ArtifactMode, state.ArtifactVersion, state.ArtifactControl = protocol.LinkMode(row.Mode), row.Version, &copy
		}
	}
	return state, nil
}

// Links lists the distinct directional links, optionally involving agent.
func (c *Controller) Links(ctx context.Context, agent protocol.AgentID) (protocol.LinkList, error) {
	rows, err := c.rows(ctx)
	if err != nil {
		return protocol.LinkList{}, err
	}
	type pair struct{ from, to protocol.AgentID }
	seen := map[pair]struct{}{}
	for _, row := range rows {
		if row.ScopeType != protocol.ScopeTypeLink {
			continue
		}
		if agent != "" && row.FromAgent != agent && row.ToAgent != agent {
			continue
		}
		seen[pair{row.FromAgent, row.ToAgent}] = struct{}{}
	}
	pairs := make([]pair, 0, len(seen))
	for p := range seen {
		pairs = append(pairs, p)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].from == pairs[j].from {
			return pairs[i].to < pairs[j].to
		}
		return pairs[i].from < pairs[j].from
	})
	out := protocol.LinkList{Links: make([]protocol.LinkState, 0, len(pairs))}
	for _, p := range pairs {
		state, err := c.Link(ctx, p.from, p.to)
		if err != nil {
			return protocol.LinkList{}, err
		}
		out.Links = append(out.Links, state)
	}
	return out, nil
}

// SetGlobal performs the global CAS.
func (c *Controller) SetGlobal(ctx context.Context, p protocol.SetGlobalParams, actor Actor) (protocol.ChannelState, error) {
	if err := p.Validate(); err != nil {
		return protocol.ChannelState{}, err
	}
	row := protocol.ChannelControl{ScopeType: protocol.ScopeTypeGlobal, ScopeKey: "*", Plane: protocol.PlaneAll, Mode: protocol.ControlMode(p.Mode), Reason: p.Reason, ThenMode: protocol.ControlMode(p.ThenMode)}
	if _, err := c.mutate(ctx, row, p.ExpectedVersion, p.TTLMS, actor, protocol.EventTypeChannelGlobalChanged); err != nil {
		return protocol.ChannelState{}, err
	}
	return c.Global(ctx)
}

// SetNode performs one node CAS.
func (c *Controller) SetNode(ctx context.Context, p protocol.SetNodeParams, actor Actor) (protocol.ChannelState, error) {
	if err := p.Validate(); err != nil {
		return protocol.ChannelState{}, err
	}
	row := protocol.ChannelControl{ScopeType: protocol.ScopeTypeNode, ScopeKey: string(p.NodeID), Plane: protocol.PlaneAll, Mode: protocol.ControlMode(p.Mode), Reason: p.Reason, ThenMode: protocol.ControlMode(p.ThenMode)}
	if _, err := c.mutate(ctx, row, p.ExpectedVersion, p.TTLMS, actor, protocol.EventTypeChannelNodeChanged); err != nil {
		return protocol.ChannelState{}, err
	}
	return c.Node(ctx, p.NodeID)
}

// SetAgent patches the named flags and performs one agent CAS.
func (c *Controller) SetAgent(ctx context.Context, p protocol.SetAgentChannelParams, actor Actor) (protocol.AgentChannel, error) {
	if err := p.Validate(); err != nil {
		return protocol.AgentChannel{}, err
	}
	current, err := c.Agent(ctx, p.AgentID)
	if err != nil {
		return protocol.AgentChannel{}, err
	}
	tx, rx, delivery, continuation := current.TX, current.RX, current.AutoDelivery, current.AutoContinue
	if p.TX != nil {
		tx = *p.TX
	}
	if p.RX != nil {
		rx = *p.RX
	}
	if p.AutoDelivery != nil {
		delivery = *p.AutoDelivery
	}
	if p.AutoContinue != nil {
		continuation = *p.AutoContinue
	}
	row := protocol.ChannelControl{ScopeType: protocol.ScopeTypeAgent, ScopeKey: string(p.AgentID), Plane: protocol.PlaneAll, TxEnabled: boolPtr(tx), RxEnabled: boolPtr(rx), AutoDeliveryEnabled: boolPtr(delivery), AutoContinueEnabled: boolPtr(continuation), Reason: p.Reason}
	if _, err := c.mutate(ctx, row, p.ExpectedVersion, p.TTLMS, actor, protocol.EventTypeChannelAgentChanged); err != nil {
		return protocol.AgentChannel{}, err
	}
	return c.Agent(ctx, p.AgentID)
}

// SetAgentMode expands an agent mode macro and performs one agent CAS.
func (c *Controller) SetAgentMode(ctx context.Context, p protocol.SetAgentModeParams, actor Actor) (protocol.AgentChannel, error) {
	if err := p.Validate(); err != nil {
		return protocol.AgentChannel{}, err
	}
	current, err := c.Agent(ctx, p.AgentID)
	if err != nil {
		return protocol.AgentChannel{}, err
	}
	var tx, rx, delivery, continuation bool
	switch p.Mode {
	case protocol.AgentModeFull:
		tx, rx, delivery, continuation = true, true, true, true
	case protocol.AgentModeManual:
		tx, rx = true, true
	case protocol.AgentModeReceiveOnly:
		rx, delivery = true, true
	case protocol.AgentModeSendOnly:
		tx, delivery = true, current.AutoDelivery
	case protocol.AgentModeOff:
	default:
		return protocol.AgentChannel{}, fmt.Errorf("mode: %w", ErrInvalidQuery)
	}
	params := protocol.SetAgentChannelParams{AgentID: p.AgentID, TX: boolPtr(tx), RX: boolPtr(rx), AutoDelivery: boolPtr(delivery), AutoContinue: boolPtr(continuation), TTLMS: p.TTLMS, Reason: p.Reason, ExpectedVersion: p.ExpectedVersion}
	return c.SetAgent(ctx, params, actor)
}

// SetLink performs all directional/plane CAS writes atomically.
func (c *Controller) SetLink(ctx context.Context, p protocol.SetLinkParams, actor Actor) (protocol.LinkList, error) {
	if err := p.Validate(); err != nil {
		return protocol.LinkList{}, err
	}
	keys := linkMutationKeys(p.FromAgent, p.ToAgent, p.Plane, p.Bidirectional)
	changes := make([]mutation, 0, len(keys))
	for _, key := range keys {
		changes = append(changes, mutation{row: protocol.ChannelControl{ScopeType: protocol.ScopeTypeLink, ScopeKey: linkKey(key.from, key.to), Plane: key.plane, Mode: protocol.ControlMode(p.Mode), FromAgent: key.from, ToAgent: key.to, Reason: p.Reason}, expected: p.ExpectedVersions[linkVersionKey(key.from, key.to, key.plane)], ttl: p.TTLMS, event: protocol.EventTypeChannelLinkChanged})
	}
	if err := c.mutateBatch(ctx, changes, actor); err != nil {
		return protocol.LinkList{}, err
	}
	out := protocol.LinkList{Links: []protocol.LinkState{}}
	seen := map[string]bool{}
	for _, key := range keys {
		name := string(key.from) + "\x00" + string(key.to)
		if seen[name] {
			continue
		}
		seen[name] = true
		state, err := c.Link(ctx, key.from, key.to)
		if err != nil {
			return protocol.LinkList{}, err
		}
		out.Links = append(out.Links, state)
	}
	return out, nil
}

// DeleteLink removes the selected link overrides atomically.
func (c *Controller) DeleteLink(ctx context.Context, p protocol.DeleteLinkOverrideParams, actor Actor) (protocol.LinkState, error) {
	if err := p.Validate(); err != nil {
		return protocol.LinkState{}, err
	}
	keys := linkMutationKeys(p.FromAgent, p.ToAgent, p.Plane, false)
	changes := make([]mutation, 0, len(keys))
	for _, key := range keys {
		changes = append(changes, mutation{delete: true, row: protocol.ChannelControl{ScopeType: protocol.ScopeTypeLink, ScopeKey: linkKey(key.from, key.to), Plane: key.plane, FromAgent: key.from, ToAgent: key.to}, expected: p.ExpectedVersions[linkVersionKey(key.from, key.to, key.plane)], event: protocol.EventTypeChannelLinkChanged})
	}
	if err := c.mutateBatch(ctx, changes, actor); err != nil {
		return protocol.LinkState{}, err
	}
	return c.Link(ctx, p.FromAgent, p.ToAgent)
}

type linkMutationKey struct {
	from, to protocol.AgentID
	plane    protocol.Plane
}

func linkMutationKeys(from, to protocol.AgentID, plane protocol.Plane, bidirectional bool) []linkMutationKey {
	planes := []protocol.Plane{plane}
	if plane == protocol.PlaneAll {
		planes = []protocol.Plane{protocol.PlaneMessage, protocol.PlaneArtifact}
	}
	out := make([]linkMutationKey, 0, len(planes)*2)
	for _, p := range planes {
		out = append(out, linkMutationKey{from, to, p})
	}
	if bidirectional {
		for _, p := range planes {
			out = append(out, linkMutationKey{to, from, p})
		}
	}
	return out
}
func linkVersionKey(from, to protocol.AgentID, plane protocol.Plane) string {
	return "link:" + string(from) + "→" + string(to) + ":" + string(plane)
}

// Effective previews one delivery gate using the local node for both agents.
func (c *Controller) Effective(p protocol.EffectiveParams) (protocol.EffectiveView, error) {
	if err := p.Validate(); err != nil {
		return protocol.EffectiveView{}, err
	}
	q := Query{Stage: protocol.DecisionStageDelivery, Plane: p.Plane, From: p.FromAgent, To: p.ToAgent, FromNode: c.nodeID, ToNode: c.nodeID, ToDeliverable: true}
	v, eff, versions, err := c.engine.evaluate(q)
	if err != nil {
		return protocol.EffectiveView{}, err
	}
	return protocol.EffectiveView{FromAgent: p.FromAgent, ToAgent: p.ToAgent, Plane: p.Plane, EffectiveControl: effectiveControl(eff, q), EffectiveOutcome: v.outcome, Blocker: v.line, ControlVersions: versions}, nil
}

// ExplainPair explains a pair form at the delivery gate.
func (c *Controller) ExplainPair(p protocol.ExplainParams) (protocol.Explanation, error) {
	if err := p.Validate(); err != nil {
		return protocol.Explanation{}, err
	}
	if p.FromAgent == "" || p.ToAgent == "" {
		return protocol.Explanation{}, ErrNotFound
	}
	plane := p.Plane
	if plane == "" {
		plane = protocol.PlaneMessage
	}
	q := Query{Stage: protocol.DecisionStageDelivery, Plane: plane, From: p.FromAgent, To: p.ToAgent, FromNode: c.nodeID, ToNode: c.nodeID, ToDeliverable: true, ThreadID: p.ThreadID}
	return c.engine.Explain(q)
}

// Expire restores every due TTL row to its recorded prior state. Each row is
// its own immediate CAS transaction so one concurrent update cannot prevent
// unrelated expirations.
func (c *Controller) Expire(ctx context.Context) (int, error) {
	rows, err := c.rows(ctx)
	if err != nil {
		return 0, err
	}
	now := c.clock.Now()
	count := 0
	for _, row := range rows {
		if row.ExpiresAt.IsZero() || row.ExpiresAt.After(now) {
			continue
		}
		if err := c.expireOne(ctx, row); err != nil {
			if errors.Is(err, ErrVersionConflict) {
				continue
			}
			return count, err
		}
		count++
	}
	return count, nil
}

type mutation struct {
	row      protocol.ChannelControl
	expected int64
	ttl      *int64
	event    protocol.EventType
	delete   bool
	expiring bool
}

func (c *Controller) mutate(ctx context.Context, row protocol.ChannelControl, expected int64, ttl *int64, actor Actor, event protocol.EventType) (protocol.ChannelControl, error) {
	err := c.mutateBatch(ctx, []mutation{{row: row, expected: expected, ttl: ttl, event: event}}, actor)
	if err != nil {
		return protocol.ChannelControl{}, err
	}
	got, _, err := c.one(ctx, row.ScopeType, row.ScopeKey, row.Plane)
	return got, err
}

func (c *Controller) mutateBatch(ctx context.Context, changes []mutation, actor Actor) error {
	if actor.Type == "" || actor.ID == "" {
		return fmt.Errorf("actor: %w", ErrInvalidQuery)
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("control connection: %w", err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate control transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	all, err := readRows(ctx, conn)
	if err != nil {
		return err
	}
	byKey := make(map[controlTriple]protocol.ChannelControl, len(all))
	for _, r := range all {
		byKey[controlTriple{scope: r.ScopeType, key: r.ScopeKey, plane: r.Plane}] = r
	}
	events := make([]ControlEvent, 0, len(changes))
	published := make([]mutation, 0, len(changes))
	currentVersions := map[string]int64{}
	for i := range changes {
		ch := &changes[i]
		key := controlTriple{scope: ch.row.ScopeType, key: ch.row.ScopeKey, plane: ch.row.Plane}
		old, exists := byKey[key]
		current := int64(0)
		if exists {
			current = old.Version
		}
		if current != ch.expected {
			currentVersions[versionKey(ch.row)] = current
			continue
		}
	}
	if len(currentVersions) > 0 {
		if len(changes) == 1 {
			for _, v := range currentVersions {
				return &VersionConflictError{CurrentVersion: v}
			}
		}
		return &VersionConflictError{CurrentVersions: currentVersions}
	}
	now := c.clock.Now().UTC()
	for i := range changes {
		ch := &changes[i]
		key := controlTriple{scope: ch.row.ScopeType, key: ch.row.ScopeKey, plane: ch.row.Plane}
		old, exists := byKey[key]
		if ch.delete {
			if !exists {
				return &VersionConflictError{CurrentVersion: 0}
			}
			if err := execDelete(ctx, conn, ch.row, ch.expected); err != nil {
				return err
			}
			ch.row.Version = ch.expected + 1
			events = append(events, changeEvent(ch.event, old, nil, ch.row.Version, actor, "", false))
			published = append(published, *ch)
			delete(byKey, key)
			continue
		}
		row := ch.row
		if !ch.expiring {
			row.ActorType = actor.Type
			row.CreatedBy = actor.ID
		}
		row.Version = ch.expected + 1
		row.UpdatedAt = now
		if exists {
			row.ControlID = old.ControlID
			row.CreatedAt = old.CreatedAt
		} else {
			row.ControlID, err = c.mintControlID(now)
			if err != nil {
				return err
			}
			row.CreatedAt = now
		}
		if ch.expiring {
			row.Restore = nil
		} else if ch.ttl != nil && *ch.ttl > 0 {
			row.ExpiresAt = now.Add(time.Duration(*ch.ttl) * time.Millisecond)
			if exists {
				snapshot := snapshotOf(old)
				row.Restore = &snapshot
			}
		} else {
			row.ExpiresAt = time.Time{}
			row.Restore = nil
		}
		if err := row.Validate(); err != nil {
			return err
		}
		if exists {
			err = execUpdate(ctx, conn, row, ch.expected)
		} else {
			err = execInsert(ctx, conn, row)
		}
		if err != nil {
			return err
		}
		events = append(events, changeEvent(ch.event, old, &row, row.Version, actor, row.Reason, ch.expiring))
		ch.row = row
		published = append(published, *ch)
		byKey[key] = row
	}
	for i := range events {
		if err := events[i].Payload.ValidateFor(events[i].Type); err != nil {
			return fmt.Errorf("control event %d: %w", i, err)
		}
	}
	if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit control transaction: %w", err)
	}
	committed = true
	for _, ch := range published {
		key := cacheKey{scope: ch.row.ScopeType, key: ch.row.ScopeKey, plane: ch.row.Plane}
		if ch.delete {
			c.engine.remove(key, ch.row.Version)
		} else if _, err := c.engine.apply(ch.row); err != nil {
			c.engine.invalidate(key, ch.row.Version)
			return err
		}
	}
	if c.events != nil && len(events) > 0 {
		if err := c.events.EmitControlEvents(ctx, events); err != nil {
			for _, ch := range published {
				c.engine.invalidate(cacheKey{scope: ch.row.ScopeType, key: ch.row.ScopeKey, plane: ch.row.Plane}, ch.row.Version)
			}
			return fmt.Errorf("emit control events: %w", err)
		}
	}
	return nil
}

func (c *Controller) expireOne(ctx context.Context, old protocol.ChannelControl) error {
	actor := Actor{Type: protocol.ActorTypeSystem, ID: "system"}
	if old.Restore == nil {
		return c.mutateBatch(ctx, []mutation{{row: old, expected: old.Version, event: protocol.EventTypeControlTTLExpired, delete: true, expiring: true}}, actor)
	}
	s := old.Restore
	row := protocol.ChannelControl{ControlID: old.ControlID, ScopeType: old.ScopeType, ScopeKey: old.ScopeKey, Plane: old.Plane, Mode: s.Mode, TxEnabled: copyFlag(s.TxEnabled), RxEnabled: copyFlag(s.RxEnabled), AutoDeliveryEnabled: copyFlag(s.AutoDeliveryEnabled), AutoContinueEnabled: copyFlag(s.AutoContinueEnabled), FromAgent: old.FromAgent, ToAgent: old.ToAgent, Reason: s.Reason, ActorType: s.ActorType, CreatedBy: s.CreatedBy, ExpiresAt: s.ExpiresAt, ThenMode: s.ThenMode, CreatedAt: old.CreatedAt}
	// A restored row must not synthesize another default. Its recorded expiry is
	// retained, while restore_json is consumed.
	return c.mutateBatch(ctx, []mutation{{row: row, expected: old.Version, event: protocol.EventTypeControlTTLExpired, expiring: true}}, actor)
}

func snapshotOf(row protocol.ChannelControl) protocol.ControlSnapshot {
	return protocol.ControlSnapshot{Mode: row.Mode, TxEnabled: copyFlag(row.TxEnabled), RxEnabled: copyFlag(row.RxEnabled), AutoDeliveryEnabled: copyFlag(row.AutoDeliveryEnabled), AutoContinueEnabled: copyFlag(row.AutoContinueEnabled), Reason: row.Reason, ActorType: row.ActorType, CreatedBy: row.CreatedBy, ExpiresAt: row.ExpiresAt, ThenMode: row.ThenMode, Version: row.Version}
}
func boolPtr(v bool) *bool { return &v }

func changeEvent(t protocol.EventType, old protocol.ChannelControl, next *protocol.ChannelControl, version int64, actor Actor, reason string, restored bool) ControlEvent {
	p := protocol.ChannelEventPayload{ScopeType: old.ScopeType, ScopeKey: old.ScopeKey, Plane: old.Plane, Version: &version, ActorType: actor.Type, ActorID: actor.ID, Reason: reason, Restored: nil}
	if next != nil {
		p.ControlID = next.ControlID
		if old.ScopeType == "" {
			p.ScopeType, p.ScopeKey, p.Plane = next.ScopeType, next.ScopeKey, next.Plane
		}
		p.NewValue = valueOf(*next)
		p.ExpiresAt = next.ExpiresAt
		p.FromAgent, p.ToAgent = next.FromAgent, next.ToAgent
	} else {
		p.ControlID = old.ControlID
		p.NewValue = protocol.Value(`{"deleted":true}`)
		p.FromAgent, p.ToAgent = old.FromAgent, old.ToAgent
	}
	if old.ControlID != "" {
		p.OldValue = valueOf(old)
	}
	if t == protocol.EventTypeControlTTLExpired {
		p.ActorType = protocol.ActorTypeSystem
		p.ActorID = "system"
		p.Reason = "TTL_EXPIRED"
		p.Restored = boolPtr(restored)
	}
	return ControlEvent{Type: t, Payload: p}
}

func valueOf(row protocol.ChannelControl) protocol.Value {
	raw, _ := json.Marshal(struct {
		Mode         protocol.ControlMode `json:"mode,omitempty"`
		TX           *bool                `json:"tx,omitempty"`
		RX           *bool                `json:"rx,omitempty"`
		AutoDelivery *bool                `json:"auto_delivery,omitempty"`
		AutoContinue *bool                `json:"auto_continue,omitempty"`
	}{row.Mode, row.TxEnabled, row.RxEnabled, row.AutoDeliveryEnabled, row.AutoContinueEnabled})
	return protocol.Value(raw)
}
func versionKey(row protocol.ChannelControl) string {
	if row.ScopeType == protocol.ScopeTypeLink {
		return linkVersionKey(row.FromAgent, row.ToAgent, row.Plane)
	}
	if row.ScopeType == protocol.ScopeTypeGlobal {
		return "global"
	}
	return string(row.ScopeType) + ":" + row.ScopeKey
}

func execInsert(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, row protocol.ChannelControl) error {
	query, err := queryText("controls_insert.sql")
	if err != nil {
		return err
	}
	args, err := controlParams(row)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, query, args...)
	return err
}
func execUpdate(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, row protocol.ChannelControl, expected int64) error {
	query, err := queryText("controls_update_cas.sql")
	if err != nil {
		return err
	}
	restore, err := encodeRestore(row.Restore)
	if err != nil {
		return err
	}
	res, err := q.ExecContext(ctx, query, nullString(string(row.Mode)), flagInt(row.TxEnabled), flagInt(row.RxEnabled), flagInt(row.AutoDeliveryEnabled), flagInt(row.AutoContinueEnabled), nullString(row.Reason), string(row.ActorType), row.CreatedBy, nullTime(row.ExpiresAt), restore, nullString(string(row.ThenMode)), row.Version, formatChannelTime(row.UpdatedAt), string(row.ScopeType), row.ScopeKey, string(row.Plane), expected)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return &VersionConflictError{CurrentVersion: expected}
	}
	return nil
}
func execDelete(ctx context.Context, q interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, row protocol.ChannelControl, expected int64) error {
	query, err := queryText("controls_delete_cas.sql")
	if err != nil {
		return err
	}
	res, err := q.ExecContext(ctx, query, string(row.ScopeType), row.ScopeKey, string(row.Plane), expected)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return &VersionConflictError{CurrentVersion: expected}
	}
	return nil
}

func (c *Controller) one(ctx context.Context, scope protocol.ScopeType, key string, plane protocol.Plane) (protocol.ChannelControl, bool, error) {
	rows, err := c.rows(ctx)
	if err != nil {
		return protocol.ChannelControl{}, false, err
	}
	for _, row := range rows {
		if row.ScopeType == scope && row.ScopeKey == key && row.Plane == plane {
			return row, true, nil
		}
	}
	return protocol.ChannelControl{}, false, nil
}
func (c *Controller) rows(ctx context.Context) ([]protocol.ChannelControl, error) {
	return readRows(ctx, c.db)
}
func readRows(ctx context.Context, q Queryer) ([]protocol.ChannelControl, error) {
	query, err := queryText("controls_select_all.sql")
	if err != nil {
		return nil, err
	}
	rs, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []protocol.ChannelControl
	for rs.Next() {
		row, err := scanControl(rs)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rs.Err()
}

func (c *Controller) mintControlID(now time.Time) (protocol.ControlID, error) {
	millis := now.UnixMilli()
	if millis < 0 || millis >= 1<<48 {
		return "", fmt.Errorf("time outside ULID range")
	}
	var raw [16]byte
	stamp := big.NewInt(millis).Bytes()
	copy(raw[6-len(stamp):6], stamp)
	if _, err := io.ReadFull(c.entropy, raw[6:]); err != nil {
		return "", err
	}
	encoded := new(big.Int).SetBytes(raw[:]).Text(32)
	const digits = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var out strings.Builder
	for out.Len()+len(encoded) < 26 {
		out.WriteByte('0')
	}
	for i := range len(encoded) {
		ch := encoded[i]
		index := int(ch - '0')
		if ch >= 'a' {
			index = int(ch-'a') + 10
		}
		out.WriteByte(digits[index])
	}
	return protocol.ControlID("ctl_" + out.String()), nil
}
