package channel

import (
	"fmt"
	"sync"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the evaluation query and the engine of §5: the cache of
// control rows, its compare-and-swap operations and the level resolution.

// Kind tells a root message from a reply; it matters only before delivery.
type Kind string

// Message kinds.
const (
	KindRoot  Kind = "root"
	KindReply Kind = "reply"
)

// Object is the concrete message or artifact being evaluated.
type Object struct {
	Type            protocol.DecisionObjectType // message or artifact; must match Query.Plane
	ID              protocol.OpaqueID
	Held            bool // an explicit hold on this object; delivery stage only
	ApprovalPending bool // a PENDING approval request exists for it; delivery stage only
}

// Query is one evaluation request. The caller fills it from the envelope,
// the registry and the loop replica; the engine adds the controls.
type Query struct {
	Stage         protocol.DecisionStage // admission, acceptance, continuation or delivery
	Plane         protocol.Plane         // message or artifact
	From          protocol.AgentID       // sender
	FromNode      protocol.NodeID        // sender's node
	To            protocol.AgentID       // receiver
	ToNode        protocol.NodeID        // receiver's node
	ToDeliverable bool                   // receiver manifest; copied into EffectiveControl, never evaluated here
	Kind          Kind                   // required before delivery, ignored at delivery
	ThreadID      protocol.ThreadID      // optional
	Loop          *protocol.Loop         // the loop replica, when the object belongs to a loop
	Object        *Object                // optional
	PolicyDenied  bool                   // level 0: the Security Policy Engine denied it
	HopCount      int64                  // hop_count the object will carry (budget check)
	ArtifactBytes int64                  // size of the reservation (artifact plane)
}

// Validate checks the query, wrapping ErrInvalidQuery with the field.
func (q Query) Validate() error {
	if err := q.validateAddress(); err != nil {
		return err
	}
	switch q.Stage {
	case protocol.DecisionStageAdmission, protocol.DecisionStageAcceptance,
		protocol.DecisionStageContinuation, protocol.DecisionStageDelivery:
	default:
		return fmt.Errorf("%w: stage", ErrInvalidQuery)
	}
	beforeDelivery := q.Stage != protocol.DecisionStageDelivery
	if beforeDelivery && q.Kind != KindRoot && q.Kind != KindReply {
		return fmt.Errorf("%w: kind", ErrInvalidQuery)
	}
	if q.Stage == protocol.DecisionStageContinuation && (q.Kind != KindReply || q.Loop == nil) {
		return fmt.Errorf("%w: kind", ErrInvalidQuery)
	}
	if beforeDelivery && q.Kind == KindRoot && q.Loop != nil {
		return fmt.Errorf("%w: loop", ErrInvalidQuery)
	}
	if err := checkQueryLoop(q.Loop); err != nil {
		return err
	}
	if q.ThreadID != "" && q.Loop != nil && q.Loop.ThreadID != "" && q.ThreadID != q.Loop.ThreadID {
		return fmt.Errorf("%w: thread_id", ErrInvalidQuery)
	}
	if q.Object != nil {
		if string(q.Object.Type) != string(q.Plane) || !q.Object.ID.Valid() {
			return fmt.Errorf("%w: object", ErrInvalidQuery)
		}
		if beforeDelivery && (q.Object.Held || q.Object.ApprovalPending) {
			return fmt.Errorf("%w: object", ErrInvalidQuery)
		}
	}
	if q.HopCount < 0 {
		return fmt.Errorf("%w: hop_count", ErrInvalidQuery)
	}
	if q.ArtifactBytes < 0 {
		return fmt.Errorf("%w: artifact_bytes", ErrInvalidQuery)
	}
	return nil
}

// validateAddress checks the fields Effective reads: the plane, the agents
// and nodes, the thread and the loop.
func (q Query) validateAddress() error {
	if q.Plane != protocol.PlaneMessage && q.Plane != protocol.PlaneArtifact {
		return fmt.Errorf("%w: plane", ErrInvalidQuery)
	}
	if !q.From.Valid() {
		return fmt.Errorf("%w: from", ErrInvalidQuery)
	}
	if q.From == q.To {
		return fmt.Errorf("%w: from", ErrInvalidQuery)
	}
	if !q.To.Valid() {
		return fmt.Errorf("%w: to", ErrInvalidQuery)
	}
	if !q.FromNode.Valid() {
		return fmt.Errorf("%w: from_node", ErrInvalidQuery)
	}
	if !q.ToNode.Valid() {
		return fmt.Errorf("%w: to_node", ErrInvalidQuery)
	}
	if q.ThreadID != "" && !q.ThreadID.Valid() {
		return fmt.Errorf("%w: thread_id", ErrInvalidQuery)
	}
	return checkQueryLoop(q.Loop)
}

// checkQueryLoop checks the loop fields the engine reads; nil is fine. It
// never calls Loop.Validate: only what the engine reads is checked.
func checkQueryLoop(loop *protocol.Loop) error {
	if loop == nil {
		return nil
	}
	if !loop.LoopID.Valid() || loop.LoopGeneration < 1 || !loop.Mode.Valid() ||
		loop.ExpiresAt.IsZero() || !loop.LastSenderAgent.Valid() && loop.LastSenderAgent != "" {
		return fmt.Errorf("%w: loop", ErrInvalidQuery)
	}
	counters := []int64{
		loop.Counters.TurnCount, loop.Counters.MessageCount, loop.Counters.HopCount,
		loop.Counters.ArtifactCount, loop.Counters.ArtifactBytes, loop.Counters.StepTokens,
	}
	for _, counter := range counters {
		if counter < 0 {
			return fmt.Errorf("%w: loop", ErrInvalidQuery)
		}
	}
	limits := []int64{
		loop.Budget.MaxTurns, loop.Budget.MaxMessages, loop.Budget.MaxDurationMS,
		loop.Budget.MaxMessagesPerMinute, loop.Budget.MaxHops, loop.Budget.MaxConsecutiveSamePair,
		loop.Budget.MaxRetries, loop.Budget.MaxArtifactCount, loop.Budget.MaxArtifactBytes,
	}
	for _, limit := range limits {
		if limit < 0 {
			return fmt.Errorf("%w: loop", ErrInvalidQuery)
		}
	}
	return nil
}

// Options configures an Engine.
type Options struct {
	Clock clock.Clock // required; the only source of time
}

// cacheKey identifies one cache entry.
type cacheKey struct {
	scope protocol.ScopeType
	key   string
	plane protocol.Plane
}

// cacheEntry is one cached row: live or tombstone, with its version.
type cacheEntry struct {
	row       control
	tombstone bool
	version   int64
}

// keySep separates the parts of a version-vector key.
const keySep = ":"

// String formats the key in the version-vector grammar.
func (k cacheKey) String() string {
	switch k.scope {
	case protocol.ScopeTypeGlobal:
		return "global"
	case protocol.ScopeTypeNode, protocol.ScopeTypeAgent:
		return string(k.scope) + keySep + k.key
	case protocol.ScopeTypeLink:
		return "link" + keySep + k.key + keySep + string(k.plane)
	case protocol.ScopeTypeThread:
		return "thread" + keySep + k.key
	case protocol.ScopeTypeLoop:
		return "loop" + keySep + k.key
	default:
		return string(k.scope) + keySep + k.key
	}
}

// Engine resolves and evaluates channel controls from an in-memory cache.
type Engine struct {
	clock   clock.Clock
	mu      sync.RWMutex
	loaded  bool
	entries map[cacheKey]cacheEntry
	stale   map[cacheKey]int64
}

// New returns an Engine whose cache is not loaded yet: until the first
// successful load every evaluation answers HOLD with CONTROL_UNAVAILABLE.
func New(opts Options) (*Engine, error) {
	if opts.Clock == nil {
		return nil, ErrInvalidOptions
	}
	return &Engine{
		clock:   opts.Clock,
		entries: make(map[cacheKey]cacheEntry),
		stale:   make(map[cacheKey]int64),
	}, nil
}

// load validates every row and replaces the cache; an empty slice is a
// valid load. On any error the cache becomes not loaded and the error
// wraps ErrInvalidControl.
func (e *Engine) load(rows []protocol.ChannelControl) error {
	converted := make([]control, 0, len(rows))
	for _, root := range rows {
		if err := root.Validate(); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidControl, err)
		}
		converted = append(converted, controlFromRoot(root))
	}
	if err := validateControl(converted); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidControl, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries = make(map[cacheKey]cacheEntry, len(converted))
	for _, row := range converted {
		key := cacheKey{scope: row.scope, key: row.key, plane: row.plane}
		e.entries[key] = cacheEntry{row: row.copy(), version: row.version}
	}
	e.stale = make(map[cacheKey]int64)
	e.loaded = true
	return nil
}

// apply validates the row and stores it when no entry exists for its key
// or its version is strictly newer; it reports whether it stored.
func (e *Engine) apply(row protocol.ChannelControl) (bool, error) {
	if err := row.Validate(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrInvalidControl, err)
	}
	control := controlFromRoot(row)
	if violation := validateControlRow(control); violation != nil {
		return false, fmt.Errorf("%w: %w", ErrInvalidControl, violation)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	key := cacheKey{scope: control.scope, key: control.key, plane: control.plane}
	if entry, ok := e.entries[key]; ok && entry.version >= control.version {
		return false, nil
	}
	e.entries[key] = cacheEntry{row: control.copy(), version: control.version}
	if stale, ok := e.stale[key]; ok && control.version >= stale {
		delete(e.stale, key)
	}
	return true, nil
}

// remove replaces the entry by a tombstone at version under the strictly
// newer rule, and reports whether it stored.
func (e *Engine) remove(key cacheKey, version int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if entry, ok := e.entries[key]; ok && entry.version >= version {
		return false
	}
	e.entries[key] = cacheEntry{tombstone: true, version: version}
	if stale, ok := e.stale[key]; ok && version >= stale {
		delete(e.stale, key)
	}
	return true
}

// invalidate marks the key stale when its cached version is lower than
// version; evaluations needing it answer CONTROL_UNAVAILABLE until a
// newer apply or remove, or a load.
func (e *Engine) invalidate(key cacheKey, version int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if entry, ok := e.entries[key]; ok && entry.version >= version {
		return
	}
	if stale, ok := e.stale[key]; !ok || version > stale {
		e.stale[key] = version
	}
}

// Effective resolves the effective control for q: each level shows the
// live row with its ControlRef or the default with a zero ControlRef.
// Expired marks a live row whose expires_at has passed. It returns
// ErrControlUnavailable, wrapped with the key, when the cache is not
// loaded or a needed key is stale.
func (e *Engine) Effective(q Query) (Effective, error) {
	if err := q.validateAddress(); err != nil {
		return Effective{}, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.resolveLocked(q)
}

// resolveLocked fills an Effective from the cache; the caller holds the
// read lock. Every level it returns is freshly built: cached entries
// never escape.
func (e *Engine) resolveLocked(q Query) (Effective, error) {
	if !e.loaded {
		return Effective{}, ErrControlUnavailable
	}
	eff := Effective{Plane: q.Plane, FromAgent: q.From, ToAgent: q.To}
	lookup := func(scope protocol.ScopeType, key string, plane protocol.Plane) (control, bool, error) {
		cache := cacheKey{scope: scope, key: key, plane: plane}
		if _, marked := e.stale[cache]; marked {
			return control{}, false, fmt.Errorf("%w: %s", ErrControlUnavailable, cache)
		}
		entry, ok := e.entries[cache]
		if !ok || entry.tombstone {
			return control{}, false, nil
		}
		return entry.row.copy(), true, nil
	}
	global, ok, err := lookup(protocol.ScopeTypeGlobal, "*", protocol.PlaneAll)
	if err != nil {
		return Effective{}, err
	}
	eff.Global = modeLevelOf(global, ok, e.clock)
	senderNode, ok, err := lookup(protocol.ScopeTypeNode, string(q.FromNode), protocol.PlaneAll)
	if err != nil {
		return Effective{}, err
	}
	eff.Nodes = append(eff.Nodes, NodeLevel{NodeID: q.FromNode, ModeLevel: modeLevelOf(senderNode, ok, e.clock)})
	if q.ToNode != q.FromNode {
		receiverNode, ok, err := lookup(protocol.ScopeTypeNode, string(q.ToNode), protocol.PlaneAll)
		if err != nil {
			return Effective{}, err
		}
		eff.Nodes = append(eff.Nodes, NodeLevel{NodeID: q.ToNode, ModeLevel: modeLevelOf(receiverNode, ok, e.clock)})
	}
	sender, ok, err := lookup(protocol.ScopeTypeAgent, string(q.From), protocol.PlaneAll)
	if err != nil {
		return Effective{}, err
	}
	eff.Sender = agentLevelOf(q.From, sender, ok, e.clock)
	receiver, ok, err := lookup(protocol.ScopeTypeAgent, string(q.To), protocol.PlaneAll)
	if err != nil {
		return Effective{}, err
	}
	eff.Receiver = agentLevelOf(q.To, receiver, ok, e.clock)
	link, ok, err := lookup(protocol.ScopeTypeLink, linkKey(q.From, q.To), q.Plane)
	if err != nil {
		return Effective{}, err
	}
	eff.Link = modeLevelOf(link, ok, e.clock)
	if threadID := effectiveThreadID(q); threadID != "" {
		thread, ok, err := lookup(protocol.ScopeTypeThread, string(threadID), protocol.PlaneAll)
		if err != nil {
			return Effective{}, err
		}
		if ok {
			eff.Thread = &ThreadLevel{ThreadID: threadID, ModeLevel: modeLevelOf(thread, true, e.clock)}
		}
	}
	if q.Loop != nil {
		loop := LoopLevel{
			LoopID:         q.Loop.LoopID,
			Generation:     q.Loop.LoopGeneration,
			Mode:           q.Loop.Mode,
			StateReason:    q.Loop.StateReason,
			ControlVersion: q.Loop.ControlVersion,
			Budget:         q.Loop.Budget,
			Counters:       q.Loop.Counters,
			ExpiresAt:      q.Loop.ExpiresAt,
		}
		override, ok, err := lookup(protocol.ScopeTypeLoop, string(q.Loop.LoopID), protocol.PlaneAll)
		if err != nil {
			return Effective{}, err
		}
		if ok {
			level := modeLevelOf(override, true, e.clock)
			loop.Override = &level
		}
		eff.Loop = &loop
	}
	return eff, nil
}

// effectiveThreadID returns the query's thread: its own id, or the loop's
// when its own is empty.
func effectiveThreadID(q Query) protocol.ThreadID {
	if q.ThreadID != "" {
		return q.ThreadID
	}
	if q.Loop != nil {
		return q.Loop.ThreadID
	}
	return ""
}

// refOf builds the ControlRef of a live row, judging expiry once.
func refOf(row control, clock clock.Clock) ControlRef {
	expired := !row.expiresAt.IsZero() && !row.expiresAt.After(clock.Now())
	return ControlRef{
		ControlID: row.id,
		Version:   row.version,
		Reason:    row.reason,
		ActorType: row.actor,
		CreatedBy: row.createdBy,
		ExpiresAt: row.expiresAt,
		Expired:   expired,
	}
}

// modeLevelOf fills a mode level from a live row or the default.
func modeLevelOf(row control, ok bool, clock clock.Clock) ModeLevel {
	if !ok {
		return ModeLevel{}
	}
	return ModeLevel{Mode: row.mode, ThenMode: row.thenMode, ControlRef: refOf(row, clock)}
}

// agentLevelOf fills an agent level with the row flags after defaults.
func agentLevelOf(agent protocol.AgentID, row control, ok bool, clock clock.Clock) AgentLevel {
	level := AgentLevel{AgentID: agent, TX: true, RX: true, AutoDelivery: true, AutoContinue: true}
	if !ok {
		return level
	}
	if row.tx != nil {
		level.TX = *row.tx
	}
	if row.rx != nil {
		level.RX = *row.rx
	}
	if row.autoDeliv != nil {
		level.AutoDelivery = *row.autoDeliv
	}
	if row.autoCont != nil {
		level.AutoContinue = *row.autoCont
	}
	level.ControlRef = refOf(row, clock)
	return level
}
