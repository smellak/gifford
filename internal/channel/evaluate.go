package channel

import (
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the precedence kernel of §6: the verdict, the per-level
// rules, their combination and the version vector.

// verdict is the outcome of one evaluation. It is not a stringer.
type verdict struct {
	outcome protocol.DecisionOutcome
	reason  protocol.DecisionReason
	class   restrictionClass
	line    protocol.ExplanationScope
	matched []protocol.ControlID
	winPos  int
	winRef  ControlRef
}

// allow builds an ALLOW verdict.
func allow(reason protocol.DecisionReason) verdict {
	return verdict{outcome: Allow, reason: reason, class: restrictionNone}
}

// hold builds a HOLD verdict.
func hold(reason protocol.DecisionReason) verdict {
	return verdict{outcome: Hold, reason: reason, class: restrictionHold}
}

// approval builds an APPROVAL verdict.
func approval(reason protocol.DecisionReason) verdict {
	return verdict{outcome: Approval, reason: reason, class: restrictionApproval}
}

// deny builds a DENY verdict.
func deny(reason protocol.DecisionReason) verdict {
	return verdict{outcome: Deny, reason: reason, class: restrictionDeny}
}

// restriction is one candidate from one position.
type restriction struct {
	ref    ControlRef
	class  restrictionClass
	reason protocol.DecisionReason
	line   protocol.ExplanationScope
	pos    int
}

// Precedence positions in evaluation order (§6.2).
const (
	posPolicy = iota
	posGlobal
	posNodeSender
	posNodeReceiver
	posTX
	posAutoContinue
	posRX
	posAutoDelivery
	posLink
	posThread
	posLoopReplica
	posLoopOverride
	posLoopRestore
	posHeld
	posPending
)

// evaluate resolves the query, runs the kernel and builds the version
// vector. It has no side effects. When the cache is not loaded or a
// needed key is stale it stops with a HOLD verdict and still builds the
// vector, so this path returns no error.
func (e *Engine) evaluate(q Query) (verdict, Effective, protocol.ControlVersions, error) {
	if err := q.Validate(); err != nil {
		return verdict{}, Effective{}, nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	now := e.clock.Now()
	v, eff, versions := e.viewLocked().evaluate(q, now)
	return v, eff, versions, nil
}

// evaluate resolves and evaluates against one locked cache view and one
// instant. The caller holds the engine's read lock for the view's lifetime.
func (v view) evaluate(q Query, now time.Time) (verdict, Effective, protocol.ControlVersions) {
	eff, err := v.resolve(q, now)
	if err != nil {
		versions := v.versions(q)
		if q.PolicyDenied {
			denied := deny(protocol.DecisionReasonPolicyDenied)
			denied.line = protocol.ExplanationScopePolicy
			denied.outcome = denyOutcome(q.Stage)
			return denied, Effective{}, versions
		}
		unavailable := hold(protocol.DecisionReasonControlUnavailable)
		unavailable.line = protocol.ExplanationScopeControls
		return unavailable, Effective{}, versions
	}
	found := v.collectRestrictions(q, eff, now)
	versions := v.versions(q)
	matched := matchedControls(eff)
	if len(found) == 0 {
		allowed := allow(protocol.DecisionReasonAllowed)
		allowed.matched = matched
		return allowed, eff, versions
	}
	best := found[0]
	for _, candidate := range found[1:] {
		if candidate.class > best.class {
			best = candidate
		}
	}
	var decided verdict
	switch best.class {
	case restrictionApproval:
		decided = approval(best.reason)
	case restrictionHold:
		decided = hold(best.reason)
	case restrictionDeny:
		decided = deny(best.reason)
		decided.outcome = denyOutcome(q.Stage)
	default:
		decided = allow(best.reason)
	}
	decided.line = best.line
	decided.matched = matched
	decided.winPos = best.pos
	decided.winRef = best.ref
	return decided, eff, versions
}

// denyOutcome maps the DENY class by stage: an object already in the queue
// is kept, never rejected or deleted.
func denyOutcome(stage protocol.DecisionStage) protocol.DecisionOutcome {
	if stage == protocol.DecisionStageDelivery {
		return Hold
	}
	return Deny
}

// adder appends one candidate restriction with its row for attribution.
type adder func(ref ControlRef, class restrictionClass, reason protocol.DecisionReason, line protocol.ExplanationScope, pos int)

// collectRestrictions runs the per-level rules in position order.
func (v view) collectRestrictions(q Query, eff Effective, now time.Time) []restriction {
	delivery := q.Stage == protocol.DecisionStageDelivery
	var found []restriction
	add := func(ref ControlRef, class restrictionClass, reason protocol.DecisionReason, line protocol.ExplanationScope, pos int) {
		found = append(found, restriction{ref: ref, class: class, reason: reason, line: line, pos: pos})
	}
	if q.PolicyDenied {
		add(ControlRef{}, restrictionDeny, protocol.DecisionReasonPolicyDenied, protocol.ExplanationScopePolicy, posPolicy)
	}
	v.channelRestrictions(q, eff, delivery, add)
	v.agentRestrictions(q, eff, delivery, now, add)
	v.linkRestrictions(q, eff, delivery, add)
	v.loopRestrictions(q, eff, delivery, now, add)
	if delivery && q.Object != nil {
		if q.Object.Held {
			add(ControlRef{}, restrictionHold, protocol.DecisionReasonObjectHeld, protocol.ExplanationScopeObject, posHeld)
		}
		if q.Object.ApprovalPending {
			add(ControlRef{}, restrictionApproval, protocol.DecisionReasonObjectApprovalPending, protocol.ExplanationScopeObject, posPending)
		}
	}
	return found
}

// rule is one mapped restriction: ok reports whether the mode restricts.
type rule struct {
	class  restrictionClass
	reason protocol.DecisionReason
	ok     bool
}

// channelRestrictions runs the global and node rules with their expired
// extras.
func (v view) channelRestrictions(q Query, eff Effective, delivery bool, add adder) {
	if ruled := channelRestriction(eff.Global.Mode, false, q, delivery); ruled.ok {
		add(eff.Global.ControlRef, ruled.class, ruled.reason, protocol.ExplanationScopeGlobal, posGlobal)
	}
	if target := v.extraTarget(protocol.ScopeTypeGlobal, "*", protocol.PlaneAll, eff.Global.ControlRef); target != nil {
		if again := channelRestriction(target.mode, false, q, delivery); again.ok {
			add(eff.Global.ControlRef, again.class, again.reason, protocol.ExplanationScopeGlobal, posGlobal)
		}
	}
	for i, node := range eff.Nodes {
		pos := posNodeSender
		if i > 0 {
			pos = posNodeReceiver
		}
		if ruled := channelRestriction(node.Mode, true, q, delivery); ruled.ok {
			add(node.ControlRef, ruled.class, ruled.reason, protocol.ExplanationScopeNode, pos)
		}
		key := cacheKey{scope: protocol.ScopeTypeNode, key: string(node.NodeID), plane: protocol.PlaneAll}
		if target := v.extraTarget(key.scope, key.key, key.plane, node.ControlRef); target != nil {
			if again := channelRestriction(target.mode, true, q, delivery); again.ok {
				add(node.ControlRef, again.class, again.reason, protocol.ExplanationScopeNode, pos)
			}
		}
	}
}

// channelRestriction maps a global or node mode. A PAUSED scope still
// accepts and persists before delivery and only holds at delivery; a
// DRAINING scope denies roots and replies that would open a loop, while a
// reply inside an existing loop passes.
func channelRestriction(mode protocol.ControlMode, node bool, q Query, delivery bool) rule {
	paused := protocol.DecisionReasonChannelPaused
	draining := protocol.DecisionReasonChannelDraining
	off := protocol.DecisionReasonChannelOff
	if node {
		paused = protocol.DecisionReasonNodePaused
		draining = protocol.DecisionReasonNodeDraining
		off = protocol.DecisionReasonNodeOff
	}
	switch protocol.ChannelMode(mode) {
	case protocol.ChannelModePaused:
		if delivery {
			return rule{class: restrictionHold, reason: paused, ok: true}
		}
		return rule{}
	case protocol.ChannelModeDraining:
		if delivery {
			return rule{}
		}
		if q.Kind == KindRoot || q.Loop == nil || q.openingLoop {
			return rule{class: restrictionDeny, reason: draining, ok: true}
		}
		return rule{}
	case protocol.ChannelModeOff:
		return rule{class: restrictionDeny, reason: off, ok: true}
	default:
		return rule{}
	}
}

// agentRestrictions runs the sender and receiver flag rules with their
// expired extras.
func (v view) agentRestrictions(q Query, eff Effective, delivery bool, now time.Time, add adder) {
	v.senderRestrictions(q, eff.Sender, delivery, add)
	if target := v.extraTarget(protocol.ScopeTypeAgent, string(q.From), protocol.PlaneAll, eff.Sender.ControlRef); target != nil {
		v.senderRestrictions(q, agentLevelOf(q.From, *target, true, now), delivery, add)
	}
	v.receiverRestrictions(eff.Receiver, delivery, q.approvedOnce, add)
	if target := v.extraTarget(protocol.ScopeTypeAgent, string(q.To), protocol.PlaneAll, eff.Receiver.ControlRef); target != nil {
		v.receiverRestrictions(agentLevelOf(q.To, *target, true, now), delivery, q.approvedOnce, add)
	}
}

// senderRestrictions runs the TX and AUTO_CONTINUE flags, which govern new
// objects only. A reply that would open an implicit loop is not affected
// by AUTO_CONTINUE here.
func (view) senderRestrictions(q Query, sender AgentLevel, delivery bool, add adder) {
	if !sender.TX && !delivery {
		add(sender.ControlRef, restrictionDeny, protocol.DecisionReasonAgentTXDisabled, protocol.ExplanationScopeSender, posTX)
	}
	if !sender.AutoContinue && !delivery && q.Kind == KindReply && q.Loop != nil {
		add(sender.ControlRef, restrictionApproval, protocol.DecisionReasonAgentAutoContinueOff, protocol.ExplanationScopeSender, posAutoContinue)
	}
}

// receiverRestrictions runs the RX and AUTO_DELIVERY flags. RX governs new
// objects only; AUTO_DELIVERY governs delivery only.
func (view) receiverRestrictions(receiver AgentLevel, delivery, approvedOnce bool, add adder) {
	if !receiver.RX && !delivery {
		add(receiver.ControlRef, restrictionDeny, protocol.DecisionReasonAgentRXDisabled, protocol.ExplanationScopeReceiver, posRX)
	}
	if !receiver.AutoDelivery && delivery && !approvedOnce {
		add(receiver.ControlRef, restrictionHold, protocol.DecisionReasonAgentAutoDeliveryOff, protocol.ExplanationScopeReceiver, posAutoDelivery)
	}
}

// linkRestrictions runs the link and thread rules with their expired
// extras, on every stage.
func (v view) linkRestrictions(q Query, eff Effective, delivery bool, add adder) {
	_ = delivery
	if ruled := linkRestriction(protocol.LinkMode(eff.Link.Mode)); ruled.ok {
		add(eff.Link.ControlRef, ruled.class, ruled.reason, protocol.ExplanationScopeLink, posLink)
	}
	link := linkKey(q.From, q.To)
	if target := v.extraTarget(protocol.ScopeTypeLink, link, q.Plane, eff.Link.ControlRef); target != nil {
		if again := linkRestriction(protocol.LinkMode(target.mode)); again.ok {
			add(eff.Link.ControlRef, again.class, again.reason, protocol.ExplanationScopeLink, posLink)
		}
	}
	if eff.Thread == nil {
		return
	}
	if ruled := linkRestriction(protocol.LinkMode(eff.Thread.Mode)); ruled.ok {
		add(eff.Thread.ControlRef, ruled.class, ruled.reason, protocol.ExplanationScopeThread, posThread)
	}
	if target := v.extraTarget(protocol.ScopeTypeThread, string(eff.Thread.ThreadID), protocol.PlaneAll, eff.Thread.ControlRef); target != nil {
		if again := linkRestriction(protocol.LinkMode(target.mode)); again.ok {
			add(eff.Thread.ControlRef, again.class, again.reason, protocol.ExplanationScopeThread, posThread)
		}
	}
}

// linkRestriction maps a link or thread mode; thread controls reuse the
// link reasons.
func linkRestriction(mode protocol.LinkMode) rule {
	switch mode {
	case protocol.LinkModeHold:
		return rule{class: restrictionHold, reason: protocol.DecisionReasonLinkHold, ok: true}
	case protocol.LinkModeRequireApproval:
		return rule{class: restrictionApproval, reason: protocol.DecisionReasonLinkApproval, ok: true}
	case protocol.LinkModeDeny:
		return rule{class: restrictionDeny, reason: protocol.DecisionReasonLinkDenied, ok: true}
	default:
		return rule{}
	}
}

// loopRestrictions runs the replica and override rules.
func (v view) loopRestrictions(q Query, eff Effective, delivery bool, now time.Time, add adder) {
	if eff.Loop == nil {
		return
	}
	ruled := replicaRestriction(q, delivery, now)
	if ruled.ok {
		add(ControlRef{}, ruled.class, ruled.reason, protocol.ExplanationScopeLoop, posLoopReplica)
	}
	if eff.Loop.Override != nil {
		v.loopOverrideRestrictions(eff, add)
	}
}

// loopOverrideRestrictions collects the local override and its restore
// target, if restrictive. It reports whether either restricted: the
// restore is read before returning, since it may beat the override.
func (v view) loopOverrideRestrictions(eff Effective, add adder) bool {
	over := eff.Loop.Override
	restricted := false
	if overruled := overrideRestriction(over.Mode); overruled.ok {
		add(over.ControlRef, overruled.class, overruled.reason, protocol.ExplanationScopeLoop, posLoopOverride)
		restricted = true
	}
	key := string(eff.Loop.LoopID)
	if target := v.extraTarget(protocol.ScopeTypeLoop, key, protocol.PlaneAll, over.ControlRef); target != nil {
		if again := overrideRestriction(target.mode); again.ok {
			add(over.ControlRef, again.class, again.reason, protocol.ExplanationScopeLoop, posLoopRestore)
			restricted = true
		}
	}
	return restricted
}

// replicaRestriction maps the loop replica fields; before delivery the
// first rule that matches wins, each column read on its own.
func replicaRestriction(q Query, delivery bool, now time.Time) rule {
	rep := q.Loop
	if !delivery {
		switch rep.Mode {
		case protocol.LoopModeStopped, protocol.LoopModeCompleted:
			return rule{class: restrictionDeny, reason: protocol.DecisionReasonLoopStopped, ok: true}
		case protocol.LoopModeDraining:
			return rule{class: restrictionDeny, reason: protocol.DecisionReasonLoopDraining, ok: true}
		default:
		}
		if hardLimitReached(q, rep, now) {
			return rule{class: restrictionDeny, reason: protocol.DecisionReasonLoopBudgetExhausted, ok: true}
		}
		if rep.Mode == protocol.LoopModePaused {
			return rule{class: restrictionHold, reason: stateReasonMapping(rep.StateReason), ok: true}
		}
		if rep.Mode == protocol.LoopModeStep && rep.Counters.StepTokens == 0 {
			return rule{class: restrictionApproval, reason: protocol.DecisionReasonLoopStepTokenRequired, ok: true}
		}
		if rep.Mode == protocol.LoopModeAuto && q.Plane == protocol.PlaneMessage &&
			opensTurn(q, rep) && rep.Counters.TurnCount >= rep.Budget.MaxTurns {
			return rule{class: restrictionHold, reason: protocol.DecisionReasonLoopBudgetExhausted, ok: true}
		}
		return rule{}
	}
	switch rep.Mode {
	case protocol.LoopModeStopped, protocol.LoopModeCompleted:
		return rule{class: restrictionDeny, reason: protocol.DecisionReasonLoopStopped, ok: true}
	case protocol.LoopModeDraining:
		return rule{}
	default:
	}
	if rep.Mode == protocol.LoopModePaused {
		return rule{class: restrictionHold, reason: stateReasonMapping(rep.StateReason), ok: true}
	}
	if !rep.ExpiresAt.After(now) {
		return rule{class: restrictionHold, reason: protocol.DecisionReasonLoopBudgetExhausted, ok: true}
	}
	if rep.Mode == protocol.LoopModeStep {
		// A durable grant tied to the exact delivery is introduced in 8.1b.
		// Until then every STEP delivery fails closed even if a replica carries
		// legacy aggregate tokens.
		return rule{class: restrictionApproval, reason: protocol.DecisionReasonLoopStepTokenRequired, ok: true}
	}
	return rule{}
}

// opensTurn reports whether the object opens a turn: the loop has no
// sender yet or the sender differs. Three messages in a row from the same
// sender are one turn.
func opensTurn(q Query, rep *protocol.Loop) bool {
	return rep.LastSenderAgent == "" || rep.LastSenderAgent != q.From
}

// hardLimitReached checks the loop arithmetic of §6.4, before delivery
// only. The hop count is the one the object's envelope will carry, so the
// replica's own hop counter is not read here. Counters compare without
// adding, so a counter at MaxInt64 denies instead of wrapping around.
func hardLimitReached(q Query, rep *protocol.Loop, now time.Time) bool {
	return hardLimitDimension(q, rep, now) != ""
}

func hardLimitDimension(q Query, rep *protocol.Loop, now time.Time) protocol.BudgetDimension {
	messageLimitReached := rep.Counters.MessageCount >= rep.Budget.MaxMessages
	if q.openingLoop {
		// An opening plan already includes the reply being evaluated, while an
		// established replica contains only previously reserved messages.
		messageLimitReached = rep.Counters.MessageCount > rep.Budget.MaxMessages
	}
	if q.Plane == protocol.PlaneMessage && messageLimitReached {
		return protocol.BudgetDimensionMaxMessages
	}
	if !now.Before(rep.ExpiresAt) {
		return protocol.BudgetDimensionMaxDurationMS
	}
	if q.HopCount > rep.Budget.MaxHops {
		return protocol.BudgetDimensionMaxHops
	}
	if q.Plane == protocol.PlaneArtifact && rep.Counters.ArtifactCount >= rep.Budget.MaxArtifactCount {
		return protocol.BudgetDimensionMaxArtifactCount
	}
	if q.Plane == protocol.PlaneArtifact &&
		q.ArtifactBytes > rep.Budget.MaxArtifactBytes-rep.Counters.ArtifactBytes {
		return protocol.BudgetDimensionMaxArtifactBytes
	}
	return ""
}

// stateReasonMapping maps a pause reason: anything but an exhaustion or a
// detected pattern is a plain pause.
func stateReasonMapping(reason protocol.LoopStateReason) protocol.DecisionReason {
	switch reason {
	case protocol.LoopStateReasonBudgetExhausted:
		return protocol.DecisionReasonLoopBudgetExhausted
	case protocol.LoopStateReasonLoopPatternDetected:
		return protocol.DecisionReasonLoopPatternDetected
	default:
		return protocol.DecisionReasonLoopPaused
	}
}

// overrideRestriction maps a loop override mode, on every stage.
func overrideRestriction(mode protocol.ControlMode) rule {
	switch protocol.LoopMode(mode) {
	case protocol.LoopModePaused:
		return rule{class: restrictionHold, reason: protocol.DecisionReasonLoopPaused, ok: true}
	case protocol.LoopModeStopped:
		return rule{class: restrictionDeny, reason: protocol.DecisionReasonLoopStopped, ok: true}
	default:
		return rule{}
	}
}

// extraTarget returns the restore target of an expired live row, if any.
// Both the row and its target belong to the same position and line.
func (v view) extraTarget(scope protocol.ScopeType, key string, plane protocol.Plane, ref ControlRef) *control {
	if ref.ControlID == "" || !ref.Expired {
		return nil
	}
	entry, ok := v.entries[cacheKey{scope: scope, key: key, plane: plane}]
	if !ok || entry.tombstone {
		return nil
	}
	// Unreachable in practice: load and apply reject illegal restores, so
	// the target of a cached row always validates.
	target, err := restoreTarget(entry.row)
	if err != nil {
		return nil
	}
	return target
}

// matchedControls lists the live rows consulted, without duplicates.
func matchedControls(eff Effective) []protocol.ControlID {
	var matched []protocol.ControlID
	seen := make(map[protocol.ControlID]bool)
	add := func(id protocol.ControlID) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		matched = append(matched, id)
	}
	add(eff.Global.ControlID)
	for _, node := range eff.Nodes {
		add(node.ControlID)
	}
	add(eff.Sender.ControlID)
	add(eff.Receiver.ControlID)
	add(eff.Link.ControlID)
	if eff.Thread != nil {
		add(eff.Thread.ControlID)
	}
	if eff.Loop != nil && eff.Loop.Override != nil {
		add(eff.Loop.Override.ControlID)
	}
	return matched
}

// versionsOf builds the version vector of an evaluation: live rows and
// tombstones carry their versions, unknown keys read 0. Cache-backed keys
// reuse the key grammar; only the loop replica keys are spelled out.
func versionsOf(q Query, at func(scope protocol.ScopeType, key string, plane protocol.Plane) int64) protocol.ControlVersions {
	versionAt := func(key cacheKey) int64 {
		return at(key.scope, key.key, key.plane)
	}
	global := cacheKey{scope: protocol.ScopeTypeGlobal, key: "*", plane: protocol.PlaneAll}
	vers := protocol.ControlVersions{global.String(): versionAt(global)}
	nodes := []protocol.NodeID{q.FromNode}
	if q.ToNode != q.FromNode {
		nodes = append(nodes, q.ToNode)
	}
	for _, node := range nodes {
		key := cacheKey{scope: protocol.ScopeTypeNode, key: string(node), plane: protocol.PlaneAll}
		vers[key.String()] = versionAt(key)
	}
	for _, agent := range []protocol.AgentID{q.From, q.To} {
		key := cacheKey{scope: protocol.ScopeTypeAgent, key: string(agent), plane: protocol.PlaneAll}
		vers[key.String()] = versionAt(key)
	}
	link := cacheKey{scope: protocol.ScopeTypeLink, key: linkKey(q.From, q.To), plane: q.Plane}
	vers[link.String()] = versionAt(link)
	if threadID := effectiveThreadID(q); threadID != "" {
		key := cacheKey{scope: protocol.ScopeTypeThread, key: string(threadID), plane: protocol.PlaneAll}
		vers[key.String()] = versionAt(key)
	}
	if q.Loop != nil {
		loopID := string(q.Loop.LoopID)
		vers["loop:"+loopID] = q.Loop.ControlVersion
		vers["loop_generation:"+loopID] = q.Loop.LoopGeneration
		override := cacheKey{scope: protocol.ScopeTypeLoop, key: loopID, plane: protocol.PlaneAll}
		vers["loop_override:"+loopID] = versionAt(override)
	}
	return vers
}

// versions builds the vector from the locked cache view.
func (v view) versions(q Query) protocol.ControlVersions {
	return versionsOf(q, func(scope protocol.ScopeType, key string, plane protocol.Plane) int64 {
		if entry, ok := v.entries[cacheKey{scope: scope, key: key, plane: plane}]; ok {
			return entry.version
		}
		return 0
	})
}
