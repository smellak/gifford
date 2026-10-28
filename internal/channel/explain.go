package channel

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

// This file holds the explanation of §8: Explain builds the protocol
// explanation of an evaluation, recordOf assembles its decision record and
// Render prints any explanation as text.

// valueSep joins the parts of loop values and note parts.
const valueSep = " · "

// Explain runs the same evaluation a gate would and returns a
// protocol.Explanation. Its only error is ErrInvalidQuery: unreadable
// controls produce the CONTROL_UNAVAILABLE explanation, not an error.
func (e *Engine) Explain(q Query) (protocol.Explanation, error) {
	v, eff, _, err := e.evaluate(q)
	if err != nil {
		return protocol.Explanation{}, err
	}
	return e.explainResult(q, v, eff), nil
}

// explainResult builds the explanation of one evaluated query.
func (e *Engine) explainResult(q Query, v verdict, eff Effective) protocol.Explanation {
	x := protocol.Explanation{
		FromAgent: q.From,
		ToAgent:   q.To,
		Plane:     q.Plane,
		Outcome:   v.outcome,
		Reason:    v.reason,
	}
	if q.Object != nil {
		x.ObjectType = q.Object.Type
		x.ObjectID = q.Object.ID
	}
	if threadID := effectiveThreadID(q); threadID != "" {
		x.ThreadID = threadID
	}
	if q.Loop != nil {
		x.LoopID = q.Loop.LoopID
		generation := q.Loop.LoopGeneration
		x.LoopGeneration = &generation
	}
	if v.outcome != Allow {
		x.Blocker = v.line
	}
	x.Lines = explainLines(q, v, eff)
	return x
}

// explainLines builds the lines in scope order, marking the blocking one.
// It is a free function so it cannot re-read the clock or the
// restrictions: everything comes from the verdict and the effective view.
func explainLines(q Query, v verdict, eff Effective) []protocol.ExplanationLine {
	var lines []protocol.ExplanationLine
	if q.PolicyDenied {
		lines = append(lines, protocol.ExplanationLine{
			Scope: protocol.ExplanationScopePolicy,
			Key:   "*",
			Value: "DENY",
		})
	}
	if eff.Plane == "" {
		lines = append(lines, protocol.ExplanationLine{
			Scope: protocol.ExplanationScopeControls,
			Key:   "*",
			Value: "unavailable",
		})
		return markBlocking(lines, v, eff)
	}
	lines = append(lines, channelLine(protocol.ExplanationScopeGlobal, "*", eff.Global.Mode, eff.Global.ThenMode, eff.Global.ControlRef))
	for _, node := range eff.Nodes {
		lines = append(lines, channelLine(protocol.ExplanationScopeNode, string(node.NodeID), node.Mode, node.ThenMode, node.ControlRef))
	}
	lines = append(lines, senderLine(q, eff.Sender))
	lines = append(lines, receiverLine(eff.Receiver))
	lines = append(lines, linkLine(q, eff.Link.Mode, eff.Link.ControlRef))
	if threadID := effectiveThreadID(q); threadID != "" {
		lines = append(lines, threadLine(threadID, eff.Thread))
	}
	if eff.Loop != nil {
		lines = append(lines, loopLine(q, eff.Loop, v))
	}
	if q.Object != nil {
		lines = append(lines, objectLine(q.Object))
	}
	return markBlocking(lines, v, eff)
}

// channelLine builds a global or node line.
func channelLine(scope protocol.ExplanationScope, key string, mode, thenMode protocol.ControlMode, ref ControlRef) protocol.ExplanationLine {
	line := protocol.ExplanationLine{
		Scope: scope,
		Key:   key,
		Value: expiredValue(channelValue(mode, thenMode), ref),
	}
	rowLine(&line, ref)
	return line
}

// channelValue renders a channel mode, defaulting to ACTIVE. A draining
// scope with then_mode OFF shows where it is going.
func channelValue(mode, thenMode protocol.ControlMode) string {
	if mode == "" {
		return string(protocol.ChannelModeActive)
	}
	if mode == protocol.ControlModeDraining && thenMode == protocol.ControlModeOff {
		return "DRAINING → OFF"
	}
	return string(mode)
}

// senderLine builds the sender line: one TX flag, or TX and AUTO_CONTINUE
// for a reply before delivery. Render derives the sub-label from the
// value's shape.
func senderLine(q Query, sender AgentLevel) protocol.ExplanationLine {
	value := onOff(sender.TX)
	if q.Stage != protocol.DecisionStageDelivery && q.Kind == KindReply {
		value += " / " + onOff(sender.AutoContinue)
	}
	line := protocol.ExplanationLine{
		Scope: protocol.ExplanationScopeSender,
		Key:   string(sender.AgentID),
		Value: value,
	}
	rowLine(&line, sender.ControlRef)
	return line
}

// receiverLine builds the receiver line, always both flags.
func receiverLine(receiver AgentLevel) protocol.ExplanationLine {
	line := protocol.ExplanationLine{
		Scope: protocol.ExplanationScopeReceiver,
		Key:   string(receiver.AgentID),
		Value: onOff(receiver.RX) + " / " + onOff(receiver.AutoDelivery),
	}
	rowLine(&line, receiver.ControlRef)
	return line
}

// linkLine builds the link line on the query's plane.
func linkLine(q Query, mode protocol.ControlMode, ref ControlRef) protocol.ExplanationLine {
	value := string(protocol.LinkModeAllow)
	if mode != "" {
		value = string(mode)
	}
	line := protocol.ExplanationLine{
		Scope: protocol.ExplanationScopeLink,
		Key:   linkKey(q.From, q.To),
		Value: expiredValue(value, ref),
	}
	rowLine(&line, ref)
	return line
}

// threadLine builds the thread line: the mode, or a dash without a row.
func threadLine(threadID protocol.ThreadID, thread *ThreadLevel) protocol.ExplanationLine {
	line := protocol.ExplanationLine{
		Scope: protocol.ExplanationScopeThread,
		Key:   string(threadID),
		Value: "—",
	}
	if thread != nil {
		line.Value = expiredValue(string(thread.Mode), thread.ControlRef)
		rowLine(&line, thread.ControlRef)
	}
	return line
}

// loopLine builds the loop line: the replica value, never a version. It
// carries the winning loop-scope row's fields when the verdict's blocker
// position is the override or its restore; a replica winner carries no
// row. The verdict is the only source: the line never re-reads the clock
// or the restrictions, so a replica that expires while the lines are
// built changes nothing.
func loopLine(q Query, loop *LoopLevel, v verdict) protocol.ExplanationLine {
	line := protocol.ExplanationLine{
		Scope: protocol.ExplanationScopeLoop,
		Key:   string(loop.LoopID),
		Value: loopValue(q.Plane, loop),
	}
	if loop.Override == nil || v.outcome == Allow || v.line != protocol.ExplanationScopeLoop {
		return line
	}
	if v.winPos != posLoopOverride && v.winPos != posLoopRestore {
		return line
	}
	if ref := v.winRef; ref.ControlID != "" {
		line.ControlID = ref.ControlID
		line.ActorType = ref.ActorType
		line.ActorID = ref.CreatedBy
		line.Reason = ref.Reason
		line.ExpiresAt = ref.ExpiresAt
	}
	return line
}

// loopValue renders the replica: the mode, the local override, the step
// tokens in STEP only, the turn and message counters and, on the artifact
// plane, the artifact counter.
func loopValue(plane protocol.Plane, loop *LoopLevel) string {
	var value strings.Builder
	value.WriteString(string(loop.Mode))
	if loop.Override != nil {
		value.WriteString(" / local " + string(loop.Override.Mode))
	}
	if loop.Mode == protocol.LoopModeStep {
		tokens := loop.Counters.StepTokens
		if tokens == 1 {
			value.WriteString(valueSep + "1 token")
		} else {
			value.WriteString(valueSep + strconv.FormatInt(tokens, 10) + " tokens")
		}
	}
	value.WriteString(valueSep + strconv.FormatInt(loop.Counters.TurnCount, 10) + "/" +
		strconv.FormatInt(loop.Budget.MaxTurns, 10) + " turns" + valueSep +
		strconv.FormatInt(loop.Counters.MessageCount, 10) + "/" +
		strconv.FormatInt(loop.Budget.MaxMessages, 10) + " messages")
	if plane == protocol.PlaneArtifact {
		value.WriteString(valueSep + strconv.FormatInt(loop.Counters.ArtifactCount, 10) + "/" +
			strconv.FormatInt(loop.Budget.MaxArtifactCount, 10) + " artifacts")
	}
	return value.String()
}

// objectLine builds the object line.
func objectLine(object *Object) protocol.ExplanationLine {
	value := "none"
	switch {
	case object.Held && object.ApprovalPending:
		value = "held" + valueSep + "approval pending"
	case object.Held:
		value = "held"
	case object.ApprovalPending:
		value = "approval pending"
	}
	return protocol.ExplanationLine{
		Scope: protocol.ExplanationScopeObject,
		Key:   string(object.ID),
		Value: value,
	}
}

// rowLine fills the row fields of a line backed by a live control row.
// Defaults and tombstones carry none.
func rowLine(line *protocol.ExplanationLine, ref ControlRef) {
	if ref.ControlID == "" {
		return
	}
	line.ControlID = ref.ControlID
	version := ref.Version
	line.Version = &version
	line.ActorType = ref.ActorType
	line.ActorID = ref.CreatedBy
	line.Reason = ref.Reason
	line.ExpiresAt = ref.ExpiresAt
}

// expiredValue suffixes the value of an expired live row.
func expiredValue(value string, ref ControlRef) string {
	if ref.ControlID != "" && ref.Expired {
		return value + " (expired)"
	}
	return value
}

// onOff renders a flag.
func onOff(set bool) string {
	if set {
		return "ON"
	}
	return "OFF"
}

// markBlocking marks the blocker's line: the only line of the blocker's
// scope, or the winning node line. Nothing is marked on ALLOW.
func markBlocking(lines []protocol.ExplanationLine, v verdict, eff Effective) []protocol.ExplanationLine {
	if v.outcome == Allow {
		return lines
	}
	key := ""
	if v.line == protocol.ExplanationScopeNode {
		key = string(blockingNode(v, eff))
	}
	for i := range lines {
		if lines[i].Scope == v.line && (key == "" || lines[i].Key == key) {
			lines[i].Blocking = true
			return lines
		}
	}
	return lines
}

// blockingNode returns the blocking node line's id: the node the verdict
// recorded as the winner, by evaluation order.
func blockingNode(v verdict, eff Effective) protocol.NodeID {
	if v.winPos == posNodeReceiver && len(eff.Nodes) > 1 {
		return eff.Nodes[1].NodeID
	}
	return eff.Nodes[0].NodeID
}

// recordOf assembles the decision record of an evaluation: the object from
// the query (ErrInvalidQuery when nil: every gate decision is about an
// object), the projection of §5.3, the matched controls of §6.7, the
// version vector, the explanation and no actor.
func recordOf(id protocol.ChannelDecisionID, q Query, v verdict, eff Effective, vers protocol.ControlVersions, x protocol.Explanation, at time.Time) (protocol.DecisionRecord, error) {
	if q.Object == nil {
		return protocol.DecisionRecord{}, fmt.Errorf("%w: object", ErrInvalidQuery)
	}
	matched := v.matched
	if matched == nil {
		matched = []protocol.ControlID{}
	}
	rec := protocol.DecisionRecord{
		DecisionID:        id,
		ObjectType:        q.Object.Type,
		ObjectID:          q.Object.ID,
		Stage:             q.Stage,
		Plane:             q.Plane,
		FromAgent:         q.From,
		ToAgent:           q.To,
		Outcome:           v.outcome,
		Reason:            v.reason,
		MatchedControlIDs: matched,
		EffectiveControl:  effectiveControl(eff, q),
		ControlVersions:   vers,
		Explanation:       x,
		CreatedAt:         at,
	}
	if err := rec.Validate(); err != nil {
		return protocol.DecisionRecord{}, fmt.Errorf("invalid decision record: %w", err)
	}
	return rec, nil
}

// Render turns any explanation, live or stored, into text. It is a pure
// function of its arguments. A controls line ends the output; otherwise
// the missing thread, loop and object lines print as placeholders in
// scope order.
func Render(x protocol.Explanation, now time.Time, loc *time.Location) string {
	var out strings.Builder
	out.WriteString(headerOf(x) + "\n")
	for _, line := range completeLines(x) {
		out.WriteString(renderLine(x, line, now, loc) + "\n")
		if line.Scope == protocol.ExplanationScopeControls {
			break
		}
	}
	return out.String()
}

// completeLines inserts the missing thread, loop and object placeholders
// in scope order: before the object line when there is one, else at the
// end. A controls line leaves the list untouched.
func completeLines(x protocol.Explanation) []protocol.ExplanationLine {
	threadPH := protocol.ExplanationLine{Scope: protocol.ExplanationScopeThread, Value: "—"}
	loopPH := protocol.ExplanationLine{Scope: protocol.ExplanationScopeLoop, Value: "—"}
	objectPH := protocol.ExplanationLine{Scope: protocol.ExplanationScopeObject, Value: "—"}
	var haveThread, haveLoop bool
	for _, line := range x.Lines {
		if line.Scope == protocol.ExplanationScopeControls {
			return x.Lines
		}
		switch line.Scope {
		case protocol.ExplanationScopeThread:
			haveThread = true
		case protocol.ExplanationScopeLoop:
			haveLoop = true
		default:
		}
	}
	out := make([]protocol.ExplanationLine, 0, len(x.Lines)+3)
	inserted := false
	for _, line := range x.Lines {
		if !inserted && line.Scope == protocol.ExplanationScopeObject {
			if !haveThread {
				out = append(out, threadPH)
			}
			if !haveLoop {
				out = append(out, loopPH)
			}
			inserted = true
		}
		out = append(out, line)
	}
	if !inserted {
		if !haveThread {
			out = append(out, threadPH)
		}
		if !haveLoop {
			out = append(out, loopPH)
		}
		out = append(out, objectPH)
	}
	return out
}

// headerOf renders the header from the outcome.
func headerOf(x protocol.Explanation) string {
	var state protocol.ControlState
	switch x.Outcome {
	case protocol.DecisionOutcomeAllow:
		state = protocol.ControlStateAllowed
	case protocol.DecisionOutcomeHold:
		state = protocol.ControlStateHeld
	case protocol.DecisionOutcomeApproval:
		state = protocol.ControlStateApprovalRequired
	default:
		state = protocol.ControlStateDenied
	}
	route := string(x.FromAgent) + " → " + string(x.ToAgent)
	switch x.ObjectType {
	case protocol.DecisionObjectTypeMessage:
		return "Message " + string(x.ObjectID) + " (" + route + ") → " + string(state)
	case protocol.DecisionObjectTypeArtifact:
		return "Artifact " + string(x.ObjectID) + " (" + route + ") → " + string(state)
	default:
		return "Channel " + route + " (" + string(x.Plane) + ") → " + string(state)
	}
}

// renderLine renders one line following §8.5 exactly.
func renderLine(x protocol.Explanation, line protocol.ExplanationLine, now time.Time, loc *time.Location) string {
	label, field := labelOf(x, line)
	displayed := label
	if field != "" {
		displayed = pad(label, 19) + field
	}
	version := int64(0)
	if line.Version != nil {
		version = *line.Version
	}
	s := "  " + pad(displayed, 31) + line.Value
	if version > 0 || line.Blocking {
		s = "  " + pad(displayed, 31) + pad(line.Value, 16)
		if version > 0 {
			s += "v" + strconv.FormatInt(version, 10)
		}
	}
	if line.Blocking {
		s += "   ← effective blocker"
		if note := blockNote(x, line, now, loc); note != "" {
			s += "   (" + note + ")"
		}
	}
	return strings.TrimRight(s, " ")
}

// labelOf renders the label and sub-label of a line.
func labelOf(x protocol.Explanation, line protocol.ExplanationLine) (string, string) {
	switch line.Scope {
	case protocol.ExplanationScopePolicy:
		return "Security policy", ""
	case protocol.ExplanationScopeControls:
		return "Controls", ""
	case protocol.ExplanationScopeGlobal:
		return "Global channel", ""
	case protocol.ExplanationScopeNode:
		return "Node " + line.Key, ""
	case protocol.ExplanationScopeSender:
		if strings.Contains(line.Value, " / ") {
			return "Sender " + line.Key, "TX / CONT"
		}
		return "Sender " + line.Key, "TX"
	case protocol.ExplanationScopeReceiver:
		return "Receiver " + line.Key, "RX / AUTO"
	case protocol.ExplanationScopeLink:
		label := "Link " + strings.Replace(line.Key, "→", " → ", 1)
		if x.Plane == protocol.PlaneArtifact {
			label += " (artifact)"
		}
		return label, ""
	case protocol.ExplanationScopeThread:
		if line.Key == "" {
			return "Thread", ""
		}
		return "Thread " + line.Key, ""
	case protocol.ExplanationScopeLoop:
		if line.Key == "" {
			return "Loop", ""
		}
		generation := int64(0)
		if x.LoopGeneration != nil {
			generation = *x.LoopGeneration
		}
		return "Loop " + shortLoopID(line.Key) + " (gen " + strconv.FormatInt(generation, 10) + ")", ""
	case protocol.ExplanationScopeObject:
		return "Object hold / approval", ""
	default:
		return string(line.Scope), ""
	}
}

// shortLoopID shortens a loop id for the rendered label only: longer than
// 10 runes shows its first 9 runes followed by ….
func shortLoopID(id string) string {
	runes := []rune(id)
	if len(runes) > 10 {
		return string(runes[:9]) + "…"
	}
	return id
}

// blockNote writes the note of the blocking line: the row parts when the
// line has a control id, else the explanation's reason.
func blockNote(x protocol.Explanation, line protocol.ExplanationLine, now time.Time, loc *time.Location) string {
	if line.ControlID == "" {
		return string(x.Reason)
	}
	parts := []string{line.ActorID}
	if line.Reason != "" {
		parts = append(parts, strconv.Quote(line.Reason))
	}
	if !line.ExpiresAt.IsZero() {
		verb := "expires"
		if !line.ExpiresAt.After(now) {
			verb = "expired"
		}
		parts = append(parts, verb+" "+shortTime(line.ExpiresAt, now, loc))
	}
	return strings.Join(parts, valueSep)
}

// shortTime renders a timestamp as 15:04 on the same calendar day as now
// in loc, else as 2006-01-02 15:04.
func shortTime(at, now time.Time, loc *time.Location) string {
	local := at.In(loc)
	present := now.In(loc)
	if local.Year() == present.Year() && local.Month() == present.Month() && local.Day() == present.Day() {
		return local.Format("15:04")
	}
	return local.Format("2006-01-02 15:04")
}

// pad appends spaces up to w runes, or one space when s is already wider.
func pad(s string, w int) string {
	if n := utf8.RuneCountInString(s); n < w {
		return s + strings.Repeat(" ", w-n)
	}
	return s + " "
}
