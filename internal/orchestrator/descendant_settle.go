package orchestrator

import (
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// descendantSettle is how long one exact set of harness descendants must
// stand, on a screen the detector holds at rest, before the PTY process
// guard trusts it: the tmux route's baseline settle.
const descendantSettle = time.Minute

// settlingDetail follows untrustedProcessDetail while a set can still
// settle: the detector route waits on it instead of deferring.
const settlingDetail = "; settling"

// descendantCandidate is a set of harness descendants waiting to settle.
type descendantCandidate struct {
	set   string
	since time.Time
}

// descendantSettling holds one candidate per session.
type descendantSettling struct {
	mu         sync.Mutex
	candidates map[protocol.OpaqueID]descendantCandidate
}

// settledLayout is validateProcessLayout, except that descendants below the
// one bound harness child, MCP servers or hooks for instance, are trusted
// once their exact set, by PID and start time, has stood unchanged for
// descendantSettle while the detector held the screen at rest, the draft
// closed and no human key arrived. The settle never changes the direct child
// rule, and a set seen at the decision must be seen again before COMMIT:
// any change, or a broken rest, starts the count over. Without a detector
// verdict nothing settles.
func (e *Executor) settledLayout(
	ctx context.Context,
	tx *sql.Tx,
	record deliveryRecord,
	tree ProcessTree,
	binding *processBinding,
	auxiliaryName string,
) (processLayout, string) {
	layout, detail := validateProcessLayout(tree, binding, auxiliaryName)
	if detail != untrustedProcessDetail {
		e.descendants.forget(record.sessionID)
		return layout, detail
	}
	var direct []ProcessNode
	extras := make([]ProcessNode, 0, len(tree.Descendants))
	for _, node := range tree.Descendants {
		if node.Parent == tree.Root {
			direct = append(direct, node)
			continue
		}
		extras = append(extras, node)
	}
	if len(direct) != 1 {
		e.descendants.forget(record.sessionID)
		return processLayout{}, untrustedProcessDetail + "; direct children: " + processNames(direct)
	}
	harness := direct[0]
	snapshot, _ := e.linkSnapshot(ctx, tx, record.AgentID)
	now := e.clock.Now()
	d, report := snapshot.Detector, snapshot.LastReport
	rest := d != nil && snapshot.HasReport && snapshot.SessionID == record.sessionID &&
		d.InputGuardVersion == report.InputGuardVersion && now.Sub(d.ReceivedAt) <= detectorFreshness &&
		d.Verdict.Certain && d.Verdict.IsCLI && !d.Verdict.Busy && !d.Verdict.BoxBusy &&
		report.DraftState == protocol.DraftStateClosed && report.ActivityState != protocol.ActivityStateActive
	if !rest {
		e.descendants.forget(record.sessionID)
		return processLayout{}, untrustedProcessDetail + settlingDetail + ": screen not at rest"
	}
	since := e.descendants.observe(record.sessionID, processNodeSet(extras), now)
	switch {
	case now.Sub(since) < descendantSettle:
		return processLayout{}, untrustedProcessDetail + settlingDetail
	case d.QuietSince.After(since):
		return processLayout{}, untrustedProcessDetail + settlingDetail + ": screen changed"
	case !report.LastHumanKeyAt.IsZero() && !report.LastHumanKeyAt.Before(since):
		return processLayout{}, untrustedProcessDetail + settlingDetail + ": human key"
	}
	return processLayout{harness: harness}, ""
}

// observe returns since when set has been the session's candidate,
// replacing a different one with set from now.
func (s *descendantSettling) observe(session protocol.OpaqueID, set string, now time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.candidates == nil {
		s.candidates = make(map[protocol.OpaqueID]descendantCandidate)
	}
	candidate, found := s.candidates[session]
	if !found || candidate.set != set || now.Before(candidate.since) {
		candidate = descendantCandidate{set: set, since: now}
		s.candidates[session] = candidate
	}
	return candidate.since
}

func (s *descendantSettling) forget(session protocol.OpaqueID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.candidates, session)
}

// processNodeSet is the order-free identity of a set of processes.
func processNodeSet(nodes []ProcessNode) string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, strconv.FormatInt(node.Token.PID, 10)+":"+strconv.FormatInt(node.Token.StartTime, 10))
	}
	slices.Sort(ids)
	return strings.Join(ids, ",")
}

// processNames lists the executables' base names, for an operator reading
// why a session holds; it carries no arguments.
func processNames(nodes []ProcessNode) string {
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		name := filepath.Base(node.Executable)
		if node.Executable == "" {
			name = "?"
		}
		names = append(names, name)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}
