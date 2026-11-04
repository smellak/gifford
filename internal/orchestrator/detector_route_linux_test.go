//go:build linux

package orchestrator

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/protocol"
)

// detectorLinks is a real agent link whose detector verdict the test sets:
// the link's own snapshot otherwise, so the guard reports and the delivery
// calls go over the real socket.
type detectorLinks struct {
	integrationLinks
	mu       sync.Mutex
	detector *agentlink.DetectorEvidence
}

func (l *detectorLinks) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	snapshot, ok := l.integrationLinks.Snapshot(agent)
	l.mu.Lock()
	defer l.mu.Unlock()
	snapshot.Detector = l.detector
	return snapshot, ok
}

func (l *detectorLinks) set(change func(**agentlink.DetectorEvidence)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	change(&l.detector)
}

// newLinkedDetector is the unwired executor over a real agent socket, on a
// measured build, with an eligible verdict.
func newLinkedDetector(t *testing.T, delivery *integrationDelivery) (*executorFixture, *detectorLinks) {
	t.Helper()
	f := newExecutorFixture(t)
	useHarnessVersion(t, f, detectorTestMeasuredVersion)
	f.executor = attachIntegrationLink(t, f, delivery, linkedWait)
	links := &detectorLinks{
		integrationLinks: f.executor.links.(integrationLinks), detector: eligibleDetector(f.clock.Now()),
	}
	f.executor.links = links
	return f, links
}

// The detector route over a real agent socket: no model, one prepare and
// one commit, the decision naming DeterministicPTYPolicy.
func TestDetectorRouteDeliversThroughRealAgentSocket(t *testing.T) {
	delivery := &integrationDelivery{}
	f, _ := newLinkedDetector(t, delivery)
	candidate := seedReadableDelivery(t, f, 130, "please review the parser change")
	require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

	require.Equal(t, 1, delivery.prepareCount())
	require.Equal(t, 1, delivery.commitCount())
	require.Equal(t, "DELIVERED", queueStateOf(t, f.store.DB(), candidate))
	decision := lastDecision(t, f.store.DB(), candidate.MessageID)
	require.Equal(t, choiceInject, decision.selected)
	require.Equal(t, DeterministicPTYPolicy, decision.policy)
}

// The commit needs the decision's quiet screen once more, after the
// prepare: a new screen, a restarted quiet window, a busy harness, a typed
// composer or a lost verdict cancels the prepared delivery with STATE_CHANGED
// and the row goes back to QUEUED: one prepare, no commit.
func TestDetectorRouteCommitNeedsTheSameQuietScreen(t *testing.T) {
	for name, change := range map[string]func(now func() time.Time) func(**agentlink.DetectorEvidence){
		"new screen": func(now func() time.Time) func(**agentlink.DetectorEvidence) {
			return func(d **agentlink.DetectorEvidence) {
				fresh := **d
				fresh.Verdict.ScreenDigest = protocol.Digest("sha256:" + strings.Repeat("b", 64))
				fresh.QuietSince = now().Add(-10 * time.Second)
				*d = &fresh
			}
		},
		"quiet window restarted": func(now func() time.Time) func(**agentlink.DetectorEvidence) {
			return func(d **agentlink.DetectorEvidence) {
				fresh := **d
				fresh.QuietSince = now()
				*d = &fresh
			}
		},
		"harness busy": func(func() time.Time) func(**agentlink.DetectorEvidence) {
			return func(d **agentlink.DetectorEvidence) {
				fresh := **d
				fresh.Verdict.Busy = true
				*d = &fresh
			}
		},
		"typed composer": func(func() time.Time) func(**agentlink.DetectorEvidence) {
			return func(d **agentlink.DetectorEvidence) {
				fresh := **d
				fresh.Verdict.BoxBusy = true
				*d = &fresh
			}
		},
		"verdict lost": func(func() time.Time) func(**agentlink.DetectorEvidence) {
			return func(d **agentlink.DetectorEvidence) { *d = nil }
		},
	} {
		t.Run(name, func(t *testing.T) {
			delivery := &integrationDelivery{}
			f, links := newLinkedDetector(t, delivery)
			delivery.prepareHook = func() { links.set(change(f.clock.Now)) }
			candidate := seedReadableDelivery(t, f, 131, "please review the parser change")
			require.NoError(t, f.executor.EvaluateAutomatic(t.Context(), candidate))

			require.Equal(t, 1, delivery.prepareCount())
			require.Zero(t, delivery.commitCount(), "no COMMIT without the decision's quiet screen")
			require.Equal(t, 1, delivery.cancelCount())
			db := f.store.DB()
			var attempt, reason string
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT state, terminal_reason
				FROM delivery_attempts WHERE message_id = ? AND agent_id = ?`,
				candidate.MessageID, candidate.AgentID).Scan(&attempt, &reason))
			require.Equal(t, string(daemon.AttemptCancelled), attempt)
			require.Equal(t, string(protocol.CancelReasonStateChanged), reason)
			require.Equal(t, queueQueued, queueStateOf(t, db, candidate))
		})
	}
}
