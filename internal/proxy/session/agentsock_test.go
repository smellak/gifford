//go:build unix

package session

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/draft"
)

const sockAgent protocol.AgentID = "agent-1"

// stillClock is a socket clock whose timers never fire: no greeting
// deadline and no heartbeat while a test reads the reports.
type stillClock struct{ *stepClock }

func (stillClock) After(time.Duration) <-chan time.Time { return nil }

// listenAgentSocket opens the agent socket over s.
func listenAgentSocket(t *testing.T, s *Session, clk agentsock.Clock) *agentsock.Server {
	t.Helper()
	runtime, err := os.MkdirTemp("", "gs") //nolint:usetesting // a socket path must stay short, and t.TempDir embeds the test name
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(runtime) })
	srv, err := agentsock.Listen(context.Background(), agentsock.Options{
		RuntimeDir: runtime,
		Register: protocol.RegisterAgent{
			Manifest: protocol.CapabilityManifest{
				SchemaVersion: protocol.SchemaVersion, AgentID: sockAgent,
				Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
				AdapterType:        protocol.AdapterTypeGenericPTY,
				InputGuardProvider: protocol.InputGuardProviderPTYProxy,
				CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
				Capabilities: protocol.Capabilities{MCP: true, PTYCapture: true},
			},
			Process: &protocol.ProcessIdentity{PID: int64(os.Getpid())},
		},
		Guard: s,
		Clock: clk,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

// fakeCore is the Core's side of the agent socket, keeping every byte
// it reads.
type fakeCore struct {
	t    *testing.T
	c    net.Conn
	raw  strings.Builder
	next int64
}

// greetCore dials the socket, answers the greeting and reads the first
// report.
func greetCore(t *testing.T, path string) (*fakeCore, protocol.InputGuardReport) {
	t.Helper()
	c, err := (&net.Dialer{}).DialContext(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	k := &fakeCore{t: t, c: c, next: 1}
	hello := k.read()
	require.Equal(t, protocol.AgentOpRegisterAgent, hello.Op)
	k.send(protocol.AgentOpRegisterAgentResult, hello.FrameID, protocol.RegisterAgentResult{AgentID: sockAgent, SessionID: "sess-1"})
	return k, k.report()
}

func (k *fakeCore) read() protocol.AgentFrame {
	k.t.Helper()
	require.NoError(k.t, k.c.SetReadDeadline(time.Now().Add(10*time.Second)))
	f, err := agentsock.ReadFrame(k.c)
	require.NoError(k.t, err)
	raw, err := json.Marshal(f)
	require.NoError(k.t, err)
	k.raw.Write(raw)
	return f
}

// report reads the next report.
func (k *fakeCore) report() protocol.InputGuardReport {
	k.t.Helper()
	f := k.read()
	require.Equal(k.t, protocol.AgentOpReportInputGuard, f.Op)
	var r protocol.InputGuardReport
	require.NoError(k.t, json.Unmarshal(f.Body, &r))
	return r
}

func (k *fakeCore) send(op protocol.AgentOp, replyTo int64, body any) int64 {
	k.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(k.t, err)
	id := k.next
	k.next++
	f := protocol.AgentFrame{Op: op, FrameID: id, SentAt: time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC), Body: raw}
	if replyTo > 0 {
		f.ReplyTo = &replyTo
	}
	require.NoError(k.t, agentsock.WriteFrame(k.c, f))
	return id
}

// TestAgentSocketReportsTheGuard types into a real session behind the
// agent socket: every new guard version reaches the Core as a report,
// and no frame carries a typed byte.
func TestAgentSocketReportsTheGuard(t *testing.T) {
	t.Parallel()

	r := startGuardSession(t, "sock-reports")
	srv := listenAgentSocket(t, r.session, stillClock{r.clock})
	k, first := greetCore(t, srv.Path())
	require.Equal(t, protocol.DraftStateClosed, first.DraftState)
	require.Equal(t, int64(0), first.InputGuardVersion)

	const typed = "qk7x2"
	writeKeys(t, r.master, typed)
	g := k.report()
	require.Equal(t, protocol.DraftStateOpen, g.DraftState)
	require.Equal(t, protocol.ActivityStateActive, g.ActivityState)
	require.Equal(t, int64(1), g.DraftGeneration)
	require.Equal(t, int64(1), g.InputGuardVersion)
	require.True(t, r.clock.Now().Equal(g.LastHumanKeyAt))
	require.Equal(t, sockAgent, g.AgentID)
	require.Equal(t, protocol.OpaqueID("sess-1"), g.SessionID)
	require.Equal(t, protocol.InputGuardProviderPTYProxy, g.DetectionSource)

	writeKeys(t, r.master, "\x03")
	g = k.report()
	require.Equal(t, protocol.DraftStateClosed, g.DraftState)
	require.Equal(t, int64(2), g.InputGuardVersion)

	endDeliverySession(t, r.master, r.done)
	require.NotContains(t, k.raw.String(), typed, "no frame carries a human byte")
}

// TestAgentSocketResyncKeepsUnknown loses the guard mid-paste: resync
// reports UNKNOWN, and the guard is still UNKNOWN after it.
func TestAgentSocketResyncKeepsUnknown(t *testing.T) {
	t.Parallel()

	r := startGuardSession(t, "sock-resync")
	srv := listenAgentSocket(t, r.session, stillClock{r.clock})
	k, _ := greetCore(t, srv.Path())

	// The harness printed its ready line, so the guard has its screen
	// baseline: a resize mid-paste loses sync.
	writeKeys(t, r.master, "\x1b[200~abc")
	require.Equal(t, protocol.DraftStatePasting, k.report().DraftState)
	require.NoError(t, r.session.Resize(100, 30))
	require.Equal(t, protocol.DraftStateUnknown, k.report().DraftState)

	id := k.send(protocol.AgentOpResync, 0, protocol.AgentTarget{AgentID: sockAgent})
	f := k.read()
	require.Equal(t, protocol.AgentOpReportInputGuard, f.Op)
	require.NotNil(t, f.ReplyTo)
	require.Equal(t, id, *f.ReplyTo)
	var g protocol.InputGuardReport
	require.NoError(t, json.Unmarshal(f.Body, &g))
	require.Equal(t, protocol.DraftStateUnknown, g.DraftState)
	require.Equal(t, draft.StateUnknown, r.session.State(), "resync moved nothing")

	writeKeys(t, r.master, "\x1b[201~")
	endDeliverySession(t, r.master, r.done)
}

// TestJammedCoreNeverDelaysKeys registers a Core that then stops
// reading, and publishes thousands of guard versions: the reports jam
// the socket, yet a line typed during the jam reaches the harness at
// once, and the Core that reads again gets the latest state.
func TestJammedCoreNeverDelaysKeys(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "harness.out")
	master, slave := testPTY(t)
	opts := testOptions(t.TempDir(), "sock-jam", slave, slave, &chanSink{},
		[]string{writeHarnessScript(t, "trap '' INT", "echo jam-ready"), out})
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	waitRaw(t, slave)
	// No cancel before the harness ignores the interrupt it raises.
	readUntil(t, master, "jam-ready")
	srv := listenAgentSocket(t, s, nil)
	k, _ := greetCore(t, srv.Path())
	require.NoError(t, k.c.(*net.UnixConn).SetReadBuffer(4096))

	// Each pair opens and cancels a draft in one chunk: one published
	// version change, one report the Core does not read.
	const pairs = 2000
	for i := range pairs {
		writeKeys(t, master, "a\x03")
		want := int64(2 * (i + 1))
		require.Eventually(t, func() bool {
			return s.InputGuard().InputGuardVersion == want
		}, 10*time.Second, time.Millisecond, "the loop never took the keys")
	}

	start := time.Now()
	writeKeys(t, master, "marker\n")
	require.Eventually(t, func() bool {
		return strings.Contains(harnessOut(t, out), "marker")
	}, 10*time.Second, time.Millisecond, "the line never reached the harness")
	require.Less(t, time.Since(start), 2*time.Second, "the jam delayed the human")

	// The Core reads again: the reports rise to the latest state, and
	// fewer of them came than versions, so the socket was jammed.
	require.NoError(t, k.c.SetReadDeadline(time.Now().Add(30*time.Second)))
	var last int64
	reports := 0
	for last < 2*pairs+1 {
		f, err := agentsock.ReadFrame(k.c)
		require.NoError(t, err)
		if f.Op != protocol.AgentOpReportInputGuard {
			continue
		}
		var g protocol.InputGuardReport
		require.NoError(t, json.Unmarshal(f.Body, &g))
		require.Greater(t, g.InputGuardVersion, last)
		last = g.InputGuardVersion
		reports++
	}
	require.Less(t, reports, pairs, "reports coalesced while the Core did not read")
	endDeliverySession(t, master, done)
}
