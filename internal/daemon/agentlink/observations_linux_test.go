//go:build linux

package agentlink

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/agentsock"
	"github.com/smellak/gifford/internal/protocol"
)

// observationCanary is screen text that only an observation carries. No
// error, snapshot or table may hold it.
const observationCanary = "observation-canary-7f3c"

// scriptedProxy answers each greeting of a link as registration and then
// writes only the frames its test sends, including frames a real Proxy never
// would.
type scriptedProxy struct {
	listener *net.UnixListener
	conn     chan *net.UnixConn
	err      chan error
	stop     chan struct{}
	done     chan struct{}
	nextID   int64

	mu       sync.Mutex
	accepted []*net.UnixConn
}

func listenScriptedProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
) *scriptedProxy {
	t.Helper()
	directory := filepath.Join(runtimeDir, agentsDirectory)
	require.NoError(t, os.MkdirAll(directory, 0o700))
	require.NoError(t, os.Chmod(directory, 0o700))
	path := agentsock.SocketPath(runtimeDir, registration.Manifest.AgentID)
	address, err := net.ResolveUnixAddr("unix", path)
	require.NoError(t, err)
	listener, err := net.ListenUnix("unix", address)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(path, 0o600))
	proxy := &scriptedProxy{
		listener: listener, conn: make(chan *net.UnixConn, 1), err: make(chan error, 1),
		stop: make(chan struct{}), done: make(chan struct{}), nextID: 1,
	}
	go proxy.accept(registration)
	t.Cleanup(func() {
		close(proxy.stop)
		_ = listener.Close()
		<-proxy.done
		proxy.mu.Lock()
		defer proxy.mu.Unlock()
		for _, conn := range proxy.accepted {
			_ = conn.Close()
		}
	})
	return proxy
}

// accept greets every connection until the test ends.
func (p *scriptedProxy) accept(registration protocol.RegisterAgent) {
	defer close(p.done)
	for {
		conn, err := p.listener.AcceptUnix()
		if err != nil {
			p.fail(fmt.Errorf("accept scripted proxy: %w", err))
			return
		}
		p.mu.Lock()
		p.accepted = append(p.accepted, conn)
		p.mu.Unlock()
		if err := greet(conn, registration); err != nil {
			p.fail(err)
			return
		}
		// The link sends nothing these tests read; draining keeps it from blocking.
		go func() { _, _ = io.Copy(io.Discard, conn) }()
		select {
		case p.conn <- conn:
		case <-p.stop:
			return
		}
	}
}

func (p *scriptedProxy) fail(err error) {
	select {
	case p.err <- err:
	default:
	}
}

func greet(conn *net.UnixConn, registration protocol.RegisterAgent) error {
	raw, err := json.Marshal(registration)
	if err != nil {
		return fmt.Errorf("encode scripted greeting: %w", err)
	}
	if err := agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: protocol.AgentOpRegisterAgent, FrameID: 1, SentAt: testNow, Body: raw,
	}); err != nil {
		return fmt.Errorf("write scripted greeting: %w", err)
	}
	response, err := agentsock.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("read scripted greeting response: %w", err)
	}
	if response.Op != protocol.AgentOpRegisterAgentResult || response.ReplyTo == nil ||
		*response.ReplyTo != 1 {
		return fmt.Errorf("unexpected scripted greeting response: %s", response.Op)
	}
	return nil
}

// wait returns the greeted connection.
func (p *scriptedProxy) wait(t *testing.T) *net.UnixConn {
	t.Helper()
	select {
	case conn := <-p.conn:
		return conn
	case err := <-p.err:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("scripted proxy did not complete its greeting")
	}
	return nil
}

func (p *scriptedProxy) send(t *testing.T, conn *net.UnixConn, op protocol.AgentOp, body any) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	p.nextID++
	require.NoError(t, agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: op, FrameID: p.nextID, SentAt: testNow, Body: raw,
	}))
}

// sendUnchecked writes a frame that agentsock.WriteFrame would refuse.
func (p *scriptedProxy) sendUnchecked(t *testing.T, conn *net.UnixConn, op protocol.AgentOp, body any) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	p.nextID++
	frame, err := json.Marshal(protocol.AgentFrame{Op: op, FrameID: p.nextID, SentAt: testNow, Body: raw})
	require.NoError(t, err)
	prefix := make([]byte, 4)
	binary.BigEndian.PutUint32(prefix, uint32(len(frame)))
	_, err = conn.Write(append(prefix, frame...))
	require.NoError(t, err)
}

func (p *scriptedProxy) heartbeat(t *testing.T, conn *net.UnixConn) {
	t.Helper()
	p.send(t, conn, protocol.AgentOpHeartbeat, protocol.AgentTarget{AgentID: testAgentID})
}

// observationSink hands each submitted report to its test. Until unblock, a
// held sink holds Submit, as a producer busy with a decision read would.
type observationSink struct {
	submitted chan protocol.StateObservationReport
	release   chan struct{}
	once      sync.Once
}

func newObservationSink(held bool) *observationSink {
	sink := &observationSink{submitted: make(chan protocol.StateObservationReport, 16)}
	if held {
		sink.release = make(chan struct{})
	}
	return sink
}

func (s *observationSink) Submit(report protocol.StateObservationReport) error {
	s.submitted <- report
	if s.release != nil {
		<-s.release
	}
	return nil
}

// unblock lets every held Submit return. A test registers it as a cleanup
// after starting its link, so it runs before Close waits for the forwarder.
func (s *observationSink) unblock() {
	s.once.Do(func() {
		if s.release != nil {
			close(s.release)
		}
	})
}

func (s *observationSink) next(t *testing.T) protocol.StateObservationReport {
	t.Helper()
	select {
	case report := <-s.submitted:
		return report
	case <-time.After(5 * time.Second):
		t.Fatal("no observation reached the sink")
	}
	return protocol.StateObservationReport{}
}

func linkObservation(generation int64, rows ...string) protocol.StateObservationReport {
	report := protocol.StateObservationReport{
		AgentID: testAgentID, SessionID: testSessionID,
		ObservationID: protocol.OpaqueID(fmt.Sprintf("observation-%d", generation)),
		ObservedAt:    testNow.Add(time.Duration(generation) * time.Second),
		Harness:       protocol.HarnessCodex, HarnessVersion: "0.1.0",
		ObservationGeneration: generation, OutputGeneration: generation, InputGuardVersion: 1,
		ScreenState: protocol.StateObservationScreenWithheld, ScreenRows: []string{},
		Activity: protocol.StateObservationActivity{
			BytesLast500ms: 1, BytesLast2s: 2, BytesLast10s: 3, SecondsSinceLastOutput: 0.25,
		},
		NativeEvents: []protocol.StateObservationEvent{},
	}
	if len(rows) > 0 {
		report.ScreenState = protocol.StateObservationScreenAvailable
		report.ScreenRows = rows
		report.Cols = 80
		report.Cursor = &protocol.StateObservationCursor{Row: int64(len(rows) - 1), Col: 2}
	}
	return report
}

// observedLink is an authenticated link whose Proxy is scripted.
type observedLink struct {
	db     *sql.DB
	client *Client
	clock  *manualClock
	proxy  *scriptedProxy
	conn   *net.UnixConn
}

func startObservedLink(t *testing.T, sink ObservationSink) observedLink {
	t.Helper()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	proxy := listenScriptedProxy(t, runtimeDir, registration)
	store, err := NewInputGuardStore(db, fixedClock{now: testNow})
	require.NoError(t, err)
	// The manual clock arms heartbeat and reconnect timers; only the test
	// fires them.
	clk := newManualClock()
	client := startTestClient(t, db, runtimeDir, testSessionID, store, clk,
		func(opts *Options) {
			if sink != nil {
				opts.Observations = sink
			}
		})
	conn := proxy.wait(t)
	waitForState(t, client, StateUp)
	return observedLink{db: db, client: client, clock: clk, proxy: proxy, conn: conn}
}

// waitForFirstHeartbeat proves that the link read every frame sent before
// the connection's first heartbeat: one connection's frames are dispatched in
// order, and a new connection starts with no heartbeat time.
func (l observedLink) waitForFirstHeartbeat(t *testing.T) {
	t.Helper()
	l.proxy.heartbeat(t, l.conn)
	require.Eventually(t, func() bool {
		snapshot := l.client.Snapshot()
		return snapshot.State == StateUp && snapshot.LastHeartbeatAt.Equal(testNow)
	}, 5*time.Second, time.Millisecond)
}

func TestLinkHandsTheSinkItsObservation(t *testing.T) {
	t.Parallel()
	sink := newObservationSink(false)
	link := startObservedLink(t, sink)
	report := linkObservation(1, "$ go test ./...", observationCanary)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, report)
	require.Equal(t, report, sink.next(t))

	link.waitForFirstHeartbeat(t)
	require.Empty(t, sink.submitted)
	requireNoTableHolds(t, link.db, observationCanary)
}

// A held sink stands for a producer whose lock a decision read holds. The
// socket reader keeps reading, and the sink gets only the newest report.
func TestLinkNeverWaitsForTheSink(t *testing.T) {
	t.Parallel()
	sink := newObservationSink(true)
	link := startObservedLink(t, sink)
	t.Cleanup(sink.unblock)
	first := linkObservation(1)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, first)
	require.Equal(t, first, sink.next(t))

	replaced, newest := linkObservation(2), linkObservation(3, observationCanary)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, replaced)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, newest)
	link.waitForFirstHeartbeat(t)
	require.Empty(t, sink.submitted)

	sink.unblock()
	require.Equal(t, newest, sink.next(t))
	after := linkObservation(4)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, after)
	require.Equal(t, after, sink.next(t))
	require.Empty(t, sink.submitted)
	require.Equal(t, StateUp, link.client.Snapshot().State)
}

// evidence is an observation whose generations and guard version are all g.
func evidence(g int64) protocol.StateObservationReport {
	report := linkObservation(g)
	report.EventGeneration, report.InputGuardVersion = g, g
	return report
}

// With nothing waiting, a report that goes back on any generation or the
// guard version from the last one taken is ignored, and so is the same
// report again. A refresh of the same evidence, later, is taken.
func TestLinkIgnoresAnObservationThatGoesBack(t *testing.T) {
	t.Parallel()
	sink := newObservationSink(false)
	link := startObservedLink(t, sink)
	base := evidence(5)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, base)
	require.Equal(t, base, sink.next(t))

	for name, back := range map[string]func(*protocol.StateObservationReport){
		"observation generation": func(r *protocol.StateObservationReport) { r.ObservationGeneration = 4 },
		"output generation":      func(r *protocol.StateObservationReport) { r.OutputGeneration = 4 },
		"event generation":       func(r *protocol.StateObservationReport) { r.EventGeneration = 4 },
		"guard version":          func(r *protocol.StateObservationReport) { r.InputGuardVersion = 4 },
	} {
		report := evidence(6)
		report.ObservationID = protocol.OpaqueID("back-" + strings.ReplaceAll(name, " ", "-"))
		back(&report)
		link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, report)
	}
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, base)
	refresh := base
	refresh.ObservationID = "refresh"
	refresh.ObservedAt = base.ObservedAt.Add(time.Second)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, refresh)
	require.Equal(t, refresh, sink.next(t))
	link.waitForFirstHeartbeat(t)
	require.Empty(t, sink.submitted)
}

// The highest evidence outlives the connection: on the next one, a report
// that goes back from it is ignored.
func TestLinkKeepsItsHighestEvidenceAcrossAReconnect(t *testing.T) {
	t.Parallel()
	sink := newObservationSink(false)
	link := startObservedLink(t, sink)
	base := evidence(5)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, base)
	require.Equal(t, base, sink.next(t))
	require.NoError(t, link.conn.Close())
	waitForReason(t, link.client, ReasonConnectionLost)
	link.clock.fire(t, defaultMinReconnectDelay)
	conn := link.proxy.wait(t)
	waitForState(t, link.client, StateUp)

	behind := evidence(6)
	behind.OutputGeneration = 4
	ahead := evidence(7)
	link.proxy.send(t, conn, protocol.AgentOpReportStateObservation, behind)
	link.proxy.send(t, conn, protocol.AgentOpReportStateObservation, ahead)
	require.Equal(t, ahead, sink.next(t))
	require.Empty(t, sink.submitted)
}

func TestLinkDropsAnObservationForAnotherIdentity(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*protocol.StateObservationReport){
		"another agent":   func(report *protocol.StateObservationReport) { report.AgentID = "agent-other" },
		"another session": func(report *protocol.StateObservationReport) { report.SessionID = "session-2" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, sink := range []*observationSink{newObservationSink(false), nil} {
				var observations ObservationSink
				if sink != nil {
					observations = sink
				}
				link := startObservedLink(t, observations)
				report := linkObservation(1, observationCanary)
				mutate(&report)
				link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation, report)
				waitForReason(t, link.client, ReasonConnectionLost)
				require.NotContains(t, fmt.Sprintf("%+v", link.client.Snapshot()), observationCanary)
				if sink != nil {
					require.Empty(t, sink.submitted)
				}
			}
		})
	}
}

func TestLinkDropsAnInvalidObservation(t *testing.T) {
	t.Parallel()
	sink := newObservationSink(false)
	link := startObservedLink(t, sink)
	report := linkObservation(1)
	report.ScreenRows = []string{observationCanary}
	link.proxy.sendUnchecked(t, link.conn, protocol.AgentOpReportStateObservation, report)
	waitForReason(t, link.client, ReasonConnectionLost)
	require.Empty(t, sink.submitted)
}

// dispatch is reached only through agentsock.ReadFrame, which already
// validates a body. dispatch still checks the report on its own.
func TestDispatchRefusesAnObservationItCannotTrust(t *testing.T) {
	t.Parallel()
	sink := newObservationSink(false)
	client := startTestClient(t, openTestDB(t), shortRuntime(t), testSessionID, &recordingWriter{},
		newManualClock(), func(opts *Options) { opts.Observations = sink })
	encode := func(report protocol.StateObservationReport) protocol.Payload {
		raw, err := json.Marshal(report)
		require.NoError(t, err)
		return raw
	}
	withheldRows := linkObservation(1)
	withheldRows.ScreenRows = []string{observationCanary}
	tooManyRows := linkObservation(1, make([]string, 10001)...)
	tooManyRows.ScreenRows[0] = observationCanary
	otherAgent, otherSession := linkObservation(1, observationCanary), linkObservation(1, observationCanary)
	otherAgent.AgentID, otherSession.SessionID = "agent-other", "session-2"
	for name, body := range map[string]protocol.Payload{
		"withheld screen with rows": encode(withheldRows),
		"over the row bound":        encode(tooManyRows),
		"another agent":             encode(otherAgent),
		"another session":           encode(otherSession),
		"not a report":              protocol.Payload(`["` + observationCanary + `"]`),
	} {
		isHeartbeat, err := client.dispatch(1, protocol.CapabilityManifest{}, protocol.AgentFrame{
			Op: protocol.AgentOpReportStateObservation, FrameID: 2, SentAt: testNow, Body: body,
		})
		require.False(t, isHeartbeat, name)
		require.ErrorIs(t, err, ErrUnexpectedFrame, name)
		require.Equal(t, ErrUnexpectedFrame.Error(), err.Error(), name)
	}
	require.Empty(t, sink.submitted)

	valid := linkObservation(1, observationCanary)
	isHeartbeat, err := client.dispatch(1, protocol.CapabilityManifest{}, protocol.AgentFrame{
		Op: protocol.AgentOpReportStateObservation, FrameID: 2, SentAt: testNow, Body: encode(valid),
	})
	require.NoError(t, err)
	require.False(t, isHeartbeat)
	require.Equal(t, valid, sink.next(t))
}

func TestLinkWithoutASinkStillReadsObservations(t *testing.T) {
	t.Parallel()
	link := startObservedLink(t, nil)
	link.proxy.send(t, link.conn, protocol.AgentOpReportStateObservation,
		linkObservation(1, observationCanary))
	link.waitForFirstHeartbeat(t)
	requireNoTableHolds(t, link.db, observationCanary)
}

// requireNoTableHolds fails when any cell of any table contains needle.
func requireNoTableHolds(t *testing.T, db *sql.DB, needle string) {
	t.Helper()
	var text strings.Builder
	for _, table := range linkTableNames(t, db) {
		linkTableText(t, db, table, &text)
	}
	require.NotContains(t, text.String(), needle)
}

func linkTableNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT name FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, tables)
	return tables
}

func linkTableText(t *testing.T, db *sql.DB, table string, text *strings.Builder) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+table)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	require.NoError(t, err)
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		require.NoError(t, rows.Scan(pointers...))
		for _, value := range values {
			text.WriteString(value.String + "\n")
		}
	}
	require.NoError(t, rows.Err())
}

// linkSource is a state-observation source behind a real agent socket. Each
// cut is the next generation of linkObservation, with the ids and the
// harness left for the socket to fill in.
type linkSource struct {
	mu         sync.Mutex
	generation int64
	changed    chan struct{}
}

func newLinkSource() *linkSource { return &linkSource{changed: make(chan struct{}, 1)} }

func (s *linkSource) Observe() (protocol.StateObservationReport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	report := linkObservation(s.generation)
	report.AgentID, report.SessionID, report.Harness, report.HarnessVersion = "", "", "", ""
	return report, true
}

func (s *linkSource) ObservationChanged() <-chan struct{} { return s.changed }

func (s *linkSource) cuts() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.generation
}

func (s *linkSource) change() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

// listenObservedProxy is a real agent socket whose session is source. Its
// clock never fires, so it cuts only on the announcement and on a change.
func listenObservedProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
	guard *socketGuard,
	source *linkSource,
) {
	t.Helper()
	server, err := agentsock.Listen(t.Context(), agentsock.Options{
		RuntimeDir: runtimeDir, Register: registration, Guard: guard,
		Observations: source, Clock: fixedClock{now: testNow},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
}

// The real link and the real agent socket agree end to end: a link with a
// sink announces it takes state observations, and each cut reaches the sink
// with the connection's ids and the registered harness, one cut per change.
func TestRealProxyHandsTheSinkItsObservations(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	source := newLinkSource()
	listenObservedProxy(t, runtimeDir, registration, newSocketGuard(), source)
	store, err := NewInputGuardStore(db, fixedClock{now: testNow})
	require.NoError(t, err)
	sink := newObservationSink(false)
	startTestClient(t, db, runtimeDir, testSessionID, store, fixedClock{now: testNow},
		func(opts *Options) { opts.Observations = sink })

	want := func(generation int64) protocol.StateObservationReport {
		report := linkObservation(generation)
		report.Harness = registration.Manifest.Harness
		report.HarnessVersion = registration.Manifest.HarnessVersion
		return report
	}
	require.Equal(t, want(1), sink.next(t), "the cut the announcement starts")
	source.change()
	require.Equal(t, want(2), sink.next(t), "the cut a change starts")
}

// A link with no sink announces nothing, and the real agent socket never
// cuts its session. Each guard report the link stored was written after the
// greeting, by the writer that would have cut first.
func TestRealProxyCutsNothingForALinkWithoutASink(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	source := newLinkSource()
	guard := newSocketGuard()
	listenObservedProxy(t, runtimeDir, registration, guard, source)
	store, err := NewInputGuardStore(db, fixedClock{now: testNow})
	require.NoError(t, err)
	client := startTestClient(t, db, runtimeDir, testSessionID, store, fixedClock{now: testNow}, nil)
	waitForState(t, client, StateUp)

	for version := int64(2); version <= 3; version++ {
		guard.set(protocol.DraftStateClosed, version, version)
		require.Eventually(t, func() bool {
			var stored int64
			err := db.QueryRowContext(t.Context(), `SELECT version FROM input_guards WHERE session_id = ?`,
				testSessionID).Scan(&stored)
			return err == nil && stored == version
		}, 5*time.Second, time.Millisecond)
	}
	require.Zero(t, source.cuts())
}
