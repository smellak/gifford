//go:build linux

package agentlink

import (
	"context"
	"encoding/json"
	"fmt"
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

type durableProxyConnection struct {
	conn    *net.UnixConn
	support protocol.AgentSocketSupport
}

// durableScriptedProxy performs only the greeting and support negotiation.
// Tests retain both directions of each connection so no result can be hidden
// by a background drain.
type durableScriptedProxy struct {
	listener *net.UnixListener
	ready    chan durableProxyConnection
	err      chan error
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once

	mu       sync.Mutex
	accepted []*net.UnixConn
}

func listenDurableScriptedProxy(
	t *testing.T,
	runtimeDir string,
	registration protocol.RegisterAgent,
) *durableScriptedProxy {
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
	proxy := &durableScriptedProxy{
		listener: listener, ready: make(chan durableProxyConnection, 4),
		err: make(chan error, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go proxy.accept(registration)
	t.Cleanup(proxy.close)
	return proxy
}

func (p *durableScriptedProxy) accept(registration protocol.RegisterAgent) {
	defer close(p.done)
	for {
		conn, err := p.listener.AcceptUnix()
		if err != nil {
			select {
			case <-p.stop:
				return
			default:
				p.fail(fmt.Errorf("accept durable proxy: %w", err))
				return
			}
		}
		p.mu.Lock()
		p.accepted = append(p.accepted, conn)
		p.mu.Unlock()
		if err := greet(conn, registration); err != nil {
			p.fail(err)
			return
		}
		announcement, err := agentsock.ReadFrame(conn)
		if err != nil {
			p.fail(fmt.Errorf("read durable support announcement: %w", err))
			return
		}
		if announcement.Op != protocol.AgentOpAnnounceSupport || announcement.ReplyTo != nil {
			p.fail(fmt.Errorf("unexpected durable support announcement: %s", announcement.Op))
			return
		}
		var support protocol.AgentSocketSupport
		if err := json.Unmarshal(announcement.Body, &support); err != nil {
			p.fail(fmt.Errorf("decode durable support announcement: %w", err))
			return
		}
		select {
		case p.ready <- durableProxyConnection{conn: conn, support: support}:
		case <-p.stop:
			return
		}
	}
}

func (p *durableScriptedProxy) fail(err error) {
	select {
	case p.err <- err:
	default:
	}
}

func (p *durableScriptedProxy) next(t *testing.T) durableProxyConnection {
	t.Helper()
	select {
	case connection := <-p.ready:
		return connection
	case err := <-p.err:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("durable proxy did not complete support negotiation")
	}
	return durableProxyConnection{}
}

func (p *durableScriptedProxy) close() {
	p.once.Do(func() {
		close(p.stop)
		_ = p.listener.Close()
		p.mu.Lock()
		accepted := append([]*net.UnixConn(nil), p.accepted...)
		p.mu.Unlock()
		for _, conn := range accepted {
			_ = conn.Close()
		}
		<-p.done
	})
}

type gatedDurableWriter struct {
	mu      sync.Mutex
	events  []protocol.EventEnvelope
	started chan protocol.EventEnvelope
	release chan DurableEventReceipt
}

func newGatedDurableWriter() *gatedDurableWriter {
	return &gatedDurableWriter{
		started: make(chan protocol.EventEnvelope, 4),
		release: make(chan DurableEventReceipt, 4),
	}
}

func (w *gatedDurableWriter) StoreDurableEvent(
	ctx context.Context,
	event protocol.EventEnvelope,
) (DurableEventReceipt, error) {
	w.mu.Lock()
	w.events = append(w.events, event)
	w.mu.Unlock()
	select {
	case w.started <- event:
	case <-ctx.Done():
		return DurableEventReceipt{}, ctx.Err()
	}
	select {
	case receipt := <-w.release:
		return receipt, nil
	case <-ctx.Done():
		return DurableEventReceipt{}, ctx.Err()
	}
}

func (w *gatedDurableWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.events)
}

type durableTestLink struct {
	client       *Client
	proxy        *durableScriptedProxy
	registration protocol.RegisterAgent
	connection   durableProxyConnection
}

func startDurableTestLink(
	t *testing.T,
	writer DurableEventWriter,
	clk Clock,
	patch func(*Options),
) durableTestLink {
	t.Helper()
	db := openTestDB(t)
	registration := currentRegistration(t, testAgentID)
	insertTestSession(t, db, testSessionID, registration)
	runtimeDir := shortRuntime(t)
	proxy := listenDurableScriptedProxy(t, runtimeDir, registration)
	client := startTestClient(t, db, runtimeDir, testSessionID, &recordingWriter{}, clk,
		func(opts *Options) {
			opts.NodeID = testNodeID
			opts.DurableEvents = writer
			if patch != nil {
				patch(opts)
			}
		})
	connection := proxy.next(t)
	waitForState(t, client, StateUp)
	require.True(t, connection.support.DurableEvents)
	return durableTestLink{
		client: client, proxy: proxy, registration: registration, connection: connection,
	}
}

func durableLinkEvent(
	id protocol.ULID,
	sequence int64,
	manifest protocol.CapabilityManifest,
) protocol.EventEnvelope {
	return protocol.EventEnvelope{
		EventID: id, SchemaVersion: protocol.SchemaVersion, NodeID: testNodeID,
		SessionID: testSessionID, AgentID: testAgentID,
		Harness: manifest.Harness, HarnessVersion: manifest.HarnessVersion,
		AdapterType: manifest.AdapterType, SourceQuality: protocol.SourceQualityPTYScreen,
		EventType: protocol.EventTypeIOChunk, Direction: protocol.DirectionOut,
		Stream: protocol.StreamPTY, Origin: protocol.OriginHarness,
		WallTime: testNow, MonotonicNS: sequence, Sequence: sequence,
		Payload:     protocol.Payload(`{"byte_count":1,"ansi_present":false,"recording_sequence":1}`),
		Sensitivity: protocol.SensitivityNormal,
	}
}

func sendDurableProxyFrame(
	t *testing.T,
	conn *net.UnixConn,
	frameID int64,
	op protocol.AgentOp,
	body any,
) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	require.NoError(t, agentsock.WriteFrame(conn, protocol.AgentFrame{
		Op: op, FrameID: frameID, SentAt: testNow, Body: raw,
	}))
}

func readDurableResult(t *testing.T, conn *net.UnixConn) (protocol.AgentFrame, protocol.EmitEventResult) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	frame, err := agentsock.ReadFrame(conn)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	require.Equal(t, protocol.AgentOpEmitEventResult, frame.Op)
	var result protocol.EmitEventResult
	require.NoError(t, json.Unmarshal(frame.Body, &result))
	return frame, result
}

func TestDurableEventResultWaitsForStoreWithoutBlockingReports(t *testing.T) {
	writer := newGatedDurableWriter()
	link := startDurableTestLink(t, writer, fixedClock{now: testNow}, nil)
	event := durableLinkEvent("01JB0000000000000000000090", 1, link.registration.Manifest)
	wireEvent := event
	wireEvent.Payload = protocol.Payload(`{ "recording_sequence": 1, "ansi_present": false, "byte_count": 1 }`)
	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, wireEvent)

	select {
	case stored := <-writer.started:
		require.Equal(t, event, stored)
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive event")
	}
	sendDurableProxyFrame(t, link.connection.conn, 3, protocol.AgentOpHeartbeat,
		protocol.AgentTarget{AgentID: testAgentID})
	report := protocol.InputGuardReport{
		AgentID: testAgentID, SessionID: testSessionID,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DraftGeneration: 2, InputGuardVersion: 2,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	}
	sendDurableProxyFrame(t, link.connection.conn, 4, protocol.AgentOpReportInputGuard, report)
	require.Eventually(t, func() bool {
		snapshot := link.client.Snapshot()
		return snapshot.LastHeartbeatAt.Equal(testNow) && snapshot.HasReport &&
			snapshot.LastReport.InputGuardVersion == 2
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, link.connection.conn.SetReadDeadline(time.Now().Add(75*time.Millisecond)))
	_, err := agentsock.ReadFrame(link.connection.conn)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "no result may precede the durable receipt")
	require.NoError(t, link.connection.conn.SetReadDeadline(time.Time{}))

	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	frame, result := readDurableResult(t, link.connection.conn)
	require.NotNil(t, frame.ReplyTo)
	require.Equal(t, int64(2), *frame.ReplyTo)
	require.Equal(t, event.EventID, result.EventID)
	require.Equal(t, protocol.EmitEventStatusStored, result.Status)
	require.Empty(t, result.Reason)
	require.Equal(t, 1, writer.count())
}

func TestDurableEventRejectsDisallowedTypeAndPayloadWithoutStore(t *testing.T) {
	writer := newGatedDurableWriter()
	link := startDurableTestLink(t, writer, fixedClock{now: testNow}, nil)
	event := durableLinkEvent("01JB0000000000000000000091", 1, link.registration.Manifest)
	event.EventType = protocol.EventTypeOutputPTYChunk
	event.Payload = protocol.Payload(`{}`)
	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, event)
	frame, result := readDurableResult(t, link.connection.conn)
	require.NotNil(t, frame.ReplyTo)
	require.Equal(t, int64(2), *frame.ReplyTo)
	require.Equal(t, protocol.EmitEventStatusRejected, result.Status)
	require.Equal(t, protocol.EmitEventRejectReasonNotAllowed, result.Reason)
	require.Zero(t, writer.count())

	event = durableLinkEvent("01JB0000000000000000000092", 2, link.registration.Manifest)
	event.Payload = protocol.Payload(`{"byte_count":1,"ansi_present":false,"recording_sequence":2,"sha256":"forbidden"}`)
	sendDurableProxyFrame(t, link.connection.conn, 3, protocol.AgentOpEmitEvent, event)
	frame, result = readDurableResult(t, link.connection.conn)
	require.NotNil(t, frame.ReplyTo)
	require.Equal(t, int64(3), *frame.ReplyTo)
	require.Equal(t, protocol.EmitEventStatusRejected, result.Status)
	require.Equal(t, protocol.EmitEventRejectReasonNotAllowed, result.Reason)
	require.Zero(t, writer.count())

	sendDurableProxyFrame(t, link.connection.conn, 4, protocol.AgentOpHeartbeat,
		protocol.AgentTarget{AgentID: testAgentID})
	require.Eventually(t, func() bool {
		return link.client.Snapshot().LastHeartbeatAt.Equal(testNow)
	}, 5*time.Second, time.Millisecond)
}

func TestDurableEventAdmitsNextAfterReceiptIsVisible(t *testing.T) {
	writer := newGatedDurableWriter()
	firstID := protocol.ULID("01JB0000000000000000000100")
	secondID := protocol.ULID("01JB0000000000000000000101")
	resultSent := make(chan struct{})
	releaseCompletion := make(chan struct{})
	offerWaiting := make(chan struct{}, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCompletion) }) }
	t.Cleanup(release)
	link := startDurableTestLink(t, writer, fixedClock{now: testNow}, func(opts *Options) {
		opts.durableHooks.afterResultSend = func(eventID protocol.ULID) {
			if eventID != firstID {
				return
			}
			close(resultSent)
			<-releaseCompletion
		}
		opts.durableHooks.beforeOffer = func(eventID protocol.ULID) {
			if eventID != secondID {
				return
			}
			select {
			case offerWaiting <- struct{}{}:
			default:
			}
		}
	})
	first := durableLinkEvent(firstID, 1, link.registration.Manifest)
	second := durableLinkEvent(secondID, 2, link.registration.Manifest)

	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, first)
	select {
	case stored := <-writer.started:
		require.Equal(t, first.EventID, stored.EventID)
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive first event")
	}
	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	firstFrame, firstResult := readDurableResult(t, link.connection.conn)
	<-resultSent
	require.NotNil(t, firstFrame.ReplyTo)
	require.Equal(t, int64(2), *firstFrame.ReplyTo)
	require.Equal(t, first.EventID, firstResult.EventID)

	sendDurableProxyFrame(t, link.connection.conn, 3, protocol.AgentOpEmitEvent, second)
	select {
	case <-offerWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("second event did not reach the visible-receipt transition")
	}
	require.Equal(t, StateUp, link.client.Snapshot().State)
	require.Equal(t, 1, writer.count(),
		"the successor must not reach storage before the receipt write is confirmed")
	release()

	select {
	case stored := <-writer.started:
		require.Equal(t, second.EventID, stored.EventID)
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive second event")
	}
	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	secondFrame, secondResult := readDurableResult(t, link.connection.conn)
	require.NotNil(t, secondFrame.ReplyTo)
	require.Equal(t, int64(3), *secondFrame.ReplyTo)
	require.Equal(t, second.EventID, secondResult.EventID)
	require.Equal(t, 2, writer.count())
	require.Equal(t, StateUp, link.client.Snapshot().State)

	require.NoError(t, link.connection.conn.SetReadDeadline(time.Now().Add(75*time.Millisecond)))
	_, err := agentsock.ReadFrame(link.connection.conn)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "each durable event must have exactly one result")
	require.NoError(t, link.connection.conn.SetReadDeadline(time.Time{}))
}

func TestDurableEventRejectsNextBeforeReceiptWrite(t *testing.T) {
	writer := newGatedDurableWriter()
	firstID := protocol.ULID("01JB0000000000000000000104")
	secondID := protocol.ULID("01JB0000000000000000000105")
	resultSendStarted := make(chan struct{}, 1)
	offerRejected := make(chan struct{}, 1)
	link := startDurableTestLink(t, writer, fixedClock{now: testNow}, func(opts *Options) {
		opts.durableHooks.beforeResultSend = func(eventID protocol.ULID) {
			if eventID == firstID {
				resultSendStarted <- struct{}{}
			}
		}
		opts.durableHooks.afterOfferReject = func(eventID protocol.ULID) {
			if eventID == secondID {
				offerRejected <- struct{}{}
			}
		}
	})
	link.client.sendGate <- struct{}{}
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(link.client.releaseSend) }
	t.Cleanup(releaseGate)

	first := durableLinkEvent(firstID, 1, link.registration.Manifest)
	second := durableLinkEvent(secondID, 2, link.registration.Manifest)
	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, first)
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive first event")
	}
	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	select {
	case <-resultSendStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("durable result send did not start")
	}

	sendDurableProxyFrame(t, link.connection.conn, 3, protocol.AgentOpEmitEvent, second)
	select {
	case <-offerRejected:
	case <-time.After(5 * time.Second):
		t.Fatal("early second event was not rejected")
	}
	releaseGate()
	snapshot := waitForState(t, link.client, StateDown)
	require.Equal(t, ReasonConnectionLost, snapshot.Reason)
	require.Equal(t, 1, writer.count(), "the early second event must not reach storage")
}

func TestDurableEventStalledReceiptAllowsHeartbeatTimeout(t *testing.T) {
	writer := newGatedDurableWriter()
	clk := newManualClock()
	const heartbeatTimeout = 40 * time.Millisecond
	resultSendStarted := make(chan struct{}, 1)
	eventID := protocol.ULID("01JB0000000000000000000106")
	link := startDurableTestLink(t, writer, clk, func(opts *Options) {
		opts.HeartbeatTimeout = heartbeatTimeout
		opts.durableHooks.beforeResultSend = func(got protocol.ULID) {
			if got == eventID {
				resultSendStarted <- struct{}{}
			}
		}
	})
	require.NoError(t, link.connection.conn.SetReadBuffer(1<<10))
	link.client.mu.RLock()
	clientConn := link.client.conn
	link.client.mu.RUnlock()
	require.NotNil(t, clientConn)
	require.NoError(t, clientConn.SetWriteBuffer(1<<10))

	prepare := testPrepare("delivery-stalled-durable")
	prepare.ControlVersions = make(protocol.ControlVersions, 64)
	for index := range 64 {
		prefix := fmt.Sprintf("scope-%02d-", index)
		prepare.ControlVersions[prefix+strings.Repeat("x", 200-len(prefix))] = int64(index + 1)
	}
	sendDone := make(chan error, 1)
	go func() {
		_, err := link.client.Send(t.Context(), protocol.AgentOpPrepareDelivery, prepare)
		sendDone <- err
	}()
	require.Eventually(t, func() bool { return len(link.client.sendGate) == 1 },
		5*time.Second, time.Millisecond, "large delivery did not occupy the send gate")

	event := durableLinkEvent(eventID, 1, link.registration.Manifest)
	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, event)
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive event")
	}
	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	select {
	case <-resultSendStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("durable result send did not reach the occupied gate")
	}
	select {
	case err := <-sendDone:
		t.Fatalf("large delivery stopped blocking before heartbeat: %v", err)
	default:
	}
	require.Len(t, link.client.sendGate, 1, "large delivery must still own the send gate")

	clk.fire(t, heartbeatTimeout)
	waitForReason(t, link.client, ReasonHeartbeatTimeout)
	select {
	case err := <-sendDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("stalled delivery send did not unblock")
	}
}

func TestDurableEventRetainsReceiptAcrossReconnectReplay(t *testing.T) {
	writer := newGatedDurableWriter()
	clk := newManualClock()
	const reconnectDelay = 25 * time.Millisecond
	link := startDurableTestLink(t, writer, clk, func(opts *Options) {
		opts.ReconnectMin = reconnectDelay
		opts.ReconnectMax = reconnectDelay
	})
	event := durableLinkEvent("01JB0000000000000000000093", 1, link.registration.Manifest)
	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, event)
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive event")
	}

	require.NoError(t, link.connection.conn.Close())
	waitForState(t, link.client, StateDown)
	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	clk.fire(t, reconnectDelay)
	reconnected := link.proxy.next(t)
	require.True(t, reconnected.support.DurableEvents)
	waitForState(t, link.client, StateUp)
	sendDurableProxyFrame(t, reconnected.conn, 2, protocol.AgentOpEmitEvent, event)

	frame, result := readDurableResult(t, reconnected.conn)
	require.NotNil(t, frame.ReplyTo)
	require.Equal(t, int64(2), *frame.ReplyTo)
	require.Equal(t, event.EventID, result.EventID)
	require.Equal(t, protocol.EmitEventStatusStored, result.Status)
	require.Equal(t, 1, writer.count(), "an exact reconnect replay reuses the retained receipt")
}

func TestDurableEventReconnectReplayWhileNextStoreIsPending(t *testing.T) {
	writer := newGatedDurableWriter()
	clk := newManualClock()
	const reconnectDelay = 25 * time.Millisecond
	replayOffered := make(chan struct{}, 1)
	secondID := protocol.ULID("01JB0000000000000000000103")
	link := startDurableTestLink(t, writer, clk, func(opts *Options) {
		opts.ReconnectMin = reconnectDelay
		opts.ReconnectMax = reconnectDelay
		opts.durableHooks.afterOffer = func(eventID protocol.ULID, frameID int64) {
			if eventID != secondID || frameID != 2 {
				return
			}
			select {
			case replayOffered <- struct{}{}:
			default:
			}
		}
	})
	first := durableLinkEvent("01JB0000000000000000000102", 1, link.registration.Manifest)
	second := durableLinkEvent(secondID, 2, link.registration.Manifest)

	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, first)
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive first event")
	}
	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	firstFrame, firstResult := readDurableResult(t, link.connection.conn)
	require.NotNil(t, firstFrame.ReplyTo)
	require.Equal(t, int64(2), *firstFrame.ReplyTo)
	require.Equal(t, first.EventID, firstResult.EventID)

	sendDurableProxyFrame(t, link.connection.conn, 3, protocol.AgentOpEmitEvent, second)
	select {
	case stored := <-writer.started:
		require.Equal(t, second.EventID, stored.EventID)
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not begin the second store")
	}
	require.NoError(t, link.connection.conn.Close())
	waitForState(t, link.client, StateDown)
	clk.fire(t, reconnectDelay)
	reconnected := link.proxy.next(t)
	waitForState(t, link.client, StateUp)
	sendDurableProxyFrame(t, reconnected.conn, 2, protocol.AgentOpEmitEvent, second)
	select {
	case <-replayOffered:
	case <-time.After(5 * time.Second):
		t.Fatal("reconnected replay was not admitted")
	}

	writer.release <- DurableEventReceipt{Status: protocol.EmitEventStatusStored}
	secondFrame, secondResult := readDurableResult(t, reconnected.conn)
	require.NotNil(t, secondFrame.ReplyTo)
	require.Equal(t, int64(2), *secondFrame.ReplyTo)
	require.Equal(t, second.EventID, secondResult.EventID)
	require.Equal(t, 2, writer.count(), "the reconnect replay must reuse the pending store")

	require.NoError(t, reconnected.conn.SetReadDeadline(time.Now().Add(75*time.Millisecond)))
	_, err := agentsock.ReadFrame(reconnected.conn)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout(), "the replay must receive exactly one result")
	require.NoError(t, reconnected.conn.SetReadDeadline(time.Time{}))
}

func TestSuspendedDurableReceiptNeedsExactReconnectReplay(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	event := durableLinkEvent(
		"01JB0000000000000000000097", 1, currentRegistration(t, testAgentID).Manifest,
	)
	job := &durableEventJob{
		event: event, generation: 1, frameID: 2,
		result: &DurableEventReceipt{Status: protocol.EmitEventStatusStored},
	}
	client := &Client{
		ctx: ctx, durableEvent: job, durableRunning: true,
		durableWake: make(chan struct{}, 1),
	}
	require.True(t, client.suspendDurableEvent(job, 1, 2))
	require.Nil(t, client.takeDurableEvent(),
		"a retained receipt cannot rearm itself while the link is down")

	replay := durableEventJob{event: event, generation: 2, frameID: 3}
	require.NoError(t, client.offerDurableEvent(replay))
	require.Same(t, job, client.takeDurableEvent(),
		"only the exact replay on a later connection rearms the retained receipt")
	require.True(t, client.suspendDurableEvent(job, 2, 3))
	cancel()
	got, ok := client.nextDurableEvent()
	require.False(t, ok, "client shutdown must stop a suspended durable worker")
	require.Nil(t, got)
}

func TestDurableEventDropsSuccessorWhenReceiptWriteFails(t *testing.T) {
	t.Parallel()
	manifest := currentRegistration(t, testAgentID).Manifest
	current := &durableEventJob{
		event:      durableLinkEvent("01JB0000000000000000000107", 1, manifest),
		generation: 1,
		frameID:    2,
	}
	successor := durableEventJob{
		event:      durableLinkEvent("01JB0000000000000000000108", 2, manifest),
		generation: 1,
		frameID:    3,
	}
	client := &Client{
		durableEvent:   current,
		durableRunning: true,
		durableWake:    make(chan struct{}, 1),
	}

	require.True(t, client.beginDurableResultWrite(current, 1, 2))
	require.NoError(t, client.offerDurableEvent(successor))
	require.NotNil(t, current.successor)
	client.finishDurableResultWrite(current, 1, 2, false)

	require.Same(t, current, client.durableEvent)
	require.Equal(t, durableReceiptPending, current.receiptState)
	require.Nil(t, current.successor,
		"a successor cannot survive an unconfirmed receipt write")
	require.True(t, client.completeDurableEvent(current, 1, 2))
	require.Nil(t, client.durableEvent)
}

func TestDurableEventCutsConnectionOnSecondInFlightEvent(t *testing.T) {
	writer := newGatedDurableWriter()
	link := startDurableTestLink(t, writer, fixedClock{now: testNow}, nil)
	first := durableLinkEvent("01JB0000000000000000000095", 1, link.registration.Manifest)
	second := durableLinkEvent("01JB0000000000000000000096", 2, link.registration.Manifest)
	sendDurableProxyFrame(t, link.connection.conn, 2, protocol.AgentOpEmitEvent, first)
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("durable writer did not receive first event")
	}
	sendDurableProxyFrame(t, link.connection.conn, 3, protocol.AgentOpEmitEvent, second)
	waitForState(t, link.client, StateDown)
	require.Equal(t, ReasonConnectionLost, link.client.Snapshot().Reason)
	require.Equal(t, 1, writer.count(), "the second event must not reach durable storage")
}

func TestCapabilityChangeBypassesGeneralDurableWriter(t *testing.T) {
	native := testNativeCapabilityManifest()
	capabilities := &recordingCapabilityWriter{}
	durable := newGatedDurableWriter()
	client := &Client{
		opts: Options{
			NodeID: testNodeID, AgentID: testAgentID, SessionID: testSessionID,
			CapabilityWriter: capabilities, DurableEvents: durable,
		},
		ctx: context.Background(),
	}
	event := testCapabilityEnvelope(t, native)
	body, err := json.Marshal(event)
	require.NoError(t, err)
	_, err = client.dispatch(1, native, protocol.AgentFrame{
		Op: protocol.AgentOpEmitEvent, FrameID: 2, SentAt: testNow, Body: body,
	})
	require.NoError(t, err)
	require.Equal(t, 1, capabilities.calls)
	require.Zero(t, durable.count())
	require.Nil(t, client.durableEvent)
}

func TestMalformedGeneralEventIsWireErrorNotCapabilityConflict(t *testing.T) {
	durable := newGatedDurableWriter()
	client := &Client{
		opts: Options{DurableEvents: durable},
		ctx:  context.Background(),
	}
	body := protocol.Payload(`{
		"event_type":"io.chunk",
		"event_id":"01JB0000000000000000000094",
		"unknown_envelope_field":true
	}`)
	_, err := client.dispatch(1, testRegistration(testAgentID, protocol.ProcessIdentity{}).Manifest,
		protocol.AgentFrame{
			Op: protocol.AgentOpEmitEvent, FrameID: 2, SentAt: testNow, Body: body,
		})
	require.ErrorIs(t, err, ErrUnexpectedFrame)
	require.NotErrorIs(t, err, ErrCapabilitiesConflict)
	require.Zero(t, durable.count())
}
