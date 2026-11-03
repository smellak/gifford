package tmuxattach

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxhelper"
)

const (
	helperBenchOwner   = "agent_owner"
	helperBenchSocket  = "/run/user/1001/tmux.sock"
	helperVerbCapture  = "capture"
	helperVerbClients  = "clients"
	helperEventEntered = "submitted"
)

var helperBenchServer = Server{
	OwnerUser: helperBenchOwner, OwnerUID: 1001, Socket: helperBenchSocket, Bank: "s02",
}

var helperBenchTarget = Target{Server: helperBenchServer, Pane: "%7"}

type helperCall struct {
	verb, pane string
	server     Server
}

type stubHelper struct {
	calls []helperCall
	code  tmuxhelper.ExitCode
	body  []byte
	err   error
	queue []helperFrame
}

type helperFrame struct {
	code tmuxhelper.ExitCode
	body []byte
}

func (s *stubHelper) callHelper(_ context.Context, server Server, verb, pane string) (tmuxhelper.ExitCode, []byte, error) {
	s.calls = append(s.calls, helperCall{verb: verb, pane: pane, server: server})
	if s.err != nil {
		return 0, nil, s.err
	}
	if len(s.queue) > 0 {
		frame := s.queue[0]
		s.queue = s.queue[1:]
		return frame.code, frame.body, nil
	}
	return s.code, s.body, nil
}

func newHelperTransport(t *testing.T, stub *stubHelper) *Transport {
	t.Helper()
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: &fakeLock{}},
		Timeout:        time.Second,
		HelperRevision: helperBenchRevision,
	}, &fakeRunner{})
	require.NoError(t, err)
	transport.helper = stub
	return transport
}

// With a configured helper every read goes through its verb, never
// through raw tmux, carrying the server bank.
func TestHelperRoutesReadsThroughVerbs(t *testing.T) {
	t.Parallel()
	stub := &stubHelper{code: tmuxhelper.ExitOK}
	transport := newHelperTransport(t, stub)

	stub.body = []byte("%7\t4242\n%5\t5150\n")
	observations, err := transport.ListPanes(t.Context(), helperBenchServer)
	require.NoError(t, err)
	require.Len(t, observations, 2)

	stub.body = []byte("screen bytes")
	screen, err := transport.Capture(t.Context(), helperBenchTarget)
	require.NoError(t, err)
	require.Equal(t, []byte("screen bytes"), screen)

	stub.body = []byte("%7\t1730000000\n")
	clients, err := transport.ListClientActivity(t.Context(), helperBenchTarget)
	require.NoError(t, err)
	require.Equal(t, []byte("%7\t1730000000\n"), clients)

	stub.body = []byte("%7\t4242\t120\t40\n")
	observation, err := transport.observePane(t.Context(), helperBenchTarget)
	require.NoError(t, err)
	require.Equal(t, PaneObservation{PaneID: "%7", PanePID: 4242, Cols: 120, Rows: 40}, observation)

	require.Equal(t, []helperCall{
		{verb: "panes", pane: "", server: helperBenchServer},
		{verb: helperVerbCapture, pane: "%7", server: helperBenchServer},
		{verb: helperVerbClients, pane: "", server: helperBenchServer},
		{verb: "observe", pane: "%7", server: helperBenchServer},
	}, stub.calls)
}

// A configured helper without a bank refuses before executing anything:
// no bank never falls back to raw tmux.
func TestHelperRefusesWithoutBank(t *testing.T) {
	t.Parallel()
	stub := &stubHelper{code: tmuxhelper.ExitOK}
	transport := newHelperTransport(t, stub)
	plain := Server{OwnerUser: helperBenchOwner, OwnerUID: 1001, Socket: helperBenchSocket}

	_, err := transport.ListPanes(t.Context(), plain)
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = transport.Capture(t.Context(), Target{Server: plain, Pane: "%7"})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Empty(t, stub.calls)
}

// The helper verbs follow the notice role: the fixed notice is the
// full one, the bridge notice the short one. Custom notices refuse
// instead of guessing which registry entry to overwrite.
func TestHelperNoticeVerbMapping(t *testing.T) {
	verb, err := helperNoticeVerb(FixedNoticeLine)
	require.NoError(t, err)
	require.Equal(t, "notice-full", verb)
	verb, err = helperNoticeVerb(DefaultBridgeNoticeLine)
	require.NoError(t, err)
	require.Equal(t, "notice-short", verb)
	_, err = helperNoticeVerb(protocol.NoticeLine(": [GIFFORD] custom"))
	require.Error(t, err)
}

func TestHelperRevisionValidation(t *testing.T) {
	t.Parallel()
	_, err := newTransport(Options{
		Locker:         fakeLocker{lock: &fakeLock{}},
		HelperRevision: "short",
	}, &fakeRunner{})
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = newTransport(Options{
		Locker:         fakeLocker{lock: &fakeLock{}},
		HelperRevision: "0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
	}, &fakeRunner{})
	require.ErrorIs(t, err, ErrInvalidOptions, "uppercase is not lowercase hex")
}

// A delivery through the helper walks the same stages as raw tmux but
// speaks only verbs: capture, observe, clients, notice-full, then
// verify, enter, and the final capture.
func TestDeliverUsesHelperVerbs(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	stub := &stubHelper{queue: []helperFrame{
		{code: tmuxhelper.ExitOK, body: []byte("clean prompt")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t4242\t120\t40\n")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t1730000000\n")},
		{code: tmuxhelper.ExitOK},
		{code: tmuxhelper.ExitOK, body: []byte("prompt " + string(FixedNoticeLine))},
		{code: tmuxhelper.ExitOK},
		{code: tmuxhelper.ExitOK, body: []byte("agent working")},
	}}
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: lock},
		Timeout:        time.Second,
		HelperRevision: helperBenchRevision,
	}, &fakeRunner{lock: lock})
	require.NoError(t, err)
	transport.helper = stub

	events := []string{}
	guard := &fakeGuard{events: &events, lock: lock}
	result, err := transport.Deliver(t.Context(), Request{Target: helperBenchTarget, Notice: FixedNoticeLine}, guard)
	require.NoError(t, err)
	require.Equal(t, Result{Outcome: OutcomeCommitted, Stage: StageConfirm}, result)
	require.Equal(t, []string{
		testEventRevalidate, "verify:" + string(FixedNoticeLine), helperEventEntered, "record:" + string(OutcomeCommitted),
	}, events)

	verbs := make([]string, 0, len(stub.calls))
	for _, call := range stub.calls {
		verbs = append(verbs, call.verb)
		require.Equal(t, helperBenchServer, call.server)
		if call.verb == helperVerbClients || call.verb == "panes" {
			require.Empty(t, call.pane, "server verbs carry no pane")
		} else {
			require.Equal(t, "%7", call.pane)
		}
	}
	require.Equal(t, []string{helperVerbCapture, "observe", helperVerbClients, "notice-full", helperVerbCapture, "enter", helperVerbCapture}, verbs)
	require.Equal(t, 1, lock.unlocked)
	require.False(t, lock.isHeld())
}

func TestHelperMapsExitCodes(t *testing.T) {
	t.Parallel()
	for code, mapped := range map[tmuxhelper.ExitCode]error{
		tmuxhelper.ExitTimeout:  ErrCommandTimeout,
		tmuxhelper.ExitGrammar:  ErrCommand,
		tmuxhelper.ExitRevision: ErrCommand,
	} {
		stub := &stubHelper{code: code}
		transport := newHelperTransport(t, stub)
		_, err := transport.ListPanes(t.Context(), helperBenchServer)
		require.ErrorIs(t, err, mapped, "code %d", code)
	}
}
