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
	helperBenchOwner                              = "agent_owner"
	helperBenchSocket                             = "/run/user/1001/tmux.sock"
	helperVerbCapture                             = "capture"
	helperVerbClients                             = "clients"
	helperVerbObserve                             = "observe"
	helperExampleResponseLine protocol.NoticeLine = NoticePrefix +
		"[GIFFORD] response required: check your inbox and reply or report a block"
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
	return s.next()
}

func (s *stubHelper) next() (tmuxhelper.ExitCode, []byte, error) {
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

func (s *stubHelper) startHelper(
	ctx context.Context,
	server Server,
	verb string,
	pane string,
) (runningEffect, error) {
	s.calls = append(s.calls, helperCall{verb: verb, pane: pane, server: server})
	return &fakeRunningEffect{
		ctx: ctx,
		child: PaneChild{
			PID:   int64(3000 + len(s.calls)),
			Start: int64(4000 + len(s.calls)),
		},
		waitFn: func() error {
			code, body, err := s.next()
			if err != nil {
				return err
			}
			if err := helperError(code); err != nil {
				return err
			}
			if len(body) != 0 {
				return ErrCommand
			}
			return nil
		},
	}, nil
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
		{verb: helperVerbObserve, pane: "%7", server: helperBenchServer},
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

// The helper verbs follow the pinned notice slots: the fixed ordinary
// notice is the short one, the configured bridge line the full one, and
// the optional manifest response line the response one. Anything else
// refuses instead of guessing which registry entry to overwrite; equal
// slots refuse at construction like the manifest.
func TestHelperNoticeVerbMapping(t *testing.T) {
	t.Parallel()
	stub := &stubHelper{code: tmuxhelper.ExitOK}
	transport := newHelperTransport(t, stub)

	verb, err := transport.helperNoticeVerb(FixedNoticeLine)
	require.NoError(t, err)
	require.Equal(t, "notice-short", verb)
	verb, err = transport.helperNoticeVerb(DefaultBridgeNoticeLine)
	require.NoError(t, err)
	require.Equal(t, "notice-full", verb)
	_, err = transport.helperNoticeVerb(protocol.NoticeLine(": [GIFFORD] custom"))
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = transport.helperNoticeVerb(helperExampleResponseLine)
	require.ErrorIs(t, err, ErrInvalidOptions, "an absent manifest response slot stays closed")
}

func TestHelperNoticeResponseRequiresExactManifestSlot(t *testing.T) {
	t.Parallel()

	manifestLine := protocol.NoticeLine(": [GIFFORD] response required: use manifest inbox")
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: &fakeLock{}},
		Timeout:        time.Second,
		HelperRevision: helperBenchRevision,
		ResponseNotice: manifestLine,
	}, &fakeRunner{})
	require.NoError(t, err)

	verb, err := transport.helperNoticeVerb(manifestLine)
	require.NoError(t, err)
	require.Equal(t, "notice-response", verb)
	_, err = transport.helperNoticeVerb(helperExampleResponseLine)
	require.ErrorIs(t, err, ErrInvalidOptions,
		"a configured line that differs byte-for-byte from the manifest slot must refuse")
}

func TestHelperResponseMismatchRefusesBeforeWriting(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	stub := &stubHelper{queue: []helperFrame{
		{code: tmuxhelper.ExitOK, body: []byte("clean prompt")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t4242\t120\t40\n")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t1730000000\n")},
	}}
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: lock},
		Timeout:        time.Second,
		AllowedNotices: []protocol.NoticeLine{FixedNoticeLine, DefaultBridgeNoticeLine, helperExampleResponseLine},
		HelperRevision: helperBenchRevision,
		ResponseNotice: ": [GIFFORD] response required: use manifest inbox",
	}, &fakeRunner{lock: lock})
	require.NoError(t, err)
	transport.helper = stub

	result, err := transport.Deliver(t.Context(), Request{
		Target: helperBenchTarget, Notice: helperExampleResponseLine,
	}, &fakeGuard{events: &[]string{}, lock: lock})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Equal(t, Result{Outcome: OutcomeNotWritten, Stage: StageWrite}, result)
	require.Equal(t, []helperCall{
		{verb: helperVerbCapture, pane: "%7", server: helperBenchServer},
		{verb: helperVerbObserve, pane: "%7", server: helperBenchServer},
		{verb: helperVerbClients, server: helperBenchServer},
	}, stub.calls, "a mismatched response line must never start a notice helper verb")
}

func TestHelperExitGrammarNoticeIsNotWritten(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	stub := &stubHelper{queue: []helperFrame{
		{code: tmuxhelper.ExitOK, body: []byte("clean prompt")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t4242\t120\t40\n")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t1730000000\n")},
		{code: tmuxhelper.ExitGrammar},
	}}
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: lock},
		Timeout:        time.Second,
		AllowedNotices: []protocol.NoticeLine{FixedNoticeLine, DefaultBridgeNoticeLine, helperExampleResponseLine},
		HelperRevision: helperBenchRevision,
		ResponseNotice: helperExampleResponseLine,
	}, &fakeRunner{lock: lock})
	require.NoError(t, err)
	transport.helper = stub
	events := []string{}
	guard := &fakeGuard{events: &events, lock: lock}

	result, err := transport.Deliver(t.Context(), Request{
		Target: helperBenchTarget, Notice: helperExampleResponseLine,
	}, guard)
	require.ErrorIs(t, err, ErrCommand)
	require.ErrorIs(t, err, errHelperGrammar)
	require.Equal(t, Result{Outcome: OutcomeNotWritten, Stage: StageWrite}, result)
	require.Equal(t, []Result{{Outcome: OutcomeNotWritten, Stage: StageWrite}}, guard.recorded)
	require.Equal(t, []helperCall{
		{verb: helperVerbCapture, pane: "%7", server: helperBenchServer},
		{verb: helperVerbObserve, pane: "%7", server: helperBenchServer},
		{verb: helperVerbClients, server: helperBenchServer},
		{verb: "notice-response", pane: "%7", server: helperBenchServer},
	}, stub.calls)
	require.Equal(t, 1, lock.unlocked)
}

func TestHelperNoticeVerbCustomBridge(t *testing.T) {
	t.Parallel()
	custom := protocol.NoticeLine(": [GIFFORD] bridge custom")
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: &fakeLock{}},
		Timeout:        time.Second,
		HelperRevision: helperBenchRevision,
		BridgeNotice:   custom,
	}, &fakeRunner{})
	require.NoError(t, err)

	verb, err := transport.helperNoticeVerb(custom)
	require.NoError(t, err)
	require.Equal(t, "notice-full", verb)
	_, err = transport.helperNoticeVerb(DefaultBridgeNoticeLine)
	require.ErrorIs(t, err, ErrInvalidOptions, "default bridge is not a slot once configured")
}

func TestHelperNoticeVerbEqualSlotsRefuse(t *testing.T) {
	t.Parallel()

	for name, opts := range map[string]Options{
		"bridge and short": {
			BridgeNotice: FixedNoticeLine,
		},
		"response and short": {
			ResponseNotice: FixedNoticeLine,
		},
		"response and full": {
			ResponseNotice: DefaultBridgeNoticeLine,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			opts.Locker = fakeLocker{lock: &fakeLock{}}
			opts.Timeout = time.Second
			opts.HelperRevision = helperBenchRevision
			_, err := newTransport(opts, &fakeRunner{})
			require.ErrorIs(t, err, ErrInvalidOptions, "equal slots mirror the manifest rejection")
		})
	}
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
// speaks only verbs: capture, observe, clients, notice-short, then two
// identical verify captures, enter, and the final capture.
func TestDeliverUsesHelperVerbs(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	stub := &stubHelper{queue: []helperFrame{
		{code: tmuxhelper.ExitOK, body: []byte("clean prompt")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t4242\t120\t40\n")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t1730000000\n")},
		{code: tmuxhelper.ExitOK},
		{code: tmuxhelper.ExitOK, body: []byte("prompt " + string(FixedNoticeLine))},
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
		testEventRevalidate, "verify:" + string(FixedNoticeLine), "verify:" + string(FixedNoticeLine),
		testEventSubmitted, "record:" + string(OutcomeCommitted),
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
	require.Equal(t, []string{
		helperVerbCapture, helperVerbObserve, helperVerbClients, "notice-short",
		helperVerbCapture, helperVerbCapture, "enter", helperVerbCapture,
	}, verbs)
	require.Equal(t, 1, lock.unlocked)
	require.False(t, lock.isHeld())
}

// Through the helper, a notice accepted only once before the settle
// deadline is not stable: the delivery stops before the enter verb.
func TestDeliverHelperSingleAcceptedNoticeNeverPressesEnter(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	stub := &stubHelper{queue: []helperFrame{
		{code: tmuxhelper.ExitOK, body: []byte("clean prompt")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t4242\t120\t40\n")},
		{code: tmuxhelper.ExitOK, body: []byte("%7\t1730000000\n")},
		{code: tmuxhelper.ExitOK},
		{code: tmuxhelper.ExitOK, body: []byte("prompt " + string(FixedNoticeLine))},
	}}
	transport, err := newTransport(Options{
		Locker:         fakeLocker{lock: lock},
		Timeout:        time.Second,
		HelperRevision: helperBenchRevision,
	}, &fakeRunner{lock: lock})
	require.NoError(t, err)
	transport.helper = stub
	start := time.Unix(100, 0)
	reads := 0
	transport.now = func() time.Time {
		reads++
		if reads == 1 {
			return start
		}
		return start.Add(transport.settle)
	}

	events := []string{}
	result, err := transport.Deliver(t.Context(), Request{Target: helperBenchTarget, Notice: FixedNoticeLine},
		&fakeGuard{events: &events, lock: lock})
	require.ErrorIs(t, err, ErrScreenUnstable)
	require.Equal(t, Result{Outcome: OutcomeUncertain, Stage: StageVerify}, result)
	verbs := make([]string, 0, len(stub.calls))
	for _, call := range stub.calls {
		verbs = append(verbs, call.verb)
	}
	require.Equal(t, []string{helperVerbCapture, helperVerbObserve, helperVerbClients, "notice-short", helperVerbCapture}, verbs)
	require.Equal(t, 1, lock.unlocked)
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
