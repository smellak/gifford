package tmuxattach

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	testNotice          protocol.NoticeLine = FixedNoticeLine
	testEventRevalidate                     = "revalidate"
)

var (
	testServer = Server{OwnerUser: "agent_owner", OwnerUID: 1001, Socket: "/run/user/1001/tmux.sock"}
	testTarget = Target{Server: testServer, Pane: "%7"}
)

type fakeLock struct {
	mu       sync.Mutex
	held     bool
	unlocked int
	err      error
}

func (l *fakeLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = false
	l.unlocked++
	return l.err
}

func (l *fakeLock) isHeld() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

type fakeLocker struct {
	lock *fakeLock
	err  error
}

func (l fakeLocker) Lock(context.Context, Target) (Lock, error) {
	if l.err != nil {
		return nil, l.err
	}
	l.lock.mu.Lock()
	l.lock.held = true
	l.lock.mu.Unlock()
	return l.lock, nil
}

type commandCall struct {
	target Server
	args   []string
}

type fakeRunner struct {
	lock    *fakeLock
	calls   []commandCall
	outputs [][]byte
	errors  []error
	block   bool
}

func (r *fakeRunner) Run(ctx context.Context, target Server, args ...string) ([]byte, error) {
	if r.lock != nil && !r.lock.isHeld() {
		return nil, errors.New("command ran without lock")
	}
	r.calls = append(r.calls, commandCall{target: target, args: slices.Clone(args)})
	if r.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	index := len(r.calls) - 1
	var output []byte
	if index < len(r.outputs) {
		output = r.outputs[index]
	}
	if index < len(r.errors) && r.errors[index] != nil {
		return output, r.errors[index]
	}
	return output, nil
}

type fakeGuard struct {
	events      *[]string
	revalidate  error
	verify      error
	submitted   error
	record      error
	lock        *fakeLock
	recorded    []Result
	firstScreen []byte
	lastScreen  []byte
	finalScreen []byte
	observation PaneObservation
	clients     []byte
}

func (g *fakeGuard) Revalidate(
	_ context.Context,
	_ Target,
	observation PaneObservation,
	clients []byte,
	screen []byte,
) error {
	*g.events = append(*g.events, testEventRevalidate)
	g.observation = observation
	g.clients = slices.Clone(clients)
	g.firstScreen = slices.Clone(screen)
	return g.revalidate
}

func (g *fakeGuard) VerifyNotice(_ context.Context, _ Target, screen []byte, notice protocol.NoticeLine) error {
	*g.events = append(*g.events, "verify:"+string(notice))
	g.lastScreen = slices.Clone(screen)
	return g.verify
}

func (g *fakeGuard) VerifySubmitted(_ context.Context, _ Target, screen []byte) error {
	*g.events = append(*g.events, "submitted")
	g.finalScreen = slices.Clone(screen)
	return g.submitted
}

func (g *fakeGuard) Record(_ context.Context, result Result) error {
	*g.events = append(*g.events, "record:"+string(result.Outcome))
	if g.lock != nil && !g.lock.isHeld() {
		return errors.New("record ran without lock")
	}
	g.recorded = append(g.recorded, result)
	return g.record
}

func newFakeTransport(t *testing.T, runner *fakeRunner, lock *fakeLock, timeout time.Duration) *Transport {
	t.Helper()
	transport, err := newTransport(Options{Locker: fakeLocker{lock: lock}, Timeout: timeout}, runner)
	require.NoError(t, err)
	return transport
}

func TestCaptureUsesEscapePreservingVisiblePane(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{outputs: [][]byte{[]byte("\x1b[2mat prompt\x1b[0m")}}
	transport := newFakeTransport(t, runner, lock, time.Second)

	screen, err := transport.Capture(t.Context(), testTarget)
	require.NoError(t, err)
	require.Equal(t, []byte("\x1b[2mat prompt\x1b[0m"), screen)
	require.Equal(t, []string{commandCapturePane, "-p", "-e", "-t", "%7"}, runner.calls[0].args)
	require.True(t, validOwnerUser(testTarget.Server.OwnerUser))
	require.Equal(t, []string{
		"-n", "-u", "agent_owner", "--", "tmux", "-S", "/run/user/1001/tmux.sock",
		commandCapturePane, "-p", "-e", "-t", "%7",
	}, commandArguments(testServer, runner.calls[0].args...))
}

func TestDiscoveryAndClientActivityUseFixedFormats(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{outputs: [][]byte{
		[]byte("%7\t4242\n%8\t4243\n"),
		[]byte("%7\t1730000000\n%8\t1730000001\n"),
	}}
	transport := newFakeTransport(t, runner, lock, time.Second)

	panes, err := transport.ListPanes(t.Context(), testServer)
	require.NoError(t, err)
	require.Equal(t, []PaneObservation{{PaneID: "%7", PanePID: 4242}, {PaneID: "%8", PanePID: 4243}}, panes)
	clients, err := transport.ListClientActivity(t.Context(), testTarget)
	require.NoError(t, err)
	require.Equal(t, []byte("%7\t1730000000\n%8\t1730000001\n"), clients)
	require.Equal(t, [][]string{
		{"list-panes", "-a", "-F", "#{pane_id}\t#{pane_pid}"},
		{"list-clients", "-F", "#{pane_id}\t#{client_activity}"},
	}, callArgs(runner.calls))
}

func TestClientActivityPreservesNeverActiveRowsForDetector(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{outputs: [][]byte{[]byte("%7\t\n")}}
	transport := newFakeTransport(t, runner, lock, time.Second)

	clients, err := transport.ListClientActivity(t.Context(), testTarget)
	require.NoError(t, err)
	require.Equal(t, []byte("%7\t\n"), clients)
}

func TestPaneObservationBoundsDimensions(t *testing.T) {
	t.Parallel()

	got, err := parsePaneObservation([]byte("%7\t4242\t120\t40\n"))
	require.NoError(t, err)
	require.Equal(t, PaneObservation{PaneID: "%7", PanePID: 4242, Cols: 120, Rows: 40}, got)

	for _, bad := range []string{
		"%01\t4242\t120\t40\n",
		"%7\t4242\t0\t40\n",
		"%7\t4242\t120\t0\n",
		"%7\t4242\t4097\t40\n",
		"%7\t4242\t4096\t4096\n",
		"%7\t4242\t120\t40\n%8\t4243\t80\t24\n",
	} {
		_, err := parsePaneObservation([]byte(bad))
		require.ErrorIs(t, err, ErrCommand, bad)
	}
}

func TestPaneTargetsRequireCanonicalDecimalIDs(t *testing.T) {
	t.Parallel()

	require.True(t, validCanonicalPane("%0"))
	require.True(t, validCanonicalPane("%7"))
	require.True(t, validCanonicalPane("%123"))
	require.False(t, validCanonicalPane("%01"))
	require.False(t, validCanonicalPane("%000000001"))
	require.ErrorIs(t, (Target{Server: testServer, Pane: "%01"}).validate(), ErrInvalidOptions)
	_, err := parsePaneObservations([]byte("%01\t4242\n"))
	require.ErrorIs(t, err, ErrCommand)
}

func TestDeliverLocksRevalidatesAndSubmitsInOrder(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock, outputs: [][]byte{
		[]byte("clean prompt"), []byte("%7\t4242\t120\t40\n"), []byte("%7\t1730000000\n"),
		nil, []byte("prompt " + string(testNotice)), nil, []byte("agent working"),
	}}
	events := []string{}
	guard := &fakeGuard{events: &events, lock: lock}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{Target: testTarget, Notice: testNotice}, guard)
	require.NoError(t, err)
	require.Equal(t, Result{Outcome: OutcomeCommitted, Stage: StageConfirm}, result)
	require.Equal(t, []string{
		testEventRevalidate, "verify:" + string(testNotice), "submitted", "record:" + string(OutcomeCommitted),
	}, events)
	require.Equal(t, []Result{{Outcome: OutcomeCommitted, Stage: StageConfirm}}, guard.recorded)
	require.Equal(t, []byte("clean prompt"), guard.firstScreen)
	require.Equal(t, []byte("prompt "+string(testNotice)), guard.lastScreen)
	require.Equal(t, []byte("agent working"), guard.finalScreen)
	require.Equal(t, PaneObservation{PaneID: "%7", PanePID: 4242, Cols: 120, Rows: 40}, guard.observation)
	require.Equal(t, []byte("%7\t1730000000\n"), guard.clients)
	require.Equal(t, [][]string{
		{commandCapturePane, "-p", "-e", "-t", "%7"},
		{"display-message", "-p", "-t", "%7", "#{pane_id}\t#{pane_pid}\t#{pane_width}\t#{pane_height}"},
		{"list-clients", "-F", "#{pane_id}\t#{client_activity}"},
		{"send-keys", "-l", "-t", "%7", string(testNotice)},
		{commandCapturePane, "-p", "-e", "-t", "%7"},
		{"send-keys", "-t", "%7", "Enter"},
		{commandCapturePane, "-p", "-e", "-t", "%7"},
	}, callArgs(runner.calls))
	require.Equal(t, 1, lock.unlocked)
	require.False(t, lock.isHeld())
}

type recordingLocker struct {
	lock    *fakeLock
	targets []Target
}

func (l *recordingLocker) Lock(ctx context.Context, target Target) (Lock, error) {
	l.targets = append(l.targets, target)
	return fakeLocker{lock: l.lock}.Lock(ctx, target)
}

// One transport, as one node, delivers to three panes in turn: B has A's pane
// id on another server, and C shares A's server and window. Each delivery
// locks and runs every command on its own server and pane only.
func TestDeliverRunsOnlyOnItsOwnPane(t *testing.T) {
	t.Parallel()

	other := Server{OwnerUser: "other_owner", OwnerUID: 1002, Socket: "/run/user/1002/tmux.sock"}
	targets := []Target{
		{Server: testServer, Pane: "%0"},
		{Server: other, Pane: "%0"},
		{Server: testServer, Pane: "%1"},
	}
	const callsPerDelivery = 7
	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock}
	for _, target := range targets {
		pane := string(target.Pane)
		runner.outputs = append(runner.outputs,
			[]byte("clean prompt"), []byte(pane+"\t4242\t120\t40\n"), []byte(pane+"\t1730000000\n"),
			nil, []byte("prompt "+string(testNotice)), nil, []byte("agent working"),
		)
	}
	locker := &recordingLocker{lock: lock}
	transport, err := newTransport(Options{Locker: locker, Timeout: time.Second}, runner)
	require.NoError(t, err)

	for i, target := range targets {
		events := []string{}
		result, err := transport.Deliver(t.Context(), Request{Target: target, Notice: testNotice},
			&fakeGuard{events: &events, lock: lock})
		require.NoError(t, err)
		require.Equal(t, OutcomeCommitted, result.Outcome)
		require.Len(t, runner.calls, (i+1)*callsPerDelivery)
		for _, call := range runner.calls[i*callsPerDelivery:] {
			require.Equal(t, target.Server, call.target, "%v", call.args)
			if at := slices.Index(call.args, "-t"); at >= 0 {
				require.Equal(t, string(target.Pane), call.args[at+1], "%v", call.args)
			}
		}
	}
	require.Equal(t, targets, locker.targets)
}

func TestDeliverFailsClosedBeforeWriting(t *testing.T) {
	t.Parallel()

	guardErr := errors.New("pane busy")
	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock, outputs: [][]byte{
		[]byte("busy"), []byte("%7\t4242\t120\t40\n"), []byte("%7\t1730000000\n"),
	}}
	events := []string{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{Target: testTarget, Notice: testNotice}, &fakeGuard{
		events: &events, revalidate: guardErr,
	})
	require.ErrorIs(t, err, guardErr)
	require.Equal(t, Result{Outcome: OutcomeNotWritten, Stage: StageRevalidate}, result)
	require.Len(t, runner.calls, 3)
	require.Equal(t, 1, lock.unlocked)
	require.Equal(t, []string{testEventRevalidate, "record:" + string(OutcomeNotWritten)}, events)
}

func TestDeliverNeverRetriesAfterStartingWrite(t *testing.T) {
	t.Parallel()

	commandErr := errors.New("tmux exited")
	lock := &fakeLock{}
	runner := &fakeRunner{
		lock: lock,
		outputs: [][]byte{
			[]byte("clean"), []byte("%7\t4242\t120\t40\n"), []byte(""), nil,
		},
		errors: []error{nil, nil, nil, commandErr},
	}
	events := []string{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{Target: testTarget, Notice: testNotice}, &fakeGuard{events: &events})
	require.ErrorIs(t, err, commandErr)
	require.Equal(t, Result{Outcome: OutcomeUncertain, Stage: StageWrite}, result)
	require.Len(t, runner.calls, 4)
	require.Equal(t, 1, lock.unlocked)
	require.Equal(t, []string{testEventRevalidate, "record:" + string(OutcomeUncertain)}, events)
}

func TestDeliverLeavesUnverifiedTextUncertain(t *testing.T) {
	t.Parallel()

	verifyErr := errors.New("notice not exact")
	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock, outputs: [][]byte{
		[]byte("clean"), []byte("%7\t4242\t120\t40\n"), []byte(""), nil, []byte("mixed text"),
	}}
	events := []string{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{Target: testTarget, Notice: testNotice}, &fakeGuard{
		events: &events, verify: verifyErr,
	})
	require.ErrorIs(t, err, verifyErr)
	require.Equal(t, Result{Outcome: OutcomeUncertain, Stage: StageVerify}, result)
	require.Len(t, runner.calls, 5, "verification failure must not press Enter or erase text")
	require.Equal(t, []string{
		testEventRevalidate, "verify:" + string(testNotice), "record:" + string(OutcomeUncertain),
	}, events)
}

func TestDeliverRecordFailureAfterSubmitStaysUncertain(t *testing.T) {
	t.Parallel()

	recordErr := errors.New("durable result unavailable")
	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock, outputs: [][]byte{
		[]byte("clean"), []byte("%7\t4242\t120\t40\n"), []byte(""),
		nil, []byte("prompt " + string(testNotice)), nil, []byte("agent working"),
	}}
	events := []string{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{Target: testTarget, Notice: testNotice}, &fakeGuard{
		events: &events, lock: lock, record: recordErr,
	})
	require.ErrorIs(t, err, recordErr)
	require.Equal(t, Result{Outcome: OutcomeUncertain, Stage: StageConfirm}, result)
	require.Equal(t, 1, lock.unlocked)
	require.False(t, lock.isHeld())
}

func TestDeliverLeavesAmbiguousPostEnterScreenUncertain(t *testing.T) {
	t.Parallel()

	confirmErr := errors.New("notice still present")
	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock, outputs: [][]byte{
		[]byte("clean"), []byte("%7\t4242\t120\t40\n"), []byte(""),
		nil, []byte("prompt " + string(testNotice)), nil, []byte("prompt " + string(testNotice)),
	}}
	events := []string{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{Target: testTarget, Notice: testNotice}, &fakeGuard{
		events: &events, lock: lock, submitted: confirmErr,
	})
	require.ErrorIs(t, err, confirmErr)
	require.Equal(t, Result{Outcome: OutcomeUncertain, Stage: StageConfirm}, result)
	require.Len(t, runner.calls, 7, "post-Enter ambiguity must never retry or erase text")
	require.Equal(t, []string{
		testEventRevalidate, "verify:" + string(testNotice), "submitted", "record:" + string(OutcomeUncertain),
	}, events)
	require.Equal(t, 1, lock.unlocked)
}

func TestEveryCommandHasAnIndependentTimeout(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{block: true}
	transport := newFakeTransport(t, runner, lock, time.Millisecond)

	_, err := transport.Capture(t.Context(), testTarget)
	require.ErrorIs(t, err, ErrCommandTimeout)
}

func TestOnlyTheContentFreeNoticeCanBeWritten(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{
		Target: testTarget, Notice: "read message 123",
	}, &fakeGuard{events: &[]string{}})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Equal(t, Result{Outcome: OutcomeNotWritten, Stage: StageLock}, result)
	require.Empty(t, runner.calls)
	require.Zero(t, lock.unlocked)
}

func TestOnlyConfiguredContentFreeNoticeLinesAreAllowed(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{}
	transport := newFakeTransport(t, runner, lock, time.Second)
	for _, notice := range []protocol.NoticeLine{FixedNoticeLine, DefaultBridgeNoticeLine} {
		require.True(t, transport.allowedNotice(notice))
		require.True(t, notice.Valid())
	}
	for _, notice := range []protocol.NoticeLine{"", "read message 123", "[GIFFORD] 2 pending messages"} {
		require.False(t, transport.allowedNotice(notice))
	}

	configured := protocol.NoticeLine("[GIFFORD] 2 pending messages: check the configured inbox")
	transport, err := newTransport(Options{
		Locker: fakeLocker{lock: lock}, AllowedNotices: []protocol.NoticeLine{configured},
	}, runner)
	require.NoError(t, err)
	require.True(t, transport.allowedNotice(configured))
	require.False(t, transport.allowedNotice(FixedNoticeLine))

	for _, invalid := range []protocol.NoticeLine{"", "[GIFFORD] pending; run another command"} {
		_, err := newTransport(Options{
			Locker: fakeLocker{lock: lock}, AllowedNotices: []protocol.NoticeLine{invalid},
		}, runner)
		require.ErrorIs(t, err, ErrInvalidOptions)
	}
}

func TestInvalidRequestNeverLocksOrRuns(t *testing.T) {
	t.Parallel()

	lock := &fakeLock{}
	runner := &fakeRunner{}
	transport := newFakeTransport(t, runner, lock, time.Second)

	result, err := transport.Deliver(t.Context(), Request{
		Target: Target{Server: Server{OwnerUser: "BAD USER", OwnerUID: 0, Socket: "relative"}, Pane: "1"}, Notice: "bad\nline",
	}, &fakeGuard{events: &[]string{}})
	require.ErrorIs(t, err, ErrInvalidOptions)
	require.Equal(t, Result{Outcome: OutcomeNotWritten, Stage: StageLock}, result)
	require.Empty(t, runner.calls)
	require.Zero(t, lock.unlocked)
}

func TestServerValidationRejectsRootOwnerUID(t *testing.T) {
	t.Parallel()

	server := testServer
	server.OwnerUID = 0
	require.ErrorIs(t, server.validate(), ErrInvalidOptions)
}

func callArgs(calls []commandCall) [][]string {
	result := make([][]string, 0, len(calls))
	for _, call := range calls {
		result = append(result, call.args)
	}
	return result
}
