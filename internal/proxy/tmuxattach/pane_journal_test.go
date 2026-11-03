package tmuxattach

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/tmuxlock"
)

// journalState is an in-memory .state descriptor; syncErrs fails chosen
// fdatasync calls and synced keeps every durable record.
type journalState struct {
	raw      []byte
	syncs    int
	syncErrs map[int]error
	synced   [][]byte
	closed   bool
}

func (s *journalState) ReadAt(p []byte, offset int64) (int, error) {
	return copy(p, s.raw[offset:]), nil
}

func (s *journalState) WriteAt(p []byte, offset int64) (int, error) {
	return copy(s.raw[offset:], p), nil
}

func (s *journalState) Datasync() error {
	call := s.syncs
	s.syncs++
	if err := s.syncErrs[call]; err != nil {
		return err
	}
	s.synced = append(s.synced, bytes.Clone(s.raw))
	return nil
}

func (s *journalState) Close() error {
	s.closed = true
	return nil
}

var journalTarget = Target{
	Server: testServer, Pane: "%7",
	Authority: PaneAuthority{
		Host: "node-a", BootID: "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0", PanePID: 4242, PaneStart: 120,
	},
}

// encodeTestRecord writes the 1024-byte record layout with an encoder
// independent of the package codec: the body's JSON (its fields declared in
// alphabetical order, so canonical), space padding to byte 952, the sha256
// of the head, seven spaces and LF.
func encodeTestRecord(t *testing.T, body testRecord) []byte {
	t.Helper()
	canonical, err := json.Marshal(body)
	require.NoError(t, err)
	head := append([]byte("PSTATE03"), canonical...)
	head = append(head, bytes.Repeat([]byte(" "), 952-len(head))...)
	digest := sha256.Sum256(head)
	head = append(head, []byte(hex.EncodeToString(digest[:]))...)
	return append(head, []byte("       \n")...)
}

// testRecord is the record body as the tests read it back.
type testRecord struct {
	Attempt     string `json:"attempt"`
	BootID      string `json:"boot_id"`
	ChildPID    int64  `json:"child_pid"`
	ChildStart  string `json:"child_start"`
	Epoch       int64  `json:"epoch"`
	Gen         int64  `json:"gen"`
	Host        string `json:"host"`
	Operation   string `json:"op"`
	Owner       string `json:"owner"`
	OwnerGen    int64  `json:"owner_gen"`
	Pane        int64  `json:"pane"`
	PanePID     int64  `json:"pane_pid"`
	PaneStart   string `json:"pane_start"`
	Phase       string `json:"phase"`
	Result      string `json:"result"`
	SocketSHA   string `json:"socket_sha"`
	Step        int64  `json:"step"`
	Timestamp   string `json:"ts"`
	TxID        string `json:"txid"`
	UID         int64  `json:"uid"`
	Version     int64  `json:"v"`
	Writer      string `json:"writer"`
	WriterPID   int64  `json:"writer_pid"`
	WriterStart string `json:"writer_start"`
}

func decodeTestRecord(t *testing.T, raw []byte) testRecord {
	t.Helper()
	require.Len(t, raw, 1024)
	require.Equal(t, "PSTATE03", string(raw[:8]))
	digest := sha256.Sum256(raw[:952])
	require.Equal(t, hex.EncodeToString(digest[:]), string(raw[952:1016]))
	decoder := json.NewDecoder(bytes.NewReader(bytes.TrimRight(raw[8:952], " ")))
	decoder.DisallowUnknownFields()
	var body testRecord
	require.NoError(t, decoder.Decode(&body))
	return body
}

const (
	testPhaseIdle      = "IDLE"
	testPhaseRunning   = "EN_CURSO"
	testPhaseDone      = "HECHO"
	testPhaseUncertain = "INCIERTO"
)

func journalTestBody(phase string) testRecord {
	socket := sha256.Sum256([]byte(testServer.Socket))
	attempt, op := "-", "-"
	if phase == testPhaseRunning || phase == testPhaseUncertain {
		attempt, op = "11111111111111111111111111111111",
			"2222222222222222222222222222222222222222222222222222222222222222"
	}
	return testRecord{
		Attempt: attempt, BootID: journalTarget.Authority.BootID, ChildStart: "0", Gen: 4,
		Host: journalTarget.Authority.Host, Operation: op, Owner: "gifford", Pane: 7, PanePID: 4242,
		PaneStart: "120", Phase: phase, Result: "-", SocketSHA: hex.EncodeToString(socket[:]),
		Timestamp: "2026-11-03T09:00:00.000000000Z", TxID: "-", UID: 1001, Version: 3,
		Writer: "provision", WriterStart: "0",
	}
}

type allowChecker struct{}

func (allowChecker) Check(context.Context, Target, PaneObservation) error { return nil }

type journalHarness struct {
	state     *journalState
	runner    *fakeRunner
	transport *Transport
	guard     *fakeGuard
}

func newJournalHarness(t *testing.T, phase string, identity error, runnerErrors []error) journalHarness {
	t.Helper()
	state := &journalState{raw: encodeTestRecord(t, journalTestBody(phase)), syncErrs: map[int]error{}}
	journal := &PaneJournal{
		schema: "example-op/1",
		open: func(binding tmuxlock.PanelBinding) (tmuxlock.StateFile, error) {
			if binding != paneBinding(journalTarget) {
				return nil, errors.New("journal opened another pane")
			}
			return state, nil
		},
		writer:   func() (tmuxlock.Writer, error) { return tmuxlock.Writer{PID: 3333, Start: 555555}, nil },
		identity: func(Target) error { return identity },
		now:      func() time.Time { return time.Date(2026, 11, 3, 9, 30, 0, 0, time.UTC) },
		attempts: map[tmuxlock.PanelBinding]*tmuxlock.Journal{},
	}
	lifecycle, err := NewJournaledLifecycle(allowChecker{}, journal)
	require.NoError(t, err)
	lock := &fakeLock{}
	runner := &fakeRunner{lock: lock, errors: runnerErrors, outputs: [][]byte{
		[]byte("clean prompt"), []byte("%7\t4242\t120\t40\n"), []byte("%7\t1730000000\n"),
		nil, []byte("prompt " + string(testNotice)), []byte("prompt " + string(testNotice)),
		nil, []byte("agent working"),
	}}
	events := []string{}
	transport, err := newTransport(Options{
		Locker: fakeLocker{lock: lock}, Lifecycle: lifecycle, Timeout: time.Second,
	}, runner)
	require.NoError(t, err)
	transport.settle = 30 * time.Millisecond
	transport.settlePoll = time.Millisecond
	return journalHarness{
		state: state, runner: runner, transport: transport,
		guard: &fakeGuard{events: &events, lock: lock},
	}
}

func (h journalHarness) deliver(ctx context.Context) (Result, error) {
	return h.transport.Deliver(ctx, Request{Target: journalTarget, Notice: testNotice}, h.guard)
}

func (h journalHarness) sendKeys() int {
	count := 0
	for _, call := range h.runner.calls {
		if len(call.args) > 0 && call.args[0] == testSendKeys {
			count++
		}
	}
	return count
}

func (h journalHarness) durable(t *testing.T) []testRecord {
	t.Helper()
	records := make([]testRecord, 0, len(h.state.synced))
	for _, raw := range h.state.synced {
		records = append(records, decodeTestRecord(t, raw))
	}
	return records
}

func journalContext(t *testing.T) context.Context {
	return WithPaneOperation(t.Context(), PaneOperation{Agent: "agent-one", Unread: 1, NewestID: 9})
}

func TestJournaledDeliverRecordsTheWholeAttempt(t *testing.T) {
	t.Parallel()

	h := newJournalHarness(t, testPhaseIdle, nil, nil)
	result, err := h.deliver(journalContext(t))
	require.NoError(t, err)
	require.Equal(t, OutcomeCommitted, result.Outcome)
	require.Equal(t, 2, h.sendKeys())
	require.True(t, h.state.closed)

	records := h.durable(t)
	// The effects observe one child per Start call, in call order; only
	// the two key effects journal theirs.
	var children []PaneChild
	for index, call := range h.runner.calls {
		if len(call.args) > 0 && call.args[0] == testSendKeys {
			children = append(children, PaneChild{PID: int64(1000 + index), Start: int64(2000 + index)})
		}
	}
	require.Len(t, children, 2, "the notice and the Enter effects each report their child")
	type row struct {
		phase string
		step  int64
	}
	got := make([]row, 0, len(records))
	for index, record := range records {
		got = append(got, row{record.Phase, record.Step})
		require.Equal(t, int64(5+index), record.Gen, "gen rises by one per durable write")
		require.Equal(t, records[0].Attempt, record.Attempt)
		require.Equal(t, "gifford", record.Writer)
		require.Equal(t, int64(3333), record.WriterPID)
		require.Equal(t, "555555", record.WriterStart)
		// Step and Finish carry the last observed child forward; only a
		// Child write replaces it.
		want := PaneChild{}
		switch index {
		case 2, 3, 4:
			want = children[0]
		case 5, 6, 7:
			want = children[1]
		}
		require.Equal(t, want.PID, record.ChildPID, "row %d child pid", index)
		require.Equal(t, strconv.FormatInt(want.Start, 10), record.ChildStart, "row %d child start", index)
	}
	// The trace of one notice: EN_CURSO step 0; step 1; its child; the
	// staged-text check as step 2; step 3; the Enter child; step 4; HECHO.
	require.Equal(t, []row{
		{testPhaseRunning, 0},
		{testPhaseRunning, 1},
		{testPhaseRunning, 1},
		{testPhaseRunning, 2},
		{testPhaseRunning, 3},
		{testPhaseRunning, 3},
		{testPhaseRunning, 4},
		{testPhaseDone, 4},
	}, got)
}

func TestJournaledDeliverWithoutOperationTypesNothing(t *testing.T) {
	t.Parallel()

	h := newJournalHarness(t, testPhaseIdle, nil, nil)
	result, err := h.deliver(t.Context())
	require.ErrorIs(t, err, ErrPaneOperationMissing)
	require.Equal(t, OutcomeNotWritten, result.Outcome)
	require.Zero(t, h.sendKeys())
	require.Empty(t, h.state.synced)
}

func TestJournaledDeliverRefusesAnAttemptLeftInFlight(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{testPhaseRunning, testPhaseUncertain} {
		h := newJournalHarness(t, phase, nil, nil)
		result, err := h.deliver(journalContext(t))
		require.ErrorIs(t, err, tmuxlock.ErrAttemptUncertain, phase)
		require.Equal(t, OutcomeNotWritten, result.Outcome, phase)
		require.Zero(t, h.sendKeys(), phase)
		require.Empty(t, h.state.synced, phase)
		require.True(t, h.state.closed, phase)
	}
}

func TestJournaledDeliverWithoutDurableBeginTypesNothing(t *testing.T) {
	t.Parallel()

	h := newJournalHarness(t, testPhaseIdle, nil, nil)
	h.state.syncErrs[0] = syscall.EIO
	result, err := h.deliver(journalContext(t))
	require.ErrorIs(t, err, tmuxlock.ErrJournalNotDurable)
	require.Equal(t, OutcomeNotWritten, result.Outcome)
	require.Zero(t, h.sendKeys())
}

func TestJournaledDeliverWithoutDurableStepTypesNothingMore(t *testing.T) {
	t.Parallel()

	// Durable writes: 0 EN_CURSO, 1 step 1, 2 child, 3 step 2, 4 step 3.
	for name, failing := range map[string]int{"step 1": 1, "step 3": 4} {
		h := newJournalHarness(t, testPhaseIdle, nil, nil)
		h.state.syncErrs[failing] = syscall.ENOSPC
		result, err := h.deliver(journalContext(t))
		require.ErrorIs(t, err, tmuxlock.ErrJournalNotDurable, name)
		require.Equal(t, OutcomeUncertain, result.Outcome, name)
		records := h.durable(t)
		last := records[len(records)-1]
		require.Equal(t, testPhaseUncertain, last.Phase, name)
		if failing == 1 {
			require.Zero(t, h.sendKeys(), "%s: no text without a durable step", name)
			require.Zero(t, last.Step, name)
		} else {
			require.Equal(t, 1, h.sendKeys(), "%s: no Enter without a durable step", name)
			require.Equal(t, int64(2), last.Step, name)
		}
	}
}

func TestJournaledDeliverFailedEnterEndsUncertain(t *testing.T) {
	t.Parallel()

	h := newJournalHarness(t, testPhaseIdle, nil, []error{6: errors.New("enter failed")})
	result, err := h.deliver(journalContext(t))
	require.Error(t, err)
	require.Equal(t, OutcomeUncertain, result.Outcome)
	records := h.durable(t)
	last := records[len(records)-1]
	require.Equal(t, testPhaseUncertain, last.Phase)
	require.Equal(t, int64(3), last.Step)
	require.Equal(t, testPhaseRunning, records[len(records)-2].Phase)
	require.Equal(t, int64(3), records[len(records)-2].Step, "no child after a failed Enter")
}

func TestJournaledDeliverChangedIdentityEndsUncertain(t *testing.T) {
	t.Parallel()

	h := newJournalHarness(t, testPhaseIdle, ErrPaneIdentityChanged, nil)
	result, err := h.deliver(journalContext(t))
	require.ErrorIs(t, err, ErrPaneIdentityChanged)
	require.Equal(t, OutcomeUncertain, result.Outcome)
	records := h.durable(t)
	last := records[len(records)-1]
	require.Equal(t, testPhaseUncertain, last.Phase)
	require.Equal(t, int64(4), last.Step)
}

func TestNewJournaledLifecycleNeedsBothHalves(t *testing.T) {
	t.Parallel()

	journal, err := NewPaneJournal("example-op/1")
	require.NoError(t, err)
	_, err = NewJournaledLifecycle(nil, journal)
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = NewJournaledLifecycle(allowChecker{}, nil)
	require.ErrorIs(t, err, ErrInvalidOptions)
	_, err = NewPaneJournal("")
	require.ErrorIs(t, err, ErrInvalidOptions)
}
