package tmuxattach

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stickyEnterRunner models the measured Muse composer that sometimes keeps
// the notice after a completed Enter: the first lost Enters leave the staged
// screen, the next one submits.
type stickyEnterRunner struct {
	lock   *fakeLock
	lost   int
	staged bool
	enters int
}

func (r *stickyEnterRunner) screen() []byte {
	switch {
	case !r.staged:
		return []byte("────────\n❯ \n────────")
	case r.enters <= r.lost:
		return slices.Clone(testMuseStagedScreen)
	default:
		return slices.Clone(testMuseSubmittedScreen)
	}
}

func (r *stickyEnterRunner) Run(_ context.Context, _ Server, args ...string) ([]byte, error) {
	if !r.lock.isHeld() {
		return nil, errors.New("command ran without lock")
	}
	switch args[0] {
	case commandCapturePane:
		return r.screen(), nil
	case testDisplayMessage:
		return []byte("%7\t4242\t120\t40\n"), nil
	case testListClients:
		return []byte("%7\t1730000000\n"), nil
	}
	return nil, nil
}

func (r *stickyEnterRunner) Start(ctx context.Context, _ Server, args ...string) (runningEffect, error) {
	if !r.lock.isHeld() {
		return nil, errors.New("command ran without lock")
	}
	return &fakeRunningEffect{ctx: ctx, child: PaneChild{PID: 1001, Start: 2001}, waitFn: func() error {
		if args[len(args)-1] == testKeyEnter {
			r.enters++
		} else {
			r.staged = true
		}
		return nil
	}}, nil
}

type resubmitGuard struct {
	*fakeGuard
}

func (resubmitGuard) ResubmitOnce() bool { return true }

func museGuard(events *[]string, lock *fakeLock) *fakeGuard {
	return &fakeGuard{
		events: events, lock: lock,
		verifyFn: func(screen []byte) error {
			if !bytes.Equal(screen, testMuseStagedScreen) {
				return errors.New("notice not the sole input")
			}
			return nil
		},
		submittedFn: func(screen []byte) error {
			if !bytes.Equal(screen, testMuseSubmittedScreen) {
				return errors.New("notice remained in composer")
			}
			return nil
		},
	}
}

func stickyTransport(t *testing.T, runner *stickyEnterRunner, lock *fakeLock) *Transport {
	t.Helper()
	transport, err := newTransport(Options{Locker: fakeLocker{lock: lock}, Timeout: time.Second}, runner)
	require.NoError(t, err)
	transport.settle = 5 * time.Millisecond
	transport.settlePoll = time.Microsecond
	return transport
}

func TestDeliverResubmitsOnceWhenTheComposerKeepsTheNotice(t *testing.T) {
	t.Parallel()
	for name, test := range map[string]struct {
		lost       int
		resubmit   bool
		lateChange bool
		outcome    Outcome
		enters     int
	}{
		"one lost Enter is pressed again":      {lost: 1, resubmit: true, outcome: OutcomeCommitted, enters: 2},
		"a second Enter is the last":           {lost: 2, resubmit: true, outcome: OutcomeUncertain, enters: 2},
		"other harnesses never press it again": {lost: 1, outcome: OutcomeUncertain, enters: 1},
		"a changed box gets no second Enter":   {lost: 1, resubmit: true, lateChange: true, outcome: OutcomeUncertain, enters: 1},
		"a submitted notice needs no resubmit": {resubmit: true, outcome: OutcomeCommitted, enters: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			lock := &fakeLock{}
			runner := &stickyEnterRunner{lock: lock, lost: test.lost}
			events := []string{}
			base := museGuard(&events, lock)
			if test.lateChange {
				verify := base.verifyFn
				base.verifyFn = func(screen []byte) error {
					if runner.enters > 0 {
						return errors.New("box changed after Enter")
					}
					return verify(screen)
				}
			}
			var guard Guard = base
			if test.resubmit {
				guard = resubmitGuard{base}
			}
			result, err := stickyTransport(t, runner, lock).Deliver(
				t.Context(), Request{Target: testTarget, Notice: testNotice}, guard,
			)
			require.Equal(t, test.outcome, result.Outcome)
			require.Equal(t, test.outcome == OutcomeCommitted, err == nil, "%v", err)
			require.Equal(t, test.enters, runner.enters)
			require.Equal(t, 1, lock.unlocked)
		})
	}
}
