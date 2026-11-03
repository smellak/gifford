//go:build linux

package tmuxlock

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnedPanelGuardCoversEveryAuthorityMismatch(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutate  func(*registerRecord, *stateRecord)
		reason  GuardReason
		invalid bool
	}{
		"owner copy": {
			mutate: func(_ *registerRecord, state *stateRecord) { state.Owner = testPeerWriter },
			reason: GuardOwnerCopyMismatch, invalid: true,
		},
		"owner generation copy": {
			mutate: func(_ *registerRecord, state *stateRecord) { state.OwnerGen++ },
			reason: GuardOwnerCopyMismatch, invalid: true,
		},
		"epoch copy": {
			mutate: func(_ *registerRecord, state *stateRecord) { state.Epoch++ },
			reason: GuardOwnerCopyMismatch, invalid: true,
		},
		"state binding copy": {
			mutate: func(_ *registerRecord, state *stateRecord) { state.PanePID++ },
			reason: GuardStateBindingMismatch, invalid: true,
		},
		"registered uid": {
			mutate: func(register *registerRecord, state *stateRecord) {
				register.Binding.UID++
				state.UID++
			},
			reason: GuardBindingMismatch, invalid: true,
		},
		"registered socket": {
			mutate: func(register *registerRecord, state *stateRecord) {
				register.Binding.SocketSHA = strings.Repeat("b", 64)
				state.SocketSHA = register.Binding.SocketSHA
			},
			reason: GuardBindingMismatch, invalid: true,
		},
		"registered pane": {
			mutate: func(register *registerRecord, state *stateRecord) {
				register.Binding.Pane++
				state.Pane++
			},
			reason: GuardBindingMismatch, invalid: true,
		},
		"configured host": {
			mutate: func(register *registerRecord, state *stateRecord) {
				register.Binding.Host = "other-host"
				state.Host = register.Binding.Host
			},
			reason: GuardBindingMismatch, invalid: true,
		},
		"foreign owner": {
			mutate: func(register *registerRecord, state *stateRecord) {
				register.Owner = testPeerWriter
				state.Owner = testPeerWriter
			},
			reason: GuardForeignOwner,
		},
		"running phase": {
			mutate: func(_ *registerRecord, state *stateRecord) { state.Phase = phaseRunning },
			reason: GuardPhaseBlocked,
		},
		"uncertain phase": {
			mutate: func(_ *registerRecord, state *stateRecord) { state.Phase = phaseUncertain },
			reason: GuardPhaseBlocked,
		},
		"generation headroom": {
			mutate: func(_ *registerRecord, state *stateRecord) {
				state.Gen = maxSafeInteger - generationHeadroom + 1
			},
			reason: GuardGenerationExhausted, invalid: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newTransferFixture(t)
			_, err := fixture.writer(t, writerHooks{}).Take(t.Context(), fixture.live)
			require.NoError(t, err)
			register, state := fixture.records(t)
			test.mutate(&register, &state)
			fixture.writeRecords(t, register, state)
			assertOwnedGuardFailure(t, fixture, test.reason, test.invalid)
		})
	}
}

func TestOwnedPanelGuardRevalidatesRequiredModeBeforeEffect(t *testing.T) {
	t.Parallel()

	fixture := newTransferFixture(t)
	_, err := fixture.writer(t, writerHooks{}).Take(t.Context(), fixture.live)
	require.NoError(t, err)
	guard, outcome, err := acquireOwnedWithModeAt(
		t.Context(), fixture.modePath, fixture.root, effectiveUID(), effectiveGID(),
		RecordPolicy{PeerOwner: testPeerWriter, PeerWriters: []string{testPeerWriter}},
		fixture.live, 0,
	)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	require.NotNil(t, guard)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })

	require.NoError(t, os.WriteFile(fixture.modePath, []byte("optional\n"), 0o644))
	err = guard.Validate(t.Context(), fixture.live)
	reason, invalid, ok := GuardFailure(err)
	require.True(t, ok)
	require.True(t, invalid)
	require.Equal(t, GuardModeInvalid, reason)
}

func TestOwnedPanelGuardRejectsCorruptRecordsAndDefersCoherentPending(t *testing.T) {
	t.Parallel()

	for name, path := range map[string]func(transferFixture) string{
		"register":       func(fixture transferFixture) string { return fixture.register },
		repairWriteState: func(fixture transferFixture) string { return fixture.state },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newTransferFixture(t)
			_, err := fixture.writer(t, writerHooks{}).Take(t.Context(), fixture.live)
			require.NoError(t, err)
			raw := readTransferPath(t, path(fixture))
			raw[100] ^= 1
			require.NoError(t, os.WriteFile(path(fixture), raw, 0o600))
			reason := GuardRegisterInvalid
			if name == "state" {
				reason = GuardStateInvalid
			}
			assertOwnedGuardFailure(t, fixture, reason, true)
		})
	}

	t.Run("pending", func(t *testing.T) {
		t.Parallel()
		fixture := newTransferFixture(t)
		stop := errors.New("stop after register")
		writer := fixture.writer(t, writerHooks{
			boundary: func(boundary writerBoundary) error {
				if boundary == boundaryRegister {
					return stop
				}
				return nil
			},
		})
		_, err := writer.Take(t.Context(), fixture.live)
		require.ErrorIs(t, err, stop)
		assertOwnedGuardFailure(t, fixture, GuardPending, false)
	})
}

func TestOwnedPanelGuardDistinguishesUnincorporatedFromInvalidInventory(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		mutate  func(*testing.T, transferFixture)
		outcome LockOutcome
		reason  GuardReason
		invalid bool
	}{
		"owner absent": {
			mutate: func(t *testing.T, fixture transferFixture) {
				raw, err := encodeEnrolledIndex(2, []uint32{fixture.live.OwnerUID + 1})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(fixture.enrolled, raw, 0o644))
			},
			outcome: LockDeferred, reason: GuardNotIncorporated,
		},
		"pane absent": {
			mutate: func(t *testing.T, fixture transferFixture) {
				raw, err := encodePanelIndex(2, []panelIndexEntry{})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(fixture.index, raw, 0o640))
			},
			outcome: LockDeferred, reason: GuardNotIncorporated,
		},
		"inventory corrupt": {
			mutate: func(t *testing.T, fixture transferFixture) {
				raw := readTransferPath(t, fixture.enrolled)
				raw[10] ^= 1
				require.NoError(t, os.WriteFile(fixture.enrolled, raw, 0o644))
			},
			outcome: LockInvalid, reason: GuardInventoryInvalid, invalid: true,
		},
		"panel attributes": {
			mutate: func(t *testing.T, fixture transferFixture) {
				require.NoError(t, os.Chmod(fixture.lock, 0o600))
			},
			outcome: LockInvalid, reason: GuardPanelInvalid, invalid: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture := newTransferFixture(t)
			test.mutate(t, fixture)
			guard, outcome, err := acquireOwnedAt(
				t.Context(), fixture.root, transferRecordPolicy(t), fixture.live, 0,
			)
			require.Nil(t, guard)
			require.Equal(t, test.outcome, outcome)
			reason, invalid, ok := GuardFailure(err)
			require.True(t, ok)
			require.Equal(t, test.reason, reason)
			require.Equal(t, test.invalid, invalid)
		})
	}
}

func assertOwnedGuardFailure(
	t *testing.T,
	fixture transferFixture,
	wantReason GuardReason,
	wantInvalid bool,
) {
	t.Helper()
	guard, outcome, err := acquireOwnedAt(
		t.Context(), fixture.root, transferRecordPolicy(t), fixture.live, 0,
	)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	require.NotNil(t, guard)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })

	err = guard.Validate(t.Context(), fixture.live)
	reason, invalid, ok := GuardFailure(err)
	require.True(t, ok)
	require.Equal(t, wantReason, reason)
	require.Equal(t, wantInvalid, invalid)
}
