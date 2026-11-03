//go:build linux

package tmuxlock

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const transferTake = "take"

func transferRecordPolicy(t *testing.T) validatedRecordPolicy {
	t.Helper()
	policy, err := validateRecordPolicy(RecordPolicy{
		PeerOwner: testPeerWriter, PeerWriters: []string{testPeerWriter},
	})
	require.NoError(t, err)
	return policy
}

type transferFixture struct {
	root      string
	modePath  string
	live      LiveBinding
	parts     lockPathParts
	lock      string
	state     string
	register  string
	enrolled  string
	index     string
	timestamp time.Time
}

func TestNodeWriterTransferRecoversEveryDurableBoundary(t *testing.T) {
	t.Parallel()
	stop := errors.New("stop after durable boundary")
	for _, direction := range []string{transferTake, "return"} {
		for _, boundary := range []writerBoundary{boundaryRegister, boundaryState, boundaryClear} {
			t.Run(direction+"/"+string(boundary), func(t *testing.T) {
				t.Parallel()
				fixture := newTransferFixture(t)
				stable := fixture.writer(t, writerHooks{})
				if direction == "return" {
					_, err := stable.Take(t.Context(), fixture.live)
					require.NoError(t, err)
				}
				wantInventory := [][]byte{
					readTransferPath(t, fixture.enrolled), readTransferPath(t, fixture.index),
				}
				crashing := fixture.writer(t, writerHooks{
					boundary: func(got writerBoundary) error {
						if got == boundary {
							return stop
						}
						return nil
					},
				})
				var err error
				if direction == transferTake {
					_, err = crashing.Take(t.Context(), fixture.live)
				} else {
					_, err = crashing.Return(t.Context(), fixture.live)
				}
				require.ErrorIs(t, err, stop)

				var result OwnershipResult
				if direction == transferTake {
					result, err = stable.Take(t.Context(), fixture.live)
				} else {
					result, err = stable.Return(t.Context(), fixture.live)
				}
				require.NoError(t, err)
				if direction == transferTake {
					require.Equal(t, nodeOwner, result.Owner)
				} else {
					require.Equal(t, testPeerWriter, result.Owner)
				}
				register, state := fixture.records(t)
				require.Nil(t, register.Pending)
				require.True(t, recordsCoherent(register, state))
				gotInventory := [][]byte{
					readTransferPath(t, fixture.enrolled), readTransferPath(t, fixture.index),
				}
				require.Equal(t, wantInventory, gotInventory)
			})
		}
	}
}

func TestNodeWriterTransferIsIdempotentAndRejectsBlockedOrigin(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	writer := fixture.writer(t, writerHooks{})

	result, err := writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, nodeOwner, result.Owner)
	registerBefore := readTransferPath(t, fixture.register)
	stateBefore := readTransferPath(t, fixture.state)
	result, err = writer.Take(t.Context(), fixture.live)
	require.NoError(t, err)
	require.False(t, result.Changed)
	require.Equal(t, registerBefore, readTransferPath(t, fixture.register))
	require.Equal(t, stateBefore, readTransferPath(t, fixture.state))

	_, err = writer.Return(t.Context(), fixture.live)
	require.NoError(t, err)
	register, state := fixture.records(t)
	state.Phase = phaseRunning
	state.Gen++
	state.Attempt = testAttemptID
	state.Operation = testOperationID
	state.Writer = testPeerWriter
	state.WriterPID = 123
	state.WriterStart = "456"
	fixture.writeRecords(t, register, state)
	wantRecords := [][]byte{readTransferPath(t, fixture.register), readTransferPath(t, fixture.state)}
	_, err = writer.Take(t.Context(), fixture.live)
	require.ErrorIs(t, err, ErrWriterInvalid)
	gotRecords := [][]byte{readTransferPath(t, fixture.register), readTransferPath(t, fixture.state)}
	require.Equal(t, wantRecords, gotRecords)
}

func TestNodeWriterDoesNotClearPendingAfterNoncanonicalStateCopy(t *testing.T) {
	t.Parallel()

	fixture := newTransferFixture(t)
	stop := errors.New("stop after state copy")
	crashing := fixture.writer(t, writerHooks{
		boundary: func(boundary writerBoundary) error {
			if boundary == boundaryState {
				return stop
			}
			return nil
		},
	})
	_, err := crashing.Take(t.Context(), fixture.live)
	require.ErrorIs(t, err, stop)

	register, state := fixture.records(t)
	require.NotNil(t, register.Pending)
	state.Writer = "provision"
	fixture.writeRecords(t, register, state)
	wantRecords := [][]byte{readTransferPath(t, fixture.register), readTransferPath(t, fixture.state)}

	_, err = fixture.writer(t, writerHooks{}).Take(t.Context(), fixture.live)
	require.ErrorIs(t, err, ErrWriterInvalid)
	gotRecords := [][]byte{readTransferPath(t, fixture.register), readTransferPath(t, fixture.state)}
	require.Equal(t, wantRecords, gotRecords)
}

func TestOwnedPanelGuardClassifiesAuthorityWithoutEffect(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)

	guard, outcome, err := acquireOwnedAt(
		t.Context(), fixture.root, transferRecordPolicy(t), fixture.live, 0,
	)
	require.NoError(t, err)
	require.NotNil(t, guard)
	require.Equal(t, LockAcquired, outcome)
	err = guard.Validate(t.Context(), fixture.live)
	reason, invalid, ok := GuardFailure(err)
	require.True(t, ok)
	require.Equal(t, GuardForeignOwner, reason)
	require.False(t, invalid)
	require.NoError(t, guard.Close())

	_, err = fixture.writer(t, writerHooks{}).Take(t.Context(), fixture.live)
	require.NoError(t, err)
	guard, outcome, err = acquireOwnedAt(
		t.Context(), fixture.root, transferRecordPolicy(t), fixture.live, 0,
	)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	require.NoError(t, guard.Validate(t.Context(), fixture.live))
	require.NoError(t, guard.Close())

	stale := fixture.live
	stale.PaneStart++
	guard, outcome, err = acquireOwnedAt(
		t.Context(), fixture.root, transferRecordPolicy(t), stale, 0,
	)
	require.NoError(t, err)
	require.NotNil(t, guard)
	require.Equal(t, LockAcquired, outcome)
	err = guard.Validate(t.Context(), stale)
	reason, invalid, ok = GuardFailure(err)
	require.True(t, ok)
	require.True(t, invalid)
	require.Equal(t, GuardBindingMismatch, reason)
	require.NoError(t, guard.Close())

	raw := readTransferPath(t, fixture.register)
	raw[100] ^= 1
	require.NoError(t, os.WriteFile(fixture.register, raw, 0o640))
	guard, outcome, err = acquireOwnedAt(
		t.Context(), fixture.root, transferRecordPolicy(t), fixture.live, 0,
	)
	require.NoError(t, err)
	require.NotNil(t, guard)
	require.Equal(t, LockAcquired, outcome)
	err = guard.Validate(t.Context(), fixture.live)
	reason, invalid, ok = GuardFailure(err)
	require.True(t, ok)
	require.True(t, invalid)
	require.Equal(t, GuardRegisterInvalid, reason)
	require.NoError(t, guard.Close())
}

func TestProtocolModeRequiresExactRootOwnedShape(t *testing.T) {
	t.Parallel()
	fixture := newTransferFixture(t)
	require.NoError(t, requireProtocolModePlatform(
		fixture.modePath, fixture.root, effectiveUID(), effectiveGID(),
	))
	for name, mutate := range map[string]func(*testing.T, string){
		"content": func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte("optional\n"), 0o644))
		},
		"mode": func(t *testing.T, path string) {
			require.NoError(t, os.Chmod(path, 0o640))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			modeCopy := filepath.Join(t.TempDir(), "pane-mode")
			require.NoError(t, os.WriteFile(modeCopy, []byte("required\n"), 0o644))
			mutate(t, modeCopy)
			require.Error(t, requireProtocolModePlatform(
				modeCopy, fixture.root, effectiveUID(), effectiveGID(),
			))
		})
	}
}

func newTransferFixture(t *testing.T) transferFixture {
	t.Helper()
	live := LiveBinding{
		PanelBinding: PanelBinding{
			OwnerUID: 60001, Socket: "/tmp/tmux-60001/transfer-test", PaneID: testPaneTwelve,
		},
		Host: "host-7", BootID: "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
		PanePID: 424242, PaneStart: 987654,
	}
	parts, err := deriveParts(live.PanelBinding)
	require.NoError(t, err)
	binding, err := recordBindingForLive(live)
	require.NoError(t, err)
	root := t.TempDir()
	setManagedRoot(t, root)
	enrolled := filepath.Join(root, "enrolled-uids")
	rawEnrolled, err := encodeEnrolledIndex(1, []uint32{live.OwnerUID})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(enrolled, rawEnrolled, 0o644))
	require.NoError(t, os.Chmod(enrolled, 0o644))
	ownerDir := filepath.Join(root, parts.owner)
	require.NoError(t, os.Mkdir(ownerDir, 0o700))
	setDirectoryACL(t, ownerDir, live.OwnerUID)
	index := filepath.Join(ownerDir, "index")
	rawIndex, err := encodePanelIndex(1, []panelIndexEntry{{Pane: binding.Pane, SocketSHA: binding.SocketSHA}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(index, rawIndex, 0o600))
	setTransferReadACL(t, index, live.OwnerUID)
	socketDir := filepath.Join(ownerDir, parts.hash)
	require.NoError(t, os.Mkdir(socketDir, 0o700))
	setDirectoryACL(t, socketDir, live.OwnerUID)
	pane := parts.file[:len(parts.file)-len(".lock")]
	lock := createTransferFile(t, filepath.Join(socketDir, pane+".lock"), 0o600, nil)
	setFileACL(t, lock, live.OwnerUID)
	timestamp := time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC)
	policy := transferRecordPolicy(t)
	stateRaw, err := encodeStateRecord(policy, initialState(binding, testPeerWriter, timestamp))
	require.NoError(t, err)
	state := createTransferFile(t, filepath.Join(socketDir, pane+".state"), 0o600, stateRaw)
	setFileACL(t, state, live.OwnerUID)
	registerRaw, err := encodeRegisterRecord(policy, registerRecord{Binding: binding, Owner: testPeerWriter})
	require.NoError(t, err)
	register := createTransferFile(t, filepath.Join(socketDir, pane+".reg"), 0o600, registerRaw)
	setTransferReadACL(t, register, live.OwnerUID)
	modePath := filepath.Join(t.TempDir(), "pane-mode")
	require.NoError(t, os.WriteFile(modePath, []byte("required\n"), 0o644))
	require.NoError(t, os.Chmod(modePath, 0o644))
	return transferFixture{
		root: root, modePath: modePath, live: live, parts: parts,
		lock: lock, state: state, register: register, enrolled: enrolled,
		index: index, timestamp: timestamp,
	}
}

func (f transferFixture) writer(t *testing.T, hooks writerHooks) *NodeWriter {
	t.Helper()
	if hooks.now == nil {
		hooks.now = func() time.Time { return f.timestamp }
	}
	if hooks.random == nil {
		hooks.random = bytes.NewReader(bytes.Repeat([]byte{0x11}, 512))
	}
	hooks.testMode = true
	hooks.modeUID = effectiveUID()
	hooks.modeGID = effectiveGID()
	writer, err := newNodeWriter(NodeWriterOptions{
		Root: f.root, Host: f.live.Host, PeerOwner: testPeerWriter,
		PeerWriters: []string{testPeerWriter}, ModePath: f.modePath,
	}, hooks)
	require.NoError(t, err)
	return writer
}

func (f transferFixture) records(t *testing.T) (registerRecord, stateRecord) {
	t.Helper()
	policy := transferRecordPolicy(t)
	register, err := decodeRegisterRecord(policy, readTransferPath(t, f.register))
	require.NoError(t, err)
	state, err := decodeStateRecord(policy, readTransferPath(t, f.state))
	require.NoError(t, err)
	return register, state
}

func (f transferFixture) writeRecords(t *testing.T, register registerRecord, state stateRecord) {
	t.Helper()
	policy := transferRecordPolicy(t)
	rawRegister, err := encodeRegisterRecord(policy, register)
	require.NoError(t, err)
	rawState, err := encodeStateRecord(policy, state)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.register, rawRegister, 0o640))
	require.NoError(t, os.WriteFile(f.state, rawState, 0o660))
}

func createTransferFile(t *testing.T, path string, mode os.FileMode, raw []byte) string {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, mode)
	require.NoError(t, err)
	if len(raw) > 0 {
		_, err = file.Write(raw)
		require.NoError(t, err)
	}
	require.NoError(t, file.Close())
	return path
}

func setTransferReadACL(t *testing.T, path string, ownerUID uint32) {
	t.Helper()
	setTestACL(t, path, encodeTestACL([]testACLEntry{
		{aclUserObject, aclRead | aclWrite, aclUndefinedID},
		{aclNamedUser, aclRead, ownerUID},
		{aclGroupObject, 0, aclUndefinedID},
		{aclMask, aclRead, aclUndefinedID},
		{aclOther, 0, aclUndefinedID},
	}))
}

func readTransferPath(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return raw
}
