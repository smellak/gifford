package tmuxlock

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecordPolicyClosesOwnersAndWritersOnEncodeAndDecode(t *testing.T) {
	t.Parallel()
	policy := neutralRecordPolicy(t)
	timestamp := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	binding := neutralRecordBinding()

	for _, writer := range append(append([]string(nil), nodeRecordWriters...), "legacy_writer") {
		state := initialState(binding, testPeerOwner, timestamp)
		state.Writer = writer
		raw, err := encodeStateRecord(policy, state)
		require.NoError(t, err, writer)
		_, err = decodeStateRecord(policy, raw)
		require.NoError(t, err, writer)
	}

	for name, mutate := range map[string]func(*stateRecord){
		"owner":  func(state *stateRecord) { state.Owner = testOtherOwner },
		"writer": func(state *stateRecord) { state.Writer = "unexpected_writer" },
	} {
		t.Run("state "+name, func(t *testing.T) {
			t.Parallel()
			state := initialState(binding, testPeerOwner, timestamp)
			mutate(&state)
			_, err := encodeStateRecord(policy, state)
			require.ErrorIs(t, err, errInvalidRecord)
			raw, err := encodeRecord(stateMagic, state)
			require.NoError(t, err)
			_, err = decodeStateRecord(policy, raw)
			require.ErrorIs(t, err, errInvalidRecord)
		})
	}

	for name, record := range map[string]registerRecord{
		"owner": {Binding: binding, Owner: testOtherOwner},
		"pending owner": {
			Binding: binding, Owner: nodeOwner, OwnerGen: 1,
			Pending: &pendingRecord{
				FromEpoch: 0, FromGen: 0, Operation: operationTransfer,
				ToEpoch: 0, ToOwner: testOtherOwner, ToOwnerGen: 1,
				TxID: strings.Repeat("1", 32),
			},
		},
	} {
		t.Run("register "+name, func(t *testing.T) {
			t.Parallel()
			_, err := encodeRegisterRecord(policy, record)
			require.ErrorIs(t, err, errInvalidRecord)
			pending := any("-")
			if record.Pending != nil {
				pending = *record.Pending
			}
			raw, err := encodeRecord(registerMagic, registerWire{
				Binding: record.Binding, Epoch: record.Epoch, Incorporated: 1,
				Owner: record.Owner, OwnerGen: record.OwnerGen, Pending: pending, Version: 1,
			})
			require.NoError(t, err)
			_, err = decodeRegisterRecord(policy, raw)
			require.ErrorIs(t, err, errInvalidRecord)
		})
	}
}

func TestRecordPolicyRejectsOpenOrAmbiguousConfiguration(t *testing.T) {
	t.Parallel()
	for name, policy := range map[string]RecordPolicy{
		"missing owner":   {PeerWriters: []string{testPeerWriter}},
		"node owner":      {PeerOwner: nodeOwner, PeerWriters: []string{testPeerWriter}},
		"missing writers": {PeerOwner: testPeerOwner},
		"unsorted": {
			PeerOwner: testPeerOwner, PeerWriters: []string{testPeerWriter, "peer_dispatch"},
		},
		"duplicate": {
			PeerOwner: testPeerOwner, PeerWriters: []string{testPeerWriter, testPeerWriter},
		},
		"node writer": {PeerOwner: testPeerOwner, PeerWriters: []string{"provision"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, policy.Validate())
		})
	}
}

func TestIndexCodecUsesEmptyArraysAndRejectsNull(t *testing.T) {
	t.Parallel()

	enrolled, err := encodeEnrolledIndex(1, nil)
	require.NoError(t, err)
	require.Contains(t, string(enrolled), `"entries":[]`)
	_, owners, err := decodeEnrolledIndex(enrolled)
	require.NoError(t, err)
	require.NotNil(t, owners)
	require.Empty(t, owners)

	panel, err := encodePanelIndex(1, nil)
	require.NoError(t, err)
	require.Contains(t, string(panel), `"entries":[]`)
	_, entries, err := decodePanelIndex(panel)
	require.NoError(t, err)
	require.NotNil(t, entries)
	require.Empty(t, entries)

	nullEnrolled, err := encodeIndex(enrolledMagic, enrolledIndexBody{Seq: 1, Version: 1})
	require.NoError(t, err)
	_, _, err = decodeEnrolledIndex(nullEnrolled)
	require.ErrorIs(t, err, errInvalidRecord)

	nullPanel, err := encodeIndex(panelIndexMagic, panelIndexBody{Seq: 1, Version: 1})
	require.NoError(t, err)
	_, _, err = decodePanelIndex(nullPanel)
	require.ErrorIs(t, err, errInvalidRecord)
}
