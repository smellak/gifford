package tmuxlock

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	neutralHost      = "node-a"
	neutralSocketSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testPeerOwner    = "legacy"
	testPeerWriter   = "peer_writer"
	testOtherOwner   = "other_owner"
	testPaneTwelve   = "%12"
)

func neutralRecordBinding() recordBinding {
	return recordBinding{
		BootID: "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0", Host: neutralHost,
		Pane: 12, PanePID: 424242, PaneStart: "987654",
		SocketSHA: neutralSocketSHA, UID: 1113,
	}
}

func neutralRecordPolicy(t *testing.T) validatedRecordPolicy {
	t.Helper()
	policy, err := validateRecordPolicy(RecordPolicy{
		PeerOwner: testPeerOwner, PeerWriters: []string{"legacy_writer"},
	})
	require.NoError(t, err)
	return policy
}

func TestRecordCompatibilityVectors(t *testing.T) {
	t.Parallel()
	binding := neutralRecordBinding()
	policy := neutralRecordPolicy(t)
	timestamp := time.Date(2026, 11, 2, 21, 20, 0, 0, time.UTC)
	state := initialState(binding, testPeerOwner, timestamp)
	rawState, err := encodeStateRecord(policy, state)
	require.NoError(t, err)
	require.Len(t, rawState, recordSize)
	require.Equal(t, "9f943f26388108c4cddde3c904a8e838d64eec788b1af3e9d529d6d1cc34b003",
		hexDigest(rawState))
	decodedState, err := decodeStateRecord(policy, rawState)
	require.NoError(t, err)
	require.Equal(t, state, decodedState)

	register := registerRecord{Binding: binding, Owner: testPeerOwner}
	rawRegister, err := encodeRegisterRecord(policy, register)
	require.NoError(t, err)
	require.Len(t, rawRegister, recordSize)
	require.Equal(t, "0868189eb09cd47dcc8ffd02a85aedfef6fb05aa82e8c061a5ead8ff69fdbd09",
		hexDigest(rawRegister))
	decodedRegister, err := decodeRegisterRecord(policy, rawRegister)
	require.NoError(t, err)
	require.Equal(t, register, decodedRegister)

	pending := pendingRecord{
		FromEpoch: 0, FromGen: 3, Operation: operationTransfer, ToEpoch: 0,
		ToOwner: nodeOwner, ToOwnerGen: 1, TxID: "33333333333333333333333333333333",
	}
	register = registerRecord{
		Binding: binding, Owner: nodeOwner, OwnerGen: 1, Pending: &pending,
	}
	rawRegister, err = encodeRegisterRecord(policy, register)
	require.NoError(t, err)
	require.Equal(t, "b950f2dd2e761160e1d08cfe47f768f19a293d6f71073d4bd319334d5cc27c66",
		hexDigest(rawRegister))
	decodedRegister, err = decodeRegisterRecord(policy, rawRegister)
	require.NoError(t, err)
	require.Equal(t, register, decodedRegister)
}

func TestIndexCompatibilityVectors(t *testing.T) {
	t.Parallel()
	enrolled, err := encodeEnrolledIndex(2, []uint32{1001, 1113})
	require.NoError(t, err)
	require.Equal(t, "PENROL01\n{\"entries\":[1001,1113],\"seq\":2,\"v\":1}\n"+
		"ff4f461f71aaf3873a0c66ece986aec85a51975ff28c68f4e69ff94cba828a85\n",
		string(enrolled))
	require.Equal(t, "949ac13a0d798c1a5f08c664dc08917035bdfea2ce46cc44d8e7de1a64f9ac27",
		hexDigest(enrolled))
	seq, owners, err := decodeEnrolledIndex(enrolled)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
	require.Equal(t, []uint32{1001, 1113}, owners)

	index, err := encodePanelIndex(1, []panelIndexEntry{{Pane: 12, SocketSHA: neutralSocketSHA}})
	require.NoError(t, err)
	require.Equal(t, "PINDEX01\n{\"entries\":[{\"pane\":12,\"socket_sha\":\""+
		neutralSocketSHA+"\"}],\"seq\":1,\"v\":1}\n"+
		"6850b278f00fd234a971d6a65ebc0bed54a72c50d49b907fa4c4b338df1983e9\n",
		string(index))
	require.Equal(t, "eacc430ceadf65b50128b7a8d4aa3484098cb80f9b7046379bc6b5b3de4a6342",
		hexDigest(index))
	seq, entries, err := decodePanelIndex(index)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)
	require.Equal(t, []panelIndexEntry{{Pane: 12, SocketSHA: neutralSocketSHA}}, entries)
}

func TestRecordDecodersRejectNoncanonicalOrCorruptBytes(t *testing.T) {
	t.Parallel()
	state := initialState(neutralRecordBinding(), testPeerOwner,
		time.Date(2026, 11, 2, 21, 20, 0, 0, time.UTC))
	policy := neutralRecordPolicy(t)
	raw, err := encodeStateRecord(policy, state)
	require.NoError(t, err)

	for name, mutate := range map[string]func([]byte) []byte{
		"short": func(raw []byte) []byte { return raw[:len(raw)-1] },
		"checksum": func(raw []byte) []byte {
			clone := append([]byte(nil), raw...)
			clone[100] ^= 1
			return clone
		},
		"trailer": func(raw []byte) []byte {
			clone := append([]byte(nil), raw...)
			clone[len(clone)-1] = 0
			return clone
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeStateRecord(policy, mutate(raw))
			require.ErrorIs(t, err, errInvalidRecord)
		})
	}

	badIndex := []byte("PENROL01\n{\"v\":1,\"seq\":1,\"entries\":[]}\n" +
		"0000000000000000000000000000000000000000000000000000000000000000\n")
	_, _, err = decodeEnrolledIndex(badIndex)
	require.ErrorIs(t, err, errInvalidRecord)
}

func hexDigest(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
