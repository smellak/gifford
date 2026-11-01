package channel

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func testFederatedArtifact() Artifact {
	return Artifact{
		ID: testArtifactID, From: testAgent1, FromNode: testNodeA,
		To:     Recipient{Agent: testAgent2, Deliverable: true, Policy: PolicyAllowed},
		ToNode: testNodeB, CiphertextBytes: 32,
	}
}

func testFederatedArtifactOrigin(artifact Artifact) protocol.ObjectControlState {
	return protocol.ObjectControlState{
		ObjectType: protocol.DecisionObjectTypeArtifact, ObjectID: artifact.ID,
		ControlState: protocol.ControlStateAllowed, ControlReason: protocol.DecisionReasonAllowed,
		ControlVersion: 4,
	}
}

func testStoreFederatedArtifact(t *testing.T, tx *sql.Tx, artifact Artifact) func(context.Context, Acceptance) error {
	t.Helper()
	return func(ctx context.Context, _ Acceptance) error {
		stamp := eventstore.FormatTime(testEngineTime)
		_, err := tx.ExecContext(ctx, `INSERT INTO artifact_transfers
			(artifact_id, client_msg_id, origin_node, sender_agent, destination_node,
			 state, encryption_profile, ciphertext_size, chunk_count, ciphertext_sha256,
			 encrypted_manifest, sender_key_id, signature, expires_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'REPLICATING', 'gifford-artifact-e2e-v1', 32, 1, ?,
			 ?, ?, ?, ?, ?, ?)`, artifact.ID, "artifact-client", artifact.FromNode, artifact.From,
			artifact.ToNode, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			[]byte("encrypted-manifest"), "ed25519:agent-1:v1", make([]byte, 64),
			eventstore.FormatTime(testEngineTime.Add(time.Hour)), stamp, stamp)
		return err
	}
}

func TestAcceptFederatedArtifactStoresReceiverControl(t *testing.T) {
	t.Parallel()
	store := testChannelStore(t)
	testSetEpoch(t, store.DB(), 11)
	engine, _ := testGateEngine(t)
	artifact := testFederatedArtifact()
	tx := testBeginGateTx(t, store)
	accepted, err := engine.AcceptFederatedArtifact(t.Context(), tx, artifact,
		testFederatedArtifactOrigin(artifact), testStoreFederatedArtifact(t, tx, artifact))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Equal(t, protocol.ControlStateAllowed, accepted.State())
	require.Equal(t, protocol.DecisionReasonAllowed, accepted.Reason())
	require.Equal(t, int64(11), accepted.Version())
	want := triple{state: protocol.ControlStateAllowed, reason: protocol.DecisionReasonAllowed, version: 11}
	require.Equal(t, want, testReadArtifactTriple(t, store.DB(), artifact.ID))
	require.Equal(t, int64(1), testDecisionCount(t, store.DB(), protocol.ULID(artifact.ID)))
}

func testReadArtifactTriple(t *testing.T, db *sql.DB, id protocol.OpaqueID) triple {
	t.Helper()
	var got triple
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT control_state, control_reason,
		control_version FROM artifact_transfers WHERE artifact_id = ?`, id).
		Scan(&got.state, &got.reason, &got.version))
	return got
}

func TestAcceptFederatedArtifactNonAllowStoresOnlyDecision(t *testing.T) {
	t.Parallel()
	for index, mode := range []protocol.LinkMode{
		protocol.LinkModeDeny, protocol.LinkModeHold, protocol.LinkModeRequireApproval,
	} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			store := testChannelStore(t)
			testSeedControl(t, store.DB(), testLinkRoot(testControlID(430+index), testAgent1, testAgent2,
				protocol.PlaneArtifact, mode))
			engine, _ := testGateEngine(t)
			artifact := testFederatedArtifact()
			called := false
			tx := testBeginGateTx(t, store)
			_, err := engine.AcceptFederatedArtifact(t.Context(), tx, artifact,
				testFederatedArtifactOrigin(artifact), func(context.Context, Acceptance) error {
					called = true
					return nil
				})
			var denied *ArtifactGateError
			require.ErrorAs(t, err, &denied)
			require.False(t, called)
			require.NoError(t, tx.Commit())
			require.Equal(t, int64(1), testDecisionCount(t, store.DB(), protocol.ULID(artifact.ID)))
			require.Zero(t, testTableCount(t, store.DB(), "artifact_transfers"))
		})
	}
}

func TestAcceptFederatedArtifactRejectsInvalidOriginAndLocalRoute(t *testing.T) {
	t.Parallel()
	t.Run("origin", func(t *testing.T) {
		t.Parallel()
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		artifact := testFederatedArtifact()
		origin := testFederatedArtifactOrigin(artifact)
		origin.ObjectID = "other-artifact"
		tx := testBeginGateTx(t, store)
		_, err := engine.AcceptFederatedArtifact(t.Context(), tx, artifact, origin,
			testStoreFederatedArtifact(t, tx, artifact))
		require.ErrorIs(t, err, ErrInvalidQuery)
		require.NoError(t, tx.Rollback())
		require.Zero(t, testDecisionCount(t, store.DB(), protocol.ULID(artifact.ID)))
	})
	t.Run("local route", func(t *testing.T) {
		t.Parallel()
		store := testChannelStore(t)
		engine, _ := testGateEngine(t)
		artifact := testFederatedArtifact()
		artifact.ToNode = artifact.FromNode
		tx := testBeginGateTx(t, store)
		_, err := engine.AcceptFederatedArtifact(t.Context(), tx, artifact,
			testFederatedArtifactOrigin(artifact), testStoreFederatedArtifact(t, tx, artifact))
		require.ErrorIs(t, err, ErrUnsupported)
		require.NoError(t, tx.Rollback())
	})
}
