//go:build linux

package tmuxlock

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/eventstore"
)

type allowPaneProvision struct{}

func (allowPaneProvision) AuthorizePane(PanePeer) error { return nil }

type paneWriterE2EClock struct{ now time.Time }

func (c paneWriterE2EClock) Now() time.Time { return c.now }

const paneWriterE2EPeer = "peer_writer"

func paneWriterE2ESpec(
	t *testing.T,
	root string,
	policy RecordPolicy,
	publicationTime time.Time,
) ProvisionSpec {
	t.Helper()
	require.NoError(t, os.Chmod(root, 0o711))
	ownerUID := uint32(60_001)
	if ownerUID == effectiveUID() {
		ownerUID++
	}
	return ProvisionSpec{
		Root: root,
		Binding: PanelBinding{
			OwnerUID: ownerUID, Socket: "/tmp/tmux-60001/writer-e2e", PaneID: "%12",
		},
		PanePID: 424_242, PaneStart: 987_654,
		BootID: "76543210-fedc-4ba9-8765-0123456789ab", Host: "node-1",
		BindUID: ownerUID, Owner: paneWriterE2EPeer, Policy: policy,
		Clock: paneWriterE2EClock{now: publicationTime},
		Peer: PanePeer{
			PID: 7000, UID: ownerUID, GID: ownerUID, Verified: true,
			Actor: "uid:60001:pid:7000",
		},
		Authorizer: allowPaneProvision{},
	}
}

// TestPaneWriterEndToEndFromProvisionThroughRepair keeps every durable input
// as the output of the preceding production step; no authority record is seeded.
func TestPaneWriterEndToEndFromProvisionThroughRepair(t *testing.T) {
	root := t.TempDir()
	publicationTime := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	policy := RecordPolicy{
		PeerOwner: paneWriterE2EPeer, PeerWriters: []string{paneWriterE2EPeer},
	}
	spec := paneWriterE2ESpec(t, root, policy, publicationTime)

	provisioned, err := Provision(spec)
	require.NoError(t, err)
	require.True(t, provisioned.Created)
	require.Equal(t, uint64(1), provisioned.Publication.OwnerSeq)
	require.Equal(t, uint64(1), provisioned.Publication.EnrolledSeq)
	repeatedProvision, err := Provision(spec)
	require.NoError(t, err)
	require.False(t, repeatedProvision.Created, "O_EXCL replay must validate rather than replace")
	require.Equal(t, provisioned.Publication.PanelKey, repeatedProvision.Publication.PanelKey)
	require.Equal(t, provisioned.Publication.OwnerSeq, repeatedProvision.Publication.OwnerSeq)
	require.Equal(t, provisioned.Publication.EnrolledSeq, repeatedProvision.Publication.EnrolledSeq)

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "node.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, (daemon.IndexPublicationStore{}).Record(
		t.Context(), tx, provisioned.Publication.PanelKey, provisioned.Publication.PublishedAt,
	))
	require.NoError(t, tx.Commit())

	var publishedText string
	require.NoError(t, store.DB().QueryRowContext(t.Context(), `SELECT published_at
		FROM pane_index_publications WHERE panel_key = ?`,
		provisioned.Publication.PanelKey).Scan(&publishedText))
	publishedAt, err := eventstore.ParseTime(publishedText)
	require.NoError(t, err)
	const initialDrain = 30 * time.Second
	now := publishedAt.Add(initialDrain)
	require.False(t, now.Before(publishedAt.Add(initialDrain)))

	modePath := filepath.Join(t.TempDir(), "pane-lock-mode")
	require.NoError(t, os.WriteFile(modePath, []byte("required\n"), 0o644))
	lostPendingAck := errors.New("synthetic lost pending acknowledgement")
	failPendingAck := true
	writer, err := newNodeWriter(NodeWriterOptions{
		Root: root, Host: spec.Host, PeerOwner: spec.Owner,
		PeerWriters: policy.PeerWriters, ModePath: modePath,
	}, writerHooks{
		now:    func() time.Time { return now },
		random: bytes.NewReader(make([]byte, 48)),
		boundary: func(boundary writerBoundary) error {
			if failPendingAck && boundary == boundaryRegister {
				failPendingAck = false
				return lostPendingAck
			}
			return nil
		},
		modeUID: effectiveUID(), modeGID: effectiveGID(), testMode: true,
	})
	require.NoError(t, err)
	live := LiveBinding{
		PanelBinding: spec.Binding, Host: spec.Host, BootID: spec.BootID,
		PanePID: spec.PanePID, PaneStart: spec.PaneStart,
	}

	_, err = writer.Take(t.Context(), live)
	require.ErrorIs(t, err, lostPendingAck)
	pendingRegister := readE2ERegister(t, provisioned.Register, writer.policy)
	require.NotNil(t, pendingRegister.Pending)
	pendingState := readE2EState(t, provisioned.State, writer.policy)
	require.Equal(t, spec.Owner, pendingState.Owner)

	taken, err := writer.Take(t.Context(), live)
	require.NoError(t, err)
	require.True(t, taken.Changed)
	require.Equal(t, nodeOwner, taken.Owner)
	require.Nil(t, readE2ERegister(t, provisioned.Register, writer.policy).Pending)
	require.Equal(t, nodeOwner, readE2EState(t, provisioned.State, writer.policy).Owner)

	now = now.Add(time.Second)
	guard, outcome, err := acquireOwnedAt(t.Context(), root, writer.policy, live, 0)
	require.NoError(t, err)
	require.Equal(t, LockAcquired, outcome)
	require.NotNil(t, guard)
	require.NoError(t, guard.Validate(t.Context(), live))
	contender, contenderOutcome, err := acquireOwnedAt(t.Context(), root, writer.policy, live, 0)
	require.NoError(t, err)
	require.Nil(t, contender)
	require.Equal(t, LockDeferred, contenderOutcome)

	stateFile, err := openStateAt(root, live.PanelBinding)
	require.NoError(t, err)
	journal, err := BeginAttempt(
		stateFile, live,
		Operation{Schema: "example-pane-op/1", Agent: "agent-one", Inbox: Inbox{Unread: 2, NewestID: 19}},
		Writer{PID: 7001, Start: 8001}, policy, func() time.Time { return now },
	)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	require.NoError(t, journal.Child(7101, 8101))
	require.NoError(t, journal.Step(2))
	require.NoError(t, journal.Step(3))
	require.NoError(t, journal.Child(7102, 8102))
	require.NoError(t, journal.Step(4))
	require.NoError(t, journal.Finish(false), "a lost effect acknowledgement is durable as uncertain")
	require.NoError(t, journal.Close())
	require.NoError(t, guard.Close())

	uncertain := readE2EState(t, provisioned.State, writer.policy)
	require.Equal(t, phaseUncertain, uncertain.Phase)
	require.Equal(t, 4, uncertain.Step)
	require.Equal(t, int64(7102), uncertain.ChildPID)
	require.True(t, validHex(uncertain.Attempt, 32))
	attempt := uncertain.Attempt

	now = now.Add(time.Second)
	observations := 0
	reconciled, err := writer.Reconcile(
		t.Context(), live, attempt,
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			observations++
			return ReconcileObservation{
				BindingEqual: EvidenceYes, LineVisible: EvidenceYes,
				ComposerEmpty: EvidenceUnknown, NoticeAbsent: EvidenceUnknown,
				WriterExited: EvidenceYes, ChildExited: EvidenceYes,
				DrainWaitElapsed: EvidenceYes,
			}, nil
		},
	)
	require.NoError(t, err)
	require.True(t, reconciled.Changed)
	require.Equal(t, RecoveryConfirmed, reconciled.Outcome)
	require.Equal(t, 1, observations)

	replayed, err := writer.Reconcile(
		t.Context(), live, attempt,
		func(context.Context, ReconcileSnapshot) (ReconcileObservation, error) {
			t.Fatal("an exact replay after a lost reconcile acknowledgement observed the pane again")
			return ReconcileObservation{}, nil
		},
	)
	require.NoError(t, err)
	require.False(t, replayed.Changed)
	require.Equal(t, RecoveryUnchanged, replayed.Outcome)

	raw, err := os.ReadFile(provisioned.State)
	require.NoError(t, err)
	raw[0] ^= 0xff
	require.NoError(t, os.WriteFile(provisioned.State, raw, 0o660))
	now = now.Add(time.Second)
	repaired, err := writer.Repair(t.Context(), live)
	require.NoError(t, err)
	require.True(t, repaired.Changed)
	require.Equal(t, RecoveryRepaired, repaired.Outcome)
	require.NotNil(t, repaired.Evidence.Record)
	require.Equal(t, EvidenceNo, repaired.Evidence.Record.MagicValid)

	repairedState := readE2EState(t, provisioned.State, writer.policy)
	require.Equal(t, operationRepair, repairedState.Writer)
	require.Equal(t, resultRepaired, repairedState.Result)
	require.Equal(t, phaseUncertain, repairedState.Phase)
	require.Nil(t, readE2ERegister(t, provisioned.Register, writer.policy).Pending)

	replayedRepair, err := writer.Repair(t.Context(), live)
	require.NoError(t, err)
	require.False(t, replayedRepair.Changed)
	require.Equal(t, RecoveryUnchanged, replayedRepair.Outcome)
}

func readE2EState(t *testing.T, path string, policy validatedRecordPolicy) stateRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	record, err := decodeStateRecord(policy, raw)
	require.NoError(t, err)
	return record
}

func readE2ERegister(t *testing.T, path string, policy validatedRecordPolicy) registerRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	record, err := decodeRegisterRecord(policy, raw)
	require.NoError(t, err)
	return record
}
