package artifact

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/endpoint"
	hubapi "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	testSendArtifactID = protocol.OpaqueID("art-0123456789abcdef0123456789abcdef")
	testSendSender     = protocol.AgentID("agent-sender")
	testSendRecipient  = protocol.AgentID("agent-recipient")
)

type fakeArtifactSendClient struct {
	states    []protocol.ArtifactState
	callCount int
	notices   []endpoint.NotificationRequest
	notifyErr error
}

func (c *fakeArtifactSendClient) call(
	_ context.Context,
	method string,
	params any,
	result any,
) error {
	if method != hubapi.MethodGetArtifactStatus {
		return fmt.Errorf("unexpected method %s", method)
	}
	idParams, ok := params.(protocol.ArtifactIDParams)
	if !ok {
		return fmt.Errorf("unexpected params %T", params)
	}
	status, ok := result.(*protocol.ArtifactStatusResult)
	if !ok {
		return fmt.Errorf("unexpected result %T", result)
	}
	if len(c.states) == 0 {
		return fmt.Errorf("no status queued")
	}
	state := c.states[0]
	if len(c.states) > 1 {
		c.states = c.states[1:]
	}
	c.callCount++
	*status = protocol.ArtifactStatusResult{ArtifactID: idParams.ArtifactID, State: state}
	return nil
}

func (c *fakeArtifactSendClient) notify(
	_ context.Context,
	request endpoint.NotificationRequest,
) error {
	c.notices = append(c.notices, request)
	return c.notifyErr
}

func TestArtifactUploadCompleteStates(t *testing.T) {
	t.Parallel()

	complete := []protocol.ArtifactState{
		protocol.ArtifactStateLocalAvailable,
		protocol.ArtifactStateQueuedRemote,
		protocol.ArtifactStateReplicating,
		protocol.ArtifactStateStoredRemote,
		protocol.ArtifactStateAcknowledged,
	}
	for _, state := range complete {
		require.True(t, artifactUploadComplete(state), state)
		needsUpload, err := artifactNeedsUpload(state, false)
		require.NoError(t, err, state)
		require.False(t, needsUpload, state)
	}

	needsUpload, err := artifactNeedsUpload(protocol.ArtifactStateUploading, true)
	require.NoError(t, err)
	require.True(t, needsUpload)

	needsUpload, err = artifactNeedsUpload(protocol.ArtifactStateUploading, false)
	require.False(t, needsUpload)
	require.ErrorContains(t, err, "durable content key is missing")
}

func TestArtifactTerminalStatesDoNotNotify(t *testing.T) {
	t.Parallel()

	states := []protocol.ArtifactState{
		protocol.ArtifactStateFailedPermanent,
		protocol.ArtifactStateExpired,
		protocol.ArtifactStateCancelled,
		protocol.ArtifactStateRevoked,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			client := &fakeArtifactSendClient{}
			needsUpload, err := artifactNeedsUpload(state, true)
			require.False(t, needsUpload)
			require.ErrorContains(t, err, string(state))

			_, err = finishArtifactSend(context.Background(), client,
				testSendSender, testSendRecipient, 42,
				sendResult{ArtifactID: testSendArtifactID, State: state}, false,
				artifactSendOptions{},
			)
			require.ErrorContains(t, err, string(state))
			require.Empty(t, client.notices)
		})
	}
}

func TestRemoteArtifactWaitDoesNotNotifyBeforeStored(t *testing.T) {
	states := []protocol.ArtifactState{
		protocol.ArtifactStateQueuedRemote,
		protocol.ArtifactStateReplicating,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			client := &fakeArtifactSendClient{states: []protocol.ArtifactState{state}}
			_, err := finishArtifactSend(context.Background(), client,
				testSendSender, testSendRecipient, 42,
				sendResult{ArtifactID: testSendArtifactID, State: state}, true,
				artifactSendOptions{remoteWait: 20 * time.Millisecond, pollEvery: time.Millisecond},
			)
			require.ErrorContains(t, err, "stored at the origin")
			require.ErrorContains(t, err, string(state))
			require.ErrorContains(t, err, "rerun the same command")
			require.Empty(t, client.notices)
		})
	}
}

func TestRemoteArtifactRetryAfterKeyDeletionWaitsThenNotifies(t *testing.T) {
	needsUpload, err := artifactNeedsUpload(protocol.ArtifactStateQueuedRemote, false)
	require.NoError(t, err)
	require.False(t, needsUpload)

	client := &fakeArtifactSendClient{states: []protocol.ArtifactState{
		protocol.ArtifactStateReplicating,
		protocol.ArtifactStateStoredRemote,
	}}
	result, err := finishArtifactSend(context.Background(), client,
		testSendSender, testSendRecipient, 42,
		sendResult{ArtifactID: testSendArtifactID, State: protocol.ArtifactStateQueuedRemote}, true,
		artifactSendOptions{remoteWait: time.Second, pollEvery: time.Millisecond},
	)
	require.NoError(t, err)
	require.Equal(t, protocol.ArtifactStateStoredRemote, result.State)
	require.True(t, result.NoticeSent)
	require.Equal(t, 2, client.callCount)
	require.Len(t, client.notices, 1)
	require.Equal(t, protocol.OpaqueID("notice-"+string(testSendArtifactID)), client.notices[0].ClientMsgID)
}

func TestRemoteNotificationFailureIsRetryableAndLocalFailureIsUnchanged(t *testing.T) {
	notificationErr := errors.New("notification unavailable")
	remote := &fakeArtifactSendClient{notifyErr: notificationErr}
	input := sendResult{ArtifactID: testSendArtifactID, State: protocol.ArtifactStateStoredRemote}

	_, err := finishArtifactSend(context.Background(), remote,
		testSendSender, testSendRecipient, 42, input, true,
		artifactSendOptions{remoteWait: time.Second, pollEvery: time.Millisecond},
	)
	require.ErrorContains(t, err, "send remote FILE_READY notification")
	require.Len(t, remote.notices, 1)

	remote.notifyErr = nil
	result, err := finishArtifactSend(context.Background(), remote,
		testSendSender, testSendRecipient, 42, input, true,
		artifactSendOptions{remoteWait: time.Second, pollEvery: time.Millisecond},
	)
	require.NoError(t, err)
	require.True(t, result.NoticeSent)
	require.Len(t, remote.notices, 2)
	require.Equal(t, remote.notices[0].ClientMsgID, remote.notices[1].ClientMsgID)

	local := &fakeArtifactSendClient{notifyErr: notificationErr}
	result, err = finishArtifactSend(context.Background(), local,
		testSendSender, testSendRecipient, 42,
		sendResult{ArtifactID: testSendArtifactID, State: protocol.ArtifactStateLocalAvailable},
		false, artifactSendOptions{},
	)
	require.NoError(t, err)
	require.False(t, result.NoticeSent)
	require.Len(t, local.notices, 1)
}
