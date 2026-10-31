package endpoint

import (
	"context"
	"fmt"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

// NotificationRequest is a normal signed message sent by a local CLI acting
// as its bound agent endpoint.
type NotificationRequest struct {
	To          protocol.AgentID
	Subject     string
	Body        string
	ClientMsgID protocol.OpaqueID
}

// SendNotification uses the same admission, envelope and signature path as
// the send_message MCP tool, without starting an MCP server.
func SendNotification(
	ctx context.Context,
	hub Hub,
	dataDir string,
	request NotificationRequest,
) (protocol.AdmissionResult, error) {
	agent, err := hub.WhoAmI(ctx)
	if err != nil {
		return protocol.AdmissionResult{}, fmt.Errorf("notification identity: %w", err)
	}
	private, err := LoadAgentPrivate(dataDir, agent)
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	if !request.To.Valid() || request.To == agent || !request.ClientMsgID.Valid() {
		return protocol.AdmissionResult{}, fmt.Errorf("notification request: %w", ErrInvalidArgument)
	}
	now := (clock.System{}).Now().UTC()
	server := &Server{
		hub: hub, dataDir: dataDir, agent: agent, priv: private, keyActive: true,
		outgoing: make(map[protocol.OpaqueID]protocol.WireEnvelope),
		requests: make(map[protocol.OpaqueID]outboundMessage),
		results:  make(map[protocol.OpaqueID]protocol.AdmissionResult),
	}
	if err := server.loadOutboundState(); err != nil {
		return protocol.AdmissionResult{}, err
	}
	result, err := server.sendRemembered(ctx, outboundMessage{
		to: request.To, subject: request.Subject, body: request.Body,
		threadID: protocol.ThreadID("thread-" + string(request.ClientMsgID)),
		priority: protocol.PriorityNormal, clientMsgID: request.ClientMsgID,
		createdAt: now, expiresAt: now.Add(defaultMessageTTL),
	})
	if err != nil {
		return protocol.AdmissionResult{}, err
	}
	admission, ok := result.(protocol.AdmissionResult)
	if !ok {
		return protocol.AdmissionResult{}, fmt.Errorf("notification result: %w", ErrHubUnavailable)
	}
	return admission, nil
}
