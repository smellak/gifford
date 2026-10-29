package endpoint

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/protocol"
)

const defaultMessageTTL = time.Hour

type sendParams struct {
	To                protocol.AgentID          `json:"to"`
	Subject           string                    `json:"subject"`
	Body              string                    `json:"body"`
	ThreadID          *protocol.ThreadID        `json:"thread_id"`
	Priority          *protocol.Priority        `json:"priority"`
	TTL               *string                   `json:"ttl"`
	ClientMsgID       *protocol.OpaqueID        `json:"client_msg_id"`
	RequestedAutonomy *protocol.Autonomy        `json:"requested_autonomy"`
	RequestedBudget   *protocol.RequestedBudget `json:"requested_budget"`
}

type outboundMessage struct {
	to                protocol.AgentID
	subject           string
	body              string
	threadID          protocol.ThreadID
	priority          protocol.Priority
	clientMsgID       protocol.OpaqueID
	createdAt         time.Time
	expiresAt         time.Time
	parentMessageID   protocol.ULID
	requestedAutonomy protocol.Autonomy
	requestedBudget   *protocol.RequestedBudget
}

// admitRequest deliberately omits message_id, thread_ref, hop_count and
// route_digest: those four signed header fields belong only to the Hub.
type admitRequest struct {
	ClientMsgID       protocol.OpaqueID         `json:"client_msg_id"`
	Recipients        []hub.MessageRecipient    `json:"recipients"`
	ThreadID          protocol.ThreadID         `json:"thread_id"`
	Priority          protocol.Priority         `json:"priority"`
	CreatedAt         time.Time                 `json:"created_at"`
	ExpiresAt         time.Time                 `json:"expires_at"`
	ParentMessageID   protocol.ULID             `json:"parent_message_id,omitempty"`
	RequestedAutonomy protocol.Autonomy         `json:"requested_autonomy,omitempty"`
	RequestedBudget   *protocol.RequestedBudget `json:"requested_budget,omitempty"`
}

func (s *Server) sendMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params sendParams
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	outbound, err := s.outboundFromSend(params)
	if err != nil {
		return nil, err
	}
	return s.sendRemembered(ctx, outbound)
}

func (s *Server) sendRemembered(ctx context.Context, outbound outboundMessage) (any, error) {
	s.mu.Lock()
	previous, exists := s.requests[outbound.clientMsgID]
	if exists {
		if !sameOutboundRequest(previous, outbound) {
			s.mu.Unlock()
			return nil, fmt.Errorf("reuse client message id: %w", ErrBadArguments)
		}
		outbound = previous
	} else {
		s.requests[outbound.clientMsgID] = outbound
	}
	s.mu.Unlock()
	return s.sendOutgoing(ctx, outbound)
}

func sameOutboundRequest(first, second outboundMessage) bool {
	firstBudget, _ := json.Marshal(first.requestedBudget)
	secondBudget, _ := json.Marshal(second.requestedBudget)
	return first.to == second.to && first.subject == second.subject && first.body == second.body &&
		first.threadID == second.threadID && first.priority == second.priority &&
		first.clientMsgID == second.clientMsgID && first.parentMessageID == second.parentMessageID &&
		first.requestedAutonomy == second.requestedAutonomy && string(firstBudget) == string(secondBudget) &&
		first.expiresAt.Sub(first.createdAt) == second.expiresAt.Sub(second.createdAt)
}

func (s *Server) outboundFromSend(params sendParams) (outboundMessage, error) {
	if !params.To.Valid() || params.To == s.agent || utf8.RuneCountInString(params.Subject) < 1 ||
		utf8.RuneCountInString(params.Subject) > 200 || utf8.RuneCountInString(params.Body) < 1 ||
		utf8.RuneCountInString(params.Body) > 65536 {
		return outboundMessage{}, fmt.Errorf("send message: %w", ErrBadArguments)
	}
	priority := protocol.PriorityNormal
	if params.Priority != nil {
		priority = *params.Priority
	}
	if !priority.Valid() {
		return outboundMessage{}, fmt.Errorf("send message priority: %w", ErrBadArguments)
	}
	ttl := defaultMessageTTL
	if params.TTL != nil {
		parsed, err := time.ParseDuration(*params.TTL)
		if err != nil || parsed <= 0 {
			return outboundMessage{}, fmt.Errorf("send message ttl: %w", ErrBadArguments)
		}
		ttl = parsed
	}
	clientID := protocol.OpaqueID("")
	if params.ClientMsgID != nil {
		clientID = *params.ClientMsgID
	} else {
		var err error
		clientID, err = mintOpaqueID("msg")
		if err != nil {
			return outboundMessage{}, err
		}
	}
	if !clientID.Valid() {
		return outboundMessage{}, fmt.Errorf("send message client id: %w", ErrBadArguments)
	}
	threadID := protocol.ThreadID("")
	if params.ThreadID != nil {
		threadID = *params.ThreadID
	} else {
		threadID = protocol.ThreadID("thread-" + string(clientID))
	}
	if !threadID.Valid() {
		return outboundMessage{}, fmt.Errorf("send message thread id: %w", ErrBadArguments)
	}
	var autonomy protocol.Autonomy
	if params.RequestedAutonomy != nil {
		autonomy = *params.RequestedAutonomy
		if !autonomy.Valid() {
			return outboundMessage{}, fmt.Errorf("send message autonomy: %w", ErrBadArguments)
		}
	}
	if params.RequestedBudget != nil {
		if err := params.RequestedBudget.Validate(); err != nil {
			return outboundMessage{}, fmt.Errorf("send message budget: %w", ErrBadArguments)
		}
	}
	now := s.now().UTC()
	return outboundMessage{
		to: params.To, subject: params.Subject, body: params.Body, threadID: threadID,
		priority: priority, clientMsgID: clientID, createdAt: now, expiresAt: now.Add(ttl),
		requestedAutonomy: autonomy, requestedBudget: params.RequestedBudget,
	}, nil
}

func (s *Server) sendOutgoing(ctx context.Context, outgoing outboundMessage) (any, error) {
	request := admitRequest{
		ClientMsgID: outgoing.clientMsgID,
		Recipients: []hub.MessageRecipient{{
			Agent: outgoing.to, DeliveryMode: protocol.DeliveryModeWhenSafe, RequiresAck: true,
		}},
		ThreadID: outgoing.threadID, Priority: outgoing.priority,
		CreatedAt: outgoing.createdAt, ExpiresAt: outgoing.expiresAt,
		ParentMessageID: outgoing.parentMessageID, RequestedAutonomy: outgoing.requestedAutonomy,
		RequestedBudget: outgoing.requestedBudget,
	}
	raw, err := s.hub.Call(ctx, hub.MethodAdmitMessage, request)
	if err != nil {
		return nil, fmt.Errorf("admit message: %w", ErrHubUnavailable)
	}
	var admitted hub.AdmitMessageResult
	if err := json.Unmarshal(raw, &admitted); err != nil || admitted.AdmissionResult.Validate() != nil ||
		admitted.MessageID != admitted.AdmissionResult.MessageID || !admitted.ThreadRef.Valid() ||
		!admitted.OriginNode.Valid() || !admitted.DestinationNode.Valid() ||
		!admitted.RouteDigest.Valid() || admitted.HopCount != 0 ||
		admitted.AdmissionResult.ThreadID != outgoing.threadID {
		return nil, fmt.Errorf("decode admission: %w", ErrHubUnavailable)
	}
	createdAt, createdErr := time.Parse(time.RFC3339Nano, admitted.CreatedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339Nano, admitted.ExpiresAt)
	if createdErr != nil || expiresErr != nil || !createdAt.Equal(outgoing.createdAt) ||
		!expiresAt.Equal(outgoing.expiresAt) {
		return nil, fmt.Errorf("bind admission times: %w", ErrHubUnavailable)
	}
	if admitted.Submitted {
		return admitted.AdmissionResult, nil
	}

	s.mu.Lock()
	envelope, cached := s.outgoing[outgoing.clientMsgID]
	s.mu.Unlock()
	if cached {
		if envelope.MessageID != admitted.MessageID {
			return nil, fmt.Errorf("reuse signed envelope: %w", ErrIntegrity)
		}
	} else {
		envelope, err = s.buildEnvelope(outgoing, admitted)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.outgoing[outgoing.clientMsgID] = envelope
		s.mu.Unlock()
	}
	if _, err := s.hub.Call(ctx, hub.MethodSubmitMessageEnvelope, envelope); err != nil {
		return nil, fmt.Errorf("submit message: %w", ErrHubUnavailable)
	}
	return admitted.AdmissionResult, nil
}

func (s *Server) buildEnvelope(
	outgoing outboundMessage,
	admitted hub.AdmitMessageResult,
) (protocol.WireEnvelope, error) {
	createdAt, err := time.Parse(time.RFC3339Nano, admitted.CreatedAt)
	if err != nil {
		return protocol.WireEnvelope{}, fmt.Errorf("parse admitted creation time: %w", ErrHubUnavailable)
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, admitted.ExpiresAt)
	if err != nil {
		return protocol.WireEnvelope{}, fmt.Errorf("parse admitted expiry: %w", ErrHubUnavailable)
	}
	logical := protocol.LogicalMessage{
		MessageID: admitted.MessageID, ClientMsgID: outgoing.clientMsgID,
		ThreadID: outgoing.threadID, FromAgent: s.agent, ToAgents: []protocol.AgentID{outgoing.to},
		OriginNode: admitted.OriginNode, DestinationNode: admitted.DestinationNode,
		Subject: outgoing.subject, Body: outgoing.body, Priority: outgoing.priority,
		DeliveryMode: protocol.DeliveryModeWhenSafe, CreatedAt: createdAt, ExpiresAt: expiresAt,
		RequiresAck: true, Sensitivity: protocol.MessageSensitivityInternal,
		ParentMessageID: outgoing.parentMessageID, ThreadSequence: 1,
		HopCount: admitted.HopCount, RouteDigest: admitted.RouteDigest,
		RequestedAutonomy: outgoing.requestedAutonomy, RequestedBudget: outgoing.requestedBudget,
		ControlVersion: admitted.AdmissionResult.ControlVersion,
	}
	if err := logical.Validate(); err != nil {
		return protocol.WireEnvelope{}, fmt.Errorf("build logical message: %w", ErrBadArguments)
	}
	ciphertext, err := json.Marshal(logical)
	if err != nil {
		return protocol.WireEnvelope{}, fmt.Errorf("encode logical message: %w", ErrIntegrity)
	}
	digest := sha256.Sum256(ciphertext)
	envelope := protocol.WireEnvelope{
		Protocol: protocol.WireProtocol, Version: protocol.WireVersion,
		MessageID: admitted.MessageID, ClientMsgID: outgoing.clientMsgID,
		OriginNode: admitted.OriginNode, OriginAgent: s.agent,
		DestinationNode: admitted.DestinationNode, RecipientAgents: []protocol.AgentID{outgoing.to},
		CreatedAt: createdAt, ExpiresAt: expiresAt, Priority: outgoing.priority,
		ThreadRef: admitted.ThreadRef, ParentMessageID: outgoing.parentMessageID,
		HopCount: admitted.HopCount, RouteDigest: admitted.RouteDigest,
		ControlVersion: admitted.AdmissionResult.ControlVersion, ContentType: protocol.WireContentType,
		EncryptionProfile:     protocol.EncryptionProfileNone,
		SenderSigningKeyID:    protocol.SigningKeyID("ed25519:" + string(s.agent) + ":v1"),
		RecipientKeyEnvelopes: []protocol.RecipientKeyEnvelope{},
		Ciphertext:            ciphertext, CiphertextSHA256: protocol.SHA256Hex(hex.EncodeToString(digest[:])),
	}
	if err := giffordcrypto.SignWireEnvelope(s.priv, &envelope); err != nil {
		return protocol.WireEnvelope{}, fmt.Errorf("sign message envelope: %w", ErrIntegrity)
	}
	if err := envelope.Validate(); err != nil {
		return protocol.WireEnvelope{}, fmt.Errorf("validate message envelope: %w", ErrIntegrity)
	}
	return envelope, nil
}

func (s *Server) listPendingMessages(ctx context.Context, args json.RawMessage) (any, error) {
	var params struct{}
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	raw, err := s.hub.Call(ctx, hub.MethodListPendingMessages, params)
	if err != nil {
		return nil, fmt.Errorf("list pending messages: %w", ErrHubUnavailable)
	}
	var answer hub.ListPendingMessagesResult
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, fmt.Errorf("decode pending messages: %w", ErrHubUnavailable)
	}
	return answer, nil
}

type readParams struct {
	ID                 protocol.ULID `json:"id"`
	DeliveryGeneration int64         `json:"delivery_generation"`
}

func (s *Server) readMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params readParams
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	if !params.ID.Valid() || params.DeliveryGeneration < 1 {
		return nil, fmt.Errorf("read message: %w", ErrBadArguments)
	}
	raw, err := s.hub.Call(ctx, hub.MethodClaimMessage, hub.ClaimMessageParams{
		MessageID: params.ID, DeliveryGeneration: params.DeliveryGeneration,
	})
	if err != nil {
		return nil, fmt.Errorf("claim message: %w", ErrHubUnavailable)
	}
	var claim hub.ClaimMessageResult
	if err := json.Unmarshal(raw, &claim); err != nil || claim.DeliveryGeneration != params.DeliveryGeneration {
		return nil, fmt.Errorf("decode claimed message: %w", ErrHubUnavailable)
	}
	message, err := decodeClaim(claim)
	if err != nil {
		return nil, err
	}
	if message.DeliveryMode != claim.DeliveryMode || message.RequiresAck != claim.RequiresAck {
		_, ackErr := s.hub.Call(ctx, hub.MethodAckMessage, hub.AckMessageParams{
			MessageID: claim.MessageID, DeliveryGeneration: claim.DeliveryGeneration,
			Status: hub.AckMessageMismatch,
		})
		if ackErr != nil {
			return nil, fmt.Errorf("report metadata mismatch: %w", ErrHubUnavailable)
		}
		return nil, ErrMetadataMismatch
	}
	s.mu.Lock()
	s.received[message.MessageID] = message
	s.mu.Unlock()
	return message, nil
}

type ackParams struct {
	ID                 protocol.ULID      `json:"id"`
	DeliveryGeneration int64              `json:"delivery_generation"`
	Status             protocol.AckStatus `json:"status"`
}

func (s *Server) ackMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params ackParams
	if err := decodeArgs(args, &params); err != nil {
		return nil, err
	}
	if !params.ID.Valid() || params.DeliveryGeneration < 1 || !params.Status.Valid() {
		return nil, fmt.Errorf("ack message: %w", ErrBadArguments)
	}
	cacheKey := fmt.Sprintf("%s:%d", params.ID, params.DeliveryGeneration)
	s.mu.Lock()
	previous, seen := s.acked[cacheKey]
	s.mu.Unlock()
	if seen && (previous == params.Status || previous == protocol.AckStatusAcknowledged) {
		return map[string]any{"acked": true}, nil
	}
	if _, err := s.hub.Call(ctx, hub.MethodAckMessage, hub.AckMessageParams{
		MessageID: params.ID, DeliveryGeneration: params.DeliveryGeneration,
		Status: hub.AckMessageStatus(params.Status),
	}); err != nil {
		return nil, fmt.Errorf("ack message: %w", ErrHubUnavailable)
	}
	s.mu.Lock()
	s.acked[cacheKey] = params.Status
	s.mu.Unlock()
	return map[string]any{"acked": true}, nil
}

type deliveryStatusParams struct {
	MessageID protocol.ULID `json:"message_id"`
}

func (s *Server) getDeliveryStatus(ctx context.Context, args json.RawMessage) (any, error) {
	var params deliveryStatusParams
	if err := decodeArgs(args, &params); err != nil || !params.MessageID.Valid() {
		return nil, fmt.Errorf("delivery status: %w", ErrBadArguments)
	}
	raw, err := s.hub.Call(ctx, hub.MethodGetDeliveryStatus, hub.GetDeliveryStatusParams(params))
	if err != nil {
		return nil, fmt.Errorf("delivery status: %w", ErrHubUnavailable)
	}
	var answer hub.GetDeliveryStatusResult
	if err := json.Unmarshal(raw, &answer); err != nil {
		return nil, fmt.Errorf("decode delivery status: %w", ErrHubUnavailable)
	}
	return answer, nil
}

type replyParams struct {
	ID                protocol.ULID      `json:"id"`
	Body              string             `json:"body"`
	ClientMsgID       *protocol.OpaqueID `json:"client_msg_id"`
	RequestedAutonomy *protocol.Autonomy `json:"requested_autonomy"`
}

func (s *Server) replyToMessage(ctx context.Context, args json.RawMessage) (any, error) {
	var params replyParams
	if err := decodeArgs(args, &params); err != nil || !params.ID.Valid() ||
		utf8.RuneCountInString(params.Body) < 1 || utf8.RuneCountInString(params.Body) > 65536 {
		return nil, fmt.Errorf("reply to message: %w", ErrBadArguments)
	}
	s.mu.Lock()
	original, ok := s.received[params.ID]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("reply source unavailable: %w", ErrUnavailable)
	}
	clientID := protocol.OpaqueID("")
	if params.ClientMsgID != nil {
		clientID = *params.ClientMsgID
	} else {
		var err error
		clientID, err = mintOpaqueID("reply")
		if err != nil {
			return nil, err
		}
	}
	if !clientID.Valid() {
		return nil, fmt.Errorf("reply client id: %w", ErrBadArguments)
	}
	var autonomy protocol.Autonomy
	if params.RequestedAutonomy != nil {
		autonomy = *params.RequestedAutonomy
		if !autonomy.Valid() {
			return nil, fmt.Errorf("reply autonomy: %w", ErrBadArguments)
		}
	}
	now := s.now().UTC()
	return s.sendRemembered(ctx, outboundMessage{
		to: original.FromAgent, subject: original.Subject, body: params.Body,
		threadID: original.ThreadID, priority: original.Priority, clientMsgID: clientID,
		createdAt: now, expiresAt: now.Add(defaultMessageTTL), parentMessageID: original.MessageID,
		requestedAutonomy: autonomy,
	})
}

func mintOpaqueID(prefix string) (protocol.OpaqueID, error) {
	var entropy [12]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("generate message id: %w", err)
	}
	return protocol.OpaqueID(prefix + "-" + hex.EncodeToString(entropy[:])), nil
}
