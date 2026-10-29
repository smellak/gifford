package endpoint

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"

	giffordcrypto "github.com/smellak/gifford/internal/crypto"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	outboundStateDir     = "endpoint"
	outboundStateFile    = "outbound.json"
	outboundStateVersion = 1
)

var outboundStageCounter atomic.Uint64

type persistedOutboundState struct {
	Version  int                        `json:"version"`
	Attempts []persistedOutboundAttempt `json:"attempts"`
}

type persistedOutboundAttempt struct {
	Request  persistedOutboundRequest  `json:"request"`
	Envelope *protocol.WireEnvelope    `json:"envelope,omitempty"`
	Result   *protocol.AdmissionResult `json:"result,omitempty"`
}

type persistedOutboundRequest struct {
	To                protocol.AgentID          `json:"to"`
	Subject           string                    `json:"subject"`
	Body              string                    `json:"body"`
	ThreadID          protocol.ThreadID         `json:"thread_id"`
	Priority          protocol.Priority         `json:"priority"`
	ClientMsgID       protocol.OpaqueID         `json:"client_msg_id"`
	CreatedAt         time.Time                 `json:"created_at"`
	ExpiresAt         time.Time                 `json:"expires_at"`
	ParentMessageID   protocol.ULID             `json:"parent_message_id,omitempty"`
	RequestedAutonomy protocol.Autonomy         `json:"requested_autonomy,omitempty"`
	RequestedBudget   *protocol.RequestedBudget `json:"requested_budget,omitempty"`
}

func persistRequest(request outboundMessage) persistedOutboundRequest {
	return persistedOutboundRequest{
		To: request.to, Subject: request.subject, Body: request.body,
		ThreadID: request.threadID, Priority: request.priority, ClientMsgID: request.clientMsgID,
		CreatedAt: request.createdAt, ExpiresAt: request.expiresAt,
		ParentMessageID: request.parentMessageID, RequestedAutonomy: request.requestedAutonomy,
		RequestedBudget: request.requestedBudget,
	}
}

func (request persistedOutboundRequest) outbound(agent protocol.AgentID) (outboundMessage, error) {
	if !request.To.Valid() || request.To == agent || utf8.RuneCountInString(request.Subject) < 1 ||
		utf8.RuneCountInString(request.Subject) > 200 || utf8.RuneCountInString(request.Body) < 1 ||
		utf8.RuneCountInString(request.Body) > 65536 || !request.ThreadID.Valid() ||
		!request.Priority.Valid() || !request.ClientMsgID.Valid() || request.CreatedAt.IsZero() ||
		!request.ExpiresAt.After(request.CreatedAt) ||
		(request.ParentMessageID != "" && !request.ParentMessageID.Valid()) ||
		(request.RequestedAutonomy != "" && !request.RequestedAutonomy.Valid()) {
		return outboundMessage{}, fmt.Errorf("invalid persisted outbound request: %w", ErrIntegrity)
	}
	if request.RequestedBudget != nil {
		if err := request.RequestedBudget.Validate(); err != nil {
			return outboundMessage{}, fmt.Errorf("invalid persisted outbound budget: %w", ErrIntegrity)
		}
	}
	return outboundMessage{
		to: request.To, subject: request.Subject, body: request.Body,
		threadID: request.ThreadID, priority: request.Priority, clientMsgID: request.ClientMsgID,
		createdAt: request.CreatedAt, expiresAt: request.ExpiresAt,
		parentMessageID: request.ParentMessageID, requestedAutonomy: request.RequestedAutonomy,
		requestedBudget: request.RequestedBudget,
	}, nil
}

func (s *Server) loadOutboundState() error {
	dir := filepath.Join(s.dataDir, outboundStateDir, string(s.agent))
	for _, path := range []string{filepath.Join(s.dataDir, outboundStateDir), dir} {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create outbound state directory: %w", err)
		}
		if err := checkPrivatePath(path, 0o700, true); err != nil {
			return err
		}
	}
	s.outboundStorePath = filepath.Join(dir, outboundStateFile)
	if err := checkPrivatePath(s.outboundStorePath, 0o600, false); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	raw, err := os.ReadFile(s.outboundStorePath)
	if err != nil {
		return fmt.Errorf("read outbound state: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var state persistedOutboundState
	if err := decoder.Decode(&state); err != nil {
		return fmt.Errorf("decode outbound state: %w", ErrIntegrity)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("decode outbound state trailing data: %w", ErrIntegrity)
	}
	if state.Version != outboundStateVersion {
		return fmt.Errorf("outbound state version: %w", ErrIntegrity)
	}
	public := s.priv.Public().(ed25519.PublicKey)
	for _, attempt := range state.Attempts {
		request, err := attempt.Request.outbound(s.agent)
		if err != nil {
			return err
		}
		if _, exists := s.requests[request.clientMsgID]; exists {
			return fmt.Errorf("duplicate persisted outbound request: %w", ErrIntegrity)
		}
		if err := validatePersistedAttempt(s.agent, public, request, attempt); err != nil {
			return err
		}
		s.requests[request.clientMsgID] = request
		if attempt.Envelope != nil {
			s.outgoing[request.clientMsgID] = *attempt.Envelope
		}
		if attempt.Result != nil {
			s.results[request.clientMsgID] = *attempt.Result
		}
	}
	return nil
}

func validatePersistedAttempt(
	agent protocol.AgentID,
	public ed25519.PublicKey,
	request outboundMessage,
	attempt persistedOutboundAttempt,
) error {
	if attempt.Envelope != nil {
		envelope := *attempt.Envelope
		if err := envelope.Validate(); err != nil || envelope.ClientMsgID != request.clientMsgID ||
			envelope.OriginAgent != agent || len(envelope.RecipientAgents) != 1 ||
			envelope.RecipientAgents[0] != request.to || !envelope.CreatedAt.Equal(request.createdAt) ||
			!envelope.ExpiresAt.Equal(request.expiresAt) || envelope.Priority != request.priority ||
			envelope.ParentMessageID != request.parentMessageID {
			return fmt.Errorf("bind persisted outbound envelope: %w", ErrIntegrity)
		}
		if err := giffordcrypto.VerifyWireEnvelope(public, envelope); err != nil {
			return fmt.Errorf("verify persisted outbound envelope: %w", ErrIntegrity)
		}
		var message protocol.LogicalMessage
		if err := json.Unmarshal(envelope.Ciphertext, &message); err != nil ||
			message.Validate() != nil ||
			message.Subject != request.subject || message.Body != request.body ||
			message.ThreadID != request.threadID || message.RequestedAutonomy != request.requestedAutonomy ||
			!sameRequestedBudget(message.RequestedBudget, request.requestedBudget) ||
			!logicalMessageBound(message, envelope) {
			return fmt.Errorf("bind persisted outbound plaintext: %w", ErrIntegrity)
		}
	}
	if attempt.Result != nil {
		if err := attempt.Result.Validate(); err != nil || attempt.Result.ThreadID != request.threadID ||
			(attempt.Envelope != nil && attempt.Result.MessageID != attempt.Envelope.MessageID) {
			return fmt.Errorf("bind persisted outbound result: %w", ErrIntegrity)
		}
	}
	return nil
}

func sameRequestedBudget(first, second *protocol.RequestedBudget) bool {
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	return bytes.Equal(firstJSON, secondJSON)
}

// saveOutboundStateLocked publishes the complete attempt set atomically.
// The caller holds s.mu, keeping the memory and durable snapshots aligned.
func (s *Server) saveOutboundStateLocked() error {
	ids := make([]string, 0, len(s.requests))
	for id := range s.requests {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	state := persistedOutboundState{Version: outboundStateVersion, Attempts: make([]persistedOutboundAttempt, 0, len(ids))}
	for _, rawID := range ids {
		id := protocol.OpaqueID(rawID)
		attempt := persistedOutboundAttempt{Request: persistRequest(s.requests[id])}
		if envelope, ok := s.outgoing[id]; ok {
			envelopeCopy := envelope
			attempt.Envelope = &envelopeCopy
		}
		if result, ok := s.results[id]; ok {
			resultCopy := result
			attempt.Result = &resultCopy
		}
		state.Attempts = append(state.Attempts, attempt)
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode outbound state: %w", err)
	}
	if err := checkPrivatePath(s.outboundStorePath, 0o600, false); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	staging := s.outboundStorePath + ".tmp." + strconv.Itoa(os.Getpid()) + "." +
		strconv.FormatUint(outboundStageCounter.Add(1), 10)
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("stage outbound state: %w", err)
	}
	removeStaging := true
	defer func() {
		if removeStaging {
			_ = os.Remove(staging)
		}
	}()
	for written := 0; written < len(raw); {
		n, err := file.Write(raw[written:])
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("write outbound state: %w", err)
		}
		if n == 0 {
			_ = file.Close()
			return fmt.Errorf("write outbound state: %w", io.ErrShortWrite)
		}
		written += n
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync outbound state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close outbound state: %w", err)
	}
	if err := os.Rename(staging, s.outboundStorePath); err != nil {
		return fmt.Errorf("publish outbound state: %w", err)
	}
	removeStaging = false
	dir, err := os.Open(filepath.Dir(s.outboundStorePath))
	if err != nil {
		return fmt.Errorf("open outbound state directory: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync outbound state directory: %w", err)
	}
	return nil
}

func checkPrivatePath(path string, want os.FileMode, wantDir bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat outbound state path: %w", err)
	}
	if (wantDir && !info.IsDir()) || (!wantDir && !info.Mode().IsRegular()) {
		return fmt.Errorf("outbound state path has wrong type: %w", ErrIntegrity)
	}
	if info.Mode().Perm() != want {
		return fmt.Errorf("outbound state path has insecure mode: %w", ErrIntegrity)
	}
	return nil
}
