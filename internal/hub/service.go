package hub

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/policy"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

// keyRegistry is the fail-closed subset of the local registry the Hub reads.
// The concrete registry owns agent_keys; the Hub never writes that table.
type keyRegistry interface {
	ActiveSigningKey(context.Context, registry.Querier, protocol.AgentID) (registry.KeyInfo, error)
	Blocked(context.Context, registry.Querier, protocol.AgentID) (bool, error)
	Route(context.Context, registry.Querier, protocol.AgentID) (registry.RouteResult, error)
	Register(context.Context, registry.Execer, registry.RegisterRequest) (registry.RegisterResult, error)
}

// ServiceOptions supplies every dependency used by the local handlers.
type ServiceOptions struct {
	DB       *sql.DB
	NodeID   protocol.NodeID
	Engine   *channel.Engine
	Registry keyRegistry
	Clock    clock.Clock
	// NotificationWake is an optional capacity-one hint consumed by the human
	// notification service. Commits never wait for it.
	NotificationWake chan<- struct{}

	// sessionRuntime is a deterministic kernel-evidence seam for package
	// tests. Production always leaves it nil and uses the platform runtime.
	sessionRuntime sessionProcessRuntime

	FederationRegistry *registry.Registry
	Policy             *policy.Policy
}

// Service implements all four handler seams over one local store.
type Service struct {
	db               *sql.DB
	nodeID           protocol.NodeID
	engine           *channel.Engine
	registry         keyRegistry
	clock            clock.Clock
	notificationWake chan<- struct{}
	sessions         sessionProcessRuntime

	federationRegistry *registry.Registry
	policy             *policy.Policy
}

// NewService builds the transactional local handlers. It performs no I/O.
func NewService(opts ServiceOptions) (*Service, error) {
	if opts.DB == nil || !opts.NodeID.Valid() || opts.Engine == nil || opts.Registry == nil || opts.Clock == nil {
		return nil, fmt.Errorf("new hub service: %w", errInvalidArgument)
	}
	if opts.sessionRuntime == nil {
		opts.sessionRuntime = newSessionProcessRuntime()
	}
	return &Service{
		db: opts.DB, nodeID: opts.NodeID, engine: opts.Engine,
		registry: opts.Registry, clock: opts.Clock, sessions: opts.sessionRuntime,
		notificationWake:   opts.NotificationWake,
		federationRegistry: opts.FederationRegistry, policy: opts.Policy,
	}, nil
}

func (s *Service) wakeNotifications() {
	if s.notificationWake == nil {
		return
	}
	select {
	case s.notificationWake <- struct{}{}:
	default:
	}
}

// Handlers returns the local method table backed by s.
func (s *Service) Handlers() Handlers {
	return Handlers{
		AgentRegistration: s, EndpointRegistration: s,
		Admit: s, Submit: s, Claim: s, Ack: s, Pending: s, DeliveryStatus: s,
	}
}

// MessageRecipient is the admission metadata bound to one recipient.
type MessageRecipient struct {
	Agent        protocol.AgentID      `json:"agent_id"`
	DeliveryMode protocol.DeliveryMode `json:"delivery_mode"`
	RequiresAck  bool                  `json:"requires_ack"`
}

// AdmitMessageParams is the plaintext-free metadata an Endpoint asks the Hub
// to admit. The authenticated socket identity supplies the sender.
type AdmitMessageParams struct {
	MessageID         protocol.ULID             `json:"message_id"`
	ClientMsgID       protocol.OpaqueID         `json:"client_msg_id"`
	OriginNode        protocol.NodeID           `json:"origin_node"`
	DestinationNode   protocol.NodeID           `json:"destination_node"`
	Recipients        []MessageRecipient        `json:"recipients"`
	ThreadRef         protocol.ThreadRef        `json:"thread_ref"`
	ThreadID          protocol.ThreadID         `json:"thread_id"`
	Priority          protocol.Priority         `json:"priority"`
	CreatedAt         time.Time                 `json:"created_at"`
	ExpiresAt         time.Time                 `json:"expires_at"`
	ParentMessageID   protocol.ULID             `json:"parent_message_id,omitempty"`
	HopCount          int64                     `json:"hop_count"`
	RouteDigest       protocol.Digest           `json:"route_digest"`
	RequestedAutonomy protocol.Autonomy         `json:"requested_autonomy,omitempty"`
	RequestedBudget   *protocol.RequestedBudget `json:"requested_budget,omitempty"`
}

// AdmitMessageResult adds the idempotency state to the frozen result.
type AdmitMessageResult struct {
	AdmissionResult protocol.AdmissionResult `json:"admission_result"`
	MessageID       protocol.ULID            `json:"message_id"`
	OriginNode      protocol.NodeID          `json:"origin_node"`
	DestinationNode protocol.NodeID          `json:"destination_node"`
	ThreadRef       protocol.ThreadRef       `json:"thread_ref"`
	CreatedAt       string                   `json:"created_at"`
	ExpiresAt       string                   `json:"expires_at"`
	HopCount        int64                    `json:"hop_count"`
	RouteDigest     protocol.Digest          `json:"route_digest"`
	Submitted       bool                     `json:"submitted"`
}

func (p AdmitMessageParams) validate(sender protocol.AgentID) error {
	switch {
	case p.MessageID != "":
		return fmt.Errorf("message_id is assigned by the hub: %w", errInvalidArgument)
	case !p.ClientMsgID.Valid():
		return fmt.Errorf("client_msg_id: %w", errInvalidArgument)
	case p.OriginNode != "" && !p.OriginNode.Valid():
		return fmt.Errorf("origin_node: %w", errInvalidArgument)
	case p.DestinationNode != "" && !p.DestinationNode.Valid():
		return fmt.Errorf("destination_node: %w", errInvalidArgument)
	case p.ThreadRef != "":
		return fmt.Errorf("thread_ref is assigned by the hub: %w", errInvalidArgument)
	case !p.ThreadID.Valid():
		return fmt.Errorf("thread_id: %w", errInvalidArgument)
	case !p.Priority.Valid():
		return fmt.Errorf("priority: %w", errInvalidArgument)
	case p.CreatedAt.IsZero() || !p.ExpiresAt.After(p.CreatedAt):
		return fmt.Errorf("message times: %w", errInvalidArgument)
	case p.ParentMessageID != "" && !p.ParentMessageID.Valid():
		return fmt.Errorf("parent_message_id: %w", errInvalidArgument)
	case p.HopCount != 0:
		return fmt.Errorf("hop_count must be zero for a root admission: %w", errInvalidArgument)
	case p.RouteDigest != "":
		return fmt.Errorf("route_digest is assigned by the hub: %w", errInvalidArgument)
	case p.RequestedAutonomy != "" && !p.RequestedAutonomy.Valid():
		return fmt.Errorf("requested_autonomy: %w", errInvalidArgument)
	case len(p.Recipients) < 1 || len(p.Recipients) > 32:
		return fmt.Errorf("recipients: %w", errInvalidArgument)
	}
	if p.RequestedBudget != nil {
		if err := p.RequestedBudget.Validate(); err != nil {
			return fmt.Errorf("requested_budget: %w", err)
		}
	}
	seen := make(map[protocol.AgentID]struct{}, len(p.Recipients))
	for i, recipient := range p.Recipients {
		if !recipient.Agent.Valid() || recipient.Agent == sender {
			return fmt.Errorf("recipients[%d].agent_id: %w", i, errInvalidArgument)
		}
		if !recipient.DeliveryMode.Valid() {
			return fmt.Errorf("recipients[%d].delivery_mode: %w", i, errInvalidArgument)
		}
		if _, exists := seen[recipient.Agent]; exists {
			return fmt.Errorf("recipients[%d].agent_id: %w", i, errInvalidArgument)
		}
		seen[recipient.Agent] = struct{}{}
	}
	return nil
}

func decodeParams(raw json.RawMessage, dst any) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return fmt.Errorf("missing params: %w", errInvalidArgument)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("decode params: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing params: %w", errInvalidArgument)
	}
	return nil
}

func (s *Service) begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin immediate transaction: %w", err)
	}
	return tx, nil
}

func rollback(tx *sql.Tx) { _ = tx.Rollback() }

func commit(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *Service) requireSenderKey(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
) (registry.KeyInfo, error) {
	blocked, err := s.registry.Blocked(ctx, q, agent)
	if err != nil || blocked {
		if err == nil {
			err = registry.ErrBlocked
		}
		return registry.KeyInfo{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	key, err := s.registry.ActiveSigningKey(ctx, q, agent)
	if err != nil {
		return registry.KeyInfo{}, domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	return key, nil
}

func (s *Service) requireUnblocked(ctx context.Context, q registry.Querier, agent protocol.AgentID) error {
	blocked, err := s.registry.Blocked(ctx, q, agent)
	if err != nil || blocked {
		if err == nil {
			err = registry.ErrBlocked
		}
		return domainError(protocol.ErrorCodeKeyUnavailable, err)
	}
	return nil
}

type recipientGate struct {
	recipients  []channel.Recipient
	destination protocol.NodeID
}

// gateRecipients checks registry blocks before resolving each route. Local
// recipients must have a deliverable session; remote recipients deliberately
// do not inspect this node's sessions. Every recipient must resolve to the
// same node.
func (s *Service) gateRecipients(
	ctx context.Context,
	tx *sql.Tx,
	params AdmitMessageParams,
) (recipientGate, error) {
	answer := make([]channel.Recipient, 0, len(params.Recipients))
	var destination protocol.NodeID
	for _, recipient := range params.Recipients {
		if err := s.requireUnblocked(ctx, tx, recipient.Agent); err != nil {
			return recipientGate{}, err
		}
		route, err := s.registry.Route(ctx, tx, recipient.Agent)
		if err != nil {
			return recipientGate{}, fmt.Errorf("route recipient: %w", err)
		}
		switch route.Kind {
		case registry.RouteNotFound:
			return recipientGate{}, domainError(protocol.ErrorCodeRecipientNotDeliverable,
				fmt.Errorf("recipient %s has no route", recipient.Agent))
		case registry.RoutePeerBlocked:
			return recipientGate{}, domainError(protocol.ErrorCodeKeyUnavailable, registry.ErrBlocked)
		case registry.RouteLocal, registry.RouteFederated:
		default:
			return recipientGate{}, fmt.Errorf("route recipient returned %q: %w", route.Kind, errInvalidArgument)
		}
		if destination == "" {
			destination = route.Node
		} else if destination != route.Node {
			return recipientGate{}, domainError(protocol.ErrorCodeRecipientNotDeliverable,
				fmt.Errorf("recipients resolve to multiple nodes"))
		}
		if route.Kind == registry.RouteLocal {
			var deliverable bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
				SELECT 1 FROM sessions WHERE agent_id = ? AND state = 'CONNECTED'
				AND deliverable = 1 AND ended_at IS NULL)`, recipient.Agent).Scan(&deliverable); err != nil {
				return recipientGate{}, fmt.Errorf("select recipient deliverability: %w", err)
			}
			if !deliverable {
				return recipientGate{}, domainError(protocol.ErrorCodeRecipientNotDeliverable,
					fmt.Errorf("recipient %s is not deliverable", recipient.Agent))
			}
		}
		answer = append(answer, channel.Recipient{
			Agent: recipient.Agent, Deliverable: true, Policy: channel.PolicyAllowed,
		})
	}
	return recipientGate{recipients: answer, destination: destination}, nil
}
