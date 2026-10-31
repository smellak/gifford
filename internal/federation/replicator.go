package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	defaultDeliveryLease    = 30 * time.Second
	defaultDeliveryMaxAge   = 24 * time.Hour
	deliveryRequestTimeout  = 15 * time.Second
	maxReceiptBody          = 1 << 20
	federationObjectAck     = "ack"
	federationObjectMessage = "message"

	outboxQueued          = "QUEUED"
	outboxReplicating     = "REPLICATING"
	outboxFailedRetryable = "FAILED_RETRYABLE"
	outboxStoredRemote    = "STORED_REMOTE"
	outboxControlDenied   = "CONTROL_DENIED"
	outboxDeadLetter      = "DEAD_LETTER"
	outboxExpired         = "EXPIRED"

	messageQueuedRemote    = "QUEUED_REMOTE"
	messageStoredRemote    = "STORED_REMOTE"
	messageFailedPermanent = "FAILED_PERMANENT"
	messageExpired         = "EXPIRED"
)

// ReplicatorAlert is a content-free operational signal. Code is deliberately
// fixed and carries neither peer response text nor message content.
type ReplicatorAlert struct {
	Code string
}

// DeliveryResponse is the bounded HTTP result returned to the replicator.
type DeliveryResponse struct {
	StatusCode int
	Body       []byte
}

// DeliverySender posts exact persisted request bytes to one authenticated
// peer. Production uses the built-in mTLS sender; tests may inject a sender.
type DeliverySender interface {
	Send(context.Context, registry.Peer, []byte) (DeliveryResponse, error)
}

// ReplicatorOptions supplies durable state and the authenticated transport.
type ReplicatorOptions struct {
	DB                     *sql.DB
	Registry               *registry.Registry
	Identity               *NodeTLS
	TrustBundle            *TrustBundle
	Clock                  clock.Clock
	Random                 io.Reader
	LeaseOwner             string
	Lease                  time.Duration
	MaxAge                 time.Duration
	Sender                 DeliverySender
	AckSender              DeliverySender
	ArtifactManifestSender DeliverySender
	ArtifactChunkSender    DeliverySender
	ArtifactCompleteSender DeliverySender
	Alert                  func(ReplicatorAlert)
}

// Replicator claims and sends one durable federation request at a time.
type Replicator struct {
	db                     *sql.DB
	registry               *registry.Registry
	clock                  clock.Clock
	random                 io.Reader
	owner                  string
	lease                  time.Duration
	maxAge                 time.Duration
	deliverySender         DeliverySender
	ackSender              DeliverySender
	artifactManifestSender DeliverySender
	artifactChunkSender    DeliverySender
	artifactCompleteSender DeliverySender
	alert                  func(ReplicatorAlert)
	breakers               peerCircuitBreaker

	randomMu sync.Mutex
	claimID  atomic.Uint64
}

type claimedDelivery struct {
	outboxID      string
	objectType    string
	objectID      string
	messageID     protocol.ULID
	destination   protocol.NodeID
	attempts      int64
	createdAt     time.Time
	expiresAt     *time.Time
	request       []byte
	requestSHA256 protocol.SHA256Hex
	sequence      int64
	nonce         []byte
	leaseOwner    string
}

type replicationSenders struct {
	delivery         DeliverySender
	ack              DeliverySender
	artifactManifest DeliverySender
	artifactChunk    DeliverySender
	artifactComplete DeliverySender
}

// NewReplicator validates the durable queue and configures the mTLS sender
// when no test sender is supplied.
func NewReplicator(options ReplicatorOptions) (*Replicator, error) {
	if options.DB == nil || options.Registry == nil || options.Clock == nil ||
		strings.TrimSpace(options.LeaseOwner) == "" {
		return nil, fmt.Errorf("new federation replicator: incomplete options")
	}
	lease := options.Lease
	if lease == 0 {
		lease = defaultDeliveryLease
	}
	maxAge := options.MaxAge
	if maxAge == 0 {
		maxAge = defaultDeliveryMaxAge
	}
	if lease <= 0 || maxAge <= 0 {
		return nil, fmt.Errorf("new federation replicator: invalid durations")
	}
	randomSource := options.Random
	if randomSource == nil {
		randomSource = rand.Reader
	}
	senders, err := resolveReplicationSenders(options)
	if err != nil {
		return nil, err
	}
	return &Replicator{
		db: options.DB, registry: options.Registry, clock: options.Clock,
		random: randomSource, owner: options.LeaseOwner, lease: lease, maxAge: maxAge,
		deliverySender: senders.delivery, ackSender: senders.ack, alert: options.Alert,
		artifactManifestSender: senders.artifactManifest,
		artifactChunkSender:    senders.artifactChunk,
		artifactCompleteSender: senders.artifactComplete,
	}, nil
}

func resolveReplicationSenders(options ReplicatorOptions) (replicationSenders, error) {
	senders := replicationSenders{
		delivery: options.Sender, ack: options.AckSender,
		artifactManifest: options.ArtifactManifestSender,
		artifactChunk:    options.ArtifactChunkSender, artifactComplete: options.ArtifactCompleteSender,
	}
	if senders.ack == nil {
		senders.ack = senders.delivery
	}
	if senders.artifactManifest == nil {
		senders.artifactManifest = senders.delivery
	}
	if senders.artifactChunk == nil {
		senders.artifactChunk = senders.delivery
	}
	if senders.artifactComplete == nil {
		senders.artifactComplete = senders.delivery
	}
	if senders.complete() {
		return senders, nil
	}
	if options.Identity == nil || options.TrustBundle == nil {
		return replicationSenders{}, fmt.Errorf("new federation replicator: incomplete mTLS options")
	}
	if senders.delivery == nil {
		senders.delivery = newMTLSRequestSender(options, "/v1/deliver")
	}
	if senders.ack == nil {
		senders.ack = newMTLSRequestSender(options, "/v1/ack")
	}
	if senders.artifactManifest == nil {
		senders.artifactManifest = newMTLSRequestSender(options, "/v1/artifacts/manifest")
	}
	if senders.artifactChunk == nil {
		senders.artifactChunk = newMTLSRequestSender(options, "/v1/artifacts/chunks")
	}
	if senders.artifactComplete == nil {
		senders.artifactComplete = newMTLSRequestSender(options, "/v1/artifacts/complete")
	}
	return senders, nil
}

func (s replicationSenders) complete() bool {
	return s.delivery != nil && s.ack != nil && s.artifactManifest != nil &&
		s.artifactChunk != nil && s.artifactComplete != nil
}

func newMTLSRequestSender(options ReplicatorOptions, path string) DeliverySender {
	return &mtlsRequestSender{
		db: options.DB, identity: options.Identity, bundle: options.TrustBundle,
		clock: options.Clock, path: path,
	}
}

// ReplicateOnce claims at most one due request. The bool reports whether a
// row was claimed, including terminalization without an HTTP attempt.
func (r *Replicator) ReplicateOnce(ctx context.Context) (bool, error) {
	claimed, found, err := r.claim(ctx)
	if err != nil {
		return false, err
	}
	if !found {
		return r.replicateArtifactOnce(ctx)
	}
	now := r.clock.Now().UTC()
	if claimed.expiresAt != nil && !now.Before(*claimed.expiresAt) {
		return true, r.finish(ctx, claimed, outboxExpired, messageExpired, "EXPIRED")
	}
	if !now.Before(claimed.createdAt.Add(r.maxAge)) {
		messageState := ""
		if claimed.objectType == federationObjectMessage {
			messageState = messageFailedPermanent
		}
		return true, r.finish(ctx, claimed, outboxDeadLetter, messageState, "MAX_AGE")
	}
	allowed, probe, retryAt := r.breakers.allow(claimed.destination, now)
	if !allowed {
		if !retryAt.After(now) {
			retryAt = now.Add(r.lease)
		}
		return true, r.deferOpenCircuit(ctx, claimed, retryAt)
	}
	peer, err := r.registry.Peer(ctx, r.db, claimed.destination)
	if err != nil || peer.Status != registry.PeerStatusTrusted {
		r.breakers.neutral(claimed.destination, probe)
		return true, r.retry(ctx, claimed, "PEER_NOT_TRUSTED", time.Time{})
	}
	sender := r.deliverySender
	if claimed.objectType == federationObjectAck {
		sender = r.ackSender
	}
	response, sendErr := sender.Send(ctx, peer, claimed.request)
	switch response.StatusCode {
	case http.StatusBadRequest, http.StatusConflict, http.StatusRequestEntityTooLarge:
		r.breakers.succeeded(claimed.destination)
		messageState := ""
		if claimed.objectType == federationObjectMessage {
			messageState = messageFailedPermanent
		}
		return true, r.finish(ctx, claimed, outboxDeadLetter, messageState,
			fmt.Sprintf("HTTP_%d", response.StatusCode))
	}
	if sendErr != nil {
		return true, r.retryPeerFailure(ctx, claimed, "TRANSPORT", probe)
	}
	if claimed.objectType == federationObjectAck {
		return true, r.handleAckResponse(ctx, claimed, response, probe)
	}
	switch {
	case response.StatusCode == http.StatusOK:
		receipt, err := r.decodeAndVerifyReceipt(ctx, claimed, response.Body)
		if err != nil || receipt.Status != DeliveryReceiptStoredRemote {
			r.emit("INVALID_RECEIPT")
			return true, r.retryPeerFailure(ctx, claimed, "INVALID_RECEIPT", probe)
		}
		r.breakers.succeeded(claimed.destination)
		return true, r.saveReceipt(ctx, claimed, receipt, response.Body)
	case response.StatusCode == http.StatusForbidden:
		receipt, err := DecodeDeliveryReceipt(response.Body)
		if err != nil {
			return true, r.retryPeerFailure(ctx, claimed, "HTTP_403", probe)
		}
		if err := r.verifyDecodedReceipt(ctx, claimed, receipt); err != nil ||
			receipt.Status != DeliveryReceiptControlDenied {
			r.emit("INVALID_RECEIPT")
			return true, r.retryPeerFailure(ctx, claimed, "INVALID_RECEIPT", probe)
		}
		r.breakers.succeeded(claimed.destination)
		return true, r.saveReceipt(ctx, claimed, receipt, response.Body)
	case response.StatusCode >= 500:
		return true, r.retryPeerFailure(ctx, claimed, "HTTP_5XX", probe)
	default:
		return true, r.retryPeerFailure(ctx, claimed, "HTTP_RESPONSE", probe)
	}
}

func (r *Replicator) claim(ctx context.Context) (claimedDelivery, bool, error) {
	now := r.clock.Now().UTC()
	nowText := eventstore.FormatTime(now)
	leaseUntil := eventstore.FormatTime(now.Add(r.lease))
	leaseOwner := fmt.Sprintf("%s:%d", r.owner, r.claimID.Add(1))
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return claimedDelivery{}, false, fmt.Errorf("begin federation claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `WITH flow_requests
		(outbox_id, object_type, request, request_sha256, sequence, nonce) AS (
			SELECT outbox_id, 'message', request, request_sha256, sequence, nonce
			FROM federation_outbox_requests
			UNION ALL
			SELECT outbox_id, 'ack', request, request_sha256, sequence, nonce
			FROM federation_outbox_acks
		)
		UPDATE federation_outbox
		SET state = 'REPLICATING', attempts = attempts + 1, next_attempt_at = NULL,
			lease_owner = ?, lease_until = ?, updated_at = ?
		WHERE outbox_id = (
			SELECT candidate.outbox_id
			FROM federation_outbox AS candidate
			JOIN flow_requests AS request ON request.outbox_id = candidate.outbox_id
				AND request.object_type = candidate.object_type
			JOIN nodes AS peer ON peer.node_id = candidate.destination_node
			WHERE candidate.object_type IN ('message', 'ack')
			AND peer.local = 0 AND peer.status = 'TRUSTED'
			AND (((candidate.state = 'QUEUED' OR candidate.state = 'FAILED_RETRYABLE')
				AND (candidate.next_attempt_at IS NULL OR candidate.next_attempt_at <= ?))
				OR (candidate.state = 'REPLICATING' AND candidate.lease_until IS NOT NULL
					AND candidate.lease_until <= ?))
			AND NOT EXISTS (
				SELECT 1 FROM flow_requests AS prior_request
				JOIN federation_outbox AS prior ON prior.outbox_id = prior_request.outbox_id
				WHERE prior.destination_node = candidate.destination_node
				AND prior.object_type = candidate.object_type
				AND prior_request.object_type = request.object_type
				AND prior_request.sequence < request.sequence
				AND prior.state NOT IN ('STORED_REMOTE', 'CONTROL_DENIED', 'DEAD_LETTER', 'EXPIRED')
			)
			ORDER BY COALESCE(candidate.next_attempt_at, candidate.created_at), candidate.created_at,
				request.sequence, candidate.outbox_id
			LIMIT 1
		)
		AND (((state = 'QUEUED' OR state = 'FAILED_RETRYABLE')
			AND (next_attempt_at IS NULL OR next_attempt_at <= ?))
			OR (state = 'REPLICATING' AND lease_until IS NOT NULL AND lease_until <= ?))`,
		leaseOwner, leaseUntil, nowText, nowText, nowText, nowText, nowText)
	if err != nil {
		return claimedDelivery{}, false, fmt.Errorf("claim federation outbox: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return claimedDelivery{}, false, fmt.Errorf("count federation claim: %w", err)
	}
	if affected == 0 {
		if err := tx.Commit(); err != nil {
			return claimedDelivery{}, false, fmt.Errorf("commit empty federation claim: %w", err)
		}
		return claimedDelivery{}, false, nil
	}
	if affected != 1 {
		return claimedDelivery{}, false, fmt.Errorf("federation claim changed %d rows", affected)
	}
	var claimed claimedDelivery
	var createdAt string
	var expiresAt sql.NullString
	claimed.leaseOwner = leaseOwner
	err = tx.QueryRowContext(ctx, `WITH flow_requests
		(outbox_id, object_type, request, request_sha256, sequence, nonce) AS (
			SELECT outbox_id, 'message', request, request_sha256, sequence, nonce
			FROM federation_outbox_requests
			UNION ALL
			SELECT outbox_id, 'ack', request, request_sha256, sequence, nonce
			FROM federation_outbox_acks
		)
		SELECT outbox.outbox_id, outbox.object_type, outbox.object_id,
		outbox.destination_node, outbox.attempts, outbox.created_at,
		request.request, request.request_sha256, request.sequence, request.nonce,
		message.expires_at
		FROM federation_outbox AS outbox
		JOIN flow_requests AS request ON request.outbox_id = outbox.outbox_id
			AND request.object_type = outbox.object_type
		LEFT JOIN messages AS message ON outbox.object_type = 'message'
			AND message.message_id = outbox.object_id
		WHERE outbox.state = 'REPLICATING' AND outbox.lease_owner = ?`, leaseOwner).Scan(
		&claimed.outboxID, &claimed.objectType, &claimed.objectID,
		&claimed.destination, &claimed.attempts,
		&createdAt, &claimed.request, &claimed.requestSHA256, &claimed.sequence,
		&claimed.nonce, &expiresAt)
	if err != nil {
		return claimedDelivery{}, false, fmt.Errorf("read federation claim: %w", err)
	}
	claimed.createdAt, err = time.Parse(eventstore.TimeLayout, createdAt)
	if err != nil {
		return claimedDelivery{}, false, fmt.Errorf("parse federation outbox creation: %w", err)
	}
	switch claimed.objectType {
	case federationObjectMessage:
		claimed.messageID = protocol.ULID(claimed.objectID)
		if !claimed.messageID.Valid() || !expiresAt.Valid {
			return claimedDelivery{}, false, fmt.Errorf("federation delivery claim has invalid message binding")
		}
		parsed, parseErr := time.Parse(eventstore.TimeLayout, expiresAt.String)
		if parseErr != nil {
			return claimedDelivery{}, false, fmt.Errorf("parse federation message expiry: %w", parseErr)
		}
		claimed.expiresAt = &parsed
	case federationObjectAck:
		if expiresAt.Valid {
			return claimedDelivery{}, false, fmt.Errorf("federation acknowledgement claim has message expiry")
		}
	default:
		return claimedDelivery{}, false, fmt.Errorf("federation claim has invalid object type")
	}
	if err := tx.Commit(); err != nil {
		return claimedDelivery{}, false, fmt.Errorf("commit federation claim: %w", err)
	}
	return claimed, true, nil
}

func (r *Replicator) decodeAndVerifyReceipt(
	ctx context.Context,
	claimed claimedDelivery,
	raw []byte,
) (DeliveryReceipt, error) {
	receipt, err := DecodeDeliveryReceipt(raw)
	if err != nil {
		return DeliveryReceipt{}, err
	}
	if err := r.verifyDecodedReceipt(ctx, claimed, receipt); err != nil {
		return DeliveryReceipt{}, err
	}
	return receipt, nil
}

func (r *Replicator) verifyDecodedReceipt(
	ctx context.Context,
	claimed claimedDelivery,
	receipt DeliveryReceipt,
) error {
	publicKey, err := r.registry.PeerSigningPublicKey(ctx, r.db, claimed.destination)
	if err != nil {
		return fmt.Errorf("read receipt signing key: %w", err)
	}
	if err := receipt.Verify(publicKey); err != nil {
		return err
	}
	request, err := DecodeDeliveryRequest(claimed.request)
	if err != nil {
		return fmt.Errorf("decode stored delivery request: %w", err)
	}
	canonical, err := request.CanonicalBytes()
	if err != nil || !bytes.Equal(canonical, claimed.request) {
		return fmt.Errorf("stored delivery request is not canonical")
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil || requestSHA != claimed.requestSHA256 {
		return fmt.Errorf("stored delivery request digest mismatch")
	}
	envelope, _, err := request.DecodeEnvelope()
	if err != nil {
		return err
	}
	envelopeSHA, err := request.EnvelopeSHA256()
	if err != nil {
		return err
	}
	if request.Sequence != claimed.sequence || !bytes.Equal(request.Nonce, claimed.nonce) ||
		receipt.MessageID != claimed.messageID || receipt.MessageID != envelope.MessageID ||
		receipt.OriginNode != envelope.OriginNode || receipt.DestinationNode != claimed.destination ||
		receipt.DestinationNode != envelope.DestinationNode ||
		receipt.DeliveryGeneration != request.DeliveryGeneration ||
		!bytes.Equal(receipt.Nonce, request.Nonce) || receipt.Sequence != request.Sequence ||
		receipt.RequestSHA256 != requestSHA || receipt.EnvelopeSHA256 != envelopeSHA ||
		len(receipt.RecipientAgents) != len(request.Recipients) {
		return fmt.Errorf("delivery receipt does not match stored request")
	}
	for i, recipient := range request.Recipients {
		if receipt.RecipientAgents[i] != recipient.Agent {
			return fmt.Errorf("delivery receipt recipients do not match stored request")
		}
	}
	return nil
}

func (r *Replicator) handleAckResponse(
	ctx context.Context,
	claimed claimedDelivery,
	response DeliveryResponse,
	probe bool,
) error {
	switch {
	case response.StatusCode == http.StatusOK:
		receipt, err := DecodeAckReceipt(response.Body)
		if err != nil || r.verifyAckReceipt(ctx, claimed, receipt) != nil {
			r.emit("INVALID_ACK_RECEIPT")
			return r.retryPeerFailure(ctx, claimed, "INVALID_ACK_RECEIPT", probe)
		}
		r.breakers.succeeded(claimed.destination)
		return r.saveAckReceipt(ctx, claimed, receipt, response.Body)
	case response.StatusCode == http.StatusForbidden:
		return r.retryPeerFailure(ctx, claimed, "HTTP_403", probe)
	case response.StatusCode >= 500:
		return r.retryPeerFailure(ctx, claimed, "HTTP_5XX", probe)
	default:
		return r.retryPeerFailure(ctx, claimed, "HTTP_RESPONSE", probe)
	}
}

func (r *Replicator) verifyAckReceipt(
	ctx context.Context,
	claimed claimedDelivery,
	receipt AckReceipt,
) error {
	publicKey, err := r.registry.PeerSigningPublicKey(ctx, r.db, claimed.destination)
	if err != nil {
		return fmt.Errorf("read acknowledgement receipt signing key: %w", err)
	}
	if err := receipt.Verify(publicKey); err != nil {
		return err
	}
	request, err := DecodeAckRequest(claimed.request)
	if err != nil {
		return fmt.Errorf("decode stored federation acknowledgement: %w", err)
	}
	canonical, err := request.CanonicalBytes()
	if err != nil || !bytes.Equal(canonical, claimed.request) {
		return fmt.Errorf("stored federation acknowledgement is not canonical")
	}
	requestSHA, err := request.RequestSHA256()
	if err != nil || requestSHA != claimed.requestSHA256 {
		return fmt.Errorf("stored federation acknowledgement digest mismatch")
	}
	if request.Sequence != claimed.sequence || !bytes.Equal(request.Nonce, claimed.nonce) ||
		receipt.MessageID != request.MessageID || receipt.Agent != request.Agent ||
		receipt.DeliveryGeneration != request.DeliveryGeneration || receipt.Status != request.Status ||
		receipt.OriginNode != claimed.destination ||
		receipt.DestinationNode != r.registry.NodeID() ||
		receipt.RequestSHA256 != requestSHA || receipt.Sequence != request.Sequence ||
		!bytes.Equal(receipt.Nonce, request.Nonce) {
		return fmt.Errorf("federation acknowledgement receipt does not match stored request")
	}
	return nil
}

func (r *Replicator) saveAckReceipt(
	ctx context.Context,
	claimed claimedDelivery,
	receipt AckReceipt,
	exact []byte,
) error {
	now := r.clock.Now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin federation acknowledgement receipt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_ack_receipts
		(outbox_id, receipt, received_at) VALUES (?, ?, ?)`, claimed.outboxID,
		exact, eventstore.FormatTime(receipt.ReceivedAt.UTC())); err != nil {
		return fmt.Errorf("insert federation acknowledgement receipt: %w", err)
	}
	if err := updateClaimedOutbox(ctx, tx, claimed, outboxStoredRemote, nil, nil, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit federation acknowledgement receipt: %w", err)
	}
	return nil
}

func (r *Replicator) saveReceipt(
	ctx context.Context,
	claimed claimedDelivery,
	receipt DeliveryReceipt,
	exact []byte,
) error {
	outboxState := outboxStoredRemote
	messageState := messageStoredRemote
	if receipt.Status == DeliveryReceiptControlDenied {
		outboxState = outboxControlDenied
		messageState = messageFailedPermanent
	}
	now := r.clock.Now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin federation receipt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO federation_receipts
		(outbox_id, receipt, state, received_at) VALUES (?, ?, ?, ?)`,
		claimed.outboxID, exact, receipt.Status, eventstore.FormatTime(receipt.ReceivedAt.UTC())); err != nil {
		return fmt.Errorf("insert federation receipt: %w", err)
	}
	if err := updateClaimedOutbox(ctx, tx, claimed, outboxState, nil, nil, now); err != nil {
		return err
	}
	if err := updateRemoteMessage(ctx, tx, claimed.messageID, messageState, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit federation receipt: %w", err)
	}
	return nil
}

func (r *Replicator) retryPeerFailure(
	ctx context.Context,
	claimed claimedDelivery,
	code string,
	probe bool,
) error {
	retryAt, opened := r.breakers.failed(claimed.destination, r.clock.Now().UTC(), probe)
	if opened {
		r.emit("PEER_CIRCUIT_OPEN")
	}
	return r.retry(ctx, claimed, code, retryAt)
}

func (r *Replicator) retry(
	ctx context.Context,
	claimed claimedDelivery,
	code string,
	peerRetryAt time.Time,
) error {
	now := r.clock.Now().UTC()
	if claimed.expiresAt != nil && !now.Before(*claimed.expiresAt) {
		return r.finish(ctx, claimed, outboxExpired, messageExpired, "EXPIRED")
	}
	if !now.Before(claimed.createdAt.Add(r.maxAge)) {
		messageState := ""
		if claimed.objectType == federationObjectMessage {
			messageState = messageFailedPermanent
		}
		return r.finish(ctx, claimed, outboxDeadLetter, messageState, "MAX_AGE")
	}
	next := now.Add(r.backoff(claimed.attempts))
	if peerRetryAt.After(next) {
		next = peerRetryAt
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin federation retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateClaimedOutbox(ctx, tx, claimed, outboxFailedRetryable, &code, &next, now); err != nil {
		return err
	}
	if peerRetryAt.After(now) {
		if err := deferPeerOutbox(ctx, tx, claimed.destination, peerRetryAt, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit federation retry: %w", err)
	}
	return nil
}

func (r *Replicator) deferOpenCircuit(
	ctx context.Context,
	claimed claimedDelivery,
	retryAt time.Time,
) error {
	now := r.clock.Now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin federation circuit deferral: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `UPDATE federation_outbox
		SET state = 'FAILED_RETRYABLE', attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
			last_error = ?, next_attempt_at = ?, lease_owner = NULL, lease_until = NULL,
			updated_at = ?
		WHERE outbox_id = ? AND state = 'REPLICATING' AND lease_owner = ?`,
		peerCircuitOpenError, eventstore.FormatTime(retryAt.UTC()), eventstore.FormatTime(now),
		claimed.outboxID, claimed.leaseOwner)
	if err != nil {
		return fmt.Errorf("defer federation claim for open circuit: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count open circuit deferral: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("open circuit deferral changed %d rows", affected)
	}
	if err := deferPeerOutbox(ctx, tx, claimed.destination, retryAt, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit federation circuit deferral: %w", err)
	}
	return nil
}

func deferPeerOutbox(
	ctx context.Context,
	tx *sql.Tx,
	destination protocol.NodeID,
	retryAt time.Time,
	now time.Time,
) error {
	retryText := eventstore.FormatTime(retryAt.UTC())
	if _, err := tx.ExecContext(ctx, `UPDATE federation_outbox
		SET state = 'FAILED_RETRYABLE',
			last_error = CASE WHEN last_error IS NULL THEN ? ELSE last_error END,
			next_attempt_at = CASE
				WHEN next_attempt_at IS NULL OR next_attempt_at < ? THEN ?
				ELSE next_attempt_at END,
			updated_at = ?
		WHERE destination_node = ? AND state IN ('QUEUED', 'FAILED_RETRYABLE')`,
		peerCircuitOpenError, retryText, retryText, eventstore.FormatTime(now.UTC()),
		destination); err != nil {
		return fmt.Errorf("defer federation peer for open circuit: %w", err)
	}
	return nil
}

func (r *Replicator) finish(
	ctx context.Context,
	claimed claimedDelivery,
	outboxState string,
	messageState string,
	code string,
) error {
	now := r.clock.Now().UTC()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin terminal federation delivery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := updateClaimedOutbox(ctx, tx, claimed, outboxState, &code, nil, now); err != nil {
		return err
	}
	if messageState != "" {
		if claimed.objectType != federationObjectMessage {
			return fmt.Errorf("federation acknowledgement cannot update message state")
		}
		if err := updateRemoteMessage(ctx, tx, claimed.messageID, messageState, now); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit terminal federation delivery: %w", err)
	}
	return nil
}

func updateClaimedOutbox(
	ctx context.Context,
	tx *sql.Tx,
	claimed claimedDelivery,
	state string,
	lastError *string,
	nextAttempt *time.Time,
	now time.Time,
) error {
	var last, next any
	if lastError != nil {
		last = *lastError
	}
	if nextAttempt != nil {
		next = eventstore.FormatTime(nextAttempt.UTC())
	}
	result, err := tx.ExecContext(ctx, `UPDATE federation_outbox
		SET state = ?, last_error = ?, next_attempt_at = ?, lease_owner = NULL,
			lease_until = NULL, updated_at = ?
		WHERE outbox_id = ? AND state = 'REPLICATING' AND lease_owner = ?`,
		state, last, next, eventstore.FormatTime(now.UTC()), claimed.outboxID, claimed.leaseOwner)
	if err != nil {
		return fmt.Errorf("update claimed federation outbox: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count claimed federation outbox update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("claimed federation outbox update changed %d rows", affected)
	}
	return nil
}

func updateRemoteMessage(
	ctx context.Context,
	tx *sql.Tx,
	messageID protocol.ULID,
	state string,
	now time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE messages SET state = ?, updated_at = ?
		WHERE message_id = ? AND state = 'QUEUED_REMOTE'`,
		state, eventstore.FormatTime(now.UTC()), messageID)
	if err != nil {
		return fmt.Errorf("update remote message: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count remote message update: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("remote message update changed %d rows", affected)
	}
	return nil
}

func (r *Replicator) backoff(attempt int64) time.Duration {
	full := time.Second
	for current := int64(1); current < attempt && full < 5*time.Minute; current++ {
		full *= 2
		if full > 5*time.Minute {
			full = 5 * time.Minute
		}
	}
	half := full / 2
	span := full - half
	r.randomMu.Lock()
	offset, err := rand.Int(r.random, big.NewInt(int64(span)+1))
	r.randomMu.Unlock()
	if err != nil {
		r.emit("RANDOM_UNAVAILABLE")
		return full
	}
	return half + time.Duration(offset.Int64())
}

func (r *Replicator) emit(code string) {
	if r.alert != nil {
		r.alert(ReplicatorAlert{Code: code})
	}
}

type mtlsRequestSender struct {
	db       Querier
	identity *NodeTLS
	bundle   *TrustBundle
	clock    clock.Clock
	path     string
}

func (s *mtlsRequestSender) Send(
	ctx context.Context,
	peer registry.Peer,
	exact []byte,
) (DeliveryResponse, error) {
	target, err := federationRequestURL(peer.Endpoint, s.path)
	if err != nil {
		return DeliveryResponse{}, err
	}
	clientTLS, err := NewClientTLSConfig(ctx, ClientOptions{
		Identity: s.identity, TrustBundle: s.bundle, DB: s.db,
		TargetNode: peer.Node, Clock: s.clock,
	})
	if err != nil {
		return DeliveryResponse{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target,
		io.NopCloser(bytes.NewReader(exact)))
	if err != nil {
		return DeliveryResponse{}, fmt.Errorf("build federation request: %w", err)
	}
	request.ContentLength = int64(len(exact))
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: clientTLS, DisableKeepAlives: true},
		Timeout:   deliveryRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return DeliveryResponse{}, fmt.Errorf("post federation request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxReceiptBody+1))
	answer := DeliveryResponse{StatusCode: response.StatusCode}
	if readErr != nil {
		return answer, fmt.Errorf("read federation response: %w", readErr)
	}
	if len(body) > maxReceiptBody {
		return answer, fmt.Errorf("federation response exceeds %d bytes", maxReceiptBody)
	}
	answer.Body = body
	return answer, nil
}

func federationRequestURL(raw, path string) (string, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != federationHTTPScheme || parsed.Host == "" || parsed.User != nil ||
		parsed.Fragment != "" || parsed.RawQuery != "" ||
		(path != "/v1/deliver" && path != "/v1/ack" &&
			path != "/v1/artifacts/manifest" && path != "/v1/artifacts/chunks" &&
			path != "/v1/artifacts/complete") {
		return "", fmt.Errorf("invalid peer federation endpoint: %w", registry.ErrInvalidArgument)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + path
	return parsed.String(), nil
}
