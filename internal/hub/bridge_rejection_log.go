package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	bridgeRejectionEvent  = "bridge_method_rejected"
	bridgeRejectionWindow = time.Minute
)

type bridgeRejectionKey struct {
	method    string
	code      string
	recipient protocol.AgentID
}

type bridgeRejectionBucket struct {
	lastLogged time.Time
	pending    uint64
}

// bridgeRejectionLog rate-limits bridge refusals in memory. Restarting the
// node deliberately loses its buckets; they are telemetry, not durable state.
type bridgeRejectionLog struct {
	mu      sync.Mutex
	logger  *slog.Logger
	now     func() time.Time
	buckets map[bridgeRejectionKey]bridgeRejectionBucket
}

func newBridgeRejectionLog(logger *slog.Logger, now func() time.Time) *bridgeRejectionLog {
	if logger == nil {
		logger = slog.Default()
	}
	if now == nil {
		now = time.Now
	}
	return &bridgeRejectionLog{
		logger: logger, now: now,
		buckets: make(map[bridgeRejectionKey]bridgeRejectionBucket),
	}
}

// Record writes at most one line per method, code and recipient per minute.
// It extracts only the two bounded identity fields needed for diagnosis and
// never records raw parameters, error text or message content.
func (l *bridgeRejectionLog) Record(ctx context.Context, req rpcRequest, response rpcResponse) {
	if l == nil || l.logger == nil || l.now == nil || response.Error == nil || req.Method == "" {
		return
	}
	metadata := bridgeLogMetadataFrom(req.Params)
	code := bridgeLogErrorCode(response.Error)
	key := bridgeRejectionKey{method: req.Method, code: code, recipient: metadata.recipient}
	now := l.now()

	l.mu.Lock()
	bucket := l.buckets[key]
	bucket.pending++
	if !bucket.lastLogged.IsZero() && now.Sub(bucket.lastLogged) < bridgeRejectionWindow {
		l.buckets[key] = bucket
		l.mu.Unlock()
		return
	}
	count := bucket.pending
	bucket.lastLogged = now
	bucket.pending = 0
	l.buckets[key] = bucket
	l.mu.Unlock()

	attrs := []slog.Attr{
		slog.String("event", bridgeRejectionEvent),
		slog.String("method", req.Method),
		slog.String("code", code),
		slog.Int("rpc_code", response.Error.Code),
		slog.Uint64("rejections", count),
	}
	if metadata.recipient != "" {
		attrs = append(attrs, slog.String("recipient_agent", string(metadata.recipient)))
	}
	if metadata.sourceMessageID != "" {
		attrs = append(attrs, slog.String("source_message_id", metadata.sourceMessageID))
	}
	l.logger.LogAttrs(ctx, slog.LevelWarn, "Bridge method rejected", attrs...)
}

type bridgeLogMetadata struct {
	recipient       protocol.AgentID
	sourceMessageID string
}

func bridgeLogMetadataFrom(raw json.RawMessage) bridgeLogMetadata {
	var params struct {
		ToAgent         protocol.AgentID `json:"to_agent"`
		RecipientAgent  protocol.AgentID `json:"recipient_agent"`
		SourceMessageID string           `json:"source_message_id"`
	}
	if json.Unmarshal(raw, &params) != nil {
		return bridgeLogMetadata{}
	}
	metadata := bridgeLogMetadata{}
	if params.ToAgent.Valid() {
		metadata.recipient = params.ToAgent
	} else if params.RecipientAgent.Valid() {
		metadata.recipient = params.RecipientAgent
	}
	if _, err := canonicalBridgeMessageID(params.SourceMessageID); err == nil {
		metadata.sourceMessageID = params.SourceMessageID
	}
	return metadata
}

func bridgeLogErrorCode(failure *rpcError) string {
	if failure.Data != nil && failure.Data.ErrorCode != "" {
		return string(failure.Data.ErrorCode)
	}
	return strconv.Itoa(failure.Code)
}
