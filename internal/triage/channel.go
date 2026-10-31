package triage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/smellak/gifford/internal/protocol"
)

// Previewer evaluates one queued delivery against the current controls,
// read through the caller's transaction. *channel.Engine satisfies it with
// PreviewQueuedDelivery, which mints no Decision and changes no control,
// queue, approval or loop row.
type Previewer interface {
	PreviewQueuedDelivery(
		ctx context.Context,
		tx *sql.Tx,
		messageID protocol.ULID,
		recipient protocol.AgentID,
		generation int64,
	) (bool, error)
}

// NewChannel is the Channel over the channel engine's read-only preview. It
// asks about exactly the delivery the prefilter names, through the
// prefilter's own transaction, so the answer is the controls of now and not
// the triple stored when the message was queued. A nil previewer is refused.
func NewChannel(p Previewer) (Channel, error) {
	if p == nil {
		return nil, ErrInvalidConfig
	}
	return previewChannel{p: p}, nil
}

type previewChannel struct{ p Previewer }

func (c previewChannel) AllowsNow(ctx context.Context, tx *sql.Tx, d Delivery) (bool, error) {
	allowed, err := c.p.PreviewQueuedDelivery(ctx, tx, d.MessageID, d.AgentID, d.DeliveryGeneration)
	if err != nil {
		return false, fmt.Errorf("preview delivery channel: %w", err)
	}
	return allowed, nil
}
