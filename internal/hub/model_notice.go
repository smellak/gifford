package hub

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// modelNoticeThread groups every automatic model transition notice.
const modelNoticeThread protocol.ThreadID = "glimmer-model-notice"

// ModelNotice is one automatic Glimmer model transition an operator is told
// about: the adapter fell back to the base model, or it was reactivated.
// Every field is control-plane metadata; no model input or output is read.
type ModelNotice struct {
	Reactivated     bool
	Generation      int64
	AdapterName     string
	AdapterRevision string
	Reason          string
}

// Text renders the only subject and body shape a model notice has.
func (n ModelNotice) Text() (subject, body string) {
	subject, status := "Glimmer adapter fell back to the base model", "fell back to the base model"
	if n.Reactivated {
		subject, status = "Glimmer adapter reactivated", "is selected again"
	}
	body = fmt.Sprintf("The Glimmer adapter %s.\nAdapter: %s@%s\nGeneration: %d\nReason: %s\n"+
		"This operator notice carries no message content. No response is needed.",
		status, n.AdapterName, n.AdapterRevision, n.Generation, n.Reason)
	return subject, body
}

// FileModelNotice files one metadata-only model transition notice to an
// operator through the signed system-sender path. The key names the selection
// generation, so a retry converges on the filed message.
func (s *Service) FileModelNotice(ctx context.Context, operator protocol.AgentID, notice ModelNotice) error {
	if !operator.Valid() || notice.Generation <= 0 {
		return fmt.Errorf("file model notice: %w", errInvalidArgument)
	}
	key := protocol.OpaqueID("model-notice:" + strconv.FormatInt(notice.Generation, 10) + ":" + string(operator))
	if !key.Valid() {
		return fmt.Errorf("file model notice: %w", errInvalidArgument)
	}
	// The key holds the operator and generation: a filed notice is final.
	if _, found, err := lookupPriorNotice(ctx, s.db, s.nodeID, key); err != nil || found {
		return err
	}
	now := s.clock.Now().UTC()
	subject, body := notice.Text()
	return s.fileSystemNoticeAttempt(ctx, operator, key, modelNoticeThread,
		protocol.PriorityNormal, now, now.Add(24*time.Hour),
		func(protocol.ThreadRef) (string, string) { return subject, body })
}
