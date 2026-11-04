package hub

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/modelprofile"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	// modelReactivationEvery is how often the loop checks for a due probe;
	// the probe backoff itself lives in modelprofile.
	modelReactivationEvery = 5 * time.Second
	// modelNoticeLifetime bounds how long a notice waits for an operator
	// that cannot take it.
	modelNoticeLifetime = time.Hour
)

type modelReactivator interface {
	ReactivateOnce(context.Context) (bool, error)
}

type modelNoticeFiler interface {
	FileModelNotice(context.Context, protocol.AgentID, hubsrv.ModelNotice) error
}

type queuedModelNotice struct {
	operator protocol.AgentID
	notice   hubsrv.ModelNotice
	queued   time.Time
}

// modelNoticeQueue holds model transition notices until each operator takes
// them. Notify only queues, so the gateway's fallback never waits on a
// message write.
type modelNoticeQueue struct {
	operators []protocol.AgentID
	filer     modelNoticeFiler
	clk       agentlink.Clock

	mu      sync.Mutex
	pending []queuedModelNotice
}

func (q *modelNoticeQueue) notify(_ context.Context, notice modelprofile.Notice) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, operator := range q.operators {
		q.pending = append(q.pending, queuedModelNotice{
			operator: operator, queued: q.clk.Now(),
			notice: hubsrv.ModelNotice{
				Reactivated: notice.Kind == modelprofile.NoticeReactivated,
				Generation:  notice.Generation, AdapterName: notice.AdapterName,
				AdapterRevision: notice.AdapterRevision, Reason: notice.Reason,
			},
		})
	}
}

// flush files every queued notice once. A notice an operator cannot take now
// stays queued until modelNoticeLifetime; any other error drops it.
func (q *modelNoticeQueue) flush(ctx context.Context, errW io.Writer) {
	q.mu.Lock()
	pending := q.pending
	q.pending = nil
	q.mu.Unlock()
	var keep []queuedModelNotice
	for _, item := range pending {
		err := q.filer.FileModelNotice(ctx, item.operator, item.notice)
		switch {
		case err == nil:
		case errors.Is(err, hubsrv.ErrOperatorNotDeliverable) && q.clk.Now().Sub(item.queued) < modelNoticeLifetime:
			keep = append(keep, item)
		default:
			sayf(errW, "model notice to %s: %v", item.operator, err)
		}
	}
	q.mu.Lock()
	q.pending = append(keep, q.pending...)
	q.mu.Unlock()
}

type ownedModelReactivationLoop struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// startModelReactivationLoop reactivates an adapter the gateway fell back
// from, and files the operator notices. Without model control it does nothing.
func startModelReactivationLoop(
	parent context.Context,
	errW io.Writer,
	control modelReactivator,
	notices *modelNoticeQueue,
	clk agentlink.Clock,
) *ownedModelReactivationLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedModelReactivationLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		if control == nil {
			return
		}
		for ctx.Err() == nil {
			if _, err := control.ReactivateOnce(ctx); err != nil && ctx.Err() == nil {
				sayf(errW, "model reactivation: %v", err)
			}
			if notices != nil {
				notices.flush(ctx, errW)
			}
			select {
			case <-ctx.Done():
				return
			case <-clk.After(modelReactivationEvery):
			}
		}
	}()
	return loop
}

func (l *ownedModelReactivationLoop) stop() {
	l.cancel()
	<-l.done
}
