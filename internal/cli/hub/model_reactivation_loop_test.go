package hub

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/modelprofile"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	opA       protocol.AgentID = "op-a"
	opOffline protocol.AgentID = "op-offline"
)

type noticeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *noticeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *noticeClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func (c *noticeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type recordingModelFiler struct {
	errs  map[protocol.AgentID]error
	filed []protocol.AgentID
	seen  []hubsrv.ModelNotice
}

func (r *recordingModelFiler) FileModelNotice(_ context.Context, operator protocol.AgentID, notice hubsrv.ModelNotice) error {
	if err := r.errs[operator]; err != nil {
		return err
	}
	r.filed = append(r.filed, operator)
	r.seen = append(r.seen, notice)
	return nil
}

func TestModelNoticesReachEveryOperatorAndWaitForAnOfflineOne(t *testing.T) {
	clk := &noticeClock{now: time.Date(2026, time.November, 4, 15, 0, 0, 0, time.UTC)}
	filer := &recordingModelFiler{errs: map[protocol.AgentID]error{
		opOffline:   hubsrv.ErrOperatorNotDeliverable,
		"op-broken": errors.New("store failed"),
	}}
	queue := &modelNoticeQueue{operators: []protocol.AgentID{opA, opOffline, "op-broken"}, filer: filer, clk: clk}
	queue.notify(t.Context(), modelprofile.Notice{
		Kind: modelprofile.NoticeReactivated, Generation: 4,
		AdapterName: "analyst", AdapterRevision: "rev-1", Reason: "automatic reactivation after fallback",
	})
	queue.flush(t.Context(), io.Discard)
	require.Equal(t, []protocol.AgentID{opA}, filer.filed)
	require.Equal(t, hubsrv.ModelNotice{
		Reactivated: true, Generation: 4, AdapterName: "analyst", AdapterRevision: "rev-1",
		Reason: "automatic reactivation after fallback",
	}, filer.seen[0])
	require.Len(t, queue.pending, 1, "only the offline operator's notice waits")

	delete(filer.errs, opOffline)
	queue.flush(t.Context(), io.Discard)
	require.Equal(t, []protocol.AgentID{opA, opOffline}, filer.filed)
	require.Empty(t, queue.pending)

	filer.errs[opA] = hubsrv.ErrOperatorNotDeliverable
	queue.notify(t.Context(), modelprofile.Notice{Kind: modelprofile.NoticeFallback, Generation: 5})
	clk.advance(modelNoticeLifetime)
	queue.flush(t.Context(), io.Discard)
	require.Empty(t, queue.pending, "a notice nobody could take for an hour is dropped")
}
