package hub

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
)

const paneOwnershipTestHost = "node-1"

type paneOwnershipWriterStub struct {
	takes   []tmuxlock.LiveBinding
	returns []tmuxlock.LiveBinding
	result  tmuxlock.OwnershipResult
	err     error
}

func (w *paneOwnershipWriterStub) Take(
	_ context.Context,
	binding tmuxlock.LiveBinding,
) (tmuxlock.OwnershipResult, error) {
	w.takes = append(w.takes, binding)
	return w.result, w.err
}

func (w *paneOwnershipWriterStub) Return(
	_ context.Context,
	binding tmuxlock.LiveBinding,
) (tmuxlock.OwnershipResult, error) {
	w.returns = append(w.returns, binding)
	return w.result, w.err
}

type paneOwnershipTestClock struct{ now time.Time }

func (c *paneOwnershipTestClock) Now() time.Time { return c.now }

func TestPaneOwnershipServiceEnforcesInitialTakeDrainAndLogsResult(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "node.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, err = store.DB().ExecContext(t.Context(), `CREATE TABLE IF NOT EXISTS pane_index_publications (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		panel_key TEXT NOT NULL,
		published_at TEXT NOT NULL
	)`)
	require.NoError(t, err)

	publication := time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)
	clock := &paneOwnershipTestClock{now: publication.Add(29 * time.Second)}
	writer := &paneOwnershipWriterStub{result: tmuxlock.OwnershipResult{Changed: true}}
	var logs bytes.Buffer
	telemetry := controlapi.NewPaneOperationTelemetry(
		slog.New(slog.NewTextHandler(&logs, nil)),
	)
	service, err := newTmuxPaneOwnershipService(
		writer, store.DB(), 30*time.Second, clock, telemetry,
	)
	require.NoError(t, err)
	target := paneOwnershipTarget()
	params := protocol.PaneOperationParams{Target: target}
	actor := controlapi.PaneOperationActor{UID: 42, PID: 700}

	_, err = service.Take(t.Context(), params, actor)
	require.ErrorIs(t, err, tmuxlock.ErrWriterDeferred)
	require.Empty(t, writer.takes)

	paths, err := tmuxlock.DerivePanelPaths(tmuxlock.PanelBinding{
		OwnerUID: target.OwnerUID, Socket: target.Socket, PaneID: string(target.PaneID),
	})
	require.NoError(t, err)
	panelKey := strings.TrimPrefix(paths.Lock, paths.Root+"/")
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO pane_index_publications
		(panel_key, published_at) VALUES (?, ?)`, panelKey, eventstore.FormatTime(publication))
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO pane_index_publications
		(panel_key, published_at) VALUES (?, ?)`, panelKey,
		eventstore.FormatTime(publication.Add(time.Minute)))
	require.NoError(t, err)

	_, err = service.Take(t.Context(), params, actor)
	require.ErrorIs(t, err, tmuxlock.ErrWriterDeferred)
	require.Empty(t, writer.takes)

	clock.now = publication.Add(30 * time.Second)
	result, err := service.Take(t.Context(), params, actor)
	require.NoError(t, err)
	require.Equal(t, protocol.PaneOperationResult{Changed: true}, result)
	require.Equal(t, []tmuxlock.LiveBinding{paneLiveBinding(target)}, writer.takes)
	require.Equal(t, 2, strings.Count(logs.String(), "result=failed"))
	require.Equal(t, 1, strings.Count(logs.String(), "result=changed"))
	require.NotContains(t, logs.String(), target.Socket)
}

func TestPaneOwnershipServiceReturnDoesNotUseTakeDrain(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "node.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	writer := &paneOwnershipWriterStub{}
	clock := &paneOwnershipTestClock{now: time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)}
	var logs bytes.Buffer
	service, err := newTmuxPaneOwnershipService(
		writer, store.DB(), time.Second, clock,
		controlapi.NewPaneOperationTelemetry(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	require.NoError(t, err)
	target := paneOwnershipTarget()

	result, err := service.Return(t.Context(), protocol.PaneOperationParams{Target: target},
		controlapi.PaneOperationActor{UID: 42, PID: 700})
	require.NoError(t, err)
	require.Equal(t, protocol.PaneOperationResult{}, result)
	require.Equal(t, []tmuxlock.LiveBinding{paneLiveBinding(target)}, writer.returns)
	require.Contains(t, logs.String(), "op=return")
	require.Contains(t, logs.String(), "result=unchanged")
}

func TestPaneOwnershipServiceLogsWriterFailure(t *testing.T) {
	t.Parallel()

	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "node.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	sentinel := errors.New("writer failed")
	writer := &paneOwnershipWriterStub{err: sentinel}
	clock := &paneOwnershipTestClock{now: time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC)}
	var logs bytes.Buffer
	service, err := newTmuxPaneOwnershipService(
		writer, store.DB(), time.Second, clock,
		controlapi.NewPaneOperationTelemetry(slog.New(slog.NewTextHandler(&logs, nil))),
	)
	require.NoError(t, err)

	_, err = service.Return(t.Context(), protocol.PaneOperationParams{Target: paneOwnershipTarget()},
		controlapi.PaneOperationActor{UID: 42, PID: 700})
	require.ErrorIs(t, err, sentinel)
	require.Contains(t, logs.String(), "result=failed")
}

func paneOwnershipTarget() protocol.PaneOperationTarget {
	return protocol.PaneOperationTarget{
		OwnerUID: 1000, Socket: "/run/tmux/default", PaneID: "%7", Host: paneOwnershipTestHost,
		BootID: "12345678-1234-1234-1234-123456789abc", PanePID: 2345, PaneStart: 987654,
	}
}
