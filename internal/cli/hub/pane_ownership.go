package hub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
)

var errPaneOwnershipConfig = errors.New("invalid pane ownership service configuration")

type paneOwnershipWriter interface {
	Take(context.Context, tmuxlock.LiveBinding) (tmuxlock.OwnershipResult, error)
	Return(context.Context, tmuxlock.LiveBinding) (tmuxlock.OwnershipResult, error)
}

type paneOwnershipClock interface {
	Now() time.Time
}

type tmuxPaneOwnershipService struct {
	writer    paneOwnershipWriter
	db        *sql.DB
	drain     time.Duration
	clock     paneOwnershipClock
	telemetry *controlapi.PaneOperationTelemetry
}

func newTmuxPaneOwnershipService(
	writer paneOwnershipWriter,
	db *sql.DB,
	drain time.Duration,
	clock paneOwnershipClock,
	telemetry *controlapi.PaneOperationTelemetry,
) (*tmuxPaneOwnershipService, error) {
	if writer == nil || db == nil || drain <= 0 || clock == nil || telemetry == nil {
		return nil, errPaneOwnershipConfig
	}
	return &tmuxPaneOwnershipService{
		writer: writer, db: db, drain: drain, clock: clock, telemetry: telemetry,
	}, nil
}

func (s *tmuxPaneOwnershipService) Take(
	ctx context.Context,
	params protocol.PaneOperationParams,
	actor controlapi.PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	return s.operate(ctx, controlapi.PaneAdminTake, params, actor)
}

func (s *tmuxPaneOwnershipService) Return(
	ctx context.Context,
	params protocol.PaneOperationParams,
	actor controlapi.PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	return s.operate(ctx, controlapi.PaneAdminReturn, params, actor)
}

func (s *tmuxPaneOwnershipService) operate(
	ctx context.Context,
	operation controlapi.PaneAdminOperation,
	params protocol.PaneOperationParams,
	actor controlapi.PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	if s == nil || ctx == nil || actor.PID <= 0 || params.Validate() != nil {
		return protocol.PaneOperationResult{}, errPaneOwnershipConfig
	}
	event := controlapi.PaneOperationTelemetryEvent{
		Operation: operation, Actor: actor, Target: params.Target,
		Attempt: "-", Result: controlapi.PaneLogFailed,
	}
	logCtx := context.WithoutCancel(ctx)
	live := paneLiveBinding(params.Target)
	if operation == controlapi.PaneAdminTake {
		if err := s.requireTakeDrain(ctx, params.Target); err != nil {
			return protocol.PaneOperationResult{}, errors.Join(err, s.telemetry.Record(logCtx, event))
		}
	}

	var ownership tmuxlock.OwnershipResult
	var err error
	switch operation {
	case controlapi.PaneAdminTake:
		ownership, err = s.writer.Take(ctx, live)
	case controlapi.PaneAdminReturn:
		ownership, err = s.writer.Return(ctx, live)
	default:
		return protocol.PaneOperationResult{}, errPaneOwnershipConfig
	}
	if err == nil {
		event.Result = controlapi.PaneLogUnchanged
		if ownership.Changed {
			event.Result = controlapi.PaneLogChanged
		}
	}
	logErr := s.telemetry.Record(logCtx, event)
	if err != nil {
		return protocol.PaneOperationResult{}, errors.Join(err, logErr)
	}
	if logErr != nil {
		return protocol.PaneOperationResult{}, fmt.Errorf("record pane ownership telemetry: %w", logErr)
	}
	return protocol.PaneOperationResult{Changed: ownership.Changed}, nil
}

func (s *tmuxPaneOwnershipService) requireTakeDrain(
	ctx context.Context,
	target protocol.PaneOperationTarget,
) error {
	paths, err := tmuxlock.DerivePanelPaths(tmuxlock.PanelBinding{
		OwnerUID: target.OwnerUID, Socket: target.Socket, PaneID: string(target.PaneID),
	})
	if err != nil {
		return fmt.Errorf("derive pane publication key: %w", err)
	}
	prefix := paths.Root + "/"
	if !strings.HasPrefix(paths.Lock, prefix) {
		return errPaneOwnershipConfig
	}
	panelKey := strings.TrimPrefix(paths.Lock, prefix)
	var publishedText string
	err = s.db.QueryRowContext(ctx, `SELECT published_at
		FROM pane_index_publications
		WHERE panel_key = ?
		ORDER BY id ASC
		LIMIT 1`, panelKey).Scan(&publishedText)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("pane has no durable index publication: %w", tmuxlock.ErrWriterDeferred)
	}
	if err != nil {
		return fmt.Errorf("read pane index publication: %w", err)
	}
	publishedAt, err := eventstore.ParseTime(publishedText)
	if err != nil {
		return fmt.Errorf("parse pane index publication: %w", err)
	}
	if s.clock.Now().Before(publishedAt.Add(s.drain)) {
		return fmt.Errorf("pane initial take drain has not elapsed: %w", tmuxlock.ErrWriterDeferred)
	}
	return nil
}

func paneLiveBinding(target protocol.PaneOperationTarget) tmuxlock.LiveBinding {
	return tmuxlock.LiveBinding{
		PanelBinding: tmuxlock.PanelBinding{
			OwnerUID: target.OwnerUID, Socket: target.Socket, PaneID: string(target.PaneID),
		},
		Host: target.Host, BootID: target.BootID,
		PanePID: target.PanePID, PaneStart: target.PaneStart,
	}
}
