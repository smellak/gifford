package hub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxlock"
)

var errPaneProvisionConfig = errors.New("invalid pane provision service configuration")

type paneProvisionFunc func(tmuxlock.ProvisionSpec) (tmuxlock.ProvisionResult, error)

type panePublicationStore interface {
	Record(context.Context, *sql.Tx, string, time.Time) error
}

type tmuxPaneProvisionAuthorizer struct {
	policy controlapi.PaneRequestAuthorizer
}

func (a tmuxPaneProvisionAuthorizer) AuthorizePane(peer tmuxlock.PanePeer) error {
	if a.policy == nil {
		return controlapi.ErrPaneOperationPolicyInvalid
	}
	if err := a.policy.Authorize(controlapi.Peer{
		PID: peer.PID, UID: peer.UID, GID: peer.GID, Verified: peer.Verified,
	}); err != nil {
		return fmt.Errorf("authorize pane provision: %w", err)
	}
	return nil
}

type tmuxPaneProvisionService struct {
	root         string
	db           *sql.DB
	clock        clock.Clock
	policy       tmuxlock.RecordPolicy
	authorizer   tmuxlock.PaneAuthorizer
	publications panePublicationStore
	telemetry    *controlapi.PaneOperationTelemetry
	provision    paneProvisionFunc
}

func newTmuxPaneProvisionService(
	root string,
	db *sql.DB,
	clk clock.Clock,
	policy tmuxlock.RecordPolicy,
	authorizer controlapi.PaneRequestAuthorizer,
	telemetry *controlapi.PaneOperationTelemetry,
) (*tmuxPaneProvisionService, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root ||
		db == nil || clk == nil || policy.Validate() != nil || authorizer == nil || telemetry == nil {
		return nil, errPaneProvisionConfig
	}
	return &tmuxPaneProvisionService{
		root: root, db: db, clock: clk, policy: policy,
		authorizer:   tmuxPaneProvisionAuthorizer{policy: authorizer},
		publications: daemon.IndexPublicationStore{}, telemetry: telemetry,
		provision: tmuxlock.Provision,
	}, nil
}

func (s *tmuxPaneProvisionService) Provision(
	ctx context.Context,
	params protocol.PaneOperationParams,
	actor controlapi.PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	if s == nil || ctx == nil || actor.PID <= 0 || params.Validate() != nil {
		return protocol.PaneOperationResult{}, errPaneProvisionConfig
	}
	target := params.Target
	event := controlapi.PaneOperationTelemetryEvent{
		Operation: controlapi.PaneAdminProvision,
		Actor:     actor, Target: target, Attempt: "-", Result: controlapi.PaneLogFailed,
	}
	logCtx := context.WithoutCancel(ctx)
	result, err := s.provision(tmuxlock.ProvisionSpec{
		Root: s.root,
		Binding: tmuxlock.PanelBinding{
			OwnerUID: target.OwnerUID, Socket: target.Socket, PaneID: string(target.PaneID),
		},
		PanePID: target.PanePID, PaneStart: target.PaneStart,
		BootID: target.BootID, Host: target.Host, BindUID: target.OwnerUID,
		Owner: s.policy.PeerOwner, Policy: s.policy, Clock: s.clock,
		Peer: tmuxlock.PanePeer{
			PID: actor.PID, UID: actor.UID, GID: actor.GID, Verified: true,
			Actor: actor.Actor.ID,
		},
		Authorizer: s.authorizer,
	})
	if err == nil {
		err = s.recordPublication(ctx, result)
	}
	if err == nil {
		event.Result = controlapi.PaneLogUnchanged
		if result.Created {
			event.Result = controlapi.PaneLogChanged
		}
	}
	logErr := s.telemetry.Record(logCtx, event)
	if err != nil {
		return protocol.PaneOperationResult{}, errors.Join(err, logErr)
	}
	if logErr != nil {
		return protocol.PaneOperationResult{}, fmt.Errorf("record pane provision telemetry: %w", logErr)
	}
	return protocol.PaneOperationResult{Changed: result.Created}, nil
}

func (s *tmuxPaneProvisionService) recordPublication(
	ctx context.Context,
	result tmuxlock.ProvisionResult,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pane index publication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.publications.Record(
		ctx, tx, result.Publication.PanelKey, result.Publication.PublishedAt,
	); err != nil {
		return fmt.Errorf("record pane index publication: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pane index publication: %w", err)
	}
	return nil
}
