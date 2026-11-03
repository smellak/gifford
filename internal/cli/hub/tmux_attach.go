package hub

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"time"

	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/procguard"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy/tmuxattach"
	"github.com/smellak/gifford/internal/tmuxinstance"
	"github.com/smellak/gifford/internal/tmuxlock"
)

const tmuxReconcileEvery = time.Second

type tmuxAttachNode struct {
	options      *orchestrator.TmuxAttachOptions
	reconciler   *tmuxattach.Reconciler
	bridgeNotice protocol.NoticeLine
	paneAuth     *controlapi.PaneOperationAuthorizer
}

type tmuxPaneLockReporter struct {
	registry *tmuxinstance.Registry
}

func (r *tmuxPaneLockReporter) ReportPaneLockIncident(
	ctx context.Context,
	binding tmuxlock.LiveBinding,
	reason tmuxlock.GuardReason,
) error {
	if r == nil || r.registry == nil {
		return fmt.Errorf("tmux pane-lock incident registry is unavailable")
	}
	if err := r.registry.ReportPaneLockIncident(ctx, binding, reason); err != nil {
		return fmt.Errorf("invalidate pane-lock registry binding: %w", err)
	}
	return nil
}

func newTmuxAttachNode(
	ctx context.Context,
	db *sql.DB,
	nodeID protocol.NodeID,
	clk agentlink.Clock,
	cfg config.Config,
	configDir string,
	errW io.Writer,
) (*tmuxAttachNode, error) {
	bridgeNotice := protocol.NoticeLine(cfg.Bridge.NoticeLine)
	if bridgeNotice == "" {
		bridgeNotice = tmuxattach.DefaultBridgeNoticeLine
	}
	reporter := &tmuxPaneLockReporter{}
	var locker *tmuxattach.PanelLocker
	var paneAuth *controlapi.PaneOperationAuthorizer
	var err error
	host := string(nodeID)
	cooperative := cfg.TmuxLocks.Enabled
	if cooperative {
		if err := tmuxlock.ValidateProtocolMode(cfg.TmuxLocks.ModePath); err != nil {
			return nil, fmt.Errorf("validate tmux pane-lock mode: %w", err)
		}
		locker, err = tmuxattach.NewCooperativePanelLocker(
			cfg.TmuxLocks.ModePath,
			tmuxlock.RecordPolicy{
				PeerOwner: cfg.TmuxLocks.PeerOwner, PeerWriters: cfg.TmuxLocks.PeerWriters,
			},
			0, reporter,
		)
		if err != nil {
			return nil, fmt.Errorf("new tmux panel locker: %w", err)
		}
		paneAuth, err = controlapi.NewPaneOperationAuthorizer(cfg.TmuxLocks.AdminUIDsPath)
		if err != nil {
			return nil, fmt.Errorf("new pane operation authorizer: %w", err)
		}
		host = cfg.TmuxLocks.Host
	} else {
		locker, err = tmuxattach.NewPanelLocker(0)
	}
	if err != nil {
		return nil, fmt.Errorf("new tmux panel locker: %w", err)
	}
	transportOptions := tmuxattach.Options{
		Locker: locker,
		AllowedNotices: []protocol.NoticeLine{
			tmuxattach.FixedNoticeLine,
			bridgeNotice,
		},
		HelperRevision: cfg.TmuxAttach.HelperManifestSHA256,
		BridgeNotice:   bridgeNotice,
	}
	if cooperative {
		transportOptions.Lifecycle = locker
	}
	transport, err := tmuxattach.New(transportOptions)
	if err != nil {
		return nil, fmt.Errorf("new tmux attachment transport: %w", err)
	}
	nodeUID := os.Getuid()
	if nodeUID < 0 {
		return nil, fmt.Errorf("new tmux instance registry: node uid is unavailable")
	}
	nodeUID32 := uint32(nodeUID) //nolint:gosec // kernel uid_t cannot overflow uint32
	instances, err := tmuxinstance.New(tmuxinstance.Options{
		DB: db, Clock: clk, Panes: tmuxPanelLister{
			transport: transport, lookupUID: lookupOwnerUID,
		},
		ExcludedAgents: nil,
		ReservedUIDs:   tmuxReservedUIDs(nodeUID32, cfg.NodeSocket),
	})
	if err != nil {
		return nil, fmt.Errorf("new tmux instance registry: %w", err)
	}
	reporter.registry = instances
	sessions, err := tmuxattach.NewSessionStore(tmuxattach.SessionStoreOptions{
		DB: db, NodeID: nodeID, Clock: clk,
	})
	if err != nil {
		return nil, fmt.Errorf("new tmux session store: %w", err)
	}
	statementsPath := cfg.TmuxInstances.StatementsPath(configDir)
	reconciler, err := tmuxattach.NewReconciler(instances, sessions,
		func() ([]tmuxinstance.Registration, error) {
			return tmuxinstance.LoadStatements(statementsPath)
		})
	if err != nil {
		return nil, fmt.Errorf("new tmux lifecycle reconciler: %w", err)
	}
	report, err := reconciler.Reconcile(ctx)
	logTmuxReconcileReport(errW, report)
	if err != nil {
		return nil, fmt.Errorf("reconcile tmux attachments at startup: %w", err)
	}
	inputGuards, err := agentlink.NewInputGuardStore(db, clk)
	if err != nil {
		return nil, fmt.Errorf("new tmux input guard store: %w", err)
	}
	return &tmuxAttachNode{
		options: &orchestrator.TmuxAttachOptions{
			Host: host, Registry: instances, Transport: transport, Sessions: sessions,
			InputGuards: inputGuards, ProcessGuard: &procguard.Guard{},
			DetectorTable:       detect.DefaultTable(),
			HumanActivityWindow: cfg.TmuxAttach.HumanActivityWindow,
		},
		reconciler: reconciler, bridgeNotice: bridgeNotice, paneAuth: paneAuth,
	}, nil
}

type paneTransport interface {
	ListPanes(context.Context, tmuxattach.Server) ([]tmuxattach.PaneObservation, error)
}

type tmuxPanelLister struct {
	transport paneTransport
	lookupUID func(string) (uint32, error)
}

func (l tmuxPanelLister) ListPanes(
	ctx context.Context,
	owner string,
	socket string,
	bank string,
) ([]tmuxinstance.PaneRow, error) {
	if l.transport == nil || l.lookupUID == nil {
		return nil, tmuxattach.ErrInvalidOptions
	}
	uid, err := l.lookupUID(owner)
	if err != nil {
		return nil, fmt.Errorf("lookup tmux owner: %w", err)
	}
	observations, err := l.transport.ListPanes(ctx, tmuxattach.Server{
		OwnerUser: owner, OwnerUID: uid, Socket: socket, Bank: bank,
	})
	if err != nil {
		return nil, fmt.Errorf("list tmux panes: %w", err)
	}
	rows := make([]tmuxinstance.PaneRow, 0, len(observations))
	for _, observation := range observations {
		rows = append(rows, tmuxinstance.PaneRow{
			PaneID: observation.PaneID, PanePID: observation.PanePID,
		})
	}
	return rows, nil
}

func lookupOwnerUID(name string) (uint32, error) {
	account, err := user.Lookup(name)
	if err != nil {
		return 0, fmt.Errorf("lookup operating-system user: %w", err)
	}
	uid, err := strconv.ParseUint(account.Uid, 10, 32)
	if err != nil || uid == 0 {
		return 0, tmuxattach.ErrInvalidOptions
	}
	return uint32(uid), nil
}

func tmuxReservedUIDs(nodeUID uint32, nodeSocket *config.NodeSocket) []uint32 {
	reserved := []uint32{nodeUID}
	if nodeSocket != nil && nodeSocket.BridgeUID != nodeUID {
		reserved = append(reserved, nodeSocket.BridgeUID)
	}
	return reserved
}

func logTmuxReconcileReport(errW io.Writer, report tmuxattach.ReconcileReport) {
	if report.StatementError != nil {
		sayf(errW, "load tmux instance statements: %v", report.StatementError)
	}
	for _, refusal := range report.Refused {
		sayf(errW, "tmux instance statement refused: agent=%s code=%s",
			refusal.AgentID, refusal.Code)
	}
}

type tmuxLifecycleReconciler interface {
	Reconcile(context.Context) (tmuxattach.ReconcileReport, error)
}

func startTmuxReconcileLoop(
	parent context.Context,
	errW io.Writer,
	reconciler tmuxLifecycleReconciler,
	clk agentlink.Clock,
) *ownedWorkerLoop {
	ctx, cancel := context.WithCancel(parent)
	loop := &ownedWorkerLoop{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(loop.done)
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-clk.After(tmuxReconcileEvery):
			}
			report, err := reconciler.Reconcile(ctx)
			if ctx.Err() == nil {
				logTmuxReconcileReport(errW, report)
				if err != nil {
					sayf(errW, "reconcile tmux attachments: %v", err)
				}
			}
		}
	}()
	return loop
}
