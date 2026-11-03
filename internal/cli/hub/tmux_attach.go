package hub

import (
	"context"
	"database/sql"
	"errors"
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
	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

const tmuxReconcileEvery = time.Second

type tmuxAttachNode struct {
	options                *orchestrator.TmuxAttachOptions
	reconciler             *tmuxattach.Reconciler
	bridgeNotice           protocol.NoticeLine
	responseRequiredNotice protocol.NoticeLine
	paneAuth               controlapi.PaneRequestAuthorizer
	paneCreate             controlapi.PaneProvisionService
	paneOwner              controlapi.PaneOwnershipService
	paneRecovery           controlapi.PaneRecoveryService
	paneLog                *controlapi.PaneOperationTelemetry
}

type paneLockRuntimeAuthority struct {
	root       string
	modeUID    uint32
	modeGID    uint32
	authorizer func(string) (controlapi.PaneRequestAuthorizer, error)
}

// paneLockRuntime is the production authority contract. Tests in this package
// replace it with a temporary root and current-process ownership; no config
// key can alter these values.
var paneLockRuntime = paneLockRuntimeAuthority{
	root: tmuxlock.DefaultRoot,
	authorizer: func(path string) (controlapi.PaneRequestAuthorizer, error) {
		return controlapi.NewPaneOperationAuthorizer(path)
	},
}

func (a paneLockRuntimeAuthority) production() bool {
	return a.root == tmuxlock.DefaultRoot && a.modeUID == 0 && a.modeGID == 0
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

func newCooperativePaneLock(
	db *sql.DB,
	clk agentlink.Clock,
	settings config.TmuxLocks,
	authority paneLockRuntimeAuthority,
	reporter *tmuxPaneLockReporter,
	telemetry *controlapi.PaneOperationTelemetry,
) (*tmuxattach.PanelLocker, controlapi.PaneRequestAuthorizer, *tmuxlock.NodeWriter, controlapi.PaneProvisionService, controlapi.PaneOwnershipService, error) {
	if authority.root == "" || authority.authorizer == nil {
		return nil, nil, nil, nil, nil, errors.New("validate tmux pane-lock authority")
	}
	var modeErr error
	if authority.production() {
		modeErr = tmuxlock.ValidateProtocolMode(settings.ModePath)
	} else {
		modeErr = tmuxlock.ValidateProtocolModeAt(
			settings.ModePath, authority.root, authority.modeUID, authority.modeGID,
		)
	}
	if modeErr != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("validate tmux pane-lock mode: %w", modeErr)
	}
	policy := tmuxlock.RecordPolicy{
		PeerOwner: settings.PeerOwner, PeerWriters: settings.PeerWriters,
	}
	var locker *tmuxattach.PanelLocker
	var err error
	if authority.production() {
		locker, err = tmuxattach.NewCooperativePanelLocker(settings.ModePath, policy, 0, reporter)
	} else {
		locker, err = tmuxattach.NewCooperativePanelLockerAt(
			settings.ModePath, authority.root, authority.modeUID, authority.modeGID, policy,
			0, reporter,
		)
	}
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new tmux panel locker: %w", err)
	}
	authorizer, err := authority.authorizer(settings.AdminUIDsPath)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new pane operation authorizer: %w", err)
	}
	writer, err := tmuxlock.NewNodeWriter(tmuxlock.NodeWriterOptions{
		Root: authority.root,
		Host: settings.Host, PeerOwner: settings.PeerOwner,
		PeerWriters: settings.PeerWriters, ModePath: settings.ModePath,
		ModeUID: authority.modeUID, ModeGID: authority.modeGID,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new pane ownership writer: %w", err)
	}
	provisioner, err := newTmuxPaneProvisionService(
		authority.root, db, clk, policy, authorizer, telemetry,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new pane provision service: %w", err)
	}
	owner, err := newTmuxPaneOwnershipService(
		writer, db, settings.InitialTakeDrain, clk, telemetry,
	)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("new pane ownership service: %w", err)
	}
	return locker, authorizer, writer, provisioner, owner, nil
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
	responseRequiredNotice := protocol.NoticeLine(cfg.Bridge.ResponseRequiredNoticeLine)
	if responseRequiredNotice == "" {
		responseRequiredNotice = tmuxattach.DefaultResponseRequiredNoticeLine
	}
	reporter := &tmuxPaneLockReporter{}
	paneLog := controlapi.NewPaneOperationTelemetry(nil)
	var locker *tmuxattach.PanelLocker
	var paneAuth controlapi.PaneRequestAuthorizer
	var paneWriter *tmuxlock.NodeWriter
	var paneCreate controlapi.PaneProvisionService
	var paneOwner controlapi.PaneOwnershipService
	var paneWriterLog *writerlog.Client
	var paneLifecycle tmuxattach.PaneLifecycle
	var paneRecovery controlapi.PaneRecoveryService
	var err error
	host := string(nodeID)
	cooperative := cfg.TmuxLocks.Enabled
	if cooperative {
		paneWriterLog, err = writerlog.New(cfg.TmuxLocks.WriterLogSocket)
		if err != nil {
			return nil, fmt.Errorf("new pane writer log: %w", err)
		}
		locker, paneAuth, paneWriter, paneCreate, paneOwner, err = newCooperativePaneLock(
			db, clk, cfg.TmuxLocks, paneLockRuntime, reporter, paneLog,
		)
		host = cfg.TmuxLocks.Host
	} else {
		locker, err = tmuxattach.NewPanelLocker(0)
	}
	if err != nil {
		return nil, fmt.Errorf("new tmux panel locker: %w", err)
	}
	if cooperative {
		policy := tmuxlock.RecordPolicy{
			PeerOwner:   cfg.TmuxLocks.PeerOwner,
			PeerWriters: cfg.TmuxLocks.PeerWriters,
		}
		var journal *tmuxattach.PaneJournal
		var journalErr error
		if paneLockRuntime.production() {
			journal, journalErr = tmuxattach.NewPaneJournal(cfg.TmuxLocks.OperationSchema, policy)
		} else {
			journal, journalErr = tmuxattach.NewPaneJournalAt(
				cfg.TmuxLocks.OperationSchema, policy, paneLockRuntime.root,
			)
		}
		if journalErr != nil {
			return nil, fmt.Errorf("new pane attempt journal: %w", journalErr)
		}
		paneLifecycle, err = tmuxattach.NewWriterLoggedLifecycle(
			locker, journal, paneWriterLog,
		)
		if err != nil {
			return nil, fmt.Errorf("new pane writer lifecycle: %w", err)
		}
	}
	transportOptions := tmuxattach.Options{
		Locker: locker,
		AllowedNotices: []protocol.NoticeLine{
			tmuxattach.FixedNoticeLine,
			bridgeNotice,
			responseRequiredNotice,
		},
		HelperRevision: cfg.TmuxAttach.HelperManifestSHA256,
		BridgeNotice:   bridgeNotice,
	}
	if cooperative {
		transportOptions.Lifecycle = paneLifecycle
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
	detectorTable := detect.DefaultTable()
	if cooperative {
		paneRecovery, err = newTmuxPaneRecoveryService(paneRecoveryServiceOptions{
			Writer: paneWriter, Registry: instances, Transport: transport,
			Clock: clk, Drain: cfg.TmuxLocks.InitialTakeDrain, Telemetry: paneLog,
			DetectorTable: detectorTable,
			Notices: []protocol.NoticeLine{
				tmuxattach.FixedNoticeLine,
				bridgeNotice,
				responseRequiredNotice,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("new pane recovery service: %w", err)
		}
	}
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
	options := &orchestrator.TmuxAttachOptions{
		Host: host, Registry: instances, Transport: transport, Sessions: sessions,
		InputGuards: inputGuards, ProcessGuard: &procguard.Guard{},
		DetectorTable:       detectorTable,
		HumanActivityWindow: cfg.TmuxAttach.HumanActivityWindow,
	}
	// The analyst route projects its tmux observations under the deploy's
	// ML policies, the same projection the dataset and analyst use. It is
	// wired only when state analysis is enabled; the deterministic path
	// keeps starting without any ML configuration.
	if cfg.StateAnalysis != nil {
		projection, err := cfg.ML.ProjectionPolicies()
		if err != nil {
			return nil, fmt.Errorf("load tmux analyst projection: %w", err)
		}
		options.Projection = projection
	}
	return &tmuxAttachNode{
		options: options, reconciler: reconciler,
		bridgeNotice: bridgeNotice, responseRequiredNotice: responseRequiredNotice,
		paneAuth: paneAuth, paneCreate: paneCreate, paneOwner: paneOwner,
		paneRecovery: paneRecovery, paneLog: paneLog,
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
