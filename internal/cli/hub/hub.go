// Package hub implements the `gifford hub` subcommand: the foreground
// node process. It opens the event store, the channel engine and
// controller, the registry and the hub service, then serves hub.sock and
// daemon.sock from the runtime directory until Ctrl-C or SIGTERM. It is
// the only process writing gifford.db; control commands arrive over
// daemon.sock.
package hub

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/policy"
	"github.com/smellak/gifford/internal/registry"
	"github.com/smellak/gifford/internal/xdg"
)

const (
	// expireEvery is how often the hub sweeps admitted reservations and the
	// controller sweeps expired controls.
	expireEvery = 5 * time.Second

	// directorySyncEvery is how often an enrolled hub refreshes the signed
	// key directories of its trusted peers after the initial startup pass.
	directorySyncEvery = 60 * time.Second
)

// controlSocketTimeout bounds the wait for daemon.sock after its server
// starts: a bind failure reports through its own error instead.
const controlSocketTimeout = 5 * time.Second

// dbName is the event store file inside the data directory. The hub is
// its only writer.
const dbName = "gifford.db"

// NewCmd returns the gifford hub command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hub",
		Short: "Run the local hub and control plane in the foreground",
		Long: `Start the node hub in the foreground: stop it with Ctrl-C or SIGTERM.

It serves hub.sock and daemon.sock from the runtime directory, sweeps
expired admissions and controls every 5 s, and prints one ready line on
stderr. A second hub on the same directories refuses without touching
the first. config.yaml must carry a valid node_id or the command fails
before creating anything.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := run(cmd.Context(), cmd.ErrOrStderr()); err != nil {
				return &deps.ExitError{
					Code: exitcode.Error,
					Err:  fmt.Errorf("gifford hub: %w", err),
				}
			}
			return nil
		},
	}
	return cmd
}

// run starts the hub and blocks until it stops. Every setup failure
// returns before the sockets serve.
func run(ctx context.Context, errW io.Writer) error {
	dirs, err := xdg.Resolve()
	if err != nil {
		return fmt.Errorf("resolve dirs: %w", err)
	}
	cfg, err := config.Load(config.DefaultPath(dirs.Config))
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if !cfg.NodeID.Valid() {
		return fmt.Errorf("config %s carries no valid node_id", config.DefaultPath(dirs.Config))
	}
	compiledPolicy, err := policy.Load(filepath.Join(dirs.Config, "policies"))
	if err != nil {
		return fmt.Errorf("load federation policy: %w", err)
	}
	if _, err := xdg.EnsureAll(dirs); err != nil {
		return fmt.Errorf("ensure dirs: %w", err)
	}

	// The singleton claim precedes the event store: a second hub
	// refuses here, before gifford.db is created or migrated.
	lock, err := acquireRuntimeLock(dirs.Runtime)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := eventstore.Open(ctx, filepath.Join(dirs.Data, dbName))
	if err != nil {
		return fmt.Errorf("open event store: %w", err)
	}
	defer func() { _ = store.Close() }()

	clk := clock.System{}
	engine, err := channel.New(channel.Options{Clock: clk})
	if err != nil {
		return fmt.Errorf("new channel engine: %w", err)
	}
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: cfg.NodeID, Events: nil,
	})
	if err != nil {
		return fmt.Errorf("new channel controller: %w", err)
	}
	if err := controller.Reload(ctx); err != nil {
		return fmt.Errorf("reload controls: %w", err)
	}
	reg, err := registry.New(cfg.NodeID, dirs.Data, clk)
	if err != nil {
		return fmt.Errorf("new registry: %w", err)
	}
	if err := reg.EnsureLocalNode(ctx, store.DB(), cfg.OrganizationID); err != nil {
		return fmt.Errorf("ensure local node: %w", err)
	}
	if err := persistPolicy(ctx, store.DB(), reg, compiledPolicy, clk.Now()); err != nil {
		return fmt.Errorf("persist federation policy: %w", err)
	}
	service, err := hubsrv.NewService(hubsrv.ServiceOptions{
		DB: store.DB(), NodeID: cfg.NodeID, Engine: engine, Registry: reg, Clock: clk,
		FederationRegistry: reg, Policy: compiledPolicy,
	})
	if err != nil {
		return fmt.Errorf("new hub service: %w", err)
	}
	hubServer, err := hubsrv.New(dirs.Runtime, store.DB(), service.Handlers())
	if err != nil {
		return fmt.Errorf("new hub server: %w", err)
	}
	hubPath, err := hubServer.Listen()
	if err != nil {
		return fmt.Errorf("another hub may be serving %s: %w", dirs.Runtime, err)
	}
	nodeManager, federationServer, federationAddress, err := setupFederation(
		ctx, cfg, dirs, store, reg, service, clk, compiledPolicy.Version,
	)
	if err != nil {
		_ = hubServer.Close()
		return err
	}
	controlServer, err := controlapi.New(controlapi.Options{
		RuntimeDir: dirs.Runtime, DB: store.DB(), Controller: controller, Node: nodeManager,
	})
	if err != nil {
		if federationServer != nil {
			_ = federationServer.Close()
		}
		_ = hubServer.Close()
		return fmt.Errorf("new control server: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	controlErr := make(chan error, 1)
	hubErrCh := make(chan error, 1)
	go func() { controlErr <- controlServer.Serve(runCtx) }()
	go func() { hubErrCh <- hubServer.Serve(runCtx) }()
	var federationErrCh chan error
	if federationServer != nil {
		federationErrCh = make(chan error, 1)
		go func() { federationErrCh <- federationServer.Serve(runCtx) }()
	}
	waitFederation := func() error {
		if federationErrCh == nil {
			return nil
		}
		return <-federationErrCh
	}
	sockWait := make(chan error, 1)
	go func() { sockWait <- waitSocket(runCtx, controlServer.Path()) }()
	select {
	case err := <-controlErr:
		cancel()
		hubServeErr := <-hubErrCh
		federationServeErr := waitFederation()
		_ = hubServer.Close()
		return errors.Join(err, hubServeErr, federationServeErr)
	case err := <-federationErrCh:
		cancel()
		controlServeErr := <-controlErr
		hubServeErr := <-hubErrCh
		_ = hubServer.Close()
		return errors.Join(err, controlServeErr, hubServeErr)
	case err := <-sockWait:
		if err != nil {
			cancel()
			controlServeErr := <-controlErr
			hubServeErr := <-hubErrCh
			federationServeErr := waitFederation()
			_ = hubServer.Close()
			if ctx.Err() != nil {
				// Stopping while starting is a clean stop,
				// not a startup failure.
				return errors.Join(controlServeErr, hubServeErr, federationServeErr)
			}
			return errors.Join(err, controlServeErr, hubServeErr, federationServeErr)
		}
	}
	if federationAddress == "" {
		sayf(errW, "ready: serving %s and %s", hubPath, controlServer.Path())
	} else {
		sayf(errW, "ready: serving %s, %s and federation %s", hubPath, controlServer.Path(), federationAddress)
	}

	go expireLoop(runCtx, errW, service, controller)
	if federationServer != nil {
		go directorySyncLoop(runCtx, errW, nodeManager, clk)
	}
	select {
	case err := <-controlErr:
		cancel()
		hubServeErr := <-hubErrCh
		federationServeErr := waitFederation()
		_ = hubServer.Close()
		return errors.Join(err, hubServeErr, federationServeErr)
	case err := <-hubErrCh:
		cancel()
		controlServeErr := <-controlErr
		federationServeErr := waitFederation()
		_ = hubServer.Close()
		return errors.Join(err, controlServeErr, federationServeErr)
	case err := <-federationErrCh:
		cancel()
		controlServeErr := <-controlErr
		hubServeErr := <-hubErrCh
		_ = hubServer.Close()
		return errors.Join(err, controlServeErr, hubServeErr)
	case <-runCtx.Done():
		controlServeErr := <-controlErr
		hubServeErr := <-hubErrCh
		federationServeErr := waitFederation()
		_ = hubServer.Close()
		return errors.Join(controlServeErr, hubServeErr, federationServeErr)
	}
}

func persistPolicy(
	ctx context.Context,
	db *sql.DB,
	reg *registry.Registry,
	compiled *policy.Policy,
	createdAt time.Time,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin policy transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := policy.Save(ctx, tx, compiled, createdAt); err != nil {
		return fmt.Errorf("save compiled policy: %w", err)
	}
	if err := reg.SetLocalPolicyVersion(ctx, tx, compiled.Version); err != nil {
		return fmt.Errorf("set local policy version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit policy transaction: %w", err)
	}
	return nil
}

func setupFederation(
	ctx context.Context,
	cfg config.Config,
	dirs xdg.Dirs,
	store *eventstore.Store,
	reg *registry.Registry,
	delivery federation.DeliveryReceiver,
	clk clock.Clock,
	policyVersion string,
) (*federation.NodeManager, *federation.Server, string, error) {
	if cfg.Federation.Listen == "" {
		manager, err := federation.NewNodeManager(federation.NodeManagerOptions{
			DB: store.DB(), Registry: reg, Clock: clk, PolicyVersion: policyVersion,
		})
		if err != nil {
			return nil, nil, "", fmt.Errorf("new federation node manager: %w", err)
		}
		return manager, nil, "", nil
	}
	identity, err := federation.EnsureNodeTLS(dirs.Data, cfg.NodeID)
	if err != nil {
		return nil, nil, "", fmt.Errorf("ensure federation TLS identity: %w", err)
	}
	bundle, err := federation.LoadTrustBundle(cfg.Federation.TrustBundle)
	if err != nil {
		return nil, nil, "", fmt.Errorf("load federation trust bundle: %w", err)
	}
	manager, err := federation.NewNodeManager(federation.NodeManagerOptions{
		DB: store.DB(), Registry: reg, Bundle: bundle, Identity: identity, Clock: clk,
		Listen: cfg.Federation.Listen, Endpoint: cfg.Federation.Advertise,
		PolicyVersion: policyVersion,
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("new federation node manager: %w", err)
	}
	organizationID, err := reg.LocalOrganizationID(ctx, store.DB())
	if err != nil {
		return nil, nil, "", fmt.Errorf("read local enrollment state: %w", err)
	}
	if organizationID == "" {
		return manager, nil, "", nil
	}
	server, err := federation.NewServer(federation.ServerOptions{
		Listen: cfg.Federation.Listen, Identity: identity, TrustBundle: bundle,
		DB: store.DB(), Clock: clk, Enrollment: manager.Enrollment(), Delivery: delivery,
		Directory: reg,
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("new federation server: %w", err)
	}
	address, err := server.Listen(ctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("listen for federation: %w", err)
	}
	return manager, server, address, nil
}

type directorySynchronizer interface {
	SyncDirectories(context.Context) error
}

func directorySyncLoop(
	ctx context.Context,
	errW io.Writer,
	synchronizer directorySynchronizer,
	clk clock.Clock,
) {
	ticker := time.NewTicker(directorySyncEvery)
	defer ticker.Stop()
	runDirectorySyncLoop(ctx, errW, synchronizer, clk, ticker.C)
}

// runDirectorySyncLoop performs one startup pass, then no more than one pass
// per 60 seconds according to the injected clock. The tick channel is split
// out so tests can advance logical time without sleeping.
func runDirectorySyncLoop(
	ctx context.Context,
	errW io.Writer,
	synchronizer directorySynchronizer,
	clk clock.Clock,
	ticks <-chan time.Time,
) {
	next := clk.Now().Add(directorySyncEvery)
	syncDirectories(ctx, errW, synchronizer)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			now := clk.Now()
			if now.Before(next) {
				continue
			}
			syncDirectories(ctx, errW, synchronizer)
			next = now.Add(directorySyncEvery)
		}
	}
}

func syncDirectories(ctx context.Context, errW io.Writer, synchronizer directorySynchronizer) {
	if err := synchronizer.SyncDirectories(ctx); err != nil && ctx.Err() == nil {
		sayf(errW, "sync peer directories: %v", err)
	}
}

// expireLoop sweeps admitted reservations and expired controls until the
// context ends. Failures are reported and the next round still runs.
func expireLoop(ctx context.Context, errW io.Writer, service *hubsrv.Service, controller *channel.Controller) {
	ticker := time.NewTicker(expireEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := service.ExpireAdmitted(ctx); err != nil {
				sayf(errW, "expire admitted: %v", err)
			}
			if _, err := controller.Expire(ctx); err != nil {
				sayf(errW, "expire controls: %v", err)
			}
		}
	}
}

// waitSocket returns once path exists, or an error when the context ends
// or the socket never appears.
func waitSocket(ctx context.Context, path string) error {
	timeout := time.NewTimer(controlSocketTimeout)
	defer timeout.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait control socket: %w", ctx.Err())
		case <-timeout.C:
			return fmt.Errorf("control socket %s never appeared", path)
		case <-tick.C:
		}
	}
}

// sayf writes one stderr line; the hub never touches stdout.
func sayf(errW io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(errW, "gifford hub: "+format+"\n", args...)
}
