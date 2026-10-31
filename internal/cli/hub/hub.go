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
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/config"
	"github.com/smellak/gifford/internal/controlapi"
	daemoncore "github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/daemon/proxies"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/federation"
	hubsrv "github.com/smellak/gifford/internal/hub"
	"github.com/smellak/gifford/internal/notifications"
	"github.com/smellak/gifford/internal/orchestrator"
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

	// replicationEvery bounds how long a newly due federation outbox row
	// waits before the next claim pass.
	replicationEvery = 250 * time.Millisecond
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
			if err := Run(cmd.Context(), cmd.ErrOrStderr()); err != nil {
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

// Run starts the complete node and blocks until it stops. Both `hub` and
// `daemon` call this entry point so there is one SQLite writer and one set of
// local sockets, never two cooperating processes.
func Run(ctx context.Context, errW io.Writer) error {
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
	controlPath := filepath.Join(dirs.Runtime, "daemon.sock")
	if err := reclaimStaleControlSocket(controlPath); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	databasePath := filepath.Join(dirs.Data, dbName)
	store, err := eventstore.Open(ctx, databasePath)
	if err != nil {
		return fmt.Errorf("open event store: %w", err)
	}
	defer func() { _ = store.Close() }()
	readDB, err := openReadDB(ctx, databasePath)
	if err != nil {
		return fmt.Errorf("open read database: %w", err)
	}
	defer func() { _ = readDB.Close() }()

	clk := agentlink.SystemClock{}
	engine, err := channel.New(channel.Options{Clock: clk})
	if err != nil {
		return fmt.Errorf("new channel engine: %w", err)
	}
	controlEvents := newControlEventSink(store.DB())
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clk, NodeID: cfg.NodeID, Events: controlEvents,
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
	capabilityStore, err := proxies.NewCapabilityStore(store, cfg.NodeID)
	if err != nil {
		return fmt.Errorf("new capability store: %w", err)
	}
	notificationWake := make(chan struct{}, 1)
	notificationAdapter := notifications.DisabledAdapter()
	if configured := cfg.Notifications.TMUX; configured != nil {
		notificationAdapter, err = notifications.NewTMUXAdapter(notifications.TMUXOptions{
			Socket: configured.Socket, Client: configured.Client,
			Duration: configured.Duration, Timeout: configured.Timeout,
			Template: configured.Template,
		})
		if err != nil {
			return fmt.Errorf("new notification adapter: %w", err)
		}
	}
	notificationService, err := notifications.NewService(notifications.ServiceOptions{
		Store: store, NodeID: cfg.NodeID, Clock: clk, Adapter: notificationAdapter,
		Wake: notificationWake,
		ErrorSink: func(err error) {
			sayf(errW, "notification service: %v", err)
		},
	})
	if err != nil {
		return fmt.Errorf("new notification service: %w", err)
	}
	if err := notificationService.Initialize(ctx); err != nil {
		return fmt.Errorf("initialize notification service: %w", err)
	}
	service, err := hubsrv.NewService(hubsrv.ServiceOptions{
		DB: store.DB(), NodeID: cfg.NodeID, Engine: engine, Registry: reg, Clock: clk,
		CapabilityLineage:  capabilityStore,
		FederationRegistry: reg, Policy: compiledPolicy,
		NotificationWake: notificationWake,
	})
	if err != nil {
		return fmt.Errorf("new hub service: %w", err)
	}
	hubServer, err := hubsrv.New(dirs.Runtime, store.DB(), service.Handlers())
	if err != nil {
		return fmt.Errorf("new hub server: %w", err)
	}
	nodeManager, federationServer, federationAddress, err := setupFederation(
		ctx, cfg, dirs, store, reg, service, clk, compiledPolicy.Version,
	)
	if err != nil {
		return err
	}
	if federationServer != nil {
		defer func() { _ = federationServer.Close() }()
	}
	// Listen closes sessions left CONNECTED by a previous node process. Do
	// this before Proxy reconciliation; Serve still starts only after recovery.
	hubPath, err := hubServer.Listen()
	if err != nil {
		return fmt.Errorf("another hub may be serving %s: %w", dirs.Runtime, err)
	}
	defer func() { _ = hubServer.Close() }()
	var replicator *federation.Replicator
	if federationServer != nil {
		replicator, err = nodeManager.NewReplicator(fmt.Sprintf("hub-%d", os.Getpid()),
			func(alert federation.ReplicatorAlert) {
				sayf(errW, "federation alert: %s", alert.Code)
			})
		if err != nil {
			return fmt.Errorf("new federation replicator: %w", err)
		}
	}

	serviceBase := context.WithoutCancel(ctx)
	stateProducer, err := newStateProducer(cfg, store, controller, clk, errW)
	if err != nil {
		return err
	}
	stateLoop := startStateProducerLoop(serviceBase, stateProducer)
	defer stateLoop.stop()
	proxyContext, cancelProxies := context.WithCancel(serviceBase)
	defer cancelProxies()
	proxyRegistry, err := proxies.Start(proxyContext, proxies.Options{
		DB: store.DB(), RuntimeDir: dirs.Runtime, NodeID: cfg.NodeID, Clock: clk,
		CapabilityWriter: capabilityStore, CapabilityLineage: capabilityStore,
	})
	if err != nil {
		return fmt.Errorf("start proxy registry: %w", err)
	}
	defer func() { _ = proxyRegistry.Close() }()
	executor, err := orchestrator.NewExecutor(orchestrator.ExecutorOptions{
		DB: store.DB(), Channel: engine, Approvals: controller,
		Links: proxyRegistry, Clock: clk, Processes: orchestrator.NewProcessInspector(""),
		Endpoints: hubServer, NotificationWake: notificationWake,
	})
	if err != nil {
		return fmt.Errorf("new orchestrator executor: %w", err)
	}
	if _, err := executor.Recover(ctx); err != nil {
		return fmt.Errorf("recover orchestrator deliveries: %w", err)
	}
	worker, err := daemoncore.NewWorker(daemoncore.WorkerOptions{
		DB: store.DB(), Processor: executor, Approvals: executor,
		Prepared: executor, Proxies: proxyRegistry,
	})
	if err != nil {
		return fmt.Errorf("new orchestrator worker: %w", err)
	}
	controlEvents.attach(worker)
	controlServer, err := controlapi.New(controlapi.Options{
		RuntimeDir: dirs.Runtime, DB: store.DB(), ReadDB: readDB,
		Controller: controller, Approvals: worker, Node: nodeManager,
	})
	if err != nil {
		return fmt.Errorf("new control server: %w", err)
	}
	serverContext, cancelServers := context.WithCancel(serviceBase)
	defer cancelServers()
	type serveResult struct {
		name string
		err  error
	}
	serverCount := 2
	serveResults := make(chan serveResult, 3)
	go func() { serveResults <- serveResult{name: "control", err: controlServer.Serve(serverContext)} }()
	go func() { serveResults <- serveResult{name: "hub", err: hubServer.Serve(serverContext)} }()
	if federationServer != nil {
		serverCount++
		go func() {
			serveResults <- serveResult{name: "federation", err: federationServer.Serve(serverContext)}
		}()
	}
	workerLoop := startWorkerLoop(serviceBase, errW, worker, clk)
	notificationLoop := startNotificationLoop(serviceBase, notificationService)
	defer func() {
		workerLoop.stop()
		notificationLoop.stop()
	}()

	readyContext, cancelReady := context.WithCancel(ctx)
	defer cancelReady()
	sockWait := make(chan error, 1)
	go func() { sockWait <- waitSocket(readyContext, controlServer.Path()) }()
	var firstResult *serveResult
	var runErr error
	select {
	case result := <-serveResults:
		firstResult = &result
		runErr = stoppedServerError(result.name, result.err, ctx.Err() != nil)
	case err := <-sockWait:
		if err != nil {
			if ctx.Err() == nil {
				runErr = err
			}
		}
	}
	cancelReady()

	backgroundContext, cancelBackground := context.WithCancel(serviceBase)
	var background sync.WaitGroup
	if runErr == nil && firstResult == nil && ctx.Err() == nil {
		if federationAddress == "" {
			sayf(errW, "ready: serving %s and %s", hubPath, controlServer.Path())
		} else {
			sayf(errW, "ready: serving %s, %s and federation %s", hubPath, controlServer.Path(), federationAddress)
		}
		background.Add(1)
		go func() {
			defer background.Done()
			expireLoop(backgroundContext, errW, service, controller)
		}()
		if federationServer != nil {
			background.Add(2)
			go func() {
				defer background.Done()
				directorySyncLoop(backgroundContext, errW, nodeManager, clk)
			}()
			go func() {
				defer background.Done()
				replicationLoop(backgroundContext, errW, replicator)
			}()
		}
		select {
		case result := <-serveResults:
			firstResult = &result
			runErr = stoppedServerError(result.name, result.err, ctx.Err() != nil)
		case <-ctx.Done():
		}
	}

	// Order matters: stop and wait for database loops, close Hub ingress,
	// drain Proxy consumers, then remove daemon.sock. The read and writer
	// pools close through the defers above, in that order, after every server.
	cancelBackground()
	background.Wait()
	workerLoop.stop()
	notificationLoop.stop()
	stateLoop.stop()
	hubCloseErr := hubServer.Close()
	if federationServer != nil {
		_ = federationServer.Close()
	}
	proxyCloseErr := proxyRegistry.Close()
	cancelServers()
	remaining := serverCount
	if firstResult != nil {
		remaining--
	}
	for range remaining {
		result := <-serveResults
		if result.err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("serve %s: %w", result.name, result.err))
		}
	}
	return errors.Join(runErr, hubCloseErr, proxyCloseErr)
}

func stoppedServerError(name string, err error, stopping bool) error {
	if err != nil {
		return fmt.Errorf("serve %s: %w", name, err)
	}
	if !stopping {
		return fmt.Errorf("serve %s: stopped unexpectedly", name)
	}
	return nil
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
	receiver federationReceiver,
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
		DB: store.DB(), Clock: clk, Enrollment: manager.Enrollment(), Delivery: receiver,
		Ack: receiver, Directory: reg,
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

type federationReceiver interface {
	federation.DeliveryReceiver
	federation.AckReceiver
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

type federationReplicator interface {
	ReplicateOnce(context.Context) (bool, error)
}

func replicationLoop(ctx context.Context, errW io.Writer, replicator federationReplicator) {
	ticker := time.NewTicker(replicationEvery)
	defer ticker.Stop()
	runReplicationPass(ctx, errW, replicator)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runReplicationPass(ctx, errW, replicator)
		}
	}
}

func runReplicationPass(ctx context.Context, errW io.Writer, replicator federationReplicator) {
	for ctx.Err() == nil {
		claimed, err := replicator.ReplicateOnce(ctx)
		if err != nil {
			if ctx.Err() == nil {
				sayf(errW, "replicate federation outbox: %v", err)
			}
			return
		}
		if !claimed {
			return
		}
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

// waitSocket returns once path accepts a connection, or an error when the
// context ends or the socket never becomes live.
func waitSocket(ctx context.Context, path string) error {
	timeout := time.NewTimer(controlSocketTimeout)
	defer timeout.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		conn, err := (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext(ctx, "unix", path)
		if err == nil {
			_ = conn.Close()
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

// reclaimStaleControlSocket removes only a socket path proven to have no live
// listener. The runtime lock is already held, so a failed dial identifies a
// crash remnant rather than a cooperating node. Non-sockets are never removed.
func reclaimStaleControlSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat control socket: %w", err)
	}
	if info.Mode().Type() != os.ModeSocket {
		return fmt.Errorf("control socket path %s is not a socket", path)
	}
	conn, dialErr := (&net.Dialer{Timeout: 100 * time.Millisecond}).Dial("unix", path)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("live control server on %s", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove stale control socket: %w", err)
	}
	return nil
}

// sayf writes one stderr line; the hub never touches stdout.
func sayf(errW io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(errW, "gifford hub: "+format+"\n", args...)
}
