package proxies

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	daemoncore "github.com/smellak/gifford/internal/daemon"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const defaultReconcileEvery = 2 * time.Second

var (
	// ErrInvalidOptions means the registry cannot safely access sessions or sockets.
	ErrInvalidOptions = errors.New("proxies: invalid options")
	// ErrDuplicateAgent means the database contains two live Proxy sessions for one agent.
	ErrDuplicateAgent = errors.New("proxies: duplicate live agent session")
	// ErrClosed means reconciliation was requested after registry shutdown began.
	ErrClosed = errors.New("proxies: registry closed")
)

// Options configures the Proxy-session registry and the links it owns.
type Options struct {
	DB                *sql.DB
	RuntimeDir        string
	NodeID            protocol.NodeID
	Writer            agentlink.GuardWriter
	CapabilityWriter  agentlink.CapabilityWriter
	CapabilityLineage agentlink.CapabilityLineage
	Clock             agentlink.Clock
	ReconcileEvery    time.Duration
	HandshakeTimeout  time.Duration
	HeartbeatTimeout  time.Duration
	ReconnectMin      time.Duration
	ReconnectMax      time.Duration
	// sessionLiveness is a deterministic /proc seam for package tests.
	sessionLiveness sessionLiveness
	// beforeLinkPublish is a deterministic scheduling seam for package tests.
	beforeLinkPublish func(desiredLink)
}

type desiredLink struct {
	session  protocol.OpaqueID
	agent    protocol.AgentID
	revision string
}

type managedLink struct {
	session      protocol.OpaqueID
	revision     string
	client       *agentlink.Client
	resultCancel context.CancelFunc
	resultDone   chan struct{}
}

// Registry owns the clients implied by the sessions table.
type Registry struct {
	opts Options

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once

	reconcileMu  sync.Mutex
	mu           sync.RWMutex
	links        map[protocol.AgentID]managedLink
	resultBlocks map[protocol.AgentID]protocol.OpaqueID
	results      *daemoncore.DeliveryResultConsumer
	processes    sessionLiveness
	resultWG     sync.WaitGroup
	lastErr      error
	closed       bool
}

// Start performs startup reconciliation before launching the periodic loop.
func Start(ctx context.Context, opts Options) (*Registry, error) {
	if ctx == nil || opts.DB == nil || !filepath.IsAbs(opts.RuntimeDir) {
		return nil, ErrInvalidOptions
	}
	if (opts.CapabilityWriter == nil) != (opts.CapabilityLineage == nil) ||
		opts.CapabilityWriter != nil && !opts.NodeID.Valid() {
		return nil, ErrInvalidOptions
	}
	if opts.Clock == nil {
		opts.Clock = agentlink.SystemClock{}
	}
	if opts.ReconcileEvery <= 0 {
		opts.ReconcileEvery = defaultReconcileEvery
	}
	if opts.sessionLiveness == nil {
		opts.sessionLiveness = newSessionLiveness()
	}
	if opts.Writer == nil {
		writer, err := agentlink.NewInputGuardStore(opts.DB, opts.Clock)
		if err != nil {
			return nil, fmt.Errorf("create input guard store: %w", err)
		}
		opts.Writer = writer
	}
	results, err := daemoncore.NewDeliveryResultConsumer(opts.DB, opts.Clock)
	if err != nil {
		return nil, fmt.Errorf("create delivery result consumer: %w", err)
	}
	registryCtx, cancel := context.WithCancel(ctx)
	r := &Registry{
		opts: opts, ctx: registryCtx, cancel: cancel, done: make(chan struct{}),
		links:        make(map[protocol.AgentID]managedLink),
		resultBlocks: make(map[protocol.AgentID]protocol.OpaqueID), results: results,
		processes: opts.sessionLiveness,
	}
	if err := r.Reconcile(ctx); err != nil {
		cancel()
		return nil, err
	}
	go r.run()
	return r, nil
}

// Reconcile makes the client set equal to the live, deliverable pty_proxy
// sessions visible in one database read. It is safe to invoke alongside the
// periodic loop.
func (r *Registry) Reconcile(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidOptions
	}
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()
	r.mu.RLock()
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if err := r.disconnectDeadDetached(ctx); err != nil {
		r.setError(err)
		return err
	}
	desired, err := readDesired(ctx, r.opts.DB)
	if err != nil {
		r.setError(err)
		return err
	}
	var stopped []managedLink
	r.mu.Lock()
	for agent, blockedSession := range r.resultBlocks {
		want, ok := desired[agent]
		if ok && want.session != blockedSession {
			// A newly registered session is a distinct authenticated lifecycle.
			delete(r.resultBlocks, agent)
			continue
		}
		// The result that failed persistence belonged to this exact session.
		// Never recreate it, even if the durable disconnect also failed.
		delete(desired, agent)
	}
	for agent, current := range r.links {
		want, ok := desired[agent]
		if ok && want.session == current.session && want.revision == current.revision {
			continue
		}
		delete(r.links, agent)
		stopped = append(stopped, current)
	}
	r.mu.Unlock()
	for _, managed := range stopped {
		r.stopManaged(managed)
	}
	for agent, want := range desired {
		r.mu.RLock()
		_, exists := r.links[agent]
		r.mu.RUnlock()
		if exists {
			continue
		}
		client, err := agentlink.Start(r.ctx, agentlink.Options{
			RuntimeDir: r.opts.RuntimeDir, DB: r.opts.DB, NodeID: r.opts.NodeID,
			AgentID: want.agent, SessionID: want.session, Writer: r.opts.Writer,
			CapabilityWriter:  r.opts.CapabilityWriter,
			CapabilityLineage: r.opts.CapabilityLineage, Clock: r.opts.Clock,
			HandshakeTimeout: r.opts.HandshakeTimeout, HeartbeatTimeout: r.opts.HeartbeatTimeout,
			ReconnectMin: r.opts.ReconnectMin, ReconnectMax: r.opts.ReconnectMax,
		})
		if err != nil {
			r.setError(err)
			return fmt.Errorf("start proxy link: %w", err)
		}
		// The client owns shutdown of its result stream. Detaching only the
		// cancellation signal lets the consumer drain every accepted buffered
		// result before it exits; stopManaged still owns an explicit cancel.
		resultCtx, resultCancel := context.WithCancel(context.WithoutCancel(r.ctx))
		managed := managedLink{
			session: want.session, revision: want.revision, client: client,
			resultCancel: resultCancel, resultDone: make(chan struct{}),
		}
		if r.opts.beforeLinkPublish != nil {
			r.opts.beforeLinkPublish(want)
		}
		stillDesired, err := linkStillDesired(ctx, r.opts.DB, want)
		if err != nil {
			resultCancel()
			_ = client.Close()
			r.setError(err)
			return err
		}
		if !stillDesired {
			resultCancel()
			_ = client.Close()
			continue
		}
		published := false
		r.mu.Lock()
		blockedSession, blocked := r.resultBlocks[agent]
		_, exists = r.links[agent]
		if !r.closed && (!blocked || blockedSession != want.session) && !exists {
			r.links[agent] = managed
			published = true
		}
		r.mu.Unlock()
		if !published {
			// This client was created from a stale desired snapshot. It never
			// became observable, so no result consumer owns its shutdown.
			resultCancel()
			_ = client.Close()
			continue
		}
		r.resultWG.Add(1)
		go r.consumeResults(resultCtx, want, managed)
	}
	r.clearErrorIfUnblocked()
	return nil
}

func (r *Registry) disconnectDeadDetached(ctx context.Context) error {
	rows, err := r.opts.DB.QueryContext(ctx, `SELECT session_id, process_json, boot_id
		FROM sessions WHERE state = ? AND capture_mode = ? AND ended_at IS NULL`,
		protocol.SessionStateDetached, protocol.CaptureModePTYProxy)
	if err != nil {
		return fmt.Errorf("select detached proxy sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type detachedSession struct {
		id      protocol.OpaqueID
		process sql.NullString
		bootID  sql.NullString
	}
	var detached []detachedSession
	for rows.Next() {
		var session detachedSession
		if err := rows.Scan(&session.id, &session.process, &session.bootID); err != nil {
			return fmt.Errorf("scan detached proxy session: %w", err)
		}
		detached = append(detached, session)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read detached proxy sessions: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close detached proxy sessions: %w", err)
	}
	stamp := eventstore.FormatTime(r.opts.Clock.Now().UTC())
	for _, session := range detached {
		var process protocol.ProcessIdentity
		alive := session.process.Valid && session.bootID.Valid &&
			json.Unmarshal([]byte(session.process.String), &process) == nil &&
			r.processes.alive(process, session.bootID.String) == nil
		if alive {
			continue
		}
		if _, err := r.opts.DB.ExecContext(ctx, `UPDATE sessions
			SET state = ?, ended_at = ?, updated_at = ?
			WHERE session_id = ? AND state = ? AND ended_at IS NULL`,
			protocol.SessionStateDisconnected, stamp, stamp, session.id,
			protocol.SessionStateDetached); err != nil {
			return fmt.Errorf("disconnect dead detached proxy session: %w", err)
		}
	}
	return nil
}

// Snapshot returns the current link state for agent.
func (r *Registry) Snapshot(agent protocol.AgentID) (agentlink.Snapshot, bool) {
	return r.SnapshotFor(r.ctx, r.opts.DB, agent)
}

// SnapshotFor returns link state only when q still sees the exact session
// lifecycle that created the client. A caller holding a transaction passes it
// as q so validation shares that transaction's database snapshot.
func (r *Registry) SnapshotFor(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
) (agentlink.Snapshot, bool) {
	managed, ok := r.currentManaged(ctx, q, agent)
	if !ok {
		return agentlink.Snapshot{}, false
	}
	return managed.client.Snapshot(), true
}

func (r *Registry) currentManaged(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
) (managedLink, bool) {
	if ctx == nil || q == nil || !agent.Valid() {
		return managedLink{}, false
	}
	r.mu.RLock()
	managed, ok := r.links[agent]
	r.mu.RUnlock()
	if !ok {
		return managedLink{}, false
	}
	current, err := linkStillDesired(ctx, q, desiredLink{
		session: managed.session, agent: agent, revision: managed.revision,
	})
	if err != nil || !current {
		return managedLink{}, false
	}
	r.mu.RLock()
	latest, ok := r.links[agent]
	r.mu.RUnlock()
	if !ok || latest.client != managed.client || latest.session != managed.session ||
		latest.revision != managed.revision {
		return managedLink{}, false
	}
	return managed, true
}

// Snapshots returns one race-safe snapshot per managed agent.
func (r *Registry) Snapshots() map[protocol.AgentID]agentlink.Snapshot {
	r.mu.RLock()
	agents := make([]protocol.AgentID, 0, len(r.links))
	for agent := range r.links {
		agents = append(agents, agent)
	}
	r.mu.RUnlock()
	snapshots := make(map[protocol.AgentID]agentlink.Snapshot, len(agents))
	for _, agent := range agents {
		if snapshot, ok := r.SnapshotFor(r.ctx, r.opts.DB, agent); ok {
			snapshots[agent] = snapshot
		}
	}
	return snapshots
}

// Client returns the managed transport for a later daemon stage. Callers do
// not own or close it.
func (r *Registry) Client(agent protocol.AgentID) (*agentlink.Client, bool) {
	return r.ClientFor(r.ctx, r.opts.DB, agent)
}

// ClientFor returns the transport only when q confirms its exact lifecycle.
func (r *Registry) ClientFor(
	ctx context.Context,
	q registry.Querier,
	agent protocol.AgentID,
) (*agentlink.Client, bool) {
	managed, ok := r.currentManaged(ctx, q, agent)
	return managed.client, ok
}

// LastError returns the most recent reconciliation or result-consumer error.
func (r *Registry) LastError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}

// Close stops reconciliation and every client. It is idempotent.
func (r *Registry) Close() error {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		r.cancel()
		<-r.done
		r.reconcileMu.Lock()
		r.mu.Lock()
		links := make([]managedLink, 0, len(r.links))
		for agent, managed := range r.links {
			links = append(links, managed)
			delete(r.links, agent)
		}
		r.mu.Unlock()
		r.reconcileMu.Unlock()
		for _, managed := range links {
			r.stopManaged(managed)
		}
		r.resultWG.Wait()
	})
	return nil
}

func (r *Registry) consumeResults(ctx context.Context, want desiredLink, managed managedLink) {
	defer r.resultWG.Done()
	defer close(managed.resultDone)
	err := r.results.Run(ctx, want.agent, want.session, managed.client)
	if err == nil || errors.Is(err, context.Canceled) || ctx.Err() != nil {
		return
	}
	failure := fmt.Errorf("consume proxy delivery results: %w", err)
	owned := false
	r.mu.Lock()
	current, ok := r.links[want.agent]
	if ok && current.session == want.session && current.client == managed.client {
		delete(r.links, want.agent)
		r.resultBlocks[want.agent] = want.session
		r.lastErr = failure
		owned = true
	}
	r.mu.Unlock()
	if !owned {
		return
	}
	if blockErr := r.disconnectResultSession(context.WithoutCancel(ctx), want); blockErr != nil {
		r.mu.Lock()
		if r.resultBlocks[want.agent] == want.session {
			r.lastErr = errors.Join(failure, blockErr)
		}
		r.mu.Unlock()
	}
	managed.resultCancel()
	_ = managed.client.Close()
}

// disconnectResultSession durably prevents a session whose delivery evidence
// could not be recorded from returning UP or becoming a normal resend source.
// A new authenticated session id may be reconciled normally.
func (r *Registry) disconnectResultSession(ctx context.Context, want desiredLink) error {
	stamp := eventstore.FormatTime(r.opts.Clock.Now().UTC())
	result, err := r.opts.DB.ExecContext(ctx, `UPDATE sessions
		SET state = ?, ended_at = ?, updated_at = ?
		WHERE session_id = ? AND agent_id = ? AND state = ? AND ended_at IS NULL`,
		protocol.SessionStateDisconnected, stamp, stamp, want.session, want.agent,
		protocol.SessionStateConnected)
	if err != nil {
		return fmt.Errorf("durably block result session: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify blocked result session: %w", err)
	}
	if changed == 1 {
		return nil
	}
	var stillLive int
	if err := r.opts.DB.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM sessions WHERE session_id = ? AND agent_id = ?
		AND state = ? AND ended_at IS NULL
	)`, want.session, want.agent, protocol.SessionStateConnected).Scan(&stillLive); err != nil {
		return fmt.Errorf("verify durable result session block: %w", err)
	}
	if stillLive == 1 {
		return fmt.Errorf("durably block result session: %w", daemoncore.ErrInvalidAttempt)
	}
	return nil
}

func (r *Registry) stopManaged(managed managedLink) {
	_ = managed.client.Close()
	if managed.resultDone != nil {
		<-managed.resultDone
	}
	if managed.resultCancel != nil {
		managed.resultCancel()
	}
}

func (r *Registry) run() {
	defer close(r.done)
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.opts.Clock.After(r.opts.ReconcileEvery):
			_ = r.Reconcile(r.ctx)
		}
	}
}

func (r *Registry) setError(err error) {
	r.mu.Lock()
	r.lastErr = err
	r.mu.Unlock()
}

func (r *Registry) clearErrorIfUnblocked() {
	r.mu.Lock()
	if len(r.resultBlocks) == 0 {
		r.lastErr = nil
	}
	r.mu.Unlock()
}

func readDesired(ctx context.Context, db *sql.DB) (map[protocol.AgentID]desiredLink, error) {
	rows, err := db.QueryContext(ctx, `SELECT session_id, agent_id, updated_at FROM sessions
		WHERE state = ? AND capture_mode = ? AND deliverable = 1 AND ended_at IS NULL
		ORDER BY session_id`, protocol.SessionStateConnected, protocol.CaptureModePTYProxy)
	if err != nil {
		return nil, fmt.Errorf("select live proxy sessions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	desired := make(map[protocol.AgentID]desiredLink)
	for rows.Next() {
		var session protocol.OpaqueID
		var agent protocol.AgentID
		var revision string
		if err := rows.Scan(&session, &agent, &revision); err != nil {
			return nil, fmt.Errorf("scan live proxy session: %w", err)
		}
		if !session.Valid() || !agent.Valid() || revision == "" {
			return nil, fmt.Errorf("%w: invalid session identity", ErrInvalidOptions)
		}
		if _, exists := desired[agent]; exists {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateAgent, agent)
		}
		desired[agent] = desiredLink{session: session, agent: agent, revision: revision}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live proxy sessions: %w", err)
	}
	return desired, nil
}

func linkStillDesired(ctx context.Context, db registry.Querier, want desiredLink) (bool, error) {
	var desired int
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions
		WHERE session_id = ? AND agent_id = ? AND state = ? AND capture_mode = ?
		AND deliverable = 1 AND ended_at IS NULL AND updated_at = ?)`,
		want.session, want.agent, protocol.SessionStateConnected,
		protocol.CaptureModePTYProxy, want.revision).Scan(&desired)
	if err != nil {
		return false, fmt.Errorf("revalidate live proxy session: %w", err)
	}
	return desired == 1, nil
}
