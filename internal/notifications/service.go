package notifications

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	defaultPollInterval = time.Second
	defaultBatchSize    = 100
	defaultMaxAttempts  = 3
	defaultRetryDelay   = time.Second
)

// Clock supplies both durable timestamps and the fallback wake timer.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

// ServiceOptions supplies the notification outbox dependencies. Wake is a
// coalescing hint only; every pass re-reads durable source tables.
type ServiceOptions struct {
	Store        *eventstore.Store
	NodeID       protocol.NodeID
	Clock        Clock
	Entropy      io.Reader
	Adapter      Adapter
	Wake         <-chan struct{}
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	RetryDelay   time.Duration
	ErrorSink    func(error)

	// afterAdapter is a crash-edge seam for package tests. Production leaves
	// it nil. Returning an error simulates loss after the external effect and
	// before RENDERED is persisted.
	afterAdapter func(Notification) error
	// duringBaseline is a package-test seam invoked after the durable-source
	// snapshot while the immediate transaction still owns the writer lock.
	duringBaseline func()
}

// Service owns human_notifications and the two notification event types.
// It never writes message delivery, channel or approval state.
type Service struct {
	store          *eventstore.Store
	dbNode         protocol.NodeID
	clock          Clock
	ids            *idGenerator
	adapter        Adapter
	wake           <-chan struct{}
	pollInterval   time.Duration
	batchSize      int
	maxAttempts    int
	retryDelay     time.Duration
	errorSink      func(error)
	afterAdapter   func(Notification) error
	duringBaseline func()
}

// NewService validates dependencies but performs no I/O.
func NewService(opts ServiceOptions) (*Service, error) {
	if opts.Store == nil || !opts.NodeID.Valid() || opts.Clock == nil {
		return nil, ErrInvalidOptions
	}
	if opts.Entropy == nil {
		opts.Entropy = rand.Reader
	}
	if opts.Adapter == nil {
		opts.Adapter = DisabledAdapter()
	}
	if !opts.Adapter.Name().valid() {
		return nil, ErrInvalidOptions
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = defaultPollInterval
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = defaultMaxAttempts
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = defaultRetryDelay
	}
	if opts.PollInterval < time.Millisecond || opts.BatchSize < 1 ||
		opts.BatchSize > eventstore.MaxBatchEvents || opts.MaxAttempts < 1 ||
		opts.MaxAttempts > 100 || opts.RetryDelay < time.Millisecond {
		return nil, ErrInvalidOptions
	}
	return &Service{
		store: opts.Store, dbNode: opts.NodeID, clock: opts.Clock,
		ids: &idGenerator{entropy: opts.Entropy}, adapter: opts.Adapter,
		wake: opts.Wake, pollInterval: opts.PollInterval, batchSize: opts.BatchSize,
		maxAttempts: opts.MaxAttempts, retryDelay: opts.RetryDelay,
		errorSink: opts.ErrorSink, afterAdapter: opts.afterAdapter,
		duringBaseline: opts.duringBaseline,
	}, nil
}

// Initialize installs the first-start baseline before any local ingress is
// served. Existing eligible objects are registered without events or effects.
func (s *Service) Initialize(ctx context.Context) error {
	_, err := s.ensureBaseline(ctx)
	return err
}

// RunOnce exhausts source discovery and all currently-due outbox rows in
// bounded transactions. On the very first call it only records the baseline.
func (s *Service) RunOnce(ctx context.Context) (int, error) {
	baselined, err := s.ensureBaseline(ctx)
	if err != nil || baselined {
		return 0, err
	}
	total := 0
	for {
		count, err := s.discoverBatch(ctx)
		if err != nil {
			return total, err
		}
		total += count
		if count == 0 {
			break
		}
	}
	for {
		count, err := s.renderPendingBatch(ctx)
		if err != nil {
			return total, err
		}
		total += count
		if count == 0 {
			break
		}
	}
	return total, nil
}

// Run reconciles immediately, then on a capacity-one wake hint or the 1 s
// fallback. Operational failures are reported with metadata-free errors and
// retried on the next pass; an adapter failure never stops the Hub.
func (s *Service) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if _, err := s.RunOnce(ctx); err != nil && ctx.Err() == nil && s.errorSink != nil {
			s.errorSink(fmt.Errorf("notification reconciliation: %w", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-s.wake:
		case <-s.clock.After(s.pollInterval):
		}
	}
}
