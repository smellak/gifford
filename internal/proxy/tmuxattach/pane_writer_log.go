package tmuxattach

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/smellak/gifford/internal/tmuxlock"
	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

// ErrPaneWriterLogRequired means configured common logging could not make a
// boundary durable. Before any physical effect it is a closed deferral; after
// a possible effect writerlog.ErrUncertain takes precedence.
var ErrPaneWriterLogRequired = errors.New("tmux attach: writer log required")

var errPaneWriterLogNotOpen = errors.New("tmux attach: writer log interval not open")

type paneLockFileProvider interface {
	DuplicateLockFile(Target) (*os.File, error)
}

type paneWriterInterval struct {
	interval *writerlog.Interval
	file     *os.File
}

// paneWriterLog owns the common-log interval and its duplicate of the locked
// open file description. Open and Release are called while the original pane
// guard still retains the exclusive claim.
type paneWriterLog struct {
	client *writerlog.Client
	locks  paneLockFileProvider

	mu        sync.Mutex
	opening   map[tmuxlock.PanelBinding]struct{}
	intervals map[tmuxlock.PanelBinding]paneWriterInterval
}

func newPaneWriterLog(
	locks paneLockFileProvider,
	client *writerlog.Client,
) (*paneWriterLog, error) {
	if client == nil || client.Enabled() && locks == nil {
		return nil, ErrInvalidOptions
	}
	return &paneWriterLog{
		client: client, locks: locks,
		opening:   make(map[tmuxlock.PanelBinding]struct{}),
		intervals: make(map[tmuxlock.PanelBinding]paneWriterInterval),
	}, nil
}

func (log *paneWriterLog) Open(ctx context.Context, target Target, attempt string) error {
	if log == nil || ctx == nil || target.validate() != nil {
		return ErrInvalidOptions
	}
	if !log.client.Enabled() {
		return nil
	}
	binding := target.liveBinding().PanelBinding
	log.mu.Lock()
	_, exists := log.intervals[binding]
	_, opening := log.opening[binding]
	if !exists && !opening {
		log.opening[binding] = struct{}{}
	}
	log.mu.Unlock()
	if exists || opening {
		return ErrInvalidOptions
	}
	opened := false
	defer func() {
		if opened {
			return
		}
		log.mu.Lock()
		delete(log.opening, binding)
		log.mu.Unlock()
	}()
	file, err := log.locks.DuplicateLockFile(target)
	if err != nil {
		return errors.Join(ErrPaneWriterLogRequired,
			fmt.Errorf("duplicate pane writer lock: %w", err))
	}
	interval, err := log.client.Open(ctx, file, attempt)
	if err != nil {
		return errors.Join(ErrPaneWriterLogRequired, err, file.Close())
	}
	log.mu.Lock()
	delete(log.opening, binding)
	log.intervals[binding] = paneWriterInterval{interval: interval, file: file}
	opened = true
	log.mu.Unlock()
	return nil
}

func (log *paneWriterLog) interval(target Target) (*writerlog.Interval, error) {
	if log == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	if !log.client.Enabled() {
		return nil, nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	entry, ok := log.intervals[target.liveBinding().PanelBinding]
	if !ok {
		return nil, errPaneWriterLogNotOpen
	}
	return entry.interval, nil
}

func (log *paneWriterLog) Release(target Target) error {
	if log == nil || target.validate() != nil {
		return ErrInvalidOptions
	}
	if !log.client.Enabled() {
		return nil
	}
	binding := target.liveBinding().PanelBinding
	log.mu.Lock()
	entry, ok := log.intervals[binding]
	delete(log.intervals, binding)
	log.mu.Unlock()
	if !ok {
		return errPaneWriterLogNotOpen
	}
	return errors.Join(
		writerLogBoundaryError(entry.interval.Release()),
		entry.file.Close(),
	)
}

func writerLogBoundaryError(err error) error {
	if err == nil {
		return nil
	}
	return errors.Join(ErrPaneWriterLogRequired, err)
}
