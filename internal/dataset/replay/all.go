package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/proxy/session"
)

// OpenAll replays every `gifford run` session of the data dir, in session
// id order, one session at a time. A session that does not replay whole
// (still running, cut, out of order, dropped events) yields no window:
// skip, when set, gets its id and the error, and the rest go on.
func OpenAll(dataDir string, every time.Duration, skip func(id string, err error)) (dataset.RecordingSource, error) {
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return nil, fmt.Errorf("%w: list recordings: %w", ErrReplay, err)
	}
	return &all{dataDir: dataDir, every: every, ids: ids, skip: skip}, nil
}

// all opens each session when the one before it is spent.
type all struct {
	dataDir string
	every   time.Duration
	ids     []string
	skip    func(id string, err error)
	cur     *Source
}

func (a *all) Next(ctx context.Context) (dataset.Recording, error) {
	for {
		if a.cur != nil {
			rec, err := a.cur.Next(ctx)
			if !errors.Is(err, io.EOF) {
				return rec, err
			}
			a.cur = nil
		}
		if len(a.ids) == 0 {
			return dataset.Recording{}, io.EOF
		}
		if err := ctx.Err(); err != nil {
			return dataset.Recording{}, fmt.Errorf("replay: %w", err)
		}
		id := a.ids[0]
		a.ids = a.ids[1:]
		src, err := Open(Options{DataDir: a.dataDir, SessionID: id, Every: a.every})
		if err != nil {
			if a.skip != nil {
				a.skip(id, err)
			}
			continue
		}
		a.cur = src
	}
}
