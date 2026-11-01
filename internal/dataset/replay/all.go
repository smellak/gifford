package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/session"
)

// OpenAll replays every `gifford run` session of the data dir, in session
// id order, one session at a time. A session that does not replay whole
// (still running, cut, out of order, dropped events) yields no window:
// skip, when set, gets its id and the error, and the rest go on.
func OpenAll(dataDir string, every time.Duration, skip func(id string, err error)) (dataset.RecordingSource, error) {
	return openAll(Options{DataDir: dataDir, Every: every}, skip)
}

// OpenAllLive is OpenAll with every window cut in live form, each
// session's harness profile picked from live: see Options.Live.
func OpenAllLive(dataDir string, every time.Duration, live profiles.Profiles, skip func(id string, err error)) (dataset.RecordingSource, error) {
	return openAll(Options{DataDir: dataDir, Every: every, Live: &live}, skip)
}

// OpenAllLiveV2 is OpenAll under the closed empty-input/permission-dialog
// policy. Permission sources are exact recorded harness/version pairs.
func OpenAllLiveV2(dataDir string, every time.Duration, live profiles.Profiles, sources []session.PermissionDialogSource, skip func(id string, err error)) (dataset.RecordingSource, error) {
	return openAll(Options{DataDir: dataDir, Every: every, LiveV2: &LiveV2Options{
		Profiles: live, PermissionSources: append([]session.PermissionDialogSource(nil), sources...),
	}}, skip)
}

func openAll(opts Options, skip func(id string, err error)) (dataset.RecordingSource, error) {
	ids, err := session.ListSessions(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("%w: list recordings: %w", ErrReplay, err)
	}
	return &all{opts: opts, ids: ids, skip: skip}, nil
}

// all opens each session when the one before it is spent.
type all struct {
	// opts are every session's options but its id.
	opts Options
	ids  []string
	skip func(id string, err error)
	cur  *Source
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
		opts := a.opts
		opts.SessionID = id
		src, err := Open(opts)
		if err != nil {
			if a.skip != nil {
				a.skip(id, err)
			}
			continue
		}
		a.cur = src
	}
}
