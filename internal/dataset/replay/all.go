package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
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

// OpenAllLiveV2WithPermissionEvidenceLegacy is OpenAllLiveV2 with one exact,
// explicit legacy cohort. It validates the cohort before returning a source,
// so evidence-policy failures cannot be reported through skip.
func OpenAllLiveV2WithPermissionEvidenceLegacy(
	dataDir string,
	every time.Duration,
	live profiles.Profiles,
	sources []session.PermissionDialogSource,
	legacy map[protocol.OpaqueID]struct{},
	skip func(id string, err error),
) (dataset.RecordingSource, error) {
	legacy = cloneLegacyPermissionEvidence(legacy)
	opts := LiveV2Options{
		Profiles: live, PermissionSources: append([]session.PermissionDialogSource(nil), sources...),
		PermissionEvidenceLegacy: legacy, StrictPermissionEvidence: true,
	}
	if err := validatePermissionEvidenceCohort(dataDir, opts); err != nil {
		return nil, err
	}
	return openAll(Options{DataDir: dataDir, Every: every, LiveV2: &opts}, skip)
}

func cloneLegacyPermissionEvidence(in map[protocol.OpaqueID]struct{}) map[protocol.OpaqueID]struct{} {
	out := make(map[protocol.OpaqueID]struct{}, len(in))
	for id := range in {
		out[id] = struct{}{}
	}
	return out
}

// validatePermissionEvidenceCohort checks every event log before the ordinary
// replay pass can skip it. Listed sessions must have no evidence at all;
// configured, unlisted sessions must have one complete and consistent stream.
func validatePermissionEvidenceCohort(dataDir string, opts LiveV2Options) error {
	ids, err := session.ListSessions(dataDir)
	if err != nil {
		return permissionEvidenceCohortError(fmt.Errorf("list recordings: %w", err))
	}
	foundLegacy := 0
	for _, id := range ids {
		_, listed := opts.PermissionEvidenceLegacy[protocol.OpaqueID(id)]
		if listed {
			foundLegacy++
		}
		raw, events, err := session.OpenPair(dataDir, id)
		if err != nil {
			return permissionEvidenceCohortError(errors.New("cannot open a cohort session capture"))
		}
		evs, readErr := session.ReadEventLog(events, id)
		_ = raw.Close()
		_ = events.Close()
		if readErr != nil {
			return permissionEvidenceCohortError(errors.New("cannot validate a cohort session event log"))
		}
		var opened session.PTYOpened
		if err := decode(evs[0].Payload, &opened); err != nil {
			return permissionEvidenceCohortError(errors.New("cannot validate a cohort session pty.opened"))
		}
		source := session.PermissionDialogSources(opts.PermissionSources, opened.Harness, opened.HarnessVersion)
		switch {
		case listed && !source:
			return permissionEvidenceCohortError(errors.New("legacy session is not a configured permission dialog source"))
		case listed:
			if err := legacyPermissionEvidence(opened, evs); err != nil {
				return permissionEvidenceCohortError(err)
			}
		case source:
			if _, err := permissionEvidence(opened, evs); err != nil {
				return permissionEvidenceCohortError(err)
			}
		}
	}
	if foundLegacy != len(opts.PermissionEvidenceLegacy) {
		return permissionEvidenceCohortError(errors.New("legacy session is absent from the data directory"))
	}
	return nil
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
			if errors.Is(err, ErrPermissionEvidenceCohort) {
				return dataset.Recording{}, err
			}
			if a.skip != nil {
				a.skip(id, err)
			}
			continue
		}
		a.cur = src
	}
}
